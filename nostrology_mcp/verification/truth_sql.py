"""The independent answer: SQL over the raw event tables, run read-only on Aurora.

This is deliberately NOT the MCP server's SQL. Every statement here is written
against the table definitions in `schema/init.sql`, and the native/republished
partition is applied from `subjects.is_native` - the domain rule - so agreement
between the two sides is evidence rather than a shared code path.

Access is a read-only login on the box: `ssh "$NOSTROLOGY_RO_SSH"` -> `psql` as
`nostrology_ro`. The long PGCONNECT_TIMEOUT dates from when the cluster scaled
to zero; it costs nothing warm and still covers a restart or a failover.

Each section is emitted as a SINGLE line of `json_agg` output. That matters:
catalog names and release notes are third-party text that can contain delimiters
and newlines, and JSON string escaping is the only framing that survives them.
"""
import json
import os
import subprocess
import tempfile

from .subjects import CURATOR_PUBKEY, CURATOR_OWN_D_TAGS

PGUSER = "nostrology_ro"
PGDATABASE = "nostrology"
# The Aurora endpoint is read on the box, from PGUSER's entry in its ~/.pgpass, so no copy of it
# lives here: the remote shell expands this inside the psql command line. Without the entry it reads
# as a host name that says so, which psql then reports, rather than as empty (psql's unix socket).
PGHOST = ('"$(awk -F: \'$4 == "%s" {print $1; f = 1; exit} END {exit !f}\' ~/.pgpass'
          ' || echo no-pgpass-entry-for-%s)"' % (PGUSER, PGUSER))

# Rendered into every statement that needs the partition. Kept as SQL text (not a
# bind parameter) so the whole script can go over one stdin pipe.
_CUR = f"'{CURATOR_PUBKEY}'"
_OWN = "array[" + ",".join(f"'{d}'" for d in CURATOR_OWN_D_TAGS) + "]"


def _native(pubkey_col, dtag_col):
    return f"({pubkey_col} <> {_CUR} OR {dtag_col} = ANY({_OWN}))"


def _sections():
    """name -> SELECT that yields the rows for that section."""
    nat_rel = _native("pubkey", "app_d_tag")
    nat_cat = _native("pubkey", "d_tag")
    nat_zap = _native("app_pubkey", "app_d_tag")
    utc = "(to_timestamp(created_at) at time zone 'UTC')::date"

    return {
        # ---- Routstr (2026-08-29): the page's own document row, plus INDEPENDENT
        # SQL over the substrate ledger (a third statement of the 30d/all-time
        # rule, written here rather than shared with the tool SQL) -----------
        "ROUTSTR_DOC": """
            select data from routstr_snapshot order by generated_utc desc, id desc limit 1
        """,
        "ROUTSTR_PROVIDER_SUMS": """
            with snap as (select generated_utc from routstr_snapshot
                          order by generated_utc desc, id desc limit 1),
                 w as (select (generated_utc at time zone 'UTC')::date as today from snap)
            select d.pubkey, p.name, p.excluded,
                   coalesce(sum(d.msats) filter (where d.grain = 'd' and d.day >= w.today - 30
                                                   and d.day <= w.today - 1), 0) msats_30d,
                   coalesce(sum(d.tokens) filter (where d.grain = 'd' and d.day >= w.today - 30
                                                    and d.day <= w.today - 1), 0) tokens_30d,
                   sum(d.msats) msats_alltime, sum(d.requests) requests_alltime,
                   sum(d.tokens) tokens_alltime
            from routstr_provider_days d
            join routstr_providers p on p.pubkey = d.pubkey, w
            group by d.pubkey, p.name, p.excluded
            order by msats_alltime desc, d.pubkey
        """,
        "ROUTSTR_MODEL_SUMS": """
            with snap as (select generated_utc from routstr_snapshot
                          order by generated_utc desc, id desc limit 1),
                 w as (select (generated_utc at time zone 'UTC')::date as today from snap)
            select m.model,
                   coalesce(sum(m.msats) filter (where m.grain = 'd' and m.day >= w.today - 30
                                                   and m.day <= w.today - 1), 0) msats_30d,
                   sum(m.msats) msats_alltime, sum(m.requests) requests_alltime
            from routstr_model_days m, w
            group by m.model
            order by msats_alltime desc, m.model
        """,
        # ---- scalars the questions quote directly -------------------------
        "SUMMARY": f"""
            select
              (select count(*) from events_kind_32267_app_catalog) catalog_rows,
              (select count(*) from events_kind_32267_app_catalog where {nat_cat}) catalog_native,
              (select count(*) from events_kind_32267_app_catalog where not {nat_cat}) catalog_republished,
              (select count(*) from events_kind_32267_app_catalog where pubkey = {_CUR}) curator_catalog_rows,
              (select count(*) from events_kind_30063_app_releases) release_rows,
              (select count(*) from (select distinct pubkey, app_d_tag from events_kind_30063_app_releases) x) apps_with_release,
              (select count(*) from events_kind_30267_app_stacks) stacks_total,
              (select count(*) from events_kind_30267_app_stacks
                 where coalesce(array_length(app_refs,1),0) > 0) stacks_public,
              (select count(*) from events_kind_9735_zaps_zapstore) zap_rows,
              (select coalesce(sum(amount_msats),0) from events_kind_9735_zaps_zapstore) zap_msats,
              (select to_char(to_timestamp(max(created_at)) at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
                 from events_kind_30063_app_releases) max_release_at,
              (select to_char(to_timestamp(max(created_at)) at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"')
                 from events_kind_9735_zaps_zapstore) max_zap_at,
              (select count(distinct pubkey) from events_kind_32267_app_catalog) catalog_pubkeys,
              (select count(distinct c.pubkey) from events_kind_32267_app_catalog c
                 join events_kind_0_profiles p on p.pubkey = c.pubkey) catalog_pubkeys_with_k0,
              (select count(distinct c.pubkey) from events_kind_32267_app_catalog c
                 join events_kind_0_profiles p on p.pubkey = c.pubkey
                 where nullif(p.name,'') is not null or nullif(p.display_name,'') is not null)
                 catalog_pubkeys_with_name
        """,

        # ---- daily publishing series, by partition ------------------------
        "DAILY_RELEASES": f"""
            select {utc} d,
                   count(*) filter (where {nat_rel})     native,
                   count(*) filter (where not {nat_rel}) republished
            from events_kind_30063_app_releases group by 1 order by 1
        """,
        "DAILY_NEWAPPS": f"""
            select (to_timestamp(first_at) at time zone 'UTC')::date d,
                   count(*) filter (where nat)     native,
                   count(*) filter (where not nat) republished
            from (
              select pubkey, app_d_tag, min(created_at) first_at, {nat_rel} nat
              from events_kind_30063_app_releases group by 1,2,4
            ) f group by 1 order by 1
        """,

        # ---- zaps, per app per day: the master zap table. Every zap question
        # (global daily, per-app total, any window) is derived from this.
        "APP_ZAPS_DAILY": f"""
            select app_pubkey pubkey, app_d_tag d_tag, {utc} d,
                   count(*) zaps, coalesce(sum(amount_msats),0)::bigint msats,
                   bool_or({nat_zap}) is_native
            from events_kind_9735_zaps_zapstore group by 1,2,3 order by 3,1,2
        """,

        # ---- current-state snapshots --------------------------------------
        "APP_STACKS": """
            select split_part(ref,':',2) pubkey, split_part(ref,':',3) d_tag,
                   count(distinct (pubkey, d_tag)) stacks
            from (select pubkey, d_tag, unnest(app_refs) ref
                  from events_kind_30267_app_stacks
                  where coalesce(array_length(app_refs,1),0) > 0) x
            where split_part(ref,':',1) = '32267'
            group by 1,2 order by stacks desc
        """,
        "PUBLISHER_APPS": f"""
            select pubkey, count(distinct d_tag) apps
            from events_kind_32267_app_catalog where {nat_cat}
            group by 1 order by apps desc
        """,

        # ---- reference-record ground truth --------------------------------
        "CATALOG": f"""
            select pubkey, d_tag, nullif(name,'') name, nullif(repository,'') repository,
                   nullif(url,'') url, {nat_cat} is_native
            from events_kind_32267_app_catalog order by pubkey, d_tag
        """,
        "RELEASES_BY_APP": f"""
            select pubkey, app_d_tag d_tag, count(*) releases,
                   min({utc})::text first_day, max({utc})::text last_day,
                   {nat_rel} is_native
            from events_kind_30063_app_releases group by 1,2,6 order by 1,2
        """,
        # Per-release detail, limited to the apps the battery asks about - the
        # full table is 16k rows and no question needs it. The curator alone would
        # contribute 13k, hence the (pubkey, app) scoping rather than pubkey-only.
        "SUBJECT_RELEASES": f"""
            select pubkey, app_d_tag d_tag, {utc}::text d, created_at,
                   nullif(version,'') version, coalesce(channel,'') channel
            from events_kind_30063_app_releases
            where %(subject_scope)s
            order by created_at desc
        """,
        "PROFILES": """
            select c.pubkey,
                   nullif(p.name,'') name, nullif(p.display_name,'') display_name,
                   nullif(p.nip05,'') nip05,
                   (p.pubkey is not null) has_k0
            from (select distinct pubkey from events_kind_32267_app_catalog) c
            left join events_kind_0_profiles p on p.pubkey = c.pubkey
            order by c.pubkey
        """,
    }


def _scope_sql(subject_apps):
    """(pubkey, app_id or None) pairs -> a WHERE predicate. None = all that key's apps."""
    terms = []
    for pubkey, app_id in subject_apps:
        if app_id is None:
            terms.append(f"(pubkey = '{pubkey}')")
        else:
            terms.append(f"(pubkey = '{pubkey}' AND app_d_tag = '{app_id}')")
    return "(" + " OR ".join(terms) + ")" if terms else "false"


def build_script(subject_apps):
    scope = _scope_sql(subject_apps)
    out = ["\\pset footer off", "\\pset tuples_only on", "\\pset format unaligned"]
    for name, sel in _sections().items():
        sel = sel.replace("%(subject_scope)s", scope)
        out.append(f"\\echo #{name}")
        out.append(f"select coalesce(json_agg(row_to_json(t)), '[]'::json) from ({sel}) t;")
    out.append("\\echo #END")
    return "\n".join(out) + "\n"


def build_sections_script(sections):
    """A psql script that emits each section as one json_agg line, no scoping."""
    out = ["\\pset footer off", "\\pset tuples_only on", "\\pset format unaligned"]
    for name, sel in sections.items():
        out.append(f"\\echo #{name}")
        out.append(f"select coalesce(json_agg(row_to_json(t)), '[]'::json) from ({sel}) t;")
    out.append("\\echo #END")
    return "\n".join(out) + "\n"


def run(subject_apps, timeout=900):
    """Execute the ground-truth script and return {section: [row dicts]}."""
    return run_script(build_script(subject_apps), timeout)


def run_sections(sections, timeout=900):
    """Execute an arbitrary {name: SELECT} map the same way (the Routstr battery)."""
    return run_script(build_sections_script(sections), timeout)


def ssh_host():
    """The ssh destination of a read-only login on the deployment's box: NOSTROLOGY_RO_SSH (an
    ~/.ssh/config alias or user@host), read when a script runs, never at import, so the module
    and its tests load without it."""
    host = os.environ.get("NOSTROLOGY_RO_SSH", "").strip()
    if not host:
        raise RuntimeError("set NOSTROLOGY_RO_SSH to the ssh destination of a read-only login on the "
                           "deployment's box (an ~/.ssh/config alias works)")
    return host


def run_script(script, timeout=900):
    host = ssh_host()  # first, so an unset NOSTROLOGY_RO_SSH fails before anything is written
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as fh:
        fh.write(script)
        path = fh.name
    remote = (f"PGCONNECT_TIMEOUT=60 psql -h {PGHOST} -U {PGUSER} -d {PGDATABASE} "
              f"-v ON_ERROR_STOP=1 -f -")
    with open(path) as stdin:
        proc = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=30", host, remote],
            stdin=stdin, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed: {proc.stderr[-800:]}")
    return parse(proc.stdout)


def parse(text):
    sections, name = {}, None
    for line in text.splitlines():
        if line.startswith("#"):
            name = line[1:].strip()
            continue
        if name and name != "END" and line.strip():
            sections[name] = json.loads(line)
            name = None
    return sections
