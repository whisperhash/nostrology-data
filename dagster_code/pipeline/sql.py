"""Render the Athena UNLOAD query for a kind — the port of run.sh's render_sql + the per-class
template selection (run.sh:388-392, 657-682).

Picks the extract template by the kind's archive class (kinds.kind_to_class — the producer range
law), substitutes its placeholders (__KIND__ __BUCKET__ __RUN_ID__ __GLUE_DB__ __GLUE_TABLE__
__EXTRA_WHERE__), and builds
__EXTRA_WHERE__ for the INTO=zapstore a-tag-filtered 9735 subset. Pure (file read + string subst);
no AWS. Faithful to the SEAM verb cmd_refresh_extract_only, which (unlike cmd_refresh) does NOT
implement LATEST_DT_ONLY, so __EXTRA_WHERE__ is empty except for the zapstore subset.
"""

import os
import re

from .kinds import kind_to_class

# class -> extract template (sql/). Mirrors the cmd_refresh_extract_only case (run.sh:657-661), which
# agrees with kind_to_class for every supported kind.
_TEMPLATE_FOR_CLASS = {
    "replaceable": "extract_replaceable_latest.sql",
    "addressable": "extract_addressable_latest.sql",
    "regular": "extract_regular_id_dedup.sql",
}

# The zapstore-attributed 9735 subset: keep only zaps carrying an `a` tag addressing a 32267 app
# (run.sh:680). Trino: element_at(t,1)/element_at(t,2) — NOT t[1]/t[2] (memory: element_at gotcha).
_ZAPSTORE_EXTRA_WHERE = (
    "AND cardinality(filter(tags, t -> element_at(t, 1) = 'a' "
    "AND element_at(t, 2) LIKE '32267:%')) > 0"
)

# The marketplaces surface's INTO overrides: (kind, into) -> template. Unlike the zapstore
# subset (same template, extra WHERE), these swap the TEMPLATE itself -- the marketplaces
# compute needs ALL versions of its addressable kinds (farm detection + archive_events span
# every revision; the current-version selection with the exact NIP-01 tie-break happens in
# pipeline/marketplaces.py), and the kind-5 (event-deletion-requests) extract is an AGGREGATION
# only the tiny self-authored (author, tag, value, created_at) set leaves Athena, never the tens
# of millions of raw deletion events. __EXTRA_WHERE__ stays empty for all three.
_INTO_TEMPLATE = {
    ("30402", "markets"): "extract_addressable_all.sql",
    ("30406", "markets"): "extract_addressable_all.sql",
    ("5", "markets-deletions"): "extract_markets_deletions.sql",
    # The routstr surface: the three small kinds reuse the marketplaces all-versions
    # template unchanged (the compute needs every announcement/catalog/review revision
    # for first-seen dates and history series); the fat
    # kind-38422 (routstr-analytics-snapshots) has its own template that keeps only the LAST
    # snapshot per (pubkey, UTC day) plus the schema-tag filter - see the template
    # header for why both semantic filters live in the SQL.
    ("38421", "routstr"): "extract_addressable_all.sql",
    ("38423", "routstr"): "extract_addressable_all.sql",
    ("38425", "routstr"): "extract_addressable_all.sql",
    ("38422", "routstr"): "extract_routstr_analytics_daily.sql",
    # The ngit surface: the two addressable roster kinds - kind-30617 (repo-announcements)
    # and kind-30618 (repo-states) - reuse the all-versions template (first-seen dates, the
    # republish-loop rules and the push ledger read every version);
    # kind-10317 (grasp-server-lists) gets the replaceable all-versions template; the four regular
    # kinds carry the git-subset filter IN their templates, so only kind-1111 (comments),
    # kind-7 (reactions), kind-17 (external-content-reactions) and kind-9735 (zap-receipts)
    # naming git objects leave Athena; the kind-5 (event-deletion-requests) extract is reduced
    # to the DISTINCT self-authored (author, coord, created_at) triples (the compute applies
    # the horizon per triple).
    ("30617", "ngit"): "extract_addressable_all.sql",
    ("30618", "ngit"): "extract_addressable_all.sql",
    ("10317", "ngit"): "extract_replaceable_all.sql",
    ("1111", "ngit"): "extract_ngit_comments.sql",
    ("7", "ngit"): "extract_ngit_reactions.sql",
    ("17", "ngit"): "extract_ngit_external_reactions.sql",
    ("9735", "ngit"): "extract_ngit_zaps.sql",
    ("5", "ngit"): "extract_ngit_deletions.sql",
}


def template_for_kind(kind, into: str = "") -> str:
    """The extract .sql filename for a kind (+ optional INTO override). Raises on an
    ephemeral/unknown kind (no archived class - matches run.sh's `unsupported KIND` abort)."""
    override = _INTO_TEMPLATE.get((str(kind), into))
    if override:
        return override
    cls = kind_to_class(kind)
    tmpl = _TEMPLATE_FOR_CLASS.get(cls) if cls else None
    if not tmpl:
        raise RuntimeError(f"unsupported kind={kind} (class={cls}) — no extract template")
    return tmpl


def extra_where(kind, into: str = "") -> str:
    """The __EXTRA_WHERE__ clause. Empty for a full extract; the a-tag filter for INTO=zapstore
    (valid ONLY with kind-9735 (zap-receipts), mirroring the seam verb's guard)."""
    if not into:
        return ""
    if (str(kind), into) in _INTO_TEMPLATE:
        return ""  # the marketplaces overrides carry their filters inside the template
    if into != "zapstore" or int(kind) != 9735:
        raise RuntimeError(
            f"INTO=zapstore requires kind-9735 (zap-receipts) (got kind={kind} into={into})")
    return _ZAPSTORE_EXTRA_WHERE


def render(sql_dir: str, *, kind, bucket: str, run_id: str, glue_db: str, glue_table: str,
           into: str = "") -> str:
    """Render the UNLOAD query: pick the template, read it, substitute its placeholders.

    The | delimiter run.sh's sed used is moot here - str.replace handles the s3:// slashes/quotes
    in the values directly. __EXTRA_WHERE__ is computed from (kind, into). The archive's Glue table
    is the deployment's own (NOSTROLOGY_GLUE_DB, NOSTROLOGY_GLUE_TABLE), so no template names a
    database. A placeholder left over refuses the render rather than reaching Athena."""
    tmpl_path = os.path.join(sql_dir, template_for_kind(kind, into))
    with open(tmpl_path) as fh:
        sql = fh.read()
    where = extra_where(kind, into)
    out = (
        sql.replace("__KIND__", str(kind))
        .replace("__BUCKET__", bucket)
        .replace("__RUN_ID__", run_id)
        .replace("__GLUE_DB__", glue_db)
        .replace("__GLUE_TABLE__", glue_table)
        .replace("__EXTRA_WHERE__", where)
    )
    left = sorted(set(re.findall(r"__[A-Z][A-Z_]*__", out)))
    if left:
        raise RuntimeError(f"{os.path.basename(tmpl_path)}: unsubstituted placeholder(s) {left}")
    return out
