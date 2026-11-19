"""The Discord posts for a failed and a recovered Dagster run (pure: facts in, webhook JSON out,
so every wording and limit rule unit-tests without dagster). sensors.py gathers the facts from
the instance and posts what these return.

What a failure post is for. These jobs rebuild derived data products (serving tables and
snapshots) from the nozzl archive, all or nothing per product: a step that cannot prove its
inputs complete refuses, and what consumers read stays at its last good version. So a failure
loses nothing at the source; what it costs is staleness, spread over whoever reads the products
the run did not refresh. The post answers, in this order: who is affected (derived from the
lineage graph, never from the job's name: one job feeds several surfaces, and a surface reads
several jobs), what did not refresh and since when, what did refresh (a reader may now combine
new and old products), why, and when it heals or how to re-run. The run itself is the footer.

Output is ASCII apart from the two status emoji, written as escapes; Discord renders the
<t:EPOCH:STYLE> timestamps in each reader's own time zone.
"""

import datetime

RED = 15158332     # the CI channel's deploy posts' failure colour
GREEN = 3066993    # ... and their success colour
FAIL_MARK = "\u274c"   # cross mark
OK_MARK = "\u2705"     # check mark

ENV_LABEL = "dev = nostrolo.gy"   # this box serves the PUBLIC site: the env is not the name
UI_BASE = "http://localhost:23010"  # the operator Dagster UI, through a tunnel

# The audited re-run that needs no tunnel, so it works from any browser, a phone's included: the
# materialize-* actions of the deployment's Deploy (dev) workflow, with repo=nostrology-data, each a
# `dagster job launch` of the whole job, queued like a scheduled run. That workflow is not in this
# repository, so the post names it and its inputs rather than linking it. test_pipeline_alerts.py
# holds this map to the deployment's verbs: materialize-<surface> for <surface>_refresh, these five.
RERUN_ACTIONS = {
    "marketplaces_refresh": "materialize-marketplaces",
    "ngit_refresh": "materialize-ngit",
    "nutzaps_refresh": "materialize-nutzaps",
    "routstr_refresh": "materialize-routstr",
    "zapstore_refresh": "materialize-zapstore",
}

TITLE_MAX = 256
DESC_MAX = 4096
CAUSE_MAX = 350
KIND_ORDER = ("web", "landing", "mcp")


# --------------------------------------------------------------------------------------------
# The lineage walk
# --------------------------------------------------------------------------------------------

def children_of(upstream: dict) -> dict:
    """Invert node -> {upstream nodes} into node -> {downstream nodes}."""
    out = {}
    for node, ups in upstream.items():
        for u in ups:
            out.setdefault(u, set()).add(node)
    return out


def reachable(start, children: dict) -> set:
    """Every node downstream of `start` (not including `start` itself)."""
    seen, todo = set(), list(start)
    while todo:
        for nxt in children.get(todo.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    return seen


def affected_consumers(stale, upstream: dict, consumer_ids) -> set:
    """The consumers downstream of any stale node."""
    return reachable(stale, children_of(upstream)) & set(consumer_ids)


def direct_products(consumers, upstream: dict, consumer_ids) -> set:
    """The non-consumer nodes the given consumers read directly: the data products themselves."""
    ids = set(consumer_ids)
    return {u for c in consumers for u in upstream.get(c, ()) if u not in ids}


# --------------------------------------------------------------------------------------------
# Small formatters
# --------------------------------------------------------------------------------------------

def discord_ts(epoch: float, style: str) -> str:
    """A Discord timestamp: style f = date and time, R = relative ("in 20 hours")."""
    return f"<t:{int(epoch)}:{style}>"


def when(epoch) -> str:
    """'<date time> (<relative>)', or 'never' for a missing time."""
    if not epoch:
        return "never"
    return f"{discord_ts(epoch, 'f')} ({discord_ts(epoch, 'R')})"


def fmt_duration(seconds) -> str:
    if seconds is None or seconds < 0:
        return "?"
    s = int(round(seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{s % 3600 // 60:02d}m"


def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def code_span(s: str, limit: int = CAUSE_MAX) -> str:
    """Text shown verbatim: one line, clipped, inside a code span so that markdown, links and
    mentions in it (error text can quote third-party data) render inert."""
    s = " ".join((s or "").split()).replace("`", "'")
    if len(s) > limit:
        s = s[: limit - 3] + "..."
    return f"`{s}`" if s else "`(no error text)`"


def clip(s: str, limit: int) -> str:
    return s if len(s) <= limit else s[: limit - 3] + "..."


def trigger_label(tags: dict) -> str:
    """How the run was started, from Dagster's own run tags."""
    tags = tags or {}
    if tags.get("dagster/parent_run_id"):
        return "re-execution"
    if tags.get("dagster/schedule_name"):
        return "scheduled"
    if tags.get("dagster/backfill"):
        return "backfill"
    if tags.get("dagster/sensor_name"):
        return "sensor"
    return "manual"


def streak_text(statuses, window: int) -> str:
    """statuses: the job's completed runs, newest first, starting with the failed one. 'SUCCESS'
    and 'FAILURE' count; anything else is skipped. `window` is how many runs were asked for, so
    a history that fills it is open-ended (38+)."""
    seq = [s for s in statuses if s in ("SUCCESS", "FAILURE")]
    if not seq or seq[0] != "FAILURE":   # the failed run is not in the history yet: count it
        seq = ["FAILURE"] + seq
    fails = 0
    while fails < len(seq) and seq[fails] == "FAILURE":
        fails += 1
    if fails > 1:
        return f"{ordinal(fails)} failure in a row"
    oks = 0
    while 1 + oks < len(seq) and seq[1 + oks] == "SUCCESS":
        oks += 1
    if oks == 0:
        return "first run"
    more = "+" if len(statuses) >= window and 1 + oks == len(seq) else ""
    return f"1st failure after {oks}{more} ok"


def next_daily_run(cron: str, now: float):
    """Next fire time of a daily 'M H * * *' cron (UTC), or None for any other shape."""
    parts = (cron or "").split()
    if len(parts) != 5 or parts[2:] != ["*", "*", "*"] or not (parts[0].isdigit() and parts[1].isdigit()):
        return None
    minute, hour = int(parts[0]), int(parts[1])
    if not (0 <= minute < 60 and 0 <= hour < 24):
        return None
    t = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc)
    nxt = t.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if nxt <= t:
        nxt += datetime.timedelta(days=1)
    return nxt.timestamp()


def iso_utc(epoch) -> str:
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def affected_lines(consumers, labels: dict) -> list:
    """'kind: a, b' per kind in KIND_ORDER; pages link to their public URL."""
    by_kind = {}
    for c in consumers:
        kind, label, url = labels[c]
        by_kind.setdefault(kind, []).append(f"[{label}]({url})" if kind == "web" and url else label)
    return [f"{k}: {', '.join(sorted(by_kind[k]))}" for k in KIND_ORDER if k in by_kind]


def _embed(title, url, desc, color, footer, timestamp):
    e = {"title": clip(title, TITLE_MAX), "description": clip(desc, DESC_MAX), "color": color,
         "footer": {"text": clip(footer, 2048)}}
    if url:
        e["url"] = url
    if timestamp:
        e["timestamp"] = iso_utc(timestamp)
    return {"embeds": [e], "allowed_mentions": {"parse": []}}


# --------------------------------------------------------------------------------------------
# The two posts
# --------------------------------------------------------------------------------------------

def failure_payload(*, job, run_id, ended, duration, trigger, streak, not_refreshed, refreshed,
                    upstream, consumer_ids, labels, last_good, step, cause, next_run,
                    env=ENV_LABEL, ui_base=UI_BASE) -> dict:
    """The failure post. not_refreshed / refreshed: node ids planned in the run that did not /
    did materialize. last_good: node id -> epoch of its latest materialization (any run)."""
    affected = affected_consumers(not_refreshed, upstream, consumer_ids)
    products = direct_products(affected, upstream, consumer_ids)
    stale = sorted(products & set(not_refreshed), key=lambda n: (last_good.get(n) or 0, n))
    mixed = sorted(products & set(refreshed))
    n = len(affected)
    if n:
        title = f"{FAIL_MARK} {job} failed: {n} surface{'s' if n != 1 else ''} affected"
    else:
        title = f"{FAIL_MARK} {job} failed: no declared surface affected"
        stale = sorted(not_refreshed, key=lambda n_: (last_good.get(n_) or 0, n_))
    lines = []
    if n:
        lines.append("**Affected** (stale until a run succeeds)")
        lines += affected_lines(affected, labels)
    if stale:
        lines.append("**Not refreshed** (last good)")
        lines += [f"`{p}` {when(last_good.get(p))}" for p in stale]
    if mixed:
        lines.append("**Refreshed in this run:** " + ", ".join(f"`{p}`" for p in mixed)
                     + " - readers now combine it with the older data above")
    why = f"[{step}] {cause}" if step else cause
    lines.append("**Why:** " + code_span(why))
    heal = f"Next scheduled run {when(next_run)}. " if next_run else ""
    via = (f"`Deploy (dev)` with `repo=nostrology-data` and `action={RERUN_ACTIONS[job]}`, or "
           if job in RERUN_ACTIONS else "")
    lines.append("**Heals:** " + heal + "Re-run now (safe, every step re-checks its inputs): " + via
                 + "in the Dagster UI (through a tunnel), open the run, Re-execute > All steps.")
    footer = f"{env} | run {run_id[:8]} | {trigger} | {fmt_duration(duration)} | {streak}"
    return _embed(title, f"{ui_base}/runs/{run_id}", "\n".join(lines), RED, footer, ended)


def recovery_payload(*, job, run_id, ended, duration, trigger, refreshed, upstream, consumer_ids,
                     labels, failed_runs, failing_since, env=ENV_LABEL, ui_base=UI_BASE) -> dict:
    """The recovery post, for the first success after one or more failures."""
    current = affected_consumers(refreshed, upstream, consumer_ids)
    n = len(current)
    title = (f"{OK_MARK} {job} recovered: {n} surface{'s' if n != 1 else ''} current again" if n
             else f"{OK_MARK} {job} recovered")
    lines = []
    if n:
        lines.append("**Current again**")
        lines += affected_lines(current, labels)
    runs = f"{failed_runs} failed run{'s' if failed_runs != 1 else ''} in a row"
    lines.append(f"Failing since {when(failing_since)}: {runs}." if failing_since else f"After {runs}.")
    footer = f"{env} | run {run_id[:8]} | {trigger} | {fmt_duration(duration)}"
    return _embed(title, f"{ui_base}/runs/{run_id}", "\n".join(lines), GREEN, footer, ended)


def failure_text(job, run_id, why) -> str:
    """The plain one-line fallback, posted when the embed cannot be built or is refused."""
    return clip(f"{FAIL_MARK} Dagster run FAILED - job `{job}` run `{run_id}`: {why}", 1900)
