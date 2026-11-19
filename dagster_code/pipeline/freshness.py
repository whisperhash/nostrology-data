"""Freshness verdicts over the sensors' per-job last-run files (pure: path + clock in, verdict
out, so it unit-tests without dagster). Used by sensors.freshness_sensor; the file format is
the one sensors._write_last_run writes: {"job", "run_id", "overall": "ok"|"fail", "started",
"finished"} with ISO-8601 UTC timestamps.
"""

import datetime
import json

STALE_AFTER_SECONDS = 26 * 3600  # daily schedules plus two hours of slack for the queue


def parse_iso(s: str | None) -> float | None:
    """'%Y-%m-%dT%H:%M:%SZ' -> epoch seconds; None for anything else."""
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return None


def freshness_status(path: str, now: float, stale_after: float = STALE_AFTER_SECONDS) -> tuple[str, str]:
    """Classify one per-job last-run file: ("ok" | "missing" | "fail" | "stale", detail).

    `missing` = no readable file (the job never completed on this instance, or the volume is
    gone); `fail` = the last completed run failed, whatever its age; `stale` = the last
    completed run is `ok` but older than `stale_after`, or its `finished` is unparseable.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            rec = json.load(fh)
    except (OSError, ValueError) as exc:
        return "missing", f"no readable last-run file ({type(exc).__name__})"
    if not isinstance(rec, dict):
        return "missing", "last-run file is not a JSON object"
    run_id = str(rec.get("run_id", "?"))[:8]
    if rec.get("overall") != "ok":
        return "fail", f"last completed run {run_id} finished {rec.get('finished')} with overall={rec.get('overall')}"
    finished = parse_iso(rec.get("finished"))
    if finished is None:
        return "stale", f"last-run file has no parseable finished time (run {run_id})"
    age = now - finished
    if age > stale_after:
        return "stale", f"last ok run {run_id} finished {rec.get('finished')}, {age / 3600:.1f} h ago"
    return "ok", f"run {run_id} finished {age / 3600:.1f} h ago"
