"""Run an Athena UNLOAD to completion — the boto3 port of cmd_refresh_extract_only's
start-query-execution + poll loop (run.sh:683-713).

start_query_execution(WorkGroup, QueryString) -> poll get_query_execution every poll_s until a
terminal state, with a wall-clock ceiling (REFRESH_TIMEOUT, 1800s) past which the query is cancelled
and we fail loud. Returns DataScannedInBytes on SUCCEEDED; raises on FAILED/CANCELLED/timeout. The
athena client is injected; `sleep`/`now` are injectable so the poll loop unit-tests without waiting.
"""

import time
from typing import Callable

_TERMINAL_OK = "SUCCEEDED"
_TERMINAL_BAD = ("FAILED", "CANCELLED")


def _noop_log(_msg: str) -> None:
    pass


def run_unload(
    athena,
    *,
    workgroup: str,
    query: str,
    log: Callable[[str], None] = _noop_log,
    poll_s: float = 8.0,
    timeout_s: float = 1800.0,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> int:
    """Execute the UNLOAD and block until it finishes. Returns scanned bytes (SUCCEEDED) or raises
    RuntimeError (FAILED/CANCELLED with the StateChangeReason; or timeout, after stop_query_execution)."""
    qid = athena.start_query_execution(WorkGroup=workgroup, QueryString=query).get("QueryExecutionId")
    if not qid:
        raise RuntimeError("Athena start-query-execution returned no QueryExecutionId")
    log(f"[extract] query {qid} (wg={workgroup})")

    t0 = now()
    while True:
        # A TRANSIENT get-query-execution failure (throttle / 5xx) must NOT nuke a query that is
        # still running fine — ride it out as UNKNOWN and re-poll, still bounded by timeout_s
        # (run.sh:694-696 does the same via `2>/dev/null || echo "UNKNOWN 0"`).
        try:
            qe = athena.get_query_execution(QueryExecutionId=qid).get("QueryExecution", {})
            status = qe.get("Status", {})
            state = status.get("State", "UNKNOWN")
            scanned = int(qe.get("Statistics", {}).get("DataScannedInBytes", 0) or 0)
        except Exception as exc:  # noqa: BLE001 — transient describe failure; keep polling
            status, state, scanned = {}, "UNKNOWN", 0
            log(f"[extract] get-query-execution transient failure (continuing): {exc}")
        elapsed = int(now() - t0)
        log(f"[extract] {state}  scanned={scanned}B  elapsed={elapsed}s")
        if state == _TERMINAL_OK:
            return scanned
        if state in _TERMINAL_BAD:
            reason = status.get("StateChangeReason", "(reason unavailable)")
            raise RuntimeError(f"Athena query {state}: {reason} (QID={qid})")
        if now() - t0 >= timeout_s:
            try:
                athena.stop_query_execution(QueryExecutionId=qid)
            except Exception:  # noqa: BLE001 — best-effort cancel; the timeout is the real failure
                pass
            raise RuntimeError(
                f"Athena query exceeded {timeout_s:g}s (last state: {state}) — cancelled {qid}"
            )
        sleep(poll_s)
