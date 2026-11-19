"""Unit tests for pipeline/athena.run_unload — the start + poll loop, with a fake Athena client and
an injected clock/sleep so the poll, the terminal states, and the timeout-cancel all test instantly."""

import pytest

from dagster_code.pipeline import athena


class _FakeAthena:
    """Scripts a sequence of get_query_execution states; records start/stop calls."""

    def __init__(self, states, *, qid="Q-1", scanned=12345):
        self.states = list(states)  # each: a State string
        self.qid = qid
        self.scanned = scanned
        self.started = None
        self.stopped = None
        self.reason = "Column 'x' cannot be resolved"

    def start_query_execution(self, *, WorkGroup, QueryString):
        self.started = (WorkGroup, QueryString)
        return {"QueryExecutionId": self.qid}

    def get_query_execution(self, *, QueryExecutionId):
        state = self.states.pop(0)
        return {
            "QueryExecution": {
                "Status": {"State": state, "StateChangeReason": self.reason},
                "Statistics": {"DataScannedInBytes": self.scanned},
            }
        }

    def stop_query_execution(self, *, QueryExecutionId):
        self.stopped = QueryExecutionId


class _Clock:
    """Monotonic-ish clock that advances `step` seconds each call."""

    def __init__(self, step=0.0):
        self.t = 0.0
        self.step = step

    def __call__(self):
        v = self.t
        self.t += self.step
        return v


def _no_sleep(_s):
    pass


def test_run_unload_succeeds_returns_scanned_bytes():
    ath = _FakeAthena(["SUCCEEDED"], scanned=99887766)
    scanned = athena.run_unload(
        ath, workgroup="example-wg", query="UNLOAD ...", sleep=_no_sleep, now=_Clock(),
    )
    assert scanned == 99887766
    assert ath.started == ("example-wg", "UNLOAD ...")


def test_run_unload_polls_through_running_then_succeeds():
    slept = []
    ath = _FakeAthena(["QUEUED", "RUNNING", "SUCCEEDED"])
    athena.run_unload(ath, workgroup="w", query="q", sleep=slept.append, now=_Clock())
    assert len(slept) == 2  # slept after QUEUED and RUNNING, returned on SUCCEEDED


def test_run_unload_failed_raises_with_reason():
    ath = _FakeAthena(["FAILED"])
    with pytest.raises(RuntimeError, match="Athena query FAILED:.*cannot be resolved"):
        athena.run_unload(ath, workgroup="w", query="q", sleep=_no_sleep, now=_Clock())


def test_run_unload_cancelled_raises():
    ath = _FakeAthena(["CANCELLED"])
    with pytest.raises(RuntimeError, match="Athena query CANCELLED"):
        athena.run_unload(ath, workgroup="w", query="q", sleep=_no_sleep, now=_Clock())


def test_run_unload_timeout_cancels_and_raises():
    # State never reaches terminal; the clock jumps 1000s/poll so it blows the 1800s ceiling fast.
    ath = _FakeAthena(["RUNNING"] * 10)
    with pytest.raises(RuntimeError, match="exceeded 1800s"):
        athena.run_unload(
            ath, workgroup="w", query="q", timeout_s=1800, sleep=_no_sleep, now=_Clock(step=1000),
        )
    assert ath.stopped == "Q-1"  # best-effort cancel fired


def test_run_unload_rides_out_transient_describe_failure():
    # A transient get_query_execution failure must NOT kill a query still running fine — re-poll
    # (run.sh:694-696 parity). First describe raises, then RUNNING, then SUCCEEDED.
    class _Flaky(_FakeAthena):
        def __init__(self):
            super().__init__(["RUNNING", "SUCCEEDED"], scanned=42)
            self.raised = False

        def get_query_execution(self, *, QueryExecutionId):
            if not self.raised:
                self.raised = True
                raise RuntimeError("ThrottlingException")
            return super().get_query_execution(QueryExecutionId=QueryExecutionId)

    ath = _Flaky()
    scanned = athena.run_unload(ath, workgroup="w", query="q", sleep=_no_sleep, now=_Clock())
    assert scanned == 42 and ath.raised  # survived the transient failure


def test_run_unload_no_query_id_raises():
    class _NoQid(_FakeAthena):
        def start_query_execution(self, *, WorkGroup, QueryString):
            return {}

    with pytest.raises(RuntimeError, match="no QueryExecutionId"):
        athena.run_unload(_NoQid([]), workgroup="w", query="q", sleep=_no_sleep, now=_Clock())
