"""Make `nostrology_mcp` importable when pytest runs from anywhere in the repo, and keep
the suite from writing fabricated call records into a real log directory."""

import os
import sys

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


@pytest.fixture(autouse=True)
def _isolate_call_log(tmp_path, monkeypatch):
    """Point the call log at a per-test temp dir for EVERY test, always.

    WHY AUTOUSE AND NOT PER-FILE. Any test that drives the real ASGI app writes real call
    records, and `calllog` defaults to `nostrology_mcp/logs/` - the same directory a local
    stdio run writes to. `test_asgilog.py` and `test_recovery.py` each remembered to
    redirect it; `test_inflight.py` did not, and one run put 474 fabricated records into
    the local corpus, 24 of them stamped `clientInfo.name = "DummyClient"` - which is
    exactly the ContextVM announcement manager's identity. Anyone later reading that
    directory as traffic would have counted our own test fixtures as a third party.

    Remembering is not a control: this is the second time the trap has been noted and the
    first time it actually bit. An autouse default makes it structurally impossible, and
    costs nothing - a test that wants to assert on the records still calls
    `monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))` itself, which runs after
    this fixture and wins.

    Stdout is silenced for the same reason one layer over: in the container that sink is
    what feeds Loki, and a test suite has no business emitting records shaped like
    production ones.
    """
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path / "calllog"))
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_STDOUT", "0")
