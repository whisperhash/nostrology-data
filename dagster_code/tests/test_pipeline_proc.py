"""Unit tests for pipeline/proc.run_streamed — exit-code propagation, log capture, and the
wall-clock timeout (real short python subprocesses; runs in the local gate)."""

import os
import sys

import pytest

from dagster_code.pipeline import proc


class _Log:
    def __init__(self):
        self.lines: list = []

    def info(self, m):
        self.lines.append(m)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def test_run_streamed_success_streams_output():
    ctx = _Ctx()
    rc = proc.run_streamed(
        ctx, [sys.executable, "-c", "print('hello-proc')"],
        env=dict(os.environ), cwd=".", timeout=30,
    )
    assert rc == 0
    assert any("hello-proc" in line for line in ctx.log.lines)


def test_run_streamed_propagates_nonzero_exit():
    ctx = _Ctx()
    rc = proc.run_streamed(
        ctx, [sys.executable, "-c", "import sys; sys.exit(3)"],
        env=dict(os.environ), cwd=".", timeout=30,
    )
    assert rc == 3


def test_run_streamed_timeout_kills_and_raises():
    ctx = _Ctx()
    with pytest.raises(RuntimeError, match="exceeded the 1s ceiling"):
        proc.run_streamed(
            ctx, [sys.executable, "-c", "import time; time.sleep(10)"],
            env=dict(os.environ), cwd=".", timeout=1,
        )
