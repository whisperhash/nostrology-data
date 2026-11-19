"""Run a subprocess, streaming merged stdout/stderr into the Dagster step log, in its OWN process
group so a step cancel / daemon restart / timeout kills the whole subtree (not just the parent).

The loader (and the signer loader) is invoked here as a plain Python subprocess (no bash, no
aws-cli, no docker). Returns the exit code; the caller decides what a non-zero means.
"""

import os
import signal
import subprocess
import threading


def run_streamed(context, argv: list[str], *, env: dict, cwd: str, timeout: int | None = None) -> int:
    """Launch argv (own process group), pump merged output to context.log line-by-line, enforce a
    wall-clock timeout, and tear the group down on timeout/cancellation so nothing is orphaned.

    NB the reader runs in a THREAD: a read-loop here would block until EOF (process exit), making
    proc.wait(timeout) a no-op and a silent hang block forever. The thread lets the main thread
    enforce the deadline."""
    context.log.info("$ " + " ".join(argv))
    child = subprocess.Popen(
        argv, cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
        start_new_session=True,
    )

    def _pump() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            context.log.info(line.rstrip("\n"))

    reader = threading.Thread(target=_pump, name="subprocess-log-pump", daemon=True)
    reader.start()
    try:
        child.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _terminate_group(child)
        raise RuntimeError(f"subprocess {argv[0]} exceeded the {timeout}s ceiling — killed")
    except BaseException:
        _terminate_group(child)
        raise
    finally:
        reader.join(timeout=5)
    return child.returncode


def _terminate_group(child: subprocess.Popen) -> None:
    """SIGTERM the process group, give it a moment, then SIGKILL."""
    if child.poll() is not None:
        return
    try:
        pgid = os.getpgid(child.pid)
    except ProcessLookupError:
        return
    try:
        os.killpg(pgid, signal.SIGTERM)
        try:
            child.wait(timeout=30)
            return
        except subprocess.TimeoutExpired:
            os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
