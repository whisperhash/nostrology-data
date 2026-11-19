"""Flush a kind's nozzl WAL to S3 before the extract reads the catalog — the urllib port of
run.sh's _flush_kind (run.sh:417-439).

Per the producer's status mapping (nozzl-http admin_server.rs): 200 = every shard durable in S3
(incl. an idle kind, writers_signaled:0 — tolerated); 502 = a shard's publish FAILED (rows stranded
in WAL) -> abort; 504 = a shard timed out but its publish "lands shortly" -> retry (bounded) until
it confirms 200, else abort. We refuse to proceed on anything but a confirmed 200, because a
silently-incomplete flush is the other half of the registration-ordering miss.

The HTTP POST is done with urllib (no curl subprocess). `post` is injectable so the code-routing
unit-tests without a live producer: it returns the HTTP status code, or 0 for a connection failure
(curl's "000"). Mirrors run.sh's `curl` WITHOUT -f so the 502/504 code is readable.
"""

import time
import urllib.error
import urllib.request
from typing import Callable


def _noop_log(_msg: str) -> None:
    pass


def _post_code(url: str, timeout: float) -> int:
    """POST to url (empty body), returning the HTTP status code (0 on a connection failure)."""
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(getattr(resp, "status", None) or resp.getcode())
    except urllib.error.HTTPError as exc:  # non-2xx: the code is the signal (502/504/404/…)
        return int(exc.code)
    except (urllib.error.URLError, OSError):  # could not reach the admin endpoint — curl's "000"
        return 0


def flush_kind(
    kind,
    flush_url: str,
    *,
    log: Callable[[str], None] = _noop_log,
    post: Callable[[str, float], int] = _post_code,
    timeout: float = 300.0,
    max_attempts: int = 4,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """POST {flush_url}/admin/flush/{kind}, retrying ONLY 504 (bounded exponential backoff). Returns
    on a confirmed 200; raises RuntimeError on 502/404/000/any other code (fail-loud — never extract
    a SHORT slice). `sleep` is injectable so tests don't actually wait.

    The per-request `timeout` (300s) is GENEROUS on purpose: the producer's flush handler is
    synchronous-to-S3 (it awaits the chunk PUT before answering, and returns 504 itself if a shard is
    slow), so the largest-kind flush — k=9735, ~2.3 GB — must comfortably fit. The bash `curl -sS`
    had no --max-time (it could hang forever); a long-but-finite ceiling turns a genuinely hung
    socket into a fail-loud 000 without false-failing a legitimately slow flush."""
    url = f"{flush_url}/admin/flush/{kind}"
    attempt = 0
    sleep_s = 2.0
    while True:
        code = post(url, timeout)
        if code == 200:
            return
        if code == 504:
            if attempt < max_attempts:
                attempt += 1
                log(
                    f"[extract] flush k={kind}: HTTP 504 (a shard timed out; its publish lands "
                    f"shortly) — retry {attempt}/{max_attempts} after {sleep_s:g}s"
                )
                sleep(sleep_s)
                sleep_s *= 2
                continue
            raise RuntimeError(
                f"flush k={kind} still HTTP 504 after {max_attempts} retries — a shard never "
                "confirmed its S3 publish; refusing to extract a SHORT slice. Re-run shortly (the "
                "chunk lands), or check the producer."
            )
        if code == 502:
            raise RuntimeError(
                f"flush k={kind} returned HTTP 502 — a shard's publish FAILED; its rows are stranded "
                "in the WAL, NOT in S3. Refusing to extract SHORT. Investigate the producer, then re-run."
            )
        if code == 404:
            raise RuntimeError(
                f"flush k={kind} returned HTTP 404 — kind {kind} is not configured for acquisition on "
                "the producer. Refusing to extract."
            )
        if code == 0:
            raise RuntimeError(
                f"flush k={kind} — could not reach the producer admin endpoint at {flush_url}. "
                "Refusing to extract without a fresh, confirmed flush."
            )
        raise RuntimeError(
            f"flush k={kind} returned HTTP {code} (expected 200). Refusing to extract."
        )
