"""Unit tests for pipeline/flush.flush_kind — the HTTP-code routing (200 ok / 504 retry / everything
else fatal), with the POST and sleep injected so nothing hits the network or actually waits."""

import pytest

from dagster_code.pipeline import flush


class _Post:
    """A fake POST: returns the next code from a scripted list; records the URLs hit."""

    def __init__(self, codes):
        self.codes = list(codes)
        self.urls = []

    def __call__(self, url, timeout):
        self.urls.append(url)
        return self.codes.pop(0)


def _no_sleep(_s):
    pass


def test_flush_200_returns_and_builds_the_admin_url():
    post = _Post([200])
    flush.flush_kind(10002, "http://nostr-nozzl:9091", post=post, sleep=_no_sleep)
    assert post.urls == ["http://nostr-nozzl:9091/admin/flush/10002"]


def test_flush_504_then_200_retries_then_succeeds():
    slept = []
    post = _Post([504, 504, 200])
    flush.flush_kind(9735, "http://h", post=post, sleep=slept.append)
    assert len(post.urls) == 3
    assert slept == [2.0, 4.0]  # exponential backoff


def test_flush_504_exhausted_raises():
    post = _Post([504, 504, 504, 504, 504])  # 1 initial + 4 retries
    with pytest.raises(RuntimeError, match="still HTTP 504 after 4 retries"):
        flush.flush_kind(0, "http://h", post=post, sleep=_no_sleep, max_attempts=4)


def test_flush_502_is_fatal_no_retry():
    post = _Post([502, 200])
    with pytest.raises(RuntimeError, match="HTTP 502"):
        flush.flush_kind(0, "http://h", post=post, sleep=_no_sleep)
    assert len(post.urls) == 1  # did NOT retry


def test_flush_404_is_fatal():
    with pytest.raises(RuntimeError, match="HTTP 404"):
        flush.flush_kind(99999, "http://h", post=_Post([404]), sleep=_no_sleep)


def test_flush_000_connection_failure_is_fatal():
    with pytest.raises(RuntimeError, match="could not reach the producer admin endpoint"):
        flush.flush_kind(0, "http://h", post=_Post([0]), sleep=_no_sleep)


def test_flush_unexpected_code_is_fatal():
    with pytest.raises(RuntimeError, match="HTTP 500 \\(expected 200\\)"):
        flush.flush_kind(0, "http://h", post=_Post([500]), sleep=_no_sleep)
