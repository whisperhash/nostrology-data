"""pipeline/freshness.py: the verdict over one per-job last-run file (pure, no dagster)."""

import json
import time

from dagster_code.pipeline import freshness

H = 3600
NOW = 1_790_000_000.0


def _write(tmp_path, name, **rec):
    p = tmp_path / f"last-run-{name}.json"
    p.write_text(json.dumps(rec) + "\n")
    return str(p)


def _iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


def test_fresh_ok_run_is_ok(tmp_path):
    p = _write(tmp_path, "ngit_refresh", job="ngit_refresh", run_id="abcdef12-x", overall="ok",
               started=_iso(NOW - 3 * H), finished=_iso(NOW - 2 * H))
    status, detail = freshness.freshness_status(p, NOW)
    assert status == "ok"
    assert "abcdef12" in detail and "2.0 h ago" in detail


def test_stale_after_26h():
    assert freshness.STALE_AFTER_SECONDS == 26 * H


def test_stale_when_older_than_26h(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="abcdef12-x", overall="ok",
               started=_iso(NOW - 27 * H), finished=_iso(NOW - 26 * H - 1))
    status, detail = freshness.freshness_status(p, NOW)
    assert status == "stale"
    assert "26.0 h ago" in detail


def test_exactly_26h_is_still_ok(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="r", overall="ok", started=None, finished=_iso(NOW - 26 * H))
    assert freshness.freshness_status(p, NOW)[0] == "ok"


def test_failed_run_is_fail_whatever_its_age(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="deadbeef-x", overall="fail",
               started=_iso(NOW - 600), finished=_iso(NOW - 60))
    status, detail = freshness.freshness_status(p, NOW)
    assert status == "fail"
    assert "overall=fail" in detail and "deadbeef" in detail


def test_missing_file_is_missing(tmp_path):
    status, detail = freshness.freshness_status(str(tmp_path / "nope.json"), NOW)
    assert status == "missing"
    assert "FileNotFoundError" in detail


def test_malformed_json_is_missing(tmp_path):
    p = tmp_path / "last-run-j.json"
    p.write_text("{not json")
    assert freshness.freshness_status(str(p), NOW)[0] == "missing"


def test_non_object_json_is_missing(tmp_path):
    p = tmp_path / "last-run-j.json"
    p.write_text("[1, 2]")
    assert freshness.freshness_status(str(p), NOW)[0] == "missing"


def test_unparseable_finished_is_stale(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="r", overall="ok", started=None, finished="not a date")
    status, detail = freshness.freshness_status(p, NOW)
    assert status == "stale" and "no parseable finished" in detail


def test_missing_finished_is_stale(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="r", overall="ok", started=None, finished=None)
    assert freshness.freshness_status(p, NOW)[0] == "stale"


def test_stale_threshold_is_configurable(tmp_path):
    p = _write(tmp_path, "j", job="j", run_id="r", overall="ok", started=None, finished=_iso(NOW - 2 * H))
    assert freshness.freshness_status(p, NOW, stale_after=H)[0] == "stale"
    assert freshness.freshness_status(p, NOW, stale_after=3 * H)[0] == "ok"


def test_parse_iso_round_trip():
    assert freshness.parse_iso(_iso(NOW)) == NOW
    assert freshness.parse_iso(None) is None
    assert freshness.parse_iso("2026-09-25 15:26:00") is None  # not the contract's format
