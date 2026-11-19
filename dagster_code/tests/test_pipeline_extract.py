"""Unit tests for pipeline/extract.run_extract — the flush -> register -> render -> UNLOAD -> count
orchestration, with every sub-step monkeypatched (the pieces have their own unit tests). Asserts the
call ORDER, the returned stats shape, and that a flush failure aborts
before any Athena spend."""

import pytest

from dagster_code.pipeline import extract
from dagster_code.pipeline.config import PipelineConfig
from dagster_code.tests.fake_env import FAKE_ENV


class _Log:
    def info(self, m):
        pass


class _Ctx:
    log = _Log()


def _patch(monkeypatch, calls, *, flush_raises=False):
    monkeypatch.setattr(extract.aws, "client", lambda *a, **k: object())

    def _flush(kind, url, *, log):
        calls.append("flush")
        if flush_raises:
            raise RuntimeError("flush k=10002 returned HTTP 502")

    def _register(s3, glue, **kw):
        # Registration and the UNLOAD read the same Glue table, from the config.
        assert (kw["glue_db"], kw["glue_table"]) == (FAKE_ENV["NOSTROLOGY_GLUE_DB"], "events")
        calls.append("register")
        return {"new": 0, "registered": 0}

    def _render(sql_dir, *, kind, bucket, run_id, glue_db, glue_table, into=""):
        # The UNLOAD reads the deployment's own Glue table, from the config, never a literal.
        assert (glue_db, glue_table) == (FAKE_ENV["NOSTROLOGY_GLUE_DB"], "events")
        calls.append("render")
        return "UNLOAD ..."

    def _unload(athena, *, workgroup, query, log, timeout_s):
        calls.append("unload")
        assert query == "UNLOAD ..."  # the rendered SQL is what runs
        return 555000

    def _count(s3, bucket, prefix):
        calls.append("count")
        return 7

    monkeypatch.setattr(extract.flush, "flush_kind", _flush)
    monkeypatch.setattr(extract.glue_catalog, "register_partitions", _register)
    monkeypatch.setattr(extract.sql, "render", _render)
    monkeypatch.setattr(extract.athena, "run_unload", _unload)
    monkeypatch.setattr(extract.s3util, "count_gz_parts", _count)


def test_run_extract_happy_path_order_and_stats(monkeypatch, tmp_path):
    calls = []
    _patch(monkeypatch, calls)
    stats = extract.run_extract(
        _Ctx(), PipelineConfig.from_env(FAKE_ENV),
        kind=10002, run_id="dag-k10002", repo_root=str(tmp_path),
    )
    assert calls == ["flush", "register", "render", "unload", "count"]
    assert stats == {"run_id": "dag-k10002", "scanned_bytes": 555000, "parts": 7}


def test_run_extract_flush_failure_aborts_before_athena(monkeypatch, tmp_path):
    calls = []
    _patch(monkeypatch, calls, flush_raises=True)
    with pytest.raises(RuntimeError, match="HTTP 502"):
        extract.run_extract(
            _Ctx(), PipelineConfig.from_env(FAKE_ENV),
            kind=10002, run_id="dag", repo_root=str(tmp_path),
        )
    # register/render/unload/count never ran — no Athena spend on a SHORT slice.
    assert calls == ["flush"]
