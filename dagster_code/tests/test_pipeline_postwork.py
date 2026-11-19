"""Unit tests for pipeline/postwork — the nativized /nutzaps MAU rebuild and the /lightning signer
load, with AWS/PG/loader all mocked. Asserts the MAU runs the rebuild SQL and counts; the signer
load applies schema then invokes the loader with a password-FREE --db-url + the secret in PGPASSWORD,
and fails loud on an unreadable YAML or a non-zero loader exit."""

import os
import types

import pytest

from dagster_code.pipeline import postwork
from dagster_code.pipeline.config import PipelineConfig
from dagster_code.tests.fake_env import FAKE_ENV


class _Log:
    def __init__(self):
        self.lines = []

    def info(self, m):
        self.lines.append(m)

    def warning(self, m):
        self.lines.append(m)


class _Ctx:
    def __init__(self):
        self.log = _Log()


def _patch_conn(monkeypatch, *, sql_calls, counts):
    monkeypatch.setattr(postwork.aws, "client", lambda *a, **k: object())
    monkeypatch.setattr(postwork.aurora, "resolve_endpoint", lambda rds, c: "host.rds.amazonaws.com")
    monkeypatch.setattr(postwork.aurora, "load_master_password", lambda rds, sm, c: "s3cr3t")
    monkeypatch.setattr(postwork.pg, "connect", lambda dsn, pw: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(postwork.pg, "execute_sql_file", lambda conn, path: sql_calls.append(path))
    monkeypatch.setattr(postwork.pg, "count_rows", lambda conn, table: counts.get(table, 0))


def test_rebuild_nutzap_mau_runs_sql_and_counts(monkeypatch, tmp_path):
    sql_calls = []
    _patch_conn(monkeypatch, sql_calls=sql_calls,
                counts={"active_user_counts_kind_9321_nutzaps": 505})
    stats = postwork.rebuild_nutzap_mau(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))
    assert stats == {"rows": 505}
    assert sql_calls == [os.path.join(str(tmp_path), "schema", "nutzap_active_users.sql")]


def test_load_signers_applies_schema_and_invokes_loader(monkeypatch, tmp_path):
    # The YAML readability check is real (not mocked) — create it.
    yaml_dir = tmp_path / "data" / "lightning"
    yaml_dir.mkdir(parents=True)
    (yaml_dir / "provider_signers.yaml").write_text("providers: []\n")

    sql_calls = []
    _patch_conn(monkeypatch, sql_calls=sql_calls, counts={"lightning_provider_signers": 31})
    capture = {}

    def _fake_run(context, argv, *, env, cwd, timeout):
        capture["argv"] = argv
        capture["env"] = env
        return 0

    monkeypatch.setattr(postwork.proc, "run_streamed", _fake_run)
    stats = postwork.load_signers(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))

    assert stats == {"rows": 31}
    assert sql_calls == [os.path.join(str(tmp_path), "schema", "init.sql")]  # schema applied first
    argv = capture["argv"]
    assert "--load-lightning-signers" in argv
    assert argv[argv.index("--load-lightning-signers") + 1].endswith("provider_signers.yaml")
    dsn = argv[argv.index("--db-url") + 1]
    assert "s3cr3t" not in dsn and dsn.endswith("connect_timeout=90")
    assert capture["env"]["PGPASSWORD"] == "s3cr3t"     # secret in env …
    assert "s3cr3t" not in " ".join(argv)               # … never in argv


def test_rebuild_nutzap_mau_count_is_best_effort(monkeypatch, tmp_path):
    # A count failure AFTER the rebuild committed must NOT fail the asset (bash `|| true; rows=0`).
    sql_calls = []
    _patch_conn(monkeypatch, sql_calls=sql_calls, counts={})

    def _boom(conn, table):
        raise RuntimeError("connection reset after commit")

    monkeypatch.setattr(postwork.pg, "count_rows", _boom)
    stats = postwork.rebuild_nutzap_mau(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))
    assert stats == {"rows": 0}  # reported 0, did not raise
    assert sql_calls  # the rebuild SQL still ran


def test_load_signers_missing_yaml_fails_loud(monkeypatch, tmp_path):
    sql_calls = []
    _patch_conn(monkeypatch, sql_calls=sql_calls, counts={})
    monkeypatch.setattr(postwork.proc, "run_streamed", lambda *a, **k: 0)
    with pytest.raises(RuntimeError, match="signer manifest not readable"):
        postwork.load_signers(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))


def test_load_signers_nonzero_loader_exit_raises(monkeypatch, tmp_path):
    yaml_dir = tmp_path / "data" / "lightning"
    yaml_dir.mkdir(parents=True)
    (yaml_dir / "provider_signers.yaml").write_text("providers: []\n")
    _patch_conn(monkeypatch, sql_calls=[], counts={})
    monkeypatch.setattr(postwork.proc, "run_streamed", lambda *a, **k: 3)
    with pytest.raises(RuntimeError, match="signer loader exited 3"):
        postwork.load_signers(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))


# -- nutzap mint classification (nostrology-data#9) --

def _registry(tmp_path):
    d = tmp_path / "data" / "nutzaps"
    d.mkdir(parents=True)
    (d / "mint_registry.yaml").write_text("mints: []\n")


def test_classify_nutzap_mints_invokes_loader_without_password_in_argv(monkeypatch, tmp_path):
    _registry(tmp_path)
    _patch_conn(monkeypatch, sql_calls=[], counts={"nutzap_mint_classes": 131})
    capture = {}

    def _fake_run(context, argv, *, env, cwd, timeout):
        capture.update(argv=argv, env=env, cwd=cwd)
        return 0

    monkeypatch.setattr(postwork.proc, "run_streamed", _fake_run)
    stats = postwork.classify_nutzap_mints(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))

    assert stats == {"rows": 131}
    argv = capture["argv"]
    assert argv[argv.index("--classify-nutzap-mints") + 1] == os.path.join(
        str(tmp_path), "data", "nutzaps", "mint_registry.yaml")
    assert "s3cr3t" not in " ".join(argv)
    assert capture["env"]["PGPASSWORD"] == "s3cr3t"
    assert capture["cwd"] == str(tmp_path)


def test_classify_nutzap_mints_missing_registry_fails_loud(monkeypatch, tmp_path):
    _patch_conn(monkeypatch, sql_calls=[], counts={})
    monkeypatch.setattr(postwork.proc, "run_streamed", lambda *a, **k: 0)
    with pytest.raises(RuntimeError, match="mint registry not readable"):
        postwork.classify_nutzap_mints(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))


def test_classify_nutzap_mints_nonzero_exit_raises(monkeypatch, tmp_path):
    # A malformed registry makes the loader exit non-zero BEFORE writing; the asset must fail,
    # so the MAU rebuild downstream does not run on a stale or partial classification.
    _registry(tmp_path)
    _patch_conn(monkeypatch, sql_calls=[], counts={})
    monkeypatch.setattr(postwork.proc, "run_streamed", lambda *a, **k: 1)
    with pytest.raises(RuntimeError, match="mint classification exited 1"):
        postwork.classify_nutzap_mints(_Ctx(), PipelineConfig.from_env(FAKE_ENV), repo_root=str(tmp_path))


def test_the_shipped_mint_registry_is_where_the_asset_reads_it():
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    assert os.path.isfile(os.path.join(repo, *postwork._MINT_REGISTRY_YAML))
