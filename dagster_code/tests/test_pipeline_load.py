"""Unit tests for pipeline/load.run_load — the orchestration, with AWS/S3/loader/PG all mocked.

Asserts the loader is invoked with the right argv (--truncate --min-rows <floor> --no-refresh-mv,
--into for the zapstore subset, a password-FREE --db-url), the secret rides in PGPASSWORD not argv,
an empty prefix and a non-zero loader exit both fail loud, and the fetch scratch is cleaned up.
"""

import os
import types

import pytest

from dagster_code.pipeline import load
from dagster_code.pipeline.config import PipelineConfig
from dagster_code.tests.fake_env import FAKE_ENV


class _Log:
    def __init__(self):
        self.lines: list = []

    def info(self, m):
        self.lines.append(("info", m))

    def warning(self, m):
        self.lines.append(("warning", m))


class _Ctx:
    def __init__(self):
        self.log = _Log()


def _patch_common(monkeypatch, *, parts_factory, run_rc=0, capture=None):
    """Stub out AWS/endpoint/secret/s3/disk/proc/pg so run_load runs without AWS or a DB."""
    monkeypatch.setattr(load.aws, "client", lambda *a, **k: object())
    monkeypatch.setattr(load.aurora, "resolve_endpoint", lambda rds, c: "host.rds.amazonaws.com")
    monkeypatch.setattr(load.aurora, "load_master_password", lambda rds, sm, c: "s3cr3t")
    monkeypatch.setattr(load.s3util, "prefix_total_bytes", lambda s3, b, p: 0)
    monkeypatch.setattr(load.shutil, "disk_usage", lambda p: types.SimpleNamespace(total=0, used=0, free=10**12))
    monkeypatch.setattr(load.s3util, "download_prefix", parts_factory)

    def _fake_run(context, argv, *, env, cwd, timeout):
        if capture is not None:
            capture["argv"] = argv
            capture["env"] = env
            capture["cwd"] = cwd
        return run_rc

    monkeypatch.setattr(load.proc, "run_streamed", _fake_run)
    monkeypatch.setattr(load.pg, "connect", lambda dsn, pw: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(load.pg, "apply_schema", lambda conn, path: None)
    monkeypatch.setattr(load.pg, "count_rows", lambda conn, table: 42)


def _cfg(tmp_path):
    return PipelineConfig.from_env({
        **FAKE_ENV,
        "NOSTROLOGY_EXTRACT_TMP": str(tmp_path),
        "NOSTROLOGY_EXTRACTS_BUCKET": "example_extracts",
    })


def test_run_load_zapstore_subset_argv_and_secret(monkeypatch, tmp_path):
    capture: dict = {}

    def parts(s3, bucket, prefix, dest):
        p = os.path.join(dest, "part-0.gz")
        open(p, "w").close()
        return [p]

    _patch_common(monkeypatch, parts_factory=parts, capture=capture)
    stats = load.run_load(
        _Ctx(), _cfg(tmp_path),
        kind="9735", run_id="dag-k9735-zapstore", into="zapstore", repo_root=str(tmp_path), timeout=60,
    )
    argv = capture["argv"]
    assert "--no-refresh-mv" in argv and "--truncate" in argv
    assert argv[argv.index("--kind") + 1] == "9735"
    assert argv[argv.index("--min-rows") + 1] == "1"           # zapstore forced to 1
    assert argv[argv.index("--into") + 1] == "zapstore"
    dsn = argv[argv.index("--db-url") + 1]
    assert "s3cr3t" not in dsn and dsn.endswith("sslmode=require&connect_timeout=90")
    # secret rides in env, never argv
    assert capture["env"]["PGPASSWORD"] == "s3cr3t"
    assert "s3cr3t" not in " ".join(argv)
    assert stats == {"rows": 42, "min_rows": 1, "copy_s": stats["copy_s"]}
    assert isinstance(stats["copy_s"], int)


def test_run_load_relays_uses_kind_floor_and_no_into(monkeypatch, tmp_path):
    capture: dict = {}

    def parts(s3, bucket, prefix, dest):
        p = os.path.join(dest, "part-0.gz")
        open(p, "w").close()
        return [p]

    _patch_common(monkeypatch, parts_factory=parts, capture=capture)
    load.run_load(_Ctx(), _cfg(tmp_path), kind="10002", run_id="dag-k10002", repo_root=str(tmp_path), timeout=60)
    argv = capture["argv"]
    assert argv[argv.index("--min-rows") + 1] == "757084"
    assert "--into" not in argv


def test_run_load_empty_prefix_fails_loud(monkeypatch, tmp_path):
    _patch_common(monkeypatch, parts_factory=lambda s3, b, p, dest: [])
    with pytest.raises(RuntimeError, match="no .gz parts"):
        load.run_load(_Ctx(), _cfg(tmp_path), kind="10002", run_id="stale", repo_root=str(tmp_path), timeout=60)


def test_run_load_nonzero_loader_exit_raises(monkeypatch, tmp_path):
    def parts(s3, bucket, prefix, dest):
        p = os.path.join(dest, "part-0.gz")
        open(p, "w").close()
        return [p]

    _patch_common(monkeypatch, parts_factory=parts, run_rc=2)
    with pytest.raises(RuntimeError, match="loader exited 2"):
        load.run_load(_Ctx(), _cfg(tmp_path), kind="10002", run_id="dag", repo_root=str(tmp_path), timeout=60)


def test_run_load_cleans_up_fetch_dir(monkeypatch, tmp_path):
    def parts(s3, bucket, prefix, dest):
        p = os.path.join(dest, "part-0.gz")
        open(p, "w").close()
        return [p]

    _patch_common(monkeypatch, parts_factory=parts)
    load.run_load(_Ctx(), _cfg(tmp_path), kind="10002", run_id="dag", repo_root=str(tmp_path), timeout=60)
    # the mktemp'd nostrology-extract.* scratch dir is removed in the finally
    leftover = [d for d in os.listdir(tmp_path) if d.startswith("nostrology-extract.")]
    assert leftover == []


def _one_part(s3, bucket, prefix, dest):
    p = os.path.join(dest, "part-0.gz")
    open(p, "w").close()
    return [p]


def _unfiltered_warnings(ctx):
    return [m for level, m in ctx.log.lines if level == "warning" and "UNFILTERED" in m]


@pytest.mark.parametrize("kind", ["0", "9321", "10002", "10019", "30267"])
def test_run_load_warns_when_a_filtering_kind_has_no_lists(monkeypatch, tmp_path, kind):
    """A job's kinds whose loader filters on the blocklist. With no lists they load UNFILTERED, exit 0
    and a green run, so a missing mount gets this warning and nothing else."""
    _patch_common(monkeypatch, parts_factory=_one_part)
    ctx = _Ctx()
    load.run_load(ctx, _cfg(tmp_path), kind=kind, run_id="dag", repo_root=str(tmp_path), timeout=60)
    assert len(_unfiltered_warnings(ctx)) == 1


def test_run_load_is_quiet_when_the_lists_are_mounted(monkeypatch, tmp_path):
    (tmp_path / "blocklists" / "kind_10002").mkdir(parents=True)
    (tmp_path / "blocklists" / "kind_10002" / "bots.txt").write_text("a" * 64 + "\n")
    _patch_common(monkeypatch, parts_factory=_one_part)
    ctx = _Ctx()
    load.run_load(ctx, _cfg(tmp_path), kind="10002", run_id="dag", repo_root=str(tmp_path), timeout=60)
    assert _unfiltered_warnings(ctx) == []


def test_run_load_does_not_warn_for_the_full_zaps_load(monkeypatch, tmp_path):
    """The full kind-9735 (zap-receipts) load takes the blocklist but does not apply it, so a missing
    mount changes nothing there and a warning would be false."""
    _patch_common(monkeypatch, parts_factory=_one_part)
    ctx = _Ctx()
    load.run_load(ctx, _cfg(tmp_path), kind="9735", run_id="dag", repo_root=str(tmp_path), timeout=60)
    assert _unfiltered_warnings(ctx) == []


def test_blocklist_present_finds_lists_in_per_kind_subdirectories(tmp_path):
    """The lists live in blocklists/<kind>/*.txt (the loader walks them recursively). A flat
    glob found none, so every load warned "loading UNFILTERED" while filtering fine."""
    (tmp_path / "blocklists" / "kind_3_follow_lists").mkdir(parents=True)
    assert not load._blocklist_present(str(tmp_path))
    (tmp_path / "blocklists" / "kind_3_follow_lists" / "bots.txt").write_text("a" * 64 + "\n")
    assert load._blocklist_present(str(tmp_path))


def test_blocklist_present_against_the_synced_lists():
    """blocklists/** is gitignored (synced onto the box and the workstation), so a fresh clone has
    none: skip there rather than fail."""
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    synced = [f for _, _, fs in os.walk(os.path.join(repo_root, "blocklists")) for f in fs if f.endswith(".txt")]
    if not synced:
        pytest.skip("no synced blocklists in this checkout")
    assert load._blocklist_present(repo_root)
