"""The Aurora dial reads its cluster from NOSTROLOGY_AURORA_CLUSTER when a run starts, the variable the
extract and load steps read, so the dial cannot move a cluster the data path does not use. No AWS: the
dial's client and its two calls are replaced."""

import pytest

pytest.importorskip("dagster", reason="the dial is a Dagster ConfigurableResource")

from dagster_code import resources  # noqa: E402
from dagster_code.pipeline.config import MissingConfig, PipelineConfig  # noqa: E402
from dagster_code.tests.fake_env import FAKE_ENV  # noqa: E402


class _Log:
    def info(self, *args, **kwargs):
        pass


class _Ctx:
    log = _Log()


def _record(monkeypatch):
    calls = []
    monkeypatch.setattr(resources.aws, "client", lambda service, region: object())
    monkeypatch.setattr(resources.aurora, "dial_up",
                        lambda rds, cluster, *args, **kwargs: calls.append(("up", cluster)))
    monkeypatch.setattr(resources.aurora, "dial_down",
                        lambda rds, cluster, *args, **kwargs: calls.append(("down", cluster)))
    return calls


def test_the_dial_has_no_cluster_of_its_own():
    assert resources.AuroraDial().cluster == ""


def test_the_deployed_dial_has_no_cluster_of_its_own():
    # An explicit cluster where definitions.py builds the resource would pass every other test here.
    from dagster_code import definitions
    assert definitions.defs.resources["aurora_dial"].cluster == ""


def test_the_dial_and_the_config_read_the_same_cluster(monkeypatch):
    for var, value in {**FAKE_ENV, "NOSTROLOGY_AURORA_CLUSTER": "another-cluster"}.items():
        monkeypatch.setenv(var, value)
    calls = _record(monkeypatch)
    with resources.AuroraDial().yield_for_execution(_Ctx()):
        assert calls == [("up", "another-cluster")]
    assert calls == [("up", "another-cluster"), ("down", "another-cluster")]
    assert PipelineConfig.from_env().aurora_cluster == "another-cluster"


def test_an_unset_cluster_fails_the_run_naming_the_variable(monkeypatch):
    monkeypatch.delenv("NOSTROLOGY_AURORA_CLUSTER", raising=False)
    calls = _record(monkeypatch)
    with pytest.raises(MissingConfig, match="NOSTROLOGY_AURORA_CLUSTER"):
        with resources.AuroraDial().yield_for_execution(_Ctx()):
            pass
    assert calls == []  # nothing was dialled


def test_an_explicit_cluster_overrides_the_variable(monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_AURORA_CLUSTER", "from-env")
    calls = _record(monkeypatch)
    with resources.AuroraDial(cluster="explicit").yield_for_execution(_Ctx()):
        pass
    assert calls == [("up", "explicit"), ("down", "explicit")]
