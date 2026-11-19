"""The /zapstore job's ordering: the catalog load must precede the three that follow it.

kind-30063 (zapstore-app-releases), kind-30267 (zapstore-app-stacks) and the
kind-9735 (zap-receipts) zapstore subset each read
events_kind_32267_app_catalog_excluded and drop whatever belongs to a listing the
catalog load rejected. Without an explicit dependency the executor's order is
incidental: the three would silently apply the PREVIOUS refresh's exclusions, or
none at all on a first run, and nothing would fail. That is the failure this file
exists to catch, because no unit test of the loader can see it.

Needs dagster, so it skips on a workstation (run it in the image).
"""

import pytest

dg = pytest.importorskip("dagster", reason="dagster is not installed locally")
from dagster import AssetKey  # noqa: E402
from dagster_code import assets, definitions  # noqa: E402

_CATALOG = AssetKey("zapstore_catalog_table")
_FOLLOWERS = ("zapstore_releases_table", "zapstore_stacks_table", "zapstore_zaps_table")


def _asset_def(name):
    key = AssetKey(name)
    for obj in vars(assets).values():
        if hasattr(obj, "keys") and hasattr(obj, "asset_deps") and key in obj.keys:
            return obj
    raise AssertionError(f"no AssetsDefinition exports {name}")


@pytest.mark.parametrize("name", _FOLLOWERS)
def test_each_dependent_load_declares_the_catalog_as_a_dependency(name):
    deps = _asset_def(name).asset_deps[AssetKey(name)]
    assert _CATALOG in deps, (
        f"{name} reads the catalog's exclusions but does not depend on it; "
        f"declared deps: {sorted(d.to_user_string() for d in deps)}")


def test_the_job_executes_the_catalog_before_every_dependent_load():
    """The dependency is only worth something if the job honours it end to end."""
    job = definitions.defs.get_repository_def().get_job("zapstore_refresh")
    from dagster._core.execution.api import create_execution_plan

    order = [s.key for s in create_execution_plan(job).get_steps_to_execute_in_topo_order()]
    catalog_at = next(i for i, s in enumerate(order) if "zapstore_catalog_table" in s)
    for name in _FOLLOWERS:
        assert catalog_at < next(i for i, s in enumerate(order) if name in s), \
            f"{name} is scheduled before the catalog load: {order}"
