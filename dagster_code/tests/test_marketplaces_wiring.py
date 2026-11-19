"""The /marketplaces job's ordering: the listings extract must precede the deletions extract.

extract_markets_deletions.sql joins `e` tags to the kind-30402 (markets-product-listings) ids, and
run_extract flushes and registers only its OWN kind's partitions. So those partitions are current
only once the listings extract has run. Without an explicit dependency the executor's order is
incidental: a listing deleted by id on the day it was published would count as live, the reference
generator (which reads S3 directly) would disagree, and nothing would fail until the agreement
gate ran. No unit test of the SQL can see this.

Needs dagster, so it skips on a workstation (run it in the image). test_pipeline_marketplaces.py checks
the declaration itself with no dagster, so the quick local gate sees the edge go missing.
"""

import pytest

dg = pytest.importorskip("dagster", reason="dagster is not installed locally")
from dagster import AssetKey  # noqa: E402
from dagster_code import assets, definitions  # noqa: E402

_LISTINGS = "marketplaces_listings_extract"
_DELETIONS = "marketplaces_deletions_extract"


def test_the_deletions_extract_declares_the_listings_extract_as_a_dependency():
    deps = assets.marketplaces_deletions_extract.asset_deps[AssetKey(_DELETIONS)]
    assert AssetKey(_LISTINGS) in deps, sorted(d.to_user_string() for d in deps)


def test_the_job_runs_the_listings_extract_first():
    """The dependency is only worth something if the job honours it end to end."""
    job = definitions.defs.get_repository_def().get_job("marketplaces_refresh")
    from dagster._core.execution.api import create_execution_plan

    order = [s.key for s in create_execution_plan(job).get_steps_to_execute_in_topo_order()]
    at = {name: next(i for i, s in enumerate(order) if name in s)
          for name in (_LISTINGS, _DELETIONS, "marketplaces_snapshot")}
    assert at[_LISTINGS] < at[_DELETIONS] < at["marketplaces_snapshot"], order
