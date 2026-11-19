"""pipeline/lineage.py: the declared lineage is internally consistent and names real things
(pure, no dagster). scripts/check_lineage.py checks it against the consumers' own code."""

import os
import re

from dagster_code.pipeline import lineage

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _read(*parts):
    with open(os.path.join(_ROOT, *parts)) as fh:
        return fh.read()


def test_the_declarations_are_consistent():
    assert lineage.problems() == []


def test_every_produced_table_exists_in_the_schema():
    init = _read("schema", "init.sql")
    declared = {m.group(1).lower() for m in re.finditer(
        r"CREATE\s+(?:TABLE|MATERIALIZED VIEW)\s+(?:IF NOT EXISTS\s+)?([a-z_][a-z0-9_]*)", init, re.I)}
    missing = sorted(t for ts in lineage.PRODUCES.values() for t in ts if t not in declared)
    assert missing == []


def test_every_producer_is_a_real_asset():
    src = _read("dagster_code", "assets.py")
    for asset in lineage.PRODUCES:
        assert re.search(r'\bdef %s\(|name="%s"|table_name="%s"' % (asset, asset, asset), src), asset


def test_a_job_is_not_a_surface_the_profiles_load_feeds_zapstore():
    """lightning_refresh's kind-0 (profile-metadata) load is read by /zapstore and the MCP
    zapstore domain: the edge a job -> page table would miss (2026-10-17)."""
    up = lineage.consumer_upstream()
    assert "lightning_profiles_table" in up["consumer/web/zapstore"]
    assert "lightning_profiles_table" in up["consumer/mcp/zapstore"]


def test_only_pages_carry_links():
    for c in lineage.CONSUMERS:
        kind = c["key"][0]
        if kind == "mcp":
            assert not c.get("url"), c["key"]  # never a clickable MCP URL in an alert
        if kind == "web":
            assert c["url"].startswith(lineage.SITE + "/"), c["key"]


def test_labels_cover_every_consumer():
    labels = lineage.consumer_labels()
    assert set(labels) == {lineage.consumer_id(c["key"]) for c in lineage.CONSUMERS}
    assert {kind for kind, _l, _u in labels.values()} <= set(lineage.KIND_ORDER)


def test_a_table_claimed_twice_is_refused(monkeypatch):
    monkeypatch.setitem(lineage.PRODUCES, "x_asset", ("ngit_snapshot",))
    assert any("written by both" in p for p in lineage.problems())


def test_an_unproduced_read_is_reported(monkeypatch):
    bad = {"key": ("web", "nope"), "label": "/nope", "url": lineage.SITE + "/nope",
           "reads": ("no_such_table",)}
    monkeypatch.setattr(lineage, "CONSUMERS", lineage.CONSUMERS + (bad,))
    assert any("no_such_table" in p for p in lineage.problems())


def test_the_declared_reads_match_the_consumers_code():
    """scripts/check_lineage.py over the sibling nostrology-web checkout and the MCP modules.
    Skips where the web checkout is absent (the Dagster image)."""
    import importlib.util

    import pytest

    web = os.path.join(os.path.dirname(_ROOT), "nostrology-web")
    if not os.path.isdir(os.path.join(web, "src", "routes")):
        pytest.skip("no nostrology-web checkout beside this repo")
    spec = importlib.util.spec_from_file_location("check_lineage", os.path.join(_ROOT, "scripts", "check_lineage.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert mod.check(web) == []
