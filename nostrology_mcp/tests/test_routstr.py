"""The Routstr family's own contract (design: docs/design/routstr-mcp-design.md).

Three layers, no DB:
  (a) spec invariants - the rule that makes these safe to serve next to the page:
      a document spec reads ONLY routstr_snapshot through the page's own newest-row
      selection, a substrate spec reads ONLY substrate tables, every statement is
      free of a clock, and the freshness probe is the refresh's generation instant;
  (b) the wired tools with a stubbed fetch - domain resolution, the two new entity
      filters (acceptance before shape, the remedies), day_inline windows, and the
      reference tool's provider_pubkey / no-scope records;
  (c) the registry - what /data reads.

The pipeline-to-tool parity (real SQL over rows the real pipeline wrote) is
test_routstr_contract_pg.py; the live parity gates against Aurora are at the
bottom of this module.
"""

import csv
import dataclasses
import io
import json
import os
import re
from datetime import date

import anyio
import pytest

from nostrology_mcp import server as srv_mod
from nostrology_mcp.db import MetricResult
from nostrology_mcp.metric import (
    ENTITY_FILTER_BINDS,
    METRICS,
    MODEL_FILTER_MAX_CHARS,
    ROUTSTR_METRIC_SPECS,
    _AS_OF_SQL_ROUTSTR,
    registry_entries,
    resolve_domain,
)
from nostrology_mcp.reference import (
    RECORDS,
    ROUTSTR_RECORD_SPECS,
    ROW_CAP,
    registry_reference_entries,
    resolve_record_domain,
)

ROUTSTR_SPECS = sorted(ROUTSTR_METRIC_SPECS, key=lambda s: s.key)
ROUTSTR_IDS = [s.key for s in ROUTSTR_SPECS]
DOC_SPECS = [s for s in ROUTSTR_SPECS if s.source_table == "routstr_snapshot"]
SUBSTRATE_SPECS = [s for s in ROUTSTR_SPECS if s.source_table != "routstr_snapshot"]
ROUTSTR_RECORDS = sorted(ROUTSTR_RECORD_SPECS, key=lambda s: s.key)
RECORD_IDS = [s.key for s in ROUTSTR_RECORDS]

# The page's own row selection (nostrology-web SQL_ROUTSTR_SNAPSHOT): newest
# generated_utc, highest id on a tie. Every document read must use exactly it.
PAGE_ROW_SELECTION = "ORDER BY generated_utc DESC, id DESC\n    LIMIT 1"
SUBSTRATE_TABLES = {"routstr_providers", "routstr_provider_days", "routstr_model_days",
                    "routstr_catalog_versions", "routstr_reviews"}
PROVIDER = "a" * 64


# -- (a) spec invariants -----------------------------------------------------------


@pytest.mark.parametrize("spec", ROUTSTR_SPECS, ids=ROUTSTR_IDS)
def test_routstr_specs_read_one_side_only(spec):
    """A document spec reads the document and nothing else; a substrate spec reads
    substrate tables and never the document - so no statement can straddle the
    nightly write that replaces both."""
    if spec in DOC_SPECS:
        assert PAGE_ROW_SELECTION in spec.sql, f"{spec.key} does not select the page's row"
        assert not any(t in spec.sql for t in SUBSTRATE_TABLES)
    else:
        assert "routstr_snapshot" not in spec.sql
        assert spec.source_table in SUBSTRATE_TABLES


@pytest.mark.parametrize("spec", ROUTSTR_SPECS, ids=ROUTSTR_IDS)
def test_routstr_freshness_is_the_refresh_instant(spec):
    """One probe for the family: the substrate is written in the same transaction
    as the snapshot row, so its generated_utc is every routstr metric's freshness."""
    assert spec.as_of_sql == _AS_OF_SQL_ROUTSTR
    assert spec.freshness_table == "routstr_snapshot"
    assert "generation instant" in spec.as_of_source


@pytest.mark.parametrize("spec", ROUTSTR_SPECS, ids=ROUTSTR_IDS)
def test_routstr_columns_are_produced_by_the_sql(spec):
    """Every declared column is an output name of the statement (the renderer
    zips strictly, so a missing alias is a crash in production, not a typo)."""
    for c in spec.columns:
        assert re.search(rf"\b{re.escape(c.name)}\b", spec.sql), f"{spec.key}: {c.name} not in SQL"


@pytest.mark.parametrize("spec", [s for s in ROUTSTR_SPECS if s.grain == "day"],
                         ids=[s.key for s in ROUTSTR_SPECS if s.grain == "day"])
def test_routstr_day_specs_put_utc_day_first_for_the_day_wrap(spec):
    assert spec.columns[0].name == "utc_day" and spec.columns[0].type == "date"
    assert spec.window_mode == "day_wrap"
    assert "ORDER BY 1" in spec.sql
    assert spec.windowed_order_by.startswith("1")


@pytest.mark.parametrize("spec", [s for s in ROUTSTR_SPECS if s.grain == "total"],
                         ids=[s.key for s in ROUTSTR_SPECS if s.grain == "total"])
def test_routstr_total_specs_window_inline_by_bucket_start_day(spec):
    """A SUM over pre-aggregated buckets: the window must filter BEFORE the
    aggregation (day_inline), both bounds always bound, keyed by the bucket's
    start day - the nutzaps convention, stated in the methodology."""
    assert spec.window_mode == "day_inline"
    assert "%(from_day)s::date IS NULL OR" in spec.sql and "%(to_day)s::date IS NULL OR" in spec.sql
    assert "SUM(" in spec.sql and "GROUP BY" in spec.sql
    assert "STARTING inside it" in spec.methodology
    assert spec.output == "entities" and spec.series_type == "event"


@pytest.mark.parametrize("spec", [s for s in ROUTSTR_SPECS if s.grain == "current"],
                         ids=[s.key for s in ROUTSTR_SPECS if s.grain == "current"])
def test_routstr_current_specs_are_snapshots(spec):
    assert spec.series_type == "snapshot" and spec.window_mode == "none"
    assert not spec.additive_over_time
    assert spec.snapshot_note and "refresh" in spec.snapshot_note


def test_provider_activity_unfiltered_is_included_providers_only():
    """The partition contract: without a filter the ledger rows exclude the
    excluded providers (so the per-day sums tile network_activity); with a
    filter the named provider is returned regardless."""
    for key in ("routstr.provider_activity@day", "routstr.provider_activity@total"):
        sql = METRICS[key].sql
        assert "%(filter_provider)s::character(64) IS NULL AND NOT p.excluded" in sql
        assert "OR d.pubkey = %(filter_provider)s::character(64)" in sql
        assert "partition" not in METRICS[key].summary  # the rule lives in the partition text
        assert "INCLUDED providers only" in METRICS[key].partition


def test_model_substrate_needs_no_exclusion_join():
    """routstr_model_days is written over included providers only, so the model
    specs neither join routstr_providers nor mention exclusion in SQL."""
    for key in ("routstr.model_activity@day", "routstr.model_activity@total"):
        assert "routstr_providers" not in METRICS[key].sql
        assert METRICS[key].label_tables == ()


@pytest.mark.parametrize("spec", ROUTSTR_SPECS, ids=ROUTSTR_IDS)
def test_routstr_prose_carries_the_self_reported_caveat_where_numbers_are_usage(spec):
    """The one caveat that frames every usage number. The two announcement-shaped
    metrics (catalog size, facets) carry no usage numbers and say what they count."""
    usage = spec.metric not in {"catalog_size", "provider_facets"}
    assert ("SELF-REPORTED" in spec.methodology or "self-reported" in spec.methodology) == usage
    if "msats" in spec.unit:
        assert "MILLI-satoshis" in spec.methodology


@pytest.mark.parametrize("spec", ROUTSTR_SPECS, ids=ROUTSTR_IDS)
def test_routstr_entity_filters_are_provider_or_model(spec):
    assert set(spec.entity_filters) <= {"provider_pubkey", "model"}
    for name in spec.entity_filters:
        bind = ENTITY_FILTER_BINDS[name]
        assert f"%({bind})s" in spec.sql and spec.bind_params[bind] is None
    for name in ("publisher_pubkey", "app_id"):
        assert f"%({ENTITY_FILTER_BINDS[name]})s" not in spec.sql


def test_provider_and_model_third_party_columns_are_declared():
    """Provider names, versions, model ids and facet values are third-party text;
    the renderer's sanitize pass and the untrusted: line key off the declaration."""
    expect = {
        "routstr.provider_leaderboard@current": {"provider_name", "version", "review"},
        "routstr.provider_activity@day": {"provider_name"},
        "routstr.provider_activity@total": {"provider_name"},
        "routstr.model_leaderboard@current": {"model"},
        "routstr.model_activity@day": {"model"},
        "routstr.model_activity@total": {"model"},
        "routstr.provider_facets@current": {"value"},
    }
    for key, names in expect.items():
        assert {c.name for c in METRICS[key].columns if c.third_party} == names, key
    for key in ("routstr.network_activity@day", "routstr.cumulative_revenue@day",
                "routstr.network_summary@current", "routstr.success_rate@current",
                "routstr.catalog_size@day"):
        assert not any(c.third_party for c in METRICS[key].columns), key


def test_routstr_summary_columns_name_the_sats_unit_exactly_twice():
    """The two revenue headline columns are SATS (the page's pre-converted values);
    every other Routstr revenue column is msats - the unit lives on the column."""
    spec = METRICS["routstr.network_summary@current"]
    sats = [c.name for c in spec.columns if c.unit == "sats"]
    assert sats == ["revenue_30d_sats", "revenue_alltime_sats"]
    assert "SATS" in spec.methodology


def test_resolve_domain_by_metric_name():
    assert resolve_domain("zapstore", "network_summary") == "routstr"
    assert resolve_domain("routstr", "network_summary") == "routstr"
    assert resolve_domain("routstr", "zap_count") == "zapstore"
    assert resolve_domain("zapstore", "no_such_metric") == "zapstore"
    assert resolve_record_domain("zapstore", "provider_profile") == "routstr"
    assert resolve_record_domain("routstr", "app_catalog") == "zapstore"
    assert resolve_record_domain("routstr", "no_such_record") == "routstr"


# -- records ------------------------------------------------------------------------


@pytest.mark.parametrize("spec", ROUTSTR_RECORDS, ids=RECORD_IDS)
def test_routstr_records_scope_by_provider_or_nothing(spec):
    assert spec.scope_param in ("provider_pubkey", None)
    assert not spec.scope_required and not spec.app_filterable
    assert spec.scope_bind in ("provider", None)
    assert "%(publisher)s" not in spec.sql and "%(app_id)s" not in spec.sql
    assert spec.sql_app_scoped is None and spec.columns_app_scoped is None
    assert spec.bind_params["row_cap"] == ROW_CAP


@pytest.mark.parametrize("spec", ROUTSTR_RECORDS, ids=RECORD_IDS)
def test_routstr_records_read_one_side_only(spec):
    if spec.source_table == "routstr_snapshot":
        assert PAGE_ROW_SELECTION in spec.sql
        assert not any(t in spec.sql for t in SUBSTRATE_TABLES)
    else:
        assert "routstr_snapshot" not in spec.sql
        assert spec.freshness_table == "routstr_snapshot"
    assert spec.as_of_sql == _AS_OF_SQL_ROUTSTR


def test_catalog_records_agree_on_the_newest_version_rule():
    """model_catalog picks the newest version by (created_at DESC, id DESC) - the
    same "last one wins" build_snapshot applies to its ascending list, so the
    record equals the document's catalog.models."""
    sql = RECORDS["routstr.model_catalog"].sql
    assert "ORDER BY version_utc DESC, event_id DESC" in sql and "LIMIT 1" in sql


def test_provider_reviews_windows_over_the_review_time():
    spec = RECORDS["routstr.provider_reviews"]
    assert spec.windowable and "EXTRACT(EPOCH FROM r.reviewed_utc) >= %(from_epoch)s" in spec.sql
    assert RECORDS["routstr.catalog_versions"].windowable
    assert not RECORDS["routstr.model_catalog"].windowable
    assert not RECORDS["routstr.exclusions"].windowable


# -- (b) the wired tools -------------------------------------------------------------


def _call(tool, arguments):
    async def go():
        return await srv_mod.server.call_tool(tool, arguments)

    return anyio.run(go)


def _dummy_row(columns):
    vals = []
    for c in columns:
        if c.type == "date":
            vals.append(date(2026, 8, 20))
        elif c.type in ("integer", "timestamp", "number"):
            vals.append(7)
        elif c.type == "boolean":
            vals.append(False)
        else:
            vals.append("x")
    return tuple(vals)


@pytest.fixture
def stub(monkeypatch, tmp_path):
    """Records (spec, sql, params) for both tools; rows match the spec's column
    contract (+ the trailing __total_rows the reference path strips)."""
    seen = []
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    def fake_fetch(spec, sql=None, params=None):
        seen.append((spec, sql, params))
        if hasattr(spec, "record") or getattr(spec, "row_cap", None) is not None:
            row = _dummy_row(spec.columns) + (1,)
        else:
            row = _dummy_row(spec.columns)
        return MetricResult(rows=[row], as_of_epoch=1787529600, connect_ms=1, query_ms=2)

    monkeypatch.setattr(srv_mod, "fetch_metric", fake_fetch)
    return tmp_path, seen


def _records(tmp_path):
    files = [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


def _text(res):
    assert res.is_error is False, res.content[0].text
    return res.content[0].text


def test_a_routstr_metric_resolves_its_domain_from_the_zapstore_default(stub):
    tmp_path, seen = stub
    text = _text(_call("nostrology_query_metric", {"metric": "network_summary"}))
    assert seen[-1][0].key == "routstr.network_summary@current"
    assert text.startswith("metric: routstr.network_summary  |  grain: current")
    (rec,) = _records(tmp_path)
    assert rec["params"]["domain"] == "zapstore" and rec["params"]["domain_resolved"] == "routstr"
    assert rec["params"]["grain_resolved"] == "current" and rec["outcome"] == "ok"


def test_every_routstr_metric_is_servable_under_its_own_domain(stub):
    _, seen = stub
    for spec in ROUTSTR_SPECS:
        _text(_call("nostrology_query_metric",
                    {"metric": spec.metric, "domain": "routstr", "grain": spec.grain}))
        assert seen[-1][0] is spec


def test_provider_activity_total_binds_the_window_inline(stub):
    _, seen = stub
    _text(_call("nostrology_query_metric", {
        "metric": "provider_activity", "grain": "total", "from": "2026-07-25", "to": "2026-08-23",
    }))
    spec, sql, params = seen[-1]
    assert spec.key == "routstr.provider_activity@total"
    assert sql is None  # the spec's own statement, never wrapped
    assert params["from_day"] == date(2026, 7, 25) and params["to_day"] == date(2026, 8, 23)
    assert params["filter_provider"] is None


def test_provider_activity_total_without_a_window_binds_null_bounds(stub):
    _, seen = stub
    text = _text(_call("nostrology_query_metric", {"metric": "provider_activity", "grain": "total"}))
    _, sql, params = seen[-1]
    assert sql is None and params == {"filter_provider": None, "from_day": None, "to_day": None}
    assert "window: all history" in text
    assert "one row per provider over the whole range" in text


def test_model_activity_total_without_a_window_names_its_row_noun(stub):
    text = _text(_call("nostrology_query_metric", {"metric": "model_activity", "grain": "total"}))
    assert "one row per model over the whole range" in text
    assert "per app" not in text


def test_total_windows_sum_daily_buckets_only_by_construction():
    """The page's windowed rule is daily-only; the two @total SQLs carry the grain
    predicate tied to the window binds so parity is structural, not geometric."""
    for key in ("routstr.provider_activity@total", "routstr.model_activity@total"):
        sql = METRICS[key].sql
        assert "(%(from_day)s::date IS NULL AND %(to_day)s::date IS NULL) OR" in sql and "grain = 'd'" in sql


def test_provider_activity_day_window_wraps_capped_and_keeps_the_day_provider_order(stub):
    _, seen = stub
    _text(_call("nostrology_query_metric", {
        "metric": "provider_activity", "from": "2026-08-01", "to": "2026-08-07",
    }))
    _, sql, params = seen[-1]
    assert sql is not None and "w.utc_day >= %(from_day)s" in sql
    assert "COUNT(*) OVER () AS __total_rows" in sql and sql.rstrip().endswith("LIMIT %(row_cap)s")
    assert "ORDER BY 1, 3, 2\nLIMIT" in sql
    assert params == {"filter_provider": None, "from_day": date(2026, 8, 1), "to_day": date(2026, 8, 7),
                      "row_cap": 2000}


def test_capped_metrics_are_wrapped_even_without_a_window_and_say_when_they_truncate(stub, monkeypatch):
    """The third-party-growable substrate metrics carry the reference records'
    row-cap contract: always wrapped (window or not), the pre-cap total stripped
    and reported, a loud truncated: line when it bites, none when it does not."""
    tmp_path, seen = stub
    text = _text(_call("nostrology_query_metric", {"metric": "model_activity", "domain": "routstr"}))
    _, sql, params = seen[-1]
    assert sql.startswith("SELECT w.*, COUNT(*) OVER () AS __total_rows FROM (")
    assert "w.utc_day" not in sql and params == {"filter_model": None, "row_cap": 2000}
    envelope = text.split("\n\n", 1)[0].splitlines()
    assert "rows: 1  |" in text and not any(ln.startswith("truncated:") for ln in envelope)
    assert "__total_rows" not in text
    # the capped SNAPSHOT keeps its own ORDER BY (the document's) - no wrapper
    _text(_call("nostrology_query_metric", {"metric": "provider_leaderboard", "domain": "routstr"}))
    _, sql, params = seen[-1]
    assert sql is None and params == {"filter_provider": None, "row_cap": 2000}
    assert METRICS["routstr.provider_leaderboard@current"].sql.rstrip().endswith("LIMIT %(row_cap)s")

    def big_fetch(spec, sql=None, params=None):
        row = _dummy_row(spec.columns) + (5000,)
        return MetricResult(rows=[row, row], as_of_epoch=1787529600, connect_ms=1, query_ms=2)

    monkeypatch.setattr(srv_mod, "fetch_metric", big_fetch)
    text = _text(_call("nostrology_query_metric", {"metric": "model_activity", "domain": "routstr"}))
    assert "rows: 2 of 5000  |" in text
    assert "truncated: showing the first 2 of 5000 rows (ordered by utc_day ASC, model ASC, grain ASC); narrow with from/to or model=, or use grain=total" in text
    recs = _records(tmp_path)
    assert [r["rows"] for r in recs] == [1, 1, 2]


def test_uncapped_metrics_are_never_wrapped_without_a_window(stub):
    _, seen = stub
    _text(_call("nostrology_query_metric", {"metric": "zap_count"}))
    assert seen[-1][1] is None and seen[-1][2] is None
    for spec in METRICS.values():
        if spec.domain == "zapstore":
            assert spec.row_cap is None


def test_provider_filter_binds_and_echoes(stub):
    _, seen = stub
    text = _text(_call("nostrology_query_metric", {
        "metric": "provider_leaderboard", "provider_pubkey": PROVIDER,
    }))
    _, _, params = seen[-1]
    assert params["filter_provider"] == PROVIDER
    assert f"scope: provider_pubkey={PROVIDER}" in text.split("\n\n")[0]


def test_model_filter_binds_and_echoes(stub):
    _, seen = stub
    text = _text(_call("nostrology_query_metric", {
        "metric": "model_activity", "grain": "total", "model": "openai/gpt-4o",
    }))
    _, _, params = seen[-1]
    assert params["filter_model"] == "openai/gpt-4o"
    assert "scope: model=openai/gpt-4o" in text


@pytest.mark.parametrize("arguments,fragment", [
    # acceptance is checked BEFORE shape, so a malformed filter on a metric that
    # rejects it gets the routing remedy, not the decode hint
    ({"metric": "network_activity", "provider_pubkey": "npub1xyz"}, "routstr.provider_leaderboard"),
    ({"metric": "network_summary", "model": "gpt"}, "routstr.model_leaderboard"),
    ({"metric": "zap_count", "provider_pubkey": PROVIDER}, "provider_pubkey is a Routstr coordinate"),
    ({"metric": "zap_count", "model": "gpt"}, "model is a Routstr coordinate"),
    ({"metric": "provider_leaderboard", "publisher_pubkey": PROVIDER}, "keyed by provider, not publisher"),
    ({"metric": "provider_leaderboard", "app_id": "x"}, "app_id is a Zapstore coordinate"),
    ({"metric": "model_leaderboard", "provider_pubkey": PROVIDER}, "routstr.provider_leaderboard"),
    # shape, once accepted
    ({"metric": "provider_leaderboard", "provider_pubkey": "npub1xyz"}, "provider_pubkey must be a 64-char"),
    ({"metric": "model_leaderboard", "model": "m" * (MODEL_FILTER_MAX_CHARS + 1)}, "at most 60 characters"),
    ({"metric": "model_leaderboard", "model": ""}, "at most 60 characters"),
])
def test_routstr_filter_rejections_are_recorded_and_name_the_remedy(stub, arguments, fragment):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen = stub
    with pytest.raises(ToolError) as exc:
        _call("nostrology_query_metric", arguments)
    assert fragment in str(exc.value), str(exc.value)
    assert not seen  # never reached the DB
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_routstr_snapshots_reject_windows_with_the_reason(stub):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call("nostrology_query_metric", {"metric": "provider_leaderboard", "from": "2026-08-01"})
    assert "current-state snapshot" in str(exc.value) and "refresh" in str(exc.value)


def test_metric_schema_advertises_the_routstr_filters():
    async def go():
        return await srv_mod.server.list_tools()

    (tool,) = [t for t in anyio.run(go) if t.name == "nostrology_query_metric"]
    props = tool.input_schema["properties"]
    assert "routstr" in props["domain"]["enum"]
    for name in ("provider_pubkey", "model"):
        assert name in props and "anyOf" in props[name]
    value_branch = [b for b in props["model"]["anyOf"] if b.get("type") == "string"][0]
    assert value_branch["maxLength"] == MODEL_FILTER_MAX_CHARS
    assert "domain='routstr'" in tool.description
    for spec in ROUTSTR_SPECS:
        assert spec.metric in tool.description


# reference tool


def test_routstr_record_resolves_domain_and_binds_the_provider_scope(stub):
    tmp_path, seen = stub
    text = _text(_call("nostrology_query_reference", {"record": "provider_profile"}))
    spec, _, binds = seen[-1]
    assert spec.key == "routstr.provider_profile"
    assert binds == {"provider": None, "row_cap": ROW_CAP}
    assert "scope: all providers (no provider_pubkey filter)" in text
    text = _text(_call("nostrology_query_reference", {
        "record": "provider_profile", "domain": "routstr", "provider_pubkey": PROVIDER,
    }))
    assert seen[-1][2]["provider"] == PROVIDER
    assert f"scope: provider_pubkey={PROVIDER}" in text
    recs = _records(tmp_path)
    assert recs[0]["params"]["domain_resolved"] == "routstr" and "domain_resolved" not in recs[1]["params"]


def test_no_scope_records_say_so_and_reject_every_scope(stub):
    from mcp.server.mcpserver.exceptions import ToolError

    _, seen = stub
    for record in ("catalog_versions", "model_catalog", "exclusions"):
        text = _text(_call("nostrology_query_reference", {"record": record}))
        assert "scope: whole domain (this record takes no entity filter)" in text
        assert seen[-1][2].get("provider") is None and "publisher" not in seen[-1][2]
        for bad in ({"provider_pubkey": PROVIDER}, {"publisher_pubkey": PROVIDER}):
            with pytest.raises(ToolError) as exc:
                _call("nostrology_query_reference", {"record": record, **bad})
            assert "takes no entity scope" in str(exc.value)


def test_provider_reviews_window_binds_epochs(stub):
    _, seen = stub
    text = _text(_call("nostrology_query_reference", {
        "record": "provider_reviews", "from": "2026-08-01", "to": "2026-08-31",
    }))
    binds = seen[-1][2]
    assert binds["from_epoch"] == 1785542400 and binds["to_epoch_excl"] == 1788220800
    assert "window: 2026-08-01..2026-08-31" in text


@pytest.mark.parametrize("arguments,fragment", [
    ({"record": "provider_profile", "publisher_pubkey": PROVIDER}, "scoped by provider_pubkey, not publisher_pubkey"),
    ({"record": "provider_profile", "app_id": "x"}, "app_id is a Zapstore coordinate"),
    ({"record": "provider_profile", "provider_pubkey": "npub1abc"}, "provider_pubkey must be a 64-char"),
    ({"record": "app_catalog", "provider_pubkey": PROVIDER}, "scoped by publisher_pubkey, not provider_pubkey"),
    ({"record": "model_catalog", "from": "2026-08-01"}, "no time axis"),
])
def test_routstr_record_rejections(stub, arguments, fragment):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen = stub
    with pytest.raises(ToolError) as exc:
        _call("nostrology_query_reference", arguments)
    assert fragment in str(exc.value), str(exc.value)
    assert not seen
    (rec,) = _records(tmp_path)
    assert rec["outcome"] in ("invalid_filter", "invalid_window")


def test_reference_schema_advertises_provider_pubkey_and_routes_the_records():
    async def go():
        return await srv_mod.server.list_tools()

    (tool,) = [t for t in anyio.run(go) if t.name == "nostrology_query_reference"]
    props = tool.input_schema["properties"]
    assert "provider_pubkey" in props and "routstr" in props["domain"]["enum"]
    for spec in ROUTSTR_RECORDS:
        assert spec.record in tool.description


def test_zapstore_reference_envelope_is_unchanged(stub):
    """The second domain must not move a byte of the first's envelope."""
    text = _text(_call("nostrology_query_reference", {"record": "publisher_profile"}))
    assert "scope: all publishers (no publisher_pubkey filter)" in text


# -- (c) the registry --------------------------------------------------------------


def test_registry_publishes_the_routstr_domain_without_physical_schema():
    entries = [e for e in registry_entries() if e["domain"] == "routstr"]
    assert {e["key"] for e in entries} == {s.key for s in ROUTSTR_SPECS}
    for e in entries:
        assert "source_table" not in e and "as_of_table" not in e
        assert e["params"]["grain"]["values"]
        if e["metric"] in ("provider_leaderboard", "provider_activity"):
            assert set(e["params"]["entity_filters"]) == {"provider_pubkey"}
        elif e["metric"] in ("model_leaderboard", "model_activity"):
            assert set(e["params"]["entity_filters"]) == {"model"}
        else:
            assert "entity_filters" not in e["params"]
    refs = {e["record"]: e for e in registry_reference_entries() if e["domain"] == "routstr"}
    assert set(refs) == {s.record for s in ROUTSTR_RECORDS}
    for e in refs.values():
        assert "publisher_pubkey" not in e["params"]
        assert e["params"]["scope"]["param"] in ("provider_pubkey", None)
    zap = [e for e in registry_reference_entries() if e["domain"] == "zapstore"]
    for e in zap:
        assert e["params"]["publisher_pubkey"] == {
            "required": e["params"]["scope"]["required"], "note": e["params"]["scope"]["note"]}


def test_routstr_multi_grain_metrics_share_one_summary():
    for metric in ("provider_activity", "model_activity"):
        assert METRICS[f"routstr.{metric}@day"].summary == METRICS[f"routstr.{metric}@total"].summary


# The Routstr web page is incubating: linked from /wip only and kept out of the
# sitemap. Everything below is public - the server instructions, both tools'
# descriptions and schemas, registry.json (which the sitemap-listed /data pages
# render) and the nostrology://registry resource - so none of it may name that
# page, or describe the Routstr data by the page's layout ("the page", its
# tabs), which means nothing to a reader who cannot see it. Name the matching
# metric or record instead. Drop this guard when the page launches.
_ROUTSTR_PAGE_URL = re.compile(r"nostrolo\.gy/routstr|(?<![\w/.])/routstr\b", re.I)
_ROUTSTR_PAGE_LAYOUT = re.compile(r"\bthe page\b|\b(pages?|tabs?|cards?|charts?|chip cloud)\b", re.I)


def test_served_text_never_names_the_incubating_routstr_page():
    async def tools():
        return await srv_mod.server.list_tools()

    served = {
        "instructions": srv_mod.INSTRUCTIONS,
        "registry.json": json.dumps(srv_mod._registry_payload()),
        "nostrology://registry": srv_mod.registry_resource(),
        "tools/list": json.dumps([t.model_dump(mode="json") for t in anyio.run(tools)]),
    }
    for where, text in served.items():
        hit = _ROUTSTR_PAGE_URL.search(text)
        assert hit is None, f"{where} names the Routstr page: ...{text[max(0, hit.start() - 80):hit.end() + 40]}..."
    # The tool descriptions' Routstr sections get the layout check too. The
    # Zapstore and nutzaps sections may cite their public pages, so they are cut
    # off: the Routstr section runs from SECOND DOMAIN to THIRD DOMAIN, or to the
    # end of a description that has no third.
    sections = 0
    for tool in anyio.run(tools):
        desc = tool.description or ""
        if "SECOND DOMAIN" not in desc:
            continue
        sections += 1
        section = desc[desc.index("SECOND DOMAIN"):].split("THIRD DOMAIN")[0]
        assert "routstr" in section
        hit = _ROUTSTR_PAGE_LAYOUT.search(section)
        assert hit is None, f"{tool.name}'s Routstr section describes the page: ...{section[max(0, hit.start() - 80):hit.end() + 40]}..."
    assert sections == 2  # nostrology_query_metric and nostrology_query_reference
    # The layout check walks the specs themselves, not the registry: some of
    # their text (ordering_note, the envelope lines) reaches callers only in a
    # tool response. The SQL fields are never served, and their comments may
    # cite the page.
    def text_of(obj, field=""):
        if isinstance(obj, str):
            if "sql" not in field:
                yield field, obj
        elif isinstance(obj, (tuple, list)):
            for item in obj:
                yield from text_of(item, field)
        elif dataclasses.is_dataclass(obj):
            for f in dataclasses.fields(obj):
                yield from text_of(getattr(obj, f.name), f.name)

    assert ROUTSTR_SPECS and ROUTSTR_RECORDS
    for spec in [*ROUTSTR_SPECS, *ROUTSTR_RECORDS]:
        for field, text in text_of(spec):
            hit = _ROUTSTR_PAGE_URL.search(text) or _ROUTSTR_PAGE_LAYOUT.search(text)
            assert hit is None, f"{spec.key}.{field} describes the Routstr page: {hit.group(0)!r} in {text!r}"


# -- live parity gates (Aurora, via the tunnel) ---------------------------------------

_LIVE = pytest.mark.skipif(
    not os.environ.get("NOSTROLOGY_TEST_DSN"), reason="live DB gate: set NOSTROLOGY_TEST_DSN"
)


def _live_rows(spec, sql=None, params=None):
    from nostrology_mcp.db import fetch_metric

    return fetch_metric(spec, dsn=os.environ["NOSTROLOGY_TEST_DSN"], sql=sql, params=params).rows


def _live_doc():
    import psycopg2

    with psycopg2.connect(os.environ["NOSTROLOGY_TEST_DSN"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT data FROM routstr_snapshot ORDER BY generated_utc DESC, id DESC LIMIT 1")
        return cur.fetchone()[0]


@_LIVE
@pytest.mark.parametrize("spec", ROUTSTR_SPECS + ROUTSTR_RECORDS,
                         ids=ROUTSTR_IDS + RECORD_IDS)
def test_live_routstr_sql_explains(spec):
    import psycopg2

    with psycopg2.connect(os.environ["NOSTROLOGY_TEST_DSN"]) as conn, conn.cursor() as cur:
        binds = dict(spec.bind_params)
        if getattr(spec, "window_mode", None) == "day_inline":
            binds.update({"from_day": None, "to_day": None})
        cur.execute("EXPLAIN " + spec.sql, binds or None)
        cur.execute("EXPLAIN " + spec.as_of_sql)


@_LIVE
def test_live_provider_activity_total_reproduces_the_leaderboards_windows():
    """THE drift detector: the SQL SUM over the substrate at the two windows the
    page publishes equals the document's per-provider 30d and all-time figures."""
    from datetime import datetime, timedelta, timezone

    doc = _live_doc()
    gen = datetime.strptime(doc["generated_utc"], "%Y-%m-%d %H:%M:%SZ").replace(tzinfo=timezone.utc)
    today = gen.date()
    spec = METRICS["routstr.provider_activity@total"]
    in30 = {r[0]: r for r in _live_rows(spec, params={
        "filter_provider": None, "from_day": today - timedelta(days=30), "to_day": today - timedelta(days=1)})}
    alltime = {r[0]: r for r in _live_rows(spec, params={
        "filter_provider": None, "from_day": None, "to_day": None})}
    board = {r[0]: r[:-1] for r in _live_rows(METRICS["routstr.provider_leaderboard@current"])}  # strip __total_rows
    for pk, row in board.items():
        if row[-2]:  # excluded
            continue
        # (provider_pubkey, name, msats, requests, tokens, daily, weekly, first, last, excluded)
        got30 = in30.get(pk, (pk, None, 0, 0, 0))
        assert (got30[2], got30[3], got30[4]) == (row[9], row[10], row[11]), pk
        gotall = alltime.get(pk, (pk, None, 0, 0, 0))
        assert (gotall[2], gotall[3], gotall[4]) == (row[12], row[13], row[14]), pk


@_LIVE
def test_live_network_activity_is_the_documents_daily_series():
    doc = _live_doc()
    rows = _live_rows(METRICS["routstr.network_activity@day"])
    assert [(r[0].isoformat(), r[1], r[2], r[3], r[4]) for r in rows] == [
        (d["d"], d["msats"], d["req"], d["tok"], d["prov"]) for d in doc["daily"]]
    day_rows = _live_rows(METRICS["routstr.provider_activity@day"])
    per_day: dict = {}
    for r in day_rows:
        if r[1] == "d":
            per_day[r[0].isoformat()] = per_day.get(r[0].isoformat(), 0) + r[4]
    assert per_day == {d["d"]: d["msats"] for d in doc["daily"] if d["msats"]}


def _csv_rows(text):
    return list(csv.DictReader(io.StringIO(text.split("\n\n", 1)[1])))
