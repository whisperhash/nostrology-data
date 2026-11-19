"""Pipeline-to-tool contract tier for the Routstr family - a real Postgres, no
Aurora, no network.

WHY THIS TIER EXISTS. The Routstr design's one load-bearing rule is that the tools
never compute a number the page publishes: the pipeline computes once, writes the
document and the substrate in one transaction, and the tools select from those.
The pure tests pin the SQL's SHAPE; the live gates need the Aurora tunnel. This
tier closes the gap in between: it runs the REAL pipeline write (build_snapshot +
write_pg, on the pipeline's own hand-verifiable fixture) into a throwaway
Postgres, then drives every Routstr metric and record THROUGH THE WIRED TOOLS
(real SQL, real envelope) and asserts each answer against the document the same
build produced. A methodology drift between the pipeline and a tool's SQL - the
29-day window bug's shape - fails here before it can reach dev.

Needs `NOSTROLOGY_CONTRACT_DSN` pointing at a scratch Postgres the test may write to
(e.g. `docker run -d -e POSTGRES_PASSWORD=contract -e POSTGRES_DB=nostrology
-e POSTGRES_INITDB_ARGS='--locale-provider=icu --icu-locale=en-US-u-ka-shifted'
-p 127.0.0.1:15433:5432 postgres:16-alpine` ->
`postgresql://postgres:contract@127.0.0.1:15433/nostrology`, sorting text as the deploy
gate and production do); skips otherwise.
The routstr DDL is applied from schema/init.sql itself (single source), and the
six tables are truncated at module start. NEVER point this at the dev cluster.
"""

import csv
import datetime as dt
import functools
import io
import json
import os
import re

import anyio
import pytest

from nostrology_mcp import server as srv_mod
from nostrology_mcp.db import fetch_metric
from nostrology_mcp.metric import METRICS, ROUTSTR_METRIC_SPECS
from nostrology_mcp.reference import ROUTSTR_RECORD_SPECS

DSN = os.environ.get("NOSTROLOGY_CONTRACT_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="contract tier: set NOSTROLOGY_CONTRACT_DSN")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_TABLES = ("routstr_providers", "routstr_provider_days", "routstr_model_days",
           "routstr_catalog_versions", "routstr_reviews", "routstr_snapshot")


def _routstr_ddl() -> str:
    """The routstr section of schema/init.sql, verbatim: from its banner to EOF."""
    with open(os.path.join(_REPO_ROOT, "schema", "init.sql")) as fh:
        sql = fh.read()
    start = sql.index("-- /routstr - the routstr_refresh job's serving tables")
    return sql[start:]


@pytest.fixture(scope="module")
def built():
    """One pipeline build on the pipeline's own fixture, written to the scratch DB."""
    import psycopg2

    from dagster_code.pipeline import routstr as R
    from dagster_code.tests import test_pipeline_routstr as F

    if "127.0.0.1" not in DSN and "localhost" not in DSN:
        pytest.fail("the contract tier only ever writes to a LOCAL scratch database")
    a, an, c, r = F._base_fixture()
    # A second included provider so per-provider partitions have two members and
    # the (others) fold is non-trivial; it reports a model A does not.
    an.append(F.snap(F.PK_C, F.D(1) + 60, {
        "30d": F.win(720, 1440, [
            F.bucket(F.D(3), req=4, msats=8000, tok=400, models={"m1": (2, 3000, 200), "m9": (2, 5000, 200)}),
            F.bucket(F.D(40), req=1, msats=1000, tok=50, models={"m9": (1, 1000, 50)}),
        ]),
    }))
    state, data = R.build_snapshot(a, an, c, r, F.NOW)
    assert R.check_invariants(state, data) == []
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(_routstr_ddl())
            for t in _TABLES:
                cur.execute(f"TRUNCATE {t}")
        conn.commit()
        stats = R.write_pg(conn, state, data, run_id="contract-tier", min_ledger_rows=0, min_providers=0)
    finally:
        conn.close()
    return {"state": state, "data": data, "stats": stats, "now": F.NOW, "F": F}


@pytest.fixture
def wired(monkeypatch, tmp_path, built):
    """The real tools against the scratch DB: fetch_metric pinned to the DSN."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(srv_mod, "fetch_metric", functools.partial(fetch_metric, dsn=DSN))
    return built


def _call(tool, arguments):
    async def go():
        return await srv_mod.server.call_tool(tool, arguments)

    res = anyio.run(go)
    assert res.is_error is False, res.content[0].text
    return res.content[0].text


def _rows(text):
    envelope, table = text.split("\n\n", 1)
    return envelope, list(csv.DictReader(io.StringIO(table)))


def _day(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).strftime("%Y-%m-%d")


# -- the document side ----------------------------------------------------------------


def test_write_stats_match_the_state(built):
    s = built["stats"]
    assert s["provider_rows"] == len(built["state"]["providers"])
    assert s["ledger_rows"] == len(built["state"]["ledger"])
    assert s["model_day_rows"] == sum(len(v) for v in built["state"]["model_days"].values())
    assert s["catalog_version_rows"] == len(built["state"]["catalog_versions"])


def test_network_activity_is_the_documents_daily_series(wired):
    doc = wired["data"]
    env, rows = _rows(_call("nostrology_query_metric", {"metric": "network_activity"}))
    assert "metric: routstr.network_activity  |  grain: day" in env
    assert [(r["utc_day"], int(r["msats"]), int(r["requests"]), int(r["tokens"]), int(r["active_providers"]))
            for r in rows] == [(d["d"], d["msats"], d["req"], d["tok"], d["prov"]) for d in doc["daily"]]
    # a window is a pure filter of the same rows
    _, win = _rows(_call("nostrology_query_metric", {
        "metric": "network_activity", "from": doc["daily"][1]["d"], "to": doc["daily"][1]["d"]}))
    assert [r["utc_day"] for r in win] == [doc["daily"][1]["d"]]


def test_cumulative_revenue_ends_at_the_alltime_headline(wired):
    doc = wired["data"]
    _, rows = _rows(_call("nostrology_query_metric", {"metric": "cumulative_revenue"}))
    assert [(r["utc_day"], int(r["cumulative_msats"])) for r in rows] == [
        (p["d"], p["msats"]) for p in doc["cum_revenue"]]
    assert round(int(rows[-1]["cumulative_msats"]) / 1000) == doc["summary"]["revenue_alltime_sats"]


def test_network_summary_is_the_headline_row(wired):
    s = wired["data"]["summary"]
    env, (row,) = _rows(_call("nostrology_query_metric", {"metric": "network_summary"}))
    assert "state: current snapshot" in env
    for col, key in (("providers_announced", "providers_announced"),
                     ("providers_reporting", "providers_reporting"),
                     ("providers_active_30d", "providers_active_30d"),
                     ("revenue_30d_sats", "revenue_30d_sats"), ("requests_30d", "requests_30d"),
                     ("tokens_30d", "tokens_30d"), ("revenue_alltime_sats", "revenue_alltime_sats"),
                     ("requests_alltime", "requests_alltime"), ("tokens_alltime", "tokens_alltime"),
                     ("models_30d", "models_30d"), ("catalog_models", "catalog_models"), ("mints", "mints")):
        assert int(row[col]) == s[key], col
    assert row["first_activity"] == s["first_activity"] and row["last_activity"] == s["last_activity"]
    assert row["refresh_generated_at"] == wired["data"]["generated_utc"].replace(" ", "T")
    excl = {e["k"]: e["v"] for e in wired["data"]["exclusions"]}
    assert int(row["excluded_test_nodes"]) == excl["Test / staging nodes"]
    assert int(row["kind_squatters"]) == excl["Kind squatters"]
    assert row["success_rate_30d"] == "\\N"  # the fixture has no fresh 30d summaries


def test_provider_leaderboard_matches_the_documents_provider_rows(wired):
    doc = wired["data"]
    env, rows = _rows(_call("nostrology_query_metric", {"metric": "provider_leaderboard"}))
    by_id = {p["id"]: p for p in doc["providers"]}
    assert [r["provider_pubkey"][:12] for r in rows] == [p["id"] for p in doc["providers"]]
    for r in rows:
        p = by_id[r["provider_pubkey"][:12]]
        assert (int(r["msats_30d"]), int(r["requests_30d"]), int(r["tokens_30d"]), int(r["msats_alltime"])) == (
            p["msats_30d"], p["req_30d"], p["tok_30d"], p["msats_alltime"])
        assert (r["excluded"] == "true") == p["excluded"]
        assert r["provider_name"] == p["name"] and r["first_seen"] == p["first_seen"]
        assert r["review"] == (p["review"] if p["review"] is not None else "\\N")
    assert "untrusted: provider_name, version and review" in env


def test_provider_activity_total_reproduces_the_leaderboard_windows(wired):
    """THE parity gate the design leans on: SUM over the substrate at the page's
    two windows == the page's per-provider figures, for every included provider."""
    F, now = wired["F"], wired["now"]
    _, board = _rows(_call("nostrology_query_metric", {"metric": "provider_leaderboard"}))
    included = [r for r in board if r["excluded"] == "false"]
    assert len(included) >= 2
    _, alltime = _rows(_call("nostrology_query_metric", {"metric": "provider_activity", "grain": "total"}))
    _, in30 = _rows(_call("nostrology_query_metric", {
        "metric": "provider_activity", "grain": "total", "from": _day(now - 30 * F.DAY), "to": _day(now - F.DAY)}))
    alltime = {r["provider_pubkey"]: r for r in alltime}
    in30 = {r["provider_pubkey"]: r for r in in30}
    assert set(alltime) == {r["provider_pubkey"] for r in included}  # excluded never unfiltered
    for r in included:
        pk = r["provider_pubkey"]
        assert (int(alltime[pk]["msats"]), int(alltime[pk]["requests"]), int(alltime[pk]["tokens"])) == (
            int(r["msats_alltime"]), int(r["requests_alltime"]), int(r["tokens_alltime"]))
        got = in30.get(pk, {"msats": 0, "requests": 0, "tokens": 0})
        assert (int(got["msats"]), int(got["requests"]), int(got["tokens"])) == (
            int(r["msats_30d"]), int(r["requests_30d"]), int(r["tokens_30d"]))
    # the weekly pre-daily era shows up in the extent columns, not in the 30d window
    a = alltime[F.PK_A]
    assert a["weekly_buckets"] == "1" and a["daily_buckets"] == "2"
    assert in30[F.PK_A]["weekly_buckets"] == "0"


def test_provider_activity_total_window_sums_daily_buckets_only(wired):
    """A window that CONTAINS the weekly bucket's start day still excludes it: the
    page's windowed figures are daily-only, so the tool's are by construction
    (review 2026-08-29 - before this the parity held only by geometry)."""
    F = wired["F"]
    _, alltime = _rows(_call("nostrology_query_metric", {"metric": "provider_activity", "grain": "total"}))
    a_all = [r for r in alltime if r["provider_pubkey"] == F.PK_A][0]
    assert a_all["weekly_buckets"] == "1"
    _, wide = _rows(_call("nostrology_query_metric", {
        "metric": "provider_activity", "grain": "total",
        "from": _day(F.THU - 20 * F.R.WEEK_S), "to": _day(F.NOW)}))
    a = [r for r in wide if r["provider_pubkey"] == F.PK_A][0]
    assert a["weekly_buckets"] == "0" and a["daily_buckets"] == a_all["daily_buckets"]
    assert int(a["msats"]) == int(a_all["msats"]) - 50000  # the weekly bucket's 50000 msats
    _, models = _rows(_call("nostrology_query_metric", {
        "metric": "model_activity", "grain": "total",
        "from": _day(F.THU - 20 * F.R.WEEK_S), "to": _day(F.NOW)}))
    assert all(r["weekly_buckets"] == "0" for r in models)


def test_provider_activity_day_tiles_the_network_series_and_reaches_excluded_on_filter(wired):
    F, doc = wired["F"], wired["data"]
    _, rows = _rows(_call("nostrology_query_metric", {"metric": "provider_activity"}))
    per_day = {}
    for r in rows:
        assert r["excluded"] == "false"
        if r["grain"] == "d":
            per_day[r["utc_day"]] = per_day.get(r["utc_day"], 0) + int(r["msats"])
    assert per_day == {d["d"]: d["msats"] for d in doc["daily"] if d["msats"]}
    env, b_rows = _rows(_call("nostrology_query_metric", {"metric": "provider_activity", "provider_pubkey": F.PK_B}))
    assert [(r["utc_day"], r["excluded"], int(r["msats"])) for r in b_rows] == [(_day(F.D(3)), "true", 999000)]
    assert f"scope: provider_pubkey={F.PK_B}" in env
    _, none = _rows(_call("nostrology_query_metric", {"metric": "provider_activity", "provider_pubkey": "0" * 64}))
    assert none == []


def test_model_leaderboard_and_model_activity_agree(wired):
    F, doc, now = wired["F"], wired["data"], wired["now"]
    _, board = _rows(_call("nostrology_query_metric", {"metric": "model_leaderboard"}))
    assert [r["model"] for r in board] == [m["model"] for m in doc["models"]]
    for r, m in zip(board, doc["models"]):
        assert (int(r["msats_30d"]), int(r["msats_alltime"]), int(r["providers_30d"])) == (
            m["msats_30d"], m["msats_alltime"], m["providers_30d"])
    _, alltime = _rows(_call("nostrology_query_metric", {"metric": "model_activity", "grain": "total"}))
    _, in30 = _rows(_call("nostrology_query_metric", {
        "metric": "model_activity", "grain": "total", "from": _day(now - 30 * F.DAY), "to": _day(now - F.DAY)}))
    alltime = {r["model"]: r for r in alltime}
    in30 = {r["model"]: r for r in in30}
    for m in doc["models"]:
        assert int(alltime[m["model"]]["msats"]) == m["msats_alltime"], m["model"]
        assert int(in30.get(m["model"], {"msats": 0})["msats"]) == m["msats_30d"], m["model"]
    # per-model day rows for one model, exact match on the identifier
    env, m1 = _rows(_call("nostrology_query_metric", {"metric": "model_activity", "model": "m1"}))
    assert [(r["utc_day"], r["grain"], int(r["msats"]), int(r["providers"])) for r in m1] == [
        (_day(F.THU - 20 * 7 * F.DAY), "w", 50000, 1),
        (_day(F.D(4)), "d", 10000, 1), (_day(F.D(3)), "d", 3000, 1), (_day(F.D(2)), "d", 12000, 1)]
    assert "scope: model=m1" in env
    # and the chart series (top-N + others) is a fold of the same rows
    top = {s["id"]: {p["d"]: p["msats"] for p in s["series"]} for s in doc["model_daily_top"]}
    assert top["m1"] == {_day(F.D(4)): 10000, _day(F.D(3)): 3000, _day(F.D(2)): 12000}


def test_catalog_size_success_rate_and_facets_read_the_document(wired):
    doc = wired["data"]
    _, hist = _rows(_call("nostrology_query_metric", {"metric": "catalog_size"}))
    assert [(r["utc_day"], int(r["catalog_models"])) for r in hist] == [(h["d"], h["n"]) for h in doc["catalog"]["history"]]
    _, (succ,) = _rows(_call("nostrology_query_metric", {"metric": "success_rate"}))
    assert succ["success_rate_30d"] == "\\N" and int(succ["providers_fresh"]) == doc["success"]["providers_fresh"]
    _, facets = _rows(_call("nostrology_query_metric", {"metric": "provider_facets"}))
    want = [("transport", e["k"], e["v"]) for e in doc["transports"]]
    want += [("version", e["k"], e["v"]) for e in doc["versions"]]
    want += [("mint_host", e["k"], e["v"]) for e in doc["mints_top"]]
    assert [(r["facet"], r["value"], int(r["providers"])) for r in facets] == want


# -- the records --------------------------------------------------------------------------


def test_provider_profile_is_the_registry(wired):
    F, state = wired["F"], wired["state"]
    env, rows = _rows(_call("nostrology_query_reference", {"record": "provider_profile"}))
    assert "record: routstr.provider_profile" in env and "scope: all providers" in env
    assert {r["provider_pubkey"] for r in rows} == set(state["providers"])
    b = [r for r in rows if r["provider_pubkey"] == F.PK_B][0]
    assert b["excluded"] == "true" and b["excl_reason"] == "test"
    assert json.loads(b["urls"]) == ["https://b.example.com"]
    a = [r for r in rows if r["provider_pubkey"] == F.PK_A][0]
    assert a["review"] == "avoid" and json.loads(a["mints"]) == ["https://mint1.x", "https://mint2.y"]
    env, one = _rows(_call("nostrology_query_reference", {"record": "provider_profile", "provider_pubkey": F.PK_C}))
    assert [r["provider_name"] for r in one] == ["Gamma"] and f"scope: provider_pubkey={F.PK_C}" in env


def test_provider_reviews_is_the_persisted_timeline(wired):
    F, state = wired["F"], wired["state"]
    _, rows = _rows(_call("nostrology_query_reference", {"record": "provider_reviews"}))
    assert [(r["label"], r["curated"]) for r in rows] == [
        (x["label"], "true" if x["reviewer"] == "curated" else "false") for x in state["reviews"]]
    assert {r["event_id"] for r in rows} == {x["id"] for x in state["reviews"]}
    _, a_rows = _rows(_call("nostrology_query_reference", {"record": "provider_reviews", "provider_pubkey": F.PK_A}))
    assert [r["label"] for r in a_rows] == ["avoid", "lgtm"]
    _, win = _rows(_call("nostrology_query_reference", {
        "record": "provider_reviews", "from": _day(F.D(25)), "to": _day(F.D(15))}))
    assert [r["label"] for r in win] == ["avoid"]


def test_catalog_versions_and_model_catalog_agree_with_the_document(wired):
    F, doc, state = wired["F"], wired["data"], wired["state"]
    _, versions = _rows(_call("nostrology_query_reference", {"record": "catalog_versions"}))
    assert [(int(r["model_count"]), json.loads(r["added"]), json.loads(r["removed"])) for r in versions] == [
        (2, [], ["m3"]), (3, ["m1", "m2", "m3"], [])]
    assert [json.loads(r["models"]) for r in versions] == [["m1", "m2"], ["m1", "m2", "m3"]]
    _, current = _rows(_call("nostrology_query_reference", {"record": "model_catalog"}))
    assert [r["model"] for r in current] == doc["catalog"]["models"]
    assert {r["version_event_id"] for r in current} == {state["catalog_versions"][-1]["id"]}
    _, win = _rows(_call("nostrology_query_reference", {"record": "catalog_versions", "from": _day(F.D(20))}))
    assert [json.loads(r["removed"]) for r in win] == [["m3"]]  # delta vs the version BEFORE the window


def test_exclusions_record_is_the_documents_ledger(wired):
    doc = wired["data"]
    env, rows = _rows(_call("nostrology_query_reference", {"record": "exclusions"}))
    assert "scope: whole domain" in env
    assert [(r["category"], int(r["providers"])) for r in rows] == [(e["k"], e["v"]) for e in doc["exclusions"]]
    assert all(r["why"] for r in rows)


def test_every_routstr_spec_runs_end_to_end(wired):
    """No spec is registered that the scratch schema cannot serve - the catch-all."""
    for spec in ROUTSTR_METRIC_SPECS:
        _call("nostrology_query_metric", {"metric": spec.metric, "domain": "routstr", "grain": spec.grain})
    for spec in ROUTSTR_RECORD_SPECS:
        _call("nostrology_query_reference", {"record": spec.record, "domain": "routstr"})
    assert len(METRICS) >= len(ROUTSTR_METRIC_SPECS)
    assert re.match(r"^\d{4}-\d{2}-\d{2}", wired["data"]["generated_utc"])
