"""Unit tests for the Routstr battery's harness pieces - the parts that turn raw
fetches into comparable values. The battery itself runs against live systems
(`routstr_run.py`); these pin the deterministic plumbing so a green run means
the systems agreed, not that the harness compared nothing."""
import json
import shutil
import subprocess

import pytest

from nostrology_mcp.verification import routstr_plan, routstr_questions, routstr_run, routstr_truth

# ---------------------------------------------------------------------------
# a tiny substrate fixture: two included providers, one excluded, one weekly bucket
PK_A, PK_B, PK_X = "a" * 64, "b" * 64, "c" * 64


def _sql():
    doc = {
        "generated_utc": "2026-08-29 22:36:41Z",
        "providers": [{"id": PK_A[:12]}, {"id": PK_B[:12]}, {"id": PK_X[:12]}],
        "models": [{"model": "m1", "msats_alltime": 250}, {"model": "(other)"}],
    }
    return {
        "DOC": [{"data": doc, "run_id": "r", "generated": "2026-08-29T22:36:41Z"}],
        "PROVIDERS": [
            {"pubkey": PK_A, "name": "A", "excluded": False, "excl_reason": None, "announced": True,
             "endpoint_class": "both", "version": "1", "review": None, "msats_alltime": 300, "mints": ["https://m.one/"]},
            {"pubkey": PK_B, "name": "B", "excluded": False, "excl_reason": None, "announced": True,
             "endpoint_class": "onion", "version": "1", "review": "lgtm", "msats_alltime": 50, "mints": ["m.one", "https://m.two"]},  # "m.one" is scheme-less: no host, not counted (the pipeline's rule)
            {"pubkey": PK_X, "name": "X test", "excluded": True, "excl_reason": "test", "announced": True,
             "endpoint_class": "clearnet", "version": "", "review": None, "msats_alltime": 0, "mints": []},
        ],
        "PROVIDER_DAYS": [
            {"pubkey": PK_A, "grain": "w", "day": "2026-06-04", "msats": 100, "requests": 1, "tokens": 10},
            {"pubkey": PK_A, "grain": "d", "day": "2026-08-20", "msats": 150, "requests": 2, "tokens": 20},
            {"pubkey": PK_A, "grain": "d", "day": "2026-08-21", "msats": 50, "requests": 1, "tokens": 5},
            {"pubkey": PK_B, "grain": "d", "day": "2026-08-21", "msats": 0, "requests": 0, "tokens": 0},
            {"pubkey": PK_B, "grain": "d", "day": "2026-08-22", "msats": 50, "requests": 3, "tokens": 30},
            {"pubkey": PK_X, "grain": "d", "day": "2026-08-22", "msats": 999, "requests": 9, "tokens": 90},
        ],
        "MODEL_DAYS": [
            {"model": "m1", "grain": "d", "day": "2026-08-20", "msats": 150, "requests": 2, "tokens": 20, "providers": 1},
            {"model": "m2", "grain": "d", "day": "2026-08-22", "msats": 50, "requests": 3, "tokens": 30, "providers": 1},
            {"model": "(other)", "grain": "d", "day": "2026-08-21", "msats": 50, "requests": 1, "tokens": 5, "providers": 1},
            {"model": "m1", "grain": "w", "day": "2026-06-04", "msats": 100, "requests": 1, "tokens": 10, "providers": 1},
        ],
        "CATALOG_VERSIONS": [
            {"event_id": "e2", "day": "2026-08-10", "model_count": 2, "models": ["a", "c"], "added": ["c"], "removed": ["b"]},
            {"event_id": "e1", "day": "2026-08-01", "model_count": 2, "models": ["a", "b"], "added": ["a", "b"], "removed": []},
        ],
        "REVIEWS": [
            # r2 and r1 are the SAME day; r1 has the lexically greater id but the earlier time
            {"event_id": "r2", "node": PK_B, "label": "lgtm", "day": "2026-08-05", "at": 1786000500, "reviewer_pubkey": "d" * 64, "curated": True},
            {"event_id": "r9", "node": PK_B, "label": "avoid", "day": "2026-08-05", "at": 1786000100, "reviewer_pubkey": "d" * 64, "curated": True},
            {"event_id": "r0", "node": PK_A, "label": "great", "day": "2026-08-01", "at": 1785600000, "reviewer_pubkey": "e" * 64, "curated": False},
        ],
    }


def test_truth_totals_follow_the_grain_rule():
    T = routstr_truth.Truth(_sql())
    assert T.included == {PK_A, PK_B}
    # unbounded: both grains; the excluded provider never enters unless asked
    assert T.provider_totals()[PK_A] == (300, 4, 35, 2, 1, "2026-06-04", "2026-08-21")
    assert PK_X not in T.provider_totals() and PK_X in T.provider_totals(include_excluded=True)
    # a bound window that CONTAINS the weekly bucket's start day still excludes it
    assert T.provider_totals("2026-06-01", "2026-08-31")[PK_A] == (200, 3, 25, 2, 0, "2026-08-20", "2026-08-21")
    assert T.model_totals()["m1"] == (250, 3, 30, 1, 1)
    assert T.model_totals("2026-08-01", "2026-08-31")["m1"] == (150, 2, 20, 1, 0)


def test_truth_network_days_counts_present_providers_even_at_zero():
    T = routstr_truth.Truth(_sql())
    nd = T.network_days()
    assert nd["2026-08-21"] == (50, 1, 5, 2)  # B reported a zero day: present, counted
    assert nd["2026-08-22"] == (50, 3, 30, 1)  # X is excluded: its 999 never enters
    assert list(nd) == ["2026-08-20", "2026-08-21", "2026-08-22"]  # dense between first and last daily day
    assert T.network_sum("2026-08-21", "2026-08-22") == (100, 4, 35)


def test_truth_by_prefix_and_ranked():
    T = routstr_truth.Truth(_sql())
    assert T.by_prefix("a" * 12) == PK_A
    assert T.by_prefix("zz") is None
    assert routstr_truth.ranked(T.provider_totals(), 2) == [(PK_A, 35), (PK_B, 30)]


def test_question_helpers():
    assert routstr_questions.sats(1499) == 1 and routstr_questions.sats(1500) == 2 and routstr_questions.sats(-1500) == -2
    assert routstr_questions.I("\\N") is None and routstr_questions.I("7") == 7
    assert routstr_questions.argmax([("d1", 5), ("d2", 5), ("d0", 4)]) == ("d2", 5)
    T = routstr_truth.Truth(_sql())
    assert routstr_questions._catalog_deltas(T) == [("e2", ["c"], ["b"]), ("e1", ["a", "b"], [])]
    assert routstr_questions._newest_curated(T) == {PK_B: "lgtm"}
    assert routstr_questions._mint_hosts(T) == {"m.one": 1, "m.two": 1}
    assert routstr_questions._endpoint_classes(T) == {"both": 1, "onion": 1, "clearnet": 1}


def test_context_derives_every_subject_from_the_data():
    ctx = routstr_run.build_context(_sql(), None, "2026-08-30")
    assert ctx["refresh_day"] == "2026-08-29"
    assert (ctx["refresh_30d_from"], ctx["refresh_30d_to"]) == ("2026-07-30", "2026-08-28")
    assert (ctx["last7_from"], ctx["last7_to"], ctx["month_from"]) == ("2026-08-23", "2026-08-29", "2026-08-01")
    assert ctx["top_provider"] == PK_A
    assert ctx["excluded_provider"] == PK_X
    assert ctx["top_model"] == "m1"
    assert ctx["has_folded_model"] is True
    assert ctx["folded_model"] == "m2"  # never "(other)", never a named row
    assert ctx["reviewed_node"] == PK_B
    assert ctx["reference_diffs"] == ["reference missing"]
    steps = routstr_plan.build(ctx)
    assert len({s["id"] for s in steps}) == len(steps)
    assert all(s["id"].startswith("RS") for s in steps)  # no collision with the Zapstore plan's ids


def test_diff_documents_is_exact_but_numeric_identity_tolerant():
    d = routstr_truth.diff_documents
    assert d({"a": 5}, {"a": 5.0}) == []
    assert d({"a": [1, 2]}, {"a": [1, 3]}) == ["$.a[1]: 2 vs 3"]
    assert d({"a": 1}, {"b": 1}) == ["$.a: only in reference", "$.b: only in served"]
    assert d({"s": [1]}, {"s": [1, 2]}) == ["$.s: length 1 vs 2"]


def test_page_document_evaluates_the_sveltekit_literal():
    if shutil.which("node") is None:  # subprocess.run raises on a missing binary, so it cannot be the test
        pytest.skip("node not available")
    html = ('<script>x={data:[{type:"data",data:{daily:[{d:"2026-08-28",msats:1}],'
            'success:{rate_30d:.9721,note:"a } inside a string"},providers:[{id:"abc",name:"O\'Brien"}]}}]}</script>')
    doc = routstr_truth.page_document(html)
    assert doc["daily"] == [{"d": "2026-08-28", "msats": 1}]
    assert doc["success"]["rate_30d"] == 0.9721 and "}" in doc["success"]["note"]
    assert doc["providers"][0]["name"] == "O'Brien"
    assert routstr_truth.page_document("<html>no payload</html>") is None


def test_run_delegates_domain_routstr(monkeypatch):
    from nostrology_mcp.verification import run as run_mod
    seen = {}
    monkeypatch.setattr(routstr_run, "main", lambda argv: (seen.__setitem__("argv", argv), 0)[1])
    assert run_mod.main(["--domain", "routstr", "--reuse", "--self-test"]) == 0
    assert seen["argv"] == ["--reuse", "--self-test"]


def test_network_days_zero_fills_gaps():
    sql = _sql()
    sql["PROVIDER_DAYS"].append({"pubkey": PK_A, "grain": "d", "day": "2026-08-25", "msats": 7, "requests": 1, "tokens": 1})
    nd = routstr_truth.Truth(sql).network_days()
    assert nd["2026-08-23"] == (0, 0, 0, 0) and nd["2026-08-24"] == (0, 0, 0, 0) and nd["2026-08-25"] == (7, 1, 1, 1)


def test_load_reference_requires_a_matching_provenance_sidecar(tmp_path):
    p = str(tmp_path / "reference.json")
    json.dump({"generated_utc": "2026-08-29 22:36:41Z"}, open(p, "w"))
    assert routstr_truth.load_reference(p, "2026-08-29 22:36:41Z") is None  # no sidecar: a hand-placed file
    json.dump({"as_of": "2026-08-29T22:36:41", "ingested_before": None}, open(p + ".meta.json", "w"))
    assert routstr_truth.load_reference(p, "2026-08-29 22:36:41Z") is None  # unpinned archive state
    json.dump({"as_of": "2026-08-29T22:36:41", "ingested_before": "2026-08-29T22:36:41"}, open(p + ".meta.json", "w"))
    assert routstr_truth.load_reference(p, "2026-08-29 22:36:41Z") == {"generated_utc": "2026-08-29 22:36:41Z"}
    assert routstr_truth.load_reference(p, "2026-08-30 22:31:00Z") is None  # the nightly moved
