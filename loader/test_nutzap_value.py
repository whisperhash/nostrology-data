"""kind-9321 (nutzaps) value: the proofs in `proof` tags, nothing else (nostrology-data#8).

Run:  .venv-mcp/bin/python -m pytest loader/test_nutzap_value.py -q
      (needs psycopg2 importable, because the loader imports it at module load)

Three tiers, no database:

  1. REAL EVENTS. loader/fixtures/nutzap_wire_families.json holds one archived event
     per wire family, verbatim. Each one's value, value_source and no_value_reason are
     pinned, next to the amount_sats the serving table held before #8, so the diff this
     change makes is visible per family. The fixtures are proved verbatim by
     recomputing each NIP-01 (basic protocol) event id.
  2. THE RULE AS A PROPERTY. Rewriting or deleting the `amount` and `unit` tags of any
     fixture never moves its value: nothing but `proof` tags is read.
  3. PARSER EDGES. Amount forms (integer, decimal string, float, bool, sign, exponent,
     non-ASCII digits, zero, negative, beyond all bitcoin), proof-object shape, tokens,
     duplicates, and junk tags.

Plus the contract between the loader, schema/init.sql and schema/nutzap_active_users.sql.
"""

import copy
import hashlib
import json
import os
import re
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _HERE)

import load_jsonl_exports_into_postgres as L  # noqa: E402

_FIXTURES = os.path.join(_HERE, "fixtures", "nutzap_wire_families.json")
with open(_FIXTURES, encoding="utf-8") as _fh:
    FIXTURES = json.load(_fh)["fixtures"]
FAMILIES = [f["family"] for f in FIXTURES]


def _fixture(family):
    return next(f for f in FIXTURES if f["family"] == family)


def _proof(amount=1, secret="s1", C="02" + "ab" * 32, kid="00ad268c4d1f5826"):
    return json.dumps({"amount": amount, "id": kid, "secret": secret, "C": C})


def _event(tags, content=""):
    return {"id": "f" * 64, "pubkey": "a" * 64, "created_at": 1_750_000_000,
            "kind": 9321, "tags": tags, "content": content}


# -- 1. real events -----------------------------------------------------------------------

def _nip01_id(e):
    serialized = json.dumps([0, e["pubkey"], e["created_at"], e["kind"], e["tags"], e["content"]],
                            separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("family", FAMILIES)
def test_fixture_is_the_verbatim_archived_event(family):
    """A hand-edited fixture would test a wire format nobody sends."""
    e = _fixture(family)["event"]
    assert e["kind"] == 9321
    assert _nip01_id(e) == e["id"]


@pytest.mark.parametrize("family", FAMILIES)
def test_real_event_value(family):
    f = _fixture(family)
    v = L._nutzap_value(f["event"])
    want = f["expected"]
    assert (v.amount_sats, v.value_source, v.no_value_reason) == (
        want["amount_sats"], want["value_source"], want["no_value_reason"])
    # A payment is exactly an event with counted proofs.
    assert (v.value_source == "proof_tags") == (v.proofs_counted > 0)
    assert (v.value_source == "none") == (v.amount_sats == 0)


def test_the_phantom_families_lose_their_tag_value():
    """The mechanisms #8 names, each served from the amount tag before the fix."""
    phantom = {"draft-content": 1000, "encrypted": 76000, "cashuA-in-proof-tag": 5000,
               "cashuB-in-proof-tag": 50000}
    for family, served in phantom.items():
        f = _fixture(family)
        assert f["served_before_fix"] == served
        assert L._nutzap_value(f["event"]).amount_sats == 0


def test_proof_backed_families_keep_their_value():
    """Where proof tags parsed, the old value was already the proof sum; #8 must not
    move it, including the lnuts event whose amount tag is 1000x while saying sat."""
    for f in FIXTURES:
        if f["expected"]["value_source"] == "proof_tags":
            assert f["served_before_fix"] == f["expected"]["amount_sats"], f["family"]


def test_every_family_of_the_issue_is_covered():
    reasons = {f["expected"]["no_value_reason"] for f in FIXTURES}
    assert {"draft-content", "encrypted", "cashu-token-in-proof-tag", "content-proofs",
            "content-token", "zero-ecash"} <= reasons
    assert sum(f["expected"]["value_source"] == "proof_tags" for f in FIXTURES) >= 5


# -- 2. the rule as a property ------------------------------------------------------------

_TAG_REWRITES = [
    ("drop amount+unit", lambda tags: [t for t in tags if t[0] not in ("amount", "unit")]),
    ("amount huge", lambda tags: [t for t in tags if t[0] != "amount"] + [["amount", "99999999"]]),
    ("amount msats[2]", lambda tags: [t for t in tags if t[0] != "amount"] + [["amount", "5000", "msats"]]),
    ("unit msat", lambda tags: [t for t in tags if t[0] != "unit"] + [["unit", "msat"]]),
    ("unit usd", lambda tags: [t for t in tags if t[0] != "unit"] + [["unit", "usd"]]),
    ("amount junk", lambda tags: [t for t in tags if t[0] != "amount"] + [["amount", "lots"]]),
]


@pytest.mark.parametrize("family", FAMILIES)
@pytest.mark.parametrize("name,rewrite", _TAG_REWRITES, ids=[n for n, _ in _TAG_REWRITES])
def test_amount_and_unit_tags_never_move_the_value(family, name, rewrite):
    e = copy.deepcopy(_fixture(family)["event"])
    before = L._nutzap_value(e).amount_sats
    e["tags"] = rewrite(e["tags"])
    assert L._nutzap_value(e).amount_sats == before


# -- 3. parser edges ----------------------------------------------------------------------

@pytest.mark.parametrize("raw,want", [
    (1, 1), (64, 64), ("1", 1), ("21", 21), ("007", 7),
    (L._MAX_PLAUSIBLE_SATS, L._MAX_PLAUSIBLE_SATS),
    (0, None), ("0", None), (-1, None), ("-1", None),
    (1.0, None), (1.5, None), ("1.5", None), ("1e3", None), ("+1", None), (" 1", None),
    ("", None), (None, None), (True, None), (False, None), ([1], None), ({"v": 1}, None),
    (chr(0x661), None),  # ARABIC-INDIC DIGIT ONE: str.isdigit() is True, but not ASCII
    (L._MAX_PLAUSIBLE_SATS + 1, None), (str(2 ** 64), None),
    # int() raises on strings over 4,300 digits (Python 3.11+); a hostile event must
    # get None, not an exception that aborts the load after its TRUNCATE.
    pytest.param("1" * 5000, None, id="str-5000-digits"),
    pytest.param("0" * 4999 + "1", None, id="str-5000-digits-leading-zeros"),
    pytest.param(10 ** 5000, None, id="int-5001-digits"),
])
def test_cashu_proof_amount(raw, want):
    assert L._cashu_proof_amount(raw) == want


def test_decimal_string_and_integer_amounts_sum():
    e = _event([["u", "https://m"], ["proof", _proof(2, "a")], ["proof", _proof("8", "b")]])
    assert L._nutzap_value(e) == L.NutzapValue(10, "proof_tags", None, 2, 0)


def test_duplicate_proof_tags_count_once():
    e = _event([["proof", _proof(4, "a")], ["proof", _proof(4, "a")], ["proof", _proof(4, "b")]])
    v = L._nutzap_value(e)
    assert (v.amount_sats, v.proofs_counted, v.proofs_rejected) == (8, 2, 1)


def test_same_secret_different_signature_is_a_different_proof():
    e = _event([["proof", _proof(4, "a", C="02" + "11" * 32)],
                ["proof", _proof(4, "a", C="02" + "22" * 32)]])
    assert L._nutzap_value(e).amount_sats == 8


def test_bad_proofs_beside_good_ones_are_skipped_not_fatal():
    e = _event([["proof", _proof(-4, "a")], ["proof", "not json"], ["proof", _proof(2, "b")],
                ["proof", _proof(1.5, "c")], ["proof", "[1, 2]"]])
    v = L._nutzap_value(e)
    assert (v.amount_sats, v.value_source, v.proofs_counted, v.proofs_rejected) == (2, "proof_tags", 1, 4)


@pytest.mark.parametrize("tags", [
    [["proof", _proof(-4)]],
    [["proof", _proof(0)]],
    [["proof", "not json"]],
    [["proof", "[1, 2]"]],
    [["proof", "42"]],
    [["proof", json.dumps({"amount": 4, "secret": "s", "C": "c"})]],           # no keyset id
    [["proof", json.dumps({"amount": 4, "id": "k", "C": "c"})]],               # no secret
    [["proof", json.dumps({"amount": 4, "id": "k", "secret": "", "C": "c"})]],  # empty secret
    [["proof", json.dumps({"amount": 4, "id": "k", "secret": "s", "C": 7})]],   # C not a string
])
def test_only_invalid_proof_tags_is_no_value(tags):
    v = L._nutzap_value(_event([["u", "https://m"], ["amount", "4"]] + tags))
    assert (v.amount_sats, v.value_source, v.no_value_reason) == (0, "none", "invalid-proof-tag")


def test_cashu_token_in_proof_tag():
    tok = "cashuB" + "o2FteBtodHRwczovL3Rlc3RudXQuY2FzaH" * 2
    v = L._nutzap_value(_event([["proof", tok], ["amount", "5000"]]))
    assert (v.amount_sats, v.no_value_reason) == (0, "cashu-token-in-proof-tag")


def test_implausible_sum_is_no_value_not_an_overflow():
    """Each proof under the cap, the sum over it: 0, never a BIGINT overflow in COPY."""
    big = L._MAX_PLAUSIBLE_SATS
    e = _event([["proof", _proof(big, "a")], ["proof", _proof(big, "b")]])
    v = L._nutzap_value(e)
    assert (v.amount_sats, v.value_source, v.no_value_reason) == (0, "none", "implausible-amount")


def test_valid_proofs_win_over_an_encryption_tag():
    v = L._nutzap_value(_event([["encryption", "nip44"], ["proof", _proof(2)]]))
    assert (v.amount_sats, v.value_source) == (2, "proof_tags")


@pytest.mark.parametrize("event,reason", [
    (_event([["amount", "0"]], content="yay"), "zero-ecash"),
    (_event([["amount", "21"]], content="yay"), "no-proof"),
    (_event([], content=""), "no-proof"),
    (_event([["amount", "1000", "msat"]], content=json.dumps([{"amount": 1}])), "draft-content"),
    (_event([["amount", "1"]], content=json.dumps([{"amount": 1}])), "content-proofs"),
    (_event([["amount", "1"]], content="[]"), "no-proof"),
    (_event([], content="thanks! cashuA" + "eyJ0b2tlbiI6W3sibWludCI6" * 2), "content-token"),
])
def test_no_value_reasons(event, reason):
    v = L._nutzap_value(event)
    assert (v.amount_sats, v.value_source, v.no_value_reason) == (0, "none", reason)


@pytest.mark.parametrize("tags", [
    None, [], [None, 7, "proof", []], [["proof"]], [["proof", None]],
    [[["x"], "y"]], [[{"k": 1}, "y"]],                      # unhashable tag names
    [["proof", json.dumps({"amount": "1" * 5000, "id": "k", "secret": "s", "C": "c"})]],
    [["proof", "[" * 100000]],                                 # nesting past the JSON parser's depth
])
def test_junk_tags_never_raise(tags):
    v = L._nutzap_value({"id": "x", "tags": tags, "content": None})
    assert (v.amount_sats, v.value_source) == (0, "none")


@pytest.mark.parametrize("raw,want", [
    ("ab" * 32, "ab" * 32), ("AB" * 32, "ab" * 32),
    ("ab" * 32 + "c", None), ("ab" * 31, None), ("ab" * 31 + "zz", None),
    ("ab" * 31 + "a\x00", None), (None, None), (7, None), (["ab" * 32], None),
])
def test_hex64_or_none(raw, want):
    """`p`/`e` tag values reach CHAR(64) columns: anything else must become NULL, not
    fail the COPY after the TRUNCATE."""
    assert L._hex64_or_none(raw) == want


@pytest.mark.parametrize("normalize,scheme", [
    (L._normalize_mint_url, "https://"), (L._normalize_relay_url, "wss://"),
])
def test_urls_a_column_or_index_cannot_hold_are_dropped(normalize, scheme):
    """An incompressible URL over _MAX_URL_BYTES would fail the btree/GIN index rebuild
    (entries max out near 2,700 bytes) and then init.sql on every surface, every night."""
    base = scheme + "m.example/"
    at_cap = base + "a" * (L._MAX_URL_BYTES - len(base))
    assert normalize(at_cap) == at_cap
    assert normalize(at_cap + "a") is None
    # The cap counts UTF-8 bytes, not characters.
    assert normalize(base + "\u00e9" * (L._MAX_URL_BYTES // 2)) is None
    assert normalize(base + "x" + chr(0xD83C)) is None
    assert normalize(base + "x\x00") is None


def test_a_nul_in_a_mint_url_is_no_mint():
    assert L._normalize_mint_url("https://mint.example\x00") is None
    assert L._normalize_mint_url("https://mint.example" + chr(0xD83C)) is None  # lone surrogate
    assert L._normalize_mint_url("https://mint.example/") == "https://mint.example"


def test_every_reason_is_declared_and_reachable():
    """Every reason is produced by SOME input, derived here, never asserted by hand."""
    big = L._MAX_PLAUSIBLE_SATS
    synthetic = [
        _event([["proof", _proof(big, "a")], ["proof", _proof(big, "b")]]),  # implausible-amount
        _event([["proof", "not json"]]),                                     # invalid-proof-tag
        _event([["amount", "21"]], content="hi"),                           # no-proof
    ]
    seen = {L._nutzap_value(e).no_value_reason for e in [f["event"] for f in FIXTURES] + synthetic}
    seen.discard(None)
    assert seen == set(L.NUTZAP_NO_VALUE_REASONS)


# -- contract with the schema -------------------------------------------------------------

def _read(*parts):
    with open(os.path.join(_REPO_ROOT, *parts), encoding="utf-8") as fh:
        return fh.read()


def test_schema_accepts_every_value_source_the_loader_writes():
    init_sql = _read("schema", "init.sql")
    checks = re.findall(r"CHECK \(value_source IN \(([^)]*)\)\)", init_sql)
    assert len(checks) == 2, "the CREATE TABLE and the ALTER must both carry the CHECK"
    for c in checks:
        allowed = {s.strip().strip("'") for s in c.split(",")}
        assert allowed == set(L.NUTZAP_VALUE_SOURCES) | {"unclassified"}


def test_copy_columns_exist_in_the_schema():
    init_sql = _read("schema", "init.sql")
    block = re.search(r"CREATE TABLE IF NOT EXISTS events_kind_9321_nutzaps \((.*?)\);", init_sql, re.S).group(1)
    declared = set(re.findall(r"^\s*([a-z_]+)\s+(?:CHAR|BIGINT|TEXT)\b", block, re.M))
    src = _read("loader", "load_jsonl_exports_into_postgres.py")
    cols = re.search(r'"events_kind_9321_nutzaps",\s*\[(.*?)\]', src, re.S).group(1)
    copied = set(re.findall(r'"([a-z_]+)"', cols))
    assert copied == declared
    for col in ("value_source", "no_value_reason"):
        assert re.search(rf"ADD COLUMN IF NOT EXISTS {col} ", init_sql), col


def test_active_users_rebuild_counts_payments_only():
    sql = _read("schema", "nutzap_active_users.sql")
    assert sql.count("FROM events_kind_9321_nutzaps") == 3
    assert sql.count("WHERE value_source = 'proof_tags'") == 3
