"""Unit tests for pipeline/nectar.py — the Nectar dataset producer.

Pure-logic (no AWS / Aurora / live relay): the DB is a fake cursor, the relay socket is a fake WS,
and the round-trip fetch is injected. The heavier deps (pyarrow / coincurve / websocket-client) are
lazy-imported inside nectar.py, so importing the module is cheap; the tests that exercise them need
those wheels present (they ship in dagster_code/requirements.txt).
"""

import hashlib
import json
import os

import pytest

from dagster_code.pipeline import nectar

# Known-answer vector (locked via a one-off computation). Fixed secret key 0x11*32, a minimal
# single-tag event, content "", created_at 1700000000 → this exact pubkey + id. Guards the NIP-01
# canonical serialization + BIP340 signing against silent drift, independent of the dataset text.
KAT_SECKEY_HEX = "11" * 32
KAT_PUBKEY = "4f355bdcb7cc0af728ef3cceb9615d90684bb5b2ca5f859ab0f0b704075871aa"
KAT_ID = "aaa60f2f6d64a3f50b3e3b0bf4c14444207863950b15b687f12c3959c061325b"
_KAT_TAGS = [["d", "zapstore/native-app-releases-count-daily"]]

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _verify(ev: dict) -> bool:
    """Independent check: recompute the id and verify the schnorr sig against the pubkey."""
    from coincurve import PublicKeyXOnly

    ser = json.dumps(
        [0, ev["pubkey"], ev["created_at"], ev["kind"], ev["tags"], ev["content"]],
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if hashlib.sha256(ser).hexdigest() != ev["id"]:
        return False
    return bool(PublicKeyXOnly(bytes.fromhex(ev["pubkey"])).verify(bytes.fromhex(ev["sig"]), bytes.fromhex(ev["id"])))


# ── SQL / DB wiring ───────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed = (sql, params)

    def fetchall(self):
        return self.rows


class _FakeConn:
    def __init__(self, rows):
        self._cur = _FakeCursor(rows)

    def cursor(self):
        return self._cur


def test_read_sql_carries_classification_and_params():
    sql = nectar.read_sql(_REPO_ROOT)
    assert "native_releases" in sql and "events_kind_30063_app_releases" in sql
    assert "%(curator)s" in sql and "%(own_d_tags)s" in sql
    assert "AT TIME ZONE 'UTC'" in sql  # UTC-explicit day bucket (no session-TZ drift)


def test_query_daily_counts_binds_native_params_and_coerces():
    import datetime

    rows = [(datetime.date(2024, 1, 1), "3"), (datetime.date(2024, 1, 2), 5)]
    conn = _FakeConn(rows)
    out = nectar.query_daily_counts(conn, "SELECT ...")
    assert out == [(datetime.date(2024, 1, 1), 3), (datetime.date(2024, 1, 2), 5)]
    _, params = conn._cur.executed
    assert params == {"curator": nectar.ZAPSTORE_PUBKEY, "own_d_tags": list(nectar.ZAPSTORE_OWN_D_TAGS)}
    assert nectar.ZAPSTORE_PUBKEY == "78ce6faa72264387284e647ba6938995735ec8c7d5c5a65737e55130f026307d"


# ── Parquet build ───────────────────────────────────────────────────────────────────────────────
def test_build_parquet_schema_values_and_determinism():
    import datetime
    import io

    import pyarrow.parquet as pq

    rows = [(datetime.date(2024, 1, 1), 3), (datetime.date(2024, 1, 2), 5)]
    blob = nectar.build_parquet(rows)
    table = pq.read_table(io.BytesIO(blob))
    assert str(table.schema.field("day").type) == "date32[day]"
    assert str(table.schema.field("native_releases").type) == "int64"
    assert table.to_pydict() == {
        "day": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)],
        "native_releases": [3, 5],
    }
    # Deterministic for a fixed input (an unchanged day re-materializes to the same sha256).
    assert nectar.sha256_hex(blob) == nectar.sha256_hex(nectar.build_parquet(rows))


def test_build_parquet_empty_is_valid():
    import io

    import pyarrow.parquet as pq

    blob = nectar.build_parquet([])
    table = pq.read_table(io.BytesIO(blob))
    assert table.num_rows == 0
    assert table.column_names == ["day", "native_releases"]


# ── Blob addressing + round-trip verify ─────────────────────────────────────────────────────────
def test_blob_key_and_url():
    assert nectar.blob_key("deadbeef") == "deadbeef.parquet"
    assert nectar.blob_url("https://blossom.nostrolo.gy/", "abc") == "https://blossom.nostrolo.gy/abc.parquet"
    assert nectar.blob_url("http://127.0.0.1:8000", "abc") == "http://127.0.0.1:8000/abc.parquet"


def test_verify_roundtrip_match_and_mismatch():
    payload = b"parquet-bytes"
    good = nectar.sha256_hex(payload)
    ok, actual = nectar.verify_roundtrip("http://x/y.parquet", good, fetch=lambda url, timeout: payload)
    assert ok and actual == good
    bad_ok, bad_actual = nectar.verify_roundtrip("http://x/y.parquet", "0" * 64, fetch=lambda url, timeout: payload)
    assert not bad_ok and bad_actual == good


# ── Event construction + signing ────────────────────────────────────────────────────────────────
def test_sign_event_known_answer():
    ev = nectar.sign_event(bytes.fromhex(KAT_SECKEY_HEX), 30800, _KAT_TAGS, "", 1700000000)
    assert ev["pubkey"] == KAT_PUBKEY
    assert ev["id"] == KAT_ID
    assert _verify(ev)


def test_serialize_matches_nip01_and_id():
    tags = [["spec", "nectar/0"]]
    eid = nectar.compute_event_id(KAT_PUBKEY, 123, 30800, tags, "")
    ser = json.dumps([0, KAT_PUBKEY, 123, 30800, tags, ""], separators=(",", ":"), ensure_ascii=False)
    assert eid == hashlib.sha256(ser.encode("utf-8")).hexdigest()


def test_parse_seckey_hex_and_nsec():
    import bech32

    sk = bytes.fromhex(KAT_SECKEY_HEX)
    assert nectar.parse_seckey(KAT_SECKEY_HEX) == sk
    nsec = bech32.bech32_encode("nsec", bech32.convertbits(sk, 8, 5))
    assert nectar.parse_seckey(nsec) == sk
    with pytest.raises(ValueError):
        nectar.parse_seckey("npub1garbage")


def test_build_tags_order_and_shape():
    tags = nectar.build_tags("thehash", "https://blossom.nostrolo.gy/thehash.parquet")
    assert [t[0] for t in tags] == ["spec", "d", "title", "description", "part"]
    assert tags[0] == ["spec", "nectar/0"]
    assert tags[1] == ["d", "zapstore/native-app-releases-count-daily"]
    assert tags[-1] == ["part", "0", "https://blossom.nostrolo.gy/thehash.parquet", "thehash"]


def test_build_signed_manifest_is_valid_and_addressable():
    import bech32

    nsec = bech32.bech32_encode("nsec", bech32.convertbits(bytes.fromhex(KAT_SECKEY_HEX), 8, 5))
    ev = nectar.build_signed_manifest(nsec, "abc123", "https://blossom.nostrolo.gy/abc123.parquet", created_at=1700000000)
    assert ev["kind"] == 30800 and ev["content"] == ""
    assert ev["pubkey"] == KAT_PUBKEY
    assert _verify(ev)
    assert nectar.coordinate(ev["pubkey"]) == f"30800:{KAT_PUBKEY}:zapstore/native-app-releases-count-daily"
    # part tag carries the url + hash we passed
    part = [t for t in ev["tags"] if t[0] == "part"][0]
    assert part == ["part", "0", "https://blossom.nostrolo.gy/abc123.parquet", "abc123"]


def test_build_signed_manifest_defaults_created_at_to_now():
    import bech32
    import datetime

    nsec = bech32.bech32_encode("nsec", bech32.convertbits(bytes.fromhex(KAT_SECKEY_HEX), 8, 5))
    before = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    ev = nectar.build_signed_manifest(nsec, "abc", "http://x/abc.parquet")
    assert before <= ev["created_at"] <= before + 5


# ── Relay publish (fake WS) ─────────────────────────────────────────────────────────────────────
class _FakeWS:
    def __init__(self, script):
        self.sent = []
        self._script = list(script)

    def send(self, data):
        self.sent.append(data)

    def settimeout(self, _t):
        pass

    def recv(self):
        if not self._script:
            raise TimeoutError("recv timeout")
        return self._script.pop(0)

    def close(self):
        pass


def _patch_ws(monkeypatch, ws):
    import websocket

    monkeypatch.setattr(websocket, "create_connection", lambda url, timeout=None: ws)


def _ok(event_id, accepted, msg=""):
    return json.dumps(["OK", event_id, accepted, msg])


def test_publish_to_relay_accepted(monkeypatch):
    ev = {"id": "eid1"}
    ws = _FakeWS([json.dumps(["NOTICE", "hi"]), _ok("other", True), _ok("eid1", True, "saved")])
    _patch_ws(monkeypatch, ws)
    accepted, msg = nectar.publish_to_relay("wss://r", ev, timeout=1.0)
    assert accepted and msg == "saved"
    assert json.loads(ws.sent[0]) == ["EVENT", ev]  # published our event


def test_publish_to_relay_rejected(monkeypatch):
    ev = {"id": "eid2"}
    _patch_ws(monkeypatch, _FakeWS([_ok("eid2", False, "blocked: pow")]))
    accepted, msg = nectar.publish_to_relay("wss://r", ev, timeout=1.0)
    assert not accepted and "blocked" in msg


def test_publish_to_relay_timeout(monkeypatch):
    _patch_ws(monkeypatch, _FakeWS([]))  # no messages → recv raises
    accepted, msg = nectar.publish_to_relay("wss://r", {"id": "eid3"}, timeout=1.0)
    assert not accepted and "no OK" in msg


def test_publish_to_relays_aggregates_and_isolates_failures(monkeypatch):
    import websocket

    ev = {"id": "eid4"}

    def factory(url, timeout=None):
        if "bad" in url:
            raise OSError("connection refused")
        return _FakeWS([_ok("eid4", True)])

    monkeypatch.setattr(websocket, "create_connection", factory)
    results = nectar.publish_to_relays(ev, ["wss://good", "wss://bad"], timeout=1.0)
    assert results["wss://good"]["accepted"] is True
    assert results["wss://bad"]["accepted"] is False and "error:" in results["wss://bad"]["message"]


# ── Handoff pointer ────────────────────────────────────────────────────────────────────────────
def test_pointer_write_read_roundtrip(tmp_path):
    art = nectar.DatasetArtifact(parquet=b"xyz", sha256="abc", rows=42, day_min="2020-03-01", day_max="2026-07-23")
    state_dir = str(tmp_path)
    path = nectar.write_pointer(state_dir, art, "https://blossom.nostrolo.gy/abc.parquet", built_at=1700000000)
    assert os.path.exists(path)
    ptr = nectar.read_pointer(state_dir)
    assert ptr == {
        "sha256": "abc",
        "url": "https://blossom.nostrolo.gy/abc.parquet",
        "rows": 42,
        "day_min": "2020-03-01",
        "day_max": "2026-07-23",
        "bytes": 3,
        "built_at": 1700000000,
    }
