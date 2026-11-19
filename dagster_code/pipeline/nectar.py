"""Nectar dataset publishing — the producer side of the kind-30800 dataset-manifest.

Native/pure like the rest of pipeline/*: DB, S3, and relay I/O are injected or done through
thin wrappers, so classification, Parquet build, sha256, event signing, and round-trip verify
all unit-test WITHOUT AWS / Aurora / a live relay. The Dagster assets (../assets.py) are thin.

Signing is IN-PROCESS — the NIP-01 event id (sha256 of the canonical serialization) is signed
with BIP340 schnorr via coincurve. The nsec never touches a subprocess argv/env (any account on
the host can read /proc cmdlines), unlike `nak --sec`. Publishing is a minimal
websocket EVENT→OK exchange, bounded by a timeout so a hung relay never wedges the run.

Design note: single Parquet "part" for v0 (the whole file), but the wire format is the
multi-part `["part", <index>, <url>, <sha256>]` shape from day one. Consumers union-scan the
parts; a one-part dataset is one part.
"""

import hashlib
import io
import json
import os
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

# ── Native classification (mirrors nostrology-web postgres.ts:1217,1225) ──────────────────────
# A kind-30063 release is "native" UNLESS signed by the Zapstore curator key AND not one of
# Zapstore's own first-party apps. Equivalently native = (pubkey <> curator) OR (d in own).
ZAPSTORE_PUBKEY = "78ce6faa72264387284e647ba6938995735ec8c7d5c5a65737e55130f026307d"
ZAPSTORE_OWN_D_TAGS = ["dev.zapstore.app", "dev.zapstore.alpha", "zapstore"]

# ── Wire constants (the dataset coordinate + manifest metadata) ───────────────────────────────
NECTAR_SPEC = "nectar/0"  # spec tag: major only
DATASET_KIND = 30800  # provisional addressable kind
DATASET_D_TAG = "zapstore/native-app-releases-count-daily"  # <domain>/<subject>-<measure>-<grain>
DATASET_TITLE = "Daily count of Zapstore-native app releases (Nostr kind 30063)"
DATASET_DESCRIPTION = (
    "One row per UTC day (sparse: only days with at least one release) giving the count of "
    "Zapstore-native app-release events (Nostr kind 30063) published that day. 'Native' means "
    "every release except those signed by the Zapstore curator key that are not one of Zapstore's "
    "own first-party apps — the same classification as the native series in Nostrology's App "
    "Releases chart. Full release history (the chart shows only the trailing 365 days). Columns: "
    "day (DATE), native_releases (BIGINT). Format: Apache Parquet, zstd-compressed."
)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
DEFAULT_BLOSSOM_BASE = "https://blossom.nostrolo.gy"
DEFAULT_RELAYS = ("wss://relay.damus.io", "wss://nos.lol")

_SQL_REL = "sql/zapstore_native_releases_daily.sql"


# ── Artifact build ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class DatasetArtifact:
    parquet: bytes
    sha256: str
    rows: int
    day_min: str  # ISO date, or "" when empty
    day_max: str


def read_sql(repo_root: str) -> str:
    with open(os.path.join(repo_root, _SQL_REL)) as fh:
        return fh.read()


def query_daily_counts(conn, sql: str) -> list[tuple]:
    """Run the native-classification SQL; return [(datetime.date, int)] sorted by day ascending."""
    with conn.cursor() as cur:
        cur.execute(sql, {"curator": ZAPSTORE_PUBKEY, "own_d_tags": list(ZAPSTORE_OWN_D_TAGS)})
        return [(r[0], int(r[1])) for r in cur.fetchall()]


def build_parquet(rows: Sequence[tuple]) -> bytes:
    """rows = [(datetime.date, int)] -> Parquet bytes (day: date32, native_releases: int64, zstd).

    Deterministic for a fixed input within a fixed pyarrow version (the only footer variability is
    pyarrow's pinned `created_by` string) — so an unchanged day re-materializes to the same sha256.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "day": pa.array([r[0] for r in rows], type=pa.date32()),
            "native_releases": pa.array([int(r[1]) for r in rows], type=pa.int64()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_artifact(conn, repo_root: str) -> DatasetArtifact:
    """Query → Parquet → sha256, in one shot. `conn` is a live psycopg2 connection (integration-side)."""
    rows = query_daily_counts(conn, read_sql(repo_root))
    parquet = build_parquet(rows)
    return DatasetArtifact(
        parquet=parquet,
        sha256=sha256_hex(parquet),
        rows=len(rows),
        day_min=rows[0][0].isoformat() if rows else "",
        day_max=rows[-1][0].isoformat() if rows else "",
    )


# ── Blob storage (S3 put + round-trip verify) ────────────────────────────────────────────────────
def blob_key(sha256: str) -> str:
    return f"{sha256}.parquet"


def blob_url(base_url: str, sha256: str) -> str:
    return f"{base_url.rstrip('/')}/{sha256}.parquet"


def put_blob(s3, bucket: str, artifact: DatasetArtifact) -> None:
    """PUT the Parquet at its content-addressed key. `s3` is a boto3 client (injected)."""
    s3.put_object(
        Bucket=bucket,
        Key=blob_key(artifact.sha256),
        Body=artifact.parquet,
        ContentType=PARQUET_CONTENT_TYPE,
    )


def fetch_bytes(url: str, timeout: float = 30.0) -> bytes:
    with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=timeout) as resp:
        return resp.read()


def verify_roundtrip(
    url: str, expected_sha256: str, *, fetch: Callable[..., bytes] = fetch_bytes, timeout: float = 30.0
) -> tuple[bool, str]:
    """Fetch the just-uploaded blob back from its serving URL and confirm the bytes hash to what we
    are about to sign — the serve-before-verify gate. Returns (ok, actual_sha256)."""
    actual = sha256_hex(fetch(url, timeout=timeout))
    return actual == expected_sha256, actual


# ── Event construction + in-process signing ────────────────────────────────────────────────────
def build_tags(sha256: str, url: str) -> list:
    """The v0 manifest tags, in the locked order (spec, d, title, description, part)."""
    return [
        ["spec", NECTAR_SPEC],
        ["d", DATASET_D_TAG],
        ["title", DATASET_TITLE],
        ["description", DATASET_DESCRIPTION],
        ["part", "0", url, sha256],
    ]


def _serialize_for_id(pubkey: str, created_at: int, kind: int, tags, content: str) -> bytes:
    """The NIP-01 canonical serialization: compact JSON of [0,pubkey,created_at,kind,tags,content]
    (no whitespace, UTF-8). Python's json escaping (\\n \\r \\t \\b \\f \\" \\\\, and NOT '/')
    matches NIP-01 — verified against `nak verify`."""
    arr = [0, pubkey, created_at, kind, tags, content]
    return json.dumps(arr, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def compute_event_id(pubkey: str, created_at: int, kind: int, tags, content: str) -> str:
    return sha256_hex(_serialize_for_id(pubkey, created_at, kind, tags, content))


def parse_seckey(nsec_or_hex: str) -> bytes:
    """Accept an nsec (bech32) or a 64-hex secret key; return the 32 raw bytes."""
    s = nsec_or_hex.strip()
    if s.startswith("nsec1"):
        import bech32

        hrp, data = bech32.bech32_decode(s)
        if hrp != "nsec" or data is None:
            raise ValueError("invalid nsec (bech32 decode failed)")
        raw = bech32.convertbits(data, 5, 8, False)
        if raw is None or len(raw) != 32:
            raise ValueError("invalid nsec (not 32 bytes)")
        return bytes(raw)
    return bytes.fromhex(s)


def sign_event(seckey: bytes, kind: int, tags, content: str, created_at: int) -> dict:
    """Build + sign a Nostr event in-process (coincurve BIP340 schnorr over the NIP-01 id)."""
    from coincurve import PrivateKey

    priv = PrivateKey(seckey)
    pubkey = priv.public_key.format(compressed=True)[1:].hex()  # 32-byte x-only pubkey
    eid = compute_event_id(pubkey, created_at, kind, tags, content)
    sig = priv.sign_schnorr(bytes.fromhex(eid)).hex()
    return {
        "id": eid,
        "pubkey": pubkey,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": sig,
    }


def build_signed_manifest(nsec: str, sha256: str, url: str, *, created_at: Optional[int] = None) -> dict:
    """The full v0 kind-30800 manifest event, signed. `created_at` defaults to now (UTC) so daily
    republishes cleanly replace the prior event at the addressable coordinate."""
    if created_at is None:
        created_at = int(datetime.now(timezone.utc).timestamp())
    return sign_event(parse_seckey(nsec), DATASET_KIND, build_tags(sha256, url), "", created_at)


def coordinate(pubkey: str) -> str:
    return f"{DATASET_KIND}:{pubkey}:{DATASET_D_TAG}"


# ── Relay publish (bounded, best-effort per relay) ─────────────────────────────────────────────
def publish_to_relay(url: str, event: dict, *, timeout: float = 15.0) -> tuple[bool, str]:
    """Publish one event to one relay over WS; return (accepted, message). Bounded by `timeout`
    (a relay that never sends our OK is reported as a timeout, not a hang)."""
    from websocket import create_connection

    ws = create_connection(url, timeout=timeout)
    try:
        ws.send(json.dumps(["EVENT", event]))
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, "timeout waiting for OK"
            ws.settimeout(remaining)
            try:
                raw = ws.recv()
            except Exception as exc:  # socket timeout or close mid-wait
                return False, f"no OK ({exc})"
            if not raw:
                continue
            msg = json.loads(raw)
            if isinstance(msg, list) and len(msg) >= 3 and msg[0] == "OK" and msg[1] == event["id"]:
                return bool(msg[2]), (msg[3] if len(msg) > 3 else "")
            # ignore NOTICE / OK-for-other-ids / EOSE etc. and keep waiting for our OK
    finally:
        try:
            ws.close()
        except Exception:
            pass


def publish_to_relays(
    event: dict, relays: Sequence[str], *, timeout: float = 15.0, log: Optional[Callable[[str], None]] = None
) -> dict:
    """Publish `event` to each relay independently; return {relay: {accepted, message}}. A single
    relay failure never raises (the caller decides the success threshold — we succeed on >=1)."""
    results: dict = {}
    for relay in relays:
        try:
            accepted, message = publish_to_relay(relay, event, timeout=timeout)
        except Exception as exc:
            accepted, message = False, f"error: {exc}"
        results[relay] = {"accepted": accepted, "message": message}
        if log:
            log(f"[publish] {relay}: {'OK' if accepted else 'FAIL'} {message}")
    return results


# ── Handoff pointer (parquet asset → manifest asset) ───────────────────────────────────────────
# The two assets don't pass values through Dagster (the repo uses MaterializeResult, whose metadata
# Dagster can't read back as an input). Instead the parquet asset writes a stable "latest" pointer
# on the Dagster scratch volume and the manifest asset reads it — so `materialize parquet+` publishes
# this run's build, while `materialize manifest`-only re-signs+republishes the latest build.
def pointer_path(state_dir: str) -> str:
    return os.path.join(state_dir, DATASET_D_TAG.replace("/", "__") + ".latest.json")


def write_pointer(state_dir: str, artifact: DatasetArtifact, url: str, *, built_at: int) -> str:
    path = pointer_path(state_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(
            {
                "sha256": artifact.sha256,
                "url": url,
                "rows": artifact.rows,
                "day_min": artifact.day_min,
                "day_max": artifact.day_max,
                "bytes": len(artifact.parquet),
                "built_at": built_at,
            },
            fh,
        )
    os.replace(tmp, path)  # atomic
    return path


def read_pointer(state_dir: str) -> dict:
    with open(pointer_path(state_dir)) as fh:
        return json.load(fh)
