"""Register AWS Glue partitions from S3 into the catalog ahead of an Athena extract — the
CORRECTNESS-CRITICAL boto3 port of run.sh cmd_register_partitions (run.sh:839-1198) and of the
pure diff/normalize core of its partition diff.

Catalog-mode Athena does NOT auto-discover partitions at query time, so every leaf the
producer wrote must be registered BEFORE the extract reads the catalog — a MISSED partition is
silent data loss (the extract returns FEWER rows with no error; only the loader's MIN_ROWS floor /
0-row refusal backstops it). So this never drops a partition.

The two correctness hazards partition-diff.sh defends against carry over verbatim:
  1. COLLATION — a locale `sort` + `comm` can mis-classify a present partition as missing. We use
     Python set membership (order-, locale-, dedup-independent), never a sorted-merge.
  2. TWO FORMATS → ONE KEY — S3 object keys vs Glue Values tuples both normalize to the SAME
     canonical key `c=<c>/k=<k>/p=<p>/dt=<dt>` (Glue partition-key order [c,k,p,dt], the
     partition_keys of the table's Terraform) so the diff compares like-with-like.

The boto3 clients (s3, glue) are injected; the diff/normalize/parse helpers are pure — so the whole
unit unit-tests with fakes and no AWS.
"""

import re
import time
from typing import Callable, Iterable

from .kinds import kind_to_class
from . import s3util

# Leftmost canonical-key substring in an S3 object key (collapses every file under a leaf to the one
# partition key; the trailing /events-….parquet past dt=<…> is not captured). A STRING pattern (not
# an awk /regex/ literal) — same as pd_normalize_s3.
_S3_KEY_RE = re.compile(r"c=[^/]+/k=[^/]+/p=[^/]+/dt=[^/]+")
# A well-formed canonical key (the malformed-diff reject, run.sh:1001).
_CANON_RE = re.compile(r"^c=[^/]+/k=[^/]+/p=[^/]+/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}$")

_BATCH_MAX = 100
_THROTTLE_CODES = ("ThrottlingException", "OperationTimeoutException")


def _noop_log(_msg: str) -> None:
    pass


# ── pure diff/normalize core (partition-diff.sh) ─────────────────────────────────────────────
def normalize_s3_keys(object_keys: Iterable[str]) -> set:
    """S3 object keys -> the set of canonical leaf keys (pd_normalize_s3). Lines without the
    pattern (footers/blanks) are silently skipped; at most one key per input."""
    out = set()
    for key in object_keys:
        m = _S3_KEY_RE.search(key)
        if m:
            out.add(m.group(0))
    return out


def normalize_catalog_values(partitions: Iterable[dict]) -> set:
    """Glue partitions ({'Values':[c,k,p,dt]}) -> the set of canonical keys (pd_normalize_catalog).
    A short/None-bearing Values (only from an externally-mangled catalog) is skipped, not emitted."""
    out = set()
    for part in partitions:
        vals = part.get("Values") or []
        if len(vals) == 4 and all(v is not None for v in vals):
            out.add(f"c={vals[0]}/k={vals[1]}/p={vals[2]}/dt={vals[3]}")
    return out


def diff(catalog_keys: Iterable[str], s3_keys: Iterable[str]) -> list:
    """The keys in S3 NOT in the catalog — the missing set to register (pd_diff, as set membership).
    Sorted for a deterministic batch/log order."""
    cat = set(catalog_keys)
    return sorted(k for k in set(s3_keys) if k not in cat)


def key_to_values(key: str):
    """`c=X/k=Y/p=Z/dt=W` -> (X, Y, Z, W). Raises ValueError on a malformed key (wrong field count,
    wrong/missing prefix, or an empty value) — the caller fails loud rather than registering garbage
    (pd_key_to_values)."""
    segs = key.split("/")
    if len(segs) != 4:
        raise ValueError(f"malformed key (want 4 segments c=…/k=…/p=…/dt=…): {key!r}")
    vals = []
    for prefix, seg in zip(("c=", "k=", "p=", "dt="), segs):
        if not seg.startswith(prefix):
            raise ValueError(f"malformed key (segment {seg!r} lacks prefix {prefix!r}): {key!r}")
        val = seg[len(prefix):]
        if val == "":
            raise ValueError(f"malformed key (empty value in segment {seg!r}): {key!r}")
        vals.append(val)
    return tuple(vals)


# ── boto3 orchestration (cmd_register_partitions) ────────────────────────────────────────────
def _is_throttle(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)
    if not isinstance(resp, dict):
        return False
    return resp.get("Error", {}).get("Code") in _THROTTLE_CODES


def _discover_s3(s3, *, archive_bucket: str, kind, log) -> set:
    """List the kind's S3 leaves (scoped to c=<class>/k=<kind>/ when the class is known — a full
    --recursive walk is ~46s, the scoped prefix ~1-3s) and normalize. Post-filters on /k=<kind>/ so
    a (hypothetically wrong) scoped prefix can only under-include, which the read-back gate catches.
    A legitimately-empty prefix (idle/new kind) returns the empty set IFF the bucket is reachable —
    else it was an outage masquerading as empty, and we fail loud (run.sh:929-939)."""
    cls = kind_to_class(kind)
    prefix = f"c={cls}/k={kind}/" if cls else ""
    scope = prefix or "full-bucket"
    log(f"[register] discovering S3 leaves under s3://{archive_bucket}/{prefix} (scope: {scope})")
    keys = normalize_s3_keys(s3util.iter_object_keys(s3, archive_bucket, prefix))
    keys = {k for k in keys if f"/k={kind}/" in k}
    if not keys and not s3util.bucket_reachable(s3, archive_bucket):
        raise RuntimeError(
            f"S3 discovery for k={kind} was empty AND s3://{archive_bucket}/ is unreachable — "
            "refusing to register against an unverified-empty result (it would short the extract). "
            "Check credentials/region."
        )
    log(f"[register] discovered {len(keys)} S3 leaf partition(s)")
    return keys


def _fetch_catalog(glue, *, glue_db: str, glue_table: str, kind, log) -> set:
    """Fetch the catalog partitions for this kind (Expression scopes server-side) and normalize.

    NOTE — this is a deliberate semantic change from run.sh's MAIN fetch (run.sh:962-977), which
    fetched ALL partitions then string-grepped `/k=K/`; here BOTH the main fetch and the read-back
    use the same typed `Expression="k = {kind}"` (k is an int-typed Glue partition key in
    the table's Terraform). It is DIFF-SAFE: a typed Expression silently drops only a catalog row
    whose k value isn't a clean integer (only possible from an externally-mangled catalog — this tool
    always writes integer k). A dropped catalog row makes its leaf look "missing" -> it is re-ADDed
    -> AlreadyExists-tolerated -> still complete. So the worst case is a redundant idempotent re-ADD,
    never a dropped S3 leaf; the completeness + read-back gates remain the real backstops."""
    pages = glue.get_paginator("get_partitions").paginate(
        DatabaseName=glue_db, TableName=glue_table, Expression=f"k = {kind}"
    )
    parts = []
    for page in pages:
        parts.extend(page.get("Partitions", []))
    keys = normalize_catalog_values(parts)
    log(f"[register] fetched {len(keys)} catalog partition(s) for k={kind}")
    return keys


def _flush_batch(
    glue, *, glue_db, glue_table, inputs, batch_idx, total_batches, log, sleep, max_attempts=5
):
    """batch_create_partition one batch, retrying ONLY throttle/timeout (capped backoff), tolerating
    ONLY AlreadyExistsException per-partition, failing loud on any other Glue error. Returns
    (created, tolerated)."""
    attempt = 0
    sleep_s = 2.0
    while True:
        try:
            resp = glue.batch_create_partition(
                DatabaseName=glue_db, TableName=glue_table, PartitionInputList=inputs
            )
            break
        except Exception as exc:  # noqa: BLE001
            if _is_throttle(exc) and attempt < max_attempts:
                attempt += 1
                code = exc.response.get("Error", {}).get("Code")
                log(f"[register] batch {batch_idx}/{total_batches}: {code} — retry {attempt}/{max_attempts} after {sleep_s:g}s")
                sleep(sleep_s)
                sleep_s *= 2
                continue
            raise RuntimeError(
                f"batch {batch_idx}/{total_batches} batch-create-partition failed after {attempt} retr(ies): {exc}"
            ) from exc

    errors = resp.get("Errors") or []
    non_tolerable = [e for e in errors if (e.get("ErrorDetail") or {}).get("ErrorCode") != "AlreadyExistsException"]
    if non_tolerable:
        detail = "; ".join(
            f"PartitionValues={e.get('PartitionValues')} "
            f"ErrorCode={(e.get('ErrorDetail') or {}).get('ErrorCode')} "
            f"Msg={(e.get('ErrorDetail') or {}).get('ErrorMessage')}"
            for e in non_tolerable
        )
        raise RuntimeError(f"batch {batch_idx}/{total_batches} had non-tolerable Glue error(s): {detail}")
    tolerated = len(errors)  # all AlreadyExists (idempotent re-assert)
    created = len(inputs) - tolerated
    log(f"[register] batch {batch_idx}/{total_batches}: {created} created"
        + (f" ({tolerated} already-existed)" if tolerated else ""))
    return created, tolerated


def register_partitions(
    s3,
    glue,
    *,
    archive_bucket: str,
    glue_db: str,
    glue_table: str,
    kind,
    log: Callable[[str], None] = _noop_log,
    sleep: Callable[[float], None] = time.sleep,
    batch_max: int = _BATCH_MAX,
) -> dict:
    """Diff this kind's S3 leaves against the catalog and ADD only the missing ones (ADD-only,
    AlreadyExists-tolerant). Returns {discovered, already, new, registered, tolerated}.

    Raises on: an unverified-empty S3 discovery, a non-tolerable Glue error, a completeness-gate
    miss (registered+tolerated != new), or a read-back-gate miss (a discovered leaf still absent
    from the catalog after registration). Drops run.sh's fd7/fd9 flock (the QueuedRunCoordinator
    serializes runs) and the --since-dt path (each Dagster extract registers its own kind, full)."""
    log(f"[register] target {glue_db}.{glue_table}  bucket s3://{archive_bucket}  kind={kind}")
    s3_keys = _discover_s3(s3, archive_bucket=archive_bucket, kind=kind, log=log)
    cat_keys = _fetch_catalog(glue, glue_db=glue_db, glue_table=glue_table, kind=kind, log=log)
    missing = diff(cat_keys, s3_keys)

    # Reject a malformed diff (a non-canonical key) — a normalize-contract mismatch (run.sh:1001).
    bad = [k for k in missing if not _CANON_RE.match(k)]
    if bad:
        raise RuntimeError(f"diff produced malformed (non-canonical) key(s): {bad[:3]}")

    discovered, already, new = len(s3_keys), len(cat_keys), len(missing)
    log(f"[register] discovered={discovered} already={already} new={new}")

    if new == 0:
        log(f"[register] registered 0 new ({already} present, {discovered} total) — catalog already covers S3")
        return {"discovered": discovered, "already": already, "new": 0, "registered": 0, "tolerated": 0}

    sd = glue.get_table(DatabaseName=glue_db, Name=glue_table).get("Table", {}).get("StorageDescriptor")
    if not sd:
        raise RuntimeError(f"empty StorageDescriptor for {glue_db}.{glue_table}")

    total_batches = (new + batch_max - 1) // batch_max
    log(f"[register] registering {new} new partition(s) in {total_batches} batch(es) of up to {batch_max}")
    registered = tolerated = 0
    batch_idx = 0
    for start in range(0, new, batch_max):
        batch_keys = missing[start:start + batch_max]
        inputs = []
        for key in batch_keys:
            c, k, p, dt = key_to_values(key)  # ValueError -> fails the verb (run.sh:1126)
            loc = f"s3://{archive_bucket}/c={c}/k={k}/p={p}/dt={dt}/"
            inputs.append({"Values": [c, k, p, dt], "StorageDescriptor": dict(sd, Location=loc)})
        batch_idx += 1
        created, tol = _flush_batch(
            glue, glue_db=glue_db, glue_table=glue_table, inputs=inputs,
            batch_idx=batch_idx, total_batches=total_batches, log=log, sleep=sleep,
        )
        registered += created
        tolerated += tol

    # Completeness self-check: every missing leaf must be created or AlreadyExists (run.sh:1158).
    handled = registered + tolerated
    if handled != new:
        raise RuntimeError(
            f"completeness self-check FAILED — {new} S3 leaves were missing but only "
            f"registered({registered}) + alreadyExists({tolerated}) = {handled} were resolved. Some "
            "discovered S3 leaves are NOT catalogued; a later extract could return SHORT. Re-run."
        )

    # POSITIVE read-back gate: re-read the catalog and prove it now COVERS every discovered S3 leaf
    # (the real invariant — catalog ⊇ S3 for this kind), not just trust batch-create's
    # self-report (run.sh:1164-1191).
    rb_keys = _fetch_catalog(glue, glue_db=glue_db, glue_table=glue_table, kind=kind, log=log)
    uncovered = diff(rb_keys, s3_keys)
    if uncovered:
        raise RuntimeError(
            f"catalog read-back gate FAILED — {len(uncovered)} discovered S3 leaf/leaves for k={kind} "
            f"are NOT in the catalog after registration (Glue did not reflect the batch-create, or a "
            f"leaf landed concurrently). The extract would return SHORT. Offending: {uncovered[:5]}"
        )
    log(f"[register] read-back OK — catalog covers all {discovered} discovered k={kind} leaf/leaves; "
        f"registered {registered} new ({already} present, {discovered} total)")
    return {"discovered": discovered, "already": already, "new": new, "registered": registered, "tolerated": tolerated}
