"""Unit tests for pipeline/glue_catalog — the CORRECTNESS-CRITICAL partition diff/normalize core and
the register orchestration (diff -> batch-create -> completeness gate -> read-back gate), all with
fake S3/Glue clients (no AWS). The pure helpers carry over partition-diff.sh's hazards: empty-catalog
bootstrap (diff must return ALL of S3), near-miss keys distinct, malformed keys rejected."""

import re

import pytest

from dagster_code.pipeline import glue_catalog as gc


# ── pure diff/normalize core ─────────────────────────────────────────────────────────────────
def test_normalize_s3_keys_extracts_leaf_and_dedups():
    keys = [
        "c=regular/k=9735/p=a/dt=2026-06-26/events-01.parquet",
        "c=regular/k=9735/p=a/dt=2026-06-26/events-02.parquet",  # same leaf, different file
        "c=regular/k=9735/p=b/dt=2026-06-26/events-03.parquet",
        "some-unrelated-key",  # no pattern -> skipped
        "",
    ]
    assert gc.normalize_s3_keys(keys) == {
        "c=regular/k=9735/p=a/dt=2026-06-26",
        "c=regular/k=9735/p=b/dt=2026-06-26",
    }


def test_normalize_catalog_values_builds_canonical_key_and_skips_short():
    parts = [
        {"Values": ["regular", "9735", "a", "2026-06-26"]},
        {"Values": ["replaceable", "10002", "0", "2026-06-25"]},
        {"Values": ["regular", "9735", "a"]},          # short -> skipped
        {"Values": ["regular", None, "a", "2026-06-26"]},  # None -> skipped
        {},                                             # no Values -> skipped
    ]
    assert gc.normalize_catalog_values(parts) == {
        "c=regular/k=9735/p=a/dt=2026-06-26",
        "c=replaceable/k=10002/p=0/dt=2026-06-25",
    }


def test_diff_returns_s3_not_in_catalog():
    cat = {"c=regular/k=9735/p=a/dt=2026-06-26"}
    s3 = {"c=regular/k=9735/p=a/dt=2026-06-26", "c=regular/k=9735/p=b/dt=2026-06-27"}
    assert gc.diff(cat, s3) == ["c=regular/k=9735/p=b/dt=2026-06-27"]


def test_diff_empty_catalog_bootstrap_returns_all_of_s3():
    # The partition-diff.sh NR==FNR hazard: an EMPTY catalog must yield EVERY S3 leaf (not zero).
    s3 = {"c=regular/k=9735/p=a/dt=2026-06-26", "c=regular/k=9735/p=b/dt=2026-06-26"}
    assert sorted(gc.diff(set(), s3)) == sorted(s3)


def test_diff_near_miss_keys_are_distinct():
    cat = {"c=regular/k=9735/p=a/dt=2026-06-26"}
    s3 = {"c=regular/k=9735/p=b/dt=2026-06-26", "c=regular/k=9321/p=a/dt=2026-06-26"}
    # p=a vs p=b and k=9735 vs k=9321 are NOT covered by the catalog entry.
    assert sorted(gc.diff(cat, s3)) == sorted(s3)


def test_key_to_values_parses():
    assert gc.key_to_values("c=regular/k=9735/p=a/dt=2026-06-26") == ("regular", "9735", "a", "2026-06-26")


@pytest.mark.parametrize("bad", [
    "c=regular/k=9735/p=a",                       # 3 segments
    "c=regular/k=9735/p=a/dt=x/extra",            # 5 segments
    "x=regular/k=9735/p=a/dt=2026-06-26",         # wrong prefix
    "c=/k=9735/p=a/dt=2026-06-26",                # empty value
])
def test_key_to_values_rejects_malformed(bad):
    with pytest.raises(ValueError):
        gc.key_to_values(bad)


# ── fake clients ─────────────────────────────────────────────────────────────────────────────
class _ClientError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _S3Pager:
    def __init__(self, keys):
        self.keys = keys

    def paginate(self, *, Bucket, Prefix=""):
        return [{"Contents": [{"Key": k} for k in self.keys if k.startswith(Prefix)]}]


class _FakeS3:
    def __init__(self, object_keys, *, reachable=True):
        self.object_keys = list(object_keys)
        self.reachable = reachable

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _S3Pager(self.object_keys)

    def list_objects_v2(self, *, Bucket, MaxKeys=None):
        if not self.reachable:
            raise _ClientError("AccessDenied")
        return {"Contents": self.object_keys[:1]}


class _GluePager:
    def __init__(self, glue):
        self.glue = glue

    def paginate(self, *, DatabaseName, TableName, Expression):
        kind = re.search(r"k\s*=\s*(\d+)", Expression).group(1)
        vals = [k for k in self.glue.partitions if f"/k={kind}/" in k]
        return [{"Partitions": [{"Values": list(gc.key_to_values(k))} for k in vals]}]


class _FakeGlue:
    """Holds a set of canonical partition keys; batch_create adds them (AlreadyExists if present),
    so a read-back after create reflects the new leaves — exercising the read-back gate honestly."""

    def __init__(self, existing=(), *, throttle_first=0, non_tolerable=False):
        self.partitions = set(existing)
        self.throttle_first = throttle_first
        self.non_tolerable = non_tolerable
        self.batch_calls = 0
        self.sd = {"Location": "s3://placeholder/", "Columns": [{"Name": "id", "Type": "string"}]}

    def get_paginator(self, name):
        assert name == "get_partitions"
        return _GluePager(self)

    def get_table(self, *, DatabaseName, Name):
        return {"Table": {"StorageDescriptor": self.sd}}

    def batch_create_partition(self, *, DatabaseName, TableName, PartitionInputList):
        self.batch_calls += 1
        if self.throttle_first > 0:
            self.throttle_first -= 1
            raise _ClientError("ThrottlingException")
        errors = []
        for inp in PartitionInputList:
            key = "c={}/k={}/p={}/dt={}".format(*inp["Values"])
            if self.non_tolerable:
                errors.append({"PartitionValues": inp["Values"],
                               "ErrorDetail": {"ErrorCode": "InternalServiceException", "ErrorMessage": "boom"}})
            elif key in self.partitions:
                errors.append({"PartitionValues": inp["Values"],
                               "ErrorDetail": {"ErrorCode": "AlreadyExistsException", "ErrorMessage": "exists"}})
            else:
                self.partitions.add(key)
        return {"Errors": errors}


def _no_sleep(_s):
    pass


_S3 = [
    "c=regular/k=9735/p=a/dt=2026-06-26/events-01.parquet",
    "c=regular/k=9735/p=b/dt=2026-06-26/events-02.parquet",
]
_KEYS = {"c=regular/k=9735/p=a/dt=2026-06-26", "c=regular/k=9735/p=b/dt=2026-06-26"}


def _register(s3, glue, **kw):
    return gc.register_partitions(
        s3, glue, archive_bucket="example_archive", glue_db="example_db",
        glue_table="events", kind=9735, sleep=_no_sleep, **kw,
    )


# ── register orchestration ─────────────────────────────────────────────────────────────────────
def test_register_no_new_when_catalog_covers_s3():
    glue = _FakeGlue(existing=_KEYS)
    out = _register(_FakeS3(_S3), glue)
    assert out == {"discovered": 2, "already": 2, "new": 0, "registered": 0, "tolerated": 0}
    assert glue.batch_calls == 0  # nothing to create


def test_register_creates_missing_and_passes_both_gates():
    glue = _FakeGlue(existing=set())  # empty catalog: both leaves missing
    out = _register(_FakeS3(_S3), glue)
    assert out["new"] == 2 and out["registered"] == 2 and out["tolerated"] == 0
    assert glue.batch_calls == 1
    assert glue.partitions == _KEYS  # both now catalogued -> read-back gate passes


def test_register_tolerates_already_exists():
    # One leaf already present; the diff only sees the other as missing, batch-create returns
    # AlreadyExists for any racy re-assert. Completeness still reconciles.
    glue = _FakeGlue(existing={"c=regular/k=9735/p=a/dt=2026-06-26"})
    out = _register(_FakeS3(_S3), glue)
    assert out["already"] == 1 and out["new"] == 1 and out["registered"] == 1


def test_register_retries_throttle_then_succeeds():
    glue = _FakeGlue(existing=set(), throttle_first=2)  # 2 throttles then success
    out = _register(_FakeS3(_S3), glue)
    assert out["registered"] == 2
    assert glue.batch_calls == 3  # 2 throttled + 1 success


def test_register_non_tolerable_error_fails_loud():
    glue = _FakeGlue(existing=set(), non_tolerable=True)
    with pytest.raises(RuntimeError, match="non-tolerable Glue error"):
        _register(_FakeS3(_S3), glue)


def test_register_empty_s3_unreachable_bucket_fails_loud():
    # No S3 leaves AND the bucket probe fails -> outage masquerading as empty -> fail loud.
    with pytest.raises(RuntimeError, match="unreachable"):
        _register(_FakeS3([], reachable=False), _FakeGlue())


def test_register_empty_s3_reachable_bucket_is_clean_zero():
    # Legitimately-empty (idle/new kind), bucket reachable -> 0 new, no error.
    out = _register(_FakeS3([], reachable=True), _FakeGlue())
    assert out["discovered"] == 0 and out["new"] == 0


class _NonReflectingGlue(_FakeGlue):
    """batch_create reports success but does NOT actually add the partition — simulates Glue not
    reflecting the create, so the read-back gate must catch the still-uncovered leaf."""

    def batch_create_partition(self, *, DatabaseName, TableName, PartitionInputList):
        self.batch_calls += 1
        return {"Errors": []}  # claims all created, but self.partitions unchanged


def test_register_readback_gate_catches_unreflected_create():
    with pytest.raises(RuntimeError, match="read-back gate FAILED"):
        _register(_FakeS3(_S3), _NonReflectingGlue(existing=set()))
