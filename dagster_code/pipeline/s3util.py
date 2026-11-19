"""S3 helpers for the load: total prefix size (the free-space preflight), the recursive fetch, and
the pure space check. The boto3-touching functions take the s3 client as an argument (testable with
a mock paginator); enough_free_space is pure. Ports run.sh:749/752-753/761.
"""

import os


def iter_object_keys(s3, bucket: str, prefix: str = ""):
    """Yield every object key under s3://bucket/prefix (paginated). The raw-key feed the Glue
    partition discovery normalizes (port of `aws s3 ls --recursive`)."""
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def count_gz_parts(s3, bucket: str, prefix: str) -> int:
    """Count the .gz objects under a prefix — the extract's part count (the extract_nonempty check)."""
    return sum(1 for key in iter_object_keys(s3, bucket, prefix) if key.endswith(".gz"))


def bucket_reachable(s3, bucket: str) -> bool:
    """Cheap top-level reachability probe (run.sh:933's `aws s3 ls s3://bucket/`) — distinguishes a
    legitimately-empty scoped prefix from a creds/throttle/region outage."""
    try:
        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        return True
    except Exception:  # noqa: BLE001 — any failure means "not reachable"; caller fails loud
        return False


def prefix_total_bytes(s3, bucket: str, prefix: str) -> int:
    """Sum the sizes of every object under s3://bucket/prefix (paginated). Port of
    `aws s3 ls --recursive --summarize | awk '/Total Size/'`."""
    total = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total += int(obj.get("Size", 0))
    return total


def enough_free_space(need_bytes: int, free_bytes: int, headroom_frac: float = 0.2) -> bool:
    """need + headroom% must fit in free (run.sh:752-753, +20%). Pure."""
    return free_bytes >= need_bytes + int(need_bytes * headroom_frac)


def download_prefix(s3, bucket: str, prefix: str, dest_dir: str) -> list[str]:
    """Download every object under the prefix into dest_dir (flat, basename), skipping directory
    placeholder keys. Returns the local .gz part paths. Port of `aws s3 cp --recursive`."""
    paths: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            local = os.path.join(dest_dir, os.path.basename(key))
            s3.download_file(bucket, key, local)
            if local.endswith(".gz"):
                paths.append(local)
    return paths
