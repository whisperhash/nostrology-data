"""Unit tests for pipeline/s3util.py — pure space check + the paginated size/fetch against a fake
s3 client (no real S3)."""

import os

from dagster_code.pipeline import s3util


def test_enough_free_space_includes_20pct_headroom():
    assert s3util.enough_free_space(100, 119) is False  # 100 + 20 = 120 > 119
    assert s3util.enough_free_space(100, 120) is True
    assert s3util.enough_free_space(0, 0) is True  # empty prefix needs nothing


class _FakePaginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_kw):
        return self._pages


class _FakeS3:
    def __init__(self, pages):
        self._pages = pages
        self.downloaded: list = []

    def get_paginator(self, _name):
        return _FakePaginator(self._pages)

    def download_file(self, bucket, key, local):
        self.downloaded.append((bucket, key, local))
        open(local, "w").close()


def test_prefix_total_bytes_sums_across_pages():
    s3 = _FakeS3([
        {"Contents": [{"Key": "k/a.gz", "Size": 10}, {"Key": "k/b.gz", "Size": 5}]},
        {"Contents": [{"Key": "k/c.gz", "Size": 2}]},
    ])
    assert s3util.prefix_total_bytes(s3, "bucket", "k=1/run/") == 17


def test_prefix_total_bytes_empty_prefix():
    assert s3util.prefix_total_bytes(_FakeS3([{}]), "bucket", "k=1/none/") == 0


def test_download_prefix_skips_dir_keys_returns_gz(tmp_path):
    s3 = _FakeS3([
        {"Contents": [
            {"Key": "k=1/run/"},            # directory placeholder -> skipped
            {"Key": "k=1/run/manifest"},    # non-.gz -> downloaded, not returned
            {"Key": "k=1/run/part-0.gz"},
            {"Key": "k=1/run/part-1.gz"},
        ]},
    ])
    parts = s3util.download_prefix(s3, "bucket", "k=1/run/", str(tmp_path))
    assert sorted(os.path.basename(p) for p in parts) == ["part-0.gz", "part-1.gz"]
    # the dir key is skipped; the 3 non-dir keys are all downloaded
    assert sorted(k for _b, k, _l in s3.downloaded) == [
        "k=1/run/manifest", "k=1/run/part-0.gz", "k=1/run/part-1.gz",
    ]
