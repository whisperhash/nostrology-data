#!/usr/bin/env python3
"""Standalone test for _open_text gz-transparency (stdlib + the loader module; no DB).

Run:  python3 loader/test_open_text.py      (exits non-zero on failure)

Locks the A2 invariant: the loader reads Athena's gzip UNLOAD parts DIRECTLY,
yielding byte-identical lines to a plain .jsonl, so the per-line load loop is
unchanged whether the input is compressed or not. This is the only new behaviour
behind dropping the decompress-to-disk step, which keeps the ~6x-larger NDJSON off
every volume: a full kind-9735 (zap-receipts) extract was ~2.3 GB gz -> ~15 GB NDJSON
when decompressing it to disk overflowed the root volume with Errno 28 on the first
full Pass 2.
"""
import gzip
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_jsonl_exports_into_postgres import _open_text, _ProgressPrinter


def _read_lines(path):
    with _open_text(path) as f:
        return [line.rstrip("\n") for line in f]


def main():
    # Representative kind-0 + kind-9735 events (content unimportant; we test I/O parity).
    # Repeat to a few thousand lines so the gz decompresses to many× its compressed
    # size — that gap is what makes the _ProgressPrinter._pos assertion meaningful.
    base = [
        {"id": "a" * 64, "pubkey": "b" * 64, "created_at": 1700000000, "kind": 0,
         "content": json.dumps({"name": "x", "lud16": "x@example.com"})},
        {"id": "c" * 64, "pubkey": "d" * 64, "created_at": 1700000001, "kind": 9735, "tags": []},
        {"id": "e" * 64, "pubkey": "f" * 64, "created_at": 1700000002, "kind": 0, "content": ""},
    ]
    events = [dict(e, created_at=e["created_at"] + i) for i in range(1000) for e in base]
    lines = [json.dumps(e) for e in events]
    blob = "\n".join(lines) + "\n"

    with tempfile.TemporaryDirectory() as d:
        plain = os.path.join(d, "part.jsonl")
        gzipped = os.path.join(d, "part.jsonl.gz")
        with open(plain, "w", encoding="utf-8") as f:
            f.write(blob)
        with gzip.open(gzipped, "wt", encoding="utf-8") as f:
            f.write(blob)

        plain_lines = _read_lines(plain)
        gz_lines = _read_lines(gzipped)

        assert plain_lines == lines, "plain read mismatch"
        assert gz_lines == lines, "gz read mismatch"
        assert gz_lines == plain_lines, "gz and plain reads differ"
        # The load loop does json.loads(line) per non-empty line — each must round-trip.
        for ln in gz_lines:
            json.loads(ln)

        # _ProgressPrinter._pos must report progress against the file ON DISK: the
        # COMPRESSED offset for a gz (else %/ETA blow past 100%), the plain byte offset
        # otherwise. Regression guard for the gz progress fix.
        comp_size = os.path.getsize(gzipped)
        decomp_size = os.path.getsize(plain)
        assert comp_size * 3 < decomp_size, "test gz not compressible enough to be a real check"
        with _open_text(gzipped) as f:
            for i, _ in enumerate(f):
                if i >= 100:
                    break
            gz_pos = _ProgressPrinter._pos(f)
        assert gz_pos <= comp_size, (
            f"_pos {gz_pos} exceeds compressed size {comp_size} — gz progress would exceed 100% "
            f"(the decompressed offset is ~{decomp_size}; the fix must read the compressed offset)")
        with _open_text(plain) as f:
            for i, _ in enumerate(f):
                if i >= 100:
                    break
            txt_pos = _ProgressPrinter._pos(f)
        assert 0 < txt_pos <= decomp_size, f"_pos {txt_pos} out of range for plain file {decomp_size}"

    print(f"OK: _open_text gz/jsonl parity ({len(gz_lines)} lines) + _pos honest "
          f"(gz compressed-offset {gz_pos}<= {comp_size}, plain {txt_pos})")


if __name__ == "__main__":
    main()
