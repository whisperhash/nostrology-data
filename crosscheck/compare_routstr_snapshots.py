#!/usr/bin/env python3
"""Diff two /routstr snapshot JSONs field-by-field -- the routstr agreement gate.

The PRODUCTION home of the routstr metrics is the Dagster `routstr_refresh` job
(dagster_code/pipeline/routstr.py), which writes one `routstr_snapshot`
JSONB row per run. `crosscheck/build_routstr_snapshot.py` is the INDEPENDENT re-derivation
(DuckDB straight over the raw S3 archive, on a workstation), and this script is the
comparator between the two. The contract is FULL agreement: zero diffs across every
metric, series element and table row. Any diff is a methodology or extract bug on one
side or the other -- not noise to be tolerated.

THE PROCEDURE (agreement check against a live Dagster run)
  1. Read the Dagster row (a read-only login on the box, from a workstation):
       ssh "${NOSTROLOGY_RO_SSH:?}" 'PGCONNECT_TIMEOUT=30 psql -h <aurora-endpoint> -U nostrology_ro \
         -d nostrology -Atc "SELECT data FROM routstr_snapshot \
         ORDER BY generated_utc DESC, id DESC LIMIT 1"' > dagster.json
     (endpoint: `cut -d: -f1 ~/.pgpass | head -1` on the box)
  2. Note its generated_utc (jq -r .generated_utc dagster.json -> "YYYY-MM-DD HH:MM:SSZ").
  3. Re-run the generator pinned to the SAME instant over the SAME archive:
       eval "$(aws configure export-credentials --profile "$AWS_PROFILE" --format env)"
       .venv-crosscheck/bin/python -I crosscheck/build_routstr_snapshot.py --as-of <generated_utc as YYYY-MM-DDTHH:MM:SS> \
         --ingested-before <the same instant> --out static.json
  4. .venv-crosscheck/bin/python -I crosscheck/compare_routstr_snapshots.py static.json dagster.json
  If summary.archive_events differs, the two sides read different archive states:
  without --ingested-before a chunk that landed after the run (the 12h roll) is in the
  reference and not in the row; with it, the residual is the ~minute between the run's
  WAL flush and its build instant. Re-run 3 -- do not chase field diffs while the
  inputs differ.

NUL note: the production row is NUL-stripped at insert while the reference JSON is
not, so a NUL-bearing hostile event surfaces here as a diff - that is the signal to
add a deliberate both-sides rule, not noise.

Exit 0 = full agreement; 1 = diffs (printed, capped); 2 = usage/parse error.
"""
import json
import sys


def diff(a, b, path="$"):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: only in B")
            elif k not in b:
                out.append(f"{path}.{k}: only in A")
            else:
                out += diff(a[k], b[k], f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            out.append(f"{path}: length {len(a)} (A) vs {len(b)} (B)")
        for i, (x, y) in enumerate(zip(a, b)):
            out += diff(x, y, f"{path}[{i}]")
    else:
        # Exact equality; int-vs-float identity is tolerated for equal numerics (jsonb
        # round-trips preserve the distinction, so 5 == 5.0 is fine but 5 != 5.001).
        if a != b and not (isinstance(a, (int, float)) and isinstance(b, (int, float)) and a == b):
            out.append(f"{path}: {a!r} (A) vs {b!r} (B)")
    return out


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    try:
        with open(sys.argv[1]) as fa:
            a = json.load(fa)
        with open(sys.argv[2]) as fb:
            b = json.load(fb)
    except (OSError, ValueError) as e:
        print(f"cannot load inputs: {e}")
        return 2
    if (a.get("summary", {}).get("archive_events") != b.get("summary", {}).get("archive_events")):
        print("summary.archive_events differ - the INPUTS differ (a chunk landed between "
              "the reads); re-extract and re-run rather than chasing field diffs:")
        print(f"  A: {a.get('summary', {}).get('archive_events')}")
        print(f"  B: {b.get('summary', {}).get('archive_events')}")
    diffs = diff(a, b)
    if not diffs:
        print("FULL AGREEMENT - zero diffs.")
        return 0
    print(f"{len(diffs)} diff(s):")
    for d in diffs[:100]:
        print("  " + d)
    if len(diffs) > 100:
        print(f"  ... and {len(diffs) - 100} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
