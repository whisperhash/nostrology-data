#!/usr/bin/env python3
"""Diff two /marketplaces snapshot JSONs field-by-field -- the marketplaces agreement gate.

Since 2026-08-23 the PRODUCTION home of the marketplaces metrics is the Dagster
`marketplaces_refresh` job (dagster_code/pipeline/marketplaces.py), which
writes one `marketplaces_snapshot` JSONB row per run. `crosscheck/build_markets_snapshot.py`
remains the INDEPENDENT re-derivation (DuckDB straight over the raw S3 archive, on a
workstation), and this script is the comparator between the two. The contract is FULL agreement:
zero diffs across every metric, series element and map point. Any diff is a methodology or
extract bug on one side or the other -- not noise to be tolerated.

THE PROCEDURE (agreement check against a live Dagster run)
  0. What the archive cannot show: the shapes it does not hold yet. No AWS and no network:
       .venv-crosscheck/bin/python -I crosscheck/build_markets_snapshot.py --self-test
       .venv-crosscheck/bin/python -I crosscheck/compare_markets_fold.py
     The second replays generated events through both implementations. Full agreement below says
     little about a rule without it.
  1. Read the Dagster row (a read-only login on the box, from a workstation):
       ssh "${NOSTROLOGY_RO_SSH:?}" 'PGCONNECT_TIMEOUT=30 psql -h <aurora-endpoint> -U nostrology_ro \
         -d nostrology -Atc "SELECT data FROM marketplaces_snapshot \
         ORDER BY generated_utc DESC, id DESC LIMIT 1"' > dagster.json
     (endpoint: `cut -d: -f1 ~/.pgpass | head -1` on the box)
  2. Note its generated_utc (jq -r .generated_utc dagster.json -> "YYYY-MM-DD HH:MM:SSZ").
  3. Re-scan the kind-5 (event-deletion-requests) self-authored requests (the SQL in
     build_markets_snapshot.py's header) -- the Dagster run flushed that kind's WAL at its
     start, so an OLD deletions CSV no longer covers the same S3 state.
  4. Re-run the generator pinned to the SAME instant over the SAME archive:
       .venv-crosscheck/bin/python -I crosscheck/build_markets_snapshot.py --deletions <deletions.csv> \
         --as-of <generated_utc as YYYY-MM-DDTHH:MM:SS> --out static.json
  5. .venv-crosscheck/bin/python -I crosscheck/compare_markets_snapshots.py static.json dagster.json
  Both run under .venv-crosscheck, built per crosscheck/requirements.in on a Python with a maintained
  certificate store: its pycountry, reverse_geocoder, numpy and scipy are pinned to production's, so a
  diff is never a gazetteer or country-table drift (no scratch venv, no bare python3).
  If archive_events differs, a chunk landed between the two reads (the 12h roll, a flush):
  re-run 3-4 -- do not chase field diffs while the inputs differ.

  archive_events counts kind-30402 (markets-product-listings) only, so it CANNOT see a
  kind-5 (event-deletion-requests) chunk that lands between the run and step 3 - and that kind
  is flushed by more than this job: /git's ngit_deletions_extract (ngit_refresh, 03:15Z)
  and the 12-hour roll publish it too. Such a chunk changes the deletion rows, and the diff
  then shows up in the deleted / withdrawn counts and looks like a methodology bug. So before
  step 3 and again after step 4, list the newest chunk times under k=5/, k=30402/ and
  k=30406/ (TZ=UTC aws s3 ls --recursive s3://$NOZZL_ARCHIVE_BUCKET/c=regular/k=5/ ... with
  AWS_PROFILE set, keeping the dt= partitions of the run day and the day before):
  nothing may be newer than the run's own flush. Against a scheduled row of 02:16Z,
  finish the deletion scan before 03:15Z; a manual run leaves a clean window of ~12 h.

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
        # Exact equality, deliberately including int-vs-float type identity for counts;
        # jsonb round-trips 45.35 as 45.35, so a numeric diff is a REAL diff.
        if a != b and not (isinstance(a, (int, float)) and isinstance(b, (int, float)) and a == b):
            out.append(f"{path}: {a!r} (A) vs {b!r} (B)")
    return out


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        print("usage: compare_markets_snapshots.py A.json B.json", file=sys.stderr)
        return 2
    try:
        a = json.load(open(sys.argv[1]))
        b = json.load(open(sys.argv[2]))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"cannot read inputs: {exc}", file=sys.stderr)
        return 2
    d = diff(a, b)
    if not d:
        print(f"FULL AGREEMENT: {sys.argv[1]} == {sys.argv[2]} "
              f"(generated_utc {a.get('generated_utc')})")
        return 0
    print(f"{len(d)} diff(s):")
    for line in d[:100]:
        print(" ", line)
    if len(d) > 100:
        print(f"  ... and {len(d) - 100} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
