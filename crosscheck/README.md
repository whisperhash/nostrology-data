# Cross-checks

Independent re-derivations of the numbers the pipeline publishes, read from the raw archive with DuckDB
(`ngit_check.py`'s pinned recipe takes three heavy kinds from the run's own extract parts). They run by
hand on a workstation. No image contains them and the pipeline does not import them. They do import from
it: `compare_markets_fold.py` imports `dagster_code/pipeline/marketplaces.py`, `build_markets_snapshot.py`
its `markets_geolib.py`, and `ngit_check.py` reads `src/shared/zap_parser.py`.

| Surface | Re-derivation | Comparison | Rule text |
|---|---|---|---|
| /marketplaces | `build_markets_snapshot.py` | `compare_markets_snapshots.py`, `compare_markets_fold.py` | `docs/dataset-reference/markets-dataset-reference.md` |
| /routstr | `build_routstr_snapshot.py` | `compare_routstr_snapshots.py` | `docs/dataset-reference/routstr-dataset-reference.md` |
| /git | `ngit_check.py` | `ngit_check.py --compare` | `docs/dataset-reference/ngit-dataset-reference.md` |

Each script's header has its procedure. The checks that need no AWS and no network, from the repo root:

```sh
.venv-crosscheck/bin/python -I crosscheck/ngit_check.py --self-test
.venv-crosscheck/bin/python -I crosscheck/build_markets_snapshot.py --self-test
.venv-crosscheck/bin/python -I crosscheck/compare_markets_fold.py
```

`requirements.in` has the recipe for the venv, and `scripts/python-lock.sh compile crosscheck` makes its lock.
The lock is a target of that script and of `scripts/check_python_locks.py`, the guard the deploys run, so
removing it means editing both.

`ngit_check.py` is written from its reference alone. Whoever changes it must not read
`dagster_code/pipeline/ngit.py`, its tests or a pipeline dry run, although they sit in the same
repository: a check that copies the production compute cannot catch that compute's mistakes.

Whether to retire the /marketplaces reference generator and comparator is #24 (Consider retiring the
adhoc /marketplaces reference generator and comparator). Running the offline checks above in the test gate
is #36 (Consider running the cross-checks' offline checks in the test gate).
