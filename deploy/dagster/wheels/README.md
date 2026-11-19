# Committed wheels for the Dagster image

`deploy/dagster/Dockerfile.dagster` copies the wheels (`*.whl`) in this directory to
`/tmp/dagster-wheels`, where pip finds them through `--find-links`. pip accepts a wheel only by
its sha256 in the Dagster lock (`dagster_code/requirements.txt`), and
`scripts/check_python_locks.py` refuses a deploy whose wheel and lock disagree. A binary lives in git
by the operator's choice, 2026-09-14; why each one is needed is under its heading.

## reverse_geocoder-1.5.1-py3-none-any.whl

- **What:** reverse_geocoder 1.5.1 (https://github.com/thampiman/reverse-geocoder), the
  offline GeoNames cities1000 gazetteer and reverse geocoder behind /marketplaces
  (`dagster_code/pipeline/markets_geolib.py`). Pure Python. Licence LGPL-2.1 (the upstream
  repository's LICENSE; the PyPI metadata says `lgpl`, and the sdist ships no licence file).
  That LICENSE is `reverse_geocoder-LICENSE.txt` beside the wheel, and the repository's
  `THIRD_PARTY_NOTICES.md` credits GeoNames (CC BY 4.0) for its `rg_cities1000.csv`.
  Unmodified: the wheel's three payload files (`__init__.py`, `cKDTree_MP.py`,
  `rg_cities1000.csv`) are byte-identical to the sdist's, which is their source.
- **Why a wheel here:** PyPI publishes only the sdist (uploaded 2016-09-15). Installing it
  means building it in the image, where its `setup_requires` fetches numpy unpinned, which
  `--require-hashes` does not cover (pypa/pip#13984).
- **Source:** https://files.pythonhosted.org/packages/0b/0f/b7d5d4b36553731f11983e19e1813a1059ad0732c5162c01b3220c927d31/reverse_geocoder-1.5.1.tar.gz,
  2,246,559 bytes, sha256 `2a2e781b5f69376d922b78fe8978f1350c84fce0ddb07e02c834ecf98b57c75c`.
- **Wheel:** 2,268,111 bytes, sha256
  `f48f38df719cc2c8b7abad35233f861fd7efce3a874046c4828e00048e7b5e22`.
- **Built by** `scripts/python-lock.sh wheel`: uv 0.12.13 with uv-managed CPython 3.12.14,
  build tools hash-checked from `scripts/requirements-wheel-build.txt` (setuptools 84.0.0,
  numpy 2.4.6), `SOURCE_DATE_EPOCH=1473897600` (the sdist's upload date, which fixes the zip
  timestamps) and `umask 022` (the zip records file modes; umask 002 gives other bytes).
  Separate builds on 2026-09-14 produced identical bytes.

Verify: `scripts/python-lock.sh wheel`, or `check`, which ends with the same step. It downloads
the sdist, checks its sha256, rebuilds, and fails unless the result is byte-identical to the
committed file.

Replace (a new version, or a setuptools change that alters the bytes): update the `SDIST_*`
and `WHEEL` constants in `scripts/python-lock.sh` (and the pins in
`scripts/requirements-wheel-build.in`, then `compile wheel-build`), delete the old wheel, run
`wheel` (it writes the new file when none is present), then `compile dagster`, `compile crosscheck`, `check`, the
guard, and a local Dagster image build with its tests, before a reviewed commit.
