#!/usr/bin/env bash
# Python dependency locks for this repo's images. Runs on a workstation, never in a deployment.
#
# Each image's DIRECT dependencies live in a hand-edited `.in` file with exact pins. This
# script compiles each `.in` into a GENERATED lock: every package, transitive ones included,
# with == and the sha256 of every file PyPI publishes for that version. The Dockerfiles
# install the locks with `pip install --require-hashes --only-binary :all:`, and
# scripts/check_python_locks.py (run by a deployment before it builds) refuses to build when a
# Dockerfile or a lock breaks that contract.
#
#   compile [target|all]        regenerate lock(s), keeping every existing pin (a version
#                               moves only when its .in pin changes or `upgrade` asks)
#   upgrade <target> <package>  move ONE package to the newest version its .in allows, plus
#                               whatever that requires; then review the lock diff
#   check                       recompile every lock in a temporary copy and diff it with the
#                               committed file, then rebuild the wheel and compare its bytes;
#                               non-zero on any difference
#   wheel                       rebuild the reverse_geocoder wheel from its hash-verified
#                               sdist; fail unless it is byte-identical to the committed one
#                               (writes it when absent)
#
# Every compile derives all hashes afresh. uv keeps the pins it finds in its output file as
# preferences, but it also carries their hashes forward, so a hash removed or altered by hand,
# or a file PyPI added or deleted later, would survive a recompile. compile_target therefore
# seeds uv with the lock's `name==version` lines only.
#
# Targets, compiled in this order (an image's lock constrains its test lock, so compiling mcp
# also recompiles mcp-dev, and compiling dagster also recompiles dagster-test):
#   mcp          nostrology_mcp/requirements.in       the MCP image          (box)
#   mcp-dev      nostrology_mcp/requirements-dev.in   MCP test tools         (workstation, .venv-mcp)
#   dagster      dagster_code/requirements.in         the Dagster image      (box, + the wheel)
#   dagster-test dagster_code/requirements-test.in    pytest beside it       (the test gate)
#   pipeline     requirements-pipeline.in             DuckDB for SQL tests   (the test gate)
#   uv           scripts/requirements-uv.in           uv itself              (workstation, .venv-uv)
#   wheel-build  scripts/requirements-wheel-build.in  build tools for `wheel` (workstation)
#   crosscheck   crosscheck/requirements.in           the cross-checks       (workstation, .venv-crosscheck)
# Each lock is the same path with .txt.
#
# "box" = linux aarch64, glibc 2.36 (Debian bookworm), CPython 3.12: what the base image
# python:3.12-slim-bookworm runs on an arm64 host. PY_VERSION below is the patch level uv
# resolves for and builds the wheel with. The image's own patch level follows its base-pin
# (3.12.15 at the 2026-10-26 pin) and need not match: the locks pin cp312 wheels, so only the
# minor version has to. The platform is spelled out because uv's `linux` shorthand means x86_64,
# and a resolution for a platform whose machine is reported as arm64 silently drops greenlet
# (SQLAlchemy requires it only where platform_machine is aarch64, x86_64 and a few others).
#
# uv writes the input paths it was given into each lock's `# via` lines, so a lock is only
# ever produced by this script, from the repo root, with the relative paths below; `check`
# relies on that to reproduce the committed bytes.
set -Eeuo pipefail
trap 'echo "python-lock.sh: aborted at line ${LINENO} (cmd: ${BASH_COMMAND})" >&2' ERR

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UV="${UV:-${REPO}/.venv-uv/bin/uv}"
PY_VERSION=3.12.14
BOX_PLATFORM=aarch64-manylinux_2_36
TARGETS=(mcp mcp-dev dagster dagster-test pipeline uv wheel-build crosscheck)

WHEEL_DIR=deploy/dagster/wheels
WHEEL=reverse_geocoder-1.5.1-py3-none-any.whl
SDIST=reverse_geocoder-1.5.1.tar.gz
SDIST_URL=https://files.pythonhosted.org/packages/0b/0f/b7d5d4b36553731f11983e19e1813a1059ad0732c5162c01b3220c927d31/reverse_geocoder-1.5.1.tar.gz
SDIST_SHA256=2a2e781b5f69376d922b78fe8978f1350c84fce0ddb07e02c834ecf98b57c75c
SDIST_EPOCH=1473897600   # 2016-09-15 00:00 UTC, the sdist's upload date: fixes the zip timestamps

# Only this script's pins and flags may shape a lock: drop uv's environment overrides
# (UV_INDEX_URL, UV_EXCLUDE_NEWER, ...); --no-config below covers the config files.
# UV_CACHE_DIR only moves the cache, so it stays.
for v in $(compgen -e); do
  if [[ "${v}" == UV_* && "${v}" != UV_CACHE_DIR ]]; then
    echo "python-lock.sh: ignoring ${v} from the environment" >&2
    unset "${v}"
  fi
done

# Every subcommand works in a temporary directory, removed on any exit.
TMP_DIR=""
cleanup() { [[ -z "${TMP_DIR}" ]] || rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

die() { echo "python-lock.sh: $*" >&2; exit 1; }
sha256() { shasum -a 256 "$1" | awk '{print $1}'; }

# The uv binary must be the version scripts/requirements-uv.txt pins: another uv can write
# a different lock from the same inputs, which `check` would then report as drift.
require_uv() {
  [[ -x "${UV}" ]] || die "no uv at ${UV}; bootstrap it as scripts/requirements-uv.in describes, or set UV="
  local want have
  want="$(sed -n 's/^uv==\([^ ]*\).*/\1/p' "${REPO}/scripts/requirements-uv.txt")"
  have="$("${UV}" --version | awk '{print $2}')"
  [[ -n "${want}" && "${have}" == "${want}" ]] \
    || die "uv at ${UV} is ${have}, but scripts/requirements-uv.txt pins ${want}; reinstall from that file"
}

# Set IN, OUT and ARGS for target $1.
target() {
  case "$1" in
    mcp)         IN=nostrology_mcp/requirements.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform "${BOX_PLATFORM}") ;;
    mcp-dev)     IN=nostrology_mcp/requirements-dev.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform aarch64-apple-darwin) ;;
    dagster)     IN=dagster_code/requirements.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform "${BOX_PLATFORM}" --find-links "${WHEEL_DIR}") ;;
    dagster-test) IN=dagster_code/requirements-test.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform "${BOX_PLATFORM}") ;;
    pipeline)    IN=requirements-pipeline.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform "${BOX_PLATFORM}") ;;
    uv)          IN=scripts/requirements-uv.in
                 ARGS=(--python-version 3.14 --python-platform aarch64-apple-darwin) ;;
    wheel-build) IN=scripts/requirements-wheel-build.in
                 ARGS=(--python-version "${PY_VERSION}" --python-platform aarch64-apple-darwin) ;;
    crosscheck)  IN=crosscheck/requirements.in
                 ARGS=(--python-version 3.14 --python-platform aarch64-apple-darwin --find-links "${WHEEL_DIR}") ;;
    *) die "unknown target '$1' (one of: ${TARGETS[*]})" ;;
  esac
  OUT="${IN%.in}.txt"
}

# Compile target $1 in the current directory (the repo root, or check's temporary copy):
# into a scratch output seeded with the lock's pins only, so uv keeps the versions and
# derives every hash again; the lock is replaced only when uv succeeds. TMP_DIR must exist.
# --no-config keeps a user-level uv.toml from changing the result.
compile_target() {
  local t="$1" work
  shift
  target "${t}"
  work="$(mktemp "${TMP_DIR}/out.XXXXXX")"
  if [[ -f "${OUT}" ]]; then
    sed -n 's/^\([A-Za-z0-9][^ ]*==[^ ]*\).*/\1/p' "${OUT}" > "${work}"
  fi
  if ! "${UV}" --no-config pip compile "${IN}" -o "${work}" --generate-hashes --only-binary :all: \
      --custom-compile-command "scripts/python-lock.sh compile ${t}" --quiet "${ARGS[@]}" "$@"; then
    die "uv pip compile failed for target ${t}; ${OUT} is unchanged"
  fi
  cat "${work}" > "${OUT}"
  rm -f "${work}"
}

# The targets a request for $1 must compile, in order; fails on an unknown target.
expand() {
  case "$1" in
    all) printf '%s\n' "${TARGETS[@]}" ;;
    mcp) printf '%s\n' mcp mcp-dev ;;
    dagster) printf '%s\n' dagster dagster-test ;;
    *)   target "$1"; printf '%s\n' "$1" ;;
  esac
}

# Build the wheel from its hash-verified sdist into ${TMP_DIR}/dist/${WHEEL}.
build_wheel() {
  mkdir -p "${TMP_DIR}/sdist" "${TMP_DIR}/dist"
  curl -fsSL --proto '=https' -o "${TMP_DIR}/sdist/${SDIST}" "${SDIST_URL}"
  [[ "$(sha256 "${TMP_DIR}/sdist/${SDIST}")" == "${SDIST_SHA256}" ]] \
    || die "${SDIST} sha256 is $(sha256 "${TMP_DIR}/sdist/${SDIST}"), expected ${SDIST_SHA256}"
  # umask 022 for the build only: the wheel's zip records file modes, and umask 002 builds
  # different bytes. The rest of the script keeps the caller's umask, so what it
  # writes gets the permissions the caller chose.
  ( umask 022
    SOURCE_DATE_EPOCH="${SDIST_EPOCH}" "${UV}" --no-config build --wheel \
      --python "${PY_VERSION}" --managed-python \
      --build-constraints "${REPO}/scripts/requirements-wheel-build.txt" --require-hashes \
      --out-dir "${TMP_DIR}/dist" "${TMP_DIR}/sdist/${SDIST}" )
  [[ -f "${TMP_DIR}/dist/${WHEEL}" ]] || die "the build produced no ${WHEEL}"
}

cmd_compile() {
  [[ $# -le 1 ]] || die "usage: $0 compile [target|all]"
  require_uv
  cd "${REPO}"
  local ts t
  ts="$(expand "${1:-all}")"
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/python-lock-compile.XXXXXX")"
  for t in ${ts}; do
    compile_target "${t}"
    echo "[compile] ${OUT}"
  done
  echo "[compile] next: review the lock diff, then '$0 check' and '/usr/bin/python3 scripts/check_python_locks.py'"
}

cmd_upgrade() {
  [[ $# -eq 2 ]] || die "usage: $0 upgrade <target> <package>"
  # One target: the upgrade flag reaches only the first compiled target (mcp also recompiles
  # mcp-dev, which the mcp lock constrains), so `all` would silently leave the rest unchanged.
  [[ "$1" != all ]] || die "upgrade takes one target, not all (one of: ${TARGETS[*]})"
  require_uv
  cd "${REPO}"
  local ts t first=1
  ts="$(expand "$1")"
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/python-lock-upgrade.XXXXXX")"
  for t in ${ts}; do
    if [[ "${first}" == 1 ]]; then
      compile_target "${t}" --upgrade-package "$2"
      first=0
    else
      compile_target "${t}"
    fi
    echo "[upgrade] ${OUT}"
  done
  git --no-pager diff --stat -- '*.txt'
  echo "[upgrade] review the diff: only $2 and what it requires may move"
}

cmd_check() {
  [[ $# -eq 0 ]] || die "usage: $0 check"
  require_uv
  local t rc=0
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/python-lock-check.XXXXXX")"
  # Mirror the inputs, the committed locks (compile_target reduces them to their pins) and
  # the wheel the dagster target resolves through --find-links.
  for t in "${TARGETS[@]}"; do
    target "${t}"
    mkdir -p "${TMP_DIR}/$(dirname "${IN}")"
    cp "${REPO}/${IN}" "${REPO}/${OUT}" "${TMP_DIR}/$(dirname "${IN}")/"
  done
  mkdir -p "${TMP_DIR}/${WHEEL_DIR}"
  cp "${REPO}/${WHEEL_DIR}/"*.whl "${TMP_DIR}/${WHEEL_DIR}/"
  for t in "${TARGETS[@]}"; do
    ( cd "${TMP_DIR}" && compile_target "${t}" )
    target "${t}"
    if cmp -s "${REPO}/${OUT}" "${TMP_DIR}/${OUT}"; then
      echo "[check] ${OUT}: reproduces"
    else
      echo "[check] ${OUT}: DIFFERS from a fresh compile (stale, edited by hand, or PyPI changed):"
      diff -u "${REPO}/${OUT}" "${TMP_DIR}/${OUT}" || true
      rc=1
    fi
  done
  # The locks cannot show a replaced wheel: the dagster lock would simply carry its hash.
  # Rebuild it from the sdist and compare bytes.
  [[ -f "${REPO}/${WHEEL_DIR}/${WHEEL}" ]] || die "no committed ${WHEEL_DIR}/${WHEEL}"
  build_wheel
  if cmp -s "${TMP_DIR}/dist/${WHEEL}" "${REPO}/${WHEEL_DIR}/${WHEEL}"; then
    echo "[check] ${WHEEL_DIR}/${WHEEL}: rebuilt byte-identical, sha256 $(sha256 "${REPO}/${WHEEL_DIR}/${WHEEL}")"
  else
    echo "[check] ${WHEEL_DIR}/${WHEEL}: DIFFERS from a rebuild from the sdist (committed sha256 $(sha256 "${REPO}/${WHEEL_DIR}/${WHEEL}"), rebuilt $(sha256 "${TMP_DIR}/dist/${WHEEL}"))"
    rc=1
  fi
  [[ "${rc}" == 0 ]] || die "check failed: recompile with '$0 compile', or rebuild the wheel, and review the diff"
  echo "[check] all ${#TARGETS[@]} locks reproduce and the wheel rebuilds identically"
}

cmd_wheel() {
  [[ $# -eq 0 ]] || die "usage: $0 wheel"
  require_uv
  cd "${REPO}"
  local built committed
  TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/python-lock-wheel.XXXXXX")"
  build_wheel
  built="${TMP_DIR}/dist/${WHEEL}"
  committed="${WHEEL_DIR}/${WHEEL}"
  if [[ ! -e "${committed}" ]]; then
    mkdir -p "${WHEEL_DIR}"
    cp "${built}" "${committed}"
    chmod 664 "${committed}"
    echo "[wheel] wrote ${committed}, sha256 $(sha256 "${committed}"); commit it and recompile the dagster lock"
  elif cmp -s "${built}" "${committed}"; then
    echo "[wheel] rebuilt ${WHEEL} is byte-identical to ${committed}, sha256 $(sha256 "${committed}")"
  else
    die "rebuilt ${WHEEL} (sha256 $(sha256 "${built}")) differs from ${committed} (sha256 $(sha256 "${committed}"))"
  fi
}

case "${1:-}" in
  compile) shift; cmd_compile "$@" ;;
  upgrade) shift; cmd_upgrade "$@" ;;
  check)   shift; cmd_check "$@" ;;
  wheel)   shift; cmd_wheel "$@" ;;
  *) echo "usage: $0 {compile [target|all] | upgrade <target> <package> | check | wheel}" >&2; exit 2 ;;
esac
