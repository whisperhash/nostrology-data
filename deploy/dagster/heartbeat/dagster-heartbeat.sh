#!/usr/bin/env bash
# Out-of-band Dagster daemon liveness heartbeat (runs ON the host, as a user in the docker
# group, on a systemd timer - NOT a Dagster sensor). A Dagster sensor cannot report its OWN
# daemon being dead, and the Discord failure sensor is itself daemon-hosted; so this independent
# process is the only thing that catches "daemon down -> no runs fire, no alerts at all". It
# writes a small JSON the read-only observer can read off the dagster volume, and best-effort
# pings Discord on a failure.
#
# Liveness signal: `dagster-daemon liveness-check` INSIDE the daemon container (it reads the
# daemon's heartbeat row from the run store; exit 0 = healthy). It also surfaces last-run.json's
# freshness so an alarm can fire on a STUCK timer (a stale `finished` with overall=ok).
set -Eeuo pipefail
# Fail loud like the rest of the deploy tooling. The explicit `exit 1` on a dead
# daemon (below) does NOT trip this trap; it only catches UNEXPECTED failures (a
# full-disk printf, a failed mv) — a silent abort in the liveness checker is the
# worst place for one, since it's what's meant to catch silence.
trap 'echo "dagster-heartbeat: aborted at line ${LINENO} (cmd: ${BASH_COMMAND})" >&2' ERR

OUT="${DAGSTER_HEARTBEAT_JSON:-/mnt/nozzl-dagster/daemon-heartbeat.json}"
LAST_RUN="${DAGSTER_LAST_RUN_JSON:-/mnt/nozzl-dagster/last-run.json}"
WEBHOOK="${DAGSTER_DISCORD_WEBHOOK:-}"
now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

daemon_ok=false
if docker exec dagster-daemon dagster-daemon liveness-check >/dev/null 2>&1; then
  daemon_ok=true
fi

# Carry through last-run.json's finished/overall (best-effort) so a single file answers
# "is the daemon alive AND did the last run finish recently?".
last_finished=null last_overall=null
if [[ -r "${LAST_RUN}" ]] && command -v jq >/dev/null 2>&1; then
  last_finished="$(jq -c '.finished // null' "${LAST_RUN}" 2>/dev/null || echo null)"
  last_overall="$(jq -c '.overall // null' "${LAST_RUN}" 2>/dev/null || echo null)"
fi

tmp="${OUT}.tmp.$$"
printf '{"checked_at":"%s","daemon_ok":%s,"last_run_finished":%s,"last_run_overall":%s}\n' \
  "${now}" "${daemon_ok}" "${last_finished}" "${last_overall}" > "${tmp}"
chmod 0644 "${tmp}"; mv -f "${tmp}" "${OUT}"

if [[ "${daemon_ok}" != "true" ]]; then
  echo "[heartbeat] dagster-daemon liveness-check FAILED at ${now}" >&2
  if [[ -n "${WEBHOOK}" ]] && command -v curl >/dev/null 2>&1; then
    curl -sS -m 10 -H 'Content-Type: application/json' \
      -d "{\"content\":\":rotating_light: dev nostrology dagster-daemon liveness-check FAILED at ${now} — schedules/sensors are NOT firing\"}" \
      "${WEBHOOK}" >/dev/null 2>&1 || true
  fi
  exit 1
fi
echo "[heartbeat] daemon OK at ${now}"
