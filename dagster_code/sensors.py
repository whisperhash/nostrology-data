"""Run-status sensors: the stable last-run.json freshness contract + Discord alerts.

last-run.json is the version-STABLE read surface for read-only accounts (it never has to parse Dagster's internal
tables): a small atomic JSON the way run.sh's status JSON was, written on every completed run.
The Discord failure sensor is the failure alert: it walks the lineage graph (pipeline/lineage.py,
the "consumers" asset group) from what the run did not refresh to the pages, landing panels and
MCP domains left stale, and posts that with the cause and when it heals (pipeline/alerts.py).
The recovery sensor posts once when a failing job succeeds again. The freshness sensor is the
watchdog over the per-job files: it posts when a job's last completed run is failed, missing or
older than 26 h - which is how a schedule tick that errored, a run stuck in the queue, or a run
that was never created becomes visible, none of which produce a FAILURE event. All of them are
daemon-hosted - a DEAD daemon writes and posts nothing, which is exactly why the out-of-band
heartbeat (deploy/dagster/heartbeat/, a systemd timer the deployment installs) exists: only an independent process catches "daemon
down -> no runs, no alerts", the one case the sensors structurally cannot self-report.

Every Discord post logs `discord: posted` or `discord: no webhook configured`, so Loki can tell
a silent webhook from an unconfigured one; the URL itself is never logged. Each deploy ends
with a probe that posts nothing (alert_selftest --probe) and says whether Discord accepts the
webhook from inside this container.
"""

import json
import os
import tempfile
import time
import urllib.request
from http import HTTPStatus

from dagster import (
    AssetKey,
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunsFilter,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    run_status_sensor,
    sensor,
)

from .pipeline import alerts
from .pipeline.freshness import freshness_status

LAST_RUN_JSON = os.environ.get("DAGSTER_LAST_RUN_JSON", "/mnt/nozzl-dagster/last-run.json")


def _iso(epoch: float | None) -> str | None:
    if not epoch:
        return None
    # Avoid importing tz juggling — format UTC explicitly.
    import datetime

    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _per_job_path(job_name: str) -> str:
    """Sibling of LAST_RUN_JSON, keyed by job -> per-surface freshness.

    With one job per surface (relays_refresh, nutzaps_refresh, …) the single LAST_RUN_JSON is
    last-write-WINS across all surfaces, so it can't answer "is /nutzaps fresh?" on its own. The
    per-job file is the stable per-surface signal a read-only account reads; LAST_RUN_JSON is kept as the
    global most-recent-run summary (back-compat with the Stage-0 heartbeat freshness check).
    """
    d = os.path.dirname(LAST_RUN_JSON)
    base = os.path.basename(LAST_RUN_JSON)
    stem, _, ext = base.rpartition(".")  # "last-run", ".", "json"
    return os.path.join(d, f"{stem or base}-{job_name}.{ext}" if stem else f"{base}-{job_name}")


def _atomic_write_json(path: str, body: str, context) -> None:
    try:
        d = os.path.dirname(path)
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".last-run.", suffix=".json", dir=d)
        with os.fdopen(fd, "w") as fh:
            fh.write(body)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)  # atomic — a reader never catches a half-write
    except OSError as exc:
        context.log.warning(f"last-run.json: write to {path} failed ({exc}) — run result unaffected")


def _write_last_run(context: RunStatusSensorContext, overall: str) -> None:
    run = context.dagster_run
    started = finished = None
    try:
        rec = context.instance.get_run_record_by_id(run.run_id)
        if rec is not None:
            started = _iso(rec.start_time)
            finished = _iso(rec.end_time)
    except Exception as exc:  # never let bookkeeping fail the sensor
        context.log.warning(f"last-run.json: could not read run record: {exc}")

    payload = {
        "job": run.job_name,
        "run_id": run.run_id,
        "overall": overall,  # "ok" | "fail" — mirrors run.sh's status JSON contract
        "started": started,
        "finished": finished,
    }
    body = json.dumps(payload) + "\n"
    # Per-surface file (stable freshness signal) + the global last-write-wins summary.
    _atomic_write_json(_per_job_path(run.job_name), body, context)
    _atomic_write_json(LAST_RUN_JSON, body, context)
    context.log.info(f"last-run.json -> {LAST_RUN_JSON} (+ per-job) job={run.job_name} overall={overall}")


# These sensors ship RUNNING (unlike the schedule, which ships STOPPED): they are pure
# observability bookkeeping — write last-run.json, post Discord on failure — NOT run triggers, so
# they must be live the moment the code location loads. OSS sensors default to STOPPED otherwise.
@run_status_sensor(run_status=DagsterRunStatus.SUCCESS, default_status=DefaultSensorStatus.RUNNING)
def last_run_success_sensor(context: RunStatusSensorContext) -> None:
    _write_last_run(context, "ok")


@run_status_sensor(run_status=DagsterRunStatus.FAILURE, default_status=DefaultSensorStatus.RUNNING)
def last_run_failure_sensor(context: RunStatusSensorContext) -> None:
    _write_last_run(context, "fail")


# Every request to Discord names itself, in the format Discord's API documents ("DiscordBot
# ($url, $versionNumber)"). Discord's edge answers urllib's default "Python-urllib/3.x" with
# 403 (forbidden), body "error code: 1010" - probed from the deployment's host and from inside
# this image on 2026-10-17 - so until then every post would have failed even with the webhook set.
DISCORD_USER_AGENT = "DiscordBot (https://nostrolo.gy, 1.0)"
DISCORD_HEADERS = {"Content-Type": "application/json", "User-Agent": DISCORD_USER_AGENT}


def http_status(code) -> str:
    """"403 (forbidden)": a status code always travels with its reason phrase (RFC 9110,
    lowercased), in log lines as everywhere else (CLAUDE.md, Writing conventions)."""
    try:
        return f"{int(code)} ({HTTPStatus(int(code)).phrase.lower()})"
    except (TypeError, ValueError):
        return f"{code} (no standard reason phrase)"


def _post_discord(context, msg, fallback: str = "") -> bool:
    """POST one message to DAGSTER_DISCORD_WEBHOOK (the alerts channel). `msg` is plain text or a
    webhook payload dict (an embed from pipeline/alerts.py); when Discord refuses a payload with a
    4xx (client errors) status and `fallback` text is given, that is posted instead, so a
    formatting fault cannot swallow an alert. Best-effort; returns True when something was posted.
    Logs the outcome (posted / no webhook configured / failed) so a reader of the logs can tell a silent
    webhook from an unconfigured one in Loki - never the URL, and never str(exc) of a urllib error,
    which can carry the URL (a secret)."""
    webhook = os.environ.get("DAGSTER_DISCORD_WEBHOOK", "").strip()
    if not webhook:
        context.log.warning("discord: no webhook configured (heartbeat.env) - message dropped")
        return False
    body = {"content": msg[:1900]} if isinstance(msg, str) else dict(msg)
    body.setdefault("allowed_mentions", {"parse": []})  # error text must never ping anyone
    try:
        req = urllib.request.Request(
            webhook,
            data=json.dumps(body).encode(),
            headers=DISCORD_HEADERS,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            context.log.info(f"discord: posted, HTTP {http_status(resp.status)}")
        return True
    except Exception as exc:  # never fail the sensor on a webhook hiccup
        code = getattr(exc, "code", "")
        context.log.warning(f"discord: post failed: {type(exc).__name__}"
                            + (f", HTTP {http_status(code)}" if isinstance(code, int) else ""))
        if fallback and isinstance(code, int) and 400 <= code < 500 and not isinstance(msg, str):
            return _post_discord(context, fallback)
        return False


def _first_step_failure(context: RunFailureSensorContext, limit: int = 400) -> tuple:
    """(step key, the failed step's own error: first non-empty line, clipped), so the alert says
    WHY - an anti-shrink refusal, an Athena cutoff, a schema error - not just that a run failed.
    Best-effort: ("", "") when the events carry no error."""
    try:
        for ev in context.get_step_failure_events():
            data = getattr(ev, "step_failure_data", None)
            err = getattr(data, "error", None)
            # Dagster wraps an exception raised in asset code in a
            # DagsterExecutionStepExecutionError whose own message only names the op; the
            # asset's error is its cause (dagster 1.9.11; a run of 2026-08-27).
            while (getattr(err, "cls_name", None) == "DagsterExecutionStepExecutionError"
                   and getattr(err, "cause", None) is not None):
                err = err.cause
            msg = (getattr(err, "message", None) or "").strip()
            if msg:
                line = next((ln.strip() for ln in msg.splitlines() if ln.strip()), "")
                return getattr(ev, "step_key", "?"), line[:limit]
    except Exception as exc:  # never let the detail break the alert
        context.log.warning(f"discord_failure_sensor: no step detail: {type(exc).__name__}")
    return "", ""


def _first_step_error(context: RunFailureSensorContext, limit: int = 400) -> str:
    """The same detail as one ' [step] error' suffix, for the plain-text fallback."""
    step, line = _first_step_failure(context, limit)
    return f" [{step}] {line}" if line else ""


# How many of a job's past runs the failure and recovery posts read for the streak.
HISTORY_WINDOW = 50


def _node(asset_key) -> str:
    return asset_key.to_user_string()


def _run_asset_nodes(instance, run_id: str) -> tuple:
    """(planned, materialized) asset node ids of one run, from its own event log."""
    planned, done = set(), set()
    for entry in instance.all_logs(run_id, of_type={
            DagsterEventType.ASSET_MATERIALIZATION_PLANNED, DagsterEventType.ASSET_MATERIALIZATION}):
        ev = entry.dagster_event
        if ev is None or ev.asset_key is None:
            continue
        (done if ev.event_type == DagsterEventType.ASSET_MATERIALIZATION else planned).add(_node(ev.asset_key))
    return planned, done


def _completed_runs(instance, job_name: str) -> list:
    """The job's finished runs (success or failure), newest first."""
    return list(instance.get_run_records(
        filters=RunsFilter(job_name=job_name,
                           statuses=[DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE]),
        limit=HISTORY_WINDOW))


def _run_times(instance, run_id: str) -> tuple:
    rec = instance.get_run_record_by_id(run_id)
    start = getattr(rec, "start_time", None)
    end = getattr(rec, "end_time", None) or time.time()
    return start, end, (end - start) if start else None


def make_discord_failure_sensor(upstream: dict, consumer_ids, labels: dict, crons: dict):
    """The failure post. `upstream` is the lineage graph (node id -> its upstream node ids, the
    data assets' deps plus the consumers' from pipeline/lineage.py), `crons` job -> its
    schedule's cron. Whatever goes wrong while gathering the detail, the plain one-liner is
    posted instead: an alert never depends on its decoration."""

    @run_failure_sensor(name="discord_failure_sensor", default_status=DefaultSensorStatus.RUNNING)
    def discord_failure_sensor(context: RunFailureSensorContext) -> None:
        run = context.dagster_run
        why = f"{context.failure_event.message}{_first_step_error(context)}"
        fallback = alerts.failure_text(run.job_name, run.run_id, why)
        try:
            inst = context.instance
            planned, done = _run_asset_nodes(inst, run.run_id)
            not_refreshed = planned - done
            last_good = {}
            for n in not_refreshed:
                latest = inst.get_latest_materialization_event(AssetKey.from_user_string(n))
                last_good[n] = getattr(latest, "timestamp", None)
            start, end, duration = _run_times(inst, run.run_id)
            step, cause = _first_step_failure(context)
            payload = alerts.failure_payload(
                job=run.job_name, run_id=run.run_id, ended=end, duration=duration,
                trigger=alerts.trigger_label(run.tags),
                streak=alerts.streak_text(
                    [r.dagster_run.status.value for r in _completed_runs(inst, run.job_name)],
                    HISTORY_WINDOW),
                not_refreshed=not_refreshed, refreshed=done, upstream=upstream,
                consumer_ids=consumer_ids, labels=labels, last_good=last_good,
                step=step, cause=cause or context.failure_event.message,
                next_run=alerts.next_daily_run(crons.get(run.job_name, ""), time.time()))
        except Exception as exc:  # the detail is best-effort; the alert is not
            context.log.warning(f"discord_failure_sensor: plain post ({type(exc).__name__}: {exc})")
            _post_discord(context, fallback)
            return
        _post_discord(context, payload, fallback=fallback)

    return discord_failure_sensor


def make_discord_recovery_sensor(upstream: dict, consumer_ids, labels: dict):
    """The recovery post: on a job's first success after one or more failures, say what is
    current again. Silent on every other success."""

    @run_status_sensor(run_status=DagsterRunStatus.SUCCESS, name="discord_recovery_sensor",
                       default_status=DefaultSensorStatus.RUNNING)
    def discord_recovery_sensor(context: RunStatusSensorContext) -> None:
        run = context.dagster_run
        try:
            inst = context.instance
            history = [r for r in _completed_runs(inst, run.job_name) if r.dagster_run.run_id != run.run_id]
            failed = []
            for r in history:  # newest first: the failures since the last success
                if r.dagster_run.status != DagsterRunStatus.FAILURE:
                    break
                failed.append(r)
            if not failed:
                return
            _planned, done = _run_asset_nodes(inst, run.run_id)
            _start, end, duration = _run_times(inst, run.run_id)
            payload = alerts.recovery_payload(
                job=run.job_name, run_id=run.run_id, ended=end, duration=duration,
                trigger=alerts.trigger_label(run.tags), refreshed=done, upstream=upstream,
                consumer_ids=consumer_ids, labels=labels, failed_runs=len(failed),
                failing_since=getattr(failed[-1], "start_time", None))
        except Exception as exc:
            context.log.warning(f"discord_recovery_sensor: plain post ({type(exc).__name__}: {exc})")
            payload = f"{alerts.OK_MARK} `{run.job_name}` recovered: run `{run.run_id}` succeeded"
        _post_discord(context, payload)

    return discord_recovery_sensor


FRESHNESS_INTERVAL_SECONDS = 30 * 60


def make_freshness_sensor(job_names):
    """The freshness watchdog over the per-job last-run files: one Discord line per (job,
    condition, UTC day), where the condition is `fail` (last completed run failed), `missing`
    (no file: the job never completed on this instance) or `stale` (last ok run older than
    26 h). Evaluated every 30 minutes; the cursor remembers what was announced today so a
    persisting condition is said once a day, not every half hour, and a job that recovers
    drops out of it. Daemon-hosted, so the one case it cannot see is a dead daemon."""
    names = list(job_names)

    @sensor(
        name="freshness_sensor",
        minimum_interval_seconds=FRESHNESS_INTERVAL_SECONDS,
        default_status=DefaultSensorStatus.RUNNING,
    )
    def freshness_sensor(context: SensorEvaluationContext):
        now = time.time()
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        try:
            state = json.loads(context.cursor) if context.cursor else {}
        except ValueError:
            state = {}
        if not isinstance(state, dict):
            state = {}
        lines = []
        for job in names:
            status, detail = freshness_status(_per_job_path(job), now)
            if status == "ok":
                state.pop(job, None)
                continue
            key = f"{status}:{day}"
            if state.get(job) == key:
                continue  # already announced today
            state[job] = key
            lines.append(f"`{job}`: {status} - {detail}")
        if lines:
            msg = f":warning: Dagster freshness ({day}): " + "; ".join(lines)
            context.log.warning(msg)
            _post_discord(context, msg)
        context.update_cursor(json.dumps(state, sort_keys=True))
        return SkipReason(f"checked {len(names)} jobs, {len(lines)} new problem(s)")

    return freshness_sensor
