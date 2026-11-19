"""sensors.py: the freshness watchdog and the Discord post path. Needs dagster (skips on a
workstation, runs in the local-image gate; CLAUDE.md has the commands)."""

import json
import time

import pytest

dg = pytest.importorskip("dagster", reason="dagster is not installed locally")
from dagster_code import sensors  # noqa: E402

H = 3600
NOW = 1_790_000_000.0


def _write(tmp_path, name, **rec):
    p = tmp_path / f"last-run-{name}.json"
    p.write_text(json.dumps(rec) + "\n")
    return str(p)


def _iso(t: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))


@pytest.fixture
def watched(tmp_path, monkeypatch):
    """Point the per-job files at tmp_path, freeze the clock, capture Discord posts."""
    monkeypatch.setattr(sensors, "LAST_RUN_JSON", str(tmp_path / "last-run.json"))
    posted = []
    monkeypatch.setattr(sensors, "_post_discord", lambda context, msg: posted.append(msg) or True)
    clock = {"now": NOW}
    monkeypatch.setattr(sensors.time, "time", lambda: clock["now"])
    return tmp_path, posted, clock


def test_freshness_sensor_posts_once_per_job_per_day(watched):
    tmp_path, posted, clock = watched
    _write(tmp_path, "a_refresh", job="a_refresh", run_id="r1", overall="ok", started=None, finished=_iso(NOW - H))
    _write(tmp_path, "b_refresh", job="b_refresh", run_id="r2", overall="fail", started=None, finished=_iso(NOW - H))
    sensor = sensors.make_freshness_sensor(["a_refresh", "b_refresh", "c_refresh"])
    ctx = dg.build_sensor_context()
    r1 = sensor(ctx)
    assert isinstance(r1, dg.SkipReason)
    # one post naming the failed job and the never-ran job; the fresh one is not mentioned
    assert len(posted) == 1
    assert "`b_refresh`: fail" in posted[0] and "`c_refresh`: missing" in posted[0]
    assert "a_refresh" not in posted[0]
    # the same day, the same state: nothing new, nothing posted
    ctx2 = dg.build_sensor_context(cursor=ctx.cursor)
    sensor(ctx2)
    assert len(posted) == 1
    # b recovers -> it leaves the cursor; c unchanged today -> still no new post
    _write(tmp_path, "b_refresh", job="b_refresh", run_id="r3", overall="ok", started=None, finished=_iso(NOW))
    ctx3 = dg.build_sensor_context(cursor=ctx2.cursor)
    sensor(ctx3)
    assert len(posted) == 1
    assert "b_refresh" not in json.loads(ctx3.cursor)
    assert json.loads(ctx3.cursor)["c_refresh"].startswith("missing:")
    # the next UTC day, c is announced again, and a (now 25 h old) is still fine
    clock["now"] = NOW + 24 * H
    ctx4 = dg.build_sensor_context(cursor=ctx3.cursor)
    sensor(ctx4)
    assert len(posted) == 2 and "`c_refresh`: missing" in posted[1] and "a_refresh" not in posted[1]
    # two hours later a is 27 h old -> stale, announced once
    clock["now"] = NOW + 26 * H + 1
    ctx5 = dg.build_sensor_context(cursor=ctx4.cursor)
    sensor(ctx5)
    assert len(posted) == 3 and "`a_refresh`: stale" in posted[2]


def test_freshness_sensor_survives_a_bad_cursor(watched):
    tmp_path, posted, clock = watched
    sensor = sensors.make_freshness_sensor(["x_refresh"])
    sensor(dg.build_sensor_context(cursor="{not json"))
    sensor(dg.build_sensor_context(cursor="[1,2]"))
    assert len(posted) == 2  # each evaluation started from an empty state and announced x


def test_freshness_sensor_is_registered_for_the_seven_surface_jobs():
    from dagster_code import definitions

    names = [s.name for s in definitions.defs.sensors]
    assert "freshness_sensor" in names
    fs = next(s for s in definitions.defs.sensors if s.name == "freshness_sensor")
    assert fs.default_status == dg.DefaultSensorStatus.RUNNING
    assert fs.minimum_interval_seconds == 30 * 60


def test_post_discord_without_webhook_logs_and_returns_false(monkeypatch):
    monkeypatch.delenv("DAGSTER_DISCORD_WEBHOOK", raising=False)
    ctx = dg.build_sensor_context()
    assert sensors._post_discord(ctx, "x") is False


def test_post_discord_never_logs_the_url(monkeypatch):
    monkeypatch.setenv("DAGSTER_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/SECRET")
    logged = []

    class Log:
        def info(self, m): logged.append(m)
        def warning(self, m): logged.append(m)

    class Ctx:
        log = Log()

    def boom(req, timeout):
        raise OSError("connect failed to https://discord.example/api/webhooks/SECRET")

    monkeypatch.setattr(sensors.urllib.request, "urlopen", boom)
    assert sensors._post_discord(Ctx(), "x") is False
    assert logged and all("SECRET" not in m for m in logged)
    assert any("post failed: OSError" in m for m in logged)


def test_first_step_error_reads_the_assets_error_under_dagsters_wrapper():
    """An exception raised in asset code reaches the failure sensor wrapped in a
    DagsterExecutionStepExecutionError whose message only names the op (dagster 1.9.11, as on
    a run of 2026-08-27); the post must carry the asset's own error, its cause."""
    from dagster._utils.error import SerializableErrorInfo as E

    def ctx(err):
        class Ev:
            step_key = "ngit_snapshot"
            step_failure_data = type("D", (), {"error": err})()

        class Ctx:
            log = type("L", (), {"warning": lambda self, m: None})()

            def get_step_failure_events(self):
                return [Ev()]
        return Ctx()

    cause = E("RuntimeError: held keys have kept announcement coordinates (nothing written): "
              "62ec33a4 406\n", [], "RuntimeError")
    wrapped = E('dagster._core.errors.DagsterExecutionStepExecutionError: Error occurred while '
                'executing op "ngit_snapshot":\n', [], "DagsterExecutionStepExecutionError", cause)
    assert sensors._first_step_error(ctx(wrapped)) == (
        " [ngit_snapshot] RuntimeError: held keys have kept announcement coordinates "
        "(nothing written): 62ec33a4 406")
    assert sensors._first_step_error(ctx(cause)).endswith("62ec33a4 406")   # unwrapped as is
    bare = E("dagster._core.errors.DagsterExecutionStepExecutionError: op failed\n", [],
             "DagsterExecutionStepExecutionError")                          # no cause: its own line
    assert sensors._first_step_error(ctx(bare)).endswith("op failed")


# ---- the lineage-aware failure post and the recovery post (2026-10-17) ----

def test_discord_sensors_are_registered_running():
    from dagster_code import definitions

    by_name = {s.name: s for s in definitions.defs.sensors}
    for name in ("discord_failure_sensor", "discord_recovery_sensor"):
        assert by_name[name].default_status == dg.DefaultSensorStatus.RUNNING


def test_consumers_are_external_assets_downstream_of_what_they_read():
    from dagster_code import definitions
    from dagster_code.pipeline import lineage

    graph = definitions.defs.get_repository_def().asset_graph
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    for c in lineage.CONSUMERS:
        cid = lineage.consumer_id(c["key"])
        assert cid in keys
        node = graph.get(dg.AssetKey.from_user_string(cid))
        assert not node.is_materializable
        assert {p.to_user_string() for p in node.parent_keys} == set(lineage.consumer_deps(c))
        assert all(p in keys for p in lineage.consumer_deps(c))  # no dangling dep


def test_the_alert_graph_reaches_zapstore_from_the_lightning_profiles_extract():
    from dagster_code import definitions
    from dagster_code.pipeline import alerts

    up = definitions._UPSTREAM
    assert up["marketplaces_snapshot"] == {"marketplaces_listings_extract",
                                           "marketplaces_shipping_extract",
                                           "marketplaces_deletions_extract"}
    reached = alerts.affected_consumers({"lightning_profiles_extract"}, up, definitions._CONSUMER_IDS)
    assert {"consumer/web/zapstore", "consumer/mcp/zapstore"} <= reached
    assert "consumer/web/marketplaces" not in reached


class _Resp:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_post_discord_sends_an_embed_that_cannot_mention(monkeypatch):
    monkeypatch.setenv("DAGSTER_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/SECRET")
    sent = []
    monkeypatch.setattr(sensors.urllib.request, "urlopen",
                        lambda req, timeout: sent.append(json.loads(req.data)) or _Resp())
    assert sensors._post_discord(dg.build_sensor_context(), {"embeds": [{"title": "t"}]}) is True
    assert sent == [{"embeds": [{"title": "t"}], "allowed_mentions": {"parse": []}}]
    assert sensors._post_discord(dg.build_sensor_context(), "plain") is True
    assert sent[1] == {"content": "plain", "allowed_mentions": {"parse": []}}


def test_a_refused_embed_falls_back_to_the_plain_line(monkeypatch):
    import urllib.error

    monkeypatch.setenv("DAGSTER_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/SECRET")
    sent = []

    def urlopen(req, timeout):
        body = json.loads(req.data)
        sent.append(body)
        if "embeds" in body:
            raise urllib.error.HTTPError("https://discord.example", 400, "Bad Request", None, None)
        return _Resp()

    monkeypatch.setattr(sensors.urllib.request, "urlopen", urlopen)
    assert sensors._post_discord(dg.build_sensor_context(), {"embeds": [{}]}, fallback="plain") is True
    assert [("embeds" in b, b.get("content")) for b in sent] == [(True, None), (False, "plain")]


def test_every_post_names_itself_to_discord(monkeypatch):
    """Discord's edge answers urllib's default User-Agent with 403 (forbidden), body "error code:
    1010" (probed from the deployment's host and from this image, 2026-10-17): without an explicit
    one no post ever landed."""
    monkeypatch.setenv("DAGSTER_DISCORD_WEBHOOK", "https://discord.example/api/webhooks/SECRET")
    seen, logged = [], []
    monkeypatch.setattr(sensors.urllib.request, "urlopen",
                        lambda req, timeout: seen.append((req.get_method(), req.get_header("User-agent"))) or _Resp())
    ctx = type("Ctx", (), {"log": type("Log", (), {"info": lambda self, m: logged.append(m)})()})()
    assert sensors._post_discord(ctx, {"embeds": [{"title": "t"}]}) is True
    assert logged == ["discord: posted, HTTP 204 (no content)"]
    assert sensors._post_discord(dg.build_sensor_context(), "plain") is True
    assert seen == [("POST", "DiscordBot (https://nostrolo.gy, 1.0)")] * 2


def test_a_status_code_travels_with_its_reason_phrase():
    assert [sensors.http_status(c) for c in (200, 204, 403, 404, 502, 520)] == [
        "200 (ok)", "204 (no content)", "403 (forbidden)", "404 (not found)", "502 (bad gateway)",
        "520 (no standard reason phrase)"]


class _Webhook(_Resp):
    status = 200

    def read(self, *a):
        return json.dumps({"name": "nostrology-dagster (dev)", "channel_id": "1234", "token": "SECRET",
                           "url": "https://discord.example/api/webhooks/1/SECRET"}).encode()


def test_the_probe_names_each_state_and_never_the_secret(monkeypatch, capsys):
    import io
    import urllib.error

    from dagster_code import alert_selftest

    url = "https://discord.example/api/webhooks/1/SECRET"
    seen = []

    def answer(outcome):
        def urlopen(req, timeout):
            seen.append((req.get_method(), req.data, req.get_header("User-agent")))
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        monkeypatch.setattr(sensors.urllib.request, "urlopen", urlopen)

    def refusal(code, body):
        return urllib.error.HTTPError(url, code, "refused", None, io.BytesIO(body))

    monkeypatch.delenv("DAGSTER_DISCORD_WEBHOOK", raising=False)
    got = [alert_selftest.probe()]
    monkeypatch.setenv("DAGSTER_DISCORD_WEBHOOK", url)
    for outcome in (_Webhook(), refusal(403, b"error code: 1010\n"),
                    refusal(404, b'{"message": "Unknown Webhook", "code": 10015}'),
                    OSError(f"connect to {url} failed")):
        answer(outcome)
        got.append(alert_selftest.probe())
    assert got == [
        (False, "Discord alerts OFF: DAGSTER_DISCORD_WEBHOOK is empty in this container "
                "(no readable heartbeat.env at deploy time) - every post is dropped"),
        (True, "Discord alerts ON: Discord accepts the webhook - HTTP 200 (ok), webhook "
               "'nostrology-dagster (dev)', channel '1234'"),
        (False, "Discord alerts BROKEN: Discord refused the request - HTTP 403 (forbidden), edge error"
                " code 1010 - every post will fail"),
        (False, "Discord alerts BROKEN: Discord refused the request - HTTP 404 (not found), Discord"
                " code 10015 - every post will fail"),
        (False, "Discord alerts UNVERIFIED: no answer from Discord (OSError)"),
    ]
    # A GET without a body posts nothing, and it goes out with the post's own User-Agent.
    assert seen == [("GET", None, sensors.DISCORD_USER_AGENT)] * 4
    assert alert_selftest.main(["--probe"]) == 1
    assert capsys.readouterr().out == "Discord alerts UNVERIFIED: no answer from Discord (OSError)\n"
    answer(_Webhook())
    assert alert_selftest.main(["--probe"]) == 0
    assert capsys.readouterr().out.startswith("Discord alerts ON: ")


_FAIL = {"on": True}


@dg.asset
def tiny_extract():
    return 1


@dg.asset(deps=[tiny_extract])
def tiny_table():
    if _FAIL["on"]:
        raise RuntimeError("refusing to load SHORT: anti-shrink gate")


def test_failure_then_recovery_posts_end_to_end_on_a_real_instance(monkeypatch):
    """Runs a real two-asset job on an ephemeral instance, fails it, then fixes it, and drives
    both sensors with Dagster's own contexts: the instance reads (planned vs materialized events,
    the latest materialization, the run history, the run record) are the ones production makes."""
    posted = []
    monkeypatch.setattr(sensors, "_post_discord",
                        lambda context, msg, fallback="": posted.append(msg) or True)
    job = dg.define_asset_job("tiny_refresh", selection=[tiny_extract, tiny_table])
    tiny_defs = dg.Definitions(assets=[tiny_extract, tiny_table], jobs=[job])
    up = {"tiny_extract": set(), "tiny_table": {"tiny_extract"}, "consumer/web/tiny": {"tiny_table"}}
    labels = {"consumer/web/tiny": ("web", "/tiny", "https://nostrolo.gy/tiny")}
    failure = sensors.make_discord_failure_sensor(up, ["consumer/web/tiny"], labels, {"tiny_refresh": "15 2 * * *"})
    recovery = sensors.make_discord_recovery_sensor(up, ["consumer/web/tiny"], labels)

    instance = dg.DagsterInstance.ephemeral()
    _FAIL["on"] = True
    bad = tiny_defs.get_job_def("tiny_refresh").execute_in_process(instance=instance, raise_on_error=False)
    assert not bad.success
    failure(dg.build_run_status_sensor_context(
        sensor_name="discord_failure_sensor", dagster_event=bad.get_job_failure_event(),
        dagster_instance=instance, dagster_run=bad.dagster_run).for_run_failure())
    (e,) = posted[-1]["embeds"]
    assert e["title"] == "\u274c tiny_refresh failed: 1 surface affected"
    assert "web: [/tiny](https://nostrolo.gy/tiny)" in e["description"]
    assert "`tiny_table` never" in e["description"]           # planned, not materialized, never before
    assert "[tiny_table] RuntimeError: refusing to load SHORT" in e["description"]
    env, run_id, trigger, _duration, streak = e["footer"]["text"].split(" | ")
    assert (env, run_id, trigger, streak) == ("dev = nostrolo.gy", f"run {bad.run_id[:8]}", "manual", "first run")

    _FAIL["on"] = False
    good = tiny_defs.get_job_def("tiny_refresh").execute_in_process(instance=instance)
    recovery(dg.build_run_status_sensor_context(
        sensor_name="discord_recovery_sensor", dagster_event=good.get_job_success_event(),
        dagster_instance=instance, dagster_run=good.dagster_run))
    (r,) = posted[-1]["embeds"]
    assert r["title"] == "\u2705 tiny_refresh recovered: 1 surface current again"
    assert "1 failed run in a row" in r["description"]

    n = len(posted)
    again = tiny_defs.get_job_def("tiny_refresh").execute_in_process(instance=instance)
    recovery(dg.build_run_status_sensor_context(
        sensor_name="discord_recovery_sensor", dagster_event=again.get_job_success_event(),
        dagster_instance=instance, dagster_run=again.dagster_run))
    assert len(posted) == n  # an ordinary success after a success says nothing


def test_the_selftest_samples_are_marked_and_valid(capsys):
    from dagster_code import alert_selftest
    from dagster_code.pipeline import alerts

    fail, ok = alert_selftest.samples()
    for p in (fail, ok):
        (e,) = p["embeds"]
        assert e["title"].startswith("TEST - ")
        assert e["footer"]["text"].endswith("test post: a replay of 2026-10-17")
        assert len(e["description"]) <= alerts.DESC_MAX and p["allowed_mentions"] == {"parse": []}
    assert "web: [/marketplaces](https://nostrolo.gy/marketplaces)" in fail["embeds"][0]["description"]
    assert "landing: marketplaces" in fail["embeds"][0]["description"]
    assert ok["embeds"][0]["title"] == "TEST - \u2705 marketplaces_refresh recovered: 2 surfaces current again"
    assert alert_selftest.main(["--dry-run"]) == 0 and '"TEST - ' in capsys.readouterr().out
