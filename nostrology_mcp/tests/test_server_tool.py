"""In-process tests of the wired MCP tool: schema generation, routing between the
two metrics, format switching, SDK-level validation, error paths, and the call-log
side effect. No live DB, no subprocess - server.call_tool() end-to-end."""

import json
import os
from datetime import date

import anyio
import pytest

from nostrology_mcp import server as srv_mod
from nostrology_mcp.asgilog import HttpCallLogMiddleware
from nostrology_mcp.inflight import DuplicateRequestIdMiddleware
from nostrology_mcp.recovery import RecoveryHintMiddleware
from nostrology_mcp.db import MetricResult
from nostrology_mcp.metric import DOMAIN_NAMES, GRAIN_NAMES, METRIC_NAMES, PAID_METRIC_NAMES
from nostrology_mcp.reference import lookup_record as reference_lookup

ROWS = [(date(2026, 1, 2), 7), (date(2026, 1, 3), 4)]
AS_OF = 1785349327  # 2026-07-29T18:22:07Z


def _call(arguments):
    async def go():
        return await srv_mod.server.call_tool("nostrology_query_metric", arguments)

    return anyio.run(go)


def _tool(name: str = "nostrology_query_metric"):
    async def go():
        return await srv_mod.server.list_tools()

    (tool,) = [t for t in anyio.run(go) if t.name == name]
    return tool


def _dummy_row(spec):
    """A row matching the spec's column contract - the renderer zips strictly, so a
    stub that returns two-column rows for a seven-column spec is itself a bug."""
    vals = []
    for c in spec.columns:
        if c.type == "date":
            vals.append(date(2026, 1, 2))
        elif c.type in ("integer", "timestamp", "number"):
            vals.append(7)
        elif c.type == "boolean":
            vals.append(True)
        else:
            vals.append("x")
    return tuple(vals)


@pytest.fixture
def stub_db(monkeypatch, tmp_path):
    """Records which spec the tool resolved to, so routing is observable without a
    DB. Legacy two-column specs get the exact ROWS the snapshot assertions pin;
    wider (leaderboard) specs get shape-matching dummy rows."""
    seen = []
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    def fake_fetch(spec, sql=None, params=None):
        seen.append((spec, sql, params))
        rows = ROWS if len(spec.columns) == 2 else [_dummy_row(spec), _dummy_row(spec)]
        if getattr(spec, "row_cap", None) is not None:
            rows = [r + (len(rows),) for r in rows]  # the wrapper's __total_rows
        return MetricResult(rows=rows, as_of_epoch=AS_OF, connect_ms=1, query_ms=2)

    monkeypatch.setattr(srv_mod, "fetch_metric", fake_fetch)
    return tmp_path, seen


def _log_records(tmp_path):
    files = [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]
    assert len(files) == 1
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


# -- schema ----------------------------------------------------------------------


def _branch_values(prop: dict) -> list[str]:
    """The values a branch-shaped parameter advertises (one single-value `enum`
    branch each, plus the null omit branch which carries no value)."""
    return [b["enum"][0] for b in prop["anyOf"] if "enum" in b]


def test_tool_schema_enumerates_every_legal_value():
    """The model picks values off the inputSchema, so every legal value must be
    advertised there - and derived from the registry, never retyped.

    `grain` carries its values one-per-branch rather than in a sibling `enum`
    (see inputschema.py); this pins that the swap did not narrow the advertised
    set, which is the thing that would silently cost a caller a legal grain. The
    field-level description carries the same list in prose - that is the only
    flat copy left for a client that transcodes the schema and drops `anyOf`, so
    it is pinned here too."""
    props = _tool().input_schema["properties"]
    assert props["metric"]["enum"] == list(METRIC_NAMES)
    assert props["domain"]["enum"] == list(DOMAIN_NAMES)
    assert _branch_values(props["grain"]) == list(GRAIN_NAMES)
    assert props["format"]["enum"] == ["csv", "markdown"]
    for grain in GRAIN_NAMES:
        assert grain in props["grain"]["description"]


def test_the_schema_enum_advertises_without_enforcing(stub_db):
    """THE load-bearing distinction, and the one easiest to undo by "tidying" the
    signature back to `Literal[...]`.

    The enum must be ADVERTISED (guidance for the model) but must NOT be ENFORCED by
    pydantic: a Literal rejects an off-list value above the tool body, so the ask
    reaches no log at all - and because a JSON-RPC error is still HTTP 200, the
    access log cannot see it either. `Annotated[str, Field(json_schema_extra=...)]`
    gets both. This test pins that an off-list value really does reach our code."""
    tmp_path, seen = stub_db
    props = _tool().input_schema["properties"]
    for advertised in ("metric", "domain"):
        assert props[advertised]["type"] == "string", f"{advertised} must not be a Literal"
    # grain is nullable and branch-shaped, but every branch is still a plain
    # string - the annotation is `str | None`, never a Literal.
    assert {b.get("type") for b in props["grain"]["anyOf"]} == {"string", "null"}

    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError):
        _call({"metric": "off_list_value"})
    assert not seen, "an unsupported ask must be rejected without querying"
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["metric"] == "off_list_value", "the ask reached the body and was recorded"


def test_every_registry_metric_is_reachable_through_the_tool(stub_db):
    """The other half of the drift guard. The enum is derived from METRICS, so an
    enum entry with no metric behind it is impossible; this covers the reverse - a
    registered metric the tool cannot actually serve."""
    _, seen = stub_db
    for name in METRIC_NAMES:
        res = _call({"metric": name})
        assert res.is_error is False, f"{name} is registered but not servable"
    from nostrology_mcp.metric import default_grain as _dg, resolve_domain as _rd
    # `domain` was left at its zapstore default on every call: the Routstr
    # names resolve to their own domain by name (resolve_domain).
    assert [t[0].key for t in seen] == [
        f"{_rd('zapstore', n)}.{n}@{_dg(_rd('zapstore', n), n)}" for n in METRIC_NAMES
    ]


def test_rejection_message_lists_every_legal_metric(stub_db):
    """The error IS the schema now. With no enum for the model to read, a wrong
    guess must come back with the full list of legal combinations so the caller can
    self-correct in one turn instead of guessing again."""
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "total_zaps"})  # plausible, not real
    msg = str(exc.value)
    for name in METRIC_NAMES:
        assert name in msg, f"a caller who guesses wrong is never told about {name}"


def test_metric_is_the_only_required_parameter():
    """With nine metrics that count different things, an under-specified call must
    fail loudly rather than silently resolve to one of them."""
    schema = _tool().input_schema
    props = schema["properties"]
    assert schema["required"] == ["metric"]
    assert "default" not in props["metric"]
    for p in ("domain", "format"):
        assert "default" in props[p]
    # `grain` carries no `default` any more: it is optional by absence from
    # `required`, and its null branch says so in words. A `default: null` beside
    # a value list is what made the form unsubmittable (inputschema.py).
    assert "default" not in props["grain"]


def test_tool_description_routes_between_every_metric():
    """The description IS the routing logic - the only thing the model reads when
    deciding which metric answers the question. Every metric name must appear, both
    partitions must be named, and the apps-vs-releases trap (new_native_app_count
    is NOT native_release_count) must be called out explicitly."""
    desc = _tool().description
    for name in METRIC_NAMES:
        assert name in desc, f"{name} is unreachable: the model never sees it"
    assert "GitHub / F-Droid" in desc
    assert "zapstore-cli" in desc
    assert "bounded above by native_release_count" in desc
    # The zaps family is a different ROW population, not the publishing partition
    # sliced another way - if the description implies otherwise the model will try
    # to reconcile the two families' totals. It must NOT overstate this as
    # "unrelated": 174 of 175 zapped apps also appear in the releases table, so the
    # app populations overlap almost entirely even though the rows never do.
    assert "different ROW population" in desc
    assert "never reconcile" in desc
    assert "largely the same apps" in desc
    # The 1000x unit trap must be visible at ROUTING time, not only after the
    # call: a model asked for "sats" that reads only the name picks zapped_msats
    # and reports the number as sats.
    assert "MILLI-satoshis" in desc and "divide by 1000" in desc


# -- happy paths -----------------------------------------------------------------


def test_happy_path_csv(stub_db):
    tmp_path, seen = stub_db
    res = _call({"metric": "new_native_app_count"})
    assert res.is_error is False
    (content,) = res.content
    text = content.text
    assert text.startswith("metric: zapstore.new_native_app_count")
    assert "2026-07-29T18:22:07Z" in text
    assert text.endswith("utc_day,new_native_app_count\n2026-01-02,7\n2026-01-03,4\n")

    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "ok"
    assert rec["rows"] == 2
    assert rec["resolved"]["sql_key"] == "zapstore.new_native_app_count@day"
    assert rec["bytes"] == len(text.encode("utf-8"))
    assert set(rec["ms"]) == {"connect", "query", "render", "total"}
    assert [t[0].key for t in seen] == ["zapstore.new_native_app_count@day"]
    # no window asked -> the byte-identical legacy path: no override SQL, no params
    assert seen[0][1] is None and seen[0][2] is None


def test_happy_path_markdown(stub_db):
    tmp_path, _ = stub_db
    res = _call({"metric": "new_native_app_count", "format": "markdown"})
    assert "| utc_day | new_native_app_count |" in res.content[0].text
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["format"] == "markdown"


def test_republished_metric_routes_to_its_own_spec(stub_db):
    """The whole point of a parameterized tool: a different `metric` must reach a
    different spec, and every layer downstream (SQL, envelope, table header, log)
    must follow it rather than the first metric."""
    tmp_path, seen = stub_db
    res = _call({"metric": "republished_release_count"})
    text = res.content[0].text
    assert text.startswith("metric: zapstore.republished_release_count")
    assert "unit: releases" in text
    assert "releases, not distinct apps" in text  # its own methodology, not the other's
    assert text.endswith("utc_day,republished_release_count\n2026-01-02,7\n2026-01-03,4\n")

    ((spec, _sql, _params),) = seen
    assert spec.key == "zapstore.republished_release_count@day"
    assert "pubkey = %(curator)s" in spec.sql  # the republished predicate reached the DB layer

    (rec,) = _log_records(tmp_path)
    assert rec["params"]["metric"] == "republished_release_count"
    assert rec["resolved"]["sql_key"] == "zapstore.republished_release_count@day"


def test_both_metrics_in_one_session_stay_independent(stub_db):
    """Two calls in one process must not leak state (a shared bind dict, a cached
    spec, a memoized preamble) from the first into the second."""
    tmp_path, seen = stub_db
    first = _call({"metric": "new_native_app_count"}).content[0].text
    second = _call({"metric": "republished_release_count"}).content[0].text

    # Assert on identity line + table header, NOT on mere substring presence: each
    # methodology names the other metric on purpose, so "the other name is absent"
    # is not a valid independence check.
    assert first.startswith("metric: zapstore.new_native_app_count  |")
    assert first.endswith("utc_day,new_native_app_count\n2026-01-02,7\n2026-01-03,4\n")
    assert second.startswith("metric: zapstore.republished_release_count  |")
    assert second.endswith("utc_day,republished_release_count\n2026-01-02,7\n2026-01-03,4\n")
    assert [t[0].key for t in seen] == [
        "zapstore.new_native_app_count@day",
        "zapstore.republished_release_count@day",
    ]
    assert [r["resolved"]["sql_key"] for r in _log_records(tmp_path)] == [
        "zapstore.new_native_app_count@day",
        "zapstore.republished_release_count@day",
    ]


# -- validation ------------------------------------------------------------------


def test_unsupported_grain_is_recorded_then_rejected(stub_db, monkeypatch):
    """"Someone wanted an hourly series" is demand data, and it used to be thrown
    away: the Literal made pydantic reject above the tool body, so the ask reached
    no log at all. Now it is recorded FIRST and rejected second - and the DB is
    still never touched."""
    tmp_path, _ = stub_db
    called = []
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: called.append(1))
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "new_native_app_count", "grain": "month"})
    assert "day" in str(exc.value)  # the error names the legal value
    assert not called, "an unsupported ask must be rejected without querying"

    (rec,) = _log_records(tmp_path)
    assert rec["kind"] == "tool"
    assert rec["outcome"] == "invalid_grain"
    assert rec["params"]["grain"] == "month"  # the ask itself, not just that it failed
    assert rec["resolved"]["sql_key"] is None
    assert rec["rows"] is None and rec["bytes"] is None


def test_unknown_metric_name_is_recorded_then_rejected(stub_db, monkeypatch):
    tmp_path, _ = stub_db
    called = []
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: called.append(1))
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "republished_app_count"})  # plausible but wrong
    msg = str(exc.value)
    assert "new_native_app_count" in msg and "republished_release_count" in msg
    assert not called

    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "unknown_metric"
    assert rec["params"]["metric"] == "republished_app_count"


def test_unknown_domain_is_recorded_then_rejected(stub_db, monkeypatch):
    """The highest-value ask of all - a caller wanting a dataset family we do not
    publish at all. Nothing downstream can act on it if it is not written down."""
    tmp_path, _ = stub_db
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: pytest.fail("must not query"))
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError):
        _call({"metric": "channel_count", "domain": "lightning"})
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "unknown_metric"
    assert rec["params"]["domain"] == "lightning"
    assert "domain_resolved" not in rec["params"]


def test_a_known_metric_under_a_wrong_domain_resolves_and_records_the_ask(stub_db):
    """A metric NAME is unique across domains, so a wrong or defaulted domain is
    corrected rather than rejected - the model's ask is still written down as
    sent (params.domain) next to what it resolved to."""
    tmp_path, seen = stub_db
    res = _call({"metric": "zap_count", "domain": "lightning"})
    assert res.is_error is False
    assert seen[-1][0].key == "zapstore.zap_count@day"
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["domain"] == "lightning"
    assert rec["params"]["domain_resolved"] == "zapstore"


def test_missing_required_metric_is_rejected(stub_db, monkeypatch):
    """The zero-argument call is no longer valid, and must fail at the SDK boundary
    rather than resolving to a default series. `metric` stayed REQUIRED when it was
    widened to `str`, so pydantic still rejects this one above the tool body.

    Hence no kind="tool" record: `server.call_tool()` is the direct handler path and
    does not run the dispatcher's middleware chain. Over a real connection this call
    still leaves a kind="rpc" record with outcome="tool_error" - that is the point of
    the middleware, and it is asserted in test_middleware.py, not here."""
    tmp_path, _ = stub_db
    called = []
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: called.append(1))
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({})
    assert "metric" in str(exc.value)
    assert not called
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]


# -- error paths -----------------------------------------------------------------


def test_db_error_instant_failure_hints_at_tunnel(monkeypatch, tmp_path):
    """An instant refusal is v0's most likely failure (SSH tunnel down) - the hint
    must say so and must NOT tell the agent to retry."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    def boom(spec, **kw):
        raise ConnectionError("connection refused")

    monkeypatch.setattr(srv_mod, "fetch_metric", boom)
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "new_native_app_count"})
    msg = str(exc.value)
    assert "tunnel" in msg
    assert "Retrying will not help" in msg
    assert "retry once" not in msg  # must NOT suggest the retry path
    # The timeout branch no longer blames a cold start: since 2026-08-03 Aurora
    # holds a warm floor and never pauses, so "resuming from zero capacity" would
    # misdiagnose a real failover/network fault as a benign wait.
    assert "resuming" not in msg

    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "db_error"
    assert rec["error"] == "ConnectionError: connection refused"
    assert rec["rows"] is None


def test_db_error_logs_the_metric_that_failed(monkeypatch, tmp_path):
    """A failure record must be attributable to a metric, or the log cannot tell
    which series is broken once there is more than one."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: (_ for _ in ()).throw(TimeoutError("x")))
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError):
        _call({"metric": "republished_release_count"})
    (rec,) = _log_records(tmp_path)
    assert rec["resolved"]["sql_key"] == "zapstore.republished_release_count@day"
    assert rec["params"]["metric"] == "republished_release_count"
    assert rec["error"] == "TimeoutError: x"


def test_db_error_record_scrubs_connection_identifiers(monkeypatch, tmp_path):
    """The log copy of a DB failure must not name the host/user/database. The prose
    that crosses to the CLIENT was always ours alone; this pins the OTHER half - the
    record that ships to Loki and is read by the sandboxed observer.

    Asserts the diagnosis survives too: scrubbing that ate the error message would
    trade one blind spot for another."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("NOSTROLOGY_PGHOST", "nostrology-example.cluster-abc.rds.amazonaws.com")
    monkeypatch.setenv("NOSTROLOGY_PGUSER", "nostrology_mcp")
    monkeypatch.setenv("NOSTROLOGY_PGDATABASE", "nostrology")

    def boom(spec, **kw):
        raise ConnectionError(
            'connection to server at "nostrology-example.cluster-abc.rds.amazonaws.com", port 5432 '
            'failed: FATAL:  no pg_hba.conf entry for user "nostrology_mcp"'
        )

    monkeypatch.setattr(srv_mod, "fetch_metric", boom)
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError):
        _call({"metric": "zap_count"})

    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "db_error"
    assert "rds.amazonaws.com" not in json.dumps(rec)
    assert "nostrology_mcp" not in json.dumps(rec)
    assert "<host>" in rec["error"] and "<user>" in rec["error"]
    assert "no pg_hba.conf entry" in rec["error"]  # the diagnosis survives


def test_git_sha_degrades_to_unknown_rather_than_failing(monkeypatch):
    """Provenance is best-effort: a server started outside a git checkout (or with
    git missing) must still serve. The preamble and every log record then carry
    'unknown' rather than the process dying at import."""
    import subprocess

    def boom(*a, **kw):
        raise FileNotFoundError("git")

    monkeypatch.setattr(subprocess, "run", boom)
    assert srv_mod._git_sha() == "unknown"


def test_main_defaults_to_stdio_transport(monkeypatch):
    """stdio must stay the default: the existing local registrations spawn
    `python -m nostrology_mcp.server` with no env and expect the JSON-RPC stream
    on stdout - an HTTP default would hang every one of them at spawn."""
    monkeypatch.delenv("NOSTROLOGY_MCP_TRANSPORT", raising=False)
    got = []
    monkeypatch.setattr(srv_mod.server, "run", lambda transport, **kw: got.append((transport, kw)))
    srv_mod.main()
    assert got == [("stdio", {})]


def test_main_streamable_http_reads_env_and_mounts_at_mcp(monkeypatch):
    """The container contract: NOSTROLOGY_MCP_TRANSPORT/HOST/PORT drive the HTTP
    server, and the path MUST be /mcp - it is what Caddy proxies and what every
    published registration URL ends in.

    main() builds the ASGI app and serves it itself (rather than calling
    `server.run("streamable-http")`) so the HTTP call-log middleware can wrap the
    app from outside; both halves are asserted here. BOTH `streamable_http_app`
    and `uvicorn.run` are stubbed - the real ones bind a socket, and a unit test
    that listens on 0.0.0.0:8080 is a test that fails on a busy machine."""
    monkeypatch.setenv("NOSTROLOGY_MCP_TRANSPORT", "streamable-http")
    monkeypatch.setenv("NOSTROLOGY_MCP_HOST", "0.0.0.0")
    monkeypatch.setenv("NOSTROLOGY_MCP_PORT", "8080")

    class _App:
        def __init__(self):
            self.middleware = []

        def add_middleware(self, cls, **kw):
            self.middleware.append((cls, kw))

    app = _App()
    built = []
    served = []
    monkeypatch.setattr(
        srv_mod.server, "streamable_http_app", lambda **kw: (built.append(kw), app)[1]
    )
    monkeypatch.setattr("uvicorn.run", lambda a, **kw: served.append((a, kw)))
    srv_mod.main()

    (kw,) = built
    assert kw["streamable_http_path"] == "/mcp"
    assert kw["json_response"] is True
    # Must be the BIND host: the SDK turns on DNS-rebinding Host/Origin validation
    # for a loopback bind, which answers 421 behind Caddy.
    assert kw["host"] == "0.0.0.0"

    ((served_app, served_kw),) = served
    assert served_app is app
    assert served_kw["host"] == "0.0.0.0" and served_kw["port"] == 8080

    # The seams this indirection exists for, in registration order. Starlette's stack is
    # built inside-out - the LAST add_middleware is the OUTERMOST - so this list read
    # bottom-up is the order a request meets them:
    #
    #   HttpCallLogMiddleware         (outermost) records what the client SAW
    #   DuplicateRequestIdMiddleware              serializes a colliding JSON-RPC id
    #   RecoveryHintMiddleware        (innermost) decorates the SDK's own rejections
    #
    # Order is asserted, not just membership, because both relationships are load-
    # bearing: the call log must be outermost or it records a status the recovery seam
    # has not finished rewriting, and duplicate-id must sit ABOVE recovery because it
    # authors a complete body including `error.data` that the rewriter would only have
    # to decline.
    ((recovery_cls, recovery_kw), (dup_cls, dup_kw), (log_cls, log_kw)) = app.middleware
    assert recovery_cls is RecoveryHintMiddleware
    assert dup_cls is DuplicateRequestIdMiddleware
    assert log_cls is HttpCallLogMiddleware
    assert recovery_kw["path"] == "/mcp" and dup_kw["path"] == "/mcp" and log_kw["path"] == "/mcp"


def test_main_streamable_http_does_not_call_server_run(monkeypatch):
    """Guard against a well-meaning revert to `server.run("streamable-http")`:
    that path builds and serves the app in one call with no seam between them, so
    the HTTP call-log middleware silently stops being installed and the blind spot
    it closes silently reopens."""
    monkeypatch.setenv("NOSTROLOGY_MCP_TRANSPORT", "streamable-http")
    monkeypatch.setattr(
        srv_mod.server, "run", lambda *a, **kw: pytest.fail("main() must not call server.run() for HTTP")
    )
    monkeypatch.setattr("uvicorn.run", lambda a, **kw: None)
    srv_mod.main()


def test_main_rejects_unknown_transport(monkeypatch):
    """Fail loud at boot, not by silently serving the wrong transport."""
    monkeypatch.setenv("NOSTROLOGY_MCP_TRANSPORT", "sse")  # exists in the SDK, not offered here
    monkeypatch.setattr(srv_mod.server, "run", lambda *a, **kw: None)
    with pytest.raises(ValueError):
        srv_mod.main()


def test_git_sha_prefers_the_baked_env(monkeypatch):
    """The container has no git binary or .git dir - the image bakes the SHA into
    NOSTROLOGY_MCP_GIT_SHA at build. It must win over the subprocess path (which
    inside the container would return 'unknown' and break attribution)."""
    monkeypatch.setenv("NOSTROLOGY_MCP_GIT_SHA", "deadbee")
    assert srv_mod._git_sha() == "deadbee"


# -- the non-tool surfaces: /health, /registry.json, nostrology://registry --------


def _route_paths():
    return {r.path for r in srv_mod.server._custom_starlette_routes}


def test_health_and_registry_routes_are_registered():
    assert {"/health", "/registry.json"} <= _route_paths()


def test_health_is_db_free_and_names_the_build(monkeypatch):
    """/health must never touch Aurora (server.health's docstring says why), so this
    must succeed with the DB layer hard-broken."""
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: (_ for _ in ()).throw(AssertionError))

    async def go():
        return await srv_mod.health(None)

    resp = anyio.run(go)
    body = json.loads(resp.body)
    assert body["status"] == "ok"
    assert body["version"] == srv_mod.__version__
    assert body["git_sha"] == srv_mod.GIT_SHA


def test_registry_json_serves_every_metric_with_both_renderings(monkeypatch):
    """The single-authored-source contract: the registry carries the PARTS
    (summary/partition/caveats) for the web AND the joined methodology the MCP
    envelope uses, and they must agree - the web page and the tool response can
    never drift because both are derived here."""
    monkeypatch.setattr(srv_mod, "fetch_metric", lambda spec, **kw: (_ for _ in ()).throw(AssertionError))

    async def go():
        return await srv_mod.registry_json(None)

    body = json.loads(anyio.run(go).body)
    assert body["v"] == 2
    # v1 compatibility key (an older web reads the metric half unchanged) plus
    # the authoritative v2 tools map.
    assert body["tool"] == "nostrology_query_metric"
    assert body["tools"] == {
        "metrics": "nostrology_query_metric",
        "reference": "nostrology_query_reference",
        "nutzaps": "nostrology_query_nutzaps",
    }
    assert body["server"]["name"] == "nostrology"
    # The registry now carries FREE entries (METRICS, no price) and PAID
    # entries (PAID_METRICS, price_sats + their own tool name) side by side.
    free = [e for e in body["metrics"] if "price_sats" not in e]
    paid = [e for e in body["metrics"] if "price_sats" in e]
    assert {e["metric"] for e in free} == set(METRIC_NAMES)
    assert {e["metric"] for e in paid} == set(PAID_METRIC_NAMES)
    for e in paid:
        assert e["domain"] == "nutzaps"
        assert e["price_sats"] == 21
        assert e["tool"] == "nostrology_query_nutzaps"
    for e in body["metrics"]:
        assert e["key"] == f"{e['domain']}.{e['metric']}@{e['grain']}"
        assert e["summary"] and e["partition"] and e["caveats"]
        assert e["methodology"] == " ".join([e["summary"], e["partition"], *e["caveats"]])
    for e in free:
        # utc_day leads exactly the day-grain series; @total/@current entries are
        # entity-keyed and carry no day column at all. (The PAID day-grain series
        # is bucket-keyed - period_start - so the pin is free-entries-only.)
        # The free per-mint active-user series (nutzaps.mint_active_users, at
        # day, week and month alike) is bucket-keyed like the paid one.
        if e["metric"] == "mint_active_users":
            assert [c["name"] for c in e["columns"]][0] == "period_start"
        elif e["grain"] == "day":
            assert [c["name"] for c in e["columns"]][0] == "utc_day"
        else:
            assert "utc_day" not in [c["name"] for c in e["columns"]]


def test_registry_json_does_not_publish_physical_schema_details():
    """`source_table` is an internal invariant-checking field, not part of the
    public contract: publishing it would pin a physical table name into a
    document strangers consume, and it adds nothing the methodology prose does
    not already say about which metrics share a population."""

    async def go():
        return await srv_mod.registry_json(None)

    body = json.loads(anyio.run(go).body)
    blob = json.dumps(body)
    assert "source_table" not in blob
    assert "events_kind_" not in blob


def test_registry_resource_matches_the_http_route():
    """One payload, two doors: the MCP resource must serve byte-equivalent content
    to /registry.json (modulo whitespace), or an agent reading the resource and a
    human reading the page would see different catalogs."""

    async def read():
        return await srv_mod.server.read_resource("nostrology://registry")

    (content,) = anyio.run(read)
    assert json.loads(content.content) == srv_mod._registry_payload()


# -- client attribution (the v2 call-log block) -----------------------------------


def test_client_info_none_ctx_and_dead_ctx_degrade_to_none():
    """The in-process path (tests) passes ctx=None; a Context outside a live
    request raises on every property. Both must degrade to 'no client block',
    never to a failed tool call."""
    assert srv_mod._client_info(None) is None

    class DeadCtx:
        @property
        def headers(self):
            raise ValueError("Context is not available outside of a request")

        @property
        def session(self):
            raise ValueError("Context is not available outside of a request")

    assert srv_mod._client_info(DeadCtx()) is None


def test_client_info_captures_session_and_client_identity():
    """The HTTP shape: Mcp-Session-Id from headers + clientInfo from the
    initialize handshake - the two fields that finally answer 'who is using it'."""

    class Info:
        name = "claude-code"
        version = "2.1.220"

    class Params:
        client_info = Info()  # snake_case: the real SDK 2.x attribute (wire is camel)

    class Session:
        client_params = Params()

    class Ctx:
        headers = {"mcp-session-id": "s-42"}
        session = Session()

    assert srv_mod._client_info(Ctx()) == {
        "session_id": "s-42",
        "name": "claude-code",
        "version": "2.1.220",
    }


def test_client_info_captures_the_first_party_marking(stub_db):
    """`?traffic_type=internal` must reach the TOOL record, not only the rpc record -
    otherwise every "was this us?" question about a tool call becomes a join, and the
    tool record is what downstream analysis reads most.

    Pins the attribute chain the tool body walks to the request
    (`ctx.request_context.request`), which is one level deeper than the middleware's.
    Verified against the real SDK over HTTP; a fake here keeps it honest offline."""
    tmp_path, _ = stub_db

    class Request:
        query_params = {"traffic_type": "internal"}

    class RequestContext:
        request = Request()

    class Ctx:
        headers = {"mcp-session-id": "s-42"}
        session = None
        request_context = RequestContext()

    assert srv_mod._client_info(Ctx()) == {"session_id": "s-42", "traffic_type": "internal"}


def test_client_info_omits_an_unmarked_or_bogus_marking(stub_db):
    """Absence is the "not first-party" signal, so an unmarked or non-allowlisted
    request must leave the field off entirely rather than record a default."""

    class Ctx:
        headers = {"mcp-session-id": "s-42"}
        session = None

        class request_context:
            class request:
                query_params = {"traffic_type": "definitely-a-real-user"}

    assert srv_mod._client_info(Ctx()) == {"session_id": "s-42"}


def test_ok_record_carries_the_client_block(stub_db, monkeypatch):
    """End-to-end through the tool impl: the client block must reach the written
    record on success (the demo surface keys off it)."""
    tmp_path, _ = stub_db
    monkeypatch.setattr(srv_mod, "_client_info", lambda ctx: {"session_id": "s-7", "name": "x"})
    _call({"metric": "new_native_app_count"})
    (rec,) = _log_records(tmp_path)
    assert rec["client"] == {"session_id": "s-7", "name": "x"}


def test_db_error_hint_branches_on_failure_shape(monkeypatch):
    """Pure branch test (globally patching time.monotonic races with anyio's own
    use of it, so the elapsed threshold is tested directly on the hint function):
    instant -> tunnel guidance without retry advice; slow -> the real-fault branch."""
    monkeypatch.delenv("NOSTROLOGY_MCP_TRANSPORT", raising=False)
    fast = srv_mod._db_error_hint(5)  # ECONNREFUSED on a dead local port: ~1ms
    assert "tunnel" in fast and "Retrying will not help" in fast
    assert "retry once" not in fast.lower()

    # A healthy connect through SSM measured 1982ms, so 2s must NOT read as "instant".
    slow = srv_mod._db_error_hint(2000)
    assert "retry once" in slow.lower() and "tunnel" not in slow
    # The cold-start explanation is RETIRED (warm floor since 2026-08-03). Telling a
    # caller to wait out a resume that cannot happen would mask a genuine outage.
    assert "resuming" not in slow and "zero capacity" not in slow
    assert "does not pause" in slow


def test_db_error_hint_never_serves_tunnel_prose_to_public_callers(monkeypatch):
    """On the HTTP deployment the caller is an anonymous agent and there is no
    tunnel: the local-dev prose (and the operator ssh alias in it) must not cross
    the wire - an instant failure there is a server-side fault, stated as such."""
    monkeypatch.setenv("NOSTROLOGY_MCP_TRANSPORT", "streamable-http")
    fast = srv_mod._db_error_hint(5)
    assert "tunnel" not in fast and "ssh" not in fast
    assert "server-side" in fast
    # The slow branch is transport-independent: a timeout is a real fault on either
    # transport now that there is no scale-to-zero resume to explain it away.
    assert "retry once" in srv_mod._db_error_hint(2000).lower()


def test_db_error_hint_connection_cap_advises_short_retry(monkeypatch):
    """Connection-limit rejections are instant but BY DESIGN - the one instant
    failure where retrying IS the right move, so it must take precedence over the
    instant-failure branches in either transport."""
    exc = Exception('FATAL:  too many connections for role "nostrology_mcp"')
    for transport in (None, "streamable-http"):
        if transport is None:
            monkeypatch.delenv("NOSTROLOGY_MCP_TRANSPORT", raising=False)
        else:
            monkeypatch.setenv("NOSTROLOGY_MCP_TRANSPORT", transport)
        hint = srv_mod._db_error_hint(3, exc)
        assert "concurrency cap" in hint and "retry" in hint
        assert "tunnel" not in hint and "server-side" not in hint


# -- leaderboard-era behaviour: grain resolution, windowing, snapshots -------------

LB_ROWS_TOTAL = [("aa" * 32, "formstr", "app.formstr.calendar", "Formstr Calendar", True, 13, 36552000)]


@pytest.fixture
def stub_db_lb(monkeypatch, tmp_path):
    """Like stub_db but returns leaderboard-shaped rows and captures kwargs."""
    seen = []
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    def fake_fetch(spec, sql=None, params=None):
        seen.append((spec, sql, params))
        return MetricResult(
            rows=[_dummy_row(spec), _dummy_row(spec)], as_of_epoch=AS_OF, connect_ms=1, query_ms=2
        )

    monkeypatch.setattr(srv_mod, "fetch_metric", fake_fetch)
    return tmp_path, seen


def test_bare_snapshot_call_resolves_grain_current(stub_db_lb):
    """{"metric": "app_stack_count"} with NO grain must work - the whole reason
    the tool default is None, not "day"."""
    tmp_path, seen = stub_db_lb
    res = _call({"metric": "app_stack_count"})
    assert res.is_error is False
    ((spec, sql, params),) = seen
    assert spec.key == "zapstore.app_stack_count@current"
    assert sql is None and params is None
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["grain"] is None            # the ask: omitted
    assert rec["params"]["grain_resolved"] == "current"  # what it became
    text = res.content[0].text
    assert "state: current snapshot (" in text
    assert "series_type: snapshot" in text


def test_bare_event_call_resolves_grain_day(stub_db):
    _, seen = stub_db
    _call({"metric": "app_zaps"})
    assert seen[0][0].key == "zapstore.app_zaps@day"


def test_app_zaps_total_without_window_binds_null_epochs(stub_db_lb):
    """epoch_inline SQL always needs its keys; no window -> None bounds."""
    _, seen = stub_db_lb
    _call({"metric": "app_zaps", "grain": "total"})
    ((spec, sql, params),) = seen
    assert spec.key == "zapstore.app_zaps@total"
    assert sql is None
    assert params["from_epoch"] is None and params["to_epoch_excl"] is None
    assert params["curator"]  # partition params still travel


def test_app_zaps_total_with_window_binds_utc_epochs(stub_db_lb):
    _, seen = stub_db_lb
    _call({"metric": "app_zaps", "grain": "total", "from": "2026-07-04", "to": "2026-08-02"})
    ((spec, sql, params),) = seen
    from datetime import datetime, timezone

    assert params["from_epoch"] == int(datetime(2026, 7, 4, tzinfo=timezone.utc).timestamp())
    assert params["to_epoch_excl"] == int(datetime(2026, 8, 3, tzinfo=timezone.utc).timestamp())
    assert sql is None  # inline mode: the spec SQL itself carries the bounds


def test_day_grain_window_wraps_the_sql(stub_db):
    """day_wrap mode: the spec SQL must arrive wrapped, with day binds."""
    from datetime import date as _date

    _, seen = stub_db
    _call({"metric": "zap_count", "from": "2026-07-01", "to": "2026-07-31"})
    ((spec, sql, params),) = [seen[-1]]
    assert spec.key == "zapstore.zap_count@day"
    assert sql is not None and spec.sql in sql  # wrapped AROUND, not edited
    assert params["from_day"] == _date(2026, 7, 1)
    assert params["to_day"] == _date(2026, 7, 31)


def test_windowed_envelope_echoes_resolved_range(stub_db):
    tmp_path, _ = stub_db
    res = _call({"metric": "zap_count", "from": "2026-07-01", "to": "2026-07-31"})
    assert "window: 2026-07-01..2026-07-31 (UTC days, inclusive)" in res.content[0].text


def test_snapshot_rejects_window_with_reason_and_logs_it(stub_db_lb, monkeypatch):
    tmp_path, seen = stub_db_lb
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "app_stack_count", "from": "2026-07-01"})
    msg = str(exc.value)
    assert "current-state snapshot" in msg and "no time axis" in msg
    assert not seen, "a rejected window must never reach the DB"
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_window"
    assert rec["params"]["from"] == "2026-07-01"
    assert rec["resolved"]["sql_key"] == "zapstore.app_stack_count@current"


def test_snapshot_rejects_explicit_day_or_total_grain(stub_db_lb):
    tmp_path, seen = stub_db_lb
    from mcp.server.mcpserver.exceptions import ToolError

    for g in ("day", "total"):
        with pytest.raises(ToolError) as exc:
            _call({"metric": "publisher_app_count", "grain": g})
        assert "serves grain current" in str(exc.value)
        assert "not additive across days" in str(exc.value)
    assert not seen
    recs = _log_records(tmp_path)
    assert [r["outcome"] for r in recs] == ["invalid_grain", "invalid_grain"]


def test_event_metric_rejects_grain_current(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "zap_count", "grain": "current"})
    assert "serves grain day" in str(exc.value)
    # Nothing better than the legal grains to say here: no redirect clause.
    assert str(exc.value).endswith("serves grain day, not 'current'")


def test_an_unserved_grain_is_rejected_with_what_answers_it(stub_db):
    """The ask Claude Code made on 2026-08-28 (zapped_msats, grain=total, one week):
    refused with the legal grains AND the redirect, and the call log keeps the whole
    message - the redirect is part of the demand record."""
    tmp_path, seen = stub_db
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "zapped_msats", "grain": "total", "from": "2026-08-20", "to": "2026-08-26"})
    expected = (
        "metric 'zapstore.zapped_msats' serves grain day, not 'total' - "
        "its day rows add up - sum them over from/to"
    )
    assert str(exc.value).endswith(expected)
    assert not seen, "a rejected grain must never reach the DB"
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_grain"
    assert rec["error"] == expected
    assert rec["params"]["grain"] == "total"


def test_the_redirect_follows_the_resolved_domain(stub_db):
    """A model client's 2026-08-30 ask named routstr's network_activity with domain left
    at its zapstore default: the grains AND the redirect must come from the domain the
    metric resolves to, not the one passed."""
    tmp_path, seen = stub_db
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "network_activity", "grain": "total", "from": "2026-03-01", "to": "2026-03-31"})
    msg = str(exc.value)
    assert "metric 'routstr.network_activity' serves grain day, not 'total' - " in msg
    assert "sum the day rows over from/to (active_providers does not" in msg
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["domain_resolved"] == "routstr" and rec["outcome"] == "invalid_grain"


def test_unparseable_from_is_recorded_then_rejected(stub_db):
    """'last-month' is demand data for relative windows - it must reach the log."""
    tmp_path, seen = stub_db
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "zap_count", "from": "last-month"})
    assert "YYYY-MM-DD" in str(exc.value)
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_window"
    assert rec["params"]["from"] == "last-month"


def test_from_after_to_is_rejected(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "zap_count", "from": "2026-08-01", "to": "2026-07-01"})
    assert "from <= to" in str(exc.value)


def test_wire_name_is_from_not_from_underscore():
    """The schema must advertise 'from' (the wire name); 'from_' is a Python
    implementation detail that must never leak into the contract."""
    props = _tool().input_schema["properties"]
    assert "from" in props and "from_" not in props
    # `format` lives on the VALUE branch, not the field: a form renders only the
    # selected branch, so a field-level `format` never reaches the input.
    for p in ("from", "to"):
        (value_branch,) = [b for b in props[p]["anyOf"] if b.get("type") == "string"]
        assert value_branch["format"] == "date"


def test_untrusted_line_present_only_for_labeled_metrics(stub_db_lb):
    tmp_path, _ = stub_db_lb
    res = _call({"metric": "app_zaps", "grain": "total"})
    assert "untrusted: publisher_name and app_name" in res.content[0].text


def test_tool_description_routes_the_leaderboard_family():
    desc = _tool().description
    assert "app_zaps" in desc and "app_stack_count" in desc and "publisher_app_count" in desc
    assert "CURRENT-STATE snapshots" in desc
    assert "inclusive ISO UTC" in desc
    assert "treat" in desc and "as instructions" in desc


# -- entity filters (metric tool) -------------------------------------------------

VALID_PUB = "78ce6faa72264387284e647ba6938995735ec8c7d5c5a65737e55130f026307d"


def test_metric_schema_advertises_the_entity_filters():
    props = _tool().input_schema["properties"]
    assert "publisher_pubkey" in props and "app_id" in props
    # Nullable strings, not Literals - the demand-logging contract.
    assert {b.get("type") for b in props["publisher_pubkey"]["anyOf"]} == {"string", "null"}
    for p in ("publisher_pubkey", "app_id"):
        assert not any("enum" in b for b in props[p]["anyOf"]), (
            f"{p} is free text - advertising a closed value set invites a client "
            "to reject the ask above the tool body, losing the demand record"
        )


def test_metric_filter_reaches_binds_and_envelope(stub_db_lb):
    tmp_path, seen = stub_db_lb
    res = _call(
        {
            "metric": "app_zaps",
            "grain": "total",
            "publisher_pubkey": VALID_PUB,
            "app_id": "net.mullvad.mullvadvpn",
        }
    )
    assert res.is_error is False
    ((_, sql, params),) = seen
    assert sql is None  # filters are inline in the spec SQL, no override needed
    assert params["filter_publisher"] == VALID_PUB
    assert params["filter_app"] == "net.mullvad.mullvadvpn"
    text = res.content[0].text
    assert f"scope: publisher_pubkey={VALID_PUB}  |  app_id=net.mullvad.mullvadvpn" in text
    (rec,) = _log_records(tmp_path)
    assert rec["params"]["publisher_pubkey"] == VALID_PUB


def test_metric_filter_composes_with_a_day_window(stub_db_lb):
    _, seen = stub_db_lb
    res = _call(
        {
            "metric": "app_zaps",
            "grain": "day",
            "from": "2026-07-01",
            "to": "2026-07-31",
            "publisher_pubkey": VALID_PUB,
        }
    )
    assert res.is_error is False
    ((_, sql, params),) = seen
    assert sql is not None and "w.utc_day" in sql  # windowed wrapper applied
    assert params["filter_publisher"] == VALID_PUB
    assert params["filter_app"] is None
    assert str(params["from_day"]) == "2026-07-01"


def test_unfiltered_leaderboard_call_keeps_the_legacy_bind_path(stub_db_lb):
    """No filters, no window -> params None (the spec's own binds), so the
    pre-filter behaviour is bit-identical."""
    _, seen = stub_db_lb
    _call({"metric": "app_stack_count"})
    ((_, sql, params),) = seen
    assert sql is None and params is None


def test_filters_on_a_store_wide_metric_are_rejected_and_logged(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen = stub_db
    with pytest.raises(ToolError) as exc:
        _call({"metric": "zap_count", "publisher_pubkey": VALID_PUB})
    assert "does not accept entity filters" in str(exc.value)
    assert "app_zaps" in str(exc.value)  # the redirect the caller needs
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"
    assert rec["params"]["publisher_pubkey"] == VALID_PUB


def test_app_id_without_publisher_is_rejected(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen = stub_db
    with pytest.raises(ToolError) as exc:
        _call({"metric": "app_zaps", "app_id": "net.mullvad.mullvadvpn"})
    assert "requires publisher_pubkey" in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_non_hex_publisher_is_rejected_with_the_npub_hint(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen = stub_db
    with pytest.raises(ToolError) as exc:
        _call({"metric": "app_zaps", "publisher_pubkey": "npub1abcdef"})
    assert "npub" in str(exc.value) and "hex" in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"
    assert rec["params"]["publisher_pubkey"] == "npub1abcdef"  # the demand signal


def test_publisher_app_count_rejects_app_id(stub_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, _ = stub_db
    with pytest.raises(ToolError) as exc:
        _call(
            {
                "metric": "publisher_app_count",
                "publisher_pubkey": VALID_PUB,
                "app_id": "x",
            }
        )
    assert "one row per publisher" in str(exc.value)
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


# -- the reference tool -----------------------------------------------------------


def _ref_call(arguments):
    async def go():
        return await srv_mod.server.call_tool("nostrology_query_reference", arguments)

    return anyio.run(go)


def _dummy_ref_row(columns, total):
    vals = []
    for c in columns:
        if c.type == "timestamp":
            vals.append(1750000000)
        elif c.type == "boolean":
            vals.append(True)
        elif c.type == "integer":
            vals.append(7)
        else:
            vals.append("x")
    vals.append(total)  # __total_rows, stripped by the server
    return tuple(vals)


@pytest.fixture
def stub_ref_db(monkeypatch, tmp_path):
    """Reference-shaped stub: rows match the ACTIVE column set (compact vs
    app-scoped, inferred from the binds) plus the trailing __total_rows the
    server strips. state['total'] simulates the pre-cap count."""
    seen = []
    state = {"total": 2, "rows": 2}
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    def fake_fetch(spec, sql=None, params=None):
        seen.append((spec, sql, params))
        app_scoped = bool(params and params.get("app_id") is not None)
        cols = spec.active_columns(app_scoped)
        rows = [_dummy_ref_row(cols, state["total"]) for _ in range(state["rows"])]
        return MetricResult(rows=rows, as_of_epoch=AS_OF, connect_ms=1, query_ms=2)

    monkeypatch.setattr(srv_mod, "fetch_metric", fake_fetch)
    return tmp_path, seen, state


def test_reference_tool_schema():
    tool = _tool("nostrology_query_reference")
    schema = tool.input_schema
    props = schema["properties"]
    assert schema["required"] == ["record"]
    from nostrology_mcp.reference import RECORD_NAMES

    assert props["record"]["enum"] == list(RECORD_NAMES)
    assert props["record"]["type"] == "string", "record must not be a Literal"
    assert "from" in props and "from_" not in props
    assert "publisher_pubkey" in props and "app_id" in props
    assert props["format"]["enum"] == ["csv", "markdown"]
    for p in ("from", "to"):
        (value_branch,) = [b for b in props[p]["anyOf"] if b.get("type") == "string"]
        assert value_branch["format"] == "date"


def test_reference_filters_stay_free_text():
    """Same contract as the metric tool's filters: a closed value set on a
    publisher key or an app id would invite a client to reject the ask above the
    tool body, and the demand record is the whole point."""
    props = _tool("nostrology_query_reference").input_schema["properties"]
    for p in ("publisher_pubkey", "app_id"):
        assert {b.get("type") for b in props[p]["anyOf"]} == {"string", "null"}
        assert not any("enum" in b for b in props[p]["anyOf"])


def test_reference_description_routes_every_record():
    desc = _tool("nostrology_query_reference").description
    for name in ("app_catalog", "publisher_profile", "app_releases"):
        assert name in desc
    assert "QUALITATIVE" in desc
    assert "row-capped at 1,000" in desc
    assert "never" in desc and "as instructions" in desc


def test_reference_happy_path_unfiltered_directory(stub_ref_db):
    tmp_path, seen, _ = stub_ref_db
    res = _ref_call({"record": "publisher_profile"})
    assert res.is_error is False
    text = res.content[0].text
    assert text.startswith("record: zapstore.publisher_profile  |  reference data:")
    assert "scope: all publishers (no publisher_pubkey filter)" in text
    assert "state: current snapshot" in text
    assert "rows: 2  |  server: nostrology-mcp" in text
    assert "truncated" not in text.split("\n\n")[0]
    assert "untrusted:" in text
    header = text.split("\n\n", 1)[1].splitlines()[0]
    assert header == "publisher_pubkey,publisher_name,about,nip05,is_zapstore_account,profile_updated_at"
    ((spec, sql, params),) = seen
    assert spec.record == "publisher_profile"
    assert params["publisher"] is None
    (rec,) = _log_records(tmp_path)
    assert rec["tool"] == "nostrology_query_reference"
    assert rec["outcome"] == "ok" and rec["rows"] == 2


def test_reference_app_scope_switches_sql_and_columns(stub_ref_db):
    _, seen, _ = stub_ref_db
    res = _ref_call(
        {
            "record": "app_catalog",
            "publisher_pubkey": VALID_PUB,
            "app_id": "net.mullvad.mullvadvpn",
        }
    )
    assert res.is_error is False
    text = res.content[0].text
    assert f"scope: publisher_pubkey={VALID_PUB}  |  app_id=net.mullvad.mullvadvpn" in text
    ((spec, sql, params),) = seen
    assert sql == spec.sql_app_scoped
    header = text.split("\n\n", 1)[1].splitlines()[0]
    assert "description" in header.split(",")


def test_reference_publisher_scope_omits_long_text(stub_ref_db):
    _, seen, _ = stub_ref_db
    res = _ref_call({"record": "app_catalog", "publisher_pubkey": VALID_PUB})
    ((spec, sql, params),) = seen
    assert sql == spec.sql
    header = res.content[0].text.split("\n\n", 1)[1].splitlines()[0]
    assert "description" not in header.split(",")


def test_reference_scope_required_is_rejected_and_logged(stub_ref_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_catalog"})
    assert "requires publisher_pubkey" in str(exc.value)
    assert "publisher_profile" in str(exc.value)  # the unscoped alternative
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_reference_app_id_on_publisher_profile_is_rejected(stub_ref_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call(
            {"record": "publisher_profile", "publisher_pubkey": VALID_PUB, "app_id": "x"}
        )
    assert "one row per publisher" in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_reference_window_rejected_on_current_state_records(stub_ref_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call(
            {"record": "app_catalog", "publisher_pubkey": VALID_PUB, "from": "2026-01-01"}
        )
    assert "no time axis" in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_window"


def test_reference_window_binds_epochs_on_app_releases(stub_ref_db):
    _, seen, _ = stub_ref_db
    res = _ref_call(
        {
            "record": "app_releases",
            "publisher_pubkey": VALID_PUB,
            "from": "2026-07-01",
            "to": "2026-07-31",
        }
    )
    assert res.is_error is False
    ((_, sql, params),) = seen
    assert params["from_epoch"] == 1782864000  # 2026-07-01T00:00:00Z
    assert params["to_epoch_excl"] == 1785542400  # 2026-08-01T00:00:00Z
    text = res.content[0].text
    assert "window: 2026-07-01..2026-07-31 (UTC days, inclusive)" in text


def test_reference_truncation_is_loud(stub_ref_db):
    tmp_path, _, state = stub_ref_db
    state["total"] = 13233
    state["rows"] = 3  # stands in for the capped prefix
    res = _ref_call({"record": "app_releases", "publisher_pubkey": VALID_PUB})
    text = res.content[0].text
    assert "rows: 3 of 13233  |  server:" in text
    assert (
        "truncated: showing the first 3 of 13233 rows "
        "(ordered by released_at DESC, event_id ASC); narrow with app_id= or from/to"
    ) in text


def test_reference_unknown_record_lists_the_catalog(stub_ref_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_directory"})
    for key in ("zapstore.app_catalog", "zapstore.publisher_profile", "zapstore.app_releases"):
        assert key in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "unknown_record"
    assert rec["params"]["record"] == "app_directory"


def test_reference_bad_pubkey_rejected(stub_ref_db):
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "publisher_profile", "publisher_pubkey": "NPUB1XYZ"})
    assert "hex" in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_registry_serves_the_reference_half(monkeypatch):
    async def go():
        return await srv_mod.registry_json(None)

    body = json.loads(anyio.run(go).body)
    refs = {e["record"]: e for e in body["reference"]}
    assert set(refs) == {
        "app_catalog", "publisher_profile", "app_releases",
        "provider_profile", "provider_reviews", "catalog_versions", "model_catalog", "exclusions",
    }
    for e in refs.values():
        assert e["params"]["row_cap"] == 1000
        assert e["summary"] and e["methodology"]


# -- every rejection that needs a publisher key must name the runnable next call --
#
# Seen in the traffic of 2026-08-03: a ContextVM client asked for
# app_catalog with no publisher THREE TIMES IN A ROW, got the same rejection each
# time and gave up - 9 of that window's 23 failures. The hypothesis is that the
# message was the difference: it stated the rule, and even named the record that
# serves the directory, but never named a CALL. (Suggestive, not proven: the same
# client did eventually self-correct a malformed 63-char pubkey after six tries,
# where the message describes a repair it could perform on the spot.)
#
# These tests go through the SDK like every other rejection test in this file, so
# they prove the remedy survives ToolError wrapping to something a client can
# actually read - which is the only place it matters.


def test_missing_publisher_rejection_names_the_next_call(stub_ref_db):
    """The exact call ContextVM made three times: app_catalog, no publisher."""
    from mcp.server.mcpserver.exceptions import ToolError

    tmp_path, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_catalog"})
    assert "requires publisher_pubkey" in str(exc.value)
    assert srv_mod._PUBLISHER_KEY_REMEDY in str(exc.value)
    assert not seen
    (rec,) = _log_records(tmp_path)
    assert rec["outcome"] == "invalid_filter"


def test_app_id_without_publisher_rejection_names_the_next_call(stub_ref_db):
    """The other shape seen in that window: app_id with no publisher."""
    from mcp.server.mcpserver.exceptions import ToolError

    _tmp, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_catalog", "app_id": "dev.zapstore.app"})
    assert srv_mod._PUBLISHER_KEY_REMEDY in str(exc.value)
    assert not seen


def test_app_id_remedy_does_not_promise_an_app_to_publisher_lookup(stub_ref_db):
    """No surface maps app_id -> publisher: publisher_profile has no app_id column,
    and app_catalog / app_releases both require the publisher we are missing. The
    lead-in must therefore NOT offer to find "the publisher for this app" - an agent
    taking that literally would fetch the 277-row directory, fail to find the app,
    and either give up one round trip poorer or fan out app_catalog across every
    publisher. That fan-out is the same shape as the concurrency burst that caused
    this window's connection-limit failure."""
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_catalog", "app_id": "dev.zapstore.app"})
    assert "for this app" not in str(exc.value)


def test_malformed_pubkey_rejection_also_names_the_next_call(stub_ref_db):
    """This message already worked - the client self-corrected 63 -> 64 chars. It
    gains the directory pointer for the caller who has no key at all rather than a
    mistyped one."""
    from mcp.server.mcpserver.exceptions import ToolError

    _tmp, seen, _ = stub_ref_db
    with pytest.raises(ToolError) as exc:
        _ref_call({"record": "app_catalog", "publisher_pubkey": "deadbeef"})
    assert "64-char lowercase hex" in str(exc.value)
    assert srv_mod._PUBLISHER_KEY_REMEDY in str(exc.value)
    assert not seen


def test_remedy_is_reachable_from_the_metric_tool_too(stub_db):
    """The entity filters exist on both tools, so a metric caller hits the same dead
    end - and the remedy names the OTHER tool, which is the point."""
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "app_zaps", "grain": "total", "publisher_pubkey": "not-hex"})
    assert srv_mod._PUBLISHER_KEY_REMEDY in str(exc.value)


def test_no_remedy_where_the_metric_takes_no_publisher_filter(stub_db):
    """Six of the nine metrics are store-wide aggregates that accept no entity
    filter at all. Sending THOSE callers to fetch a 277-row publisher directory
    (~14k tokens) would be actively wrong: the fix is to
    drop the filter, which the message must say instead."""
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "zap_count", "publisher_pubkey": "npub1abc"})
    assert srv_mod._PUBLISHER_KEY_REMEDY not in str(exc.value)
    assert "does not accept entity filters" in str(exc.value)


def test_the_remedy_names_the_column_the_directory_actually_returns(stub_ref_db):
    """The half of the advice the other tests cannot reach: it promises a column
    literally named publisher_pubkey. Drive the recommended call and read the header
    back, so a rename in the column set fails HERE rather than silently turning the
    advice into a dead end."""
    _tmp, _seen, state = stub_ref_db
    state["total"] = state["rows"] = 2
    res = _ref_call({"record": "publisher_profile"})
    assert res.is_error is False
    header = [
        ln for ln in res.content[0].text.splitlines() if ln.startswith("publisher_pubkey")
    ][0]
    first_column = header.split(",")[0]
    assert first_column == "publisher_pubkey"
    assert first_column in srv_mod._PUBLISHER_KEY_REMEDY
    # ...and the recommended call must not itself be rejected, or the remedy points
    # at a second rejection - worse than no remedy at all.
    assert reference_lookup("zapstore", "publisher_profile").scope_required is False


def test_the_remedy_text_carries_the_load_bearing_instruction():
    """Guards the constant itself. Without "and no publisher_pubkey" the caller is
    told to call the directory but not HOW to get the unscoped form - which is the
    one thing they had to infer from the old message."""
    remedy = srv_mod._PUBLISHER_KEY_REMEDY
    assert "nostrology_query_reference" in remedy
    assert "record='publisher_profile'" in remedy
    assert "no publisher_pubkey" in remedy
    assert "publisher_pubkey column" in remedy


def test_metric_app_id_without_publisher_names_the_next_call(stub_db):
    """The one remedy call site the other tests miss: app_id with no publisher on a
    metric that DOES accept both filters (the acceptance check runs first, so a
    metric accepting neither never reaches here)."""
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "app_zaps", "grain": "total", "app_id": "dev.zapstore.app"})
    assert "app_id requires publisher_pubkey" in str(exc.value)
    assert srv_mod._PUBLISHER_KEY_REMEDY in str(exc.value)


def test_the_remedy_never_claims_to_map_an_app_to_its_publisher(stub_ref_db):
    """Property, not wording: no surface maps app_id -> publisher, so no rejection
    may imply the directory does. Guards every site at once."""
    assert "for this app" not in srv_mod._APP_ID_REQUIRES_PUBLISHER_MSG
    assert "for this app" not in srv_mod._PUBLISHER_KEY_REMEDY


# -- optional-parameter shape (the form-rendering contract) -----------------------
#
# These pin the shape authored in inputschema.py. They exist because the failure
# they guard against is INVISIBLE from Python: the schema stayed valid, the tool
# kept working for every model, and the only broken surface was the one client
# that renders the schema as a form (contextvm.org), where the parameters read
# "Grain option 1" and the form could not be submitted at all.

OPTIONAL_PARAMS = {
    "nostrology_query_metric": ("grain", "from", "to", "publisher_pubkey", "app_id"),
    "nostrology_query_reference": ("publisher_pubkey", "app_id", "from", "to"),
}


def test_every_optional_parameter_offers_a_titled_omit_branch():
    """Two properties in one: a caller can always SEE how to leave a parameter out
    (exactly one null branch, titled in words), and no branch is untitled - an
    untitled branch is what a form labels "<field> option <n>"."""
    for tool_name, params in OPTIONAL_PARAMS.items():
        props = _tool(tool_name).input_schema["properties"]
        for p in params:
            branches = props[p]["anyOf"]
            nulls = [b for b in branches if b.get("type") == "null"]
            assert len(nulls) == 1, f"{tool_name}.{p} must offer exactly one omit branch"
            # FIRST, not merely present: a form preselects the branch closest to
            # the current (empty) value, and "leave it out" is the state an
            # untouched form must land in.
            assert branches[0].get("type") == "null", f"{tool_name}.{p} omit branch is not first"
            titles = [b.get("title", "") for b in branches]
            assert all(t.strip() for t in titles), f"{tool_name}.{p} has an untitled branch"
            assert len(set(titles)) == len(titles), f"{tool_name}.{p} has duplicate branch titles"


def test_no_optional_parameter_contradicts_its_own_omit_branch():
    """THE regression. `grain` used to carry `enum: [current, day, total]` beside
    `anyOf: [string, null]`, so its own null default failed its own enum: a strict
    validator rejected the untouched form with "must be equal to one of the allowed
    values" and nothing to point at. A sibling `enum` (or a `default` that no
    branch admits) must never come back."""
    for tool_name, params in OPTIONAL_PARAMS.items():
        props = _tool(tool_name).input_schema["properties"]
        for p in params:
            assert "enum" not in props[p], f"{tool_name}.{p} has a sibling enum again"
            assert "default" not in props[p], f"{tool_name}.{p} has a sibling default again"


def _keys(node) -> set:
    """Every key name anywhere in a nested schema."""
    if isinstance(node, dict):
        return set(node) | {k for v in node.values() for k in _keys(v)}
    if isinstance(node, list):
        return {k for v in node for k in _keys(v)}
    return set()


def test_the_schemas_never_use_oneOf():
    """`anyOf` and `oneOf` render identically in the form, but `oneOf` sits outside
    the JSON Schema subset strict/constrained tool-calling clients accept. Never
    trade agent compatibility for a UI no-op. (Keys, not a substring search - a
    description is free to contain the word.)"""
    for tool_name in OPTIONAL_PARAMS:
        assert "oneOf" not in _keys(_tool(tool_name).input_schema)


def test_the_advertised_schema_admits_an_untouched_form():
    """The general form of the bug this change fixes, stated as the property rather
    than as a denylist of keywords: the instance a caller sends when they fill in
    only what is required MUST validate against the schema we published. The old
    `grain` failed exactly this (its sibling enum forbade its own null default), and
    so would a stray `type`, `const` or `minLength` on the field."""
    jsonschema = pytest.importorskip("jsonschema")
    minimum = {"nostrology_query_metric": {"metric": "zap_count"},
               "nostrology_query_reference": {"record": "publisher_profile"}}
    for tool_name, params in OPTIONAL_PARAMS.items():
        schema = _tool(tool_name).input_schema
        jsonschema.Draft202012Validator.check_schema(schema)
        untouched = {**minimum[tool_name], **{p: None for p in params}}
        jsonschema.Draft202012Validator(schema).validate(untouched)


def test_explicit_nulls_are_accepted_exactly_like_omitted(stub_db):
    """What a form actually sends. The rendered form submits EVERY untouched
    optional as an explicit null, not by omitting the key - so `grain: null` must
    resolve to the metric default rather than take the invalid-grain path."""
    _, seen = stub_db
    res = _call(
        {
            "metric": "new_native_app_count",
            "grain": None,
            "from": None,
            "to": None,
            "publisher_pubkey": None,
            "app_id": None,
        }
    )
    assert res.is_error is False
    ((spec, _sql, _params),) = seen
    assert spec.key == "zapstore.new_native_app_count@day"


def test_reference_explicit_nulls_are_accepted_exactly_like_omitted(stub_ref_db):
    """Same contract on the reference tool - its form submits the same way."""
    res = _ref_call(
        {"record": "publisher_profile", "publisher_pubkey": None, "app_id": None,
         "from": None, "to": None}
    )
    assert res.is_error is False
    assert "scope: all publishers (no publisher_pubkey filter)" in res.content[0].text
