"""Observation-middleware tests: the record it writes for every inbound JSON-RPC
message, the failure shapes it must catch, and - the part that matters most - an
END-TO-END pass over a real client session proving the middleware is actually
wired into the server and that discovery traffic lands in the log.

The unit tests below drive `CallLogMiddleware.__call__` with a fake context; the
integration tests at the bottom drive the real SDK dispatcher over in-memory
streams, which is the only way to prove `MCPServer(middleware=[...])` took effect
(`server.call_tool()` is the direct handler path and bypasses the chain entirely).
"""

import json
import os
from dataclasses import dataclass
from typing import Any

import anyio
import mcp_types
import pytest
from mcp.client.session import ClientSession
from mcp.shared.memory import create_client_server_memory_streams

from nostrology_mcp import server as srv_mod
from nostrology_mcp.db import MetricResult
from nostrology_mcp.middleware import CallLogMiddleware, _result_error, traffic_type_of
from nostrology_mcp.tests.test_server_tool import AS_OF, ROWS


def _records(tmp_path):
    files = [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]
    assert len(files) == 1, f"expected one day-file, got {files}"
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


# -- unit: the fake-context path -------------------------------------------------


@dataclass
class _FakeConnection:
    session_id: str | None = "conn-sess"


class _FakeSession:
    def __init__(self, client_params=None, connection=None):
        self.client_params = client_params
        self._connection = connection if connection is not None else _FakeConnection()


class _FakeRequest:
    def __init__(self, headers):
        self.headers = headers


@dataclass
class _FakeCtx:
    method: str = "tools/list"
    params: Any = None
    request_id: Any = 1
    session: Any = None
    request: Any = None


def _run(mw, ctx, call_next):
    async def go():
        return await mw(ctx, call_next)

    return anyio.run(go)


@pytest.fixture
def mw(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    return CallLogMiddleware("0.1.0", "abc1234"), tmp_path


def test_records_the_method_and_passes_the_result_through(mw):
    middleware, tmp_path = mw
    ctx = _FakeCtx(method="resources/read", params={"uri": "nostrology://registry"}, session=_FakeSession())

    async def call_next(c):
        return {"contents": []}

    assert _run(middleware, ctx, call_next) == {"contents": []}
    (rec,) = _records(tmp_path)
    assert rec["kind"] == "rpc"
    assert rec["method"] == "resources/read"
    assert rec["params"] == {"uri": "nostrology://registry"}
    assert rec["outcome"] == "ok"
    assert "total" in rec["ms"]


def test_a_raised_failure_is_recorded_and_re_raised_unchanged(mw):
    """Never swallow: the caller's error semantics must be exactly what they were
    before this middleware existed."""
    middleware, tmp_path = mw
    boom = ValueError("malformed params")

    async def call_next(c):
        raise boom

    with pytest.raises(ValueError) as exc:
        _run(middleware, _FakeCtx(session=_FakeSession()), call_next)
    assert exc.value is boom  # the same object, not a wrapper

    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "error"
    assert rec["error"] == "ValueError: malformed params"


def test_a_logging_failure_never_breaks_the_call(mw, monkeypatch, capsys):
    """The middleware sits in front of EVERY message, so a broken log here would
    take the whole server down rather than one call."""
    middleware, _ = mw
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", "/proc/nonexistent/nope")

    async def call_next(c):
        return {"ok": True}

    assert _run(middleware, _FakeCtx(session=_FakeSession()), call_next) == {"ok": True}
    assert "call log write failed" in capsys.readouterr().err


def test_the_snapshot_is_what_the_client_sent_not_what_a_rewrite_left(mw):
    """Middleware may rewrite the context on the way down; what belongs on record is
    the client's original ask."""
    middleware, tmp_path = mw
    ctx = _FakeCtx(method="tools/call", params={"name": "orig"}, session=_FakeSession())

    async def call_next(c):
        ctx.method, ctx.params = "tools/rewritten", {"name": "mutated"}
        return {}

    _run(middleware, ctx, call_next)
    (rec,) = _records(tmp_path)
    assert rec["method"] == "tools/call" and rec["params"] == {"name": "orig"}


# -- unit: identity attribution --------------------------------------------------


def test_session_id_prefers_the_request_header(mw):
    middleware, tmp_path = mw
    ctx = _FakeCtx(
        session=_FakeSession(connection=_FakeConnection(session_id="from-conn")),
        request=_FakeRequest({"mcp-session-id": "from-header"}),
    )

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert _records(tmp_path)[0]["client"]["session_id"] == "from-header"


def test_session_id_falls_back_to_the_connection_for_initialize(mw):
    """The fallback path, exercised with a fake. The REAL SDK attribute chain it
    depends on is pinned separately below - a fake alone would keep passing after an
    SDK rename, which is the failure mode that matters."""
    middleware, tmp_path = mw
    ctx = _FakeCtx(
        method="initialize",
        session=_FakeSession(connection=_FakeConnection(session_id="from-conn")),
        request=_FakeRequest({}),  # no header on initialize, as on the wire
    )

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert _records(tmp_path)[0]["client"]["session_id"] == "from-conn"


def test_the_private_sdk_attribute_chain_still_exists(mw):
    """PINS A PRIVATE SDK ATTRIBUTE against the REAL SDK: `_session_id` reaches
    `ctx.session._connection.session_id` because `ServerSession` exposes no public
    session id, and that chain is the ONLY way to attribute the `initialize` record
    (the client has no session id to send yet on that message, and initialize is the
    one message carrying clientInfo).

    Built from a real `mcp.server.connection.Connection`, deliberately: a fake with
    a `_connection` attribute of its own would keep passing after an upstream rename
    while production silently degraded to unattributed records. Verified: renaming
    `ServerSession._connection` in the installed SDK leaves the fake-based test green
    and fails this one.

    The production path is inside try/except, so an SDK bump costs attribution, not
    availability. This test is what turns that silent cost into a loud one."""
    from mcp.server.connection import Connection
    from mcp.server.session import ServerSession

    connection = Connection.for_loop(None, session_id="sess-from-real-sdk")
    session = ServerSession.__new__(ServerSession)  # bypass the dispatcher-shaped ctor
    session._connection = connection
    assert isinstance(session, ServerSession)

    middleware, tmp_path = mw
    ctx = _FakeCtx(method="initialize", session=session, request=None)

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert _records(tmp_path)[0]["client"]["session_id"] == "sess-from-real-sdk"


def test_server_session_still_has_no_public_session_id(mw):
    """The justification for the private access, asserted rather than assumed: if a
    public accessor ever appears, switch to it and delete the private path."""
    from mcp.server.session import ServerSession

    public = {name for name in dir(ServerSession) if not name.startswith("_")}
    assert "session_id" not in public, "a public accessor exists now - use it instead"


def test_a_context_that_raises_on_every_access_still_yields_a_record(mw):
    """The identity lookups reach through several layers of SDK internals (one of
    them private). Every one of those accesses is guarded, and this proves it: a
    context where EVERY attribute explodes must still produce a record - the fact
    that a call happened outlives our ability to attribute it."""
    middleware, tmp_path = mw

    class Hostile:
        def __getattr__(self, name):
            raise RuntimeError(f"no {name}")

    class HostileCtx:
        method = "tools/list"
        request_id = 1

        # Properties, not plain attributes: the ACCESS itself must raise, which is
        # the real hazard (a Context outside a live request raises on every
        # property). Plain attributes returning an exploding object would only
        # exercise the guards one level deeper and leave the outer ones untested.
        @property
        def params(self):
            raise RuntimeError("no params")

        @property
        def session(self):
            raise RuntimeError("no session")

        @property
        def request(self):
            raise RuntimeError("no request")

    async def call_next(c):
        return {}

    _run(middleware, HostileCtx(), call_next)
    (rec,) = _records(tmp_path)
    assert rec["method"] == "tools/list" and rec["outcome"] == "ok"
    assert "client" not in rec


def test_getattr_safe_survives_a_raising_property():
    """The builtin `getattr(o, name, default)` does NOT swallow an exception raised
    INSIDE a property getter - it only handles the attribute being missing. That gap
    is exactly the case the snapshot fallback has to survive."""
    from nostrology_mcp.middleware import getattr_safe

    class Raising:
        @property
        def boom(self):
            raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        getattr(Raising(), "boom", "fallback")  # the builtin propagates
    assert getattr_safe(Raising(), "boom", "fallback") == "fallback"
    assert getattr_safe(Raising(), "absent", "fallback") == "fallback"


def test_result_error_tolerates_a_result_that_raises_on_access():
    """Guards the is_error read itself: an exotic result type must cost us the
    error text, not the record."""

    class Hostile:
        def __getattr__(self, name):
            raise RuntimeError("nope")

    assert _result_error(Hostile()) is None


def test_identity_falls_back_to_wire_params_when_the_session_explodes(mw):
    """The two identity sources are independent: a broken session must not stop the
    raw `clientInfo` on an initialize body from being read."""
    middleware, tmp_path = mw

    class HostileSession:
        def __getattr__(self, name):
            raise RuntimeError("nope")

    ctx = _FakeCtx(
        method="initialize",
        params={"clientInfo": {"name": "resilient", "version": "3"}},
        session=HostileSession(),
    )

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    rec = _records(tmp_path)[0]
    assert rec["client"] == {"name": "resilient", "version": "3"}


# -- first-party marking (?traffic_type=internal) --------------------------------


class _FakeQueryRequest:
    def __init__(self, query, headers=None):
        self.query_params = query
        self.headers = headers or {}


@pytest.mark.parametrize(
    "query,expected",
    [
        ({"traffic_type": "internal"}, "internal"),
        ({}, None),
        ({"traffic_type": "external"}, None),  # not allowlisted
        ({"traffic_type": ""}, None),
        ({"traffic_type": "INTERNAL"}, None),  # exact match only
        ({"traffic_type": "internal "}, None),  # exact match only
        ({"other": "internal"}, None),
    ],
)
def test_traffic_type_is_allowlisted(query, expected):
    """An unauthenticated caller must not be able to write free text into a log that
    ships to Loki, so anything outside the allowlist is dropped rather than
    recorded. `internal` is the only marking that means anything."""
    assert traffic_type_of(_FakeQueryRequest(query)) == expected


def test_traffic_type_degrades_rather_than_failing():
    """stdio has no request at all; an exotic one must cost the label, not the call."""

    class Hostile:
        def __getattr__(self, name):
            raise RuntimeError("nope")

    assert traffic_type_of(None) is None
    assert traffic_type_of(Hostile()) is None


def test_marked_traffic_is_recorded_on_the_rpc_record(mw):
    middleware, tmp_path = mw
    ctx = _FakeCtx(
        session=_FakeSession(),
        request=_FakeQueryRequest({"traffic_type": "internal"}, {"mcp-session-id": "s-9"}),
    )

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert _records(tmp_path)[0]["client"]["traffic_type"] == "internal"


def test_unmarked_traffic_carries_no_marking(mw):
    """Absence is the signal for "not first-party" - so it must be ABSENT, not
    recorded as some default that would be indistinguishable from a real value."""
    middleware, tmp_path = mw
    ctx = _FakeCtx(session=_FakeSession(), request=_FakeQueryRequest({}, {"mcp-session-id": "s-9"}))

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert "traffic_type" not in _records(tmp_path)[0]["client"]


def test_identity_degrades_to_absent_rather_than_failing(mw):
    """stdio has no headers and no session id; a client may omit clientInfo. The
    record must still be written, just without a client block."""
    middleware, tmp_path = mw
    ctx = _FakeCtx(session=_FakeSession(connection=_FakeConnection(session_id=None)))

    async def call_next(c):
        return {}

    _run(middleware, ctx, call_next)
    assert "client" not in _records(tmp_path)[0]


# -- unit: is_error detection ----------------------------------------------------


@pytest.mark.parametrize(
    "result,expected",
    [
        ({"isError": True, "content": [{"type": "text", "text": "boom"}]}, "boom"),
        ({"is_error": True, "content": [{"text": "snake"}]}, "snake"),
        ({"isError": False, "content": [{"text": "fine"}]}, None),
        ({"content": [{"text": "no flag"}]}, None),
        ({"isError": True, "content": []}, "is_error result with no text content"),
        ({"isError": True}, "is_error result with no text content"),
        (None, None),
        ("not a result", None),
        # Literal True only. Truthy non-bools are what a sloppy client sends, and
        # coercing them would invent failures in the demand analysis this feeds -
        # note "false" and 0 are truthy/falsy in ways that both mislead.
        ({"isError": "false", "content": [{"text": "x"}]}, None),
        ({"isError": 1, "content": [{"text": "x"}]}, None),
        ({"isError": 0, "content": [{"text": "x"}]}, None),
    ],
)
def test_result_error_reads_both_spellings_and_tolerates_junk(result, expected):
    assert _result_error(result) == expected


def test_a_known_failure_with_unreadable_content_stays_a_failure():
    """Regression: an exception while reading the error TEXT must not downgrade a
    real failure to outcome="ok". Losing the reason is acceptable; losing the fact
    is the bug this function exists to prevent."""

    class Hostile(list):
        def __getitem__(self, i):
            raise RuntimeError("nope")

    got = _result_error({"isError": True, "content": Hostile([1])})
    assert got == "is_error result with no text content"


def test_result_error_is_capped(mw):
    from nostrology_mcp.middleware import MAX_ERROR_CHARS

    got = _result_error({"isError": True, "content": [{"text": "x" * (MAX_ERROR_CHARS + 50)}]})
    assert len(got) == MAX_ERROR_CHARS


# -- integration: a real session over the real dispatcher ------------------------


@pytest.fixture
def live(monkeypatch, tmp_path):
    """A real client/server pair over in-memory streams, with the DB stubbed."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        srv_mod,
        "fetch_metric",
        lambda spec, **kw: MetricResult(rows=ROWS, as_of_epoch=AS_OF, connect_ms=1, query_ms=2),
    )

    def drive(body):
        async def go():
            low = srv_mod.server._lowlevel_server
            async with create_client_server_memory_streams() as (client_streams, server_streams):
                async with anyio.create_task_group() as tg:

                    async def serve():
                        await low.run(
                            server_streams[0],
                            server_streams[1],
                            low.create_initialization_options(),
                            raise_exceptions=False,
                        )

                    tg.start_soon(serve)
                    async with ClientSession(
                        client_streams[0],
                        client_streams[1],
                        client_info=mcp_types.Implementation(name="test-harness", version="9.9"),
                    ) as session:
                        await session.initialize()
                        await body(session)
                    tg.cancel_scope.cancel()

        anyio.run(go)
        return _records(tmp_path)

    return drive


def test_discovery_traffic_is_recorded_end_to_end(live):
    """THE regression this whole change exists to prevent. Before it, a client that
    connected, listed the tools, browsed the catalog and left produced NOT ONE log
    record - which is what 18 of the endpoint's first 38 sessions did."""

    async def body(session):
        await session.list_tools()
        await session.list_resources()
        await session.read_resource("nostrology://registry")

    methods = [r["method"] for r in live(body) if r["kind"] == "rpc"]
    assert methods == [
        "initialize",
        "notifications/initialized",
        "tools/list",
        "resources/list",
        "resources/read",
    ]


def test_the_registry_resource_read_names_its_uri(live):
    """Catalog consumption was entirely unrecorded; knowing WHICH resource was read
    is what makes the record actionable once there is more than one."""

    async def body(session):
        await session.read_resource("nostrology://registry")

    (rec,) = [r for r in live(body) if r.get("method") == "resources/read"]
    assert rec["params"]["uri"] == "nostrology://registry"
    assert rec["outcome"] == "ok"


def test_initialize_captures_client_identity_from_the_handshake(live):
    """clientInfo arrives on `initialize`, where the SDK has not committed it to the
    session yet - so it is read off the raw wire params. If that fallback breaks, the
    one record that names the client is the one record missing it."""

    async def body(session):
        pass

    (rec,) = [r for r in live(body) if r.get("method") == "initialize"]
    assert rec["client"]["name"] == "test-harness"
    assert rec["client"]["version"] == "9.9"
    assert rec["params"]["clientInfo"]["name"] == "test-harness"


def test_every_session_message_carries_the_client_identity(live):
    """Not just initialize: attribution must survive onto the calls themselves, or
    the log cannot say who asked for what."""

    async def body(session):
        await session.list_tools()

    assert all(r["client"]["name"] == "test-harness" for r in live(body) if r["kind"] == "rpc")


def test_a_successful_tool_call_writes_both_records(live):
    """The rpc record carries the RAW wire arguments (answering "did the caller pass
    grain, or inherit it?"), the tool record the resolved series and its cost."""

    async def body(session):
        await session.call_tool("nostrology_query_metric", {"metric": "zap_count"})

    recs = live(body)
    (rpc,) = [r for r in recs if r.get("method") == "tools/call"]
    (tool,) = [r for r in recs if r["kind"] == "tool"]
    assert rpc["outcome"] == "ok"
    assert rpc["params"]["arguments"] == {"metric": "zap_count"}  # exactly what was sent
    assert tool["params"] == {  # post-default, hence different on purpose
        "domain": "zapstore",
        "metric": "zap_count",
        "grain": None,               # the RAW ask: grain was omitted
        "grain_resolved": "day",     # what the omission resolved to
        "from": None,
        "to": None,
        "publisher_pubkey": None,
        "app_id": None,
        "provider_pubkey": None,
        "model": None,
        "mint_url": None,
        "format": "csv",
    }
    assert tool["outcome"] == "ok" and tool["rows"] == len(ROWS)


@pytest.mark.parametrize(
    "arguments,marker",
    [
        ({"metric": "zap_count", "format": "yaml"}, "format"),  # pydantic, above our code
        ({}, "metric"),  # missing required, above our code
        ({"metric": "no_such_metric"}, "no_such_metric"),  # ours, in the body
    ],
)
def test_rejected_calls_are_recorded_with_their_reason(live, arguments, marker):
    """The SDK does NOT raise for a failed tool call - it returns an ordinary result
    with isError set. Without the is_error check these all logged outcome="ok" with
    no reason, leaving the blind spot open for exactly the calls worth seeing."""

    async def body(session):
        await session.call_tool("nostrology_query_metric", arguments)

    (rpc,) = [r for r in live(body) if r.get("method") == "tools/call"]
    assert rpc["outcome"] == "tool_error"
    assert marker in rpc["error"]


def test_an_unknown_tool_name_is_recorded(live):
    """A caller reaching for a tool we do not have is demand data too, and it never
    touches our tool body at all."""

    async def body(session):
        await session.call_tool("nostrology_list_relays", {})

    (rpc,) = [r for r in live(body) if r.get("method") == "tools/call"]
    assert rpc["outcome"] == "tool_error"
    assert "nostrology_list_relays" in rpc["error"]
    assert rpc["params"]["name"] == "nostrology_list_relays"
