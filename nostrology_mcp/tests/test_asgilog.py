"""HTTP-seam tests: the `kind:"http"` record, and - the part that matters most -
an END-TO-END pass over the real SDK ASGI app proving that the transport-level
rejections this seam exists for actually land in the call log.

The unit tests drive `HttpCallLogMiddleware` with hand-built ASGI scopes. The
integration tests at the bottom run the middleware over the SDK's real
`streamable_http_app()` via an ASGI transport, which is the only way to prove the
400 that motivated all of this is now recorded - a fake downstream app would
prove only that our own code can write a record.
"""

import functools
import json
import os

import anyio
import httpx2
import pytest

from nostrology_mcp import server as srv_mod
from nostrology_mcp.asgilog import HttpCallLogMiddleware, _traffic_type
from nostrology_mcp.calllog import MAX_CLIENT_FIELD_CHARS


def _async_test(fn):
    """Run an async test body under anyio, keeping pytest's fixture injection.

    The repo has no async-test plugin (test_middleware.py hand-rolls `anyio.run`
    the same way); `functools.wraps` sets `__wrapped__` so pytest still resolves
    `tmp_path`/`monkeypatch` from the real signature.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return anyio.run(functools.partial(fn, *args, **kwargs))

    return wrapper


def _records(tmp_path):
    files = [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]
    assert len(files) == 1, f"expected one day-file, got {files}"
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


def _scope(path="/mcp", method="POST", headers=(), query=b"", client=("10.0.0.1", 5)):
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query,
        "headers": list(headers),
        "client": client,
    }


async def _ok_app(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"{}"})


def _mw(app=_ok_app, path="/mcp"):
    return HttpCallLogMiddleware(app, path=path, server_version="9.9", git_sha="deadbee")


# -- unit ------------------------------------------------------------------------


@_async_test
async def test_records_a_request_on_the_mcp_path(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    await _mw()(_scope(), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["v"] == 4
    assert rec["kind"] == "http"
    assert rec["method"] == "POST"
    assert rec["path"] == "/mcp"
    assert rec["status"] == 200
    assert rec["had_session_header"] is False
    assert rec["server"] == {"version": "9.9", "git_sha": "deadbee"}
    assert isinstance(rec["ms"]["total"], int)


async def _noop():
    return None


@_async_test
async def test_non_mcp_paths_are_not_recorded(tmp_path, monkeypatch):
    """/health is polled by the container healthcheck; recording it would write
    thousands of records a day that answer no question."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    for path in ("/health", "/registry.json", "/mcpsomething"):
        await _mw()(_scope(path=path), None, lambda m: _noop())
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]


@_async_test
async def test_subpaths_of_mcp_are_recorded(tmp_path, monkeypatch):
    """Mirrors Caddy's `@mcp path /mcp /mcp/*`, so both sides agree on the
    public surface. `/mcpsomething` above must NOT match; `/mcp/x` must."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    await _mw()(_scope(path="/mcp/x"), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["path"] == "/mcp/x"


@_async_test
async def test_non_http_scopes_pass_through_untouched(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    seen = []

    async def app(scope, receive, send):
        seen.append(scope["type"])

    await _mw(app)({"type": "lifespan"}, None, None)
    assert seen == ["lifespan"]
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]


@_async_test
async def test_request_session_header_is_recorded_and_flagged(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    headers = [(b"mcp-session-id", b"abc123"), (b"user-agent", b"node")]
    await _mw()(_scope(headers=headers), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["had_session_header"] is True
    assert rec["client"]["session_id"] == "abc123"
    assert rec["client"]["user_agent"] == "node"


@_async_test
async def test_server_minted_session_id_is_the_fallback(tmp_path, monkeypatch):
    """`initialize` carries no session id - the server mints it in the RESPONSE.
    Without lifting it off the response, the one record that opens a session
    would be the one record that cannot be joined to the rest of it."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"mcp-session-id", b"minted99")],
            }
        )

    await _mw(app)(_scope(), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["client"]["session_id"] == "minted99"
    # ...but the flag must still say the CLIENT sent nothing, which is the
    # never-initialized-transport signature.
    assert rec["had_session_header"] is False


@_async_test
async def test_traffic_type_is_read_from_the_query_string(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    await _mw()(_scope(query=b"traffic_type=internal"), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["client"]["traffic_type"] == "internal"


def test_traffic_type_allowlist_rejects_free_text():
    """The value is attacker-controlled and ships to Loki; only the allowlist
    passes. Shared with the JSON-RPC seam by import, so they cannot disagree."""
    assert _traffic_type({"query_string": b"traffic_type=internal"}) == "internal"
    assert _traffic_type({"query_string": b"traffic_type=evil"}) is None
    assert _traffic_type({"query_string": b"traffic_type="}) is None
    assert _traffic_type({"query_string": b"other=1"}) is None
    assert _traffic_type({}) is None


@_async_test
async def test_the_record_carries_no_caller_address(tmp_path, monkeypatch, capsys):
    """The client block is the session, the User-Agent and the marking - nothing
    else. Neither a forwarded address nor the ASGI peer reaches either sink: the
    day-file, or stdout, which the container turns on."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_STDOUT", "1")
    headers = [(b"x-forwarded-for", b"198.51.100.7, 203.0.113.9"), (b"user-agent", b"node")]
    await _mw()(_scope(headers=headers), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert set(rec["client"]) <= {"session_id", "user_agent", "traffic_type"}
    line = json.dumps(rec)
    out = capsys.readouterr().out
    assert '"kind": "http"' in out or '"kind":"http"' in out, "the stdout sink wrote no record"
    for address in ("198.51.100.7", "203.0.113.9", "10.0.0.1"):
        assert address not in line
        assert address not in out


@_async_test
async def test_caller_controlled_strings_are_capped_at_the_sink(tmp_path, monkeypatch):
    """clientInfo taught us this: an unbounded caller-controlled string stamped
    on every record amplifies into the day-file AND into Loki. This seam adds no
    ceiling of its own - it relies on `_clip_client`, so the cap can never drift
    out of step with the one the other two record kinds get. Pinned here so a
    change at the sink that stopped covering this block fails loudly."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    headers = [(b"user-agent", b"x" * 5000), (b"mcp-session-id", b"9" * 500)]
    await _mw()(_scope(headers=headers), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    for field in ("user_agent", "session_id"):
        value = rec["client"][field]
        assert value.startswith(("x" * MAX_CLIENT_FIELD_CHARS, "9" * MAX_CLIENT_FIELD_CHARS))
        assert value.endswith("]"), value  # clip() annotates what it dropped
        assert len(value) < 200


@_async_test
async def test_a_record_is_written_even_when_the_app_raises(tmp_path, monkeypatch):
    """A disconnect mid-SSE unwinds through here; a stream that died is exactly
    the event worth having on record. The exception must still propagate."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    async def boom(scope, receive, send):
        raise RuntimeError("client went away")

    with pytest.raises(RuntimeError):
        await _mw(boom)(_scope(), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["status"] is None


@_async_test
async def test_logging_failure_never_fails_the_request(tmp_path, monkeypatch):
    """The never-fail-the-request guarantee, forced rather than assumed."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        "nostrology_mcp.asgilog.write_http_record",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("sink down")),
    )
    served = []

    async def app(scope, receive, send):
        served.append(1)
        await send({"type": "http.response.start", "status": 200, "headers": []})

    await _mw(app)(_scope(), None, lambda m: _noop())
    assert served == [1]


# -- integration: the real SDK ASGI app ------------------------------------------
#
# These are what prove the seam is in the right PLACE. Everything above would pass
# just as well with the middleware wrapping a toy app.


def _real_app():
    app = srv_mod.server.streamable_http_app(
        streamable_http_path=srv_mod.MCP_PATH, json_response=True, host="0.0.0.0"
    )
    app.add_middleware(
        HttpCallLogMiddleware,
        path=srv_mod.MCP_PATH,
        server_version=srv_mod.__version__,
        git_sha=srv_mod.GIT_SHA,
    )
    return app


@_async_test
async def test_missing_session_id_is_recorded(tmp_path, monkeypatch):
    """THE regression test for the 2026-08-03 blind spot: a `tools/list` with no
    `Mcp-Session-Id`, which the SDK's session manager rejects above every seam we
    had. Before this middleware existed, this request produced no record at all.
    """
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=transport, base_url="http://t"
    ) as client:
        resp = await client.post(
            "/mcp",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        )
    assert resp.status_code == 400
    (rec,) = _records(tmp_path)
    assert rec["kind"] == "http"
    assert rec["status"] == 400
    assert rec["had_session_header"] is False
    assert rec["method"] == "POST"


@_async_test
async def test_bad_accept_header_is_recorded(tmp_path, monkeypatch):
    """The other family of transport-level rejections: content negotiation, which
    also never becomes a JSON-RPC message."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=transport, base_url="http://t"
    ) as client:
        resp = await client.post(
            "/mcp",
            headers={"Content-Type": "application/json", "Accept": "text/plain"},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        )
    assert resp.status_code >= 400
    (rec,) = _records(tmp_path)
    assert rec["status"] == resp.status_code


@_async_test
async def test_initialize_is_recorded_with_the_minted_session_id(tmp_path, monkeypatch):
    """A successful handshake over the real app: the http record must carry the
    session id the server minted, so it joins to the rpc/tool records that follow.
    """
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=transport, base_url="http://t"
    ) as client:
        resp = await client.post(
            "/mcp?traffic_type=internal",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "asgilog-test", "version": "1.0"},
                },
            },
        )
    assert resp.status_code == 200
    http_recs = [r for r in _records(tmp_path) if r["kind"] == "http"]
    assert len(http_recs) == 1
    rec = http_recs[0]
    assert rec["status"] == 200
    assert rec["had_session_header"] is False
    assert rec["client"]["session_id"] == resp.headers["mcp-session-id"]
    assert rec["client"]["traffic_type"] == "internal"


@_async_test
async def test_a_hostile_path_cannot_inflate_the_record(tmp_path, monkeypatch):
    """`_matches` accepts anything under `/mcp/` and h11 allows a ~16 KiB request
    target, so an unclipped `path` would be a 16 KB record per request onto the
    obs volume and through stdout into Loki. Unauthenticated endpoint, no rate
    limit, exempt from Caddy's bot filter: one curl loop would be the whole
    amplification. Bound the RECORD, not just the field."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    await _mw()(_scope(path="/mcp/" + "A" * 8000), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert len(rec["path"]) < 200
    assert len(json.dumps(rec)) < 1024


@_async_test
async def test_a_rejected_request_gets_no_session_id(tmp_path, monkeypatch):
    """The SDK mints a throwaway transport for a session-less POST and stamps its
    id on the 400 as well. Recording that would give every request in a failure
    storm a distinct session id no client ever held - "sessions that bounced"
    would silently become "requests that bounced", and joins to rpc/tool records
    would match nothing."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))

    async def rejecting(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"mcp-session-id", b"throwaway")],
            }
        )

    await _mw(rejecting)(_scope(), None, lambda m: _noop())
    (rec,) = _records(tmp_path)
    assert rec["status"] == 400
    assert "session_id" not in (rec.get("client") or {})


@_async_test
async def test_missing_session_id_records_no_session_over_the_real_app(tmp_path, monkeypatch):
    """The same guarantee, over the real SDK app rather than a stub - this is the
    exact request the ContextVM gateway sends."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    transport = httpx2.ASGITransport(app=app)
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=transport, base_url="http://t"
    ) as client:
        resp = await client.post(
            "/mcp",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        )
    assert resp.status_code == 400
    (rec,) = _records(tmp_path)
    assert "session_id" not in (rec.get("client") or {})


def test_traffic_type_matches_the_json_rpc_seam_on_percent_encoding():
    """Both seams read the same param; if one percent-decodes and the other does
    not, the same URL is "marked" to one and unmarked to the other."""
    assert _traffic_type({"query_string": b"traffic%5Ftype=internal"}) == "internal"
