"""Recovery-hint seam: does the transport's own rejection now tell the client how
to recover, and - the part that would actually hurt if it broke - does everything
else still pass through byte-for-byte?

The integration tests at the bottom run over the real Python MCP SDK ASGI app,
because a stub downstream would prove only that our rewriter can rewrite; what
needs proving is that the SDK's ACTUAL 400 (the one a live gateway has been
collecting for eight hours) comes out enriched.
"""

import functools
import json
import os

import anyio
import httpx2

from nostrology_mcp import server as srv_mod
from nostrology_mcp.asgilog import HttpCallLogMiddleware, on_mcp_path
from nostrology_mcp.recovery import (
    MAX_REWRITE_BYTES,
    RECOVERY_HINTS,
    RecoveryHintMiddleware,
)


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return anyio.run(functools.partial(fn, *args, **kwargs))

    return wrapper


def _scope(path="/mcp", method="POST"):
    return {"type": "http", "method": method, "path": path, "headers": [], "query_string": b""}


class _Sink:
    """Collects ASGI messages and reassembles the response the client would see."""

    def __init__(self):
        self.messages = []

    async def __call__(self, message):
        self.messages.append(message)

    @property
    def start(self):
        return next(m for m in self.messages if m["type"] == "http.response.start")

    @property
    def body(self) -> bytes:
        out = bytearray()
        for m in self.messages:
            if m["type"] == "http.response.body":
                out.extend(m.get("body") or b"")
        return bytes(out)

    def header(self, name: bytes):
        for key, value in self.start.get("headers") or ():
            if key.lower() == name:
                return value
        return None


def _json_app(status, payload, *, chunks=1, content_type=b"application/json", declare_length=True):
    """A downstream app answering like Starlette's `Response`: content-length set,
    body optionally split across chunks.

    `declare_length=False` is what reaches the ACTUAL-bytes ceiling: with a length
    declared, an oversized body is rejected by the cheap prefilter at the start
    message and the buffering path is never entered at all.
    """
    body = json.dumps(payload, separators=(",", ":")).encode()

    async def app(scope, receive, send):
        headers = [(b"content-type", content_type)]
        if declare_length:
            headers.append((b"content-length", str(len(body)).encode()))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        size = (len(body) + chunks - 1) // chunks
        for i in range(chunks):
            piece = body[i * size : (i + 1) * size]
            await send(
                {"type": "http.response.body", "body": piece, "more_body": i < chunks - 1}
            )

    return app


def _sdk_error(code=-32600, message="Bad Request: Missing session ID"):
    return {"jsonrpc": "2.0", "id": None, "error": {"code": code, "message": message}}


def _mw(app, path="/mcp"):
    return RecoveryHintMiddleware(app, path=path)


async def _run(app, scope=None):
    sink = _Sink()
    await _mw(app)(scope or _scope(), None, sink)
    return sink


# -- the enrichment itself --------------------------------------------------------


@_async_test
async def test_missing_session_400_gains_a_recovery_hint():
    sink = await _run(_json_app(400, _sdk_error()))
    payload = json.loads(sink.body)
    data = payload["error"]["data"]
    assert data["recovery"] == "initialize"
    assert "Mcp-Session-Id" in data["detail"]
    assert data["spec"].startswith("https://modelcontextprotocol.io/")


@_async_test
async def test_the_untouchable_parts_stay_untouched():
    """Compliance is the point: we ADD `data` and change nothing else. A client that
    switches on the status, the code or the message must not be able to tell."""
    original = _sdk_error()
    sink = await _run(_json_app(400, original))
    payload = json.loads(sink.body)
    assert sink.start["status"] == 400
    assert payload["jsonrpc"] == "2.0"
    assert payload["id"] is None
    assert payload["error"]["code"] == original["error"]["code"]
    assert payload["error"]["message"] == original["error"]["message"]


@_async_test
async def test_content_length_is_corrected_to_the_new_body():
    """The rewrite grows the body; a stale content-length would produce a response
    the client cannot frame - a far worse bug than the one being fixed."""
    sink = await _run(_json_app(400, _sdk_error()))
    assert int(sink.header(b"content-length")) == len(sink.body)
    # ...and exactly one of them, not the original plus ours.
    lengths = [v for k, v in sink.start["headers"] if k.lower() == b"content-length"]
    assert len(lengths) == 1


@_async_test
async def test_expired_session_404_says_initialize_again():
    sink = await _run(_json_app(404, _sdk_error(message="Not Found: Invalid or expired session ID")))
    assert json.loads(sink.body)["error"]["data"]["recovery"] == "initialize"


@_async_test
async def test_not_acceptable_and_conflict_get_their_own_tokens():
    for status, token in ((406, "accept"), (409, "single_sse_stream")):
        sink = await _run(_json_app(status, _sdk_error(message="x")))
        assert json.loads(sink.body)["error"]["data"]["recovery"] == token


@_async_test
async def test_a_body_split_across_chunks_is_still_rewritten():
    sink = await _run(_json_app(400, _sdk_error(), chunks=3))
    assert json.loads(sink.body)["error"]["data"]["recovery"] == "initialize"
    assert int(sink.header(b"content-length")) == len(sink.body)


# -- everything we must NOT touch -------------------------------------------------


@_async_test
async def test_a_parse_error_400_gets_no_session_advice():
    """Both are 400s from the same handler. Keying on the status alone would answer
    "run initialize" to a client whose real problem is malformed JSON on a session
    that is perfectly valid - advice that is not merely useless but wrong."""
    sink = await _run(_json_app(400, _sdk_error(code=-32700, message="Parse error: ...")))
    assert "data" not in json.loads(sink.body)["error"]
    assert int(sink.header(b"content-length")) == len(sink.body)


@_async_test
async def test_an_existing_data_field_is_never_overwritten():
    """If a future Python MCP SDK starts populating `data`, its value is the
    authoritative one; replacing it would be a downgrade dressed as an improvement."""
    payload = _sdk_error()
    payload["error"]["data"] = {"mine": True}
    sink = await _run(_json_app(400, payload))
    assert json.loads(sink.body)["error"]["data"] == {"mine": True}


@_async_test
async def test_success_responses_are_forwarded_unbuffered():
    """The SSE stream is a 200. If a 200 could ever be withheld, `GET /mcp` would
    stop streaming - the exact failure `BaseHTTPMiddleware` was avoided for."""
    seen = []

    async def streaming(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        # The start must already be through before the first chunk is produced.
        seen.append(len(sink.messages))
        await send({"type": "http.response.body", "body": b"data: 1\n\n", "more_body": True})
        seen.append(len(sink.messages))
        await send({"type": "http.response.body", "body": b"data: 2\n\n", "more_body": False})

    sink = _Sink()
    await _mw(streaming)(_scope(method="GET"), None, sink)
    assert seen == [1, 2], "response was buffered instead of streamed"
    assert sink.body == b"data: 1\n\ndata: 2\n\n"


@_async_test
async def test_non_json_error_bodies_pass_through():
    """Starlette's own plain-text 404 for an unrouted /mcp/... subpath."""

    async def plain(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain; charset=utf-8")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not Found"})

    sink = await _run(plain, _scope(path="/mcp/nope"))
    assert sink.body == b"Not Found"


@_async_test
async def test_non_mcp_paths_are_not_intercepted():
    sink = await _run(_json_app(400, _sdk_error()), _scope(path="/health"))
    assert "data" not in json.loads(sink.body)["error"]


@_async_test
async def test_non_http_scopes_pass_through():
    seen = []

    async def app(scope, receive, send):
        seen.append(scope["type"])

    await _mw(app)({"type": "lifespan"}, None, None)
    assert seen == ["lifespan"]


@_async_test
async def test_an_oversized_body_is_released_mid_stream():
    """No auth, no rate limit: an error path that streamed something large must not
    be silently accumulated whole in memory.

    `declare_length=False` is load-bearing - with a length declared this never gets
    past the prefilter and would silently duplicate the test below, leaving the
    actual mid-stream release (the one place `more_body` is propagated out of a
    partial buffer) with no coverage at all.
    """
    # 4x the ceiling over 4 chunks, so the FIRST chunk already busts it and the
    # release happens with `more_body` still true. Sized just over the ceiling
    # instead, only the final chunk trips it and the mid-stream path is skipped.
    filler = "x" * (MAX_REWRITE_BYTES * 4)
    big = {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": filler}}
    sink = await _run(_json_app(400, big, chunks=4, declare_length=False))
    assert json.loads(sink.body)["error"]["message"] == filler
    assert "data" not in json.loads(sink.body)["error"]
    # Released mid-stream, so the tail must still arrive as separate chunks with
    # `more_body` set on all but the last - a release that dropped the flag would
    # truncate every large response.
    flags = [m.get("more_body") for m in sink.messages if m["type"] == "http.response.body"]
    assert len(flags) > 1 and flags[-1] is False and all(flags[:-1])


@_async_test
async def test_a_declared_oversized_length_is_never_withheld():
    """The cheap prefilter: don't even start buffering when the app says up front
    that the body is bigger than we would ever rewrite."""
    order = []

    async def app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(MAX_REWRITE_BYTES * 2).encode()),
                ],
            }
        )
        order.append(len(sink.messages))
        await send({"type": "http.response.body", "body": b"{}"})

    sink = _Sink()
    await _mw(app)(_scope(), None, sink)
    assert order == [1], "start was withheld despite a declared oversized length"


@_async_test
async def test_a_withheld_start_with_no_body_is_still_released():
    """An app that starts a candidate response and then returns without a body would
    otherwise leave the request hanging on a header frame we are holding."""

    async def truncated(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"content-type", b"application/json")],
            }
        )

    sink = await _run(truncated)
    assert sink.start["status"] == 400
    assert sink.body == b""


@_async_test
async def test_a_malformed_json_body_is_forwarded_verbatim():
    async def broken(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b"{not json"})

    sink = await _run(broken)
    assert sink.body == b"{not json"


def test_path_scoping_is_shared_with_the_call_log_seam():
    """Imported, not re-declared: two ASGI seams disagreeing about which requests
    are "the MCP endpoint" would be invisible from either file alone."""
    assert on_mcp_path({"path": "/mcp"}, "/mcp")
    assert on_mcp_path({"path": "/mcp/x"}, "/mcp")
    assert not on_mcp_path({"path": "/mcpsomething"}, "/mcp")


def test_recovery_tokens_are_a_stable_contract():
    """Clients may switch on `recovery`; these tokens are an API. Add, never rename -
    this test exists to make a rename a deliberate act rather than a refactor."""
    assert {h["recovery"] for h in RECOVERY_HINTS.values()} == {
        "initialize",
        "accept",
        "single_sse_stream",
    }
    for hint in RECOVERY_HINTS.values():
        assert set(hint) == {"recovery", "detail", "spec", "docs"}


def test_only_client_errors_are_ever_candidates():
    """The buffering path is derived from this table, so a `(200, ...)` or
    `(202, ...)` row added in good faith would silently put `GET /mcp` - the SSE
    stream - behind a body buffer. That is the one failure this whole design is
    shaped to avoid, and nothing but this assertion stands between it and a
    plausible-looking one-line addition."""
    assert all(400 <= status < 500 for status, _ in RECOVERY_HINTS)


# -- integration: the real Python MCP SDK app -------------------------------------


def _real_app():
    app = srv_mod.server.streamable_http_app(
        streamable_http_path=srv_mod.MCP_PATH, json_response=True, host="0.0.0.0"
    )
    app.add_middleware(RecoveryHintMiddleware, path=srv_mod.MCP_PATH)
    return app


async def _post(client, body, headers=None):
    return await client.post(
        "/mcp",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **(headers or {}),
        },
        json=body,
    )


@_async_test
async def test_the_real_sdk_400_carries_the_hint(tmp_path, monkeypatch):
    """THE test: the exact request the ContextVM gateway sends, answered by the real
    Python MCP SDK, must come back with the recovery data attached."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://t"
    ) as client:
        resp = await _post(client, {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    assert resp.status_code == 400
    error = resp.json()["error"]
    assert error["message"] == "Bad Request: Missing session ID"
    assert error["data"]["recovery"] == "initialize"
    # httpx2 frames the body from content-length; a mismatch would fail before here,
    # but pin it explicitly since that is the one thing this middleware can break.
    assert int(resp.headers["content-length"]) == len(resp.content)


@_async_test
async def test_the_real_sdk_406_carries_the_accept_hint(tmp_path, monkeypatch):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://t"
    ) as client:
        resp = await client.post(
            "/mcp",
            headers={"Content-Type": "application/json", "Accept": "text/plain"},
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}},
        )
    assert resp.status_code == 406
    assert resp.json()["error"]["data"]["recovery"] == "accept"


@_async_test
async def test_the_two_seams_stack_without_blinding_each_other(tmp_path, monkeypatch):
    """Both seams wrap the same app, in the order main() installs them. The risk is
    specific: the recovery seam WITHHOLDS `http.response.start` and re-sends a copy,
    and the call log reads the status off that message - so a rewrite that dropped or
    altered it would silently turn every enriched rejection into a `status: null`
    record, re-opening the blind spot the call log was built to close."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = srv_mod.server.streamable_http_app(
        streamable_http_path=srv_mod.MCP_PATH, json_response=True, host="0.0.0.0"
    )
    app.add_middleware(RecoveryHintMiddleware, path=srv_mod.MCP_PATH)
    app.add_middleware(
        HttpCallLogMiddleware,
        path=srv_mod.MCP_PATH,
        server_version=srv_mod.__version__,
        git_sha=srv_mod.GIT_SHA,
    )
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://t"
    ) as client:
        resp = await _post(client, {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})

    assert resp.status_code == 400
    assert resp.json()["error"]["data"]["recovery"] == "initialize"
    # The MORE fragile of the two properties the rewrite must preserve: the rebuilt
    # start message carries every original header. A `_with_content_length` that
    # constructed a fresh header list instead of filtering one would drop this and
    # the status assertion below would still pass.
    assert resp.headers.get("mcp-session-id")

    (day_file,) = [f for f in os.listdir(tmp_path) if f.endswith(".jsonl")]
    with open(os.path.join(tmp_path, day_file)) as fh:
        (rec,) = [json.loads(line) for line in fh]
    assert rec["kind"] == "http"
    assert rec["status"] == 400
    assert rec["had_session_header"] is False


@_async_test
async def test_a_successful_handshake_is_unaffected(tmp_path, monkeypatch):
    """The happy path through the real app, with both the response body and the
    session header intact - proof the seam costs the working client nothing.

    The log dir is redirected even though this test asserts nothing about the log:
    the real app carries the JSON-RPC seam, so without it every run appends a
    fabricated `initialize` to the developer's own `logs/calls-*.jsonl` - the corpus
    the README's `jq` recipes analyse.
    """
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    app = _real_app()
    async with app.router.lifespan_context(app), httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://t"
    ) as client:
        resp = await _post(
            client,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": {"name": "recovery-test", "version": "1.0"},
                },
            },
        )
    assert resp.status_code == 200
    assert resp.headers["mcp-session-id"]
    assert resp.json()["result"]["serverInfo"]["name"] == "nostrology"


@_async_test
async def test_a_second_start_while_one_is_held_stays_loud():
    """An ASGI protocol violation must not be laundered into a silently malformed
    response. Overwriting the held start would concatenate two JSON bodies into one
    unparseable body served under the second status; releasing the first instead
    keeps the violation where uvicorn can raise on it."""
    sink = _Sink()

    async def double_start(scope, receive, send):
        # The first start is a candidate and is WITHHELD; sending a second before
        # any body is what puts two starts in flight at once.
        for status in (404, 400):
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
        await send({"type": "http.response.body", "body": json.dumps(_sdk_error()).encode()})

    await _mw(double_start)(_scope(), None, sink)
    starts = [m["status"] for m in sink.messages if m["type"] == "http.response.start"]
    assert starts == [404, 400], "the held response was swallowed instead of released"


@_async_test
async def test_a_body_before_any_start_is_forwarded():
    """Also a protocol violation, and also not ours to swallow: forward it and let
    the server complain."""
    sink = _Sink()

    async def body_first(scope, receive, send):
        await send({"type": "http.response.body", "body": b"stray"})

    await _mw(body_first)(_scope(), None, sink)
    assert sink.body == b"stray"


@_async_test
async def test_an_unparseable_content_length_does_not_break_the_response():
    """Every decision helper swallows its own errors: a malformed header costs the
    enrichment, never the request."""
    sink = _Sink()

    async def bad_header(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 400,
                "headers": [(b"content-type", b"application/json"), (b"content-length", b"not-a-number")],
            }
        )
        await send({"type": "http.response.body", "body": json.dumps(_sdk_error()).encode()})

    await _mw(bad_header)(_scope(), None, sink)
    assert sink.start["status"] == 400
    assert json.loads(sink.body)["error"]["message"] == "Bad Request: Missing session ID"
