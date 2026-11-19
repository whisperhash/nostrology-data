"""Duplicate in-flight JSON-RPC id: does the collision still corrupt, and does the
guard fix it without touching anything else?

The integration tests at the bottom run over the REAL Python MCP SDK ASGI app, and the
first of them is the important one: it reproduces the three-way corruption
(cross-delivery / hang / 500) against the unguarded app, so if a future SDK version ever
fixes the overwrite upstream, that test fails and tells us this middleware has become
dead weight. A stub downstream would prove only that our bookkeeping is self-consistent;
what needs proving is what the SDK actually does with two live requests on one id.
"""

import functools
import json

import anyio
import httpx2

from nostrology_mcp import server as srv_mod
from nostrology_mcp.inflight import (
    MAX_INSPECT_BYTES,
    MAX_WAITERS_PER_ID,
    DuplicateRequestIdMiddleware,
    _parsed_request,
)
from nostrology_mcp.recovery import DUPLICATE_REQUEST_ID_HINT, INVALID_REQUEST

HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}

# result key -> the method that must have produced it. The whole cross-delivery check is
# "did the body that came back belong to the call we made".
RESULT_KEY = {
    "tools/list": "tools",
    "resources/list": "resources",
    "resources/templates/list": "resourceTemplates",
    "prompts/list": "prompts",
}


def _async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return anyio.run(functools.partial(fn, *args, **kwargs))

    return wrapper


def _app(*, guarded: bool):
    app = srv_mod.server.streamable_http_app(
        streamable_http_path=srv_mod.MCP_PATH, json_response=True, host="0.0.0.0"
    )
    if guarded:
        app.add_middleware(DuplicateRequestIdMiddleware, path=srv_mod.MCP_PATH)
    return app


async def _handshake(client) -> dict:
    """initialize + notifications/initialized, returning headers with the session id."""
    resp = await client.post(
        srv_mod.MCP_PATH,
        headers=HEADERS,
        json={
            "jsonrpc": "2.0",
            "id": "handshake",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0.0"},
            },
        },
    )
    assert resp.status_code == 200, resp.text
    headers = {**HEADERS, "Mcp-Session-Id": resp.headers["mcp-session-id"]}
    await client.post(
        srv_mod.MCP_PATH, headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    return headers


async def _burst(client, headers, methods, *, request_id, timeout=4.0):
    """Fire `methods` concurrently, all under `request_id`. Returns per-call outcomes.

    A per-call timeout, not a whole-run one: the unguarded failure mode is a request that
    never returns at all, and a test that hangs is a test that tells you nothing.
    """
    results = {}

    async def one(method):
        try:
            with anyio.fail_after(timeout):
                resp = await client.post(
                    srv_mod.MCP_PATH,
                    headers=headers,
                    json={"jsonrpc": "2.0", "id": request_id, "method": method, "params": {}},
                )
            results[method] = (resp.status_code, resp.text)
        except TimeoutError:
            results[method] = ("HANG", "")

    async with anyio.create_task_group() as tg:
        for method in methods:
            tg.start_soon(one, method)
    return results


def _served_correctly(method, outcome) -> bool:
    status, text = outcome
    if status != 200:
        return False
    try:
        result = json.loads(text).get("result") or {}
    except Exception:
        return False
    return RESULT_KEY[method] in result


# --------------------------------------------------------------------------------------
# Unit level: what counts as a trackable request at all.
# --------------------------------------------------------------------------------------


def test_parsed_request_accepts_a_plain_request():
    assert _parsed_request(b'{"jsonrpc":"2.0","id":"a","method":"tools/list"}') == ("a", "tools/list")


def test_parsed_request_stringifies_the_id_like_the_sdk():
    """`streamable_http.py` keys on `str(message.id)`, so `1` and `"1"` are ONE key
    there and must be one key here - otherwise the guard would wave through exactly the
    collision the SDK then suffers."""
    assert _parsed_request(b'{"jsonrpc":"2.0","id":1,"method":"m"}')[0] == "1"
    assert _parsed_request(b'{"jsonrpc":"2.0","id":"1","method":"m"}')[0] == "1"


def test_parsed_request_ignores_notifications():
    """No id, no response, no stream to collide on. Tracking these would be pure false
    positive - and ContextVM sends `notifications/initialized` in the same burst."""
    assert _parsed_request(b'{"jsonrpc":"2.0","method":"notifications/initialized"}') is None


def test_parsed_request_ignores_null_id_batches_and_garbage():
    assert _parsed_request(b'{"jsonrpc":"2.0","id":null,"method":"m"}') is None
    assert _parsed_request(b'[{"jsonrpc":"2.0","id":"a","method":"m"}]') is None
    assert _parsed_request(b"not json at all") is None
    assert _parsed_request(b'{"jsonrpc":"2.0","id":"a"}') is None  # no method: a response


def test_parsed_request_ignores_structured_ids():
    """JSON-RPC ids are strings or numbers. A dict or list id would stringify to
    something with no stable ordering guarantee, so it is not a key we want."""
    assert _parsed_request(b'{"jsonrpc":"2.0","id":{"a":1},"method":"m"}') is None
    assert _parsed_request(b'{"jsonrpc":"2.0","id":true,"method":"m"}') is None


# --------------------------------------------------------------------------------------
# Integration: the real SDK app, guarded and unguarded.
# --------------------------------------------------------------------------------------


@_async_test
async def test_unguarded_app_corrupts_on_duplicate_id():
    """THE reproduction. Two concurrent requests sharing one id, straight at the SDK.

    Asserts only that SOMETHING went wrong, not which of the three modes - the mode is a
    race and pinning it would make this flaky. If this ever passes cleanly, the SDK has
    fixed `_request_streams` upstream and `inflight.py` can go.
    """
    app = _app(guarded=False)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://t"
        ) as client:
            headers = await _handshake(client)
            outcomes = await _burst(
                client, headers, ["tools/list", "prompts/list"], request_id="announcement"
            )
    def _is_a_known_corruption(method, outcome) -> bool:
        """One of the three documented modes - NOT merely "something failed".

        The weaker `not all(_served_correctly(...))` form this replaces stayed green when
        our own harness rotted (a broken handshake, a renamed RESULT_KEY, a slow machine
        tripping the burst timeout would all satisfy it). This test's job is to be the
        canary that fires when the SDK fixes `_request_streams` upstream, so it has to
        fail on anything that is not specifically this bug.
        """
        status, text = outcome
        if status == "HANG":
            return True  # mode 2
        if status == 500:
            return True  # mode 3
        if status != 200:
            return False  # a 4xx is not this bug - the harness is broken
        try:
            result = json.loads(text).get("result") or {}
        except Exception:
            return False
        if RESULT_KEY[method] in result:
            return False  # served correctly
        # mode 1: a 200 carrying a DIFFERENT method's result key.
        return any(key in result for m, key in RESULT_KEY.items() if m != method)

    assert any(
        _is_a_known_corruption(method, outcome) for method, outcome in outcomes.items()
    ), f"expected one of the three documented corruption modes, got {outcomes}"


@_async_test
async def test_guard_serializes_the_contextvm_announcement_burst():
    """The live case: four announcement calls fired together, every one stamped
    `id: "announcement"` exactly as the ContextVM gateway sends them.

    All four must come back 200 AND carry their OWN result. Status alone is not enough -
    cross-delivery returns 200 with somebody else's body, which is the mode that would
    publish the wrong payload into their directory.
    """
    app = _app(guarded=True)
    methods = ["tools/list", "resources/list", "resources/templates/list", "prompts/list"]
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://t"
        ) as client:
            headers = await _handshake(client)
            outcomes = await _burst(client, headers, methods, request_id="announcement")
    for method in methods:
        assert _served_correctly(method, outcomes[method]), f"{method} -> {outcomes[method]}"


@_async_test
async def test_guard_leaves_unique_ids_untouched():
    """The control, and the property that matters most: a correct client must not be able
    to tell this middleware exists."""
    app = _app(guarded=True)
    methods = ["tools/list", "resources/list", "prompts/list"]
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://t"
        ) as client:
            headers = await _handshake(client)
            results = {}

            async def one(i, method):
                resp = await client.post(
                    srv_mod.MCP_PATH,
                    headers=headers,
                    json={"jsonrpc": "2.0", "id": f"u{i}", "method": method, "params": {}},
                )
                results[method] = (resp.status_code, resp.text)

            async with anyio.create_task_group() as tg:
                for i, method in enumerate(methods):
                    tg.start_soon(one, i, method)
    for method in methods:
        assert _served_correctly(method, results[method]), f"{method} -> {results[method]}"


@_async_test
async def test_sequential_reuse_of_an_id_is_allowed():
    """Deliberately narrower than the spec, which bars reuse within a session outright.
    ContextVM reuses `id: "announcement"` sequentially and works; refusing that would
    break a client to punish a violation that harms nobody. Only OVERLAP corrupts."""
    app = _app(guarded=True)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app), base_url="http://t"
        ) as client:
            headers = await _handshake(client)
            for method in ["tools/list", "prompts/list", "tools/list"]:
                resp = await client.post(
                    srv_mod.MCP_PATH,
                    headers=headers,
                    json={"jsonrpc": "2.0", "id": "reused", "method": method, "params": {}},
                )
                assert resp.status_code == 200, resp.text
                assert RESULT_KEY[method] in json.loads(resp.text)["result"]


def test_parsed_request_ignores_an_absurdly_long_id():
    """Both halves of the registry key are attacker-controlled. An id past
    `MAX_KEY_FIELD_CHARS` is not tracked at all rather than becoming a dict key."""
    from nostrology_mcp.inflight import MAX_KEY_FIELD_CHARS

    ok = json.dumps({"jsonrpc": "2.0", "id": "a" * MAX_KEY_FIELD_CHARS, "method": "m"}).encode()
    too_long = json.dumps({"jsonrpc": "2.0", "id": "a" * (MAX_KEY_FIELD_CHARS + 1), "method": "m"}).encode()
    assert _parsed_request(ok) is not None
    assert _parsed_request(too_long) is None


@_async_test
async def test_forged_or_oversized_session_ids_are_never_keyed():
    """The key is taken BEFORE the SDK validates the session, so a session id it is about
    to reject must not mint a registry entry on the way through."""
    from nostrology_mcp.inflight import MAX_KEY_FIELD_CHARS

    reached = []

    async def app(scope, receive, send):
        # Reached, but must NOT be asked for the body: an untrackable session id
        # short-circuits above the buffering, so the request costs us nothing.
        reached.append(1)

    async def receive():
        raise AssertionError("body must not be read for an untrackable session id")

    async def send(message):
        pass

    for bad in [b"x" * (MAX_KEY_FIELD_CHARS + 1), b"has space", b"tab\there"]:
        guard = DuplicateRequestIdMiddleware(app, path="/mcp")
        await guard(
            {
                "type": "http",
                "method": "POST",
                "path": "/mcp",
                "query_string": b"",
                "headers": [(b"mcp-session-id", bad)],
            },
            receive,
            send,
        )
        assert guard._in_flight == {}, f"{bad!r} became a registry key"
    assert len(reached) == 3


@_async_test
async def test_timeout_refuses_when_the_holder_never_finishes():
    """The OTHER refusal path, and the only one involving a timer: a holder that outlasts
    `MAX_WAIT_SECONDS`. Patched down to milliseconds so the assertion is about the branch,
    not about the wall clock."""
    import nostrology_mcp.inflight as inflight_mod

    original = inflight_mod.MAX_WAIT_SECONDS
    inflight_mod.MAX_WAIT_SECONDS = 0.05
    try:
        release = anyio.Event()
        entered = anyio.Event()
        statuses = []

        async def wedged_app(scope, receive, send):
            await receive()
            entered.set()
            await release.wait()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        guard = DuplicateRequestIdMiddleware(wedged_app, path="/mcp")
        body = json.dumps({"jsonrpc": "2.0", "id": "wedged", "method": "tools/list"}).encode()
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", b"s1")],
        }

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def call():
            async def send(message):
                if message["type"] == "http.response.start":
                    statuses.append(message["status"])

            await guard(dict(scope), receive, send)

        async with anyio.create_task_group() as tg:
            tg.start_soon(call)
            await entered.wait()
            tg.start_soon(call)  # waits, then times out
            await anyio.sleep(0.3)
            assert statuses == [409], statuses
            release.set()
    finally:
        inflight_mod.MAX_WAIT_SECONDS = original
    assert guard._waiters == {}


@_async_test
async def test_two_sessions_sharing_an_id_do_not_interact():
    """Ids are unique only WITHIN a session, so the key must be scoped by session - a
    global registry would make two unrelated clients block each other."""
    entered = anyio.Event()
    release = anyio.Event()
    order = []

    async def app(scope, receive, send):
        await receive()
        first = not order
        order.append(1)
        if first:
            entered.set()
            await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    body = json.dumps({"jsonrpc": "2.0", "id": "same", "method": "tools/list"}).encode()

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        pass

    def scope_for(session):
        return {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", session)],
        }

    async with anyio.create_task_group() as tg:
        tg.start_soon(guard, scope_for(b"session-a"), receive, send)
        await entered.wait()
        # Same JSON-RPC id, DIFFERENT session: must not wait on session-a's holder.
        await guard(scope_for(b"session-b"), receive, send)
        assert len(order) == 2, "session-b was blocked by session-a"
        release.set()


@_async_test
async def test_disconnect_mid_body_forwards_what_was_read():
    """A client that goes away mid-body: the guard must not parse a partial body, and must
    replay the messages it consumed - including the disconnect - in order."""
    seen = []

    async def app(scope, receive, send):
        while True:
            message = await receive()
            seen.append(message["type"])
            if message["type"] != "http.request":
                break

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    parts = [
        {"type": "http.request", "body": b'{"jsonrpc":"2.0",', "more_body": True},
        {"type": "http.disconnect"},
    ]

    async def receive():
        return parts.pop(0)

    async def send(message):
        pass

    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", b"s1")],
        },
        receive,
        send,
    )
    assert seen == ["http.request", "http.disconnect"]
    assert guard._in_flight == {}


@_async_test
async def test_replay_delegates_to_the_real_receive_once_drained():
    """An app that reads past the end of the body must reach the underlying `receive`,
    not a stuck buffer."""
    seen = []

    async def app(scope, receive, send):
        for _ in range(2):
            seen.append((await receive())["type"])

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    body = json.dumps({"jsonrpc": "2.0", "id": "x", "method": "tools/list"}).encode()
    queue = [
        {"type": "http.request", "body": body, "more_body": False},
        {"type": "http.disconnect"},
    ]

    async def receive():
        return queue.pop(0)

    async def send(message):
        pass

    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", b"s1")],
        },
        receive,
        send,
    )
    assert seen == ["http.request", "http.disconnect"]
    assert guard._in_flight == {}


@_async_test
async def test_queue_past_the_waiter_cap_is_refused_with_the_hint(monkeypatch):
    """The bound. One request holds the id, `MAX_WAITERS_PER_ID` park behind it, and the
    next one must be refused rather than queued - otherwise a single duplicated id is a
    way to hold connections open on an endpoint with no auth and no rate limit.

    Driven against a stub that blocks, not the SDK: the point is the queue discipline,
    and a real handler is too fast to keep a slot held while the others pile up.

    `MAX_WAIT_SECONDS` is patched up so the wall clock plays no part in the assertion -
    otherwise a stall between parking and `released.set()` would produce extra 409s from
    the TIMEOUT branch and flip the result for a reason this test is not about.
    """
    import nostrology_mcp.inflight as inflight_mod

    monkeypatch.setattr(inflight_mod, "MAX_WAIT_SECONDS", 300.0)
    released = anyio.Event()
    started = anyio.Event()

    async def slow_app(scope, receive, send):
        await receive()
        started.set()
        await released.wait()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})

    guard = DuplicateRequestIdMiddleware(slow_app, path="/mcp")
    body = json.dumps({"jsonrpc": "2.0", "id": "dup", "method": "tools/list"}).encode()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "query_string": b"",
        "headers": [(b"mcp-session-id", b"s1")],
    }
    statuses = []

    async def call():
        sent = []

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            if message["type"] == "http.response.start":
                sent.append(message["status"])

        await guard(dict(scope), receive, send)
        statuses.extend(sent)

    async with anyio.create_task_group() as tg:
        tg.start_soon(call)
        await started.wait()  # holder is in the app and staying there
        for _ in range(MAX_WAITERS_PER_ID + 1):
            tg.start_soon(call)
        await anyio.sleep(0.15)  # let every waiter park
        released.set()

    assert statuses.count(409) == 1, statuses
    assert statuses.count(200) == MAX_WAITERS_PER_ID + 1, statuses


def test_conflict_body_carries_the_hint_and_a_null_id():
    """The refusal must be self-explaining, and must NOT echo the duplicate id - an
    echoing error is indistinguishable from the answer to the request still running."""
    from nostrology_mcp.inflight import _conflict_body

    payload = json.loads(_conflict_body("tools/list"))
    assert payload["id"] is None
    assert payload["error"]["code"] == INVALID_REQUEST
    data = payload["error"]["data"]
    assert data["recovery"] == DUPLICATE_REQUEST_ID_HINT["recovery"] == "unique_request_id"
    assert data["method"] == "tools/list"
    assert data["spec"].startswith("https://modelcontextprotocol.io/")


def test_conflict_body_omits_method_when_it_is_unknown():
    """`method` is additive. Refusing without it must still produce a valid hint rather
    than a `"method": null` a client would have to special-case."""
    from nostrology_mcp.inflight import _conflict_body

    data = json.loads(_conflict_body(None))["error"]["data"]
    assert "method" not in data
    assert data["recovery"] == "unique_request_id"


@_async_test
async def test_a_receive_that_raises_forwards_instead_of_failing_the_request():
    """The guard must never turn a transport hiccup into a failure of its own: a
    `receive()` that raises falls back to forwarding what was read, unchecked."""
    forwarded = []

    async def app(scope, receive, send):
        forwarded.append(1)

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    calls = []

    async def receive():
        calls.append(1)
        raise RuntimeError("transport blew up")

    async def send(message):
        pass

    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", b"s1")],
        },
        receive,
        send,
    )
    assert forwarded == [1]
    assert guard._in_flight == {}


def test_releasing_an_unheld_key_is_a_no_op():
    """`_release` runs from a `finally`; it must tolerate a key that is not there rather
    than raising inside cleanup and masking whatever caused it."""

    async def app(scope, receive, send):
        pass

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    guard._release(("nobody", "nothing"))
    assert guard._in_flight == {}


def test_conflict_body_does_not_mutate_the_shared_hint():
    """`_conflict_body` adds `method` to the hint dict. Doing that in place would make
    every later rejection carry whichever method happened to be refused first."""
    from nostrology_mcp.inflight import _conflict_body

    _conflict_body("tools/list")
    assert "method" not in DUPLICATE_REQUEST_ID_HINT


@_async_test
async def test_oversized_body_is_forwarded_unchecked():
    """A body past the inspect cap must reach the app byte-for-byte. The guard's job is
    to be invisible when it cannot help, not to reject what it could not parse."""
    seen = {}

    async def echo_app(scope, receive, send):
        chunks = bytearray()
        while True:
            message = await receive()
            if message["type"] != "http.request":
                break
            chunks.extend(message.get("body") or b"")
            if not message.get("more_body"):
                break
        seen["body"] = bytes(chunks)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    guard = DuplicateRequestIdMiddleware(echo_app, path="/mcp")
    big = b"x" * (MAX_INSPECT_BYTES + 10)
    parts = [
        {"type": "http.request", "body": big[: len(big) // 2], "more_body": True},
        {"type": "http.request", "body": big[len(big) // 2 :], "more_body": False},
    ]

    async def receive():
        return parts.pop(0)

    async def send(message):
        pass

    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/mcp",
            "query_string": b"",
            "headers": [(b"mcp-session-id", b"s1")],
        },
        receive,
        send,
    )
    assert seen["body"] == big


@_async_test
async def test_non_mcp_and_non_post_requests_are_passed_straight_through():
    """Scope discipline: the guard reads request bodies, so it must not be anywhere near
    the GET SSE stream or any path Caddy does not proxy here."""
    calls = []

    async def app(scope, receive, send):
        calls.append((scope["method"], scope["path"]))

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")

    async def receive():
        raise AssertionError("body must not be read on a pass-through")

    async def send(message):
        pass

    for method, path in [("GET", "/mcp"), ("POST", "/health"), ("POST", "/registry.json")]:
        await guard(
            {"type": "http", "method": method, "path": path, "query_string": b"", "headers": []},
            receive,
            send,
        )
    assert calls == [("GET", "/mcp"), ("POST", "/health"), ("POST", "/registry.json")]


@_async_test
async def test_no_session_header_is_passed_through_unread():
    """Without a session there is nothing to scope an id to, and the SDK's own 400 (with
    the initialize hint) is the more useful answer."""
    calls = []

    async def app(scope, receive, send):
        calls.append(scope["path"])

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")

    async def receive():
        raise AssertionError("body must not be read without a session header")

    async def send(message):
        pass

    await guard(
        {"type": "http", "method": "POST", "path": "/mcp", "query_string": b"", "headers": []},
        receive,
        send,
    )
    assert calls == ["/mcp"]


@_async_test
async def test_fail_open_request_does_not_release_somebody_elses_slot():
    """The safety valve proceeds WITHOUT registering. If it also released, it would pop an
    entry a different request legitimately holds and wake that request's waiters early -
    manufacturing the exact collision this middleware prevents, and only under the load
    where the valve fires.

    Driven by pre-filling the registry past the cap with a real holder for the key under
    test, so the next call takes the fail-open branch against a live entry.
    """
    from nostrology_mcp.inflight import MAX_TRACKED_IN_FLIGHT

    release = anyio.Event()
    inside = []

    async def app(scope, receive, send):
        await receive()
        first = not inside
        inside.append(1)
        # ONLY the first request parks. The fail-open one must run to COMPLETION while
        # the holder is still inside, because the bug lives in its `finally` - an
        # assertion taken before that `finally` runs would pass either way, which is
        # exactly how the first version of this test passed against the broken code.
        if first:
            await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"2")]})
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})

    guard = DuplicateRequestIdMiddleware(app, path="/mcp")
    key = ("s1", "dup")
    body = json.dumps({"jsonrpc": "2.0", "id": "dup", "method": "tools/list"}).encode()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "query_string": b"",
        "headers": [(b"mcp-session-id", b"s1")],
    }

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        pass

    async with anyio.create_task_group() as tg:
        tg.start_soon(guard, dict(scope), receive, send)
        while not inside:
            await anyio.sleep(0)
        assert key in guard._in_flight
        holder_event = guard._in_flight[key]

        # Push the registry past its cap so the NEXT request takes the fail-open branch,
        # against a key a live request already holds.
        filler = {("pad", str(i)): anyio.Event() for i in range(MAX_TRACKED_IN_FLIGHT)}
        guard._in_flight.update(filler)
        # Awaited, not started: this one returns immediately, so by the time it returns
        # its `finally` has already run - which is the moment the bug would strike.
        await guard(dict(scope), receive, send)

        # The real holder must be untouched: still registered, event not fired. Before the
        # fix the fail-open request's `finally` popped this entry and set this event.
        assert guard._in_flight.get(key) is holder_event
        assert not holder_event.is_set()

        for pad_key in filler:
            guard._in_flight.pop(pad_key, None)
        release.set()

    assert key not in guard._in_flight
    assert guard._waiters == {}


@_async_test
async def test_slot_is_released_when_the_app_raises():
    """A downstream exception must not strand an id as permanently in-flight - that would
    turn one failed call into a session-long block on that id."""
    boom = RuntimeError("downstream failed")

    async def failing_app(scope, receive, send):
        await receive()
        raise boom

    guard = DuplicateRequestIdMiddleware(failing_app, path="/mcp")
    body = json.dumps({"jsonrpc": "2.0", "id": "x", "method": "tools/list"}).encode()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "query_string": b"",
        "headers": [(b"mcp-session-id", b"s1")],
    }

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        pass

    for _ in range(2):
        try:
            await guard(dict(scope), receive, send)
        except RuntimeError as err:
            assert err is boom
    assert guard._in_flight == {}
    assert guard._waiters == {}
