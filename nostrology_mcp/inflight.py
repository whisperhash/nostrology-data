"""Serialize (and, past a hard bound, refuse) a request reusing an in-flight JSON-RPC id.

NAMING: "the Python MCP SDK" below is `mcp` on PyPI, the one serving this endpoint.
ContextVM (the motivating case) has its own `@contextvm/sdk` and depends on the
TypeScript MCP SDK (`@modelcontextprotocol/sdk`); all three are named in full.

WHY THIS EXISTS - the traffic of 2026-08-04. A single `500` (internal server error)
appeared in the call log at 08:12:48.232, inside the ContextVM gateway's announcement
burst, with a session header, no matching JSON-RPC record, and NOTHING in container
stdout explaining it. The SDK's generic POST failure path logs `logger.exception`, so an
invisible 500 (internal server error) had to be
coming from somewhere else. It was: `streamable_http.py` answers a closed request stream
with `HTTPStatus.INTERNAL_SERVER_ERROR` under `logger.debug` - the one 500 in the file
that says nothing at INFO, which is why eight hours of stdout held only the uvicorn
access line.

THE ACTUAL BUG, reproduced locally against this app (see `tests/test_inflight.py`). The
SDK keys its per-request response plumbing on the CLIENT-CHOSEN JSON-RPC id:

    request_id = str(message.id)                           # streamable_http.py:601
    self._request_streams[request_id] = create_stream()     # :604  <- overwrites
    ...
    finally: await self._clean_up_memory_streams(request_id)    # :631  <- closes whoever
                                                                #        holds the key NOW

Two concurrent POSTs on one session carrying the same id therefore collide on one dict
entry, and which of three things happens is a race:

  1. CROSS-DELIVERY - the second request receives the FIRST one's result, at HTTP 200,
     under a matching JSON-RPC id. Verified locally: a `prompts/list` POST came back with
     `{"result":{"tools":[...]}}`. Nothing anywhere signals it. This is the dangerous one:
     an announcement built from that response would publish the wrong payload under the
     right kind, and every layer downstream would believe it.
  2. HANG - the orphaned request waits forever on a stream nobody will write to or close.
  3. 500 - the loser's stream is closed under it. The log line we started from.

ContextVM's announcement manager sends `tools/list`, `resources/list`,
`resources/templates/list` and `prompts/list` in one burst and stamps every one of them
`id: "announcement"` - visible in our own call log as four records with
`"request_id": "announcement"`. So it meets this on every gateway restart.

WHOSE BUG. Theirs first: MCP's base protocol says "The request ID MUST NOT have been
previously used by the requestor within the same session", and JSON-RPC needs the id to
decide which response answers which call - two live requests sharing one id makes that
undecidable by construction. The SDK's silent overwrite is arguably a second bug. Neither
is ours to fix, and we are not waiting for either: what we own is that our endpoint turns
their protocol error into OUR silent corruption.

WHY SERIALIZE RATHER THAN JUST REFUSE. Refusing was written first and was strictly
correct - the spec entitles us to it, and it kills modes 1 and 2 outright. It also leaves
the integration broken: whichever of their four announcement calls lost the race would
get a clean 409 instead of a confusing 500, and their published directory entry would be
missing that section either way. Holding the second request until the first finishes
costs single-digit milliseconds, needs nothing from them, and makes all four succeed -
which is the difference between documenting their bug and the interop actually working.
The requests are independent MCP calls; running two of them in sequence instead of
overlapped changes no semantics, only latency.

Refusal is still here, as the bound. Waiting is what a cooperating client gets; a client
that piles same-id requests up past `MAX_WAITERS_PER_ID` or holds one past
`MAX_WAIT_SECONDS` gets the 409 and the hint. That matters more than usual because this
endpoint has no auth and no rate limit: an unbounded queue would convert a client bug
into a way to hold connections open, so the queue is bounded first and useful second.

WHY "IN FLIGHT" IS THE RIGHT WINDOW, verified against the SDK rather than assumed. Two
facts make holding the slot for exactly the downstream call safe:

  * The SDK removes `_request_streams[request_id]` at `:631` strictly BEFORE it writes
    the response at `:632`, so there is no instant where we have released the id and it
    still holds the key.
  * uvicorn does NOT cancel the ASGI task when the client disconnects - `h11_impl.py` and
    `httptools_impl.py` only set `cycle.disconnected = True` - so the handler runs to
    completion and our slot covers the true server-side duration. A client that gives up
    and retries with the same id still meets the guard rather than racing the corpse of
    its first request.

The one residual this cannot cover: if anything ever DOES cancel the ASGI task while the
MCP handler is still running, the SDK's `message_router` will route that late response by
`str(message.id)` into whatever holds the key at that moment. That is an SDK hazard, not
one introduced here, and it is the only way sequential reuse could still corrupt.
"""

import json
import re

import anyio

from .asgilog import on_mcp_path
from .recovery import DUPLICATE_REQUEST_ID_HINT, INVALID_REQUEST

# Bodies larger than this are forwarded WITHOUT the check rather than buffered. This is
# a per-POST memory cost on EVERY request, not only duplicates: the body is held twice
# for the life of the request (the raw messages in `consumed`, captured by the `replay`
# closure, plus the joined copy) on top of the SDK's own copy underneath. The largest
# argument set this server accepts is a handful of short scalars, so 16 KiB is already
# ~100x headroom - and on an endpoint with no auth and no rate limit, headroom bought
# with attacker-controlled bytes is the wrong kind. A body over the cap keeps today's
# behaviour exactly: unknown input is passed through, not rejected.
MAX_INSPECT_BYTES = 16 * 1024

# Backstop against a holder that never finishes, NOT a bound on how long a normal call
# takes - the distinction matters and an earlier 2.0 got it wrong. Every DB call in
# `server.py` runs psycopg2 SYNCHRONOUSLY on the event loop, so one slow query stalls the
# whole loop; when it resumes, every parked waiter's wall-clock deadline has already
# passed and they all take the refusal branch at once. A 2-second bound therefore 409s
# the exact burst this middleware exists to serve, during exactly the Aurora hiccup where
# it matters. 30 s clears any stall the DB can produce (the `nostrology_mcp` role runs
# `statement_timeout='10s'`, and connect is ~68 ms warm since the Aurora warm floor).
#
# THE RESOURCE BOUND IS `MAX_WAITERS_PER_ID`, NOT THIS TIMER. Queue depth is what caps
# how many connections one duplicated id can tie up; this only stops a wedged holder
# blocking its id forever. Moving `fetch_metric` onto `anyio.to_thread` would remove the
# stall (and the head-of-line blocking it causes generally) and let this come back down.
MAX_WAIT_SECONDS = 30.0

# How many requests may queue behind ONE id, and the real ceiling on connections a single
# duplicated id can tie up.
#
# NOT 3. ContextVM sends FOUR announcement calls sharing an id, which is one holder plus
# three waiters - exactly 3, with zero margin, so a fifth `*/list` method or one retry
# landing inside the burst would be refused. Sized at 5 to leave room for the client this
# was written for to grow slightly without tripping.
MAX_WAITERS_PER_ID = 5

# An id or session id longer than this is not tracked (the request proceeds unguarded).
# Both halves of the registry key are ATTACKER-CONTROLLED and otherwise unbounded - the
# session id only by h11's ~16 KiB header budget, the JSON-RPC id only by the body cap -
# and the key is taken BEFORE the SDK validates the session, so a forged `Mcp-Session-Id`
# about to be 404'd would still mint an entry first. `asgilog.py` carries the same warning
# about the same surface and answers it by clipping at the sink; this sink answers it by
# declining to key on absurd input at all. Real ids are uuid4 hex (32) or a client
# counter; 200 is generous.
MAX_KEY_FIELD_CHARS = 200

# Mirrors the SDK's own `SESSION_ID_PATTERN` (`streamable_http.py:77`, visible ASCII).
# Copied rather than imported so a private-name change upstream cannot break request
# handling here - the cost of drift is that we stop tracking a session the SDK accepts,
# which is the safe direction.
_SESSION_ID_PATTERN = re.compile(r"^[\x21-\x7E]+$")

# Safety valve on the registry itself. Every entry is removed in a `finally`, so this
# should never bind; if a future change ever leaks one, this is what stops a slow leak
# from becoming an unbounded dict on a public endpoint. When it binds we stop TRACKING
# (and therefore stop both waiting and refusing) rather than start rejecting - failing
# open, because a guard that refuses valid traffic under load is worse than the bug it
# prevents.
MAX_TRACKED_IN_FLIGHT = 4096

_JSON_RPC_VERSION = "2.0"


def _header(scope, name: bytes) -> str | None:
    try:
        for key, value in scope.get("headers") or ():
            if key.lower() == name:
                return value.decode("latin-1", "replace")
    except Exception:
        return None
    return None


def _parsed_request(body: bytes) -> tuple[str, str] | None:
    """`(request_id, method)` if this body is a single REQUEST, else None.

    None - meaning "do not track, do not delay, do not refuse" - on everything ambiguous:

      * NOTIFICATIONS (no `id`). They get no response, so they never take a stream and
        cannot collide. Delaying a duplicate notification would be pure false positive.
      * `"id": null`, which JSON-RPC treats as an unidentifiable call.
      * BATCH arrays. MCP removed JSON-RPC batching in 2025-06-18 and this server never
        sees one; a future batch would need per-element tracking, and guessing at it from
        here would be worse than not checking.
      * Anything unparseable. The SDK rejects it with a proper -32700 on its own, and
        pre-empting that from this seam would only move the error somewhere stranger.

    The id is stringified exactly as the SDK does at `streamable_http.py:601`
    (`str(message.id)`), so our key and its key agree on `1` vs `"1"` - two forms
    JSON-RPC permits and which MUST collide here if they collide there.
    """
    try:
        payload = json.loads(body)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    method = payload.get("method")
    if not isinstance(method, str):
        return None
    rid = payload.get("id")
    if rid is None or isinstance(rid, (dict, list, bool)):
        return None
    request_id = str(rid)
    if len(request_id) > MAX_KEY_FIELD_CHARS:
        # Not ours to hold as a dict key. Fail open, like every other doubt in this file.
        return None
    return request_id, method


def _conflict_body(method: str | None) -> bytes:
    """The 409 body: a JSON-RPC error object shaped like the SDK's own rejections.

    `id` is null, NOT the duplicate id, and that is deliberate. A response echoing the id
    would be indistinguishable from the answer to the request still in flight - precisely
    the confusion this whole guard exists to remove. JSON-RPC allows null when the id
    cannot be determined, and here it genuinely cannot: two live requests claim it.
    """
    data = dict(DUPLICATE_REQUEST_ID_HINT)
    if method:
        # Name the call that was refused. The client sent two requests under one id; the
        # only way it can tell WHICH one bounced is if we say so.
        data["method"] = method
    return json.dumps(
        {
            "jsonrpc": _JSON_RPC_VERSION,
            "id": None,
            "error": {
                "code": INVALID_REQUEST,
                "message": "Conflict: JSON-RPC id already in flight on this session",
                "data": data,
            },
        },
        separators=(",", ":"),
    ).encode("utf-8")


class DuplicateRequestIdMiddleware:
    """Raw ASGI middleware serializing concurrent reuse of one JSON-RPC id.

    RAW ASGI, like its two neighbours, and for a sharper reason here: this one has to
    read the REQUEST body before the app does, then hand the app a byte-identical
    stream. Starlette's `BaseHTTPMiddleware` would give us `Request.body()` for free and
    take the SSE stream down with it (see `asgilog.py`); replaying `receive` ourselves is
    the only way to inspect a POST without changing what the app receives.

    Never fails a request it does not understand: every helper swallows its own errors,
    an unparseable or oversized body is forwarded unchecked, and the slot is released in
    a `finally` so a downstream exception cannot strand an id as permanently in-flight.
    """

    def __init__(self, app, *, path: str) -> None:
        self.app = app
        self._path = path
        # (session_id, request_id) -> Event fired when that request finishes.
        #
        # Membership IS the lock: presence means in-flight. A plain dict is safe without
        # a mutex because asyncio is single-threaded and every check-then-insert below
        # has no `await` between the two - the one property that makes this atomic. If
        # you add an await in there, you have introduced a race. Do not.
        self._in_flight: dict[tuple[str, str], anyio.Event] = {}
        # How many requests are parked on each key, so one id cannot tie up connections
        # without bound.
        self._waiters: dict[tuple[str, str], int] = {}

    async def __call__(self, scope, receive, send) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("method") != "POST"
            or not on_mcp_path(scope, self._path)
        ):
            await self.app(scope, receive, send)
            return

        # No session header means the SDK is about to answer 400 "Missing session ID"
        # with `recovery.py`'s initialize hint, which is the more useful error. It also
        # means there is no session to scope an id to: ids are unique only WITHIN a
        # session, so a global registry would make two unrelated clients collide.
        session_id = _header(scope, b"mcp-session-id")
        if not session_id:
            await self.app(scope, receive, send)
            return
        # Forged or absurd session ids never become registry keys. The SDK is about to
        # reject these itself (404 unknown session, or 400 on a malformed one); tracking
        # them first would let an anonymous caller mint dictionary entries on the way to
        # being refused.
        if len(session_id) > MAX_KEY_FIELD_CHARS or not _SESSION_ID_PATTERN.fullmatch(session_id):
            await self.app(scope, receive, send)
            return

        consumed: list[dict] = []
        body, complete = await self._read_body(receive, consumed)

        async def replay():
            # Hand back exactly the messages we took, in order, then get out of the way.
            # Delegating once the buffer drains is what keeps `http.disconnect` working
            # for a client that goes away mid-request.
            if consumed:
                return consumed.pop(0)
            return await receive()

        parsed = _parsed_request(body) if complete else None
        # Drop our joined copy now. CPython keeps a local alive until the frame exits, so
        # without this the body would sit in memory across the whole `await self.app(...)`
        # - on every POST, not just duplicates. `consumed` still holds the raw messages
        # the replay owes downstream; that copy is unavoidable, this one is not.
        del body
        if parsed is None:
            await self.app(scope, replay, send)
            return

        request_id, method = parsed
        key = (session_id, request_id)

        proceed, owns = await self._acquire(key)
        if not proceed:
            await self._refuse(method, send)
            return
        try:
            await self.app(scope, replay, send)
        finally:
            # ONLY the request that actually took the slot may release it. The fail-open
            # path below proceeds WITHOUT registering, and if it released anyway it would
            # pop an entry another request legitimately holds and wake that request's
            # waiters early - manufacturing the exact collision this middleware exists to
            # prevent, and only ever under the load where the valve fires.
            if owns:
                self._release(key)

    async def _acquire(self, key) -> tuple[bool, bool]:
        """Take the slot for `key`, waiting for the current holder if there is one.

        Returns `(proceed, owns)`:
          * `(True, True)`  - this request owns the id and MUST release it.
          * `(True, False)` - proceed untracked (the safety valve fired); release nothing.
          * `(False, False)` - refuse: the queue for this id is full, or the holder did
            not finish inside `MAX_WAIT_SECONDS`.

        Loops rather than assuming the slot is ours after the wake-up, because several
        waiters can be released by the same event and only one of them may take it. Each
        pass re-checks under the same no-await-in-between rule as the first.
        """
        if len(self._in_flight) >= MAX_TRACKED_IN_FLIGHT:
            # Fail open - see MAX_TRACKED_IN_FLIGHT. Untracked means unserialized, i.e.
            # exactly today's behaviour, which is the safe direction for a valve that
            # should never fire.
            return True, False

        deadline = anyio.current_time() + MAX_WAIT_SECONDS
        while True:
            holder = self._in_flight.get(key)
            if holder is None:
                self._in_flight[key] = anyio.Event()
                return True, True
            if self._waiters.get(key, 0) >= MAX_WAITERS_PER_ID:
                return False, False
            remaining = deadline - anyio.current_time()
            if remaining <= 0:
                return False, False
            self._waiters[key] = self._waiters.get(key, 0) + 1
            try:
                with anyio.move_on_after(remaining):
                    await holder.wait()
            finally:
                remaining_waiters = self._waiters.get(key, 1) - 1
                if remaining_waiters > 0:
                    self._waiters[key] = remaining_waiters
                else:
                    self._waiters.pop(key, None)

    def _release(self, key) -> None:
        """Drop the slot and wake everyone parked on it.

        `pop` before `set`: a waiter woken while the key is still present would loop,
        find the holder, and park again on an event that has already fired - a spin, not
        a wait. Popping first means the first waiter to run finds the slot free.
        """
        holder = self._in_flight.pop(key, None)
        if holder is not None:
            holder.set()

    async def _read_body(self, receive, consumed: list[dict]) -> tuple[bytes, bool]:
        """Buffer the request body, recording every message so it can be replayed.

        Returns `(body, complete)`. `complete` is False when the client disconnected
        mid-body or the body ran past `MAX_INSPECT_BYTES` - both forward unchecked,
        because a partial body cannot be parsed and a huge one is not ours to hold.
        """
        chunks: list[bytes] = []
        total = 0
        try:
            while True:
                message = await receive()
                consumed.append(message)
                if message.get("type") != "http.request":
                    # `http.disconnect` - nothing more is coming.
                    return b"".join(chunks), False
                chunk = message.get("body") or b""
                chunks.append(chunk)
                total += len(chunk)
                if total > MAX_INSPECT_BYTES:
                    return b"".join(chunks), False
                if not message.get("more_body"):
                    return b"".join(chunks), True
        except Exception:
            return b"".join(chunks), False

    async def _refuse(self, method: str | None, send) -> None:
        """Answer 409 without invoking the app at all.

        Not calling downstream is the entire point: the corruption happens the moment the
        session manager writes `_request_streams[id]`, so the only safe place to stop it
        is before that.
        """
        payload = _conflict_body(method)
        await send(
            {
                "type": "http.response.start",
                "status": 409,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(payload)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload, "more_body": False})
