"""Make the transport's own rejections actionable: attach `error.data` telling the
client HOW to recover.

NAMING: "the Python MCP SDK" below is `mcp` on PyPI, the one serving this endpoint.
ContextVM (mentioned as the motivating case) has its own `@contextvm/sdk` and depends
on the TypeScript MCP SDK (`@modelcontextprotocol/sdk`); all three are named in full.

WHY THIS EXISTS. The traffic of 2026-08-03 showed a live third-party gateway
POSTing `tools/list` with no `Mcp-Session-Id` every 1-3 hours for eight hours, 100%
failing, because its transport was never initialized. What we sent back was the
Python MCP SDK's stock body:

    {"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Bad Request: Missing session ID"}}

That is correct, and it is nearly useless to whoever is reading their logs: it names
the symptom, not the fix, and nothing in it is machine-readable beyond the code. The
operator on the other end IS looking at those logs. So we add the one field JSON-RPC
reserves for exactly this - `error.data`, "a primitive or structured value that
contains additional information about the error" - carrying a stable `recovery` token
plus the prose that says what call to make next.

COMPLIANT, deliberately. JSON-RPC 2.0 leaves `data` entirely to the server. The MCP
spec's Streamable HTTP section says the body of these responses MAY comprise a
JSON-RPC error response and constrains its contents no further. We change no status
code, no `code`, and no `message` - only ADD `data`, and only when it is absent. A
client that ignores `data` sees byte-identical semantics to before.

WHAT IT CANNOT DO. This does not fix the client that motivated it: that gateway never
reads the error body. This is insurance for the NEXT consumer, and a much better
artifact for a human debugging an integration against us. Said plainly rather than
overclaimed, because "we made the error nicer" is not "we fixed the integration".

KEYED ON (status, JSON-RPC code), NOT ON THE MESSAGE STRING. The Python MCP SDK's
English wording is not API - it changes between versions and would silently stop
matching. The pair is precise here because the codes disambiguate the two families of
400 that the SDK emits from the same handler (see `RECOVERY_HINTS`).

SEPARATE FROM `asgilog.py` ON PURPOSE. That seam OBSERVES and passes `send` through
untouched, which is what lets it sit safely across the long-lived SSE stream. This one
MODIFIES a response, which means buffering it. Keeping them in one class would put a
body-buffering code path on the streaming seam, and the first mistake there breaks
`GET /mcp` for everyone. Two objects, one property each.

THE OUTBOUND COST, ACCEPTED. A rejection grows from ~95 to ~772 bytes, about 8x, on the
cheapest possible failing request, and a scanner gets the enriched body too - nothing at
the edge filters by user agent (the `@badbots` deny-list this line used to cite as the
thing `@mcp` had to precede was deleted 2026-09-22 when the site opened to crawlers).
This is an egress-and-log-volume cost, not an amplification attack (TCP/TLS, no
spoofable source, and the response is bounded and constant). 677 extra bytes per
rejection against a client population measured in tens of requests a day is worth a
third party being able to fix their integration. Named here
rather than left unmade, because this is the one surface with no auth and no rate limit;
if the rejection rate ever becomes a real volume, trim `detail` before anything else.
"""

import json

from .asgilog import on_mcp_path

# JSON-RPC 2.0 error codes, as the Python MCP SDK passes them to
# `_create_error_response`. INVALID_REQUEST is its DEFAULT, which is why every hint
# below carries it; the parse (-32700) and validation (-32602) 400s take explicit
# codes and are therefore excluded by construction, not by hope. That is the whole
# reason the key is a pair: "400" alone would answer "run initialize" to a client
# whose real problem is that it sent malformed JSON on a perfectly good session.
#
# Public because `inflight.py` authors a rejection of its own and must use the same
# code: two seams answering the same protocol family with different codes would make
# `(status, code)` stop being a reliable key for whoever is switching on it.
INVALID_REQUEST = -32600
_INVALID_REQUEST = INVALID_REQUEST

# One-hop pointers for whoever is reading this in a log. `docs` also answers "who is
# telling me this?" - `data` is our addition, not the SDK's, and a client author
# hitting it for the first time deserves to know where it came from.
_DOCS = "https://nostrolo.gy/data"
# Pinned to the revision this endpoint was audited against, NOT to whatever the SDK's
# LATEST_PROTOCOL_VERSION happens
# to be - a link we ship to third parties should point at the text our behaviour was
# actually checked against. Session semantics are unchanged in later revisions; revisit
# this string if that ever stops being true.
_SPEC_SESSION = "https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#session-management"
# Base protocol, "Messages" -> "Requests", where the id rules live. Quoted verbatim in
# `DUPLICATE_REQUEST_ID_HINT` below and checked against the published text, because a
# hint that misquotes the spec at a third party is worse than no hint.
_SPEC_REQUESTS = "https://modelcontextprotocol.io/specification/2025-06-18/basic#requests"

# (HTTP status, JSON-RPC code) -> the `data` object we attach.
#
# `recovery` is the stable, machine-readable part: a client may switch on it and
# MUST be able to keep doing so, so these tokens are an API - add, never rename.
# `detail` is prose for a human reading a log, and is free to improve.
RECOVERY_HINTS = {
    # "Bad Request: Missing session ID" - the ContextVM signature. The last sentence
    # is the actual bug we diagnosed on their side (a transport re-created per call,
    # so `initialize` never ran), stated as a general rule rather than an accusation.
    (400, _INVALID_REQUEST): {
        "recovery": "initialize",
        "detail": (
            "This server is stateful: it assigns a session during `initialize` and "
            "requires the `Mcp-Session-Id` header it returned on every request after "
            "that. This request carried none. To recover: POST an `initialize` request "
            "with no session header, read the `Mcp-Session-Id` RESPONSE header, POST "
            "the `notifications/initialized` notification with that header set, then "
            "retry this request with it. A transport that is created per call will "
            "fail this way forever - initialize once and reuse the session."
        ),
        "spec": _SPEC_SESSION,
        "docs": _DOCS,
    },
    # "Not Found: Invalid or expired session ID" and "Not Found: Session has been
    # terminated" - same fix, different cause, so the same token with its own prose.
    (404, _INVALID_REQUEST): {
        "recovery": "initialize",
        "detail": (
            "The `Mcp-Session-Id` sent is not a session this server currently holds - "
            "it was terminated, or the server restarted. Sessions do not survive a "
            "restart and are not resumable here. Discard the id and run the "
            "`initialize` handshake again to obtain a new one."
        ),
        "spec": _SPEC_SESSION,
        "docs": _DOCS,
    },
    # "Not Acceptable: Client must accept application/json" (this server runs
    # json_response=True) / "...both application/json and text/event-stream".
    (406, _INVALID_REQUEST): {
        "recovery": "accept",
        "detail": (
            "Send `Accept: application/json, text/event-stream` on POSTs and "
            "`Accept: text/event-stream` on the GET stream. This server answers POSTs "
            "with plain JSON, but the spec requires clients to accept both."
        ),
        "spec": _SPEC_SESSION,
        "docs": _DOCS,
    },
    # "Conflict: Only one SSE stream is allowed per session".
    #
    # DISAMBIGUATE BY METHOD, not by this key. Since `inflight.py` shipped, `(409,
    # -32600)` has TWO meanings on the wire: this one is the SDK's, raised only from
    # `_handle_get_request` (so it is always a GET), while a POST 409 is the duplicate-id
    # refusal. The pair alone no longer identifies the failure. Switch on the `recovery`
    # token instead - `single_sse_stream` vs `unique_request_id` - which is exactly what
    # that field is for and why it is the part promised as API.
    (409, _INVALID_REQUEST): {
        "recovery": "single_sse_stream",
        "detail": (
            "This session already has an open `GET` SSE stream, and only one is "
            "allowed per session. Close the existing stream before opening another, "
            "or reuse it - note that nothing this server exposes needs the GET stream "
            "at all, since every response comes back on its own POST."
        ),
        "spec": _SPEC_SESSION,
        "docs": _DOCS,
    },
}

# Statuses worth buffering at all, derived from the table so the two cannot drift.
_CANDIDATE_STATUSES = frozenset(status for status, _ in RECOVERY_HINTS)

# DELIBERATELY NOT IN `RECOVERY_HINTS`. Everything in that table describes a response the
# Python MCP SDK produced and we are decorating on the way out. This one describes a
# response WE author end to end (`inflight.py` answers 409 without ever calling the app),
# so there is nothing to match and nothing to rewrite. Keeping it out of the table also
# keeps the table's key unambiguous: `(409, INVALID_REQUEST)` already means the SDK's
# "only one SSE stream per session", which is a GET-side error with a completely
# different fix. Two meanings behind one key is how a hint starts telling people the
# wrong thing.
#
# It lives in THIS file anyway so that every string we hand a third party is in one
# place - the tokens are an API (`recovery` may be switched on; add, never rename) and
# they need to stay consistent in tone and vocabulary with their four neighbours.
DUPLICATE_REQUEST_ID_HINT = {
    "recovery": "unique_request_id",
    "detail": (
        "Another request with this same JSON-RPC `id` is still in flight on this "
        "session, so this one was refused rather than served. The MCP base protocol "
        "requires that 'the request ID MUST NOT have been previously used by the "
        "requestor within the same session'; JSON-RPC needs the id to decide which "
        "response answers which call, and two live requests sharing one id makes that "
        "undecidable. To recover: give every request a fresh id - a counter or a uuid "
        "per call is enough - and retry. Note this server refuses ONLY ids that overlap "
        "in time; reusing an id after its response has arrived is accepted."
    ),
    "spec": _SPEC_REQUESTS,
    "docs": _DOCS,
}

# Hard ceiling on what we will hold in memory to rewrite. Every response in the table
# is a couple of hundred bytes; anything larger is not one of ours and gets forwarded
# untouched. Without this, a future error path that streamed a large JSON body would
# be silently buffered whole, on an endpoint with no auth and no rate limit.
MAX_REWRITE_BYTES = 64 * 1024

_JSON_CONTENT_TYPE = b"application/json"


def _header(headers, name: bytes) -> bytes | None:
    for key, value in headers or ():
        if key.lower() == name:
            return value
    return None


def _is_candidate(message) -> bool:
    """Worth withholding? Status in the table, a JSON body, and small enough.

    Checked at `http.response.start`, where the JSON-RPC code is not yet known - so
    this is the cheap prefilter and `_rewrite` makes the real decision. Anything that
    is not a candidate is forwarded immediately and never buffered, which is what
    keeps this middleware off the streaming path: the SSE stream is a 200.
    """
    try:
        if message.get("status") not in _CANDIDATE_STATUSES:
            return False
        headers = message.get("headers") or ()
        content_type = _header(headers, b"content-type") or b""
        if not content_type.lower().startswith(_JSON_CONTENT_TYPE):
            return False
        declared = _header(headers, b"content-length")
        if declared is not None and int(declared) > MAX_REWRITE_BYTES:
            return False
        return True
    except Exception:
        return False


def _rewrite(status: int, body: bytes) -> bytes | None:
    """The rewritten body, or None to leave the response exactly as it was.

    None on ANY doubt - unparseable body, not a JSON-RPC error object, no hint for
    this (status, code), or a `data` the SDK already set. Never overwriting an
    existing `data` matters: if a future SDK version starts populating it, its value
    is the authoritative one and ours would be a downgrade dressed as an improvement.
    """
    try:
        payload = json.loads(body)
        error = payload.get("error")
        if not isinstance(error, dict) or "data" in error:
            return None
        hint = RECOVERY_HINTS.get((status, error.get("code")))
        if hint is None:
            return None
        error["data"] = hint
        # separators: match what the SDK's pydantic `model_dump_json` emits, so the
        # body stays compact. One difference beyond the added field, in theory:
        # `json.dumps` defaults to `ensure_ascii=True` where pydantic emits raw
        # UTF-8, so a non-ASCII `message` would come back \uXXXX-escaped. All four
        # messages we rewrite are fixed ASCII English, and an escaped string is
        # still the same string to any JSON parser.
        return json.dumps(payload, separators=(",", ":")).encode("utf-8")
    except Exception:
        return None


def _with_content_length(headers, length: int):
    """Original headers with `content-length` replaced. Rebuilt rather than mutated:
    the message is the SDK's object, and h11 will happily send a body that disagrees
    with a stale content-length, producing a response the client cannot frame."""
    kept = [(k, v) for k, v in (headers or ()) if k.lower() != b"content-length"]
    kept.append((b"content-length", str(length).encode("ascii")))
    return kept


class RecoveryHintMiddleware:
    """Raw ASGI middleware adding `error.data` to the transport's own rejections.

    Withholds `http.response.start` only for a small JSON response whose status is in
    `RECOVERY_HINTS`, because the rewrite changes `content-length` and the header
    frame therefore cannot go out before the body is known. Everything else - notably
    the `GET /mcp` SSE stream - is forwarded the instant it arrives, unbuffered.

    Never fails the request: every decision helper swallows its own errors and falls
    back to forwarding the original bytes.
    """

    def __init__(self, app, *, path: str) -> None:
        self.app = app
        self._path = path

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or not on_mcp_path(scope, self._path):
            await self.app(scope, receive, send)
            return

        state = {"held": None, "buffered": bytearray(), "passthrough": False}

        async def release_unmodified(more_body: bool) -> None:
            """Give up on rewriting and emit exactly what the app produced."""
            start, state["held"] = state["held"], None
            state["passthrough"] = True
            await send(start)
            body = bytes(state["buffered"])
            state["buffered"] = bytearray()
            await send({"type": "http.response.body", "body": body, "more_body": more_body})

        async def send_wrapper(message) -> None:
            if state["passthrough"]:
                await send(message)
                return

            if message.get("type") == "http.response.start":
                if state["held"] is not None:
                    # A second start while one is held is an ASGI protocol
                    # violation. Overwriting `held` would silently concatenate two
                    # JSON bodies into one unparseable response served under the
                    # second status - turning a loud, detectable bug into a quiet
                    # corruption. Release what we have, then forward the offending
                    # start so uvicorn raises `Unexpected ASGI message` exactly as
                    # it would without this middleware. Not reachable through the
                    # Python MCP SDK today (every error site returns immediately);
                    # this keeps it unreachable-and-loud rather than
                    # unreachable-and-silent.
                    await release_unmodified(more_body=False)
                    await send(message)
                    return
                if _is_candidate(message):
                    state["held"] = message
                    return
                state["passthrough"] = True
                await send(message)
                return

            # `http.response.trailers` while a start is held would go out BEFORE the
            # start here. Unreachable - the extension requires `trailers: True` in
            # the start message, and neither Starlette's `Response` nor the Python
            # MCP SDK ever sets it - so it is not worth a guard, only a note.
            if message.get("type") != "http.response.body" or state["held"] is None:
                await send(message)
                return

            state["buffered"].extend(message.get("body") or b"")
            if len(state["buffered"]) > MAX_REWRITE_BYTES:
                # Bigger than anything in the table; stop pretending we know what it
                # is and hand the rest through as it comes.
                await release_unmodified(more_body=bool(message.get("more_body")))
                return
            if message.get("more_body"):
                return

            start = state["held"]
            rewritten = _rewrite(start.get("status"), bytes(state["buffered"]))
            if rewritten is None:
                await release_unmodified(more_body=False)
                return
            state["held"] = None
            state["passthrough"] = True
            await send({**start, "headers": _with_content_length(start.get("headers"), len(rewritten))})
            await send({"type": "http.response.body", "body": rewritten, "more_body": False})

        await self.app(scope, receive, send_wrapper)

        # Reached only on a NORMAL return from the app. If it withheld a start and
        # then never sent a body, releasing it here is what stops a held response from
        # becoming a hung request. Deliberately not in a `finally`: when the app
        # raises or is cancelled, the response was never going to be delivered anyway,
        # and awaiting `send` inside a cancelled scope would only replace the real
        # exception with a confusing one.
        if state["held"] is not None:
            await release_unmodified(more_body=False)
