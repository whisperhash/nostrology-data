"""Observation seam #2: one call-log record per inbound HTTP request.

NAMING: "the SDK" in this file always means the PYTHON MCP SDK (`mcp` on PyPI), the
one serving this endpoint. The incident described below also involves ContextVM,
which has its own SDK (`@contextvm/sdk`) and depends on the TypeScript MCP SDK
(`@modelcontextprotocol/sdk`) - both are named in full wherever they appear.

WHY THIS EXISTS - the third blind spot. `middleware.py` (a `ServerMiddleware`)
closed two of them, but by construction it can only see a request that has already
become a JSON-RPC message on a valid session. Requests the SDK's
`StreamableHTTPSessionManager` answers *itself*, above that layer, produce no
record at all:

  * `400 Bad Request: Missing session ID` - a POST with no `Mcp-Session-Id`. The
    traffic of 2026-08-03 showed a live ContextVM gateway sending exactly
    this every 1-3 hours for eight hours straight, 100% failing, and the ONLY
    evidence anywhere in our stack was Caddy's access log. A returning consumer
    we could have fixed was invisible to the system built to see consumers.
  * `404` on an expired/unknown session id, `409` on a second concurrent SSE
    stream, `406` on a bad `Accept`, `421` on host validation.

None of those are exotic: they are what a slightly-wrong client does, and
"slightly-wrong client" is the single most valuable thing an unauthenticated
public endpoint can learn about itself.

RAW ASGI, NOT `BaseHTTPMiddleware`. Starlette's `BaseHTTPMiddleware` wraps the
downstream app in an anyio task group and buffers the response, which breaks
long-lived streaming responses - and `GET /mcp` IS one (the SSE channel). A raw
ASGI middleware passes `send` through untouched, so the stream is byte-for-byte
what it was without us. This is not a style preference; the other choice silently
degrades the transport.

SCOPED TO THE MCP PATH. Deliberately not "every request": `/health` is polled by
the container healthcheck and would write thousands of records a day that answer
no question. Caddy only proxies `/mcp` and `/mcp/*` to this container from the
public path anyway, so the MCP path is exactly the public surface. `/registry.json`
is fetched container-to-container by the web app and is equally uninteresting here.

RECORDS EVERY REQUEST, not only failures. The failures are the point, but a
failure count without its denominator cannot answer "what fraction of arrivals
bounce", and the whole reason this blind spot mattered was needing a Caddy join to
answer basic questions. One log, self-sufficient.
"""

import time
from urllib.parse import parse_qs

from .calllog import write_http_record
from .middleware import ALLOWED_TRAFFIC_TYPES, TRAFFIC_TYPE_PARAM

# NO local ceilings here, deliberately - but note WHERE the ceiling actually is.
# `calllog.write_http_record` clips EVERY caller-controlled field on the way to
# both sinks: the `client` block via `_clip_client`, and the top-level `path` and
# `method` explicitly. Duplicating those caps here would be a second number to
# keep in step forever, and the two would eventually disagree - which is why the
# clip lives at the sink. That matters more than usual at this seam: it runs
# before any validation at all, on an unauthenticated endpoint that is ALSO
# exempt from Caddy's bot filter, so the User-Agent and the request path are both
# entirely attacker-chosen. `path` is the nastier of the two:
# `_matches` accepts anything under `/mcp/` and h11 allows a request target of
# ~16 KiB, so an unclipped path would be a 16 KB record per request onto the obs
# volume AND through stdout into Loki - the exact amplification that
# MAX_CLIENT_FIELD_CHARS exists to stop, on the one surface with no rate limit.


def on_mcp_path(scope, path: str) -> bool:
    """The MCP endpoint and anything mounted under it - mirroring Caddy's
    `@mcp path /mcp /mcp/*`, so every layer agrees on what the public surface is.

    Public and shared: `recovery.py`'s seam scopes itself the same way and imports
    this rather than re-declaring it, for the same reason `_traffic_type` below
    imports its allowlist from `middleware.py`. Two ASGI seams that disagreed about
    which requests are "the MCP endpoint" would be a bug nobody could see from
    either file alone.
    """
    raw = scope.get("path") or ""
    return raw == path or raw.startswith(path + "/")


def _header(scope, name: bytes) -> str | None:
    """First value of a header from the raw ASGI scope, decoded leniently.

    Lenient decode, not strict: a client sending invalid UTF-8 in a header must
    cost us the field's fidelity, never the record.
    """
    try:
        for key, value in scope.get("headers") or ():
            if key.lower() == name:
                return value.decode("latin-1", "replace")
    except Exception:
        return None
    return None


def _traffic_type(scope) -> str | None:
    """The first-party marking off the raw query string.

    Parsed from `scope` rather than a Starlette `Request` because this middleware
    runs on the raw ASGI interface. Same param and same allowlist as the JSON-RPC
    seam - imported, not re-declared - and percent-DECODED like Starlette's
    `query_params`, so `?traffic%5Ftype=internal` is read the same way by both.
    Hand-splitting on `&`/`=` without decoding was a real divergence: the other
    seam would have called that request marked and this one would not.
    """
    try:
        raw = scope.get("query_string") or b""
        values = parse_qs(raw.decode("latin-1", "replace"), keep_blank_values=True)
        for value in values.get(TRAFFIC_TYPE_PARAM, ()):
            if value in ALLOWED_TRAFFIC_TYPES:
                return value
    except Exception:
        return None
    return None


class HttpCallLogMiddleware:
    """Raw ASGI middleware writing one `kind:"http"` record per MCP request.

    Never fails the request: the record is written in a `finally`, and every
    field-extraction helper swallows its own errors. An exception escaping here
    would take down a connection to buy a log line, which is never the trade.
    """

    def __init__(self, app, *, path: str, server_version: str, git_sha: str) -> None:
        self.app = app
        self._path = path
        self._version = server_version
        self._git_sha = git_sha

    def _matches(self, scope) -> bool:
        return on_mcp_path(scope, self._path)

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or not self._matches(scope):
            await self.app(scope, receive, send)
            return

        t0 = time.monotonic()
        status: int | None = None
        # The session id the SERVER assigned, lifted off the response headers.
        # On `initialize` the request carries no session id (the server mints it
        # here), so without this the one record that opens a session would be the
        # one record that cannot be joined to the rest of it.
        #
        # ONLY USED ON A SUCCESSFUL RESPONSE - see _write. The SDK mints a fresh
        # transport for a session-less POST and stamps its id on the 400 too, so
        # taking it unconditionally would give every request in a failure storm a
        # distinct session id that no client ever held: "distinct sessions that
        # bounced" would count the requests, and joins to rpc/tool records would
        # match nothing.
        response_session_id: str | None = None

        async def send_wrapper(message) -> None:
            nonlocal status, response_session_id
            try:
                if message.get("type") == "http.response.start":
                    status = message.get("status")
                    for key, value in message.get("headers") or ():
                        if key.lower() == b"mcp-session-id":
                            response_session_id = value.decode("latin-1", "replace")
                            break
            except Exception:
                pass
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            # `finally`, not the happy path: an SSE stream that ends because the
            # peer went away unwinds through here (uvicorn signals the disconnect
            # to the app rather than cancelling the task, but either way the
            # response never completes normally), and a stream that died is
            # precisely the event worth having on record. The body below is pure
            # sync with no checkpoints, so it cannot be interrupted part-written,
            # and `except Exception` cannot swallow a CancelledError.
            #
            # TIMING, for anyone joining on `ts`: a long-lived GET is recorded
            # when the stream CLOSES, so the day-file is in completion order and
            # `ts` is the completion time, not the arrival time. A process kill
            # loses the record for a stream that was still open.
            self._write(scope, status, response_session_id, t0)

    def _write(self, scope, status, response_session_id, t0) -> None:
        try:
            request_session_id = _header(scope, b"mcp-session-id") or None
            client = {}
            # Request id first: it is what the CLIENT believed its session was,
            # which is the diagnostic. The response id is the fallback that makes
            # `initialize` joinable - and it is trusted ONLY on a 2xx/3xx, because
            # on a rejection it is an id the SDK minted and immediately threw away.
            minted = response_session_id if (status is not None and status < 400) else None
            session_id = request_session_id or minted
            if session_id:
                client["session_id"] = session_id
            ua = _header(scope, b"user-agent")
            if ua:
                client["user_agent"] = ua
            marking = _traffic_type(scope)
            if marking:
                client["traffic_type"] = marking
            write_http_record(
                method=scope.get("method"),
                path=scope.get("path"),
                status=status,
                # The distinction the 400s turned on: did the caller send a
                # session header at all? `false` here with a 400 IS the
                # never-initialized-transport signature, and it is not
                # recoverable from `session_id` once the response fallback fills
                # that field in.
                had_session_header=request_session_id is not None,
                ms={"total": int((time.monotonic() - t0) * 1000)},
                server_version=self._version,
                git_sha=self._git_sha,
                client=client or None,
            )
        except Exception:
            # calllog._emit already reports its own failures to stderr; anything
            # that escapes it is still not worth a failed request.
            pass
