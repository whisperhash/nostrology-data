"""Observation middleware: one call-log record per inbound JSON-RPC message.

WHY THIS EXISTS. The tool body's own record (calllog.write_record) only fires for
calls that reach it, which left two blind spots that the traffic of the
first 43 hours of the public endpoint ran straight into:

  * DISCOVERY IS INVISIBLE. `initialize`, `tools/list`, `resources/list` and
    `resources/read` produced no record at all - yet 18 of the first 38 sessions
    listed the tools and left without calling one, and the nostrology://registry
    resource was being read by clients we could not identify. The funnel that says
    whether the tool description is doing its job could only be reconstructed by
    fingerprinting Caddy response SIZES (2405 bytes = tools/list, ...), which
    breaks the moment the catalog changes.
  * REJECTED CALLS ARE INVISIBLE. Params validation happens above the tool body,
    so an argument the schema refuses never reaches our code; and because a
    JSON-RPC error is still HTTP 200, Caddy cannot distinguish it from a success
    either. "What did a caller ask for that we don't offer" is the single most
    valuable signal for steering what we expose, and it was landing nowhere.

`ServerMiddleware` (SDK 2.x) is the correct seam for both: it wraps every inbound
request AND notification, runs BEFORE validation/lookup/handshake, and receives a
handler-side failure as a raised exception so it can record it. See
mcp.server.context.ServerMiddleware.

DELIBERATELY SYNCHRONOUS. `_emit` does a blocking <1 KB O_APPEND on the event
loop rather than hopping to a worker thread. Two reasons: ordering (records must
land in call order, and thread-hopping makes the interleaving nondeterministic),
and the existing stdout sink is already a blocking `print` on this loop. The cost
is bounded by local-disk append latency on a log that sees single-digit records
per session.

INITIALIZE IS OBSERVED, NOT REWRITTEN, and nothing here awaits the peer: the
dispatcher handles `initialize` inline with the read loop parked, so awaiting a
server-to-client request from inside this chain would deadlock the connection.
Logging only touches the filesystem, so that hazard does not arise - but keep it
that way.
"""

import time

from .calllog import write_rpc_record

# Ceiling on the error text lifted off an is_error result. A pydantic validation
# dump is multi-line and can run long; the first few hundred characters carry the
# failing field and the reason, which is the whole point of recording it.
MAX_ERROR_CHARS = 500

# First-party traffic marking. Our own clients register the endpoint with
# `?traffic_type=internal` on the URL; every record from that session then carries
# `client.traffic_type`, and absence means "not marked".
#
# WHY A QUERY PARAM, on the SAME url everything else uses. The problem it solves:
# all of the endpoint's first 35 calls were our own, and 23 of them carried the
# client name any user of that client sends, so first-party traffic could not be
# separated from demand. The Caddy operator-marker cookie cannot reach an MCP
# client. A query param does, because in MCP the CLIENT CONFIG IS THE PERSISTENCE: the URL lives in
# the client's config file and the client replays it verbatim on every request
# for the life of the registration - the role a cookie has to play on the web. Verified against the real client SDK: it survives
# on initialize, every subsequent message, and the terminating DELETE. It also lands
# in Caddy's access log for free, since the query string is part of `request.uri`.
#
# WHY THIS NAME. `traffic_type=internal` is GA4's own convention for the identical
# job (its internal-traffic filter keys off exactly this parameter and value), and
# GA4 already runs on the web side of this stack - so the vocabulary is one an
# operator here has already met. Rejected: `nozzl_op` (the web's operator marker
# cookie, `__Host-nozzl_op`, uses that name for something else - one name, two meanings),
# `origin`/`client`/`src` (collide with the HTTP Origin header, the `client` block in
# these very records, and vagueness respectively).
#
# IT IS A LABEL, NOT A CREDENTIAL - nothing is gated on it and anyone may send it.
# That is fine: this is analytics, not access control. The values are ALLOWLISTED so
# an unauthenticated caller cannot inject free text into a log that ships to Loki.
#
# ONE first-party class:
#   * internal - our own traffic: verification curls, smoke tests, post-deploy checks,
#                manual probes.
# Third-party demand analysis = records with NO traffic_type (absence means
# "unmarked", never "external" - see the corollary in the workspace conventions).
TRAFFIC_TYPE_PARAM = "traffic_type"
ALLOWED_TRAFFIC_TYPES = frozenset({"internal"})


def getattr_safe(obj, name: str, default):
    """`getattr` that also survives a PROPERTY raising, which the builtin does not:
    `getattr(o, "x", d)` still propagates an exception raised inside `x`'s getter."""
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def traffic_type_of(request) -> str | None:
    """The marking on this request, or None if absent/unrecognized.

    Best-effort like every other attribution here: stdio has no request at all, and
    an unparseable query must cost the label, never the call."""
    try:
        if request is None:
            return None
        value = request.query_params.get(TRAFFIC_TYPE_PARAM)
    except Exception:
        return None
    return value if value in ALLOWED_TRAFFIC_TYPES else None


def _result_error(result) -> str | None:
    """The reason an `is_error` result failed, or None if it did not fail.

    THIS IS NOT AN EDGE CASE - it is most of the point. The SDK does NOT raise
    for a failed tool call: params validation failures, an unknown tool name and
    an exception from the tool body all come back through `call_next` as an
    ORDINARY result carrying `isError: true`. Without this, every one of them
    logs `outcome: "ok"` with no reason, and the rejected-call blind spot this
    middleware exists to close stays open for exactly the cases that matter most
    (verified empirically against mcp 2.0.0 - see test_middleware.py).

    In this configuration the result is always a wire dict (the runner serializes
    before the chain unwinds), so the camelCase `isError` branch is the live one;
    the model branch is defensive, for a future where a handler result reaches us
    unserialized.

    Matched against literal `True`, not truthiness: `{"isError": "false"}` and
    `{"isError": 0}` are the values a sloppy client sends, and coercing them would
    invent failures in the demand analysis this log exists to feed. The SDK's own
    OpenTelemetry middleware makes the same choice for the same reason.
    """
    try:
        if isinstance(result, dict):
            failed = result.get("isError", result.get("is_error"))
            content = result.get("content")
        else:
            failed = getattr(result, "is_error", None)
            if failed is None:
                failed = getattr(result, "isError", None)
            content = getattr(result, "content", None)
        if failed is not True:
            return None
    except Exception:
        return None
    # Separate try, deliberately: once the result is KNOWN to be a failure, an
    # unreadable content list must cost us only the TEXT. Folding this into the
    # block above would let a raising `content[0]` return None, silently
    # downgrading a real failure to `outcome: "ok"` - the exact bug this
    # function exists to prevent.
    text = ""
    try:
        if content:
            first = content[0]
            text = first.get("text", "") if isinstance(first, dict) else (getattr(first, "text", "") or "")
    except Exception:
        text = ""
    return str(text)[:MAX_ERROR_CHARS] or "is_error result with no text content"


def _session_id(ctx) -> str | None:
    """Best-effort session id, tried in two places because neither alone covers
    every message:

      * the `mcp-session-id` REQUEST header - public and stable, but the client
        does not have one yet on `initialize` (the server mints it and returns it
        in the RESPONSE), which is exactly the message carrying clientInfo;
      * `ctx.session._connection.session_id` - private SDK attribute, but already
        populated when `initialize` dispatches, so it is what joins the handshake
        record to the rest of the session.

    The private access is pinned by a test so an SDK bump fails loudly instead of
    silently dropping attribution. Both are inside try/except: this is a logging
    nicety and must never fail a call.
    """
    try:
        request = ctx.request
        if request is not None:
            sid = request.headers.get("mcp-session-id")
            if sid:
                return sid
    except Exception:
        pass
    try:
        return ctx.session._connection.session_id
    except Exception:
        return None


def _client_name_version(ctx) -> tuple[str | None, str | None]:
    """Client identity from the handshake. On every post-handshake message the
    SDK has it parsed on the session; on `initialize` itself it is not committed
    yet, so read it off the raw wire params instead - otherwise the one record
    that names the client is the one record that lacks it.

    NB the wire is camelCase (`clientInfo`) here, unlike the SDK's snake_case
    `client_params.client_info` - these are the two different sides of the same
    field, not a typo.
    """
    try:
        params = ctx.session.client_params
        if params is not None and params.client_info is not None:
            return params.client_info.name, params.client_info.version
    except Exception:
        pass
    try:
        info = (ctx.params or {}).get("clientInfo")
        if isinstance(info, dict):
            return info.get("name"), info.get("version")
    except Exception:
        pass
    return None, None


def _client_block(ctx) -> dict | None:
    info: dict = {}
    sid = _session_id(ctx)
    if sid:
        info["session_id"] = sid
    name, version = _client_name_version(ctx)
    if name:
        info["name"] = name
    if version:
        info["version"] = version
    try:
        marking = traffic_type_of(ctx.request)
    except Exception:
        marking = None
    if marking:
        info["traffic_type"] = marking
    return info or None


class CallLogMiddleware:
    """`ServerMiddleware`: log every inbound message, then get out of the way.

    Never swallows: the original exception is re-raised unchanged after the
    record is written, so error semantics for the caller are exactly what they
    were before this middleware existed.
    """

    def __init__(self, server_version: str, git_sha: str) -> None:
        self._version = server_version
        self._git_sha = git_sha

    async def __call__(self, ctx, call_next):
        t0 = time.monotonic()
        # Snapshot method/params BEFORE call_next: a downstream middleware may
        # rewrite the context, and what we want on record is what the client sent.
        #
        # Guarded, even though ServerRequestContext is a plain dataclass whose fields
        # cannot raise today: this middleware wraps EVERY inbound message, so an
        # exception escaping HERE would take down the connection rather than one
        # call - the exact opposite of the never-fail-the-call guarantee the rest of
        # this module is built on. Cheap insurance at the one point where the blast
        # radius is the whole server.
        try:
            method, params, request_id = ctx.method, ctx.params, ctx.request_id
        except Exception:
            method = getattr_safe(ctx, "method", "<unreadable>")
            params, request_id = None, None
        try:
            result = await call_next(ctx)
        except Exception as exc:
            self._write(
                method,
                params,
                request_id,
                ctx,
                t0,
                outcome="error",
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        # A handler that "succeeded" at the protocol level may still carry a tool
        # failure. Three outcomes, kept distinct because they are three different
        # layers and collapsing them makes the log unanalyzable:
        #   "ok"         - handled, no error
        #   "error"      - the message itself failed (raised): malformed params
        #                  the runner rejected, METHOD_NOT_FOUND, pre-init gate
        #   "tool_error" - the message was handled fine, the TOOL failed
        #
        # Caveat on "ok" for NOTIFICATIONS: the runner drops a pre-init or
        # malformed notification silently and returns None, which is
        # indistinguishable here from one a handler processed. An "ok"
        # notification record means "accepted for dispatch", not "acted upon".
        tool_error = _result_error(result)
        self._write(
            method,
            params,
            request_id,
            ctx,
            t0,
            outcome="ok" if tool_error is None else "tool_error",
            error=tool_error,
        )
        return result

    def _write(self, method, params, request_id, ctx, t0, outcome, error) -> None:
        # DO NOT "simplify" the wire-params fallback in _client_name_version by
        # assuming this runs late enough to see the committed handshake. It does
        # not: ServerRunner commits `connection.client_params` only AFTER the whole
        # middleware chain unwinds, so on `initialize` the session identity is None
        # both before and after call_next (verified against mcp 2.0.0). The wire
        # fallback is the ONLY thing that names the client on that record, and it is
        # the one record where the client is named at all.
        write_rpc_record(
            method=method,
            params=params,
            outcome=outcome,
            ms={"total": int((time.monotonic() - t0) * 1000)},
            server_version=self._version,
            git_sha=self._git_sha,
            request_id=request_id,
            client=_client_block(ctx),
            error=error,
        )
