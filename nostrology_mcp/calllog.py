"""Structured JSONL call log - one record per inbound MCP message.

This is the only record of what consumers actually attempt against the server
(method, params, outcome, sizes, timings, and - on HTTP - which client), kept in a
structured form so it can go downstream later (usage analysis, optimization targets,
spotting what callers try that the tool can't yet express). Design decisions:

  * v-field on every record so the schema can evolve without ambiguity. v2 added
    the optional `client` block (session id + clientInfo from the initialize
    handshake - only the HTTP transport carries them). v3 added the `kind`
    discriminator and the second record kind:
      - kind="tool": the rich per-tool-call record (params, resolved sql_key,
        rows/bytes/timings) written from the tool body.
      - kind="rpc":  one record per inbound JSON-RPC message, written by the
        observation middleware BEFORE params validation - so `initialize`,
        `tools/list`, `resources/list`, `resources/read` and calls the SDK
        rejects are all visible, not just successful tool calls.
    v4 added the THIRD kind, one layer further out:
      - kind="http": one record per inbound HTTP request on the MCP path, written
        by the raw-ASGI seam in asgilog.py - OUTSIDE the SDK's session manager, so
        it is the ONLY record that exists for a request the transport rejects
        before it ever becomes a JSON-RPC message (400 missing/unknown session,
        409 second SSE stream, 406, 421). Closing that blind spot is why v4
        exists; see asgilog.py for the incident that motivated it.
    Over HTTP a successful tools/call therefore produces THREE records (http, rpc,
    tool); under stdio there is no http record at all, because there is no HTTP.
    They join on ts + client.session_id. Read them by switching on `kind`, never
    by position.
  * Best-effort: a logging failure must NEVER fail the call (stderr note only).
  * Two sinks, both the same record:
      - the JSONL day-file (calls-YYYY-MM-DD.jsonl), the durable copy - a deployment
        keeps it on a volume so it survives container recreates and feeds the
        eventual downstream analysis;
      - stdout, ONLY when NOSTROLOGY_MCP_LOG_STDOUT=1 (the container sets it), which
        the deployment's log shipper collects into Loki with no change to its config - the live
        demo surface (Grafana Explore on {container="nostrology-mcp"}). The flag is
        off by default because under the stdio transport stdout IS the protocol
        channel and one log line would corrupt the JSON-RPC stream.

Deliberately NOT recorded: the response body size of an rpc record. The tool
record already carries exact `bytes` for the one expensive path, and Caddy's
access log carries the HTTP response size for every message - joinable on the
session id, which both logs carry. Serializing every result twice to measure it
here would cost more than it tells us.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

_DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

SCHEMA_VERSION = 4

# Serialized-params ceiling for rpc records. Every param this server defines is
# tens of bytes (a metric name, a resource URI); the cap exists because `params`
# is attacker-controlled - an unauthenticated public endpoint will eventually be
# sent a multi-megabyte body, and the log must not amplify it into Loki.
MAX_PARAMS_CHARS = 1024

# Ceiling on each field of the `client` block. Client identity comes from the
# `initialize` body, which is as attacker-controlled as `params` - but unlike
# params it is stamped on EVERY subsequent record for the life of the session, so
# an uncapped one amplifies: one handshake with a 4 MB clientInfo.name (the SDK's
# default max request body) writes 4 MB per message thereafter, to the day-file
# AND through the log shipper into Loki. Capping params alone does not close that; this does.
# 128 is generous for an `Implementation` name/version or a session id.
MAX_CLIENT_FIELD_CHARS = 128


def clip(value, limit: int = MAX_CLIENT_FIELD_CHARS):
    """Bound one identity string, keeping the head (which is the identifying part)
    and recording how much was dropped. Non-strings pass through untouched - the
    caller's own coercion rules apply, and a surprise type is the emitter's problem,
    not something to silently stringify here."""
    if not isinstance(value, str) or len(value) <= limit:
        return value
    return f"{value[:limit]}...[+{len(value) - limit}]"


# Kept when params are truncated: the routing fields, i.e. the ones the demand
# analysis is actually about. Dropping "which tool / which resource" along with an
# oversized argument blob would throw away the whole point of the record.
_TRUNCATION_KEEP_KEYS = ("name", "uri", "method")

# Redacted before anything is written. This endpoint is unauthenticated BY DESIGN,
# so no credential can legitimately appear in params today - this is forward
# insurance for the day one does (auth params, a client stuffing a token into
# tool arguments). Cheap, and the alternative is a security bug in a log the
# sandboxed observer reads: see nozzl/CLAUDE.md, "never log secrets".
_SECRET_KEY_MARKERS = ("token", "secret", "password", "passwd", "authorization", "apikey", "api_key")


def log_dir() -> str:
    return os.environ.get("NOSTROLOGY_MCP_LOG_DIR", _DEFAULT_DIR)


def _redact(value):
    """Recursively blank values whose KEY looks credential-bearing. Key-based, not
    value-based: guessing at secret-shaped strings both misses real secrets and
    mangles legitimate data (a base64 metric name), whereas the key name is what
    the caller actually chose."""
    if isinstance(value, dict):
        return {
            k: ("[redacted]" if any(m in str(k).lower() for m in _SECRET_KEY_MARKERS) else _redact(v))
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


# The VALUE-shaped backstop for free text (the `error` field), where there is no
# key to match on. A Cashu token/payment-request is a bearer instrument, and it
# reaches this server in TWO shapes, both of which must be scrubbable:
#   * self-labeling serializations - `cashuA`/`cashuB` + base64url, `creq` +
#     version char + payload (NUT-00, NUT-18);
#   * a NUT-18 PaymentRequestPayload JSON object, whose proof secrets carry no
#     prefix at all - there the give-away is the JSON shape itself
#     (`"secret":"..."` / `"C":"02..."` pairs), which the second pattern blanks
#     by VALUE while keeping the key visible for diagnosis.
# The paid-tool path is REQUIRED to build its errors from fixed strings and
# never interpolate the credential (pinned by
# tests) - these regexes exist for the path nobody wrote: an SDK layer or a
# future handler echoing request content into an exception message. Key-based
# `_redact` cannot see inside a string, so the error field gets this instead.
_BEARER_TOKEN_RE = re.compile(r"(?:cashu|creq)[A-Z][A-Za-z0-9_+/=-]{8,}")
_PROOF_FIELD_RE = re.compile(r'("(?:secret|C)"\s*:\s*")[^"]{8,}(")')


def scrub_bearer_tokens(text):
    """Replace any Cashu token / payment-request serialization - or the
    secret-bearing fields of a JSON-shaped proof - embedded in free text with a
    marker. Non-strings pass through (the writers pass None through). Applied
    at the SINK to the `error` field of BOTH record kinds, so no future call
    site can forget it."""
    if not isinstance(text, str):
        return text
    text = _BEARER_TOKEN_RE.sub("[redacted-cashu]", text)
    return _PROOF_FIELD_RE.sub(r"\1[redacted-cashu]\2", text)


def _cap_params(params):
    """Params for an rpc record: redacted, and capped so one hostile body cannot
    flood the day-file. Over the cap we keep the size (the interesting fact about an
    oversized call) plus the routing keys, and drop the rest.

    Serialized with the SAME arguments `_emit` uses, deliberately: a value that only
    `default=str` can encode would pass a laxer check here and then blow up in
    `_emit`, costing the WHOLE record instead of just the params field."""
    if params is None:
        return None
    try:
        safe = _redact(dict(params))
        encoded = json.dumps(safe, separators=(",", ":"))
    except Exception:
        # Non-dict, non-serializable, or a __str__ that raises: record the shape
        # rather than losing the whole log record over an odd payload.
        return {"_unserializable": type(params).__name__}
    if len(encoded) > MAX_PARAMS_CHARS:
        capped = {"_truncated_chars": len(encoded)}
        for key in _TRUNCATION_KEEP_KEYS:
            value = safe.get(key)
            if isinstance(value, str):
                capped[key] = clip(value, MAX_PARAMS_CHARS)
        return capped
    return safe


def _clip_param_values(params: dict) -> dict:
    """The tool record's params with every string value clipped to
    MAX_PARAMS_CHARS - the values are post-default scalars, so a per-value clip
    keeps every key (the demand signal) where the rpc record's whole-dict cap
    would drop them."""
    return {k: clip(v, MAX_PARAMS_CHARS) if isinstance(v, str) else v for k, v in params.items()}


def _clip_client(client: dict | None) -> dict | None:
    """Applied at the SINK, in both writers, so no caller can forget it. Clipping
    at the two call sites instead would be one refactor away from a regression that
    is invisible until someone sends a hostile handshake."""
    if not client:
        return client
    return {k: clip(v) for k, v in client.items()}


def _emit(record: dict) -> None:
    """The two sinks. Shared by both record kinds so they can never drift apart.

    except Exception, not OSError: the never-fail-the-call guarantee must hold by
    construction (a json.dumps TypeError in the db_error path would otherwise MASK
    the original DB error), not by the current shape of the inputs."""
    try:
        line = json.dumps(record, separators=(",", ":"))
        if os.environ.get("NOSTROLOGY_MCP_LOG_STDOUT") == "1":
            # -> the log shipper -> Loki. print() adds the newline; flush so a demo tail sees
            # the call as it happens, not on buffer pressure.
            print(line, flush=True)
        d = log_dir()
        os.makedirs(d, exist_ok=True)
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = os.path.join(d, f"calls-{day}.jsonl")
        # Concurrent calls append concurrently (the SDK runs sync tools on worker
        # threads). O_APPEND writes of a <1 KB line don't interleave on POSIX.
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception as exc:
        print(f"nostrology-mcp: call log write failed: {exc}", file=sys.stderr)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_record(
    tool: str,
    params: dict,
    outcome: str,  # "ok" | "db_error" | "unknown_metric"
    sql_key: str | None,
    rows: int | None,
    bytes_out: int | None,
    ms: dict,
    server_version: str,
    git_sha: str,
    client: dict | None = None,  # {"session_id"?, "name"?, "version"?} - HTTP only
    error: str | None = None,
) -> None:
    """kind="tool": the rich record, written from the tool body.

    NOTE `params` here are the EFFECTIVE values (post-default), so this record
    cannot tell whether the caller passed `grain` or inherited it. The paired
    kind="rpc" record carries the raw wire arguments and answers that.

    `params` and `error` pass through the same redaction as the rpc record's.
    This closed a blocker-class gap found in the paid tool's review: before the
    paid tool existed, every tool param was a short public scalar and this
    writer stored `params` verbatim - the moment a tool takes a credential
    argument (`cashu_token`), the verbatim path would have written a bearer
    instrument to the day-file and Loki. Redaction lives HERE at the sink, not
    in the tool bodies, so no future tool can forget it.
    """
    record = {
        "v": SCHEMA_VERSION,
        "kind": "tool",
        "ts": _now(),
        "tool": tool,
        # Values clipped like the rpc record's: every tool parameter is caller
        # text on an unauthenticated endpoint, and an uncapped 4 MB mint_url (or
        # model, app_id, from) would land whole on disk AND in Loki.
        "params": _clip_param_values(_redact(params)) if isinstance(params, dict) else params,
        "resolved": {"sql_key": sql_key},
        "outcome": outcome,
        "rows": rows,
        "bytes": bytes_out,
        "est_tokens": (bytes_out + 3) // 4 if bytes_out is not None else None,
        "ms": ms,
        "server": {"version": server_version, "git_sha": git_sha},
    }
    if client is not None:
        record["client"] = _clip_client(client)
    if error is not None:
        # Rejection messages echo the caller's input (an off-contract `from`, grain
        # or metric), so the error is caller text too: scrubbed FIRST (a token cut at
        # the clip boundary would slip past the scrub's pattern), then clipped.
        record["error"] = clip(scrub_bearer_tokens(error), MAX_PARAMS_CHARS)
    _emit(record)


def write_http_record(
    method: str | None,
    path: str | None,
    status: int | None,
    had_session_header: bool,
    ms: dict,
    server_version: str,
    git_sha: str,
    client: dict | None = None,
) -> None:
    """kind="http": one record per inbound HTTP request on the MCP path.

    Written by the raw-ASGI seam (asgilog.py), which sits OUTSIDE the SDK's
    session manager - so this is the only record that exists for a request the
    transport rejects before it becomes a JSON-RPC message.

    `status` and `client.session_id` may be None when the connection died before
    a response started; that is a real outcome, recorded as such rather than
    guessed at.

    `had_session_header` is deliberately NOT derivable from `client.session_id`:
    that field falls back to the id the SERVER minted (so `initialize` is
    joinable), which would otherwise mask the never-initialized-transport
    signature this record exists to make visible.
    """
    record = {
        "v": SCHEMA_VERSION,
        "kind": "http",
        "ts": _now(),
        # CLIPPED HERE, not at the seam: `method` and `path` are top-level, so
        # `_clip_client` below does not reach them, and both are chosen entirely
        # by the caller. The path is the dangerous one - the ASGI seam matches
        # anything under the MCP path and h11 admits a ~16 KiB request target, so
        # unclipped this is a 16 KB record per request onto the obs volume and
        # through stdout into Loki, on an endpoint with no auth and no rate limit.
        "method": clip(method),
        "path": clip(path),
        "status": status,
        "had_session_header": had_session_header,
        "ms": ms,
        "server": {"version": server_version, "git_sha": git_sha},
    }
    if client is not None:
        record["client"] = _clip_client(client)
    _emit(record)


def write_rpc_record(
    method: str,
    params,  # raw, pre-validation; may be any JSON value or None
    outcome: str,  # "ok" | "error" (message raised) | "tool_error" (handler returned is_error)
    ms: dict,
    server_version: str,
    git_sha: str,
    request_id=None,  # None => this was a notification, not a request
    client: dict | None = None,
    error: str | None = None,
) -> None:
    """kind="rpc": one record per inbound JSON-RPC message.

    Written by the observation middleware, which sits ABOVE params validation -
    so this is the record that exists for a call the SDK rejects, and the only
    record that exists at all for discovery traffic (initialize / tools/list /
    resources/list / resources/read). `params` are the RAW wire arguments,
    redacted and capped.
    """
    record = {
        "v": SCHEMA_VERSION,
        "kind": "rpc",
        "ts": _now(),
        "method": method,
        "request_id": request_id,
        "params": _cap_params(params),
        "outcome": outcome,
        "ms": ms,
        "server": {"version": server_version, "git_sha": git_sha},
    }
    if client is not None:
        record["client"] = _clip_client(client)
    if error is not None:
        record["error"] = scrub_bearer_tokens(error)
    _emit(record)
