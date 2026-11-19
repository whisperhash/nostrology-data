"""Cashu payment support for the paid tool: the NUT-18 challenge the
tool serves, and the HTTP client for the cashu-redeemer sidecar that actually
moves money.

Split of responsibilities: THIS process
(public, DB-connected) never touches proof secrets, never talks to a mint, and
never holds anything spendable - it forwards the presented credential to the
sidecar over the compose bridge and maps the sidecar's typed outcome onto the
tool-result envelope. The sidecar (cashu_redeemer/) does everything monetary.

Log-safety contract: every string this
module produces for an error message, a challenge, or a receipt is built from
CONSTANTS plus the token's sha256 - the token itself is never interpolated
anywhere. calllog's sink-side redaction is the backstop, not the mechanism.

The challenge's machine-readable half is a real NUT-18 payment request
(`creqA`), encoded HERE in ~40 lines of fixed-shape CBOR rather than by asking
the sidecar, so the challenge stays servable when the sidecar is down. The
encoding is pinned byte-for-byte against cashu-ts's PaymentRequest encoder by
test_cashu.py (field order i,a,u,m,d,s; standard base64 with padding - matching
the reference implementation the ecosystem actually decodes with).
"""

import hashlib
import json
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping

# The price and mint list are SHARED CONFIG with the cashu-redeemer sidecar -
# the compose environment sets CASHU_PRICE_SATS / CASHU_MINTS identically on
# both containers, and these defaults match the sidecar's (config.ts). A
# challenge advertising one price while the sidecar enforces another would
# reject every honest payer, so test_cashu.py pins the defaults against the
# sidecar's and the compose file sets both from one env block.
DEFAULT_PRICE_SATS = 21
DEFAULT_MINTS = "https://mint.minibits.cash/Bitcoin,https://testnut.cashu.space"
UNIT = "sat"

# Mints we ACCEPT but never ADVERTISE.
#
# testnut.cashu.space is a FakeWallet mint: it marks every invoice paid, so its
# tokens cost nothing to mint. That is exactly why it is allowlisted - it lets
# us exercise the real payment path against the live public service, with real
# tokens and real fee arithmetic, without spending sats. Publishing it would
# turn our test instrument into a free-entry side door: anyone reading the
# challenge could pay in nothing, and the paid tool would be paid in name only.
#
# So the accept-list and the advertise-list are DIFFERENT LISTS, and every
# caller-facing surface must be built from the advertise-list. Acceptance stays
# with the sidecar (it enforces CASHU_MINTS, which still contains testnut);
# this constant only removes it from what we say out loud. When testnut is
# eventually dropped from CASHU_MINTS altogether, this becomes inert rather
# than wrong.
DEFAULT_UNADVERTISED_MINTS = "https://testnut.cashu.space"

# Hard cap on the credential argument, enforced FIRST, before anything else
# looks at the value: inflight.py's duplicate-id guard
# inspects bodies only up to MAX_INSPECT_BYTES (16 KiB) and forwards larger
# bodies UNGUARDED, restoring the SDK's cross-delivery overwrite for that
# request. A 21-sat token is three proofs, a few hundred bytes; 4 KiB is ~10x
# headroom.
#
# The cap is CHARS **plus an ASCII requirement**, enforced together, because
# chars alone do not bound the serialized body (the review's catch): a
# 4,096-char non-ASCII value can serialize as \uXXXX escapes at 6 bytes per
# char (~24 KiB), sailing past the 16 KiB inspection ceiling. ASCII-only, the
# worst case is 2 bytes per char (escaped quote/backslash), so a maximal
# credential serializes under 8 KiB - and every legal credential IS ascii
# (cashuB is base64url; a PaymentRequestPayload's fields are hex/URLs).
# test_server_nutzaps.py pins MAX_TOKEN_CHARS * 2 <= MAX_INSPECT_BYTES so a
# future raise of either constant cannot silently defeat the other.
#
# Honest scope note: this keeps HONEST paid traffic guarded. Any caller can
# already ship a >16 KiB junk body on any method and be forwarded unguarded -
# that is inflight.py's documented design (unknown input passes through); the
# cap's job is to ensure the paid tool never REQUIRES such a body.
MAX_TOKEN_CHARS = 4096


def price_sats() -> int:
    return int(os.environ.get("CASHU_PRICE_SATS", str(DEFAULT_PRICE_SATS)))


def mint_urls() -> list[str]:
    """The ACCEPT list - shared with the sidecar, which enforces it. Not what
    to show a caller: use advertised_mint_urls() for anything caller-facing."""
    raw = os.environ.get("CASHU_MINTS", DEFAULT_MINTS)
    return [m.strip() for m in raw.split(",") if m.strip()]


def _canonical_mint(url: str) -> str | None:
    """Mirror of the sidecar's canonicalMintUrl (canonical.ts, the
    draft-cashu-charge profile): lowercase scheme and host, drop a default
    port, strip trailing slashes; PATH CASE IS PRESERVED (mint.minibits.cash's
    /Bitcoin is a distinct path from /bitcoin). This is the EXACT-IDENTITY
    form - two spellings that reduce to the same string are the same mint to
    the sidecar too.

    TOTAL: returns None for anything that is not a well-formed absolute
    http(s) URL, rather than raising. It has to be, because since the receipt
    scrub it runs AFTER the money has moved: `urlsplit(...).port` raises
    ValueError on a non-numeric port, and one malformed CASHU_MINTS entry
    would otherwise turn a paid call into an unhandled exception with no
    delivery-audit record - the worst outcome in the whole design. None never
    equals anything, and an entry we cannot parse is one we cannot name
    safely, so None also means "not advertised"."""
    try:
        parsed = urllib.parse.urlsplit(url.strip())
        port = parsed.port
    except (ValueError, AttributeError):
        return None
    scheme = (parsed.scheme or "").lower()
    host = parsed.hostname or ""
    if scheme not in ("http", "https") or not host:
        return None
    if (scheme == "https" and port == 443) or (scheme == "http" and port == 80):
        port = None
    netloc = f"{host}:{port}" if port else host
    return f"{scheme}://{netloc}{parsed.path.rstrip('/')}"


def _hiding_key(url: str) -> tuple[str, str] | None:
    """The key the HIDE comparison uses - deliberately BROADER than
    _canonical_mint, and the difference is the point.

    Acceptance must be exact: a token names its mint, and the match decides
    whether money moves, so the sidecar compares canonical URLs. Hiding is the
    opposite kind of question, because its failure direction is PUBLICATION,
    which cannot be taken back. So this ignores the port and a trailing FQDN
    dot - `https://testnut.cashu.space:8443` and `https://testnut.cashu.space.`
    are the same operator's FakeWallet mint, and naming either one publishes
    the host we are hiding.

    Returns (host, path). The caller treats an empty hidden path as "this
    whole host", which keeps the one distinction that matters: a hidden entry
    that DOES name a path (example.test/bitcoin) must not swallow a real mint
    on the same host at a different path (example.test/Bitcoin)."""
    try:
        parsed = urllib.parse.urlsplit(url.strip())
        parsed.port
    except (ValueError, AttributeError):
        return None
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").rstrip(".")
    if scheme not in ("http", "https") or not host:
        return None
    return (host, parsed.path.rstrip("/"))


def _is_hidden(url: str) -> bool:
    """Whether this mint is one we never name. Fails toward hiding in all
    three directions: a malformed accept-list entry is hidden (we cannot name
    what we cannot parse), a malformed UNADVERTISED entry still hides its own
    exact spelling (so a typo there cannot silently publish the thing it was
    written to hide), and a host match with no path on the hidden side hides
    the host."""
    key = _hiding_key(url)
    if key is None:
        return True
    host, path = key
    for hidden in unadvertised_mint_urls():
        hidden_key = _hiding_key(hidden)
        if hidden_key is None:
            if hidden.strip() == url.strip():
                return True
            continue
        hidden_host, hidden_path = hidden_key
        if host == hidden_host and hidden_path in ("", path):
            return True
    return False


def unadvertised_mint_urls() -> list[str]:
    """Mints accepted but deliberately never named to a caller - see
    DEFAULT_UNADVERTISED_MINTS.

    Falls back to the default on a BLANK value, not just an unset one: a set
    but empty var (docker run -e CASHU_UNADVERTISED_MINTS=, a stray line in an
    .env) would otherwise mean "hide nothing" and quietly republish the test
    mint. The .strip() matters as much as the `or` - a whitespace-only value is
    truthy, and would sail past a bare `or` into an empty hide list. There is
    deliberately no way to express "hide nothing" through this variable; the
    way to stop hiding a mint is to stop accepting it."""
    raw = os.environ.get("CASHU_UNADVERTISED_MINTS", "").strip() or DEFAULT_UNADVERTISED_MINTS
    return [m.strip() for m in raw.split(",") if m.strip()]


def advertised_mint_urls() -> list[str]:
    """The accept list minus the unadvertised ones - the ONLY mint list that
    may reach a caller (challenge text, structuredContent, the creqA payment
    request, the registry, the web catalog)."""
    return [m for m in mint_urls() if not _is_hidden(m)]


def is_advertised_mint(url: str | None) -> bool:
    """Whether this mint may be NAMED to a caller. Two conditions, both
    required: not on the hide list, AND actually on the accept list - so an
    unknown mint (one that could not have redeemed anything, meaning something
    upstream is wrong) is not advertised either, and a future source of mint
    URLs cannot publish one by forgetting to ask."""
    if not url or _is_hidden(url):
        return False
    canon = _canonical_mint(url)
    if canon is None:
        return False
    return any(_canonical_mint(m) == canon for m in advertised_mint_urls())


def public_mint_label(url: str | None) -> str:
    """How to refer to the mint a payment actually came from, in caller-facing
    prose. Named when advertised, described when not.

    The SUCCESS envelope is a caller-facing surface exactly like the challenge -
    and it is the one a rehearsal prints to a terminal that may be screen-shared
    or pasted into a doc, which is the realistic way a hidden mint escapes.
    Scrubbing the challenge and echoing the mint back on the paid path would
    have published it through the very run that exists to avoid spending sats."""
    return url if is_advertised_mint(url) else "an accepted mint"


# Receipt fields that identify WHICH mint took the payment. `mint` is the URL
# itself; `fee_sats` is the mint's NUT-02 input fee, which is 1 for the test
# mint and 0 for the advertised one - a one-field fingerprint, on a receipt
# whose missing `mint` key already says "an unadvertised mint was used". Both
# go when the mint is scrubbed.
_MINT_IDENTIFYING_FIELDS = ("mint", "fee_sats")


def scrub_receipt(receipt: Mapping[str, object]) -> dict:
    """The sidecar's receipt, safe to put on the wire. Drops the identifying
    fields outright when the mint is unadvertised rather than replacing them
    with prose: this copy is the MACHINE-readable one, and a non-URL where a
    client expects a URL is a worse answer than an absent key. What survives -
    the receipt id, the amount, the timestamp - is everything a support claim
    needs.

    Any NEW field the sidecar adds to Receipt must be assessed against this
    list; a field that varies by mint belongs in it."""
    out = dict(receipt)
    if "mint" in out and not is_advertised_mint(str(out["mint"])):
        for field in _MINT_IDENTIFYING_FIELDS:
            out.pop(field, None)
    return out


def redeemer_url() -> str:
    return os.environ.get("NOSTROLOGY_MCP_CASHU_REDEEMER_URL", "http://127.0.0.1:8081")


def nutzaps_tool_enabled() -> bool:
    """The kill-switch: NOSTROLOGY_MCP_NUTZAPS_ENABLED=0
    disables the paid tool while the two free tools keep serving. Read at
    import (registration) AND at call time - the call-time read is the belt
    that holds if the env changes under a live process's cached registration."""
    return os.environ.get("NOSTROLOGY_MCP_NUTZAPS_ENABLED", "1") != "0"


def token_hash(raw: str) -> str:
    """sha256 hex of the trimmed credential - the receipt reference. MUST match
    the sidecar's tokenHash (redeem.ts) or receipts and redemption records stop
    joining; both suites pin the same vector."""
    return hashlib.sha256(raw.strip().encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# creqA - NUT-18 payment request, fixed-shape CBOR
# ---------------------------------------------------------------------------


def _cbor(value) -> bytes:
    """Definite-length CBOR (RFC 8949) for exactly the shapes a NUT-18 payment
    request needs: unsigned int, text string, bool, array, map (insertion
    order). Deliberately NOT a general encoder - an unsupported type is a
    programming error and raises."""
    if isinstance(value, bool):  # before int: bool subclasses int
        return b"\xf5" if value else b"\xf4"
    if isinstance(value, int):
        if value < 0:
            raise ValueError("negative ints not needed for a payment request")
        return _cbor_head(0, value)
    if isinstance(value, str):
        data = value.encode("utf-8")
        return _cbor_head(3, len(data)) + data
    if isinstance(value, list):
        return _cbor_head(4, len(value)) + b"".join(_cbor(v) for v in value)
    if isinstance(value, dict):
        return _cbor_head(5, len(value)) + b"".join(_cbor(k) + _cbor(v) for k, v in value.items())
    raise TypeError(f"unsupported CBOR type: {type(value).__name__}")


def _cbor_head(major: int, arg: int) -> bytes:
    if arg < 24:
        return bytes([(major << 5) | arg])
    for bits, code in ((8, 24), (16, 25), (32, 26), (64, 27)):
        if arg < (1 << bits):
            return bytes([(major << 5) | code]) + arg.to_bytes(bits // 8, "big")
    raise ValueError("length exceeds u64")


def encode_creqa(amount: int, unit: str, mints: list[str], request_id: str, description: str) -> str:
    """`"creq" + "A" + base64(CBOR(RawPaymentRequest))`. Field ORDER matches
    cashu-ts's encoder (i, a, u, m, d, s) and the base64 is the standard
    alphabet with padding, byte-identical to what cashu-ts 4.8.0 emits -
    decoders accept any CBOR map order, but matching the reference bytes is
    what the cross-language pin test asserts, and staying byte-identical means
    a decode bug in ANY wallet reproduces against cashu-ts too, not just
    against us.

    `t` (transports) is deliberately ABSENT: NUT-18 defines an empty/missing
    transport as in-band - the payment rides the same channel the request
    arrived on, which here is the tool's `cashu_token` argument.

    `a` carries the price NET of input fees (NUT-18's post-2026-07-21
    convention: sum(proofs) - input_fee(proofs) >= a) - so this is 21, never
    "21 plus fee"."""
    import base64

    raw = {
        "i": request_id,
        "a": amount,
        "u": unit,
        "m": mints,
        "d": description,
        "s": True,  # single-use: one token, one call
    }
    return "creqA" + base64.b64encode(_cbor(raw)).decode("ascii")


# ---------------------------------------------------------------------------
# The challenge: an is_error tool result, terms in TEXT, HTTP 200 (ok)
# ---------------------------------------------------------------------------

CHALLENGE_REQUEST_ID = "nutzaps-v0"


def challenge_text() -> str:
    """The model-visible half. As verification/cashu_probe.py measured: on an error result
    the model receives ONLY this text - error.data, structuredContent and _meta
    are all stripped by the host - so everything a model needs to act must be
    in here, short enough to be read back verbatim."""
    mints = advertised_mint_urls()
    # Every advertised mint hidden (a misconfiguration, or the day the last
    # public mint is retired) must degrade to a vaguer sentence, NEVER to
    # naming a hidden mint and never to an empty list rendered as
    # "these mints: .". The caller can still discover what is accepted by
    # presenting a token: the sidecar names the rule in its refusal.
    # "one of these mints" in front of a single URL reads as though something
    # was elided from a paid tool's terms - the one place a reader is entitled
    # to assume nothing was.
    if not mints:
        source = "an accepted Cashu mint"
    elif len(mints) == 1:
        source = f"this mint: {mints[0]}"
    else:
        source = f"one of these mints: {', '.join(mints)}"
    return (
        f"Payment required: this tool costs {price_sats()} {UNIT} per call. "
        f"To pay: send exactly {price_sats()} {UNIT} (plus the mint's swap fee, "
        f"if any) as a Cashu token from {source}. "
        "Pass the token in the `cashu_token` argument of this same call - either "
        "a cashuB (TokenV4) string or a NUT-18 PaymentRequestPayload JSON "
        "object. The token is redeemed immediately and is single-use; "
        "overpayment is rejected, no change is made. A machine-readable NUT-18 "
        "payment request is attached to this result's structuredContent as "
        "`paymentRequest`."
    )


def challenge_structured() -> dict:
    """The programmatic half. Invisible to the model (measured), intact to any
    SDK-level client - carries the real creqA so a wallet can decode terms
    offline. Keys follow draft-payment-transport-mcp-00's data shape where one
    exists (amount/unit/paymentRequest), ours where none does."""
    # advertised_, not mint_: this dict and the creqA built from it are both
    # caller-facing, and the creqA's `m` field is the one a wallet reads.
    mints = advertised_mint_urls()
    return {
        "amount": price_sats(),
        "unit": UNIT,
        "mints": mints,
        "paymentRequest": encode_creqa(
            price_sats(),
            UNIT,
            mints,
            CHALLENGE_REQUEST_ID,
            f"nostrology_query_nutzaps: {price_sats()} {UNIT} per call",
        ),
        "accepts": ["cashuB-string", "nut18-payment-request-payload"],
        "argument": "cashu_token",
    }


# ---------------------------------------------------------------------------
# Sidecar client
# ---------------------------------------------------------------------------

# MUST comfortably exceed the sidecar's provable worst case, or a slow (not
# dead) mint turns a completing payment into a client-side timeout - the
# review's blocker finding. The arithmetic, from the sidecar's config
# (mintTimeoutMs=15s) and cashu-ts's hardcoded NUT-19 retry loop (up to 9
# replays with jittered backoff capped at 1s each): swap phase <= 10 x 15s +
# ~6s backoff ~= 156s, plus one restore attempt (15s) and one checkstate
# (15s), plus session loading ~= 200s worst case. 240 is that plus headroom.
# The sidecar, not this timeout, is the authority on giving up; agents
# tolerate a slow call better than a wrong answer (the free tools already
# block up to 90s on a slow database connect by the same reasoning).
DEFAULT_REDEEMER_TIMEOUT_SECONDS = 240


def _redeemer_timeout() -> float:
    return float(os.environ.get("NOSTROLOGY_MCP_CASHU_TIMEOUT_SECONDS", str(DEFAULT_REDEEMER_TIMEOUT_SECONDS)))


class RedeemerUnreachable(Exception):
    """The sidecar could not be CONTACTED - the failure happened before the
    request was sent (connection refused, DNS). Nothing was presented to a
    mint, so the caller's token is certainly intact and the error text may say
    so."""


class RedeemerIndeterminate(Exception):
    """The request (may have) reached the sidecar but no usable answer came
    back - a timeout mid-call, a dropped connection, an unparseable body. The
    sidecar may STILL BE COMPLETING the redemption (its mint conversation is
    legitimately slow), so the money state is UNKNOWN and the caller must
    never be told the token is intact - the review's blocker finding. The
    right guidance is: do not pay again; retry the SAME call with the SAME
    token (the sidecar's resume-deliver path answers it correctly whether or
    not the payment completed)."""


def redeem(token: str, client_ip: str | None) -> dict:
    """POST the credential to the sidecar; return its typed outcome dict
    (redeem.ts RedeemResult). A 503 body from a healthy sidecar (unresolved /
    store_unavailable) is a valid outcome and is returned, not raised. Raises
    RedeemerUnreachable ONLY when provably nothing was sent; every other
    failure is RedeemerIndeterminate."""
    body = json.dumps({"token": token, "client_ip": client_ip}).encode("utf-8")
    request = urllib.request.Request(
        redeemer_url().rstrip("/") + "/redeem",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    sent = False
    try:
        with urllib.request.urlopen(request, timeout=_redeemer_timeout()) as response:
            sent = True
            payload = response.read()
    except urllib.error.HTTPError as exc:
        # The sidecar answers 503 with a JSON body for unresolved/store states;
        # that body IS the outcome. An HTTPError means a response arrived, so
        # the request was certainly processed - unreadable bodies from here on
        # are indeterminate, never "token intact".
        sent = True
        try:
            payload = exc.read()
        except Exception:
            raise RedeemerIndeterminate(f"http {exc.code}, unreadable body") from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if not sent and isinstance(reason, (ConnectionRefusedError, socket.gaierror)):
            raise RedeemerUnreachable(type(reason).__name__) from exc
        raise RedeemerIndeterminate(type(reason).__name__ if reason else "URLError") from exc
    except ConnectionRefusedError as exc:
        raise RedeemerUnreachable("ConnectionRefusedError") from exc
    except Exception as exc:
        # TimeoutError, ConnectionResetError, anything mid-conversation: the
        # request may have been sent and may still be completing.
        raise RedeemerIndeterminate(type(exc).__name__) from exc
    try:
        outcome = json.loads(payload)
    except Exception as exc:
        raise RedeemerIndeterminate("unparseable response") from exc
    if not isinstance(outcome, dict) or "outcome" not in outcome:
        raise RedeemerIndeterminate("malformed outcome")
    return outcome


def report_outcome(redemption_id: str, delivered: bool, note: str | None) -> None:
    """The delivery audit record: tell the sidecar whether the paid-for data
    was actually delivered. Best-effort by design - the delivery already
    happened (or failed) and this report must never change the caller's
    result; the sidecar's cashu_delivery_failed_total metric is what alerts."""
    body = json.dumps({"id": redemption_id, "delivered": delivered, "note": note}).encode("utf-8")
    request = urllib.request.Request(
        redeemer_url().rstrip("/") + "/redemptions/outcome",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        # 3s, not the redeem timeout: this runs on the SUCCESS path after the
        # data is already rendered, so a hung-but-listening sidecar must not
        # tax every paid response (review nit). The sidecar's own outcome
        # watchdog covers a report that never lands.
        with urllib.request.urlopen(request, timeout=3):
            pass
    except Exception:
        # Silent: the sidecar's watchdog + persist-failure metric cover the
        # record; a logging nicety must never fail the call (calllog's rule).
        pass
