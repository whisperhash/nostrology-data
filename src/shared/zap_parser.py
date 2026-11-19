"""
Shared zap receipt (kind-9735) parsing utilities.

Extracts sender pubkey, recipient, target event, and amount from a kind-9735
zap receipt by parsing the embedded kind-9734 zap request in the description tag.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass


@dataclass
class ParsedZap:
    """Parsed fields from a kind-9735 zap receipt."""
    sender_pubkey: str
    recipient_pubkey: str | None
    target_event_id: str | None
    amount_msats: int | None
    created_at: int


def parse_zap_receipt(event: dict, warn: bool = False, line_num: int = 0) -> ParsedZap | None:
    """Parse a kind-9735 zap receipt, extracting sender, recipient, target, and amount.

    Returns a ParsedZap, or None if the event is malformed (missing description
    tag, unparseable zap request, or missing sender pubkey).

    Args:
        event: The full kind-9735 event dict.
        warn: If True, print warnings for malformed events to stderr.
        line_num: Line number for warning context.
    """
    tags = event.get("tags")
    if not isinstance(tags, list):
        if warn:
            print(f"  WARNING: Missing or invalid tags on line {line_num}", file=sys.stderr)
        return None

    # Extract top-level tags
    recipient = None
    target_event = None
    bolt11 = None
    zap_request_json = None

    for tag in tags:
        if not isinstance(tag, list) or len(tag) < 2:
            continue
        if tag[0] == "description":
            zap_request_json = tag[1]
        elif tag[0] == "p":
            recipient = tag[1]
        elif tag[0] == "e":
            target_event = tag[1]
        elif tag[0] == "bolt11":
            bolt11 = tag[1]

    if zap_request_json is None:
        if warn:
            print(f"  WARNING: No description tag on line {line_num}", file=sys.stderr)
        return None

    # Parse the embedded kind-9734 zap request
    try:
        zap_request = json.loads(zap_request_json)
    except (json.JSONDecodeError, TypeError):
        if warn:
            print(
                f"  WARNING: Unparseable description tag on line {line_num}: "
                f"{str(zap_request_json)[:80]}",
                file=sys.stderr,
            )
        return None

    # A description tag may contain valid JSON that is NOT an object (e.g. a bare
    # array or string from a malformed client). Guard before .get() so one bad
    # event can't crash a whole load. Treat as unparseable, like a JSON error.
    if not isinstance(zap_request, dict):
        if warn:
            print(
                f"  WARNING: description tag is not a JSON object on line {line_num}: "
                f"{str(zap_request_json)[:80]}",
                file=sys.stderr,
            )
        return None

    sender = zap_request.get("pubkey")
    if not isinstance(sender, str) or not sender:
        if warn:
            print(f"  WARNING: Missing sender pubkey in description on line {line_num}", file=sys.stderr)
        return None

    # Use zap request's created_at (when sender initiated) if available
    created_at = zap_request.get("created_at", event.get("created_at"))
    if not isinstance(created_at, (int, float)):
        if warn:
            print(f"  WARNING: Missing created_at on line {line_num}", file=sys.stderr)
        return None

    # Parse amount from bolt11 invoice
    amount_msats = _parse_bolt11_amount(bolt11) if bolt11 else None

    return ParsedZap(
        sender_pubkey=sender,
        recipient_pubkey=recipient,
        target_event_id=target_event,
        amount_msats=amount_msats,
        created_at=int(created_at),
    )


# Bolt11 amount multipliers: suffix → multiply by to get msats
_BOLT11_MULTIPLIERS = {
    "m": 100_000_000,   # milli-BTC → msats
    "u": 100_000,       # micro-BTC → msats
    "n": 100,           # nano-BTC → msats
    "p": 0.1,           # pico-BTC → msats
}

_BOLT11_AMOUNT_RE = re.compile(r"^lnbc(\d+)([munp])?1")


def _parse_bolt11_amount(bolt11: str) -> int | None:
    """Parse amount in msats from a bolt11 invoice string.

    The human-readable part format is: 'lnbc' + amount + multiplier + '1' + data.
    Examples:
        lnbc100n1...  → 100 nano-BTC = 10,000 msats
        lnbc1u1...    → 1 micro-BTC = 100,000 msats
        lnbc2m1...    → 2 milli-BTC = 200,000,000 msats

    Returns msats or None if unparseable.
    """
    if not bolt11:
        return None
    match = _BOLT11_AMOUNT_RE.match(bolt11.lower())
    if not match:
        return None
    amount_str, suffix = match.groups()
    try:
        amount = int(amount_str)
    except ValueError:
        return None
    multiplier = _BOLT11_MULTIPLIERS.get(suffix)
    if multiplier is None:
        # No suffix means amount is in BTC (rare for zaps)
        return int(amount * 100_000_000_000)  # BTC → msats
    return int(amount * multiplier)
