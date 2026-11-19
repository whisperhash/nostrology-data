"""In-process tests of the PAID tool (nostrology_query_nutzaps):
challenge shape, per-channel assertions, the security pins (argument name vs
redaction markers, maxLength vs MAX_INSPECT_BYTES, the log-grep), outcome
mapping for every sidecar verdict, and the payment-before-query ordering.
No live DB, no live sidecar - cashu.redeem/report_outcome and fetch_metric are
monkeypatched at the seams the real code calls."""

import json
import os
from datetime import date

import anyio
import pytest

from nostrology_mcp import cashu
from nostrology_mcp import calllog
from nostrology_mcp import server as srv_mod
from nostrology_mcp.db import MetricResult
from nostrology_mcp.inflight import MAX_INSPECT_BYTES
from nostrology_mcp.metric import METRICS, PAID_METRICS, lookup

AS_OF = 1785349327
ROWS = [(date(2026, 7, 1), 1234), (date(2026, 8, 1), 1310)]

# A plausible token for tests: shape only - it never reaches a mint here.
TOKEN = "cashuB" + "o2FtdWNzYXR4" * 20
RECEIPT = {
    "id": cashu.token_hash(TOKEN),
    "mint": "https://testnut.cashu.space",
    "amount_sats": 21,
    "fee_sats": 1,
    "ts": "2026-08-13T00:00:00Z",
}


def _call(arguments):
    async def go():
        return await srv_mod.server.call_tool("nostrology_query_nutzaps", arguments)

    return anyio.run(go)


@pytest.fixture
def stub_paid(monkeypatch, tmp_path):
    """Happy-path seams: sidecar redeems, DB answers, outcomes are captured."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    calls = {"redeem": [], "outcome": [], "fetch": []}

    def fake_redeem(token, client_ip):
        calls["redeem"].append((token, client_ip))
        return {"outcome": "redeemed", "receipt": dict(RECEIPT)}

    def fake_report(redemption_id, delivered, note):
        calls["outcome"].append((redemption_id, delivered, note))

    def fake_fetch(spec, sql=None, params=None, **kw):
        calls["fetch"].append((spec.key, params))
        return MetricResult(rows=list(ROWS), as_of_epoch=AS_OF, connect_ms=1, query_ms=2)

    monkeypatch.setattr(cashu, "redeem", fake_redeem)
    monkeypatch.setattr(cashu, "report_outcome", fake_report)
    monkeypatch.setattr(srv_mod, "fetch_metric", fake_fetch)
    return calls


def _records(tmp_path):
    files = [f for f in os.listdir(tmp_path) if f.startswith("calls-")]
    if not files:
        return []
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


# -- registration and schema ------------------------------------------------------


def test_tool_is_listed_with_price_and_credential_schema():
    async def go():
        return await srv_mod.server.list_tools()

    (tool,) = [t for t in anyio.run(go) if t.name == "nostrology_query_nutzaps"]
    assert "21 sat per call" in tool.description
    props = tool.input_schema["properties"]
    assert "cashu_token" in props
    assert "metric" in tool.input_schema["required"]
    # The credential is optional (omitting it is the free terms path).
    assert "cashu_token" not in tool.input_schema["required"]


def test_paid_metrics_never_reachable_via_free_tool(stub_paid):
    """THE payment-bypass pin: paid specs live outside METRICS, so the free
    tool cannot resolve them - by construction, not by an if-branch."""
    assert set(PAID_METRICS) == {
        "nutzaps.active_users@day",
        "nutzaps.active_users@week",
        "nutzaps.active_users@month",
    }
    assert not set(PAID_METRICS) & set(METRICS)
    with pytest.raises(KeyError):
        lookup("nutzaps", "active_users", "month")

    # In-process call_tool surfaces the body's ValueError as ToolError (at the
    # wire the SDK renders it as an is_error text result); either way the free
    # tool cannot serve the paid data.
    from mcp.server.mcpserver.exceptions import ToolError

    async def go():
        return await srv_mod.server.call_tool(
            "nostrology_query_metric",
            {"metric": "active_users", "domain": "nutzaps", "grain": "month"},
        )

    with pytest.raises(ToolError) as exc:
        anyio.run(go)
    # Since the free nutzaps domain (2026-09-30) the rejection REDIRECTS: the
    # caller asked the right question of the wrong tool, so the error names the
    # paid tool and the free per-mint alternative instead of listing every free
    # combination - but it is still a rejection, before any fetch.
    assert "PAID metric" in str(exc.value)
    assert "nostrology_query_nutzaps" in str(exc.value)
    assert "mint_active_users" in str(exc.value)
    # And the free tool's stubbed fetch was never reached.
    assert stub_paid["fetch"] == []


# -- the security pins ------------------------------------------------------------


def test_active_users_methodology_says_payments_only():
    """nostrology-data#8: the rebuild behind this metric counts a sender only for
    events carrying a Cashu proof in a `proof` tag (schema/nutzap_active_users.sql
    filters on value_source = 'proof_tags'). A buyer reads the partition line to know
    what is counted, so it must say so, and must not claim every nutzap event."""
    with open(os.path.join(os.path.dirname(__file__), "..", "..", "schema", "nutzap_active_users.sql")) as fh:
        assert fh.read().count("WHERE value_source = 'proof_tags'") == 3
    for spec in PAID_METRICS.values():
        assert "at least one Cashu proof in a `proof` tag" in spec.partition
        assert "are not payments and are not counted" in spec.partition


def test_active_users_methodology_says_test_and_private_mints_are_excluded():
    """nostrology-data#9: the rebuild drops every event on a mint that
    nutzap_mint_classes marks excluded, three times (day, week, month). The partition
    line must say which mints, and that an unreviewed mint still counts."""
    with open(os.path.join(os.path.dirname(__file__), "..", "..", "schema", "nutzap_active_users.sql")) as fh:
        assert fh.read().count("WHERE c.mint_url = n.mint_url AND c.excluded") == 3
    for spec in PAID_METRICS.values():
        assert "Test and private mints are excluded" in spec.partition
        assert "fake Lightning backend" in spec.partition
        assert "Any other mint counts, reviewed or not." in spec.partition


def test_credential_argument_name_matches_a_redaction_marker():
    """The argument MUST be named so calllog's key-based redaction catches it -
    rename it to `payment` or `ecash` and the day-file gets the bearer
    instrument. This pin turns that silent regression into a red test."""
    assert any(marker in "cashu_token" for marker in calllog._SECRET_KEY_MARKERS)


def test_max_token_chars_stays_well_under_max_inspect_bytes():
    """A body over MAX_INSPECT_BYTES is forwarded UNGUARDED by the
    duplicate-id middleware, restoring the SDK's cross-delivery hazard. The
    credential is the only long attacker-controlled argument, so its cap (plus
    JSON envelope overhead) must keep a maximal request under the inspection
    ceiling. 2x headroom, so neither constant can be raised past the other
    silently."""
    assert cashu.MAX_TOKEN_CHARS * 2 <= MAX_INSPECT_BYTES


def test_paid_call_log_records_never_contain_the_token(stub_paid, tmp_path):
    """The required log-grep: render EVERY record a paid call produces
    and grep the raw file for the token and its prefix."""
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    raw = ""
    for f in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, f)) as fh:
            raw += fh.read()
    assert TOKEN not in raw
    assert "cashuB" not in raw
    assert "[redacted]" in raw  # the token key was present and redacted


# -- the challenge ----------------------------------------------------------------


def test_challenge_is_error_with_terms_in_text_and_creqa_in_structured(stub_paid, tmp_path):
    """Per-channel assertions, the first layer of the paid tool's tests: the server
    must EMIT terms in the text (the only model-visible channel on an error)
    AND the machine-readable half in structuredContent."""
    result = _call({"metric": "active_users"})
    assert result.is_error
    text = result.content[0].text
    assert "21 sat" in text
    assert "cashu_token" in text
    assert "mint.minibits.cash/Bitcoin" in text
    assert result.structured_content["amount"] == 21
    assert result.structured_content["paymentRequest"].startswith("creqA")
    # Free path: no sidecar call, no DB call, and the ask is demand-logged.
    assert stub_paid["redeem"] == []
    assert stub_paid["fetch"] == []
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "payment_required"
    assert rec["params"]["cashu_payment_present"] is False


def test_the_challenge_the_TOOL_emits_never_names_the_hidden_mint(stub_paid, tmp_path):
    """cashu.py's own tests pin the accept-vs-advertise split; this pins it at
    the surface that actually reaches the public - a real tool result off the
    real handler, both channels, with the payment request decoded rather than
    string-searched (the mint URL rides inside base64 CBOR, so a substring
    check on the blob would pass while the URL sat right there in it)."""
    import base64

    result = _call({"metric": "active_users"})
    text = result.content[0].text
    creqa = result.structured_content["paymentRequest"]
    payload = base64.b64decode(creqa[len("creqA"):])

    for hidden in cashu.unadvertised_mint_urls():
        host = hidden.split("//", 1)[-1]
        assert host not in text
        assert host.encode() not in payload
        assert hidden not in result.structured_content["mints"]
    # The mint we DO advertise is still named, so this cannot pass by emitting
    # nothing at all.
    assert "mint.minibits.cash/Bitcoin" in text
    assert result.structured_content["mints"] == cashu.advertised_mint_urls()


def test_oversized_token_is_refused_before_the_sidecar(stub_paid, tmp_path):
    result = _call({"metric": "active_users", "cashu_token": "cashuB" + "x" * cashu.MAX_TOKEN_CHARS})
    assert result.is_error
    assert "not a plausible payment" in result.content[0].text
    assert stub_paid["redeem"] == []  # never forwarded
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "token_too_long"


def test_invalid_grain_is_demand_logged_and_costs_nothing(stub_paid, tmp_path):
    from mcp.server.mcpserver.exceptions import ToolError

    with pytest.raises(ToolError) as exc:
        _call({"metric": "active_users", "grain": "hour", "cashu_token": TOKEN})
    assert "day | week | month" in str(exc.value)
    assert stub_paid["redeem"] == []  # validation precedes payment
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "invalid_grain"


def test_kill_switch_refuses_at_call_time(stub_paid, monkeypatch, tmp_path):
    monkeypatch.setenv("NOSTROLOGY_MCP_NUTZAPS_ENABLED", "0")
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    assert "temporarily disabled" in result.content[0].text
    assert "no payment was taken" in result.content[0].text
    assert stub_paid["redeem"] == []
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "disabled"


# -- outcome mapping: already spent, undelivered and unpaid stay distinct ---------


def _redeem_stub(monkeypatch, tmp_path, outcome):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(cashu, "redeem", lambda token, ip: dict(outcome))
    monkeypatch.setattr(
        srv_mod, "fetch_metric", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no DB on this path"))
    )
    reports = []
    monkeypatch.setattr(cashu, "report_outcome", lambda rid, delivered, note: reports.append((rid, delivered, note)))
    return reports


@pytest.mark.parametrize(
    "outcome, fragment, logged",
    [
        (
            {"outcome": "structural", "reason": "amount_mismatch", "message": "the token must carry exactly 21 + swap_fee sats."},
            "rejected before reaching the mint (amount_mismatch)",
            "payment_invalid:amount_mismatch",
        ),
        ({"outcome": "rate_limited", "message": "m"}, "too many payment attempts", "payment_rate_limited"),
        ({"outcome": "mint_unreachable", "message": "m"}, "was not consumed", "mint_unreachable"),
        ({"outcome": "store_unavailable", "message": "m"}, "not consumed", "payment_store_unavailable"),
    ],
)
def test_no_charge_outcomes_say_the_token_survived(monkeypatch, tmp_path, outcome, fragment, logged):
    _redeem_stub(monkeypatch, tmp_path, outcome)
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    assert fragment in result.content[0].text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == logged


def test_structural_rejection_re_presents_the_terms(monkeypatch, tmp_path):
    _redeem_stub(monkeypatch, tmp_path, {"outcome": "structural", "reason": "unit_mismatch", "message": "only sat."})
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    # Paying correctly is the right next step, so the terms ride along again.
    assert "Payment required" in result.content[0].text
    assert result.structured_content["paymentRequest"].startswith("creqA")


def test_already_spent_says_do_not_pay_again_and_quotes_the_receipt(monkeypatch, tmp_path):
    _redeem_stub(monkeypatch, tmp_path, {"outcome": "already_spent", "message": "m", "id": cashu.token_hash(TOKEN)})
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "ALREADY SPENT" in text
    assert f"sha256={cashu.token_hash(TOKEN)}" in text
    # The distinct-discriminator rule: never the unpaid framing.
    assert "Payment required" not in text


def test_already_spent_quotes_the_redeemer_on_WHO_refused(monkeypatch, tmp_path):
    """The three already-spent paths differ in who said no - our redemption
    record, the mint's 11001, or a checkstate showing the proofs spent
    elsewhere - and the redeemer says which in `message`. The tool must quote
    that, not assert 'the mint refused': for a replay the mint would happily
    have answered from its NUT-19 cache, so OUR store is what stopped the
    second delivery, and claiming otherwise misattributes the refusal."""
    ours = "this token was already redeemed, and the data it paid for was delivered."
    _redeem_stub(
        monkeypatch, tmp_path,
        {"outcome": "already_spent", "message": ours, "id": cashu.token_hash(TOKEN)},
    )
    text = _call({"metric": "active_users", "cashu_token": TOKEN}).content[0].text
    assert ours in text
    assert "the mint refused" not in text
    # Nor "redeemed HERE": the mint is the replay-protection state,
    # and claiming to be the authority on spentness is exactly
    # the design position we do not hold.
    assert "redeemed here" not in text

    # ... and when the MINT is genuinely the refuser, that is what it says.
    mints = "the mint refused this token as already spent."
    _redeem_stub(
        monkeypatch, tmp_path,
        {"outcome": "already_spent", "message": mints, "id": cashu.token_hash(TOKEN)},
    )
    text = _call({"metric": "active_users", "cashu_token": TOKEN}).content[0].text
    assert mints in text


def test_unresolved_says_do_not_pay_again(monkeypatch, tmp_path):
    _redeem_stub(monkeypatch, tmp_path, {"outcome": "unresolved", "message": "m", "id": "x"})
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    assert "Do NOT pay again" in result.content[0].text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "payment_unresolved"


def test_persist_failed_is_the_delivery_failed_class_with_receipt(monkeypatch, tmp_path):
    reports = _redeem_stub(
        monkeypatch,
        tmp_path,
        {"outcome": "persist_failed", "message": "m", "receipt": dict(RECEIPT)},
    )
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "payment WAS received" in text
    assert f"sha256={RECEIPT['id']}" in text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "delivery_failed_after_payment"
    # Review finding: this paid-but-undelivered path must feed the audit
    # record / delivery-failed alert from this side too.
    assert reports == [(RECEIPT["id"], False, "persist_failed")]


def test_mint_rejected_is_a_settled_refusal_without_support_framing(monkeypatch, tmp_path):
    """A fabricated-secrets token gets a settled 4xx from the mint: nothing
    was spent, so the answer must NOT be unresolved-shaped and must not hand
    the caller a support-claim reference (review finding: that framing plus
    the unresolved alarm made free junk tokens an alert-flooding primitive)."""
    _redeem_stub(
        monkeypatch,
        tmp_path,
        {"outcome": "mint_rejected", "message": "the mint rejected this token as invalid (mint error code 10003); nothing was spent.", "id": "x"},
    )
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "nothing was spent" in text
    assert "support claim" not in text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "payment_mint_rejected"


def test_indeterminate_never_claims_the_token_is_intact(monkeypatch, tmp_path):
    """The review's blocker, tool-side: a timeout mid-redemption must say the
    state is UNKNOWN and steer to retrying the SAME token (which the sidecar's
    resume-deliver path answers correctly either way) - never 'token intact',
    never 'pay again'."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        cashu, "redeem", lambda token, ip: (_ for _ in ()).throw(cashu.RedeemerIndeterminate("TimeoutError"))
    )
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "UNKNOWN state" in text
    assert "SAME cashu_token" in text
    assert "Do NOT pay with a new token" in text
    assert "remains spendable" not in text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "payment_indeterminate"


def test_credential_schema_advertises_the_hard_cap(monkeypatch):
    """The literal requirement: an explicit maxLength on
    the cashu_token schema (review finding: the comments claimed it while the
    schema lacked it)."""
    async def go():
        return await srv_mod.server.list_tools()

    (tool,) = [t for t in anyio.run(go) if t.name == "nostrology_query_nutzaps"]
    branches = tool.input_schema["properties"]["cashu_token"]["anyOf"]
    (value_branch,) = [b for b in branches if b.get("type") == "string"]
    assert value_branch["maxLength"] == cashu.MAX_TOKEN_CHARS


def test_non_ascii_credential_is_refused_before_the_sidecar(stub_paid, tmp_path):
    """Chars alone do not bound the serialized body (a \\uXXXX escape is 6
    bytes per char); the ASCII requirement is what makes the char cap a byte
    cap. Every legal credential is ASCII."""
    result = _call({"metric": "active_users", "cashu_token": "cashuB" + "\u00e9" * 10})
    assert result.is_error
    assert "non-ASCII" in result.content[0].text
    assert stub_paid["redeem"] == []
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "token_too_long"


def test_redeemer_unreachable_says_token_intact(monkeypatch, tmp_path):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(
        cashu, "redeem", lambda token, ip: (_ for _ in ()).throw(cashu.RedeemerUnreachable("ConnectionRefusedError"))
    )
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    assert "NOT presented to a mint" in result.content[0].text
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "redeemer_unreachable"


# -- the paid path ----------------------------------------------------------------


def test_redeemed_call_queries_renders_and_reports_delivered(stub_paid, tmp_path):
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    text = result.content[0].text
    preamble, table = text.split("\n\n", 1)
    # The envelope: identity, window, freshness, columns, methodology - and
    # the RECEIPT AS A TEXT LINE (the success-path rule: this is the
    # only channel the model sees on a success WITHOUT structuredContent, and
    # adding structuredContent would suppress the whole prose envelope).
    assert "metric: nutzaps.active_users  |  grain: month" in preamble
    assert "additive_over_time: false" in preamble
    # RECEIPT's mint is the UNADVERTISED one, so the line describes it rather
    # than naming it - see the pair of tests below, which own that rule.
    assert "payment: redeemed 21 sat via an accepted mint" in preamble
    assert f"receipt sha256={RECEIPT['id']}" in preamble
    assert table.splitlines()[0] == "period_start,unique_users"
    assert "2026-07-01,1234" in table
    # Payment came BEFORE the query (redeem, then query).
    assert [k for k, _ in stub_paid["fetch"]] == ["nutzaps.active_users@month"]
    assert stub_paid["redeem"][0][0] == TOKEN
    # The delivery outcome went back to the sidecar as the audit record.
    assert stub_paid["outcome"] == [(RECEIPT["id"], True, None)]
    # And the machine-readable receipt rides in _meta - safe on a success
    # precisely because _meta can never displace the text (measured).
    assert result.meta["ai.nostrolo.gy/payment-receipt"]["id"] == RECEIPT["id"]


@pytest.fixture
def pinned_mint_env(monkeypatch):
    """Both mint vars at their defaults. Without this, these tests read the
    ambient env: under a CASHU_MINTS that omits testnut, the never-names test
    would pass through the UNKNOWN-mint fallback instead of the unadvertised-
    list path - green without exercising what it was written for - and
    advertised_mint_urls()[0] would IndexError under an env that hides
    everything."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)


def test_the_paid_SUCCESS_envelope_never_names_the_hidden_mint(stub_paid, pinned_mint_env):
    """The leak the live run-through found: the challenge was scrubbed but the
    success envelope echoed `receipt.mint` straight back, so the paid path
    published the hidden mint - and published it precisely during the FREE
    REHEARSAL, the one run guaranteed to use it, into a terminal that gets
    screen-shared and pasted. Both wire channels, since _meta reaches SDK
    clients even though it never reaches a model."""
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    text = result.content[0].text
    meta = result.meta["ai.nostrolo.gy/payment-receipt"]

    for hidden in cashu.unadvertised_mint_urls():
        assert hidden.split("//", 1)[-1] not in text
        assert hidden not in json.dumps(meta)
    # Described, not silently dropped - the payer still learns the payment
    # settled, and the receipt (the only thing a support claim needs) is intact.
    assert "payment: redeemed 21 sat via an accepted mint" in text
    assert meta["id"] == RECEIPT["id"]
    assert "mint" not in meta
    # fee_sats goes too: it is 1 for the test mint and 0 for the advertised
    # one, so on a receipt whose absent `mint` key already says "unadvertised",
    # it is the remaining one-field fingerprint.
    assert "fee_sats" not in meta
    assert meta["amount_sats"] == 21


def test_the_paid_success_envelope_DOES_name_an_advertised_mint(monkeypatch, tmp_path, pinned_mint_env):
    """The other half, so the rule above cannot be satisfied by never naming a
    mint at all: a real Minibits payment says so, in both channels."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    advertised = cashu.advertised_mint_urls()[0]
    receipt = dict(RECEIPT, mint=advertised)
    monkeypatch.setattr(cashu, "redeem", lambda t, ip: {"outcome": "redeemed", "receipt": receipt})
    monkeypatch.setattr(cashu, "report_outcome", lambda *a, **k: None)
    monkeypatch.setattr(
        srv_mod,
        "fetch_metric",
        lambda spec, **kw: MetricResult(rows=list(ROWS), as_of_epoch=AS_OF, connect_ms=1, query_ms=2),
    )

    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    assert f"payment: redeemed 21 sat via {advertised}" in result.content[0].text
    meta = result.meta["ai.nostrolo.gy/payment-receipt"]
    assert meta["mint"] == advertised
    # Nothing is dropped when there is nothing to hide.
    assert meta["fee_sats"] == RECEIPT["fee_sats"]


def test_a_malformed_mint_cannot_turn_a_PAID_call_into_an_exception(monkeypatch, tmp_path, stub_paid):
    """The scrub put URL parsing on the path AFTER the money moved, and
    urlsplit(...).port raises on a non-numeric port. An unhandled raise there
    is the worst failure in the design: the caller pays, gets a bare protocol
    error, and report_outcome() never runs - so neither the delivery-audit
    record nor the delivery_failed alert fires. Belt (total _canonical_mint)
    and braces (the handler around the whole render) are both pinned here."""
    monkeypatch.setenv("CASHU_MINTS", "https://mint.minibits.cash:notaport/Bitcoin")
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    text = result.content[0].text
    # Unparseable means unnameable, so it is described, not named.
    assert "payment: redeemed 21 sat via an accepted mint" in text
    assert "notaport" not in text
    # And the delivery was still reported - the audit trail is intact.
    assert stub_paid["outcome"] == [(RECEIPT["id"], True, None)]


def test_a_render_failure_after_payment_is_audited_not_raised(monkeypatch, tmp_path, stub_paid):
    """The braces on their own: whatever else goes wrong between a taken
    payment and the returned data, the caller must get the receipt and the
    sidecar must get delivered=False."""
    def boom(*a, **kw):
        raise RuntimeError("renderer exploded")

    monkeypatch.setattr(srv_mod, "render_csv", boom)
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "payment WAS received" in text
    assert f"sha256={RECEIPT['id']}" in text
    assert "Do NOT pay again" in text
    assert stub_paid["outcome"] == [(RECEIPT["id"], False, "render_error")]
    assert _records(tmp_path)[-1]["outcome"] == "delivery_failed_after_payment"


def test_no_structured_content_on_the_paid_success(stub_paid):
    """Channel rule 2 (verification/cashu_probe.py), the live regression risk: structuredContent on a
    success makes the model stop seeing the prose envelope entirely. Pin its
    ABSENCE so nobody adds it for SDK ergonomics."""
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert not result.is_error
    assert result.structured_content is None


def test_window_flows_into_the_paid_query(stub_paid):
    result = _call({"metric": "active_users", "grain": "day", "from": "2026-07-01", "to": "2026-07-31", "cashu_token": TOKEN})
    assert not result.is_error
    key, binds = stub_paid["fetch"][0]
    assert key == "nutzaps.active_users@day"
    assert binds["granularity"] == "day"
    assert binds["from_epoch"] == 1782864000  # 2026-07-01T00:00:00Z
    assert binds["to_epoch_excl"] == 1785542400  # 2026-08-01T00:00:00Z
    assert "window: 2026-07-01..2026-07-31" in result.content[0].text


def test_db_failure_after_payment_reports_delivery_failed(monkeypatch, tmp_path):
    """The succeeded-then-delivery-failed case: payment taken, DB
    down. The caller gets the receipt and 'do not pay again'; the sidecar gets
    delivered=False (which is what feeds the one alert that matters most)."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    outcomes = []
    monkeypatch.setattr(cashu, "redeem", lambda token, ip: {"outcome": "redeemed", "receipt": dict(RECEIPT)})
    monkeypatch.setattr(cashu, "report_outcome", lambda rid, delivered, note: outcomes.append((rid, delivered, note)))
    monkeypatch.setattr(
        srv_mod, "fetch_metric", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("connection refused"))
    )
    result = _call({"metric": "active_users", "cashu_token": TOKEN})
    assert result.is_error
    text = result.content[0].text
    assert "payment WAS received" in text
    assert f"sha256={RECEIPT['id']}" in text
    assert "Do NOT pay again" in text
    assert outcomes == [(RECEIPT["id"], False, "db_error")]
    (rec,) = _records(tmp_path)
    assert rec["outcome"] == "delivery_failed_after_payment"
