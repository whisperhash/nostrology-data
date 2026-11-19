"""Tests for the cashu module: the creqA encoder (pinned byte-for-byte against
cashu-ts), the receipt hash (pinned against the sidecar), the shared-config
defaults (pinned against the sidecar's), and the sidecar HTTP client's failure
taxonomy (against a live local stub, not mocks of urllib)."""

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from nostrology_mcp import cashu


# -- creqA ------------------------------------------------------------------------


def test_creqa_matches_the_cashu_ts_reference_bytes():
    """Byte-identical to what cashu-ts 4.8.0's PaymentRequest.toEncodedCreqA()
    emits for the same fields (captured 2026-08-13 from the vendored library;
    cashu_redeemer's suite decodes our output with the real library, this pin
    catches OUR encoder drifting). Field order i,a,u,m,d,s; standard base64
    with padding."""
    encoded = cashu.encode_creqa(
        21,
        "sat",
        ["https://mint.minibits.cash/Bitcoin", "https://testnut.cashu.space"],
        "nutzaps-v0",
        "nostrology_query_nutzaps: 21 sat per call",
    )
    assert encoded == (
        "creqApmFpam51dHphcHMtdjBhYRVhdWNzYXRhbYJ4Imh0dHBzOi8vbWludC5taW5pYml0"
        "cy5jYXNoL0JpdGNvaW54G2h0dHBzOi8vdGVzdG51dC5jYXNodS5zcGFjZWFkeClub3N0"
        "cm9sb2d5X3F1ZXJ5X251dHphcHM6IDIxIHNhdCBwZXIgY2FsbGFz9Q=="
    )


def test_creqa_cbor_is_decodable_and_carries_the_terms():
    """Independent structural decode (stdlib only): the payload after 'creqA'
    is definite-length CBOR whose map carries our exact terms. Not a CBOR
    library round-trip - a tiny hand decoder for the shapes we emit, so this
    test cannot share a bug with the encoder's own helper."""
    encoded = cashu.encode_creqa(21, "sat", ["https://m.example"], "id-1", "d")
    blob = base64.b64decode(encoded[len("creqA"):])

    def rd(buf, pos):
        head = buf[pos]
        major, arg = head >> 5, head & 0x1F
        pos += 1
        if arg == 24:
            arg, pos = buf[pos], pos + 1
        elif arg >= 25:
            width = 2 ** (arg - 24)
            arg, pos = int.from_bytes(buf[pos : pos + width], "big"), pos + width
        if major == 0:
            return arg, pos
        if major == 3:
            return buf[pos : pos + arg].decode(), pos + arg
        if major == 4:
            items = []
            for _ in range(arg):
                item, pos = rd(buf, pos)
                items.append(item)
            return items, pos
        if major == 5:
            out = {}
            for _ in range(arg):
                k, pos = rd(buf, pos)
                v, pos = rd(buf, pos)
                out[k] = v
            return out, pos
        if major == 7:
            return {20: False, 21: True}[arg], pos
        raise AssertionError(f"unexpected major {major}")

    decoded, end = rd(blob, 0)
    assert end == len(blob)  # no trailing bytes
    assert decoded == {
        "i": "id-1",
        "a": 21,
        "u": "sat",
        "m": ["https://m.example"],
        "d": "d",
        "s": True,
    }


def test_cbor_encoder_rejects_what_a_payment_request_never_needs():
    with pytest.raises(ValueError):
        cashu._cbor(-1)
    with pytest.raises(TypeError):
        cashu._cbor(3.14)
    with pytest.raises(TypeError):
        cashu._cbor(None)


def test_cbor_length_encodings_cross_the_width_boundaries():
    # 23 -> single byte; 24 -> 1-byte arg; 256 -> 2-byte arg.
    assert cashu._cbor(23) == b"\x17"
    assert cashu._cbor(24) == b"\x18\x18"
    assert cashu._cbor(256) == b"\x19\x01\x00"
    assert cashu._cbor("x" * 24).startswith(b"\x78\x18")


# -- shared config ----------------------------------------------------------------


def test_defaults_match_the_sidecar(monkeypatch):
    """Price and mint list are ONE config shared with cashu_redeemer (the
    compose sets CASHU_PRICE_SATS / CASHU_MINTS on both containers). These
    literals mirror cashu_redeemer/src/config.ts's defaults; a drift means the
    challenge advertises terms the redeemer refuses."""
    monkeypatch.delenv("CASHU_PRICE_SATS", raising=False)
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    assert cashu.price_sats() == 21
    assert cashu.mint_urls() == [
        "https://mint.minibits.cash/Bitcoin",
        "https://testnut.cashu.space",
    ]
    assert cashu.UNIT == "sat"


def test_token_hash_pins_the_sidecar_vector():
    """Same vector as cashu_redeemer/test/unit.test.ts's tokenHash pin - the
    receipt reference the MCP text quotes and the redemption store's object key
    prefix must be the same hash of the same trimmed string."""
    assert (
        cashu.token_hash("  cashuBpin-vector  ")
        == "e393fa9463b78c9ddb52391fb1bcad7ee40d862ec3722698bb048577b35e376a"
    )


# -- the accept list vs the advertise list ----------------------------------------
#
# testnut.cashu.space is a FakeWallet mint: its tokens cost nothing to mint,
# which is exactly why we accept it (it exercises the real payment path against
# the live service without spending sats) and exactly why we must never publish
# it. Naming it in the challenge would hand every caller a free-entry side door
# into a paid tool. So: accepted, never advertised - and these tests are what
# keep those two lists from collapsing back into one.


def test_the_hidden_mint_is_accepted_but_not_advertised(monkeypatch):
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    # Still ACCEPTED: this list is the one the sidecar enforces, so a testnut
    # token keeps redeeming.
    assert "https://testnut.cashu.space" in cashu.mint_urls()
    # Never ADVERTISED.
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]


@pytest.mark.parametrize(
    "written_as",
    [
        "https://testnut.cashu.space",
        "https://testnut.cashu.space/",       # trailing slash
        "https://TestNut.Cashu.Space",        # host case
        "HTTPS://testnut.cashu.space",        # scheme case
        "https://testnut.cashu.space:443",    # default port
        "https://testnut.cashu.space///",     # several trailing slashes
        # The four below do NOT reduce to the canonical form, which is why an
        # earlier version of this list - covering only the six above - read as
        # thorough and was not: every one of these was published verbatim,
        # because the accept list is where the published string comes from.
        # The realistic trigger is the documented override path, an operator
        # appending a hand-typed CASHU_MINTS line to deploy/.env.
        "testnut.cashu.space",                # no scheme
        "//testnut.cashu.space",              # scheme-relative
        "https://testnut.cashu.space.",       # FQDN trailing dot
        "https://testnut.cashu.space:8443",   # non-default port
        "http://testnut.cashu.space",         # plain http
    ],
)
def test_hiding_survives_however_the_mint_url_was_written(monkeypatch, written_as):
    """Hiding compares on HOST, not on the sidecar's exact canonical form, and
    that difference is deliberate. Acceptance must be exact - the match decides
    whether money moves. Hiding must be conservative, because its failure
    direction is PUBLICATION, which cannot be taken back: a different port or a
    trailing dot is the same operator's FakeWallet mint, and naming either one
    publishes the host we are hiding. Anything unparseable is hidden too, since
    a URL we cannot parse is one we cannot name safely."""
    monkeypatch.setenv("CASHU_MINTS", f"https://mint.minibits.cash/Bitcoin,{written_as}")
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]


def test_a_malformed_mint_url_never_raises_and_is_never_advertised(monkeypatch):
    """_canonical_mint runs AFTER the money moves (the receipt scrub), so it
    must be total: urlsplit(...).port raises ValueError on a non-numeric port,
    and one bad CASHU_MINTS entry would otherwise take out the challenge AND
    turn a paid call into an unhandled exception with no audit record."""
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    for junk in ["https://h:notaport/x", "https://h:99999999/x", "", "not a url", "ftp://h/x"]:
        assert cashu._canonical_mint(junk) is None
        assert cashu.is_advertised_mint(junk) is False
        assert cashu.public_mint_label(junk) == "an accepted mint"

    monkeypatch.setenv("CASHU_MINTS", "https://mint.minibits.cash/Bitcoin,https://h:notaport/x")
    assert cashu.challenge_text()  # does not raise
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]


def test_a_typo_in_the_hidden_list_still_hides_its_own_spelling(monkeypatch):
    """The hide list is config too. If someone writes an unparseable entry
    there, the mint it was meant to hide must not become public as a side
    effect of the typo - the exact spelling still matches."""
    monkeypatch.setenv("CASHU_MINTS", "https://mint.minibits.cash/Bitcoin,htps://typo.mint")
    monkeypatch.setenv("CASHU_UNADVERTISED_MINTS", "htps://typo.mint")
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]


def test_an_empty_unadvertised_var_does_not_mean_hide_nothing(monkeypatch):
    """A SET BUT EMPTY var - `docker run -e CASHU_UNADVERTISED_MINTS=`, a stray
    .env line - used to republish the test mint, because get(k, default)
    returns the empty string rather than the default. Fail-toward-publication
    in the one module whose stated rule is the opposite."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.setenv("CASHU_UNADVERTISED_MINTS", "")
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]
    monkeypatch.setenv("CASHU_UNADVERTISED_MINTS", "   ")
    assert cashu.advertised_mint_urls() == ["https://mint.minibits.cash/Bitcoin"]


def test_a_single_advertised_mint_is_not_called_one_of_these(monkeypatch):
    """"one of these mints: <one URL>" reads as though something was elided
    from a paid tool's terms - the one place a reader may assume nothing was."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    text = cashu.challenge_text()
    assert "this mint: https://mint.minibits.cash/Bitcoin" in text
    assert "one of these mints" not in text

    monkeypatch.setenv("CASHU_MINTS", "https://a.test/m,https://b.test/m")
    assert "one of these mints: https://a.test/m, https://b.test/m" in cashu.challenge_text()


def test_path_case_still_distinguishes_mints(monkeypatch):
    """Canonicalization lowercases scheme and host but NOT the path: Minibits
    serves /Bitcoin, and a mint at /bitcoin would be a different mint. Hiding
    must not become a prefix match that swallows a real one."""
    monkeypatch.setenv("CASHU_MINTS", "https://example.test/Bitcoin,https://example.test/bitcoin")
    monkeypatch.setenv("CASHU_UNADVERTISED_MINTS", "https://example.test/bitcoin")
    assert cashu.advertised_mint_urls() == ["https://example.test/Bitcoin"]


def test_no_caller_facing_surface_names_the_hidden_mint(monkeypatch):
    """The three places a mint list reaches a caller: the challenge text (all
    a model sees), structuredContent.mints, and the creqA payment request a
    wallet decodes. All three must be built from the advertise list."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    monkeypatch.delenv("CASHU_PRICE_SATS", raising=False)

    text = cashu.challenge_text()
    structured = cashu.challenge_structured()
    # The creqA is base64 CBOR - the mint URL would be inside it, unreadable in
    # the encoded string, so decode rather than substring-search the blob.
    payload = base64.b64decode(structured["paymentRequest"][len("creqA"):])

    assert "testnut" not in text
    assert structured["mints"] == ["https://mint.minibits.cash/Bitcoin"]
    assert b"testnut" not in payload
    # ... and the advertised mint IS still there, in all three.
    assert "mint.minibits.cash/Bitcoin" in text
    assert b"mint.minibits.cash/Bitcoin" in payload


@pytest.mark.parametrize(
    "written_as",
    [
        "https://testnut.cashu.space",
        "https://testnut.cashu.space/",
        "https://TestNut.Cashu.Space:443",
    ],
)
def test_a_hidden_mint_is_described_not_named_on_the_paid_path(monkeypatch, written_as):
    """The mint a payment CAME FROM is a fourth caller-facing surface, found by
    running the demo rather than by reading the code: the success envelope's
    `payment:` line. Same canonical compare as the challenge, because the
    receipt's spelling comes from the sidecar, not from our config."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    assert cashu.public_mint_label(written_as) == "an accepted mint"
    assert cashu.scrub_receipt({"id": "abc", "mint": written_as}) == {"id": "abc"}


def test_an_advertised_mint_is_named_on_the_paid_path(monkeypatch):
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    mint = "https://mint.minibits.cash/Bitcoin"
    assert cashu.public_mint_label(mint) == mint
    assert cashu.scrub_receipt({"id": "abc", "mint": mint})["mint"] == mint


@pytest.mark.parametrize("unknown", [None, "", "https://some.other.mint/x"])
def test_an_unknown_mint_fails_toward_hiding(monkeypatch, unknown):
    """A mint that is not on the accept list at all cannot have redeemed
    anything - so if one ever appears here, something upstream is wrong and the
    safe answer is silence. Publication is the irreversible direction."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    assert cashu.is_advertised_mint(unknown) is False
    assert cashu.public_mint_label(unknown) == "an accepted mint"


def test_scrub_receipt_does_not_mutate_the_sidecars_dict(monkeypatch):
    """The unscrubbed receipt is still what the delivery-audit path reports on;
    scrubbing is a wire-copy concern only."""
    monkeypatch.delenv("CASHU_UNADVERTISED_MINTS", raising=False)
    original = {"id": "abc", "mint": "https://testnut.cashu.space", "amount_sats": 21}
    scrubbed = cashu.scrub_receipt(original)
    assert original["mint"] == "https://testnut.cashu.space"
    assert "mint" not in scrubbed
    assert scrubbed["amount_sats"] == 21


def test_hiding_every_mint_degrades_the_sentence_rather_than_leaking(monkeypatch):
    """If the advertise list empties - a misconfiguration, or the day the last
    public mint is retired - the text must not render "these mints: ." and must
    certainly not fall back to the accept list."""
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    monkeypatch.setenv(
        "CASHU_UNADVERTISED_MINTS",
        "https://testnut.cashu.space,https://mint.minibits.cash/Bitcoin",
    )
    text = cashu.challenge_text()
    assert cashu.advertised_mint_urls() == []
    assert "an accepted Cashu mint" in text
    assert "these mints" not in text
    assert "testnut" not in text and "minibits" not in text


def test_challenge_text_carries_everything_the_model_needs(monkeypatch):
    """On an error result the model sees ONLY the text - so price,
    unit, mints and the argument name must all be IN it."""
    monkeypatch.delenv("CASHU_PRICE_SATS", raising=False)
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    text = cashu.challenge_text()
    assert "21 sat" in text
    assert "mint.minibits.cash/Bitcoin" in text
    assert "cashu_token" in text
    assert "single-use" in text


def test_challenge_structured_carries_a_real_creqa(monkeypatch):
    monkeypatch.delenv("CASHU_PRICE_SATS", raising=False)
    monkeypatch.delenv("CASHU_MINTS", raising=False)
    structured = cashu.challenge_structured()
    assert structured["amount"] == 21
    assert structured["unit"] == "sat"
    assert structured["paymentRequest"].startswith("creqA")
    assert structured["argument"] == "cashu_token"
    assert "cashuB-string" in structured["accepts"]


# -- the sidecar client, against a live local stub --------------------------------


class _StubRedeemer(BaseHTTPRequestHandler):
    """A minimal live sidecar: what to answer is injected per-test via the
    server object. Exercises the REAL urllib path including the HTTPError-body
    branch, which a mock of urlopen would not."""

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        self.server.requests.append((self.path, json.loads(body)))
        status, payload = self.server.responses.pop(0)
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):  # silence
        pass


@pytest.fixture
def stub_redeemer(monkeypatch):
    server = HTTPServer(("127.0.0.1", 0), _StubRedeemer)
    server.requests = []
    server.responses = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv(
        "NOSTROLOGY_MCP_CASHU_REDEEMER_URL", f"http://127.0.0.1:{server.server_address[1]}"
    )
    yield server
    server.shutdown()


def test_redeem_returns_the_outcome_and_forwards_ip(stub_redeemer):
    stub_redeemer.responses.append((200, {"outcome": "redeemed", "receipt": {"id": "ab" * 32}}))
    outcome = cashu.redeem("cashuBwhatever", "203.0.113.9")
    assert outcome["outcome"] == "redeemed"
    (path, body) = stub_redeemer.requests[0]
    assert path == "/redeem"
    assert body == {"token": "cashuBwhatever", "client_ip": "203.0.113.9"}


def test_redeem_treats_a_503_body_as_an_outcome_not_a_failure(stub_redeemer):
    """The sidecar answers 503 WITH a JSON outcome for unresolved/store states
    - that body is the answer, and raising would misreport 'do not pay again'
    as 'sidecar down, token intact', which is exactly wrong."""
    stub_redeemer.responses.append((503, {"outcome": "unresolved", "message": "m", "id": "x"}))
    outcome = cashu.redeem("cashuBwhatever", None)
    assert outcome["outcome"] == "unresolved"


def _dead_port() -> int:
    """A port that refuses connections: bind, learn the number, close. (Merely
    shutdown()-ing the stub leaves its listener OPEN, so a request would hang
    for the full client timeout instead of failing fast - measured 75s.)"""
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_redeem_failure_taxonomy_pre_send_vs_post_send(stub_redeemer, monkeypatch):
    """The review's blocker: only a PROVABLY-unsent request may claim the token
    is intact. Connection refused = Unreachable (nothing sent); everything
    after the request may have left - garbage bodies, timeouts - is
    Indeterminate (the sidecar may still be completing the payment)."""
    # A 200 with an unparseable body: the sidecar processed SOMETHING.
    stub_redeemer.responses.append((200, b"not json"))
    with pytest.raises(cashu.RedeemerIndeterminate):
        cashu.redeem("cashuBwhatever", None)
    # A JSON body with no outcome field: same.
    stub_redeemer.responses.append((200, {"weird": True}))
    with pytest.raises(cashu.RedeemerIndeterminate):
        cashu.redeem("cashuBwhatever", None)
    # Connection refused: provably nothing sent - the one Unreachable case.
    monkeypatch.setenv("NOSTROLOGY_MCP_CASHU_REDEEMER_URL", f"http://127.0.0.1:{_dead_port()}")
    with pytest.raises(cashu.RedeemerUnreachable):
        cashu.redeem("cashuBwhatever", None)


def test_redeem_timeout_is_indeterminate_not_unreachable(monkeypatch):
    """A sidecar that accepts the request and then stalls past the client
    timeout: the payment may be completing, so the classification MUST be
    Indeterminate. The timeout is env-tunable exactly so this test can run in
    a second instead of four minutes."""
    import socket as socketlib
    import threading

    listener = socketlib.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    conns = []
    threading.Thread(target=lambda: conns.append(listener.accept()), daemon=True).start()
    monkeypatch.setenv("NOSTROLOGY_MCP_CASHU_REDEEMER_URL", f"http://127.0.0.1:{port}")
    monkeypatch.setenv("NOSTROLOGY_MCP_CASHU_TIMEOUT_SECONDS", "1")
    try:
        with pytest.raises(cashu.RedeemerIndeterminate):
            cashu.redeem("cashuBwhatever", None)
    finally:
        listener.close()
        for conn, _ in conns:
            conn.close()


def test_default_timeout_covers_the_sidecar_worst_case(monkeypatch):
    """Pin the arithmetic the blocker fix rests on: sidecar worst case ~= 10
    swap attempts x 15s + backoff + restore + checkstate + loading ~= 200s.
    The client default must exceed it with headroom - anyone lowering it
    below 200 re-opens the false-'token intact' window."""
    monkeypatch.delenv("NOSTROLOGY_MCP_CASHU_TIMEOUT_SECONDS", raising=False)
    assert cashu._redeemer_timeout() >= 200


def test_report_outcome_posts_and_never_raises(stub_redeemer, monkeypatch):
    stub_redeemer.responses.append((200, {"ok": True}))
    cashu.report_outcome("ab" * 32, True, None)
    (path, body) = stub_redeemer.requests[0]
    assert path == "/redemptions/outcome"
    assert body == {"id": "ab" * 32, "delivered": True, "note": None}
    # A dead sidecar must not raise - delivery already happened; this is audit.
    monkeypatch.setenv("NOSTROLOGY_MCP_CASHU_REDEEMER_URL", f"http://127.0.0.1:{_dead_port()}")
    cashu.report_outcome("ab" * 32, False, "db_error")
