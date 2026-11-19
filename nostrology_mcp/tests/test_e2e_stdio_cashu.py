"""End-to-end channel-integrity tests for the paid tool: the REAL Python MCP
SDK client (`mcp` on PyPI) driving the REAL server as a subprocess over stdio -
layer 2 of three, which checks that a client receives every channel intact.

Layer 1 (test_server_nutzaps.py) asserts what the server EMITS in-process;
this file asserts what a CLIENT RECEIVES after the SDK serializes, frames and
parses both sides. Layer 3 (what the MODEL sees) is not automatable and is
covered by the committed probe fixture (verification/cashu_probe.py).

The subprocess gets a fake sidecar (a local HTTP stub with canned outcomes)
and a dead DB port, so every path here is DB-free or deliberately DB-failing;
the true-DB paid path is covered in-process with a stubbed fetch."""

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import anyio
import pytest

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from nostrology_mcp import cashu

TOKEN = "cashuB" + "o2FtdWNzYXR4" * 20


class _CannedRedeemer(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length", 0))
        self.rfile.read(length)
        payload = json.dumps(self.server.canned).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


@pytest.fixture
def canned_redeemer():
    server = HTTPServer(("127.0.0.1", 0), _CannedRedeemer)
    server.canned = {}
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()


def _dead_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _call_over_stdio(tmp_path, redeemer_port, arguments):
    """One initialize + tools/call round trip through the real SDK stack."""
    import os
    import sys

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "nostrology_mcp.server"],
        env={
            **os.environ,
            "NOSTROLOGY_MCP_LOG_DIR": str(tmp_path / "log"),
            "NOSTROLOGY_MCP_LOG_STDOUT": "0",  # stdout IS the protocol channel
            "NOSTROLOGY_MCP_CASHU_REDEEMER_URL": f"http://127.0.0.1:{redeemer_port}",
            # A dead DB, failing fast: connection refused beats a 90s timeout.
            "NOSTROLOGY_PGHOST": "127.0.0.1",
            "NOSTROLOGY_PGPORT": str(_dead_port()),
            "NOSTROLOGY_PGCONNECT_TIMEOUT": "2",
        },
    )

    async def go():
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool("nostrology_query_nutzaps", arguments)

    return anyio.run(go)


def test_challenge_arrives_intact_at_the_sdk_client(tmp_path, canned_redeemer):
    """The challenge's BOTH channels must survive the wire: the text (the only
    model-visible channel) and structuredContent (the programmatic channel
    carrying the creqA). A regression in either is exactly the class of silent
    channel loss the challenge's design was written against."""
    result = _call_over_stdio(tmp_path, canned_redeemer.server_address[1], {"metric": "active_users"})
    assert result.is_error is True
    text = result.content[0].text
    assert "Payment required" in text
    assert "21 sat" in text
    assert "cashu_token" in text
    assert result.structured_content is not None
    assert result.structured_content["amount"] == 21
    assert result.structured_content["paymentRequest"].startswith("creqA")
    # The creqA decodes as CBOR whose map carries the advertised amount - the
    # client-side half of the rule that only the envelope is ours: the terms
    # travel as a standard NUT-18 payment request.
    import base64

    blob = base64.b64decode(result.structured_content["paymentRequest"][len("creqA"):])
    assert blob[0] == 0xA6  # definite-length map of 6 entries


def test_already_spent_verdict_arrives_with_the_receipt_reference(tmp_path, canned_redeemer):
    canned_redeemer.canned = {"outcome": "already_spent", "message": "m", "id": cashu.token_hash(TOKEN)}
    result = _call_over_stdio(
        tmp_path, canned_redeemer.server_address[1], {"metric": "active_users", "cashu_token": TOKEN}
    )
    assert result.is_error is True
    text = result.content[0].text
    assert "ALREADY SPENT" in text
    assert f"sha256={cashu.token_hash(TOKEN)}" in text


def test_paid_but_db_down_arrives_as_delivery_failed_with_receipt(tmp_path, canned_redeemer):
    """The full succeeded-then-delivery-failed path over the real stack: the
    fake sidecar redeems, the DB refuses, and the CLIENT receives the
    delivery-failed text with the receipt reference - never a pay-again."""
    receipt_id = cashu.token_hash(TOKEN)
    canned_redeemer.canned = {
        "outcome": "redeemed",
        "receipt": {"id": receipt_id, "mint": "https://testnut.cashu.space", "amount_sats": 21, "fee_sats": 1, "ts": "t"},
    }
    result = _call_over_stdio(
        tmp_path, canned_redeemer.server_address[1], {"metric": "active_users", "cashu_token": TOKEN}
    )
    assert result.is_error is True
    text = result.content[0].text
    assert "payment WAS received" in text
    assert f"sha256={receipt_id}" in text
    assert "Do NOT pay again" in text


# -- the full-stack live gate: the demo, as a test --------------------------------

_CASHU_LIVE = pytest.mark.skipif(
    not __import__("os").environ.get("NOSTROLOGY_TEST_CASHU_LIVE"),
    reason=(
        "full-stack live gate: set NOSTROLOGY_TEST_CASHU_LIVE=1 (needs node >= 22, "
        "network to testnut.cashu.space, and the Aurora tunnel on localhost:15432)"
    ),
)


@_CASHU_LIVE
def test_live_full_stack_pay_query_replay(tmp_path):
    """The paid tool's demo, as an unattended test: a REAL node cashu-redeemer
    subprocess, a REAL token minted at testnut (FakeWallet - no sats), the REAL
    MCP server over stdio reading the REAL Aurora table through the tunnel.
    Challenge -> pay -> replay-refused."""
    import os
    import subprocess
    import sys
    import time
    import urllib.request

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    redeemer_dir = os.path.join(repo_root, "cashu_redeemer")

    def free_port() -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    port = free_port()
    sidecar = subprocess.Popen(
        ["node", "src/server.ts"],
        cwd=redeemer_dir,
        env={
            **os.environ,
            "CASHU_PORT": str(port),
            "CASHU_MINTS": "https://testnut.cashu.space",
            "CASHU_STORE": "file",
            "CASHU_STORE_DIR": str(tmp_path / "redemptions"),
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(50):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1):
                    break
            except Exception:
                time.sleep(0.2)
        else:
            raise AssertionError(f"sidecar never became healthy: {sidecar.stderr.read()!r}")

        # Mint a real exact-amount token at testnut via the sidecar's own
        # test helper (the FakeWallet marks the invoice paid instantly).
        minted = subprocess.run(
            [
                "node",
                "--input-type=module",
                "-e",
                "import {mintExactPaymentToken} from "
                f"'file://{redeemer_dir}/test/helpers.ts';"
                "console.log(await mintExactPaymentToken(21));",
            ],
            cwd=redeemer_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert minted.returncode == 0, minted.stderr[-800:]
        token = minted.stdout.strip().splitlines()[-1]
        assert token.startswith("cashuB")

        server_env = {
            "NOSTROLOGY_MCP_LOG_DIR": str(tmp_path / "log"),
            "NOSTROLOGY_MCP_LOG_STDOUT": "0",
            "NOSTROLOGY_MCP_CASHU_REDEEMER_URL": f"http://127.0.0.1:{port}",
            "CASHU_MINTS": "https://testnut.cashu.space",
            # The Aurora tunnel (localhost:15432, password via ~/.pgpass).
            "NOSTROLOGY_PGHOST": "localhost",
            "NOSTROLOGY_PGPORT": "15432",
            "NOSTROLOGY_PGUSER": "nostrology_ro",
        }
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "nostrology_mcp.server"],
            env={**os.environ, **server_env},
        )

        async def run_demo():
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    challenge = await session.call_tool(
                        "nostrology_query_nutzaps", {"metric": "active_users"}
                    )
                    paid = await session.call_tool(
                        "nostrology_query_nutzaps",
                        {"metric": "active_users", "cashu_token": token},
                    )
                    replay = await session.call_tool(
                        "nostrology_query_nutzaps",
                        {"metric": "active_users", "cashu_token": token},
                    )
                    return challenge, paid, replay

        challenge, paid, replay = anyio.run(run_demo)

        # Demo step 2: the terms, free.
        assert challenge.is_error is True
        assert "Payment required" in challenge.content[0].text

        # Demo step 3: real MAU rows for real (fake-backed) ecash.
        assert paid.is_error is False, paid.content[0].text[:400]
        text = paid.content[0].text
        assert "metric: nutzaps.active_users  |  grain: month" in text
        # The mint that took the payment IS testnut here, and the envelope must
        # still not say so - this is the only test where a REAL redemption from
        # the hidden mint reaches the real renderer, so it is the strongest
        # place to pin the scrub. It is also the run that used to leak it: a
        # rehearsal prints this line to a terminal.
        assert "payment: redeemed 21 sat via an accepted mint" in text
        assert "testnut" not in text
        assert f"receipt sha256={cashu.token_hash(token)}" in text
        header = text.split("\n\n", 1)[1].splitlines()[0]
        assert header == "period_start,unique_users"
        # Live data: at least the 2023..now months the table already holds.
        assert len(text.split("\n\n", 1)[1].splitlines()) > 10

        # Demo step 4: the same token again is refused as spent - the step
        # that proves redemption was enforced, and it must come last because
        # it burns the token.
        assert replay.is_error is True
        assert "ALREADY SPENT" in replay.content[0].text

        # And the redemption store holds all three objects incl. the
        # delivered=true outcome record.
        rid = cashu.token_hash(token)
        base = tmp_path / "redemptions" / rid
        assert (base / "outputs.json").exists()
        assert (base / "signatures.json").exists()
        outcome = json.loads((base / "outcome.json").read_text())
        assert outcome["delivered"] is True
    finally:
        sidecar.terminate()
        sidecar.wait(timeout=10)
