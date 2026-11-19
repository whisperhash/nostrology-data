"""The six-probe channel fixture - COMMITTED, not
throwaway: the channel rules below exist because a channel we assumed worked did not, so
this is the standing procedure that re-verifies the host-rendering layer no
automated test can reach.

QUESTION: which channels of an MCP tool response actually reach the MODEL in
Claude Code, as opposed to merely reaching the client?

PROCEDURE - re-run BY HAND after any Python MCP SDK (`mcp` on PyPI) bump or
any Claude Code upgrade:

  1. register:  claude mcp add cashu-probe --scope user -- \
                  <repo>/.venv-mcp/bin/python <repo>/nostrology_mcp/verification/cashu_probe.py
  2. restart the Claude Code session, then ask the model to call each of the
     six probe tools and report VERBATIM what it received.
  3. compare against the baseline below; any deviation reopens the channel rules.
  4. deregister:  claude mcp remove cashu-probe --scope user

MEASURED BASELINE (Claude Code + mcp 2.0.0, 2026-08-13):

  | Probe | Channels sent                  | What the model received          |
  |-------|--------------------------------|----------------------------------|
  | A     | error code, message, data      | `message` ONLY; data stripped    |
  | B     | is_error + text + structured   | text ONLY; structured stripped   |
  | C     | plain raise -> text            | text, SDK-prefixed               |
  | D     | success: text + structured     | structured ONLY; TEXT STRIPPED   |
  | E     | success: structured only       | structured                       |
  | F     | success: text + _meta          | text ONLY; _meta stripped        |

The three rules the paid tool is built on:
  1. _meta NEVER reaches the model.
  2. text and structuredContent are mutually exclusive to the model, and which
     one wins FLIPS with is_error (error: text wins; success: structured wins).
  3. error.data is unusable for anything the model must act on.

(The SDK-level wire truth - that a CLIENT receives every channel intact in
every case - is asserted automatically in tests/test_e2e_stdio_cashu.py; this
fixture covers only the model-visibility layer above it.)
"""

from mcp.server.mcpserver import MCPServer
from mcp.shared.exceptions import MCPError
from mcp.types import CallToolResult, TextContent

server = MCPServer("cashu-probe")

# Payment terms, identical across the error probes so the only variable is the envelope.
TERMS = {
    "amount": 21,
    "unit": "sat",
    "mint": "https://mint.minibits.cash/Bitcoin",
    "paymentRequest": "creqA-PROBE",
    "howToPay": (
        "This tool costs 21 sat. Send a Cashu token from "
        "mint.minibits.cash/Bitcoin in the cashu_token argument."
    ),
}

# A receipt, for the success probes. Every value is a distinctive marker string so
# there is no ambiguity about which channel a given token arrived through.
RECEIPT = {
    "status": "success",
    "method": "cashu",
    "reference": "STRUCTURED-CHANNEL-9b71d224",
    "challengeId": "STRUCTURED-CHANNEL-kM9xPqWv",
    "timestamp": "2026-08-13T12:00:00Z",
}
META_RECEIPT = {"org.paymentauth/receipt": dict(RECEIPT, reference="META-CHANNEL-9b71d224")}

DATA_ROWS = "period_start  unique_users\n2026-07-01    1234\n2026-08-01    1310"


# ---------------------------------------------------------------- error paths

@server.tool(description="PROBE A: signals payment required as a JSON-RPC error (-32042).")
def probe_a_jsonrpc_error() -> str:
    raise MCPError(-32042, "Payment required", data=TERMS)


@server.tool(description="PROBE B: signals payment required as an is_error tool result.")
def probe_b_is_error() -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text="Payment required: 21 sat via mint.minibits.cash/Bitcoin")],
        structured_content=TERMS,
        is_error=True,
    )


@server.tool(description="PROBE C: raises a plain exception (the implementer's mistake).")
def probe_c_plain_raise() -> str:
    raise ValueError("Payment required: 21 sat via mint.minibits.cash/Bitcoin")


# -------------------------------------------------------------- success paths

@server.tool(description="PROBE D: successful result with BOTH text and structuredContent.")
def probe_d_success_text_and_structured() -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text="TEXT-CHANNEL rows follow\n" + DATA_ROWS)],
        structured_content=RECEIPT,
    )


@server.tool(description="PROBE E: successful result with structuredContent ONLY, no text.")
def probe_e_success_structured_only() -> CallToolResult:
    return CallToolResult(content=[], structured_content=RECEIPT)


@server.tool(description="PROBE F: successful result with text and a _meta receipt.")
def probe_f_success_meta() -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text="TEXT-CHANNEL rows follow\n" + DATA_ROWS)],
        meta=META_RECEIPT,
    )


if __name__ == "__main__":
    server.run()
