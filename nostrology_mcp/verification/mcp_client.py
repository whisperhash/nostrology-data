"""Minimal MCP streamable-HTTP client for the deployed nostrology server.

Transport is `curl`, not `urllib`: a workstation this harness runs from may have a python
without a usable CA bundle, and shelling out avoids pinning a certifi version
into the repo for a read-only diagnostic tool.
"""
import json
import re
import subprocess

DEFAULT_URL = "https://nostrolo.gy/mcp?traffic_type=internal"


class McpError(RuntimeError):
    pass


class McpClient:
    """One initialized MCP session. Call `init()` before `call()`."""

    def __init__(self, url=DEFAULT_URL, timeout=180):
        self.url = url
        self.timeout = timeout
        self.session_id = None
        self._rid = 0

    # -- transport ---------------------------------------------------------
    def _post(self, body):
        cmd = [
            "curl", "-sS", "-X", "POST", self.url,
            "-D", "/dev/stderr", "--max-time", str(self.timeout),
            "-H", "Content-Type: application/json",
            "-H", "Accept: application/json, text/event-stream",
        ]
        if self.session_id:
            cmd += ["-H", "Mcp-Session-Id: " + self.session_id]
        cmd += ["-d", json.dumps(body)]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise McpError(f"curl failed ({proc.returncode}): {proc.stderr[-400:]}")
        m = re.search(r"(?im)^mcp-session-id:\s*(\S+)", proc.stderr)
        return (m.group(1) if m else None), proc.stdout

    @staticmethod
    def _parse(raw):
        """Accept either a plain JSON body or an SSE `data:` frame."""
        if raw.startswith("event:") or raw.startswith("data:"):
            for line in raw.splitlines():
                if line.startswith("data:"):
                    return json.loads(line[5:].strip())
            return None
        return json.loads(raw) if raw.strip() else None

    # -- protocol ----------------------------------------------------------
    def init(self):
        sid, _ = self._post({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "nostrology-verification", "version": "1.0"},
            },
        })
        if not sid:
            raise McpError("server returned no mcp-session-id on initialize")
        self.session_id = sid
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return sid

    def call(self, tool, args, retries=2):
        """Call a tool and return its text payload.

        An empty HTTP body has been observed once in the wild (see the harness
        README); it answered correctly on retry, so transport-level emptiness is
        retried rather than reported as a server answer.
        """
        last = None
        for attempt in range(retries + 1):
            self._rid += 1
            _, raw = self._post({
                "jsonrpc": "2.0", "id": 100 + self._rid, "method": "tools/call",
                "params": {"name": tool, "arguments": args},
            })
            resp = self._parse(raw)
            if resp is None:
                last = "ERROR:empty response"
                continue
            if "error" in resp:
                return "ERROR:" + json.dumps(resp["error"]), attempt
            result = resp["result"]
            text = "\n".join(c.get("text", "") for c in result.get("content", []))
            if result.get("isError"):
                return "TOOLERROR:" + text, attempt
            return text, attempt
        return last, retries
