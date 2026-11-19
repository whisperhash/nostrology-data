"""Call-log tests: record shape, appending, day-file naming, and the best-effort
guarantee (a logging failure must never propagate into the tool call)."""

import json
import os

from nostrology_mcp import calllog


def _write_ok(tmp_path, monkeypatch, **overrides):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    kwargs = dict(
        tool="nostrology_query_metric",
        params={"domain": "zapstore", "metric": "new_native_app_count", "grain": "day", "format": "csv"},
        outcome="ok",
        sql_key="zapstore.new_native_app_count@day",
        rows=412,
        bytes_out=8134,
        ms={"connect": 12, "query": 41, "render": 3, "total": 56},
        server_version="0.1.0",
        git_sha="abc1234",
    )
    kwargs.update(overrides)
    calllog.write_record(**kwargs)


def _read_records(tmp_path):
    files = sorted(os.listdir(tmp_path))
    assert len(files) == 1 and files[0].startswith("calls-") and files[0].endswith(".jsonl")
    with open(os.path.join(tmp_path, files[0])) as fh:
        return [json.loads(line) for line in fh]


def test_record_shape(tmp_path, monkeypatch):
    _write_ok(tmp_path, monkeypatch)
    (rec,) = _read_records(tmp_path)
    assert rec["v"] == 4
    assert rec["kind"] == "tool"  # the v3 discriminator; rpc records share the file
    assert rec["ts"].endswith("Z")
    assert rec["tool"] == "nostrology_query_metric"
    assert rec["params"]["format"] == "csv"
    assert rec["resolved"] == {"sql_key": "zapstore.new_native_app_count@day"}
    assert rec["outcome"] == "ok"
    assert rec["rows"] == 412
    assert rec["bytes"] == 8134
    assert rec["est_tokens"] == (8134 + 3) // 4
    assert rec["ms"]["total"] == 56
    assert rec["server"] == {"version": "0.1.0", "git_sha": "abc1234"}
    assert "error" not in rec
    assert "client" not in rec  # absent, not null, when the transport has no identity


def test_client_block_is_recorded_when_present(tmp_path, monkeypatch):
    """The v2 field: HTTP calls carry who called (session id + clientInfo). The
    file record and any downstream consumer key off `client` being present."""
    _write_ok(
        tmp_path,
        monkeypatch,
        client={"session_id": "abc-123", "name": "claude-code", "version": "2.1.220"},
    )
    (rec,) = _read_records(tmp_path)
    assert rec["client"] == {"session_id": "abc-123", "name": "claude-code", "version": "2.1.220"}


def test_stdout_mirror_is_opt_in_and_identical(tmp_path, monkeypatch, capsys):
    """stdout is the Loki path on the box (the log shipper tails container stdout), but under
    the stdio transport stdout IS the JSON-RPC channel - so the mirror must be OFF
    unless NOSTROLOGY_MCP_LOG_STDOUT=1, and when on, must emit the SAME record the
    file gets (one analysis schema, two sinks)."""
    monkeypatch.delenv("NOSTROLOGY_MCP_LOG_STDOUT", raising=False)
    _write_ok(tmp_path, monkeypatch)
    assert capsys.readouterr().out == ""  # stdio-safe by default

    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_STDOUT", "1")
    _write_ok(tmp_path, monkeypatch)
    out = capsys.readouterr().out
    assert out.endswith("\n") and out.count("\n") == 1
    assert json.loads(out) == _read_records(tmp_path)[1]


def test_error_record_carries_error_and_null_sizes(tmp_path, monkeypatch):
    _write_ok(
        tmp_path,
        monkeypatch,
        outcome="db_error",
        rows=None,
        bytes_out=None,
        ms={"total": 3000},
        error="OperationalError: connection refused",
    )
    (rec,) = _read_records(tmp_path)
    assert rec["outcome"] == "db_error"
    assert rec["rows"] is None and rec["bytes"] is None and rec["est_tokens"] is None
    assert rec["error"].startswith("OperationalError")


def test_appends_one_line_per_call(tmp_path, monkeypatch):
    _write_ok(tmp_path, monkeypatch)
    _write_ok(tmp_path, monkeypatch)
    assert len(_read_records(tmp_path)) == 2


# -- rpc records (v3) ------------------------------------------------------------


def _write_rpc(tmp_path, monkeypatch, **overrides):
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    kwargs = dict(
        method="tools/list",
        params={"_meta": {}},
        outcome="ok",
        ms={"total": 2},
        server_version="0.1.0",
        git_sha="abc1234",
        request_id=7,
    )
    kwargs.update(overrides)
    calllog.write_rpc_record(**kwargs)


def test_rpc_record_shape(tmp_path, monkeypatch):
    _write_rpc(tmp_path, monkeypatch)
    (rec,) = _read_records(tmp_path)
    assert rec["v"] == 4 and rec["kind"] == "rpc"
    assert rec["ts"].endswith("Z")
    assert rec["method"] == "tools/list"
    assert rec["request_id"] == 7
    assert rec["params"] == {"_meta": {}}
    assert rec["outcome"] == "ok"
    assert rec["ms"] == {"total": 2}
    assert rec["server"] == {"version": "0.1.0", "git_sha": "abc1234"}
    assert "client" not in rec and "error" not in rec


def test_rpc_and_tool_records_share_one_file_and_are_told_apart_by_kind(tmp_path, monkeypatch):
    """A tools/call writes both. Downstream must switch on `kind` - the two shapes
    have different fields (method/request_id vs tool/resolved/rows/bytes) and reading
    one as the other silently yields None."""
    _write_rpc(tmp_path, monkeypatch, method="tools/call")
    _write_ok(tmp_path, monkeypatch)
    rpc, tool = _read_records(tmp_path)
    assert rpc["kind"] == "rpc" and "tool" not in rpc
    assert tool["kind"] == "tool" and "method" not in tool


def test_rpc_notification_is_marked_by_a_null_request_id(tmp_path, monkeypatch):
    """`request_id is None` is how a notification is distinguished from a request -
    the SDK's own contract, preserved into the record."""
    _write_rpc(tmp_path, monkeypatch, method="notifications/initialized", request_id=None, params=None)
    (rec,) = _read_records(tmp_path)
    assert rec["request_id"] is None and rec["params"] is None


def test_rpc_params_redact_credential_shaped_keys(tmp_path, monkeypatch):
    """Params are attacker-controlled on an unauthenticated public endpoint, and
    this log is read by a sandboxed observer whose whole security model is that it
    cannot reach secrets. Nothing credential-named may be written, at any nesting."""
    _write_rpc(
        tmp_path,
        monkeypatch,
        params={
            "metric": "zap_count",
            "authorization": "Bearer sk-live-123",
            "nested": {"api_key": "AKIA-nope", "Secret": "s3kr1t", "keep": "visible"},
            "list": [{"password": "hunter2"}],
        },
    )
    (rec,) = _read_records(tmp_path)
    blob = json.dumps(rec)
    for leaked in ("sk-live-123", "AKIA-nope", "s3kr1t", "hunter2"):
        assert leaked not in blob
    assert rec["params"]["metric"] == "zap_count"  # non-secret data survives intact
    assert rec["params"]["nested"]["keep"] == "visible"
    assert rec["params"]["list"][0]["password"] == "[redacted]"


def test_rpc_oversized_params_are_replaced_by_their_size(tmp_path, monkeypatch):
    """One hostile body must not flood the day-file (or Loki). Over the cap we keep
    the interesting fact - that it was huge - and drop the content."""
    _write_rpc(tmp_path, monkeypatch, params={"blob": "x" * (calllog.MAX_PARAMS_CHARS + 100)})
    (rec,) = _read_records(tmp_path)
    assert rec["params"]["_truncated_chars"] > calllog.MAX_PARAMS_CHARS
    assert "blob" not in rec["params"]


def test_truncation_keeps_the_routing_keys(tmp_path, monkeypatch):
    """Truncating away WHICH tool was called would discard the whole point of the
    record - an oversized call is still demand data, and "someone sent 2 MB at
    nostrology_query_metric" is the analyzable part."""
    _write_rpc(
        tmp_path,
        monkeypatch,
        method="tools/call",
        params={"name": "nostrology_query_metric", "arguments": {"x": "y" * calllog.MAX_PARAMS_CHARS}},
    )
    (rec,) = _read_records(tmp_path)
    assert rec["params"]["name"] == "nostrology_query_metric"
    assert rec["params"]["_truncated_chars"] > calllog.MAX_PARAMS_CHARS
    assert "arguments" not in rec["params"]


def test_client_identity_fields_are_clipped(tmp_path, monkeypatch):
    """THE amplification guard. clientInfo comes from the attacker-controlled
    `initialize` body but is stamped on EVERY record for the life of the session, so
    an uncapped one multiplies a single hostile handshake across every later message
    - into the day-file and through the log shipper into Loki. Capping `params` alone does not
    close this."""
    huge = "A" * 200_000
    _write_rpc(tmp_path, monkeypatch, client={"session_id": "s-1", "name": huge, "version": "1.0"})
    _write_ok(tmp_path, monkeypatch, client={"session_id": "s-1", "name": huge, "version": "1.0"})
    rpc, tool = _read_records(tmp_path)
    for rec in (rpc, tool):  # both writers, not just the new one
        assert len(rec["client"]["name"]) < 200
        assert rec["client"]["name"].startswith("AAA")
        assert "[+199872]" in rec["client"]["name"]  # how much was dropped, on record
        assert rec["client"]["session_id"] == "s-1"  # short values pass through intact


def test_clipping_passes_through_absent_identity_untouched():
    """Both producers return None (never `{}`) when there is no identity to record,
    which is what keeps `client` absent-not-null - the contract since v2. The clip
    must not disturb that on the way through."""
    assert calllog._clip_client(None) is None
    assert calllog._clip_client({}) == {}


def test_a_hostile_handshake_cannot_inflate_the_day_file(tmp_path, monkeypatch):
    """The property that actually matters, stated as a size bound rather than a
    field length: N records cost O(N), not O(N * hostile_input)."""
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    client = {"session_id": "s", "name": "B" * 500_000, "version": "C" * 500_000}
    for _ in range(5):
        _write_rpc(tmp_path, monkeypatch, client=client)
    size = os.path.getsize(os.path.join(tmp_path, os.listdir(tmp_path)[0]))
    assert size < 5_000, f"5 records cost {size} bytes from a 1 MB handshake"


def test_rpc_unserializable_params_degrade_to_their_type(tmp_path, monkeypatch):
    """An odd payload must cost the params field, never the whole record - losing
    the record would lose the fact that the call happened at all."""

    class Exploding:
        def __str__(self):
            raise RuntimeError("nope")

    _write_rpc(tmp_path, monkeypatch, params={"bad": Exploding()})
    (rec,) = _read_records(tmp_path)
    assert rec["params"] == {"_unserializable": "dict"}
    assert rec["method"] == "tools/list"  # the rest of the record survived


def test_rpc_write_failure_never_raises(tmp_path, monkeypatch, capsys):
    """Same best-effort guarantee as the tool record: logging must never be the
    reason a call fails."""
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    blocked.chmod(0o400)
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(blocked / "sub"))
    try:
        calllog.write_rpc_record(
            method="initialize",
            params=None,
            outcome="ok",
            ms={"total": 1},
            server_version="0.1.0",
            git_sha="abc1234",
        )
    finally:
        blocked.chmod(0o700)
    assert "call log write failed" in capsys.readouterr().err


def test_unwritable_dir_never_raises(tmp_path, monkeypatch, capsys):
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    blocked.chmod(0o400)
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(blocked / "sub"))
    try:
        calllog.write_record(  # must not raise
            tool="nostrology_query_metric",
            params={},
            outcome="ok",
            sql_key="zapstore.new_native_app_count@day",
            rows=1,
            bytes_out=10,
            ms={"total": 1},
            server_version="0.1.0",
            git_sha="abc1234",
        )
    finally:
        blocked.chmod(0o700)
    assert "call log write failed" in capsys.readouterr().err


# -- the paid tool's redaction closures (tool-record params + error-field scrub) --


def test_tool_record_params_redact_credential_shaped_keys(tmp_path, monkeypatch):
    """The blocker-class gap the paid tool's review found: write_record used to
    store `params` VERBATIM (redaction lived only in write_rpc_record), so the
    first tool with a credential argument would have written a bearer
    instrument to the day-file and Loki. Redaction now lives at this sink too."""
    _write_ok(
        tmp_path,
        monkeypatch,
        tool="nostrology_query_nutzaps",
        params={"metric": "active_users", "cashu_token": "cashuBo2FtdSREDACTME", "grain": "month"},
    )
    (rec,) = _read_records(tmp_path)
    assert rec["params"]["cashu_token"] == "[redacted]"
    assert rec["params"]["metric"] == "active_users"  # non-secret keys untouched
    assert "cashuB" not in json.dumps(rec)


def test_error_fields_scrub_embedded_bearer_tokens_on_both_record_kinds(tmp_path, monkeypatch):
    """The VALUE-shaped backstop: the paid path builds errors from constants,
    but an SDK layer echoing request content into an exception message is the
    path nobody wrote - so the sink scrubs cashu/creq serializations out of
    free text on BOTH writers."""
    leak = "ValueError: bad token cashuBo2FtdWNzYXR4Impo29nustSpendMe123 (rejected)"
    _write_ok(tmp_path, monkeypatch, outcome="db_error", error=leak)
    monkeypatch.setenv("NOSTROLOGY_MCP_LOG_DIR", str(tmp_path))
    calllog.write_rpc_record(
        method="tools/call",
        params={"name": "nostrology_query_nutzaps"},
        outcome="error",
        ms={"total": 1},
        server_version="0.1.0",
        git_sha="abc1234",
        request_id=1,
        error="creqAgaRhdBhQaGVsbG8Payload leaked into an exception",
    )
    tool_rec, rpc_rec = _read_records(tmp_path)
    assert "cashuB" not in json.dumps(tool_rec)
    assert "[redacted-cashu]" in tool_rec["error"]
    assert "(rejected)" in tool_rec["error"]  # surrounding diagnostic text survives
    assert "creqA" not in json.dumps(rpc_rec)
    assert "[redacted-cashu]" in rpc_rec["error"]


def test_scrub_bearer_tokens_leaves_ordinary_text_alone():
    """The scrub must not mangle legitimate error text: 'cashu' as a word, the
    tool name, short prefixes that are not a serialized instrument."""
    for text in (
        "the cashu_token argument is required",
        "a Cashu token from mint.minibits.cash",
        "cashuB",  # bare prefix, no payload - not an instrument
        None,
    ):
        assert calllog.scrub_bearer_tokens(text) == text


def test_scrub_catches_json_shaped_proof_secrets_too():
    """The second accepted credential shape (a NUT-18 PaymentRequestPayload)
    carries proof secrets with NO self-labeling prefix - the scrub must catch
    the JSON shape itself (review finding: the prefix regex alone passed these
    to Loki verbatim)."""
    leak = (
        'ValueError: bad payload {"mint":"https://x","proofs":[{"amount":21,'
        '"id":"00ad268c4d1f5826","secret":"407915bc212be61a77e3e6d2aeb4c727",'
        '"C":"02bc9097997d81afb2cc7346b5e4345a93"}]}'
    )
    scrubbed = calllog.scrub_bearer_tokens(leak)
    assert "407915bc" not in scrubbed
    assert "02bc9097" not in scrubbed
    assert '"secret":"[redacted-cashu]"' in scrubbed
    assert '"C":"[redacted-cashu]"' in scrubbed
    assert '"id":"00ad268c4d1f5826"' in scrubbed  # keyset id is public - kept for diagnosis
