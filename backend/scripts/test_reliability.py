"""
Reliability patch tests.
Validates: envelope structure, parse-fail signaling, response logging.
"""
import json


def test_envelope_helpers():
    """_ok_envelope and _error_envelope must produce correct shapes."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    from main import _build_meta, _ok_envelope, _error_envelope

    meta = _build_meta("test-123", preset_intent="trending", conv_id="conv-abc")
    assert meta["request_id"] == "test-123"
    assert meta["preset_intent"] == "trending"
    assert meta["conversation_id"] == "conv-abc"
    assert "routing" in meta
    assert "timing_ms" in meta

    ok = _ok_envelope({"type": "chat", "analysis": "hello", "structured": {"message": "hi"}}, meta)
    assert ok["type"] == "ok"
    assert ok["error"] is None
    assert ok["meta"]["request_id"] == "test-123"
    assert ok["request_id"] == "test-123"
    assert ok["conversation_id"] == "conv-abc"
    assert "as_of" in ok

    err = _error_envelope("TEST_ERROR", "Something broke", meta, details={"foo": "bar"})
    assert err["type"] == "error"
    assert err["error"]["code"] == "TEST_ERROR"
    assert err["error"]["message"] == "Something broke"
    assert err["error"]["details"]["foo"] == "bar"
    assert err["meta"]["request_id"] == "test-123"
    assert err["analysis"] == ""
    assert isinstance(err["structured"], dict)

    print("PASS: envelope helpers produce correct shapes")


def test_ok_envelope_never_empty():
    """_ok_envelope must always set type, analysis, structured even on minimal input."""
    from main import _build_meta, _ok_envelope
    meta = _build_meta("test-456")

    minimal = _ok_envelope({}, meta)
    assert minimal["type"] == "ok"
    assert "analysis" in minimal
    assert "structured" in minimal
    assert minimal["error"] is None

    non_dict = _ok_envelope("raw string", meta)
    assert non_dict["type"] == "ok"
    assert non_dict["error"] is None

    none_input = _ok_envelope(None, meta)
    assert none_input["type"] == "ok"
    assert none_input["error"] is None

    print("PASS: _ok_envelope never returns empty")


def test_error_envelope_with_partial():
    """_error_envelope should include partial candidates when provided."""
    from main import _build_meta, _error_envelope
    meta = _build_meta("test-789")
    partial = {"partial_candidates": [{"ticker": "AAPL"}, {"ticker": "NVDA"}]}
    err = _error_envelope("TIMEOUT", "Timed out", meta, partial=partial)
    assert err["structured"]["partial_candidates"][0]["ticker"] == "AAPL"
    assert err["type"] == "error"

    print("PASS: error envelope includes partial candidates")


def test_parse_response_signals_failure():
    """_parse_response must attach _parse_error when all tiers fail."""
    from agent.claude_agent import TradingAgent
    agent = TradingAgent.__new__(TradingAgent)
    agent._parse_response = TradingAgent._parse_response.__get__(agent)

    result = agent._parse_response("This is not JSON at all, no braces anywhere", request_id="test-parse")
    assert "_parse_error" in result, "_parse_error should be present when all parse tiers fail"
    assert len(result["_parse_error"]["preview"]) > 0
    assert result["type"] == "chat"

    good = agent._parse_response('{"display_type":"chat","message":"hello"}', request_id="test-good")
    assert "_parse_error" not in good, "_parse_error should NOT be present on valid JSON"

    print("PASS: parse failure signaling works correctly")


def test_resp_log_format():
    """_resp_log should not raise and should print correct format."""
    import io, sys
    from main import _resp_log

    captured = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured
    _resp_log("test-log", 200, "ok", {"type": "ok", "analysis": "hello"})
    sys.stdout = old_stdout

    output = captured.getvalue()
    assert "[RESP]" in output
    assert "id=test-log" in output
    assert "status=200" in output
    assert "type=ok" in output
    assert "bytes=" in output

    print("PASS: _resp_log format is correct")


if __name__ == "__main__":
    test_envelope_helpers()
    test_ok_envelope_never_empty()
    test_error_envelope_with_partial()
    test_parse_response_signals_failure()
    test_resp_log_format()
    print("\n=== ALL RELIABILITY TESTS PASSED ===")
