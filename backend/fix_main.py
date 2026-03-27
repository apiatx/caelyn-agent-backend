content = open('/home/runner/workspace/main.py').read()

old = (
    '        # --- everything below is identical to the original try block ---\n'
    '        try:\n'
    '\n'
    '        timing_meta = None\n'
)
new = (
    '        try:\n'
    '            timing_meta = None\n'
)
assert old in content, "ERROR: anchor 1 not found"
content = content.replace(old, new, 1)

# Fix indentation of the entire try body and yield instead of return JSONResponse
old2 = (
    '            timing_meta = None\n'
    '        if isinstance(result, dict) and result.get("_timing"):\n'
)
new2 = (
    '            timing_meta = None\n'
    '            if isinstance(result, dict) and result.get("_timing"):\n'
)
assert old2 in content, "ERROR: anchor 2 not found"

# Do a targeted block replacement of the full try body
old3 = '''        try:
            timing_meta = None
        if isinstance(result, dict) and result.get("_timing"):
            timing_meta = result.pop("_timing")
        if isinstance(result, dict) and result.get("_routing"):
            meta["routing"] = result.pop("_routing")
        if isinstance(result, dict) and result.get("_cross_asset_debug"):
            meta["cross_asset_debug"] = result.pop("_cross_asset_debug")
        if timing_meta:
            meta["timing_ms"] = timing_meta

        def _is_truly_empty(r):
            if not r:
                return True
            if not isinstance(r, dict):
                return True
            if r.get("type") == "error":
                return False
            structured = r.get("structured", {})
            if not isinstance(structured, dict) or not structured:
                analysis = r.get("analysis", "")
                return not analysis or len(str(analysis).strip()) == 0
            meaningful_keys = {"message", "summary", "picks", "conviction_picks",
                               "recommendations", "tickers", "sectors", "results",
                               "analysis_text", "briefing", "holdings", "top_picks",
                               "opportunities", "ranked_candidates", "watchlist",
                               "equities", "crypto", "commodities", "social_trading_signal",
                               "rows", "screen_name",
                               "top_trades", "bearish_setups"}
            has_content = any(structured.get(k) for k in meaningful_keys)
            if has_content:
                return False
            non_meta = {k: v for k, v in structured.items()
                        if k not in {"display_type", "type", "scan_type"} and v}
            return len(non_meta) == 0

        if isinstance(result, dict) and result.get("_parse_error"):
            parse_err = result.pop("_parse_error")
            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _error_envelope(
                "CLAUDE_JSON_PARSE_FAIL",
                "Claude returned a response that could not be parsed as structured JSON.",
                meta,
                details={"preview": parse_err.get("preview", "")[:800]},
            )
            _resp_log(req_id, 200, "error", resp)
            if conv_id:
                try:
                    updated_messages = list(history)
                    updated_messages.append({"role": "user", "content": user_query})
                    _asst_content = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                    updated_messages.append({"role": "assistant", "content": _asst_content})
                    _save_msgs(conv_id, updated_messages)
                except Exception:
                    pass
            return JSONResponse(content=resp)

        if _is_truly_empty(result):
            print(f"[API] WARNING: Empty/blank result returned for query: {user_query[:80]}")
            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _error_envelope(
                "EMPTY_RESPONSE",
                "The analysis returned empty. This usually means data sources were rate-limited. Please wait a minute and try again.",
                meta,
            )
            _resp_log(req_id, 200, "error", resp)
            if conv_id:
                try:
                    updated_messages = list(history)
                    updated_messages.append({"role": "user", "content": user_query})
                    _asst_content2 = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                    updated_messages.append({"role": "assistant", "content": _asst_content2})
                    _save_msgs(conv_id, updated_messages)
                except Exception:
                    pass
            return JSONResponse(content=resp)

        if conv_id:
            try:
                updated_messages = list(history)
                updated_messages.append({"role": "user", "content": user_query})
                _asst_content3 = result.get("analysis", "") if isinstance(result, dict) else ""
                if not _asst_content3:
                    _asst_content3 = _json.dumps(result, default=str)[:8000]
                updated_messages.append({"role": "assistant", "content": _asst_content3})
                _save_msgs(conv_id, updated_messages)
            except Exception as e:
                print(f"[API] Failed to save conversation: {e}")

            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _ok_envelope(result, meta)
            _resp_log(req_id, 200, "ok", resp)
            yield _j.dumps(resp).encode()'''

new3 = '''        try:
            timing_meta = None
            if isinstance(result, dict) and result.get("_timing"):
                timing_meta = result.pop("_timing")
            if isinstance(result, dict) and result.get("_routing"):
                meta["routing"] = result.pop("_routing")
            if isinstance(result, dict) and result.get("_cross_asset_debug"):
                meta["cross_asset_debug"] = result.pop("_cross_asset_debug")
            if timing_meta:
                meta["timing_ms"] = timing_meta

            def _is_truly_empty(r):
                if not r:
                    return True
                if not isinstance(r, dict):
                    return True
                if r.get("type") == "error":
                    return False
                structured = r.get("structured", {})
                if not isinstance(structured, dict) or not structured:
                    analysis = r.get("analysis", "")
                    return not analysis or len(str(analysis).strip()) == 0
                meaningful_keys = {"message", "summary", "picks", "conviction_picks",
                                   "recommendations", "tickers", "sectors", "results",
                                   "analysis_text", "briefing", "holdings", "top_picks",
                                   "opportunities", "ranked_candidates", "watchlist",
                                   "equities", "crypto", "commodities", "social_trading_signal",
                                   "rows", "screen_name",
                                   "top_trades", "bearish_setups"}
                has_content = any(structured.get(k) for k in meaningful_keys)
                if has_content:
                    return False
                non_meta = {k: v for k, v in structured.items()
                            if k not in {"display_type", "type", "scan_type"} and v}
                return len(non_meta) == 0

            if isinstance(result, dict) and result.get("_parse_error"):
                parse_err = result.pop("_parse_error")
                meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
                resp = _error_envelope(
                    "CLAUDE_JSON_PARSE_FAIL",
                    "Claude returned a response that could not be parsed as structured JSON.",
                    meta,
                    details={"preview": parse_err.get("preview", "")[:800]},
                )
                _resp_log(req_id, 200, "error", resp)
                if conv_id:
                    try:
                        updated_messages = list(history)
                        updated_messages.append({"role": "user", "content": user_query})
                        _asst_content = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        updated_messages.append({"role": "assistant", "content": _asst_content})
                        _save_msgs(conv_id, updated_messages)
                    except Exception:
                        pass
                yield _j.dumps(resp).encode()
                return

            if _is_truly_empty(result):
                print(f"[API] WARNING: Empty/blank result returned for query: {user_query[:80]}")
                meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
                resp = _error_envelope(
                    "EMPTY_RESPONSE",
                    "The analysis returned empty. This usually means data sources were rate-limited. Please wait a minute and try again.",
                    meta,
                )
                _resp_log(req_id, 200, "error", resp)
                if conv_id:
                    try:
                        updated_messages = list(history)
                        updated_messages.append({"role": "user", "content": user_query})
                        _asst_content2 = resp.get("analysis", "") or _json.dumps(resp, default=str)[:8000]
                        updated_messages.append({"role": "assistant", "content": _asst_content2})
                        _save_msgs(conv_id, updated_messages)
                    except Exception:
                        pass
                yield _j.dumps(resp).encode()
                return

            if conv_id:
                try:
                    updated_messages = list(history)
                    updated_messages.append({"role": "user", "content": user_query})
                    _asst_content3 = result.get("analysis", "") if isinstance(result, dict) else ""
                    if not _asst_content3:
                        _asst_content3 = _json.dumps(result, default=str)[:8000]
                    updated_messages.append({"role": "assistant", "content": _asst_content3})
                    _save_msgs(conv_id, updated_messages)
                except Exception as e:
                    print(f"[API] Failed to save conversation: {e}")

            meta["timing_ms"]["total"] = int((_time.time() - t0) * 1000)
            resp = _ok_envelope(result, meta)
            _resp_log(req_id, 200, "ok", resp)
            yield _j.dumps(resp).encode()'''

assert old3 in content, "ERROR: anchor 3 not found"
content = content.replace(old3, new3, 1)

open('/home/runner/workspace/main.py', 'w').write(content)
print("SUCCESS")
