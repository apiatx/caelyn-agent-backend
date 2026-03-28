"""
Tests for cross_asset_trending pipeline.
Validates: preset routing, social keyword detection, TA-only exclusion, schema validation.
"""


def test_cross_asset_preset_routing():
    """cross_asset_trending preset should route to cross_asset_trending category, not trending."""
    from agent.claude_agent import TradingAgent
    agent = TradingAgent.__new__(TradingAgent)
    agent.INTENT_PROFILES = TradingAgent.INTENT_PROFILES
    agent.PRESET_ALIASES = TradingAgent.PRESET_ALIASES
    agent.INTENT_TO_CATEGORY = TradingAgent.INTENT_TO_CATEGORY

    agent._resolve_preset = TradingAgent._resolve_preset.__get__(agent)
    agent._build_plan_from_preset = TradingAgent._build_plan_from_preset.__get__(agent)
    agent._plan_to_query_info = TradingAgent._plan_to_query_info.__get__(agent)

    plan = agent._build_plan_from_preset("cross_asset_trending")
    assert plan is not None, "cross_asset_trending preset must resolve"
    assert plan["modules"].get("x_social_scan") is True, "x_social_scan module must be enabled"
    assert plan.get("x_social_scan_mode") == "cross_asset", "mode must be cross_asset"

    query_info = agent._plan_to_query_info(plan)
    assert query_info["category"] == "cross_asset_trending", f"Expected cross_asset_trending, got {query_info['category']}"
    print("PASS: cross_asset_trending preset routes correctly")


def test_social_keywords_enable_x_social_scan():
    """Freeform queries with social keywords should enable x_social_scan."""
    from agent.claude_agent import TradingAgent
    agent = TradingAgent.__new__(TradingAgent)
    agent._refine_plan_with_query = TradingAgent._refine_plan_with_query.__get__(agent)

    base_plan = {
        "intent": "cross_asset_trending",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": False, "social_sentiment": False, "x_social_scan": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {},
        "tickers": [],
    }

    social_queries = [
        "what's trending across markets",
        "show me the hype stocks today",
        "what's the sentiment on tech",
        "most talked about tickers",
        "what's hot right now",
        "social momentum plays",
    ]

    for q in social_queries:
        plan = agent._refine_plan_with_query(dict(base_plan), q)
        assert plan["modules"].get("x_social_scan") is True, f"x_social_scan should be enabled for: '{q}'"
    print("PASS: social keywords enable x_social_scan")


def test_ta_only_does_not_trigger_x_social_scan():
    """Pure TA explainer queries should NOT enable x_social_scan."""
    from agent.claude_agent import TradingAgent
    agent = TradingAgent.__new__(TradingAgent)
    agent._refine_plan_with_query = TradingAgent._refine_plan_with_query.__get__(agent)

    base_plan = {
        "intent": "chat",
        "asset_classes": ["equities"],
        "modules": {"x_sentiment": False, "social_sentiment": False, "x_social_scan": False},
        "risk_framework": "neutral",
        "response_style": "institutional_brief",
        "priority_depth": "medium",
        "filters": {},
        "tickers": [],
    }

    ta_queries = [
        "what is rsi and how does it work",
        "explain macd crossover",
        "how does fibonacci retracement work",
        "what is a chart pattern tutorial",
    ]

    for q in ta_queries:
        plan = agent._refine_plan_with_query(dict(base_plan), q)
        assert not plan["modules"].get("x_social_scan"), f"x_social_scan should NOT be enabled for: '{q}'"
    print("PASS: TA-only queries do not trigger x_social_scan")


def test_cross_asset_schema_validation():
    """Schema validator should accept valid and reject invalid Grok responses."""
    from data.xai_sentiment_provider import XAISentimentProvider
    provider = XAISentimentProvider.__new__(XAISentimentProvider)

    valid = {
        "as_of_utc": "2025-01-01T00:00:00Z",
        "market_direction_call": "Bullish",
        "sector_focus": ["Tech", "Energy"],
        "top_traders_view": ["AI is hot"],
        "your_opinion": "Markets look strong",
        "data_quality_flag": "high",
        "equities": {
            "large_caps": [{"symbol": "NVDA", "reason": "AI boom"}],
            "mid_caps": [],
            "small_micro_caps": [],
        },
        "crypto": [{"symbol": "BTC", "reason": "halving"}],
        "commodities": [{"commodity": "Gold", "reason": "safe haven"}],
    }
    is_valid, errors = provider._validate_cross_asset_schema(valid)
    assert is_valid, f"Valid schema rejected: {errors}"

    invalid = {"market_direction_call": "Bullish"}
    is_valid, errors = provider._validate_cross_asset_schema(invalid)
    assert not is_valid, "Invalid schema should be rejected"
    assert len(errors) > 0, "Should have error messages"

    bad_flag = dict(valid)
    bad_flag["data_quality_flag"] = "excellent"
    is_valid, errors = provider._validate_cross_asset_schema(bad_flag)
    assert not is_valid, "Bad data_quality_flag should be rejected"
    print("PASS: schema validation works correctly")


if __name__ == "__main__":
    test_cross_asset_preset_routing()
    test_social_keywords_enable_x_social_scan()
    test_ta_only_does_not_trigger_x_social_scan()
    test_cross_asset_schema_validation()
    print("\n=== ALL CROSS-ASSET TESTS PASSED ===")
