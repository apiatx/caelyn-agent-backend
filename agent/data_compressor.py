import json


MAX_TOTAL_CHARS = 80000
MAX_ARRAY_ITEMS = 15
MAX_STRING_LENGTH = 200
MAX_DESCRIPTION_LENGTH = 100


STRIP_FIELDS = {
    "image", "thumb", "small", "large", "logo", "icon",
    "sparkline_in_7d", "sparkline", "roi",
    "localization", "description",
    "links", "repos_url", "homepage", "blockchain_site",
    "official_forum_url", "chat_url", "announcement_url",
    "subreddit_url", "genesis_date", "ico_data",
    "last_updated", "updated_at",
    "platform", "contract_address",
    "urls", "date_added",
    "notice", "tags",
    "slug",
}

COIN_KEEP_FIELDS = {
    "id", "symbol", "name", "market_cap_rank",
    "market_data", "community_data", "developer_data",
    "sentiment_votes_up_percentage", "sentiment_votes_down_percentage",
    "watchlist_portfolio_users", "categories",
}

MARKET_DATA_KEEP = {
    "current_price", "market_cap", "total_volume",
    "price_change_percentage_24h", "price_change_percentage_7d",
    "price_change_percentage_30d", "price_change_percentage_1y",
    "ath", "ath_change_percentage", "atl", "atl_change_percentage",
    "circulating_supply", "total_supply", "max_supply",
    "fully_diluted_valuation",
}


def compress_data(data: dict, scan_type: str = "general") -> dict:
    if not isinstance(data, dict):
        return data

    compressed = {}
    for key, value in data.items():
        compressed[key] = _compress_value(value, key)

    result_str = json.dumps(compressed, default=str)
    if len(result_str) > MAX_TOTAL_CHARS:
        compressed = _aggressive_truncate(compressed, MAX_TOTAL_CHARS)

    return compressed


def _compress_value(value, key=""):
    if value is None:
        return None

    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            return value[:MAX_STRING_LENGTH] + "..."
        return value

    if isinstance(value, (int, float, bool)):
        return value

    if isinstance(value, list):
        truncated = value[:MAX_ARRAY_ITEMS]
        return [_compress_value(item, key) for item in truncated]

    if isinstance(value, dict):
        return _compress_dict(value, key)

    return str(value)[:MAX_STRING_LENGTH]


def _compress_dict(d: dict, parent_key: str = "") -> dict:
    result = {}
    for k, v in d.items():
        if k.lower() in STRIP_FIELDS:
            continue

        if parent_key == "deep_dive" and isinstance(v, dict):
            v = _compress_coin_detail(v)

        if k == "market_data" and isinstance(v, dict):
            v = {mk: mv for mk, mv in v.items() if mk in MARKET_DATA_KEEP}
            for mk in list(v.keys()):
                if isinstance(v[mk], dict) and "usd" in v[mk]:
                    v[mk] = v[mk]["usd"]

        if k in ("community_data", "developer_data") and isinstance(v, dict):
            v = {ck: cv for ck, cv in v.items() if cv is not None and cv != 0 and cv != ""}

        result[k] = _compress_value(v, k)

    return result


def _compress_coin_detail(coin: dict) -> dict:
    return {k: v for k, v in coin.items() if k in COIN_KEEP_FIELDS}


def _truncate_value(val):
    if isinstance(val, list):
        cut_to = max(1, len(val) // 2)
        return [_truncate_value(item) for item in val[:cut_to]]
    elif isinstance(val, dict):
        if len(val) > 3:
            cut_to = max(3, len(val) // 2)
            keys = list(val.keys())[:cut_to]
            return {k: val[k] for k in keys}
        return {k: _truncate_value(v) for k, v in val.items()}
    elif isinstance(val, str) and len(val) > 100:
        return val[:100] + "..."
    return val


PROTECTED_KEYS = {
    "two_tier_analysis", "grok_x_analysis", "scan_type",
    "source_summary", "x_market_mood", "total_unique_tickers",
    "ranked_tickers", "orchestration_metadata",
    "ranked_candidates", "ranking_debug",
    "institutional_scoring", "prior_score",
    "data_completeness", "budget_exhausted_at", "social_discipline_flag",
    "regime_context", "position_sizing", "catalyst_breakdown",
    "adjusted_final_score", "regime", "regime_multiplier",
    "creative_discovery_override", "weight_matrix",
}


def _aggressive_truncate(data: dict, max_chars: int) -> dict:
    result = json.loads(json.dumps(data, default=str))

    for pass_num in range(20):
        result_str = json.dumps(result, default=str)
        if len(result_str) <= max_chars:
            break

        avg_budget = max_chars // max(len(result), 1)
        truncated_any = False
        for k, v in list(result.items()):
            if k in PROTECTED_KEYS:
                continue
            size = len(json.dumps(v, default=str))
            if size > avg_budget and size > 20:
                result[k] = _truncate_value(v)
                truncated_any = True

        if not truncated_any:
            break

    return result
