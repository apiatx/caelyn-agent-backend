"""
Cross-Asset Weight Engine.

Applies regime-aware multipliers to raw scores based on:
  - Current market regime (risk_on / risk_off / inflationary / neutral)
  - Asset class (equity, crypto, commodity)
  - Market cap tier (nano, micro, small, large)
  - Liquidity tier (low, medium, high)
  - Sector characteristics

Multipliers are BOUNDED [0.75, 1.25] — no extreme swings.
Liquidity-aware: low-liquidity microcaps get harder penalization.
"""

MULT_FLOOR = 0.75
MULT_CEILING = 1.25

REGIME_WEIGHTS = {
    "risk_on": {
        "equity": {
            "nano": {"low": 0.85, "medium": 1.05, "high": 1.15},
            "micro": {"low": 0.90, "medium": 1.10, "high": 1.20},
            "small": {"low": 0.95, "medium": 1.05, "high": 1.15},
            "large": {"low": 1.00, "medium": 1.00, "high": 1.00},
        },
        "crypto": 1.15,
        "commodity": 0.95,
        "sector_boosts": {
            "technology": 1.12,
            "semiconductors": 1.12,
            "ai": 1.12,
            "software": 1.08,
            "consumer cyclical": 1.08,
            "fintech": 1.08,
            "biotech": 1.05,
        },
    },
    "risk_off": {
        "equity": {
            "nano": {"low": 0.75, "medium": 0.80, "high": 0.85},
            "micro": {"low": 0.75, "medium": 0.85, "high": 0.90},
            "small": {"low": 0.80, "medium": 0.90, "high": 0.95},
            "large": {"low": 1.05, "medium": 1.08, "high": 1.10},
        },
        "crypto": 0.80,
        "commodity": 1.15,
        "sector_boosts": {
            "utilities": 1.12,
            "consumer defensive": 1.12,
            "healthcare": 1.08,
            "energy": 1.05,
            "real estate": 0.90,
        },
    },
    "inflationary": {
        "equity": {
            "nano": {"low": 0.80, "medium": 0.88, "high": 0.92},
            "micro": {"low": 0.82, "medium": 0.90, "high": 0.95},
            "small": {"low": 0.88, "medium": 0.95, "high": 1.00},
            "large": {"low": 1.00, "medium": 1.03, "high": 1.05},
        },
        "crypto": 1.08,
        "commodity": 1.20,
        "sector_boosts": {
            "energy": 1.15,
            "basic materials": 1.12,
            "mining": 1.12,
            "commodities": 1.12,
            "financial": 1.05,
            "real estate": 0.85,
            "technology": 0.95,
        },
    },
    "neutral": {
        "equity": {
            "nano": {"low": 0.85, "medium": 0.95, "high": 1.00},
            "micro": {"low": 0.90, "medium": 0.98, "high": 1.00},
            "small": {"low": 0.95, "medium": 1.00, "high": 1.00},
            "large": {"low": 1.00, "medium": 1.00, "high": 1.00},
        },
        "crypto": 1.00,
        "commodity": 1.00,
        "sector_boosts": {},
    },
}


def compute_avg_dollar_volume(asset_data: dict) -> float:
    snapshot = asset_data.get("snapshot", {})
    details = asset_data.get("details", {})
    price = snapshot.get("price")
    avg_vol = details.get("avg_volume")
    if price and avg_vol:
        try:
            return float(price) * float(avg_vol)
        except (TypeError, ValueError):
            pass
    return 0.0


def get_liquidity_tier(avg_dollar_volume: float) -> str:
    if avg_dollar_volume >= 20_000_000:
        return "high"
    elif avg_dollar_volume >= 2_000_000:
        return "medium"
    return "low"


def get_mcap_tier(market_cap) -> str:
    if market_cap is None:
        return "micro"
    try:
        mc = float(market_cap)
    except (TypeError, ValueError):
        return "micro"
    if mc < 50_000_000:
        return "nano"
    elif mc < 300_000_000:
        return "micro"
    elif mc < 2_000_000_000:
        return "small"
    return "large"


def apply_asset_weights(raw_score: float, asset_metadata: dict, regime: str) -> dict:
    regime_config = REGIME_WEIGHTS.get(regime, REGIME_WEIGHTS["neutral"])
    asset_class = asset_metadata.get("asset_class", "equity").lower()
    mcap_tier = asset_metadata.get("market_cap_tier", "large").lower()
    liq_tier = asset_metadata.get("liquidity_tier", "medium").lower()
    sector = (asset_metadata.get("sector") or "").lower()

    if asset_class in ("equity", "stock", "etf"):
        equity_weights = regime_config.get("equity", {})
        tier_data = equity_weights.get(mcap_tier, equity_weights.get("large", {}))
        if isinstance(tier_data, dict):
            base_mult = tier_data.get(liq_tier, tier_data.get("medium", 1.0))
        else:
            base_mult = float(tier_data) if tier_data else 1.0
    elif asset_class == "crypto":
        base_mult = regime_config.get("crypto", 1.0)
    elif asset_class == "commodity":
        base_mult = regime_config.get("commodity", 1.0)
    else:
        base_mult = 1.0

    sector_boosts = regime_config.get("sector_boosts", {})
    sector_mult = 1.0
    for hot_sector, boost in sector_boosts.items():
        if hot_sector in sector:
            sector_mult = boost
            break

    final_mult = base_mult * sector_mult
    final_mult = max(MULT_FLOOR, min(MULT_CEILING, round(final_mult, 3)))

    adjusted_score = round(raw_score * final_mult, 1)
    adjusted_score = max(0, min(100, adjusted_score))

    return {
        "raw_score": round(raw_score, 1),
        "adjusted_score": adjusted_score,
        "regime_multiplier": final_mult,
        "base_multiplier": base_mult,
        "sector_multiplier": sector_mult,
        "regime": regime,
        "liquidity_tier": liq_tier,
        "mcap_tier": mcap_tier,
    }
