"""
Cross-Asset Weight Engine.

Applies regime-aware multipliers to raw scores based on:
  - Current market regime (risk_on / risk_off / inflationary / neutral)
  - Asset class (equity, crypto, commodity)
  - Market cap tier (micro, small, large)
  - Sector characteristics

Does NOT filter — only adjusts scores to reflect regime-appropriate positioning.
"""

REGIME_WEIGHTS = {
    "risk_on": {
        "equity": {
            "micro": 1.20,
            "small": 1.15,
            "large": 1.00,
        },
        "crypto": 1.20,
        "commodity": 0.95,
        "sector_boosts": {
            "technology": 1.15,
            "semiconductors": 1.15,
            "ai": 1.15,
            "software": 1.10,
            "consumer cyclical": 1.10,
            "fintech": 1.10,
            "biotech": 1.05,
        },
    },
    "risk_off": {
        "equity": {
            "micro": 0.80,
            "small": 0.85,
            "large": 1.10,
        },
        "crypto": 0.75,
        "commodity": 1.15,
        "sector_boosts": {
            "utilities": 1.15,
            "consumer defensive": 1.15,
            "healthcare": 1.10,
            "energy": 1.05,
            "real estate": 0.90,
        },
    },
    "inflationary": {
        "equity": {
            "micro": 0.90,
            "small": 0.95,
            "large": 1.05,
        },
        "crypto": 1.10,
        "commodity": 1.25,
        "sector_boosts": {
            "energy": 1.20,
            "basic materials": 1.15,
            "mining": 1.15,
            "commodities": 1.15,
            "financial": 1.05,
            "real estate": 0.85,
            "technology": 0.95,
        },
    },
    "neutral": {
        "equity": {
            "micro": 1.00,
            "small": 1.00,
            "large": 1.00,
        },
        "crypto": 1.00,
        "commodity": 1.00,
        "sector_boosts": {},
    },
}


def apply_asset_weights(raw_score: float, asset_metadata: dict, regime: str) -> dict:
    regime_config = REGIME_WEIGHTS.get(regime, REGIME_WEIGHTS["neutral"])
    asset_class = asset_metadata.get("asset_class", "equity").lower()
    mcap_tier = asset_metadata.get("market_cap_tier", "large").lower()
    sector = (asset_metadata.get("sector") or "").lower()

    if asset_class in ("equity", "stock"):
        equity_weights = regime_config.get("equity", {})
        base_mult = equity_weights.get(mcap_tier, 1.0)
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
    adjusted_score = round(raw_score * final_mult, 1)
    adjusted_score = max(0, min(100, adjusted_score))

    return {
        "raw_score": round(raw_score, 1),
        "adjusted_score": adjusted_score,
        "regime_multiplier": round(final_mult, 3),
        "base_multiplier": base_mult,
        "sector_multiplier": sector_mult,
        "regime": regime,
    }
