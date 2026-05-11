"""
Hyperliquid Market Matrix categorizer.

Assigns every ScreenerAsset in the live Hyperliquid universe to exactly one of:
  stocks_etfs | crypto | commodities | indices | pre_ipo | themes

Classification precedence:
  1. Hyperliquid HIP-3 category tags applied by the normalizer
     (e.g. "equity", "commodity", "index", "pre-IPO", "macro").
  2. DEX prefix hint for HIP-3 markets (xyz/flx/cash → stocks, vntl → pre-IPO,
     km → macro, abcd → index, hyna/para → crypto).
  3. Symbol/name fallback against curated keyword lists.
  4. Default: crypto (covers BTC/ETH/SOL/HYPE and the entire native HL perp set).

Native Hyperliquid perps that do NOT carry a HIP-3 dex prefix are always crypto.
Spot markets are crypto unless tagged otherwise.
"""
from __future__ import annotations

from typing import Optional

from .models import ScreenerAsset


# ─────────────────────────────────────────────────────────────────────────────
# Canonical tab labels
# ─────────────────────────────────────────────────────────────────────────────

TAB_LABELS: dict[str, str] = {
    "stocks_etfs":  "Stocks/ETFs",
    "crypto":       "Crypto",
    "commodities":  "Commodities",
    "indices":      "Indices",
    "pre_ipo":      "Pre-IPO Stocks",
    "themes":       "Themes",
}
TAB_ORDER: list[str] = ["stocks_etfs", "crypto", "commodities", "indices", "pre_ipo", "themes"]


# ─────────────────────────────────────────────────────────────────────────────
# Fallback symbol/name keyword lists (used only when HL tags are inconclusive)
# ─────────────────────────────────────────────────────────────────────────────

# Common commodity tickers/markers seen on HL HIP-3 DEXes.
_COMMODITY_KEYWORDS = {
    "GOLD", "XAU", "SILVER", "XAG", "PLATINUM", "XPT", "PALLADIUM", "XPD",
    "COPPER", "OIL", "WTI", "BRENT", "BRENTOIL", "USOIL", "USENERGY",
    "NATGAS", "GAS", "NGAS", "CORN", "WHEAT", "SOYBEAN", "SUGAR", "COFFEE",
}

# Common index / index-ETF tickers on HL HIP-3.
_INDEX_KEYWORDS = {
    "SPX", "SP500", "SPY", "USA500", "US500", "NDX", "QQQ", "USTECH",
    "DJI", "DOW", "RUSSELL", "RUT", "IWM", "SMALL2000", "VIX",
    "MAG7", "INFOTECH", "NUCLEAR", "DEFENSE", "ENERGY",
    "EWY", "EWZ", "EWJ", "EFA", "EEM", "FXI", "XLK", "XLF",
    "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE",
}

# Common pre-IPO names. These appear on HL HIP-3 (Ventral / vntl DEX).
_PREIPO_KEYWORDS = {
    "SPACEX", "OPENAI", "ANTHROPIC", "CEREBRAS", "STRIPE", "DATABRICKS",
    "XAI", "PERPLEXITY", "FIGURE", "NEURALINK", "RIPPLE", "EPICGAMES",
    "BYTEDANCE", "TIKTOK", "DISCORD", "REDDIT", "INSTACART", "KLARNA",
    "SHEIN", "REVOLUT", "CHIME", "PLAID",
}

_THEME_KEYWORDS = {
    "ROBOT", "SEMI", "SEMIS", "INFOTECH", "NUCLEAR", "DEFENSE", "ENERGY", "AI",
}

_COMMODITY_OVERRIDE_SYMBOLS = {
    "CL", "WTI", "BRENTOIL", "BRENT", "OIL", "NATGAS", "GOLD", "SILVER", "COPPER",
    "PLATINUM", "PALLADIUM",
}

_INDEX_OVERRIDE_SYMBOLS = {
    "SP500", "US500", "USA100", "USTECH", "NASDAQ", "XYZ100", "JP225", "KR200",
    "EWY", "EWJ", "EWZ",
}

# Common equity tickers spanning major US stocks/ETFs traded on HL HIP-3 DEXes.
_EQUITY_KEYWORDS = {
    "AAPL", "MSFT", "NVDA", "AMD", "INTC", "GOOG", "GOOGL", "META", "AMZN",
    "TSLA", "NFLX", "ORCL", "CRM", "ADBE", "CSCO", "QCOM", "AVGO", "MU",
    "TSM", "ASML", "ARM", "PLTR", "SNOW", "DDOG", "NET", "CRWD", "PANW",
    "ZS", "OKTA", "MDB", "TEAM", "SHOP", "SQ", "PYPL", "COIN", "HOOD",
    "CRCL", "RBLX", "U", "ROKU", "ABNB", "UBER", "LYFT", "DASH",
    "JPM", "BAC", "GS", "MS", "WFC", "C", "V", "MA", "AXP",
    "BRK", "BRKA", "BRKB", "BABA", "BIDU", "PDD", "JD", "NIO", "XPEV",
    "BA", "LMT", "RTX", "NOC", "GD", "GE", "F", "GM", "RIVN", "LCID",
    "WMT", "TGT", "COST", "HD", "LOW", "MCD", "SBUX", "NKE", "DIS",
    "KO", "PEP", "JNJ", "PFE", "MRK", "ABBV", "LLY", "UNH",
    "XOM", "CVX", "COP", "OXY", "SLB", "HAL", "MPC", "VLO",
}

# Macro/FX (rare). Map to commodities only when the symbol is explicitly a
# commodity; otherwise macro flows to crypto as a safe default (no FX tab exists).
_MACRO_TO_INDEX = {"TOTAL2", "OTHERS", "BTCD"}  # crypto-aggregate macro markers
_MACRO_TO_COMMODITY: set[str] = set()
_MACRO_TO_FX: set[str] = set()  # placeholder; FX has no dedicated tab

# Stable exception map for known symbols that are frequently mis-tagged upstream.
_EXCEPTION_CATEGORY_BY_SYMBOL: dict[str, str] = {
    "CBRS": "pre_ipo",
    "CEREBRAS": "pre_ipo",
    "SPACEX": "pre_ipo",
    "OPENAI": "pre_ipo",
    "ANTHROPIC": "pre_ipo",
    "ROBOT": "themes",
    "SEMI": "themes",
}


def _symbol_matches_override(sym: str, symbols: set[str]) -> bool:
    """Match exact override symbols and common prefixed variants (e.g. US:SP500)."""
    if sym in symbols:
        return True
    return any(sym.endswith(f":{s}") for s in symbols)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def categorize_asset(asset: ScreenerAsset) -> tuple[str, str]:
    """
    Return (asset_type, category_source) for a ScreenerAsset.

    asset_type ∈ TAB_ORDER
    category_source ∈ {"hyperliquid_category", "annotation", "fallback"}
    """
    tags_lower = {t.lower() for t in (asset.tags or [])}
    dex = (asset.dex or "").lower()
    sym = (asset.display_name or asset.coin or "").upper()
    # Strip HIP-3 prefix if present (e.g. "xyz:TSLA" → "TSLA")
    if ":" in sym:
        sym = sym.split(":", 1)[1]

    # ── 1. Stable explicit non-stock overrides (hard precedence) ───────────
    if _symbol_matches_override(sym, _THEME_KEYWORDS):
        return "themes", "annotation"
    if sym in _EXCEPTION_CATEGORY_BY_SYMBOL:
        return _EXCEPTION_CATEGORY_BY_SYMBOL[sym], "annotation"
    if _symbol_matches_override(sym, _COMMODITY_OVERRIDE_SYMBOLS):
        return "commodities", "annotation"
    if _symbol_matches_override(sym, _INDEX_OVERRIDE_SYMBOLS):
        return "indices", "annotation"

    # ── 2. Strong Hyperliquid metadata tags (specific only) ────────────────
    if "pre-ipo" in tags_lower or "pre_ipo" in tags_lower:
        return "pre_ipo", "hyperliquid_category"
    if "commodity" in tags_lower:
        return "commodities", "hyperliquid_category"
    if "theme" in tags_lower:
        return "themes", "hyperliquid_category"
    if "index" in tags_lower:
        return "indices", "hyperliquid_category"
    if "crypto" in tags_lower:
        return "crypto", "hyperliquid_category"

    # ── 3. DEX prefix hint (annotation-level) ──────────────────────────────
    # HL HIP-3 DEX prefixes carry a strong category signal even if asset-level
    # tags missed it (e.g. new ticker not yet in the curated keyword list).
    if dex.startswith("hl-"):
        prefix = dex[3:]
        if prefix in ("xyz", "flx", "cash"):
            # Refine with symbol-level keywords before defaulting to stocks_etfs
            if sym in _COMMODITY_KEYWORDS:
                return "commodities", "annotation"
            if sym in _INDEX_KEYWORDS:
                return "indices", "annotation"
            if sym in _THEME_KEYWORDS:
                return "themes", "annotation"
            if sym in _PREIPO_KEYWORDS:
                return "pre_ipo", "annotation"
            return "stocks_etfs", "annotation"
        if prefix == "vntl":
            return "pre_ipo", "annotation"
        if prefix == "abcd":
            return "indices", "annotation"
        if prefix == "km":
            # macro DEX — refine, otherwise treat as indices (closest analog)
            if sym in _COMMODITY_KEYWORDS:
                return "commodities", "annotation"
            return "indices", "annotation"
        if prefix in ("hyna", "para"):
            return "crypto", "annotation"

    # ── 4. Generic equity tags only after all non-stock exclusions ─────────
    if "equity" in tags_lower:
        return "stocks_etfs", "hyperliquid_category"

    # ── 5. Symbol/name fallback ────────────────────────────────────────────
    if sym in _PREIPO_KEYWORDS:
        return "pre_ipo", "fallback"
    if sym in _THEME_KEYWORDS:
        return "themes", "fallback"
    if sym in _COMMODITY_KEYWORDS:
        return "commodities", "fallback"
    if sym in _INDEX_KEYWORDS:
        return "indices", "fallback"
    if sym in _EQUITY_KEYWORDS:
        return "stocks_etfs", "fallback"

    # ── 6. Default: crypto ─────────────────────────────────────────────────
    # Native HL perps (BTC, ETH, SOL, HYPE, ...) and spot markets all land here.
    return "crypto", "fallback"


def asset_to_matrix_row(asset: ScreenerAsset, rank: Optional[int] = None) -> dict:
    """
    Convert a ScreenerAsset to the Market Matrix row shape expected by the
    frontend Hyperliquid screener tabs.

    Field naming matches the Market Matrix contract (snake_case for derived
    metric fields, distinct from the camelCase /snapshot ScreenerRow).
    """
    asset_type, category_source = categorize_asset(asset)

    mark = asset.mark_px
    oracle = asset.oracle_px
    mid = asset.mid_px
    prev = asset.prev_day_px
    fund = asset.funding

    change_24h_pct = asset.pct_change_24h  # already in %
    funding_ann_pct = (fund * 8760 * 100) if fund is not None else None

    premium_pct = None
    if asset.premium is not None:
        premium_pct = round(asset.premium * 100, 6)

    mark_oracle_pct = asset.distance_mark_oracle_pct  # already in %

    vol_score = None
    if asset.volatility_score is not None:
        vol_score = round(asset.volatility_score / 100, 4)

    signal = None
    if asset.composite_signal_score is not None:
        signal = round(asset.composite_signal_score / 100, 4)

    agent_score = None
    if asset.overall_score is not None:
        agent_score = round(asset.overall_score / 100, 4)

    annotation = None
    if asset.dex and asset.dex.startswith("hl-"):
        annotation = asset.dex

    # OI cap state set elsewhere; surfaced when available
    oi_cap_status = None
    oi_cap_util_pct = None

    return {
        "coin":                   asset.display_name or asset.coin,
        "display_name":           asset.display_name or asset.coin,
        "canonical_coin_id":      asset.canonical_coin_id or asset.coin,
        "asset_type":             asset_type,
        "category_source":        category_source,
        "market_type":            asset.market_type,
        "dex":                    asset.dex,
        "annotation":             annotation,
        "tags":                   list(asset.tags or []),

        "mark":                   mark,
        "oracle":                 oracle,
        "mid":                    mid,
        "prev_day_px":            prev,
        "change_24h_pct":         change_24h_pct,

        "funding":                fund,
        "funding_annualized_pct": round(funding_ann_pct, 4) if funding_ann_pct is not None else None,

        "open_interest_usd":      asset.open_interest_usd,
        "volume_24h_usd":         asset.day_ntl_vlm,

        "premium_pct":            premium_pct,
        "mark_oracle_pct":        mark_oracle_pct,

        "book_imbalance":         asset.orderbook_imbalance,
        "trade_imbalance":        asset.recent_trade_imbalance,

        "vol_score":              vol_score,
        "signal":                 signal,
        "agent_score":            agent_score,
        "agent_rank":             rank if rank is not None else asset.rank,

        "oi_cap_status":          oi_cap_status,
        "oi_cap_utilization_pct": oi_cap_util_pct,
        "max_leverage":           asset.max_leverage,

        "is_active":              asset.market_status == "active",
        "market_status":          asset.market_status,
    }


def build_market_matrix(
    assets: list[ScreenerAsset],
    oi_caps: Optional[dict[str, float]] = None,
    perps_at_oi_cap: Optional[set[str]] = None,
) -> dict:
    """
    Build the full Market Matrix payload from a list of ScreenerAssets.

    Returns the contract:
      {
        "tabs": { <tab>: { label, count, assets[] } },
        "all_assets_count": int,
        "warnings": [str],
      }
    `source` and `updated_at` are added by the caller.
    """
    tabs: dict[str, dict] = {
        t: {"label": TAB_LABELS[t], "count": 0, "assets": []}
        for t in TAB_ORDER
    }

    # Rank assets globally by overall_score (desc) for consistent agent_rank
    # values across tabs (mirrors how the snapshot endpoint ranks).
    ranked = sorted(
        assets,
        key=lambda a: (a.overall_score if a.overall_score is not None else -1),
        reverse=True,
    )

    warnings: list[str] = []
    fallback_count = 0
    for idx, a in enumerate(ranked):
        row = asset_to_matrix_row(a, rank=idx + 1)
        # Market Matrix crypto tab is perp-only; exclude spot rows.
        if row["asset_type"] == "crypto" and (row.get("market_type") or "").lower() == "spot":
            continue
        if row["category_source"] == "fallback" and row["asset_type"] != "crypto":
            fallback_count += 1
        # Attach OI cap state if available
        if oi_caps:
            cap = oi_caps.get(a.coin)
            oi_usd = a.open_interest_usd
            if cap and cap > 0 and oi_usd is not None:
                util = (oi_usd / cap) * 100
                row["oi_cap_utilization_pct"] = round(util, 2)
                if util >= 95:
                    row["oi_cap_status"] = "at_cap"
                elif util >= 80:
                    row["oi_cap_status"] = "near_cap"
                else:
                    row["oi_cap_status"] = "ok"
        if perps_at_oi_cap and a.coin in perps_at_oi_cap:
            row["oi_cap_status"] = "at_cap"

        tab = row["asset_type"]
        tabs[tab]["assets"].append(row)
        tabs[tab]["count"] += 1

    if fallback_count > 0:
        warnings.append(
            f"{fallback_count} non-crypto markets categorized via symbol fallback "
            "(no Hyperliquid category tag or DEX hint)"
        )

    return {
        "tabs": tabs,
        "all_assets_count": len(ranked),
        "warnings": warnings,
    }
