"""
Hyperliquid Market Matrix categorizer.

Assigns every ScreenerAsset in the live Hyperliquid universe to exactly one of:
  stocks_etfs | crypto | commodities | indices | pre_ipo | themes

Classification precedence (enforced inside categorize_asset):
  1. Stable backend exceptions (_EXCEPTION_CATEGORY_BY_SYMBOL)
     Known symbols mis-tagged upstream; always wins.
  2. Commodity overrides (_COMMODITY_OVERRIDES)
     Hard commodity/agri/metal/energy futures — BEFORE any "pre-ipo" tag.
  3. Index overrides (_INDEX_OVERRIDES)
     Hard benchmark/index symbols — BEFORE any "equity" tag.
  4. Theme/basket overrides (_THEME_OVERRIDES)
     Theme/sector/basket markets — BEFORE any "pre-ipo" tag.
     (Prevents vntl DEX theme baskets from leaking into pre_ipo.)
  5. Strong specific HIP-3 category tags:
     pre-ipo → pre_ipo; commodity → commodities; theme → themes; index → indices.
  6. DEX prefix hints (annotation-level, per-symbol refined):
     xyz/flx/cash → stocks_etfs (with keyword refinement)
     vntl → pre_ipo (with commodity/theme/index refinement)
     abcd → indices
     km → equity/stock/commodity/theme check; default stocks_etfs
     hyna/para → crypto
  7. Generic "equity" tag → stocks_etfs (only after all non-stock overrides).
  8. Symbol/name fallback against curated keyword lists.
  9. Default: crypto (native HL perps; spot markets filtered at endpoint level).
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
# Hard symbol overrides  (steps 1–4 in categorize_asset)
#
# These run BEFORE any tag-based logic so noisy upstream metadata
# (e.g. a vntl DEX basket tagged "pre-IPO", or a commodity tagged "equity")
# cannot force an asset into the wrong tab.
# ─────────────────────────────────────────────────────────────────────────────

# Step 1 — known mis-tagged assets whose upstream tag is reliably wrong.
_EXCEPTION_CATEGORY_BY_SYMBOL: dict[str, str] = {
    # Pre-IPO private companies with noisy DEX equity tags
    "CBRS":      "pre_ipo",
    "CEREBRAS":  "pre_ipo",
    "SPACEX":    "pre_ipo",
    "OPENAI":    "pre_ipo",
    "ANTHROPIC": "pre_ipo",
    # Theme/sector baskets that sit on non-theme DEXes
    "ROBOT":     "themes",
    "SEMI":      "themes",
}

# Step 2 — commodity futures / spot / agri / metals.
# MUST fire before the "pre-ipo" tag check so vntl-hosted commodity perps
# (e.g. WHEAT, SOY) are not routed to pre_ipo.
_COMMODITY_OVERRIDES: frozenset[str] = frozenset({
    # Energy / crude
    "CL", "WTI", "BRENTOIL", "BRENT", "OIL", "NATGAS",
    # Metals
    "GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM",
    # Agri / softs
    "WHEAT", "SOY", "SOYBEAN", "CORN", "COCOA", "COFFEE", "SUGAR",
})

# Step 3 — benchmark / index / country-index markets.
# MUST fire before the "equity" tag check so known index perps are not
# routed to stocks_etfs when tagged ["perp", "equity"].
_INDEX_OVERRIDES: frozenset[str] = frozenset({
    "SP500", "US500", "USA500", "USA100", "USTECH", "NASDAQ", "XYZ100",
    "JP225", "KR200", "EWY", "EWJ", "EWZ",
})

# Step 4 — theme/sector/basket markets.
# MUST fire before the "pre-ipo" tag check so vntl-hosted basket perps
# (e.g. MAG7, BIOTECH, ENERGY) are not routed to pre_ipo.
# Note: XAI is intentionally absent — it is a native Hyperliquid crypto token.
_THEME_OVERRIDES: frozenset[str] = frozenset({
    "MAG7",
    "ENERGY", "DEFENSE", "NUCLEAR", "INFOTECH",
    "BIOTECH", "SEMIS",
    "AI", "DATACENTER", "POWER", "URANIUM",
    "DRAM", "MEMORY", "QUANTUM", "PHOTONICS", "CPO",
})


# ─────────────────────────────────────────────────────────────────────────────
# Fallback keyword lists  (step 8 — only reached when tag/DEX logic is
# inconclusive).  These must stay consistent with the override sets above.
# ─────────────────────────────────────────────────────────────────────────────

_COMMODITY_KEYWORDS: frozenset[str] = frozenset({
    "CL",
    "GOLD", "XAU", "SILVER", "XAG", "PLATINUM", "XPT", "PALLADIUM", "XPD",
    "COPPER", "OIL", "WTI", "BRENT", "BRENTOIL", "USOIL", "USENERGY",
    "NATGAS", "GAS", "NGAS",
    "WHEAT", "SOY", "SOYBEAN", "CORN", "COCOA", "COFFEE", "SUGAR",
})

_INDEX_KEYWORDS: frozenset[str] = frozenset({
    "SPX", "SP500", "SPY", "USA500", "US500", "USA100", "NDX", "QQQ",
    "USTECH", "NASDAQ", "XYZ100",
    "DJI", "DOW", "RUSSELL", "RUT", "IWM", "SMALL2000", "VIX",
    "JP225", "KR200",
    "EWY", "EWZ", "EWJ", "EFA", "EEM", "FXI",
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE",
    "USBOND",
})

# Theme/basket keyword set — XAI is excluded (it is a Hyperliquid crypto perp).
_THEME_KEYWORDS: frozenset[str] = frozenset({
    "ROBOT", "SEMI", "SEMIS",
    "MAG7", "INFOTECH", "NUCLEAR", "DEFENSE", "ENERGY",
    "BIOTECH", "AI", "DATACENTER", "POWER", "URANIUM",
    "DRAM", "MEMORY", "QUANTUM", "PHOTONICS", "CPO",
})

# Private / pre-IPO company names.
# XAI removed — on Hyperliquid, XAI is a native crypto token, not a pre-IPO.
_PREIPO_KEYWORDS: frozenset[str] = frozenset({
    "SPACEX", "OPENAI", "ANTHROPIC", "CEREBRAS", "STRIPE", "DATABRICKS",
    "DATBRICKS", "PERPLEXITY", "RIPPLING", "FIGMA",
    "FIGURE", "NEURALINK", "RIPPLE", "EPICGAMES",
    "BYTEDANCE", "TIKTOK", "DISCORD", "KLARNA",
    "SHEIN", "REVOLUT", "CHIME", "PLAID",
})

# Single-company public equities / ETFs.
# Includes common US, Asian, and European names that appear on HL HIP-3 DEXes
# without reliable equity tags (e.g. km-DEX stocks tagged only "macro").
_EQUITY_KEYWORDS: frozenset[str] = frozenset({
    # US mega-cap tech
    "AAPL", "MSFT", "NVDA", "AMD", "INTC", "GOOG", "GOOGL", "META", "AMZN",
    "TSLA", "NFLX", "ORCL", "CRM", "ADBE", "CSCO", "QCOM", "AVGO", "MU",
    "TSM", "ASML", "ARM", "PLTR", "SNOW", "DDOG", "NET", "CRWD", "PANW",
    "ZS", "OKTA", "MDB", "TEAM", "SHOP", "SQ", "PYPL", "COIN", "HOOD",
    "CRCL", "RBLX", "U", "ROKU", "ABNB", "UBER", "LYFT", "DASH",
    # US finance
    "JPM", "BAC", "GS", "MS", "WFC", "C", "V", "MA", "AXP",
    "BRK", "BRKA", "BRKB",
    # US consumer / industrial / healthcare
    "WMT", "TGT", "COST", "HD", "LOW", "MCD", "SBUX", "NKE", "DIS",
    "KO", "PEP", "JNJ", "PFE", "MRK", "ABBV", "LLY", "UNH", "BMRN",
    "BA", "LMT", "RTX", "NOC", "GD", "GE", "F", "GM", "RIVN", "LCID",
    "RKLB", "DKNG", "GME", "HIMS", "MSTR", "MRVL", "ZM",
    # US energy (single-company, not commodity futures)
    "XOM", "CVX", "COP", "OXY", "SLB", "HAL", "MPC", "VLO",
    # EM / Asian / European single stocks
    "BABA", "BIDU", "PDD", "JD", "NIO", "XPEV",
    "TENCENT", "XIAOMI", "HYUNDAI", "SMSN",
    # Other HIP-3 equities
    "SNDK", "EBAY", "HOOD", "URNM", "CAR", "LITE", "BIRD", "USAR",
    "SKHX", "SMSN", "HYUNDAI",
})

# Symbols that are crypto-domain macro markers, not index/commodity assets.
_MACRO_TO_INDEX: frozenset[str] = frozenset({"TOTAL2", "OTHERS", "BTCD"})


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def categorize_asset(asset: ScreenerAsset) -> tuple[str, str]:
    """
    Return (asset_type, category_source) for a ScreenerAsset.

    asset_type     ∈ TAB_ORDER
    category_source ∈ {"hyperliquid_category", "annotation", "fallback"}
    """
    tags_lower = {t.lower() for t in (asset.tags or [])}
    dex = (asset.dex or "").lower()
    sym = (asset.display_name or asset.coin or "").upper()
    # Strip HIP-3 DEX prefix if present (e.g. "xyz:TSLA" → "TSLA")
    if ":" in sym:
        sym = sym.split(":", 1)[1]

    # ── 1. Stable backend exceptions ──────────────────────────────────────────
    # Known assets whose upstream HIP-3 tag is systematically wrong.
    if sym in _EXCEPTION_CATEGORY_BY_SYMBOL:
        return _EXCEPTION_CATEGORY_BY_SYMBOL[sym], "annotation"

    # ── 2. Commodity symbol overrides ─────────────────────────────────────────
    # Fire before the "pre-ipo" tag check so vntl-hosted commodity perps
    # (WHEAT, SOY, CORN, COCOA, …) are never routed to pre_ipo.
    if sym in _COMMODITY_OVERRIDES:
        return "commodities", "annotation"

    # ── 3. Index symbol overrides ─────────────────────────────────────────────
    # Fire before the "equity" tag check so index perps tagged ["perp","equity"]
    # are never routed to stocks_etfs.
    if sym in _INDEX_OVERRIDES:
        return "indices", "annotation"

    # ── 4. Theme/basket symbol overrides ──────────────────────────────────────
    # Fire before the "pre-ipo" tag check so vntl-hosted basket perps
    # (MAG7, BIOTECH, ENERGY, DEFENSE, …) are never routed to pre_ipo.
    if sym in _THEME_OVERRIDES:
        return "themes", "annotation"

    # ── 5. Strong specific HIP-3 category tags ────────────────────────────────
    # At this point all known commodity/index/theme symbols have been handled,
    # so these tag checks are reliable for the remaining population.
    if "pre-ipo" in tags_lower or "pre_ipo" in tags_lower:
        return "pre_ipo", "hyperliquid_category"
    if "commodity" in tags_lower:
        return "commodities", "hyperliquid_category"
    if "theme" in tags_lower:
        return "themes", "hyperliquid_category"
    if "index" in tags_lower:
        return "indices", "hyperliquid_category"

    # ── 6. DEX prefix hint (annotation-level, per-symbol refined) ────────────
    if dex.startswith("hl-"):
        prefix = dex[3:]

        if prefix in ("xyz", "flx", "cash"):
            # These DEXes are primarily equity/stock perps.
            # Refine with symbol-level keywords before defaulting to stocks_etfs.
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
            # vntl is primarily pre-IPO, but also hosts theme baskets and
            # agri commodities. Hard overrides (steps 1–4) handle the known
            # cases; anything reaching here without a match is treated as
            # pre-IPO (true private company markets not yet in the override sets).
            if sym in _COMMODITY_KEYWORDS:
                return "commodities", "annotation"
            if sym in _THEME_KEYWORDS:
                return "themes", "annotation"
            if sym in _INDEX_KEYWORDS:
                return "indices", "annotation"
            return "pre_ipo", "annotation"

        if prefix == "abcd":
            return "indices", "annotation"

        if prefix == "km":
            # km is a "macro" DEX hosting equities, true indices, FX, and macro
            # markers.  True indices on km always carry an "index" tag and were
            # already handled at step 5.  Remaining assets:
            #   • equity-tagged stocks (NVDA, GOOGL, TSLA, …) → stocks_etfs
            #   • stocks without equity tag (TENCENT, XIAOMI, RTX, …) → stocks_etfs
            #   • known commodities → commodities
            #   • known themes → themes
            #   • unknowns (EUR, GLDMINE, …) → stocks_etfs (safer default; no FX tab)
            if sym in _COMMODITY_KEYWORDS:
                return "commodities", "annotation"
            if sym in _THEME_KEYWORDS:
                return "themes", "annotation"
            if "equity" in tags_lower or sym in _EQUITY_KEYWORDS:
                return "stocks_etfs", "annotation"
            # Unknown km-DEX asset (TENCENT, XIAOMI, RTX, EUR, etc. without
            # equity tag): default stocks_etfs — true indices are caught by
            # their "index" tag at step 5 before reaching this handler.
            return "stocks_etfs", "annotation"

        if prefix in ("hyna", "para"):
            return "crypto", "annotation"

    # ── 7. Generic "equity" tag ───────────────────────────────────────────────
    # Only reached after all symbol-level overrides and DEX hints above.
    # Safe to trust here because commodities/indices/themes are already gone.
    if "equity" in tags_lower:
        return "stocks_etfs", "hyperliquid_category"

    # ── 8. Symbol / name keyword fallback ─────────────────────────────────────
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

    # ── 9. Default: crypto ────────────────────────────────────────────────────
    # Native HL perps (BTC, ETH, SOL, HYPE, XAI, …) land here.
    # Spot market filtering is enforced at the endpoint/router level.
    return "crypto", "fallback"


def asset_to_matrix_row(asset: ScreenerAsset, rank: Optional[int] = None) -> dict:
    """
    Convert a ScreenerAsset to the Market Matrix row shape expected by the
    frontend Hyperliquid screener tabs.
    """
    asset_type, category_source = categorize_asset(asset)

    mark = asset.mark_px
    oracle = asset.oracle_px
    mid = asset.mid_px
    prev = asset.prev_day_px
    fund = asset.funding

    change_24h_pct = asset.pct_change_24h
    funding_ann_pct = (fund * 8760 * 100) if fund is not None else None

    premium_pct = None
    if asset.premium is not None:
        premium_pct = round(asset.premium * 100, 6)

    mark_oracle_pct = asset.distance_mark_oracle_pct

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

    Returns:
      {
        "tabs": { <tab>: { label, count, assets[] } },
        "all_assets_count": int,
        "warnings": [str],
      }
    `source` and `updated_at` are added by the caller (router).
    """
    tabs: dict[str, dict] = {
        t: {"label": TAB_LABELS[t], "count": 0, "assets": []}
        for t in TAB_ORDER
    }

    ranked = sorted(
        assets,
        key=lambda a: (a.overall_score if a.overall_score is not None else -1),
        reverse=True,
    )

    warnings: list[str] = []
    fallback_count = 0
    spot_excluded = 0
    for idx, a in enumerate(ranked):
        row = asset_to_matrix_row(a, rank=idx + 1)

        # Crypto tab = perpetuals only.
        # Exclude native Hyperliquid spot listings (market_type=="spot" or
        # canonical_coin_id like "@123") from the crypto tab.
        if row["asset_type"] == "crypto":
            is_spot = (
                (a.market_type or "").lower() == "spot"
                or "spot" in {t.lower() for t in (a.tags or [])}
                or str(a.canonical_coin_id or "").startswith("@")
            )
            if is_spot:
                spot_excluded += 1
                continue

        if row["category_source"] == "fallback" and row["asset_type"] != "crypto":
            fallback_count += 1

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

    # ── Per-tab deduplication by normalized coin symbol ────────────────────────
    # Hyperliquid lists the same underlying asset across multiple DEX prefixes
    # (xyz:TSLA, cash:TSLA, flx:TSLA, km:TSLA → all display as "TSLA").
    # Within each tab, keep only the highest-volume row per display symbol.
    # Deduplication operates per-tab so cross-tab classification is unaffected.
    total_dupes = 0
    for tab_key, tab_data in tabs.items():
        best: dict[str, dict] = {}
        for row in tab_data["assets"]:
            key = (row.get("coin") or "").upper()
            vol = row.get("volume_24h_usd") or 0.0
            if key not in best or vol > (best[key].get("volume_24h_usd") or 0.0):
                best[key] = row
        deduped = list(best.values())
        dupes = len(tab_data["assets"]) - len(deduped)
        total_dupes += dupes
        tab_data["assets"] = deduped
        tab_data["count"] = len(deduped)

    if total_dupes > 0:
        warnings.append(
            f"{total_dupes} duplicate asset rows removed (same symbol, multiple DEX listings; "
            "highest-volume row retained per tab)"
        )

    return {
        "tabs": tabs,
        "all_assets_count": len(ranked),
        "warnings": warnings,
    }
