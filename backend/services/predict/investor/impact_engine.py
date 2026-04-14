"""
Investor Mode — Theme → Equity Impact Engine.

Deterministic, curated mappings from macro themes and market directions to:
  - bullish/bearish sectors
  - bullish/bearish stocks
  - affected asset baskets (ETFs / indices)
  - regime implications
  - narrative text templates

This file is designed to be edited and extended by hand.
All mappings are purely deterministic — no LLM calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ── Curated Sector → Stock Mappings ──────────────────────────────────────────
#
# These are the canonical watchlists.  Edit freely.

SECTOR_STOCKS: dict[str, list[str]] = {
    "Energy": [
        "XOM", "CVX", "COP", "EOG", "OXY", "PXD", "SLB", "HAL", "VLO", "MPC",
    ],
    "Defense/Aerospace": [
        "RTX", "LMT", "NOC", "GD", "BA", "HII", "LDOS", "CACI", "L3T", "MANT",
    ],
    "Airlines/Transport": [
        "DAL", "UAL", "AAL", "LUV", "JBLU", "FDX", "UPS", "EXPD",
    ],
    "Industrials": [
        "CAT", "DE", "ETN", "EMR", "HON", "ITW", "GE", "ROK", "PWR",
    ],
    "Financials": [
        "JPM", "GS", "BAC", "WFC", "MS", "C", "BLK", "AXP",
    ],
    "Regional Banks": [
        "KRE", "ZION", "CMA", "FITB", "HBAN", "RF", "KEY",
    ],
    "Small Caps": [
        "IWM", "IJR", "VBR",
    ],
    "Semiconductors": [
        "NVDA", "AMD", "AVGO", "MU", "ASML", "TSM", "INTC", "QCOM", "AMAT", "LRCX",
    ],
    "Software/Growth Tech": [
        "MSFT", "GOOGL", "META", "CRM", "SNOW", "PLTR", "SHOP", "CRWD",
    ],
    "Cybersecurity": [
        "CRWD", "PANW", "ZS", "FTNT", "S", "OKTA", "CHKP", "RPM",
    ],
    "AI Infra/Data Centers": [
        "VRT", "ANET", "DELL", "HPE", "SMCI", "WDC", "NTAP",
    ],
    "REITs/Housing": [
        "VNQ", "O", "SPG", "AMT", "DHI", "LEN", "TOL", "PHM",
    ],
    "Consumer Discretionary": [
        "AMZN", "TSLA", "NKE", "MCD", "SBUX", "TGT", "HD", "LOW", "GM", "F",
    ],
    "Utilities/Nuclear Power": [
        "CEG", "VST", "NRG", "TLN", "SMR", "OKLO", "CCJ", "DUK", "NEE", "SO",
    ],
    "Gold/Metals/Commodities": [
        "GLD", "SLV", "NEM", "GOLD", "AEM", "KGC", "FCX", "CLF", "AA",
    ],
    "Crypto Proxies": [
        "MSTR", "COIN", "MARA", "RIOT", "CLSK", "BTBT",
    ],
    "Long-Duration Growth Tech": [
        "MSFT", "GOOGL", "AMZN", "META", "NFLX", "TSLA",
    ],
    "Clean Energy": [
        "ENPH", "SEDG", "FSLR", "NEE", "BE", "PLUG",
    ],
}

# ── Asset Basket Mappings ─────────────────────────────────────────────────────

THEME_BASKETS: dict[str, list[str]] = {
    "macro_rates_inflation":       ["TLT", "SHY", "IEF", "GLD", "KRE", "XLF"],
    "geopolitics_war_trade":       ["ITA", "XLE", "GLD", "JETS", "VNQ"],
    "energy_commodities":          ["XLE", "XOP", "GLD", "SLV", "USO", "DBB"],
    "us_politics_policy":          ["XLE", "ITA", "XLF", "QQQ"],
    "ai_semis_tech":               ["SMH", "SOXX", "QQQ", "NVDA", "ARKK"],
    "crypto_risk_appetite":        ["IBIT", "FBTC", "MSTR", "COIN", "QQQ"],
    "china_taiwan_supply_chain":   ["SMH", "SOXX", "ITA", "FXI", "KWEB"],
    "defense_security":            ["ITA", "XAR", "CIBR", "HACK"],
    "consumer_labor_growth":       ["XLY", "XRT", "IWM", "KRE", "XLI"],
}

# ── Per-Theme Impact Definitions ─────────────────────────────────────────────
#
# Each entry maps (theme_id, summary_direction) → impact spec.
# summary_direction: "rising" | "falling" | "mixed" | "unstable"
# "rising" = odds of the theme event RISING (e.g. more inflation, more war)
# "falling" = odds FALLING (e.g. de-escalation, rate cuts priced out)

@dataclass
class ThemeImpact:
    bullish_sectors: list[str]
    bearish_sectors: list[str]
    bullish_stocks: list[str]
    bearish_stocks: list[str]
    baskets: list[str]
    regime_implications: list[str]
    narrative: str            # grounded 1-sentence implication
    confidence_modifier: float = 0.0  # -1 to +1 adjustment to base confidence


def _stocks_for(sectors: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for sec in sectors:
        for s in SECTOR_STOCKS.get(sec, [])[:5]:
            if s not in seen:
                out.append(s)
                seen.add(s)
    return out


# Maps: (theme_id, direction) → ThemeImpact
# direction can be "rising", "falling", "mixed", "unstable"
THEME_IMPACTS: dict[tuple[str, str], ThemeImpact] = {

    # ── Macro / Rates / Inflation ───────────────────────────────────────────
    ("macro_rates_inflation", "rising"): ThemeImpact(
        bullish_sectors=["Financials", "Energy", "Gold/Metals/Commodities"],
        bearish_sectors=["REITs/Housing", "Long-Duration Growth Tech", "Utilities/Nuclear Power"],
        bullish_stocks=_stocks_for(["Financials", "Energy"]),
        bearish_stocks=_stocks_for(["REITs/Housing", "Long-Duration Growth Tech"]),
        baskets=["GLD", "TBT", "XLF", "XLE"],
        regime_implications=["Higher-for-longer regime", "Commodity inflation pressure"],
        narrative="Rising inflation odds imply a higher-for-longer rate regime — "
                  "headwind for rate-sensitive REITs and long-duration growth, "
                  "supportive for financials and real assets.",
    ),
    ("macro_rates_inflation", "falling"): ThemeImpact(
        bullish_sectors=["REITs/Housing", "Long-Duration Growth Tech", "Small Caps"],
        bearish_sectors=["Financials", "Energy"],
        bullish_stocks=_stocks_for(["REITs/Housing", "Software/Growth Tech", "Small Caps"]),
        bearish_stocks=_stocks_for(["Financials"]),
        baskets=["TLT", "IEF", "VNQ", "IWM", "QQQ"],
        regime_implications=["Easing cycle incoming", "Risk-on", "Duration assets bid"],
        narrative="Disinflation/rate-cut odds rising — supportive for duration assets, "
                  "REITs, and rate-sensitive growth; financials face NIM compression.",
    ),

    # ── Geopolitics / War / Trade ───────────────────────────────────────────
    ("geopolitics_war_trade", "rising"): ThemeImpact(
        bullish_sectors=["Defense/Aerospace", "Energy", "Gold/Metals/Commodities", "Cybersecurity"],
        bearish_sectors=["Airlines/Transport", "Consumer Discretionary", "Industrials"],
        bullish_stocks=_stocks_for(["Defense/Aerospace", "Energy", "Gold/Metals/Commodities"]),
        bearish_stocks=_stocks_for(["Airlines/Transport", "Consumer Discretionary"]),
        baskets=["ITA", "XLE", "GLD", "CIBR"],
        regime_implications=["Geopolitical risk-off", "Energy supply shock risk", "Defense spending uplift"],
        narrative="Escalating geopolitical odds are a classic tailwind for defense and energy; "
                  "airlines and globally-exposed industrials face direct headwinds.",
    ),
    ("geopolitics_war_trade", "falling"): ThemeImpact(
        bullish_sectors=["Airlines/Transport", "Consumer Discretionary", "Industrials"],
        bearish_sectors=["Defense/Aerospace", "Gold/Metals/Commodities"],
        bullish_stocks=_stocks_for(["Airlines/Transport", "Consumer Discretionary"]),
        bearish_stocks=_stocks_for(["Defense/Aerospace", "Gold/Metals/Commodities"]),
        baskets=["JETS", "XLY", "XLI", "GLD"],
        regime_implications=["Geopolitical risk-on", "Demand recovery", "Supply chain normalization"],
        narrative="De-escalation odds rising — supply chain normalization and risk-on sentiment "
                  "benefit airlines, transports, and consumer names; defense/gold face relative fading.",
    ),

    # ── Energy / Commodities ────────────────────────────────────────────────
    ("energy_commodities", "rising"): ThemeImpact(
        bullish_sectors=["Energy", "Gold/Metals/Commodities"],
        bearish_sectors=["Airlines/Transport", "Consumer Discretionary"],
        bullish_stocks=_stocks_for(["Energy", "Gold/Metals/Commodities"]),
        bearish_stocks=_stocks_for(["Airlines/Transport", "Consumer Discretionary"]),
        baskets=["XLE", "XOP", "GLD", "USO"],
        regime_implications=["Commodity inflation", "Energy supply pressure", "Real asset bid"],
        narrative="Rising commodity odds support energy producers and real assets; "
                  "margin-sensitive consumers and airlines absorb higher input costs.",
    ),
    ("energy_commodities", "falling"): ThemeImpact(
        bullish_sectors=["Airlines/Transport", "Consumer Discretionary", "Industrials"],
        bearish_sectors=["Energy", "Gold/Metals/Commodities"],
        bullish_stocks=_stocks_for(["Airlines/Transport", "Consumer Discretionary"]),
        bearish_stocks=_stocks_for(["Energy", "Gold/Metals/Commodities"]),
        baskets=["JETS", "XLY", "XLI"],
        regime_implications=["Input cost relief", "Consumer purchasing power recovery"],
        narrative="Falling energy/commodity odds reduce input cost pressure — "
                  "direct benefit for airlines, transports, and consumer discretionary.",
    ),

    # ── US Politics / Policy ────────────────────────────────────────────────
    ("us_politics_policy", "rising"): ThemeImpact(
        bullish_sectors=["Defense/Aerospace", "Energy"],
        bearish_sectors=["Clean Energy", "Financials"],
        bullish_stocks=_stocks_for(["Defense/Aerospace", "Energy"]),
        bearish_stocks=_stocks_for(["Clean Energy", "Financials"]),
        baskets=["ITA", "XLE", "ICLN"],
        regime_implications=["Policy uncertainty", "Deregulation potential", "Defense budget expansion"],
        narrative="Shifting US political odds imply defense/energy deregulation tailwinds "
                  "and potential headwinds for clean energy subsidies and financial regulation.",
    ),
    ("us_politics_policy", "falling"): ThemeImpact(
        bullish_sectors=["Clean Energy", "Consumer Discretionary"],
        bearish_sectors=["Defense/Aerospace", "Energy"],
        bullish_stocks=_stocks_for(["Clean Energy", "Consumer Discretionary"]),
        bearish_stocks=_stocks_for(["Defense/Aerospace"]),
        baskets=["ICLN", "XLY"],
        regime_implications=["Policy stability", "Regulatory status quo"],
        narrative="Political risk easing reduces regulatory uncertainty — "
                  "generally supportive for sectors dependent on stable policy frameworks.",
    ),

    # ── AI / Semis / Tech ───────────────────────────────────────────────────
    ("ai_semis_tech", "rising"): ThemeImpact(
        bullish_sectors=["AI Infra/Data Centers", "Semiconductors", "Software/Growth Tech"],
        bearish_sectors=["Consumer Hardware"],
        bullish_stocks=_stocks_for(["AI Infra/Data Centers", "Semiconductors"]),
        bearish_stocks=[],
        baskets=["SMH", "SOXX", "QQQ", "NVDA"],
        regime_implications=["AI capex cycle", "Compute demand surge", "Semiconductor leadership"],
        narrative="Rising AI/chip market odds reinforce the AI capex supercycle narrative — "
                  "infrastructure enablers (semis, data centers) lead.",
    ),
    ("ai_semis_tech", "falling"): ThemeImpact(
        bullish_sectors=["Consumer Hardware", "Legacy Tech"],
        bearish_sectors=["AI Infra/Data Centers", "Semiconductors"],
        bullish_stocks=[],
        bearish_stocks=_stocks_for(["Semiconductors"]),
        baskets=["SMH", "SOXX"],
        regime_implications=["AI capex pause risk", "Export control headwind"],
        narrative="AI/chip restriction odds rising — direct headwind for semiconductor "
                  "companies with China exposure and data center supply chains.",
    ),

    # ── Crypto / Risk Appetite ──────────────────────────────────────────────
    ("crypto_risk_appetite", "rising"): ThemeImpact(
        bullish_sectors=["Crypto Proxies", "Software/Growth Tech", "Small Caps"],
        bearish_sectors=["Gold/Metals/Commodities"],
        bullish_stocks=_stocks_for(["Crypto Proxies"]),
        bearish_stocks=_stocks_for(["Gold/Metals/Commodities"]),
        baskets=["IBIT", "FBTC", "MSTR", "COIN", "QQQ"],
        regime_implications=["Risk-on", "Liquidity expansion", "Speculative appetite high"],
        narrative="Rising crypto odds signal elevated risk appetite — "
                  "supportive for high-beta growth, small caps, and speculative assets.",
    ),
    ("crypto_risk_appetite", "falling"): ThemeImpact(
        bullish_sectors=["Gold/Metals/Commodities", "Utilities/Nuclear Power"],
        bearish_sectors=["Crypto Proxies", "Software/Growth Tech"],
        bullish_stocks=_stocks_for(["Gold/Metals/Commodities"]),
        bearish_stocks=_stocks_for(["Crypto Proxies"]),
        baskets=["GLD", "TLT", "VNQ"],
        regime_implications=["Risk-off", "Flight to safety", "Liquidity contraction"],
        narrative="Crypto/risk appetite fading — signal of broader risk-off; "
                  "defensive positions in gold and utilities outperform.",
    ),

    # ── China / Taiwan / Supply Chain ───────────────────────────────────────
    ("china_taiwan_supply_chain", "rising"): ThemeImpact(
        bullish_sectors=["Defense/Aerospace", "Cybersecurity", "Semiconductors (US domestic)"],
        bearish_sectors=["Consumer Electronics", "Industrials", "Airlines/Transport"],
        bullish_stocks=_stocks_for(["Defense/Aerospace", "Cybersecurity"]),
        bearish_stocks=["AAPL", "QCOM", "AMAT", "DAL", "UAL"],
        baskets=["ITA", "CIBR", "SMH", "FXI"],
        regime_implications=["Supply chain risk-off", "Taiwan premium in semis", "Defense spending uplift"],
        narrative="Rising China/Taiwan tension odds stress semiconductor supply chains and "
                  "globally-exposed industrials while boosting defense and cybersecurity.",
    ),
    ("china_taiwan_supply_chain", "falling"): ThemeImpact(
        bullish_sectors=["Consumer Electronics", "Industrials", "Airlines/Transport"],
        bearish_sectors=["Defense/Aerospace"],
        bullish_stocks=["AAPL", "QCOM", "DAL", "UAL"],
        bearish_stocks=_stocks_for(["Defense/Aerospace"]),
        baskets=["FXI", "KWEB", "XLI", "JETS"],
        regime_implications=["Supply chain normalization", "China demand re-coupling"],
        narrative="China/Taiwan de-escalation reduces semiconductor risk premium — "
                  "supportive for tech supply chains and Asia-exposed transports.",
    ),

    # ── Defense / Security ──────────────────────────────────────────────────
    ("defense_security", "rising"): ThemeImpact(
        bullish_sectors=["Defense/Aerospace", "Cybersecurity"],
        bearish_sectors=[],
        bullish_stocks=_stocks_for(["Defense/Aerospace", "Cybersecurity"]),
        bearish_stocks=[],
        baskets=["ITA", "XAR", "CIBR", "HACK"],
        regime_implications=["Defense budget expansion", "Cybersecurity demand surge"],
        narrative="Rising defense/security odds are a direct tailwind for defense primes and "
                  "cybersecurity vendors — government contract pipeline expansion expected.",
    ),
    ("defense_security", "falling"): ThemeImpact(
        bullish_sectors=["Consumer Discretionary", "Industrials"],
        bearish_sectors=["Defense/Aerospace"],
        bullish_stocks=_stocks_for(["Consumer Discretionary"]),
        bearish_stocks=_stocks_for(["Defense/Aerospace"]),
        baskets=["XLY", "XLI"],
        regime_implications=["Defense budget normalization", "Peace dividend"],
        narrative="Defense risk easing signals a potential peace dividend — "
                  "budget reallocation to civilian infrastructure benefits industrials.",
    ),

    # ── Consumer / Labor / Growth ───────────────────────────────────────────
    ("consumer_labor_growth", "rising"): ThemeImpact(
        bullish_sectors=["Consumer Discretionary", "Financials", "Small Caps", "Industrials"],
        bearish_sectors=["Gold/Metals/Commodities", "Utilities/Nuclear Power"],
        bullish_stocks=_stocks_for(["Consumer Discretionary", "Financials"]),
        bearish_stocks=_stocks_for(["Gold/Metals/Commodities"]),
        baskets=["XLY", "XLF", "IWM", "XLI"],
        regime_implications=["Growth acceleration", "Risk-on", "Cyclical leadership"],
        narrative="Improving growth odds favor cyclical sectors — consumer, financials, "
                  "and small caps lead in acceleration environments.",
    ),
    ("consumer_labor_growth", "falling"): ThemeImpact(
        bullish_sectors=["Gold/Metals/Commodities", "Utilities/Nuclear Power", "REITs/Housing"],
        bearish_sectors=["Consumer Discretionary", "Financials", "Small Caps"],
        bullish_stocks=_stocks_for(["Gold/Metals/Commodities", "Utilities/Nuclear Power"]),
        bearish_stocks=_stocks_for(["Consumer Discretionary", "Financials"]),
        baskets=["GLD", "TLT", "XLU", "VNQ"],
        regime_implications=["Slowdown/recession risk", "Defensive rotation", "Risk-off"],
        narrative="Slowdown odds rising trigger defensive rotation — "
                  "utilities, gold, and bonds outperform in contraction environments.",
    ),
}

# Fallback for "mixed" and "unstable" — use same as "rising" but with lower confidence
for _theme_id in [t.split(",")[0].strip().strip("(\"'") for t in
                   [str(k) for k in THEME_IMPACTS.keys()]]:
    for _dir in ("mixed", "unstable"):
        _rising_key = (_theme_id, "rising")
        if _rising_key in THEME_IMPACTS and (_theme_id, _dir) not in THEME_IMPACTS:
            _base = THEME_IMPACTS[_rising_key]
            THEME_IMPACTS[(_theme_id, _dir)] = ThemeImpact(
                bullish_sectors=_base.bullish_sectors[:2],
                bearish_sectors=_base.bearish_sectors[:2],
                bullish_stocks=_base.bullish_stocks[:3],
                bearish_stocks=_base.bearish_stocks[:3],
                baskets=_base.baskets[:3],
                regime_implications=[f"Mixed signals — {_dir} outlook"],
                narrative=(
                    f"Conflicting {_theme_id.replace('_', ' ')} signals make directional "
                    f"conviction difficult — monitor for resolution before acting."
                ),
                confidence_modifier=-0.25,
            )


def get_theme_impact(theme_id: str, direction: str) -> Optional[ThemeImpact]:
    """Return the impact spec for a (theme_id, direction) pair."""
    return THEME_IMPACTS.get((theme_id, direction)) or THEME_IMPACTS.get((theme_id, "mixed"))
