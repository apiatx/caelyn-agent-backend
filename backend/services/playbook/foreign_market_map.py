"""
Foreign Market Map — explicit, confidence-aware foreign stock coverage for Serenity discovery.

For each supported country and company:
  - country / exchange metadata
  - ADR ticker (if listed on US exchanges)
  - ETF proxy (if no direct US-tradable access)
  - data_confidence: "high" | "medium" | "low"
  - coverage_status: "full" | "partial" | "thin"

Philosophy:
  - Be explicit about what is and isn't accessible
  - Never pretend thin coverage is full
  - Prefer ADRs over ETFs; prefer full data over estimation
  - Finnhub is primary for international OHLC where supported
  - FMP used sparsely for profile/reference only
  - If support is thin, say so clearly in the response

Supported countries (v1):
  US, JP, KR, TW, NL, DE, FR, UK
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


# ── Country metadata ───────────────────────────────────────────────────────────

COUNTRY_METADATA: Dict[str, Dict[str, Any]] = {
    "US": {
        "name":           "United States",
        "exchanges":      ["NASDAQ", "NYSE", "NYSE Arca"],
        "currency":       "USD",
        "direct_tradable": True,
        "finnhub_support": True,
        "tradier_support": True,
        "fmp_support":    True,
        "data_confidence": "high",
        "coverage_status": "full",
        "notes":          "Full data support across all providers",
    },
    "JP": {
        "name":           "Japan",
        "exchanges":      ["Tokyo Stock Exchange (TSE)"],
        "currency":       "JPY",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "Native tickers (e.g. 6857.T) via Finnhub. ADRs tradable in US. Tradier not supported for native tickers.",
    },
    "KR": {
        "name":           "South Korea",
        "exchanges":      ["Korea Stock Exchange (KSE)", "KOSDAQ"],
        "currency":       "KRW",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "Native tickers (e.g. 000660.KS). Many Samsung/SK names lack liquid US ADRs. Use ETF proxies for broad exposure.",
    },
    "TW": {
        "name":           "Taiwan",
        "exchanges":      ["Taiwan Stock Exchange (TWSE)"],
        "currency":       "TWD",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "TSM has liquid US ADR. Other TW names via Finnhub; US trading via OTC/ADR only.",
    },
    "NL": {
        "name":           "Netherlands",
        "exchanges":      ["Euronext Amsterdam (AMS)"],
        "currency":       "EUR",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "ASML has full US ADR (ASML). BESI (BE Semiconductor) trades OTC as BESIY. Finnhub supports Amsterdam tickers.",
    },
    "DE": {
        "name":           "Germany",
        "exchanges":      ["Frankfurt Stock Exchange (FRA)", "XETRA"],
        "currency":       "EUR",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "AIXTRON (AIXA.DE) has US ADR as AIXG. Finnhub supports FRA/XETRA tickers.",
    },
    "FR": {
        "name":           "France",
        "exchanges":      ["Euronext Paris"],
        "currency":       "EUR",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "STMicroelectronics (STM) is NYSE-listed — full US data. Other French names via Finnhub.",
    },
    "UK": {
        "name":           "United Kingdom",
        "exchanges":      ["London Stock Exchange (LSE)"],
        "currency":       "GBP",
        "direct_tradable": False,
        "finnhub_support": True,
        "tradier_support": False,
        "fmp_support":    True,
        "data_confidence": "medium",
        "coverage_status": "partial",
        "notes":          "Some UK names have US ADRs or are cross-listed on NYSE/NASDAQ. Finnhub supports LSE tickers.",
    },
}


# ── Foreign company → US access map ───────────────────────────────────────────
# Key = native ticker (e.g. "6857.T"), Value = US access metadata

FOREIGN_ACCESS_MAP: Dict[str, Dict[str, Any]] = {
    # ── Japan ──────────────────────────────────────────────────────────────────
    "6857.T": {
        "company_name":    "Advantest",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "ATEYY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH"],
        "data_confidence": "high",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "ATEYY OTC ADR — moderate liquidity. Finnhub supports native 6857.T for OHLC/news.",
    },
    "4063.T": {
        "company_name":    "Shin-Etsu Chemical",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "SHECY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "SHECY OTC ADR — thin US liquidity. Full data via Finnhub native ticker.",
    },
    "4523.T": {
        "company_name":    "Eisai Co.",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "ESALY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["XPH"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "ESALY OTC ADR — Alzheimer's drug (Leqembi) with Biogen.",
    },
    "6723.T": {
        "company_name":    "Renesas Electronics",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "RNECY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "RNECY OTC ADR. Automotive MCU leader — thin US coverage.",
    },
    "6506.T": {
        "company_name":    "Yaskawa Electric",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "YASKY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["ROBO", "IRBO"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": True,
        "notes":           "YASKY OTC ADR — very thin US liquidity. Primary via native TSE ticker.",
    },

    # ── South Korea ────────────────────────────────────────────────────────────
    "000660.KS": {
        "company_name":    "SK Hynix",
        "country":         "KR",
        "native_exchange": "KSE",
        "adr_ticker":      "HXSCL",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "KORU"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "HXSCL OTC — thin liquidity. Best exposure via KORU ETF or SOXX for sector.",
    },
    "005930.KS": {
        "company_name":    "Samsung Electronics",
        "country":         "KR",
        "native_exchange": "KSE",
        "adr_ticker":      "SSNLF",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "KORU", "EWY"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "SSNLF OTC — moderate US trading. KORU/EWY for broad Korea exposure.",
    },
    "009150.KS": {
        "company_name":    "Samsung Electro-Mechanics",
        "country":         "KR",
        "native_exchange": "KSE",
        "adr_ticker":      "SEMCY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "EWY"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": True,
        "notes":           "SEMCY OTC ADR — thin US liquidity. MLCC and FC-BGA bottleneck.",
    },

    # ── Netherlands ────────────────────────────────────────────────────────────
    "BESI.AS": {
        "company_name":    "BE Semiconductor (Besi)",
        "country":         "NL",
        "native_exchange": "AMS",
        "adr_ticker":      "BESIY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "BESIY OTC — hybrid bonding bottleneck. Finnhub supports BESI.AS native.",
    },

    # ── Germany ────────────────────────────────────────────────────────────────
    "AIXA.DE": {
        "company_name":    "AIXTRON",
        "country":         "DE",
        "native_exchange": "FRA",
        "adr_ticker":      "AIXG",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "AIXG OTC ADR — MOCVD reactor monopoly for GaN/SiC/InP.",
    },
}


# ── ETF proxy map: theme → ETF tickers ────────────────────────────────────────
# For themes where direct stock access is limited, suggest liquid ETF proxies

ETF_PROXY_MAP: Dict[str, List[str]] = {
    "semicap_supply_chain":    ["SOXX", "SMH", "SOXQ"],
    "advanced_packaging_test": ["SOXX", "SMH"],
    "memory_hbm":              ["SOXX", "KORU"],
    "photonics_cpo":           ["SOXX", "SMH"],
    "ai_infrastructure":       ["SOXX", "SMH", "BOTZ"],
    "ai_power_energy":         ["XLU", "GRID", "GEX"],
    "grid_transformers":       ["GRID", "ICLN"],
    "defense_optics":          ["ITA", "XAR", "CTRM"],
    "space_sensing":           ["UFO", "ROKT"],
    "energy_transition":       ["ICLN", "QCLN", "FAN"],
    "industrial_onshoring":    ["ROBO", "IRBO", "BOTZ"],
    "cooling_thermal":         ["SOXX", "SMH"],
}


# ── Helper functions ───────────────────────────────────────────────────────────

def get_country_meta(country_code: str) -> Optional[Dict[str, Any]]:
    """Return country metadata by ISO-2 code."""
    return COUNTRY_METADATA.get(country_code.upper())


def get_foreign_access(native_ticker: str) -> Optional[Dict[str, Any]]:
    """Return US access metadata for a foreign native ticker."""
    return FOREIGN_ACCESS_MAP.get(native_ticker)


def get_us_proxy(native_ticker: str) -> Optional[str]:
    """Return the best US proxy ticker for a foreign stock (ADR > ETF[0] > None)."""
    meta = FOREIGN_ACCESS_MAP.get(native_ticker)
    if meta is None:
        return None
    if meta.get("adr_ticker"):
        return meta["adr_ticker"]
    etf_proxies = meta.get("etf_proxies", [])
    return etf_proxies[0] if etf_proxies else None


def get_etf_proxies_for_theme(theme_id: str) -> List[str]:
    """Return ETF proxy tickers for a given theme."""
    return ETF_PROXY_MAP.get(theme_id, [])


def list_supported_countries() -> List[Dict[str, Any]]:
    """Return all supported countries with their metadata."""
    result = []
    for code, meta in COUNTRY_METADATA.items():
        result.append({
            "code":             code,
            "name":             meta["name"],
            "exchanges":        meta["exchanges"],
            "currency":         meta["currency"],
            "direct_tradable":  meta["direct_tradable"],
            "data_confidence":  meta["data_confidence"],
            "coverage_status":  meta["coverage_status"],
            "finnhub_support":  meta["finnhub_support"],
            "tradier_support":  meta["tradier_support"],
            "notes":            meta.get("notes", ""),
        })
    return result


def get_foreign_companies_by_country(country_code: str) -> List[Dict[str, Any]]:
    """Return all foreign company access entries for a given country code."""
    code = country_code.upper()
    return [
        {"native_ticker": ticker, **meta}
        for ticker, meta in FOREIGN_ACCESS_MAP.items()
        if meta.get("country") == code
    ]
