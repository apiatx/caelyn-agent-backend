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
    "IFNNY": {
        "company_name":    "Infineon Technologies",
        "country":         "DE",
        "native_exchange": "FRA",
        "adr_ticker":      "IFNNY",
        "adr_exchange":    "OTCMKTS",
        "etf_proxies":     ["SOXX", "FEZ"],
        "data_confidence": "high",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "IFNNY OTC ADR — moderate liquidity. #1 auto power semiconductor. Finnhub supports native IFX.DE.",
    },

    # ── Japan Phase 4 additions ────────────────────────────────────────────────
    "8035.T": {
        "company_name":    "Tokyo Electron (TEL)",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "TOELY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "JPXN"],
        "data_confidence": "high",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "TOELY OTC ADR — moderate liquidity. TEL is #3 semicap equipment globally. Finnhub supports native 8035.T.",
    },
    "6981.T": {
        "company_name":    "Murata Manufacturing",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "MRAAY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "MRAAY OTC ADR — thin US liquidity. MLCC global leader; best data via native TSE ticker.",
    },
    "3436.T": {
        "company_name":    "Sumco Corporation",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "JPXN"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "SUMCF OTC pink sheets — very thin US liquidity. Best via native TSE. ETF exposure via SOXX/SMH.",
    },
    "6146.T": {
        "company_name":    "Disco Corporation",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "DISCF OTC pink sheets — very thin US liquidity. Near-monopoly dicing saw maker. Best via native TSE.",
    },
    "4901.T": {
        "company_name":    "Fujifilm Holdings",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "FUJIY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "FUJIY OTC ADR — moderate liquidity. EUV photoresist and materials. Finnhub supports native 4901.T.",
    },
    "7735.T": {
        "company_name":    "SCREEN Holdings",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "DINRY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": True,
        "notes":           "DINRY OTC ADR — very thin US liquidity. Wafer clean equipment. Best via native TSE ticker.",
    },

    # ── UK Phase 4 additions ───────────────────────────────────────────────────
    "IQE.L": {
        "company_name":    "IQE plc",
        "country":         "GB",
        "native_exchange": "LSE",
        "adr_ticker":      "IQEPY",
        "adr_exchange":    "OTC",
        "etf_proxies":     [],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": True,
        "notes":           "IQEPY OTC — very thin. Compound semiconductor epi wafer specialist. Apple VCSEL and 5G RF supplier.",
    },

    # ── Phase 5 Japan additions ────────────────────────────────────────────────
    "4062.T": {
        "company_name":    "Ibiden Co.",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "IBDNY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "JPXN"],
        "data_confidence": "high",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "IBDNY OTC ADR — moderate liquidity. Near-monopoly ABF substrate. Every AI GPU depends on Ibiden. Finnhub supports native 4062.T.",
    },
    "6988.T": {
        "company_name":    "Nitto Denko",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "NDEKY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "NDEKY OTC ADR — moderate liquidity. Dicing tape and process films. Best data via native TSE ticker.",
    },
    "5802.T": {
        "company_name":    "Sumitomo Electric Industries",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "SMTOY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["JPXN"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "SMTOY OTC ADR — thin liquidity. Optical fiber and SiC materials. Best data via native 5802.T.",
    },
    "6501.T": {
        "company_name":    "Hitachi",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "HTHIY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["JPXN"],
        "data_confidence": "high",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "HTHIY OTC ADR — reasonable liquidity. Hitachi Energy is key grid/HVDC subsidiary. Finnhub supports native 6501.T.",
    },
    "6963.T": {
        "company_name":    "Rohm Semiconductor",
        "country":         "JP",
        "native_exchange": "TSE",
        "adr_ticker":      "ROHCY",
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "JPXN"],
        "data_confidence": "medium",
        "coverage_status": "partial",
        "tradeable_in_us": True,
        "notes":           "ROHCY OTC ADR — thin liquidity. SiC power device supplier; vertically integrated substrate to module.",
    },

    # ── Phase 5 Korea additions ────────────────────────────────────────────────
    "042700.KS": {
        "company_name":    "Hanmi Semiconductor",
        "country":         "KR",
        "native_exchange": "KSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "EWY"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "No liquid US ADR. HASEY OTC is very thin. ~70-80% TC bonder market share for HBM. Best via native KSE. ETF proxy EWY.",
    },
    "086390.KS": {
        "company_name":    "Wonik IPS",
        "country":         "KR",
        "native_exchange": "KSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "EWY"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "No US ADR. ALD equipment supplier to Samsung and SK Hynix. Best via native KSE or EWY ETF.",
    },

    # ── Phase 5 Taiwan additions ───────────────────────────────────────────────
    "3037.TW": {
        "company_name":    "Unimicron Technology",
        "country":         "TW",
        "native_exchange": "TWSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "EWT"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "No liquid US ADR. #2 ABF substrate globally. ETF proxy via SOXX/SMH. Best via native TWSE.",
    },
    "6239.TW": {
        "company_name":    "Powertech Technology",
        "country":         "TW",
        "native_exchange": "TWSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "EWT"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "No US ADR. Memory OSAT specialist. Best via native TWSE. ETF proxy EWT.",
    },
    "3711.TW": {
        "company_name":    "ASMedia Technology",
        "country":         "TW",
        "native_exchange": "TWSE",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "EWT"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "No US ADR. PCIe/USB controller IC for AI server motherboards. Best via native TWSE.",
    },

    # ── Phase 5 France additions ───────────────────────────────────────────────
    "SOI.PA": {
        "company_name":    "Soitec",
        "country":         "FR",
        "native_exchange": "EPA",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "FEZ"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "SOITF OTC pink sheets — very thin. Sole RF-SOI wafer supplier globally. Best via native EPA (Euronext Paris). Critical 5G supply chain.",
    },

    # ── Phase 5 Germany additions ──────────────────────────────────────────────
    "WAF.DE": {
        "company_name":    "Siltronic",
        "country":         "DE",
        "native_exchange": "FRA",
        "adr_ticker":      None,
        "adr_exchange":    "OTC",
        "etf_proxies":     ["SOXX", "SMH", "FEZ"],
        "data_confidence": "low",
        "coverage_status": "thin",
        "tradeable_in_us": False,
        "notes":           "SSLLF OTC — very thin. #3 silicon wafer globally. Best via native FRA. ETF proxy SOXX/SMH.",
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
