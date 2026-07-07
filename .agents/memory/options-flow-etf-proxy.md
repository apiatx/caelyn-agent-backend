---
name: Options Flow ETF proxy classification
description: warm_up_from_theme_universe fixes the ETF misclassification root cause; source precedence rules
---

## The bug
SOXX/PSI/XSD/XLE had isEtf=None in screener_fundamentals_cache + sector="Financial Services".
sector_inference wrote "stock" to LKG. Both warm_up_from_db (existing_src != "sector_inference" check)
and classify_symbols_background (source != sector_inference check) then permanently skipped them.
IAU: get_etf_flag returned "stock" because isEtf=None + companyName → wrong fallback.

## The fix
Three-pass startup sequence:
  1. warm_up_from_theme_universe() — proxy_symbols=ETF, candidate_symbols=stock (highest authority)
  2. warm_up_from_db() — explicit isEtf=True always upgrades; sector_inference only fires for unclassified
  3. classify_symbols_background (async) — includes lkg-sourced symbols from required universe

get_etf_flag precedence: isEtf=True or isFund=True → etf; isEtf=False and not isFund and companyName → stock; isEtf=None → unknown (never infer stock from companyName alone, trusts like IAU have companyName).

**Why:** ETF trusts (IAU) and some ETFs (SOXX/PSI/XSD/XLE) present with isEtf=None in FMP profile responses. Sector "Financial Services" is common for ETFs in FMP data — it is NOT a stock indicator.

**How to apply:** If adding new ETF proxy symbols to THEME_RS_UNIVERSE, they will be auto-classified as etf by warm_up_from_theme_universe(). Never rely solely on FMP isEtf field for ETF proxies.
