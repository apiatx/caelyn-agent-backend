---
name: Canonical security identity adapter
description: FMP exchange code to Caelyn Watchlist canonical prefix translation; Part G registry; Part H alias detection
---

## Three distinct identity namespaces (Part E)
- provider_symbol: FMP dotted format (IQE.L, SOI.PA)
- provider_exchange: FMP exchange code (LSE, PAR, XETRA)
- canonical_ticker: Caelyn Watchlist identity (AIM:IQE, EPA:SOI, ETR:AIXA)

## Key FMP exchange → Caelyn canonical prefix mismatches (confirmed July 2026)
- XETRA → ETR  (not XETRA:)
- PAR → EPA    (not PAR:)
- KSC → KRX    (not KSC:)
- FSX → FRA    (not FSX:)
- SHH → SHA    (not SHH:)
- TAI → TPE    (not TAI:)
- TWO → TPEX   (not TWO:)
- JPX → TYO    (not JPX:)
- SIX → SWX    (not SIX:)
- CNQ → CSE    (FMP uses CNQ for Canadian Securities Exchange)
- LSE → LON (default) or AIM (via Part G registry match)

## LSE ambiguity
FMP does not distinguish AIM sub-market from LSE main board.
Both AIM:IQE and LON:CWR appear as exchange=LSE in FMP.
Default: LON: prefix. Part G registry overrides when AIM: already exists in any Watchlist.

## Part G: existing member wins
build_canonical_registry() loads all Watchlist tickers, cached 120s.
resolve_with_registry() checks bare_symbol against registry before constructing new canonical.
_EXCHANGE_PREFIX_FAMILY defines which prefixes are in the same family (LSE→{AIM,LON,LSE}).

## Part H: exchange-family alias detection in add endpoint
watchlist_add_ticker() accepts family_aliases list.
If any alias already in tickers → returns conflict_type=exchange_family_alias.
Router calls exchange_family_aliases(canonical_ticker) before each add.

**Why:** Direct FMP exchange codes as canonical caused duplicate-identity splits
(AIM:IQE and LON:IQE both addable for same security).

**How to apply:** Always use fmp_to_canonical() / resolve_with_registry() from
services/canonical_security_adapter.py. Never use FMP exchange code directly as prefix.
