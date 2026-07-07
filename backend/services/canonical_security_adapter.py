"""
Canonical Security Identity Adapter — CaelynAI Watchlist

Translates between FMP provider exchange codes and Caelyn canonical ticker
prefixes — the authoritative Watchlist membership identity.

Three explicitly-distinct namespaces (Part E):
  provider_symbol    FMP native dotted-suffix format  e.g. IQE.L, SOI.PA, AIXA.DE
  provider_exchange  FMP exchange code                 e.g. LSE, PAR, XETRA
  canonical_ticker   Caelyn Watchlist identity         e.g. AIM:IQE, EPA:SOI, ETR:AIXA

Source of truth: live audit of Primary Watchlist + all saved Watchlists (July 2026).
58 prefixed tickers across 20 exchange families were audited.  Every mapping below
is confirmed against at least one live Primary Watchlist ticker.

DO NOT use FMP exchange codes directly as canonical prefixes.
This adapter is the single translation layer.
"""

from __future__ import annotations
import time

# ── Part F: FMP exchange code → Caelyn canonical prefix ─────────────────────
#
# Every entry is derived from the live Primary Watchlist (July 2026 audit).
# Format: FMP_exchange_code → canonical_prefix_used_in_Watchlist
#
# US exchanges are NOT in this map; their absence means bare-symbol identity.

_FMP_EXCHANGE_TO_PREFIX: dict[str, str] = {
    # ── Direct matches (FMP code == Caelyn prefix) ───────────────────────────
    "AMS":   "AMS",    # Euronext Amsterdam          AMS:BESI (confirmed)
    "ASX":   "ASX",    # Australian Securities Exch  ASX:AXE, ASX:EOS, ASX:EQR
    "CNQ":   "CSE",    # Canadian Securities Exch    CSE:FTEL, CSE:MAXX [FMP code: CNQ]
    "OTC":   "OTC",    # OTC Markets                 OTC:ATEYY, OTC:CGEH, OTC:HGRAF,
    #                                                 OTC:IFNNY, OTC:KRKNF, OTC:NLST
    "OSL":   "OSL",    # Oslo Stock Exchange         OSL:NAPA, OSL:SMOP
    "STO":   "STO",    # Stockholm Stock Exchange    STO:ACCON, STO:GAPW.B, STO:HTRO,
    #                                                 STO:SILEX, STO:SIVE
    "TSX":   "TSX",    # Toronto Stock Exchange      TSX:FLT, TSX:HPS.A, TSX:MAL,
    #                                                 TSX:MOLY, TSX:VNP
    "TSXV":  "TSXV",   # TSX Venture Exchange        TSXV:AAG, TSXV:XBOT
    "WSE":   "WSE",    # Warsaw Stock Exchange       WSE:VGO

    # ── FMP code differs from Caelyn prefix (corrected by audit) ─────────────
    "XETRA": "ETR",    # Deutsche Börse / XETRA      ETR:AIXA, ETR:JEN, ETR:LPK,
    #                                                 ETR:P4O, ETR:SMHN, ETR:TPE, ETR:WAF
    "FSX":   "FRA",    # Frankfurt Stock Exchange    FRA:APR, FRA:KLA
    "KSC":   "KRX",    # Korea Exchange              KRX:000660
    "PAR":   "EPA",    # Euronext Paris              EPA:ALRIB, EPA:MRN, EPA:SOI, EPA:XFAB
    "SHH":   "SHA",    # Shanghai Stock Exchange     SHA:603986, SHA:688008
    "TAI":   "TPE",    # Taiwan Stock Exchange       TPE:3661, TPE:8271
    "TWO":   "TPEX",   # Taipei Exchange             TPEX:6643
    "JPX":   "TYO",    # Tokyo Stock Exchange        TYO:6315
    "SIX":   "SWX",    # Swiss Exchange              SWX:HUBN

    # ── LSE — AMBIGUOUS (both AIM: and LON: exist in Primary Watchlist) ──────
    #
    # FMP does NOT distinguish AIM sub-market from LSE main market.
    # Confirmed: AIM:IQE, AIM:ENSI, AIM:VLX, AIM:FTC, AIM:TRT all return
    #   exchange=LSE in FMP (IQE.L, ENSI.L, etc.)
    # Confirmed: LON:CWR, LON:QQ also return exchange=LSE.
    #
    # Default mapping: LSE → "LON" (conservative — does not falsely label as AIM).
    # Part-G authority (existing member registry) overrides this for any security
    # already known to the application under AIM: or LON: prefix.
    "LSE":   "LON",

    # ── Additional exchanges (no existing Watchlist tickers, best-effort) ────
    "BSE":   "BOM",    # Bombay Stock Exchange
    "HKSE":  "HKG",    # Hong Kong Stock Exchange
    "NSE":   "NSE",    # National Stock Exchange India
    "SAO":   "BVMF",   # B3 Brazil
    "SGX":   "SGX",    # Singapore Exchange
    "SET":   "BKK",    # Stock Exchange of Thailand
    "SHZ":   "SHE",    # Shenzhen Stock Exchange
    "NZX":   "NZX",    # New Zealand Exchange
}

# US exchange codes from FMP — bare symbol, no prefix
_US_FMP_EXCHANGE_CODES: frozenset[str] = frozenset({
    "NASDAQ", "NYSE", "AMEX", "BATS", "CBOE",
    "NYSEARCA", "NYSEAMERICAN", "OTCQB", "OTCQX",
    "PCX", "ETF", "INDEX", "CRYPTO", "NEO",
})

# ── Part G: Exchange equivalence families for registry lookup ────────────────
#
# When the registry contains an existing canonical like AIM:IQE and FMP returns
# exchange=LSE for the same security, we must recognise that "AIM" is in the
# LSE family and return the existing canonical rather than generating "LON:IQE".
#
# Keys: FMP exchange codes.
# Values: frozenset of Caelyn canonical prefixes that map to this exchange.

_EXCHANGE_PREFIX_FAMILY: dict[str, frozenset[str]] = {
    "LSE":   frozenset({"AIM", "LON", "LSE"}),   # both AIM and LON sub-markets
    "PAR":   frozenset({"EPA", "PAR"}),
    "XETRA": frozenset({"ETR", "XETRA"}),
    "FSX":   frozenset({"FRA", "FSX"}),
    "KSC":   frozenset({"KRX", "KSC"}),
    "SHH":   frozenset({"SHA", "SHH"}),
    "SHZ":   frozenset({"SHE", "SHZ"}),
    "TAI":   frozenset({"TPE", "TAI"}),
    "TWO":   frozenset({"TPEX", "TWO"}),
    "JPX":   frozenset({"TYO", "JPX"}),
    "SIX":   frozenset({"SWX", "SIX"}),
    "CNQ":   frozenset({"CSE", "CNQ"}),
}

# Reverse: canonical prefix → frozenset of alternative prefixes in same family
# Used by Part-H add-mutation alias detection.
_PREFIX_TO_FAMILY: dict[str, frozenset[str]] = {}
for _exch, _family in _EXCHANGE_PREFIX_FAMILY.items():
    for _p in _family:
        _PREFIX_TO_FAMILY[_p] = _family


# ── Core translation functions ───────────────────────────────────────────────

def fmp_to_canonical(bare_symbol: str, fmp_exchange: str) -> str:
    """
    Translate (FMP bare symbol, FMP exchange code) → Caelyn canonical ticker.

    bare_symbol   FMP symbol with dotted suffix already stripped (e.g. 'IQE' not 'IQE.L')
    fmp_exchange  FMP exchange code (e.g. 'LSE', 'PAR', 'XETRA')

    US exchange  → bare symbol             e.g. 'NVDA', 'TRT'
    Non-US       → 'PREFIX:BARE_SYMBOL'    e.g. 'LON:IQE', 'EPA:SOI', 'ETR:AIXA'
    Unknown exch → 'FMP_EXCH_CODE:SYMBOL'  fallback using FMP code as prefix

    This function does NOT consult the existing-member registry.
    Use resolve_with_registry() for Part-G authority resolution.
    """
    sym = (bare_symbol or "").strip().upper()
    exch = (fmp_exchange or "").strip()
    if not sym:
        return ""
    exch_up = exch.upper()
    if exch_up in _US_FMP_EXCHANGE_CODES or not exch:
        return sym
    prefix = _FMP_EXCHANGE_TO_PREFIX.get(exch_up)
    if prefix:
        return f"{prefix}:{sym}"
    return f"{exch}:{sym}"


def resolve_with_registry(
    bare_symbol: str,
    fmp_exchange: str,
    registry: dict[str, list[str]],
) -> str:
    """
    Part G: existing Watchlist member wins.

    Before constructing a new canonical identity from FMP exchange metadata,
    check whether the same security is already known in any saved Watchlist
    under a different canonical prefix (e.g. AIM:IQE when FMP returns LSE).

    Match condition: same bare symbol AND the existing prefix belongs to the
    known equivalence family for the FMP exchange code.  This is deterministic
    — no fuzzy company-name matching.

    registry  dict built by build_canonical_registry() from all saved Watchlists
              maps bare_symbol.upper() → list of known canonical tickers

    Returns the existing canonical if found, otherwise falls back to
    fmp_to_canonical(bare_symbol, fmp_exchange).
    """
    sym = (bare_symbol or "").strip().upper()
    exch_up = (fmp_exchange or "").strip().upper()
    if not sym:
        return ""

    family = _EXCHANGE_PREFIX_FAMILY.get(exch_up)
    if family:
        for canonical in registry.get(sym, []):
            if ":" in canonical:
                prefix = canonical.split(":", 1)[0].upper()
                if prefix in family:
                    return canonical

    return fmp_to_canonical(bare_symbol, fmp_exchange)


def exchange_family_aliases(canonical_ticker: str) -> list[str]:
    """
    Part H: return alternative canonical forms in the same exchange family.

    Given an incoming canonical_ticker (e.g. 'LON:IQE'), returns the other
    plausible forms in the same family (e.g. ['AIM:IQE']).  Used by the add
    endpoint to detect when the same security already exists under a legacy
    canonical alias.

    Returns [] for US (bare) tickers and for tickers whose prefix has no
    known family (single-exchange names).
    """
    t = (canonical_ticker or "").strip().upper()
    if ":" not in t:
        return []
    prefix, bare = t.split(":", 1)
    family = _PREFIX_TO_FAMILY.get(prefix)
    if not family:
        return []
    return [f"{p}:{bare}" for p in sorted(family) if p != prefix]


# ── Part G: Canonical registry ───────────────────────────────────────────────

_registry_cache: dict[str, list[str]] = {}
_registry_ts: float = 0.0
_REGISTRY_TTL: float = 120.0   # rebuild at most once per 2 min


def build_canonical_registry(force: bool = False) -> dict[str, list[str]]:
    """
    Build bare_symbol → [canonical_ticker, ...] from ALL saved Watchlists.

    Loaded synchronously from Neon via pg_storage connection pool.
    Cached for _REGISTRY_TTL seconds (short so newly-added tickers are quickly
    reflected in subsequent search queries).

    Example output:
      {"IQE": ["AIM:IQE"], "NVDA": ["NVDA"], "SOI": ["EPA:SOI", "SOI"], ...}
    """
    global _registry_cache, _registry_ts
    now = time.monotonic()
    if not force and (now - _registry_ts) < _REGISTRY_TTL and _registry_cache:
        return _registry_cache

    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        if conn is None:
            return _registry_cache
        cur = conn.cursor()
        cur.execute("SELECT tickers FROM public.watchlist")
        rows = cur.fetchall()
        cur.close()
        _put_conn(conn)
    except Exception as exc:
        print(f"[CANONICAL_ADAPTER] registry build failed: {exc}")
        return _registry_cache

    reg: dict[str, list[str]] = {}
    for (tickers,) in rows:
        for t in (tickers or []):
            if not t or not isinstance(t, str):
                continue
            bare = t.rsplit(":", 1)[-1].strip().upper()
            if bare:
                reg.setdefault(bare, [])
                if t.upper() not in [x.upper() for x in reg[bare]]:
                    reg[bare].append(t)

    _registry_cache = reg
    _registry_ts = now
    return reg
