"""OTC canonical symbol helpers.

OTC watchlist listings have the canonical form  OTC:BESIY.
FMP provider calls use the bare symbol          BESIY.

This module is the single place that knows about the OTC: prefix
convention.  Every other file imports from here rather than
implementing its own strip/detect logic so the rule can change in one
place if needed.

Deliberately has no imports from the rest of the codebase so it can be
imported by any module without circular-import risk.
"""
from __future__ import annotations

_OTC_PREFIX = "OTC:"
_OTC_PREFIX_UPPER = _OTC_PREFIX.upper()  # "OTC:"


def is_otc_symbol(symbol: str) -> bool:
    """Return True iff *symbol* is a canonical OTC watchlist listing.

    Only the literal ``OTC:`` prefix qualifies — other exchange prefixes
    (LON:, TSX:, AIM:, etc.) return False and are left to their own
    existing exclusion logic.

    >>> is_otc_symbol("OTC:BESIY")
    True
    >>> is_otc_symbol("otc:besiy")   # case-insensitive
    True
    >>> is_otc_symbol("NVDA")
    False
    >>> is_otc_symbol("LON:VOD")
    False
    """
    if not symbol:
        return False
    return symbol.upper().startswith(_OTC_PREFIX_UPPER)


def otc_to_fmp(symbol: str) -> str:
    """Return the bare FMP provider symbol for an OTC canonical symbol.

    Strips the ``OTC:`` prefix.  If *symbol* is not an OTC listing it is
    returned unchanged so callers can safely pass any symbol.

    >>> otc_to_fmp("OTC:BESIY")
    'BESIY'
    >>> otc_to_fmp("otc:besiy")
    'besiy'
    >>> otc_to_fmp("NVDA")
    'NVDA'
    """
    sym = symbol.strip()
    if sym.upper().startswith(_OTC_PREFIX_UPPER):
        return sym[len(_OTC_PREFIX):]
    return sym


def split_otc_us(symbols: list[str]) -> tuple[list[str], list[str]]:
    """Partition *symbols* into ``(otc_canonical, us_plain)``.

    - OTC symbols  : have the ``OTC:`` prefix  → end up in *otc_canonical*
    - US symbols   : plain tickers, no colon   → end up in *us_plain*
    - Other colon-prefixed symbols (AIM:, LON:, TSX:, …) end up in neither
      list and continue to be silently excluded by the existing logic.

    The original capitalisation is preserved in both output lists.
    """
    otc: list[str] = []
    us:  list[str] = []
    for s in symbols:
        if not s:
            continue
        if s.upper().startswith(_OTC_PREFIX_UPPER):
            otc.append(s)
        elif ":" not in s:
            us.append(s)
    return otc, us
