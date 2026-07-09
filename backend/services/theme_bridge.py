"""
theme_bridge.py — READ-ONLY bridge: canonical ticker→Theme membership → Theme
Rotation → Confluence V2.

This module does NOT create any new theme categorization logic. It is a thin
read-only adapter over two EXISTING systems:

  1. services.theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE
       — the canonical, already-merged Theme membership result (base Theme
         Universe + watchlist seeds + manual admin add/remove overrides,
         applied in that exact order by theme_merge_layer._build_enriched_universe).
         Manual overrides remain authoritative exactly as in the existing
         Themes admin system — this module reads that already-merged result,
         it does not re-implement the merge.

  2. services.theme_rotation_service.build_theme_rotation_snapshot()
       — the EXISTING theme-level rotation engine (LEADING / CONFIRMING /
         STALLING / BOTTOMING / LAGGING / UNCLASSIFIED). Not recomputed per
         ticker — computed once per snapshot build, at theme granularity.

Zero provider calls. Zero LLM calls. Zero writes. Cache/LKG-read only.
"""
from __future__ import annotations

from typing import Optional


# ── Part 1: ticker → canonical Theme membership (read-only) ────────────────

def _norm(sym: str) -> str:
    return (sym or "").strip().upper()


def build_ticker_theme_index() -> dict[str, list[str]]:
    """
    Build {TICKER: [canonical_theme_id, ...]} from the EXISTING merged
    canonical membership result (theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE).

    Membership = union(candidate_symbols, proxy_symbols) per theme, since both
    fields are stock/ETF tickers that already reflect manual add/remove
    overrides (theme_merge_layer._build_enriched_universe applies overrides to
    both fields identically). representative_symbol/tv_symbol are display-only
    and intentionally excluded from membership.

    A ticker may legitimately belong to multiple themes — all are preserved.
    Rebuilt fresh on every call (cheap: 51 themes, dict reads only) so it
    always reflects the live, in-memory ENRICHED_THEME_RS_UNIVERSE — including
    any admin overrides applied since the process started.
    """
    from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE

    index: dict[str, list[str]] = {}
    for theme_id, meta in ENRICHED_THEME_RS_UNIVERSE.items():
        members = set(meta.get("candidate_symbols") or []) | set(meta.get("proxy_symbols") or [])
        for raw_sym in members:
            sym = _norm(raw_sym)
            if not sym:
                continue
            index.setdefault(sym, [])
            if theme_id not in index[sym]:
                index[sym].append(theme_id)
    return index


def get_themes_for_ticker(symbol: str, ticker_idx: Optional[dict[str, list[str]]] = None) -> list[str]:
    """Return the canonical theme_id list for one ticker. Never raises."""
    try:
        idx = ticker_idx if ticker_idx is not None else build_ticker_theme_index()
        return list(idx.get(_norm(symbol), []))
    except Exception:
        return []


def get_ticker_theme_diagnostics(ticker_idx: Optional[dict[str, list[str]]] = None) -> dict:
    """Diagnostics for Part 1 of the spec. Zero provider calls."""
    try:
        idx = ticker_idx if ticker_idx is not None else build_ticker_theme_index()
        mapped = [s for s, t in idx.items() if t]
        multi = [s for s, t in idx.items() if len(t) > 1]
        avg_themes = round(sum(len(t) for t in idx.values()) / len(idx), 3) if idx else 0.0

        manual_add_count = 0
        manual_remove_count = 0
        try:
            from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE
            for meta in ENRICHED_THEME_RS_UNIVERSE.values():
                manual_add_count += len(meta.get("manual_added_symbols") or [])
                manual_remove_count += len(meta.get("manual_removed_symbols") or [])
        except Exception:
            pass

        return {
            "ticker_theme_lookup_symbols":      len(idx),
            "ticker_theme_lookup_memberships":  sum(len(t) for t in idx.values()),
            "mapped_ticker_count":              len(mapped),
            "unmapped_ticker_count":            0,  # index only contains mapped tickers by construction
            "multi_theme_ticker_count":         len(multi),
            "average_themes_per_mapped_ticker": avg_themes,
            "manual_add_membership_count":      manual_add_count,
            "manual_remove_exclusion_count":    manual_remove_count,
        }
    except Exception as e:
        return {"error": str(e)}


# ── Part 2/3: connect memberships to EXISTING Theme Rotation + pick primary ─

def get_theme_rotation_index() -> dict[str, dict]:
    """
    {theme_id: rotation_row} from the EXISTING Theme Rotation engine.
    Does NOT recompute Theme Rotation per ticker — one snapshot build,
    theme-level granularity, reused for every ticker lookup in this call.
    """
    try:
        from services.theme_rotation_service import build_theme_rotation_snapshot
        snap = build_theme_rotation_snapshot()
        if not snap.get("ok"):
            return {}
        return {row["theme_id"]: row for row in (snap.get("themes") or [])}
    except Exception:
        return {}


def get_ticker_rotation_bridge(
    symbol: str,
    ticker_idx: Optional[dict[str, list[str]]] = None,
    rotation_idx: Optional[dict[str, dict]] = None,
) -> dict:
    """
    Full Part 2/3 result for one ticker:
      canonical_theme_memberships: [theme_id, ...]
      theme_rotation_memberships:  [{theme_id, rotation_score, rotation_state,
                                      rotation_direction, rotation_available}, ...]
      primary_rotation_theme / primary_theme_rotation_score /
      primary_theme_rotation_state / primary_theme_rotation_direction
      theme_signal_available: bool
      theme_signal_reason: MAPPED_ROTATION_AVAILABLE | NO_CANONICAL_THEME_MEMBERSHIP
                            | THEME_MEMBERSHIP_NO_ROTATION_RESULT
    """
    t_idx = ticker_idx if ticker_idx is not None else build_ticker_theme_index()
    r_idx = rotation_idx if rotation_idx is not None else get_theme_rotation_index()

    memberships = get_themes_for_ticker(symbol, t_idx)

    if not memberships:
        return {
            "canonical_theme_memberships": [],
            "theme_rotation_memberships":  [],
            "primary_rotation_theme":      None,
            "primary_theme_rotation_score": None,
            "primary_theme_rotation_state": None,
            "primary_theme_rotation_direction": None,
            "theme_signal_available":      False,
            "theme_signal_reason":         "NO_CANONICAL_THEME_MEMBERSHIP",
        }

    rotation_rows: list[dict] = []
    for theme_id in memberships:
        row = r_idx.get(theme_id)
        if row is None:
            rotation_rows.append({
                "theme_id":            theme_id,
                "rotation_score":      None,
                "rotation_state":      None,
                "rotation_direction":  None,
                "rotation_available":  False,
            })
        else:
            rotation_rows.append({
                "theme_id":            theme_id,
                "rotation_score":      row.get("rotation_score"),
                "rotation_state":      row.get("rotation_phase"),
                "rotation_direction":  row.get("signals", {}).get("momentum_signal"),
                "rotation_available":  True,
            })

    available_rows = [r for r in rotation_rows if r["rotation_available"]]

    if not available_rows:
        return {
            "canonical_theme_memberships": memberships,
            "theme_rotation_memberships":  rotation_rows,
            "primary_rotation_theme":      None,
            "primary_theme_rotation_score": None,
            "primary_theme_rotation_state": None,
            "primary_theme_rotation_direction": None,
            "theme_signal_available":      False,
            "theme_signal_reason":         "THEME_MEMBERSHIP_NO_ROTATION_RESULT",
        }

    # Primary = the ticker's own strongest legitimate membership (never a
    # theme outside its canonical memberships, never the globally strongest).
    primary = max(available_rows, key=lambda r: r["rotation_score"])

    return {
        "canonical_theme_memberships": memberships,
        "theme_rotation_memberships":  rotation_rows,
        "primary_rotation_theme":      primary["theme_id"],
        "primary_theme_rotation_score": primary["rotation_score"],
        "primary_theme_rotation_state": primary["rotation_state"],
        "primary_theme_rotation_direction": primary["rotation_direction"],
        "theme_signal_available":      True,
        "theme_signal_reason":         "MAPPED_ROTATION_AVAILABLE",
    }


# ── Part 1 (Trade Alignment Foundation): canonical PRIMARY Theme identity ──
#
# get_ticker_rotation_bridge() above answers "what are ALL of this ticker's
# Theme memberships, from ENRICHED_THEME_RS_UNIVERSE only, and which scores
# highest?" — useful multi-theme diagnostics, but it only sees one of the
# four sources the Watchlist UI consults, so it undercounts (51.47% of the
# live watchlist vs. the Watchlist UI's 99.46%).
#
# get_primary_theme_alignment() instead answers the Trade-Alignment-relevant
# question: "what SINGLE primary Theme does the Watchlist UI already show
# for this ticker, and does Theme Rotation have a result for it?" It
# delegates identity resolution entirely to services.theme_resolver — the
# exact same function the Watchlist skeleton path now calls — so the two
# surfaces can never diverge. This does not replace get_ticker_rotation_bridge;
# secondary/multi-theme membership remains diagnostic-only via that function.


def _normalize_theme_rotation_key(theme_id: Optional[str]) -> Optional[str]:
    """
    Normalize a resolved theme_id to the exact key format used by
    Theme Rotation's snapshot (services.theme_rotation_service /
    themes_rs_lkg.json), which always slugifies "X / Y" display names to
    "x_y" (no literal slash character).

    Some explicit entries in theme_ticker_mapper._FOREIGN_ALIAS_MAP were
    authored with the slash preserved (e.g. "photonics_/_lasers",
    "power_/_cooling", "substrates_/_packaging") — a literal formatting
    inconsistency in that static table, not a fuzzy-matching decision. This
    collapses "_/_" -> "_" so those ids match the Theme Rotation key exactly.
    No other normalization/aliasing is applied.
    """
    if not theme_id:
        return theme_id
    return theme_id.replace("_/_", "_")


def get_primary_theme_alignment(
    symbol: str,
    industry: Optional[str] = None,
    resolver_ctx: Optional[dict] = None,
    rotation_idx: Optional[dict[str, dict]] = None,
) -> dict:
    """
    Resolve the canonical PRIMARY Theme identity for `symbol` using the same
    resolver the Watchlist UI uses (services.theme_resolver), then look up
    the EXISTING Theme Rotation result for that theme_id. Never substitutes
    a different (e.g. highest-scoring secondary) theme — the user's/UI's
    resolved primary Theme is authoritative for the 25% Theme Alignment
    signal.

    Returns:
      canonical_primary_theme         — display name (matches Watchlist UI)
      canonical_primary_theme_source  — canonical_map | industry_fallback |
                                         themes_page_membership | manual_override
                                         | no_mapping
      theme_rotation_key              — theme_id used to look up rotation
      theme_signal_available          — bool
      primary_theme_rotation_score/state/direction
      theme_signal_reason             — NO_CANONICAL_THEME_MEMBERSHIP |
                                         THEME_MEMBERSHIP_NO_ROTATION_RESULT |
                                         MAPPED_ROTATION_AVAILABLE
    """
    from services.theme_resolver import (
        build_theme_resolution_context,
        resolve_primary_theme_for_ticker,
    )

    ctx = resolver_ctx if resolver_ctx is not None else build_theme_resolution_context()
    r_idx = rotation_idx if rotation_idx is not None else get_theme_rotation_index()

    resolution = resolve_primary_theme_for_ticker(symbol, industry=industry, ctx=ctx)
    theme_name = resolution["theme_name"]
    theme_id = _normalize_theme_rotation_key(resolution["theme_id"])
    theme_source = resolution["source"]

    base = {
        "canonical_primary_theme":        theme_name,
        "canonical_primary_theme_source": theme_source,
        "theme_rotation_key":             theme_id,
    }

    if not theme_name:
        return {
            **base,
            "theme_signal_available":           False,
            "primary_theme_rotation_score":     None,
            "primary_theme_rotation_state":      None,
            "primary_theme_rotation_direction": None,
            "theme_signal_reason":              "NO_CANONICAL_THEME_MEMBERSHIP",
        }

    row = r_idx.get(theme_id) if theme_id else None
    if row is None:
        return {
            **base,
            "theme_signal_available":           False,
            "primary_theme_rotation_score":     None,
            "primary_theme_rotation_state":      None,
            "primary_theme_rotation_direction": None,
            "theme_signal_reason":              "THEME_MEMBERSHIP_NO_ROTATION_RESULT",
        }

    return {
        **base,
        "theme_signal_available":           True,
        "primary_theme_rotation_score":     row.get("rotation_score"),
        "primary_theme_rotation_state":     row.get("rotation_phase"),
        "primary_theme_rotation_direction": row.get("signals", {}).get("momentum_signal"),
        "theme_signal_reason":              "MAPPED_ROTATION_AVAILABLE",
    }
