"""
watchlist_strategy_report.py — Cached-data-only strategy report generator.

Generates a Bottlenecks or Asymmetry strategy report for a watchlist using
ONLY already-cached data. No new FMP, Tradier, or LLM calls are made.

Display aliases (frontend-facing):
  "bottlenecks" → internal id "serenity"
  "asymmetry"   → internal id "sjcapital"
  (internal IDs preserved to keep playbook_registry.py unchanged)

Report storage: backend/data/strategy_reports/{report_id}.json
Index:          backend/data/strategy_reports/index.json
"""
from __future__ import annotations

import json as _json
import time
import uuid
from pathlib import Path
from typing import Any

# ── Storage ───────────────────────────────────────────────────────────────────

_REPORTS_DIR  = Path(__file__).parent.parent / "data" / "strategy_reports"
_INDEX_PATH   = _REPORTS_DIR / "index.json"

# ── Strategy aliases ──────────────────────────────────────────────────────────

_ALIAS_TO_ID: dict[str, str] = {
    "bottlenecks": "serenity",
    "asymmetry":   "sjcapital",
    "serenity":    "serenity",
    "sjcapital":   "sjcapital",
}

_DISPLAY_NAMES: dict[str, str] = {
    "serenity":  "Bottlenecks",
    "sjcapital": "Asymmetry",
}

# ── Scoring helpers ───────────────────────────────────────────────────────────

def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _score_balance_sheet(debt_equity: float | None) -> float:
    """Lower D/E → higher score. D/E ≤ 0 (net cash) = 95, D/E > 4 = 10."""
    if debt_equity is None:
        return 50.0
    if debt_equity <= 0:
        return 95.0
    if debt_equity >= 4.0:
        return 10.0
    return _clamp(95.0 - (debt_equity / 4.0) * 85.0)


def _score_revenue_growth(rg_pct: float | None) -> float:
    """Revenue growth % → 0-100. Negative = low, >40% = 100."""
    if rg_pct is None:
        return 50.0
    if rg_pct <= -10:
        return 5.0
    if rg_pct >= 40:
        return 100.0
    return _clamp(((rg_pct + 10) / 50.0) * 95.0 + 5.0)


def _score_valuation_vs_peers(pe_ratio: float | None, sector: str = "") -> float:
    """PE discount vs sector median → higher score = cheaper."""
    _SECTOR_PE_MEDIANS: dict[str, float] = {
        "Technology": 28.0, "Healthcare": 22.0, "Health Care": 22.0,
        "Consumer Cyclical": 18.0, "Financial Services": 12.0, "Financials": 12.0,
        "Energy": 10.0, "Utilities": 16.0, "Basic Materials": 14.0,
        "Materials": 14.0, "Consumer Defensive": 20.0, "Consumer Staples": 20.0,
        "Communication Services": 20.0, "Industrials": 18.0, "Real Estate": 30.0,
    }
    if pe_ratio is None or pe_ratio <= 0:
        return 50.0
    median = _SECTOR_PE_MEDIANS.get(sector, 18.0)
    ratio  = pe_ratio / median
    if ratio <= 0.5:
        return 90.0
    if ratio >= 2.0:
        return 10.0
    return _clamp(90.0 - ((ratio - 0.5) / 1.5) * 80.0)


def _score_small_cap_asymmetry(market_cap: float | None) -> float:
    """Small-to-mid cap = higher asymmetry score."""
    if market_cap is None:
        return 50.0
    if market_cap < 500_000_000:        # <500M micro/small
        return 90.0
    if market_cap < 2_000_000_000:      # 500M-2B small
        return 75.0
    if market_cap < 10_000_000_000:     # 2-10B mid
        return 60.0
    if market_cap < 50_000_000_000:     # 10-50B large
        return 40.0
    return 20.0                          # >50B mega


def _score_technical(timing_score: float | None, label: str | None) -> float:
    """Use technical_timing_score if present, label as fallback."""
    if timing_score is not None:
        return _clamp(float(timing_score))
    mapping = {
        "strong_uptrend": 90.0, "uptrend": 75.0, "mild_uptrend": 65.0,
        "consolidation": 50.0, "neutral": 50.0, "mild_downtrend": 35.0,
        "downtrend": 20.0, "strong_downtrend": 10.0,
    }
    return mapping.get((label or "").lower(), 50.0)


def _score_theme_alignment(canonical_theme_id: str | None, preferred_themes: list[str]) -> float:
    """Theme_id in preferred_themes list → 90, else → 30."""
    if not canonical_theme_id:
        return 30.0
    return 90.0 if canonical_theme_id in preferred_themes else 30.0


def _score_bottleneck(sym: str, canonical_theme_id: str | None, preferred_themes: list[str]) -> float:
    """
    Check symbol against curated BOTTLENECK_MAP (static data, no API calls).
    Fall back to theme_alignment if BOTTLENECK_MAP unavailable.
    """
    try:
        from services.playbook.theme_map import BOTTLENECK_MAP  # type: ignore
        if sym in BOTTLENECK_MAP:
            entry     = BOTTLENECK_MAP[sym]
            raw_score = entry.get("score", 0.5)
            return _clamp(float(raw_score) * 100.0)
    except Exception:
        pass
    # Fallback: theme-based heuristic
    bottleneck_themes = {
        "semicap_supply_chain", "advanced_packaging_test", "photonics_cpo",
        "defense_optics", "grid_transformers", "memory", "substrates_packaging",
        "pcb_materials", "specialty_gas", "critical_minerals",
    }
    if canonical_theme_id and canonical_theme_id in bottleneck_themes:
        return 75.0
    if canonical_theme_id and canonical_theme_id in (preferred_themes or []):
        return 60.0
    return 25.0


def _score_sector_strength(sector: str, preferred_sectors: list[str]) -> float:
    """Sector in preferred list = 80, else = 30."""
    if not sector:
        return 40.0
    return 80.0 if sector in preferred_sectors else 30.0


def _score_catalyst_proximity(earnings_date: str | None) -> float:
    """Days until earnings → closer = higher urgency score."""
    if not earnings_date:
        return 40.0
    try:
        import datetime
        ed = datetime.date.fromisoformat(str(earnings_date)[:10])
        days_away = (ed - datetime.date.today()).days
        if days_away < 0:
            return 40.0     # past
        if days_away <= 14:
            return 90.0
        if days_away <= 45:
            return 75.0
        if days_away <= 90:
            return 55.0
        return 35.0
    except Exception:
        return 40.0


def _score_ebitda_inflection(ebit: float | None, revenue: float | None, fcf_margin: float | None) -> float:
    """Simple EBITDA inflection proxy: positive EBIT + improving margin."""
    if ebit is None and fcf_margin is None:
        return 50.0
    score = 50.0
    if ebit is not None and ebit > 0:
        score += 20.0
    if fcf_margin is not None:
        if fcf_margin > 15:
            score += 20.0
        elif fcf_margin > 5:
            score += 10.0
        elif fcf_margin < 0:
            score -= 10.0
    return _clamp(score)


def _parse_num(v: Any) -> float | None:
    """Safely parse a numeric field that may be string, int, or float."""
    if v is None or v == "" or v == "N/A":
        return None
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def _hard_filter_pass(sym: str, fund_fields: dict, pb_def: Any) -> tuple[bool, str]:
    """
    Apply playbook hard filters against cached fundamentals.
    Returns (passes, reason_if_failed).
    """
    mkt_cap     = _parse_num(fund_fields.get("Market Cap"))
    debt_equity = _parse_num(fund_fields.get("Debt / Equity"))

    for hf in (pb_def.hard_filters or []):
        field = hf.field
        val   = hf.value
        op    = hf.op

        if field == "mkt_cap" and mkt_cap is not None:
            if op == "gte" and mkt_cap < val:
                return False, hf.label
        if field == "debt_to_equity" and debt_equity is not None:
            if op == "lte" and debt_equity > val:
                return False, hf.label

    return True, ""


def _penalty_points(factor_scores: dict[str, float], pb_def: Any) -> tuple[float, list[str]]:
    """Apply penalty rules, return total deduction and triggered labels."""
    total_deduction = 0.0
    triggered: list[str] = []
    for rule in (pb_def.penalty_rules or []):
        score = factor_scores.get(rule.factor, 50.0)
        if score > rule.threshold:
            total_deduction += rule.deduction
            triggered.append(rule.label)
    return total_deduction, triggered


# ── Per-ticker scoring ────────────────────────────────────────────────────────

def _score_ticker_cached(
    sym: str,
    fund_fields: dict,
    stage2_entry: dict | None,
    canonical_theme_id: str | None,
    canonical_theme_name: str | None,
    pb_def: Any,
    fund_sector: str = "",
) -> dict:
    """
    Score a single ticker against pb_def using only cached data.
    Returns a score result dict.
    """
    # Extract fundamentals
    debt_equity = _parse_num(fund_fields.get("Debt / Equity"))
    revenue_growth = _parse_num(fund_fields.get("Revenue Growth (Q)"))
    pe_ratio    = _parse_num(fund_fields.get("PE Ratio"))
    market_cap  = _parse_num(fund_fields.get("Market Cap"))
    earnings_dt = fund_fields.get("Earnings Date")
    ebit        = _parse_num(fund_fields.get("EBIT"))
    fcf_margin  = _parse_num(fund_fields.get("FCF Margin"))

    # Extract stage2 technical
    timing_score = None
    tech_label   = None
    tech_state   = None
    if stage2_entry:
        timing_score = _parse_num(stage2_entry.get("technical_timing_score") or stage2_entry.get("score"))
        tech_label   = stage2_entry.get("technical_state") or stage2_entry.get("label")
        tech_state   = stage2_entry.get("technical_state") or stage2_entry.get("label")

    preferred_themes  = pb_def.preferred_themes or []
    preferred_sectors = pb_def.preferred_sectors or []

    # ── Factor scores ─────────────────────────────────────────────────────────
    factor_scores: dict[str, float] = {
        "technical_confirmation":       _score_technical(timing_score, tech_label),
        "balance_sheet_strength":       _score_balance_sheet(debt_equity),
        "valuation_discount_vs_peers":  _score_valuation_vs_peers(pe_ratio, fund_sector),
        "revenue_growth":               _score_revenue_growth(revenue_growth),
        "small_cap_asymmetry":          _score_small_cap_asymmetry(market_cap),
        "theme_alignment":              _score_theme_alignment(canonical_theme_id, preferred_themes),
        "bottleneck_exposure":          _score_bottleneck(sym, canonical_theme_id, preferred_themes),
        "sector_strength":              _score_sector_strength(fund_sector, preferred_sectors),
        "catalyst_proximity":           _score_catalyst_proximity(earnings_dt),
        "supply_chain_confirmation":    _score_bottleneck(sym, canonical_theme_id, preferred_themes) * 0.9,
        "ebitda_inflection_proximity":  _score_ebitda_inflection(ebit, None, fcf_margin),
        "backlog_quality":              50.0,  # no cached signal
        "evidence_freshness":           50.0,  # no cached signal
        "execution_risk":               _clamp(100.0 - _score_balance_sheet(debt_equity)),
        "crowding_risk":                50.0,  # no cached signal
        "dilution_risk":                50.0,  # no cached signal
        "policy_tailwind":              _score_theme_alignment(canonical_theme_id, preferred_themes) * 0.8,
        "revenue_acceleration":         50.0,  # stubbed
    }

    # ── Weighted raw score ────────────────────────────────────────────────────
    weights = pb_def.factor_weights or {}
    weighted = sum(factor_scores.get(f, 50.0) * w for f, w in weights.items())

    # ── Penalties ─────────────────────────────────────────────────────────────
    deduction, triggered_labels = _penalty_points(factor_scores, pb_def)
    final_score = _clamp(weighted - deduction)

    # ── Hard filter ───────────────────────────────────────────────────────────
    passes_filter, filter_reason = _hard_filter_pass(sym, fund_fields, pb_def)

    missing: list[str] = []
    if debt_equity is None:     missing.append("Debt/Equity")
    if revenue_growth is None:  missing.append("Revenue Growth")
    if pe_ratio is None:        missing.append("PE Ratio")
    if market_cap is None:      missing.append("Market Cap")
    if stage2_entry is None:    missing.append("Technical data")
    if not earnings_dt:         missing.append("Earnings Date")

    return {
        "ticker":               sym,
        "final_score":          round(final_score, 1),
        "raw_score":            round(weighted, 1),
        "passes_hard_filter":   passes_filter,
        "filter_reason":        filter_reason or None,
        "canonical_theme":      canonical_theme_name,
        "canonical_theme_id":   canonical_theme_id,
        "technical_state":      tech_state,
        "technical_timing_score": timing_score,
        "factor_scores":        {k: round(v, 1) for k, v in factor_scores.items() if k in weights},
        "penalties_applied":    triggered_labels,
        "penalty_deduction":    round(deduction, 1),
        "missing_data":         missing,
        "earnings_date":        earnings_dt,
        "market_cap":           market_cap,
        "debt_equity":          debt_equity,
        "pe_ratio":             pe_ratio,
        "revenue_growth_q":     revenue_growth,
    }


# ── Report index helpers ──────────────────────────────────────────────────────

def _load_index() -> list[dict]:
    try:
        if _INDEX_PATH.exists():
            return _json.loads(_INDEX_PATH.read_text()) or []
    except Exception:
        pass
    return []


def _save_index(entries: list[dict]) -> None:
    try:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        _INDEX_PATH.write_text(_json.dumps(entries, indent=2))
    except Exception as exc:
        print(f"[STRATEGY_REPORT] index save error: {exc}")


def _save_report(report: dict) -> str:
    report_id = report["report_id"]
    path = _REPORTS_DIR / f"{report_id}.json"
    try:
        _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(_json.dumps(report, indent=2))
    except Exception as exc:
        print(f"[STRATEGY_REPORT] report save error: {exc}")
    return report_id


def _add_to_index(report: dict) -> None:
    entries = _load_index()
    entries.insert(0, {
        "report_id":    report["report_id"],
        "watchlist_id": report["watchlist_id"],
        "strategy_id":  report["strategy_id"],
        "strategy_name":report["strategy_name"],
        "generated_at": report["generated_at"],
        "ticker_count": report["ticker_count"],
        "matched_count":len(report.get("ranked_results") or []),
    })
    # Keep last 50 entries per watchlist
    _save_index(entries[:100])


# ── Public API ────────────────────────────────────────────────────────────────

async def generate_report(
    watchlist_id: str,
    strategy_id: str,
    save: bool = True,
) -> dict:
    """
    Generate a strategy report for a watchlist using only cached data.

    Accepts display aliases ("bottlenecks", "asymmetry") or internal IDs.
    Does not call FMP, Tradier, or any LLM.
    """
    import asyncio

    # ── Resolve strategy ──────────────────────────────────────────────────────
    internal_id = _ALIAS_TO_ID.get(strategy_id.lower())
    if not internal_id:
        raise ValueError(f"Unknown strategy_id: {strategy_id!r}. Use bottlenecks or asymmetry.")

    from services.playbook.playbook_registry import get as get_playbook
    pb_def = get_playbook(internal_id)
    if pb_def is None:
        raise ValueError(f"Playbook {internal_id!r} not registered.")

    display_name = _DISPLAY_NAMES.get(internal_id, pb_def.name)

    # ── Load watchlist ────────────────────────────────────────────────────────
    from services.watchlist_service import load_watchlist
    store = load_watchlist(watchlist_id)
    if store is None:
        raise ValueError(f"Watchlist not found: {watchlist_id!r}")

    tickers: list[str] = store.get("tickers", []) or []
    csv_data: list[dict] = store.get("csv_data", []) or []

    # Build CSV sector/industry map
    csv_map: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_map[sym] = row

    # Exclude foreign symbols
    _FX_PREFIXES = ("AIM:", "STO:", "FRA:", "TYO:", "HKG:", "NSE:", "TSX:", "ASX:",
                    "LON:", "ETR:", "EPA:", "BIT:", "ELI:", "CPH:", "OTC:")
    eligible = [
        t.upper().strip() for t in tickers
        if t.strip() and not any(t.upper().startswith(p) for p in _FX_PREFIXES)
    ]

    # ── Load cached fundamentals (bulk) ───────────────────────────────────────
    fund_map: dict[str, dict] = {}
    try:
        from data.watchlist_fundamentals_store import get_snapshots_bulk
        snaps = get_snapshots_bulk(eligible) or {}
        for sym, snap in snaps.items():
            fund_map[sym] = snap.get("fields") or {}
    except Exception as exc:
        print(f"[STRATEGY_REPORT] fundamentals load error (non-fatal): {exc}")

    # ── Load stage2 technical LKG ─────────────────────────────────────────────
    stage2_lkg: dict[str, dict] = {}
    try:
        from services.watchlist_stage2_service import _STAGE2_LKG
        stage2_lkg = dict(_STAGE2_LKG)
    except Exception as exc:
        print(f"[STRATEGY_REPORT] stage2 LKG load error (non-fatal): {exc}")

    # ── Load theme mappings ────────────────────────────────────────────────────
    from services.theme_ticker_mapper import (
        map_ticker_to_primary_theme,
        map_ticker_to_theme_id,
    )

    # ── Score each ticker ─────────────────────────────────────────────────────
    results: list[dict] = []
    cache_freshness_notes: list[str] = []
    all_missing_notes: list[str] = []

    for sym in eligible:
        fund_fields  = fund_map.get(sym) or {}
        stage2_entry = stage2_lkg.get(sym)
        canon_theme  = map_ticker_to_primary_theme(sym)
        canon_id     = map_ticker_to_theme_id(sym) if canon_theme else None

        # Sector: from FMP profile cache first, then CSV
        sector = ""
        try:
            from services.fmp_cache_service import get_company_profiles_bulk_cached
            prof_map = get_company_profiles_bulk_cached([sym]) or {}
            prof     = prof_map.get(sym) or {}
            sector   = prof.get("sector") or ""
        except Exception:
            pass
        if not sector:
            row = csv_map.get(sym, {})
            sector = row.get("Sector") or row.get("sector") or ""

        result = _score_ticker_cached(
            sym, fund_fields, stage2_entry, canon_id, canon_theme, pb_def, sector
        )
        results.append(result)

        if result["missing_data"]:
            all_missing_notes.append(f"{sym}: missing {', '.join(result['missing_data'])}")

    # ── Apply hard filters + rank ─────────────────────────────────────────────
    filtered_in  = [r for r in results if r["passes_hard_filter"]]
    filtered_out = [r for r in results if not r["passes_hard_filter"]]

    ranked = sorted(filtered_in, key=lambda r: r["final_score"], reverse=True)

    # ── Build report ──────────────────────────────────────────────────────────
    ts         = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report_id  = f"{internal_id}_{watchlist_id[:8]}_{int(time.time())}"

    # Summary reasons for top picks
    reasons: list[dict] = []
    for r in ranked[:10]:
        top_factors = sorted(
            [(f, r["factor_scores"].get(f, 50.0)) for f in (pb_def.factor_weights or {})],
            key=lambda x: x[1],
            reverse=True,
        )[:3]
        reasons.append({
            "ticker":       r["ticker"],
            "score":        r["final_score"],
            "top_factors":  [{"factor": f, "score": round(s, 1)} for f, s in top_factors],
            "theme":        r["canonical_theme"],
            "missing_data": r["missing_data"],
        })

    # Cache freshness
    fund_count    = len([s for s in eligible if fund_map.get(s)])
    stage2_count  = len([s for s in eligible if stage2_lkg.get(s)])
    cache_freshness_notes = [
        f"Fundamentals cached for {fund_count}/{len(eligible)} symbols",
        f"Technical data available for {stage2_count}/{len(eligible)} symbols",
    ]

    report = {
        "report_id":           report_id,
        "watchlist_id":        watchlist_id,
        "strategy_id":         internal_id,
        "strategy_name":       display_name,
        "strategy_display_id": strategy_id.lower(),
        "generated_at":        ts,
        "ticker_count":        len(eligible),
        "matched_tickers":     len(ranked),
        "hard_filtered_out":   len(filtered_out),
        "ranked_results":      ranked,
        "filtered_out":        [
            {"ticker": r["ticker"], "reason": r["filter_reason"]} for r in filtered_out
        ],
        "reasons":             reasons,
        "supporting_fields": {
            "fundamentals": "PE Ratio, Debt/Equity, Revenue Growth (Q), Market Cap, Earnings Date, EBIT, FCF Margin",
            "technical":    "technical_timing_score, technical_state (stage2 LKG)",
            "theme":        "canonical_theme_id, theme_alignment vs preferred_themes",
            "bottleneck":   "BOTTLENECK_MAP curated static data",
        },
        "missing_data_notes":  all_missing_notes[:50],
        "cache_freshness":     cache_freshness_notes,
        "model_used":          None,
        "provider_calls_used": [],
        "playbook_version":    getattr(pb_def, "version", None),
        "factor_weights":      pb_def.factor_weights,
    }

    if save:
        _save_report(report)
        _add_to_index(report)

    print(
        f"[STRATEGY_REPORT] generated report_id={report_id} "
        f"strategy={display_name} watchlist={watchlist_id} "
        f"eligible={len(eligible)} matched={len(ranked)}"
    )
    return report


def get_report(report_id: str) -> dict | None:
    """Load a saved report by ID."""
    path = _REPORTS_DIR / f"{report_id}.json"
    try:
        if path.exists():
            return _json.loads(path.read_text())
    except Exception as exc:
        print(f"[STRATEGY_REPORT] get_report error: {exc}")
    return None


def get_report_history(watchlist_id: str | None = None) -> list[dict]:
    """Return list of report index entries, optionally filtered by watchlist_id."""
    entries = _load_index()
    if watchlist_id:
        entries = [e for e in entries if e.get("watchlist_id") == watchlist_id]
    return entries
