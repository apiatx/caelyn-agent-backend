"""
Playbook Analyzer — logic for POST /api/playbooks/analyze endpoint.

Deterministic answer generation from scored results + explanation metadata.
Does NOT call the main AI brain. No coupling to /api/query.

Supports context modes:
  watchlist  — score provided tickers, rank and summarize
  portfolio  — score holdings with weights, aggregate
  custom     — score provided tickers (alias for watchlist)
  universe   — score from predefined universe

Answer is built deterministically from:
  - Ranked scores
  - Explanation fields (thesis_summary, fit_reasoning)
  - Playbook context (preferred_themes, positioning_style)
  - Top/rejected split
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


# ── Request / Response models ─────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id:       str
    query:             str = ""            # DECORATIVE — shapes the answer string only; does NOT filter tickers or affect scoring.
    context_mode:      str = "watchlist"   # watchlist | portfolio | custom | universe
    tickers:           List[str] = Field(default_factory=list)
    holdings:          List[Dict[str, Any]] = Field(default_factory=list)
    universe:          str = "ai_infra"
    limit:             int = 10
    include_breakdown: bool = True
    # ── Optional discovery bridge (Serenity only) ──────────────────────────────
    # If discovery_mode is set AND playbook_id=serenity:
    #   1. Runs the discovery engine to find candidates
    #   2. Scores the top discovered candidates against the Serenity playbook
    #   3. Returns a combined answer (discovery context + playbook scores)
    # S&J: discovery_mode is accepted but does NOT change S&J philosophy or factor logic.
    discovery_mode:    Optional[str] = None  # giant_chain | theme_scan | foreign_bottlenecks | ...
    giant:             Optional[str] = None  # e.g. "NVDA" — for giant_chain mode
    theme_ids:         List[str] = Field(default_factory=list)
    include_foreign:   bool = False
    max_depth:         int = 3
    country_filters:   List[str] = Field(default_factory=list)
    # ── Optional cross-playbook compare (Phase 4) ─────────────────────────────
    # If set, the response will include a compare_summary showing how both playbooks
    # see the top scored candidates. Does NOT change the primary playbook scores.
    compare_with_playbook: Optional[str] = None  # e.g. "sjcapital"


class AnalyzeTickerSummary(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ticker:             str
    final_score:        float
    hard_filter_pass:   bool
    thesis_summary:     str = ""
    fit_reasoning:      List[str] = Field(default_factory=list)
    non_fit_reasoning:  List[str] = Field(default_factory=list)
    matched_themes:     List[str] = Field(default_factory=list)
    bottleneck_tags:    List[str] = Field(default_factory=list)
    catalyst_signals:   List[str] = Field(default_factory=list)
    supply_chain_tags:  List[str] = Field(default_factory=list)
    factor_scores:      Dict[str, float] = Field(default_factory=dict)
    penalties_applied:  Dict[str, float] = Field(default_factory=dict)
    stub_factors:       List[str] = Field(default_factory=list)


class PortfolioSummaryResult(BaseModel):
    model_config = ConfigDict(extra="ignore")
    aggregate_score:    float
    strongest_aligned:  List[str]
    weakest_aligned:    List[str]
    concentration_notes: List[str]


class AnalyzeResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")
    playbook_id:       str
    playbook_name:     str
    query:             str
    context_mode:      str
    answer:            str
    top_ranked:        List[AnalyzeTickerSummary]
    rejected_or_low_fit: List[AnalyzeTickerSummary]
    portfolio_summary: Optional[PortfolioSummaryResult] = None
    playbook_context:  Dict[str, Any] = Field(default_factory=dict)
    discovery_context: Dict[str, Any] = Field(default_factory=dict)
    meta:              Dict[str, Any] = Field(default_factory=dict)
    # Populated when playbook_id=serenity and discovery_mode="auto".
    # Contains the SerenityRegime serialized as dict — explains what path Auto chose and why.
    regime_context:    Optional[Dict[str, Any]] = None


# ── Core analyzer ─────────────────────────────────────────────────────────────

async def run_analyze(req: AnalyzeRequest) -> AnalyzeResponse:
    """
    Execute a playbook analysis request.
    Routes to the appropriate scoring strategy based on context_mode.

    Discovery bridge:
      If req.discovery_mode is set AND req.playbook_id == "serenity":
        - Runs the discovery engine to surface candidates
        - Injects top discovered tickers into the analysis ticker list
        - Returns a combined answer referencing both discovery context + playbook scores
      For sjcapital: discovery_mode is accepted but does not alter S&J logic or philosophy.
      The /api/query handler is fully isolated — this bridge has zero coupling to it.
    """
    import os
    from services.playbook.playbook_registry import get as get_playbook
    from services.playbook.playbook_scoring import score_tickers_batch, score_ticker
    from services.playbook.playbook_service import score_portfolio, get_top_names
    from services.playbook.playbook_types import (
        ScorePortfolioRequest, PlaybookScoreResult,
    )
    from services.playbook.explainer import generate_explanation

    fmp_key     = os.getenv("FMP_API_KEY", "")
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")

    pb = get_playbook(req.playbook_id)
    if pb is None:
        raise ValueError(f"Unknown playbook_id: {req.playbook_id!r}")
    if not pb.enabled:
        raise ValueError(f"Playbook {req.playbook_id!r} is disabled")

    mode = (req.context_mode or "watchlist").lower().strip()
    portfolio_summary_result: Optional[PortfolioSummaryResult] = None
    all_results: List[PlaybookScoreResult] = []

    # ── Discovery bridge (Serenity only) ──────────────────────────────────────
    # If discovery_mode is set, run the discovery engine and inject top
    # discovered tickers into the analysis. For sjcapital, we accept the field
    # but do NOT alter S&J factor logic or philosophy.
    # This has ZERO coupling to /api/query.
    discovery_context: Dict[str, Any] = {}

    if req.discovery_mode:
        try:
            from services.playbook.discovery_types import DiscoverRequest
            from services.playbook.discovery_service import run_discover

            disc_req = DiscoverRequest(
                playbook_id=req.playbook_id,
                mode=req.discovery_mode,
                giant=req.giant,
                theme_ids=req.theme_ids,
                ticker=req.tickers[0] if req.tickers and req.discovery_mode == "ticker_chain" else None,
                include_foreign=req.include_foreign,
                country_filters=req.country_filters,
                max_depth=req.max_depth,
                limit=min(req.limit * 2, 20),
                only_hidden=False,
                use_web_validation=False,   # validation off in bridge mode for speed
                include_adr_or_etf_proxies=req.include_foreign,
            )
            disc_result = await run_discover(disc_req)

            # Inject discovered US-accessible tickers into the analysis
            discovered_tickers = [
                c.ticker for c in disc_result.top_candidates
                if c.direct_tradable  # only tickers we can actually score
            ]
            discovery_context = {
                "mode":          req.discovery_mode,
                "summary":       disc_result.summary,
                "top_discovered": discovered_tickers[:10],
                "total_found":   disc_result.meta.get("total_candidates_found", 0),
                "foreign_enabled": req.include_foreign,
                "candidate_previews": [
                    {
                        "ticker":   c.ticker,
                        "name":     c.company_name,
                        "country":  c.country,
                        "layer":    c.layer_depth,
                        "bc_score": c.scores.bottleneck_criticality_score,
                        "theme":    c.themes[0] if c.themes else "",
                    }
                    for c in disc_result.top_candidates[:8]
                ],
            }
            # Always include regime context for Serenity analyze calls.
            # When mode=auto: disc_result.regime_context is already the full dict.
            # When mode=anything_else: discovery didn't run regime detection, so
            # we compute it here (cheap, deterministic, no external calls) so the
            # frontend always knows which path was chosen and why.
            if req.playbook_id.lower() == "serenity":
                if disc_result.regime_context:
                    discovery_context["regime_context"] = disc_result.regime_context
                else:
                    from services.playbook.regime_service import compute_serenity_regime
                    discovery_context["regime_context"] = compute_serenity_regime().model_dump()

            # Merge discovered tickers into analysis tickers (US-accessible only)
            if discovered_tickers:
                existing = set(t.upper() for t in req.tickers)
                new_tickers = [t for t in discovered_tickers if t.upper() not in existing]
                # For serenity: augment; for sjcapital: augment only if tickers not already set
                if req.playbook_id.lower() == "serenity":
                    req = req.model_copy(update={
                        "tickers":      list(req.tickers) + new_tickers[:15],
                        "context_mode": "watchlist",
                    })
                elif not req.tickers:
                    req = req.model_copy(update={
                        "tickers":      new_tickers[:15],
                        "context_mode": "watchlist",
                    })
                mode = "watchlist"  # override mode for discovery bridge

        except Exception as e:
            print(f"[ANALYZER] Discovery bridge error: {e}")
            discovery_context = {"error": str(e)}

    # ── Route by context_mode ─────────────────────────────────────────────────
    if mode == "portfolio":
        holdings = req.holdings or []
        if not holdings and req.tickers:
            # Treat tickers as equal-weight holdings
            w = round(1.0 / len(req.tickers), 4)
            holdings = [{"ticker": t, "weight": w} for t in req.tickers]

        pf_req = ScorePortfolioRequest(
            playbook_id=req.playbook_id,
            holdings=holdings,
        )
        pf_result = await score_portfolio(pf_req)
        all_results = pf_result.holdings

        portfolio_summary_result = PortfolioSummaryResult(
            aggregate_score=pf_result.aggregate_score,
            strongest_aligned=pf_result.strongest_aligned,
            weakest_aligned=pf_result.weakest_aligned,
            concentration_notes=pf_result.concentration_notes,
        )

    elif mode == "universe":
        all_results = await get_top_names(req.playbook_id, req.universe, limit=50)

    else:
        # watchlist / custom
        tickers = [t.upper().strip() for t in req.tickers if t.strip()][:50]
        if not tickers and req.holdings:
            tickers = [h.get("ticker", "").upper().strip() for h in req.holdings if h.get("ticker")]

        if not tickers:
            raise ValueError("No tickers provided. Supply tickers or holdings.")

        all_results = await score_tickers_batch(tickers, pb, fmp_key, finnhub_key)

    # ── Generate explanations ─────────────────────────────────────────────────
    limit = max(1, min(req.limit, 30))

    top_results  = [r for r in all_results if r.hard_filter_pass and r.final_score >= 50.0][:limit]
    low_results  = [r for r in all_results if not r.hard_filter_pass or r.final_score < 50.0][:6]

    def _to_summary(r: PlaybookScoreResult, include_exp: bool = True) -> AnalyzeTickerSummary:
        exp: Dict = {}
        if include_exp:
            try:
                exp = generate_explanation(r, pb)
            except Exception as e:
                print(f"[ANALYZER] explanation error for {r.ticker}: {e}")

        sc_detail = r.factor_details.get("supply_chain_confirmation")
        sc_tags = sc_detail.source_tags if sc_detail else []

        return AnalyzeTickerSummary(
            ticker=r.ticker,
            final_score=r.final_score,
            hard_filter_pass=r.hard_filter_pass,
            thesis_summary=exp.get("thesis_summary", r.summary_label),
            fit_reasoning=exp.get("fit_reasoning", []),
            non_fit_reasoning=exp.get("non_fit_reasoning", r.hard_filter_failures[:2]),
            matched_themes=r.matched_themes,
            bottleneck_tags=r.bottleneck_tags,
            catalyst_signals=r.catalyst_signals[:2],
            supply_chain_tags=sc_tags,
            factor_scores=r.factor_scores if req.include_breakdown else {},
            penalties_applied=r.penalties_applied,
            stub_factors=r.stub_factors[:5],
        )

    top_summaries = [_to_summary(r, include_exp=True)  for r in top_results]
    low_summaries = [_to_summary(r, include_exp=False) for r in low_results]

    # ── Build answer string ───────────────────────────────────────────────────
    answer = _build_answer(
        req=req,
        pb=pb,
        mode=mode,
        top_summaries=top_summaries,
        low_summaries=low_summaries,
        portfolio_summary=portfolio_summary_result,
        all_count=len(all_results),
    )

    # ── Playbook context block ────────────────────────────────────────────────
    pb_context = {
        "preferred_themes":    pb.preferred_themes,
        "preferred_sectors":   pb.preferred_sectors,
        "positioning_style":   pb.positioning_style,
        "entry_style":         pb.entry_style,
        "exit_style":          pb.exit_style,
        "hard_filters":        [hf.label for hf in pb.hard_filters],
        "top_factor_weights": sorted(
            pb.factor_weights.items(), key=lambda x: x[1], reverse=True
        )[:5],
    }

    return AnalyzeResponse(
        playbook_id=req.playbook_id,
        playbook_name=pb.name,
        query=req.query,
        context_mode=mode,
        answer=answer,
        top_ranked=top_summaries,
        rejected_or_low_fit=low_summaries,
        portfolio_summary=portfolio_summary_result,
        playbook_context=pb_context,
        discovery_context=discovery_context,
        regime_context=discovery_context.pop("regime_context", None),
        meta={
            "total_scored":       len(all_results),
            "high_fit":           len(top_results),
            "low_fit_or_failed":  len(low_results),
            "discovery_used":     bool(discovery_context),
            "discovery_mode":     req.discovery_mode,
        },
    )


# ── Answer generation ─────────────────────────────────────────────────────────

def _build_answer(
    req: AnalyzeRequest,
    pb: Any,
    mode: str,
    top_summaries: List[AnalyzeTickerSummary],
    low_summaries: List[AnalyzeTickerSummary],
    portfolio_summary: Optional[PortfolioSummaryResult],
    all_count: int,
) -> str:
    """
    Build a concise, deterministic analytical answer string.
    Tone: analytical, strategy-aware, not verbose.
    """
    pb_name = pb.name
    query   = req.query.strip() if req.query else None

    if mode == "portfolio" and portfolio_summary:
        return _portfolio_answer(pb_name, portfolio_summary, top_summaries, low_summaries)

    if not top_summaries:
        return (
            f"No tickers passed the {pb_name} playbook filters out of {all_count} analyzed. "
            f"Reasons may include hard filter failures, weak sector momentum, or missing data. "
            f"Consider reviewing rejected names below."
        )

    # Build top-picks narrative
    top_names = [s.ticker for s in top_summaries[:3]]
    top_scores = {s.ticker: s.final_score for s in top_summaries[:3]}

    lead = f"Analyzed {all_count} name(s) against the {pb_name} playbook. "

    if len(top_summaries) == 1:
        t = top_summaries[0]
        lead += (
            f"{t.ticker} is the strongest match ({t.final_score:.0f}/100) — "
            f"{t.thesis_summary}"
        )
    else:
        picks_str = ", ".join(
            f"{t.ticker} ({t.final_score:.0f})" for t in top_summaries[:3]
        )
        lead += f"Top picks: {picks_str}. "

        # Best fit reasoning
        best = top_summaries[0]
        if best.fit_reasoning:
            lead += f"{best.ticker} leads with: {best.fit_reasoning[0].lower()}. "

    # Weak names context
    if low_summaries:
        low_names = [s.ticker for s in low_summaries[:2]]
        lead += (
            f"Lower-conviction names: {', '.join(low_names)} — "
            f"insufficient factor alignment for the {pb_name} framework."
        )

    # Playbook-specific addendum
    if pb.id == "serenity":
        lead += (
            " Serenity favors supply-chain bottleneck positions, clean balance sheets, "
            "and upcoming catalyst windows over broad sector exposure."
        )
    elif pb.id == "sjcapital":
        lead += (
            " S&J Capital prioritizes hot-sector momentum, EBITDA inflection proximity, "
            "and undervalued names with technical confirmation."
        )

    return lead.strip()


def _portfolio_answer(
    pb_name: str,
    ps: PortfolioSummaryResult,
    top_summaries: List[AnalyzeTickerSummary],
    low_summaries: List[AnalyzeTickerSummary],
) -> str:
    score = ps.aggregate_score
    if score >= 70:
        alignment = "strong"
    elif score >= 55:
        alignment = "moderate"
    else:
        alignment = "weak"

    ans = (
        f"Portfolio has {alignment} {pb_name} alignment (aggregate score {score:.0f}/100). "
    )

    if ps.strongest_aligned:
        ans += f"Best-aligned holdings: {', '.join(ps.strongest_aligned[:3])}. "

    if ps.weakest_aligned:
        ans += f"Weakest links: {', '.join(ps.weakest_aligned[:3])}. "

    for note in ps.concentration_notes[:2]:
        ans += note + " "

    return ans.strip()
