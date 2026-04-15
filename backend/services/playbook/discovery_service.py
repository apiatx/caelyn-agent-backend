"""
Discovery Service — core on-demand Serenity discovery + supply-chain intelligence engine.

Entry points:
  run_discover(req: DiscoverRequest) -> DiscoverResponse
  run_supply_chain_map(req: SupplyChainMapRequest) -> SupplyChainMapResponse

Discovery modes:
  giant_chain        — traverse supply chain from a major platform giant
  theme_scan         — scan companies relevant to one or more themes
  foreign_bottlenecks — focus on non-US supply chain positions
  ticker_chain       — find upstream/downstream neighbors of a known ticker
  country_theme_scan — cross-filter by country + theme
  custom             — query-driven fallback using all maps

Scoring dimensions (all deterministic/heuristic — no LLM inference):
  chain_depth_score             — layer 3-4 = deeper = more hidden
  bottleneck_criticality_score  — from curated NODE_REGISTRY
  hiddenness_score              — market cap + coverage + country + crowding heuristics
  giant_dependency_score        — how many giants anchor this node
  foreign_uniqueness_score      — non-US presence penalty/bonus
  supply_chain_confidence_score — evidence count + confidence label
  proxy_accessibility_score     — US=100, ADR=72, ETF=55, foreign=30
  theme_purity_score            — 1 theme=95, 2=80, 3=65, 4+=50

Provider usage summary:
  Finnhub   — profile + news enrichment for enriched candidates
  Tradier   — quote / liquidity for US-listed and ADR names
  FMP       — market cap (sparing; 250/day cap)
  Perplexity — shortlist validation only (max 5 per request)

Guardrails:
  - No /api/query coupling
  - No background scans — user-triggered only
  - No Brave / Tavily usage
  - Cache all provider calls aggressively
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional, Set

from services.playbook.discovery_types import (
    DiscoverRequest,
    DiscoverResponse,
    DiscoveryCandidate,
    DiscoveryScores,
    SupplyChainMapRequest,
    SupplyChainMapResponse,
    ChainLayer,
)
from services.playbook.supply_chain_graph import (
    NODE_REGISTRY,
    LAYER_LABELS,
    get_chain_for_theme,
    get_chain_for_giant,
    get_all_tickers_for_themes,
)
from services.playbook.giant_map import GIANT_MAP, get_giant, list_giants
from services.playbook.theme_discovery import THEME_TAXONOMY, get_theme, list_themes
from services.playbook.foreign_market_map import (
    COUNTRY_METADATA,
    FOREIGN_ACCESS_MAP,
    get_etf_proxies_for_theme,
)


# ── Discovery scoring engine ───────────────────────────────────────────────────

def _chain_depth_score(layer: int) -> float:
    """
    Layer depth → chain_depth_score.
    Deeper in chain = more hidden supplier = higher score.
    Layer 0 (giant) = 10, Layer 4 (upstream materials) = 95.
    """
    mapping = {0: 10, 1: 40, 2: 65, 3: 85, 4: 95}
    return float(mapping.get(layer, 50))


def _hiddenness_score(
    ticker: str,
    country: str,
    market_cap_usd: Optional[float],
    coverage_status: str,
    node: Dict[str, Any],
) -> tuple[float, str]:
    """
    Estimate how 'hidden' a company is — not widely known, under-covered.
    Returns (score, reason_string).
    """
    score = 40.0
    reason_parts: List[str] = []

    # Market cap heuristic
    if market_cap_usd is not None:
        cap_m = market_cap_usd / 1e6  # convert to millions if in USD
        # If passed as millions already (Finnhub format):
        if cap_m > 1e6:
            cap_m = cap_m / 1e6
        if cap_m < 2_000:      # < $2B
            score += 30
            reason_parts.append(f"Small cap (<$2B)")
        elif cap_m < 10_000:   # $2-10B
            score += 15
            reason_parts.append(f"Mid cap ($2-10B)")
        elif cap_m > 100_000:  # > $100B mega-cap
            score -= 20
            reason_parts.append("Mega-cap — well-covered")

    # Country heuristic (foreign = less US coverage = more hidden)
    if country != "US":
        score += 15
        reason_parts.append(f"Foreign ({country}) — limited US analyst coverage")

    # Coverage status from foreign map
    if coverage_status == "thin":
        score += 20
        reason_parts.append("Thin US data coverage")
    elif coverage_status == "partial":
        score += 10
        reason_parts.append("Partial US data coverage")

    # Bottleneck score: highly specialized = more hidden
    bottleneck_score = float(node.get("bottleneck_score", 50))
    if bottleneck_score >= 85:
        score += 8
        reason_parts.append("Highly specialized bottleneck position")

    score = max(10.0, min(95.0, score))
    return score, "; ".join(reason_parts) if reason_parts else "Mid-cap US company"


def _giant_dependency_score(node: Dict[str, Any]) -> float:
    """
    How directly tied to a major platform giant?
    Based on giant_anchors count.
    """
    anchors = node.get("giant_anchors", [])
    if not anchors:
        return 30.0
    if len(anchors) == 1:
        return 60.0
    if len(anchors) == 2:
        return 72.0
    if len(anchors) >= 3:
        return 82.0
    return 50.0


def _foreign_uniqueness_score(country: str, has_adr: bool, coverage: str) -> float:
    """
    Unique foreign exposure not easily accessible.
    US = 20 (not foreign), foreign with no ADR = 80+, foreign with ADR = 60.
    """
    if country == "US":
        return 20.0
    if not has_adr:
        return 82.0 if coverage == "thin" else 72.0
    return 58.0   # has ADR = more accessible = lower uniqueness bonus


def _supply_chain_confidence_score(node: Dict[str, Any]) -> float:
    """
    Evidence quality → confidence score.
    """
    conf_label = node.get("confidence", "medium")
    evidence   = node.get("evidence", [])
    base = {"high": 85.0, "medium": 65.0, "low": 45.0}.get(conf_label, 60.0)
    bonus = min(10.0, len(evidence) * 3.0)
    return min(98.0, base + bonus)


def _proxy_accessibility_score(country: str, has_us_proxy: bool, adr_ticker: Optional[str]) -> float:
    """
    How easily tradable from a US investor perspective.
    US-listed = 100, US ADR = 72, ETF proxy = 55, foreign-native-only = 30.
    """
    if country == "US":
        return 100.0
    if adr_ticker:
        return 72.0
    if has_us_proxy:
        return 55.0     # ETF proxy
    return 30.0


def _theme_purity_score(themes: List[str]) -> float:
    """
    Pure-play vs conglomerate. Fewer themes = purer = higher score.
    """
    n = len(themes)
    if n == 0:
        return 40.0
    if n == 1:
        return 95.0
    if n == 2:
        return 80.0
    if n == 3:
        return 65.0
    return 50.0


def _build_candidate(
    ticker: str,
    node: Dict[str, Any],
    market_cap_usd: Optional[float] = None,
    price: Optional[float] = None,
    extra_giant_anchors: Optional[List[str]] = None,
    coverage_override: Optional[str] = None,
) -> DiscoveryCandidate:
    """
    Construct a DiscoveryCandidate from a NODE_REGISTRY entry + optional live data.
    """
    country         = node.get("country", "US")
    layer           = node.get("layer", 2)
    themes          = node.get("themes", [])
    confidence      = node.get("confidence", "medium")
    us_access_proxy = node.get("us_access_proxy")
    adr_ticker      = node.get("adr_ticker")
    evidence        = node.get("evidence", [])
    giant_anchors   = node.get("giant_anchors", []) + (extra_giant_anchors or [])

    # Determine coverage status
    if country != "US":
        foreign_meta = FOREIGN_ACCESS_MAP.get(ticker, {})
        coverage_status = coverage_override or foreign_meta.get("coverage_status", "partial")
        data_confidence = foreign_meta.get("data_confidence", "medium")
        direct_tradable = bool(us_access_proxy or adr_ticker)
    else:
        coverage_status = coverage_override or "full"
        data_confidence = "high"
        direct_tradable = True

    # Score all 8 dimensions
    hidden_score, hidden_reason = _hiddenness_score(
        ticker, country, market_cap_usd, coverage_status, node
    )
    scores = DiscoveryScores(
        chain_depth_score=_chain_depth_score(layer),
        bottleneck_criticality_score=float(node.get("bottleneck_score", 50)),
        hiddenness_score=hidden_score,
        giant_dependency_score=_giant_dependency_score(node),
        foreign_uniqueness_score=_foreign_uniqueness_score(country, bool(adr_ticker), coverage_status),
        supply_chain_confidence_score=_supply_chain_confidence_score(node),
        proxy_accessibility_score=_proxy_accessibility_score(country, bool(us_access_proxy), adr_ticker),
        theme_purity_score=_theme_purity_score(themes),
    )

    # Build thesis summary
    role = node.get("role", "")
    thesis = _build_thesis(ticker, node.get("company_name", ticker), layer, scores, role, themes)

    # Fit reasoning bullets
    fit_reasoning = _build_fit_reasoning(node, scores, evidence)

    return DiscoveryCandidate(
        ticker=ticker,
        company_name=node.get("company_name", ticker),
        country=country,
        exchange=node.get("exchange", ""),
        themes=themes,
        chain_layers=[node.get("role", "")],
        layer_depth=layer,
        scores=scores,
        chain_depth_score=scores.chain_depth_score,
        bottleneck_criticality_score=scores.bottleneck_criticality_score,
        hiddenness_score=scores.hiddenness_score,
        supply_chain_confidence_score=scores.supply_chain_confidence_score,
        thesis_summary=thesis,
        fit_reasoning=fit_reasoning,
        giant_anchors=list(set(giant_anchors)),
        us_access_proxy=us_access_proxy,
        adr_ticker=adr_ticker,
        coverage_status=coverage_status,
        data_confidence=data_confidence,
        direct_tradable=direct_tradable,
        market_cap_usd=market_cap_usd,
        price=price,
        hiddenness_reason=hidden_reason,
        enriched=market_cap_usd is not None,
    )


def _build_thesis(
    ticker: str,
    company_name: str,
    layer: int,
    scores: DiscoveryScores,
    role: str,
    themes: List[str],
) -> str:
    """Build a concise thesis string from discovery scores."""
    depth_adj = {0: "platform-level", 1: "tier-1 supplier", 2: "component-level",
                 3: "bottleneck-positioned", 4: "upstream materials"}.get(layer, "supply-chain")
    sc_conf = scores.supply_chain_confidence_score
    bc_score = scores.bottleneck_criticality_score
    conviction = "high" if bc_score >= 80 else "moderate" if bc_score >= 60 else "lower"
    theme_str = themes[0] if themes else "supply-chain"
    return (
        f"{company_name} is a {depth_adj} name in {theme_str} "
        f"with {conviction} bottleneck criticality ({bc_score:.0f}/100). "
        f"{role[:80] if role else ''}"
    ).strip()


def _build_fit_reasoning(
    node: Dict[str, Any],
    scores: DiscoveryScores,
    evidence: List[str],
) -> List[str]:
    """Build fit reasoning bullets from node data and scores."""
    bullets: List[str] = []
    if evidence:
        bullets.append(evidence[0])
    if scores.bottleneck_criticality_score >= 80:
        bullets.append(f"Bottleneck criticality: {scores.bottleneck_criticality_score:.0f}/100 — hard to substitute")
    if scores.chain_depth_score >= 80:
        bullets.append(f"Deep supply chain position (layer {node.get('layer', 2)}) — low investor visibility")
    if scores.foreign_uniqueness_score >= 70:
        bullets.append(f"Foreign ({node.get('country', '?')}) — limited US analyst coverage creates alpha opportunity")
    if node.get("us_access_proxy"):
        bullets.append(f"US access via ADR: {node['us_access_proxy']}")
    return bullets[:4]


def _rank_candidates(candidates: List[DiscoveryCandidate], only_hidden: bool = False) -> List[DiscoveryCandidate]:
    """
    Rank candidates by composite discovery score.
    Weights: bottleneck_criticality (35%) + chain_depth (25%) + hiddenness (20%) + confidence (20%)
    """
    def _composite(c: DiscoveryCandidate) -> float:
        return (
            c.scores.bottleneck_criticality_score * 0.35 +
            c.scores.chain_depth_score            * 0.25 +
            c.scores.hiddenness_score             * 0.20 +
            c.scores.supply_chain_confidence_score * 0.20
        )
    filtered = [c for c in candidates if c.scores.hiddenness_score >= 55] if only_hidden else candidates
    return sorted(filtered, key=_composite, reverse=True)


def _build_summary(
    mode: str,
    top_candidates: List[DiscoveryCandidate],
    req: DiscoverRequest,
) -> str:
    """Generate a concise natural-language summary of discovery results."""
    if not top_candidates:
        return "No matching supply-chain candidates found with the given filters."

    top_names = [f"{c.ticker} ({c.scores.bottleneck_criticality_score:.0f})" for c in top_candidates[:3]]
    lead = f"Top hidden bottleneck candidates: {', '.join(top_names)}. "

    if top_candidates:
        best = top_candidates[0]
        lead += f"{best.ticker} leads — {best.thesis_summary[:100]}. "

    foreign_count = sum(1 for c in top_candidates if c.country != "US")
    if foreign_count > 0:
        lead += f"{foreign_count} foreign name(s) included with ADR/proxy guidance. "

    if mode == "giant_chain" and req.giant:
        lead += f"Discovery anchored on {req.giant} supply chain."
    elif mode == "theme_scan" and req.theme_ids:
        lead += f"Theme scan: {', '.join(req.theme_ids[:3])}."

    return lead.strip()


# ── Mode handlers ──────────────────────────────────────────────────────────────

def _candidates_from_nodes(
    node_items: List[tuple],
    include_foreign: bool,
    country_filters: List[str],
    max_depth: int,
) -> List[DiscoveryCandidate]:
    """Build candidates from (ticker, node_dict) pairs."""
    candidates: List[DiscoveryCandidate] = []
    seen: Set[str] = set()

    for ticker, node in node_items:
        if node is None:
            continue
        country = node.get("country", "US")
        if not include_foreign and country != "US":
            continue
        if country_filters and country not in country_filters:
            continue
        if node.get("layer", 2) > max_depth:
            continue

        # Use ADR as canonical ticker for foreign names
        canon = node.get("us_access_proxy", ticker) if country != "US" else ticker
        if canon in seen:
            continue
        seen.add(canon)

        c = _build_candidate(canon, node)
        candidates.append(c)
    return candidates


def _mode_giant_chain(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Giant chain mode: traverse from a giant anchor."""
    giant_id = (req.giant or "").strip()
    if not giant_id:
        return []

    giant = get_giant(giant_id)
    relevant_themes = set(giant.get("themes", [])) if giant else set()
    if req.theme_ids:
        relevant_themes = (relevant_themes & set(req.theme_ids)) if relevant_themes else set(req.theme_ids)

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        node_themes = set(node.get("themes", []))
        if relevant_themes and not (node_themes & relevant_themes):
            continue
        items.append((ticker, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_theme_scan(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Theme scan mode: find all companies across specified themes."""
    if not req.theme_ids:
        # Default to all high-priority Serenity themes
        req_themes = [tid for tid, meta in THEME_TAXONOMY.items()
                      if meta.get("serenity_priority") == "high"]
    else:
        req_themes = req.theme_ids
    theme_set = set(req_themes)

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if not (set(node.get("themes", [])) & theme_set):
            continue
        items.append((ticker, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_foreign_bottlenecks(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Foreign bottleneck mode: only non-US companies."""
    items: List[tuple] = []
    country_filters = req.country_filters or []

    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        country = node.get("country", "US")
        if country == "US":
            continue
        if country_filters and country not in country_filters:
            continue
        if node.get("layer", 2) > req.max_depth:
            continue
        items.append((ticker, node))

    # Foreign mode always includes foreign
    req_copy_include = True
    candidates: List[DiscoveryCandidate] = []
    seen: Set[str] = set()
    for ticker, node in items:
        if node is None:
            continue
        canon = node.get("us_access_proxy", ticker)
        if canon in seen:
            continue
        seen.add(canon)
        c = _build_candidate(canon, node)
        candidates.append(c)
    return candidates


def _mode_ticker_chain(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """
    Ticker chain mode: given a known ticker, find related upstream/downstream nodes
    that share themes with the given ticker.
    """
    ticker = (req.ticker or "").upper().strip()
    if not ticker:
        return []

    # Find the anchor node
    anchor_node = NODE_REGISTRY.get(ticker)
    if anchor_node is None:
        # Try to find by us_access_proxy
        for t, n in NODE_REGISTRY.items():
            if n and n.get("us_access_proxy", "").upper() == ticker:
                anchor_node = n
                break

    if anchor_node is None:
        # Fallback: match by themes from MANUAL_THEME_MAP in theme_map.py
        from services.playbook.theme_map import MANUAL_THEME_MAP
        anchor_themes = set(MANUAL_THEME_MAP.get(ticker, []))
    else:
        anchor_themes = set(anchor_node.get("themes", []))

    if not anchor_themes:
        return []

    if req.theme_ids:
        anchor_themes = anchor_themes & set(req.theme_ids)

    items: List[tuple] = []
    for t, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if t == ticker:
            continue
        if not (set(node.get("themes", [])) & anchor_themes):
            continue
        items.append((t, node))

    return _candidates_from_nodes(items, req.include_foreign, req.country_filters, req.max_depth)


def _mode_country_theme_scan(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Country+theme cross-filter."""
    country_set = set(req.country_filters) if req.country_filters else {"US", "JP", "KR", "TW", "NL", "DE"}
    theme_set   = set(req.theme_ids) if req.theme_ids else set(THEME_TAXONOMY.keys())

    items: List[tuple] = []
    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        if node.get("country", "US") not in country_set:
            continue
        if not (set(node.get("themes", [])) & theme_set):
            continue
        items.append((ticker, node))

    # Country-theme mode always includes foreign since country_filters is explicit
    return _candidates_from_nodes(items, True, req.country_filters, req.max_depth)


def _mode_custom(req: DiscoverRequest) -> List[DiscoveryCandidate]:
    """Custom mode: run theme_scan + optionally giant_chain as fallback."""
    if req.giant:
        return _mode_giant_chain(req)
    if req.theme_ids:
        return _mode_theme_scan(req)
    # Default: all nodes with serenity-priority themes
    return _mode_theme_scan(req)


# ── Main discovery orchestrator ────────────────────────────────────────────────

async def run_discover(req: DiscoverRequest) -> DiscoverResponse:
    """
    Run the discovery engine for a given request.
    Orchestrates mode-specific candidate building, scoring, ranking, and enrichment.
    """
    finnhub_key = os.getenv("FINNHUB_API_KEY", "")
    tradier_key = os.getenv("TRADIER_API_KEY", "")
    fmp_key     = os.getenv("FMP_API_KEY", "")
    perp_key    = os.getenv("PERPLEXITY_API_KEY", "")
    tradier_sandbox = os.getenv("TRADIER_SANDBOX", "false").lower() == "true"

    from services.playbook.discovery_enrichment import (
        enrich_batch_finnhub,
        enrich_us_quotes_tradier,
        fmp_market_cap,
        validate_shortlist_perplexity,
    )

    # ── 1. Run mode-specific candidate extraction ──────────────────────────────
    mode = req.mode.lower().strip()

    if mode == "giant_chain":
        candidates = _mode_giant_chain(req)
    elif mode == "theme_scan":
        candidates = _mode_theme_scan(req)
    elif mode == "foreign_bottlenecks":
        candidates = _mode_foreign_bottlenecks(req)
    elif mode == "ticker_chain":
        candidates = _mode_ticker_chain(req)
    elif mode == "country_theme_scan":
        candidates = _mode_country_theme_scan(req)
    else:
        candidates = _mode_custom(req)

    # ── 2. Rank candidates by composite score ─────────────────────────────────
    ranked = _rank_candidates(candidates, only_hidden=req.only_hidden)
    shortlist = ranked[:min(req.limit * 2, 40)]  # fetch enrichment for 2x limit

    # ── 3. Finnhub enrichment for shortlist ───────────────────────────────────
    us_tickers     = [c.ticker for c in shortlist if c.country == "US"]
    adr_tickers    = [c.adr_ticker for c in shortlist if c.adr_ticker and c.country != "US"]
    enrich_tickers = (us_tickers + adr_tickers)[:20]  # cap to ~20 Finnhub calls

    finnhub_profiles: Dict[str, Any] = {}
    if finnhub_key and enrich_tickers:
        try:
            finnhub_profiles = await enrich_batch_finnhub(enrich_tickers, finnhub_key)
        except Exception as e:
            print(f"[DISCOVERY] Finnhub batch enrich error: {e}")

    # ── 4. Tradier quote enrichment for US/ADR names ──────────────────────────
    tradier_quotes: Dict[str, Any] = {}
    tradier_tickers = [t for t in enrich_tickers if t in us_tickers][:10]
    if tradier_key and tradier_tickers:
        try:
            tradier_quotes = await enrich_us_quotes_tradier(tradier_tickers, tradier_key, tradier_sandbox)
        except Exception as e:
            print(f"[DISCOVERY] Tradier batch quote error: {e}")

    # ── 5. Apply enrichment data to candidates ─────────────────────────────────
    enriched_candidates: List[DiscoveryCandidate] = []
    for c in shortlist:
        lookup_key = c.ticker if c.ticker in finnhub_profiles else (c.adr_ticker or "")
        fh = finnhub_profiles.get(lookup_key, {})
        tq = tradier_quotes.get(c.ticker, {})

        # Update with live data if available
        market_cap = None
        if fh.get("market_cap"):
            market_cap = float(fh["market_cap"]) * 1e6  # Finnhub in millions → USD

        price = tq.get("price") or fh.get("price")

        # Re-score hiddenness with real market cap if available
        if market_cap is not None:
            # Lookup the node to re-score hiddenness with real cap data
            node = None
            for t, n in NODE_REGISTRY.items():
                if n and (n.get("us_access_proxy", t) == c.ticker or t.upper() == c.ticker.upper()):
                    node = n
                    break
            if node:
                hidden_score, hidden_reason = _hiddenness_score(
                    c.ticker, c.country, market_cap, c.coverage_status, node
                )
                new_scores = c.scores.model_copy(update={"hiddenness_score": hidden_score})
                c = c.model_copy(update={
                    "market_cap_usd":  market_cap,
                    "price":           price,
                    "hiddenness_reason": hidden_reason,
                    "enriched":        True,
                    "scores":          new_scores,
                    "hiddenness_score": hidden_score,
                })
            else:
                c = c.model_copy(update={"market_cap_usd": market_cap, "price": price, "enriched": True})
        elif price:
            c = c.model_copy(update={"price": price, "enriched": True})

        enriched_candidates.append(c)

    # ── 6. Perplexity validation on finalists (if requested) ──────────────────
    perp_notes: List[str] = []
    if req.use_web_validation and perp_key and enriched_candidates:
        top_for_validation = [
            {"ticker": c.ticker, "company_name": c.company_name, "role": c.chain_layers[0] if c.chain_layers else ""}
            for c in enriched_candidates[:5]
        ]
        try:
            validation = await validate_shortlist_perplexity(top_for_validation, perp_key, max_validate=5)
            for ticker, val in validation.items():
                if val.get("confirmed"):
                    perp_notes.append(f"Perplexity confirmed supply-chain role for {ticker}")
                else:
                    perp_notes.append(f"Perplexity could not confirm role for {ticker}")
        except Exception as e:
            print(f"[DISCOVERY] Perplexity validation error: {e}")
            perp_notes.append(f"Perplexity validation unavailable: {e}")

    # ── 7. Final ranking and split ─────────────────────────────────────────────
    final_ranked = _rank_candidates(enriched_candidates, only_hidden=req.only_hidden)
    limit = max(1, min(req.limit, 30))

    top_candidates  = final_ranked[:limit]
    low_confidence  = [c for c in final_ranked[limit:] if c.scores.supply_chain_confidence_score < 65][:5]

    # ── 8. Chain map preview (layer breakdown for top theme) ──────────────────
    chain_map_preview: Dict[str, Any] = {}
    preview_theme = req.theme_ids[0] if req.theme_ids else None
    if preview_theme:
        layers = get_chain_for_theme(preview_theme, req.max_depth, req.include_foreign, req.country_filters)
        chain_map_preview = {
            "theme":  preview_theme,
            "layers": [
                {
                    "layer": cl.layer_index,
                    "label": cl.label,
                    "tickers": [n.ticker for n in cl.nodes[:5]],
                }
                for cl in layers
            ],
        }

    # ── 9. ADR/ETF proxy suggestions for low-access foreign ───────────────────
    proxy_suggestions: Dict[str, str] = {}
    if req.include_adr_or_etf_proxies:
        for c in top_candidates:
            if c.country != "US" and not c.adr_ticker:
                for theme in c.themes[:1]:
                    etfs = get_etf_proxies_for_theme(theme)
                    if etfs:
                        proxy_suggestions[c.ticker] = etfs[0]

    # ── 10. Build summary and response ────────────────────────────────────────
    summary = _build_summary(mode, top_candidates, req)

    countries_scanned = list({c.country for c in top_candidates + low_confidence})
    provider_notes = ["Finnhub primary enrichment for profile/news"]
    if tradier_quotes:
        provider_notes.append("Tradier quote enrichment for US-listed and ADR names")
    if fmp_key:
        provider_notes.append("FMP market cap (sparing — 250/day cap)")
    if req.use_web_validation and perp_notes:
        provider_notes.extend(perp_notes[:3])
    provider_notes.append("No Brave/Tavily used in this flow")

    return DiscoverResponse(
        playbook_id=req.playbook_id,
        mode=mode,
        query=req.query,
        summary=summary,
        top_candidates=top_candidates,
        low_confidence_candidates=low_confidence,
        chain_map_preview=chain_map_preview,
        meta={
            "total_candidates_found":  len(candidates),
            "total_after_ranking":     len(final_ranked),
            "returned":               len(top_candidates),
            "low_confidence_returned": len(low_confidence),
            "countries_scanned":      countries_scanned,
            "foreign_enabled":        req.include_foreign,
            "max_depth":              req.max_depth,
            "only_hidden":            req.only_hidden,
            "proxy_suggestions":      proxy_suggestions,
            "provider_notes":         provider_notes,
        },
    )


# ── Supply Chain Map ───────────────────────────────────────────────────────────

async def run_supply_chain_map(req: SupplyChainMapRequest) -> SupplyChainMapResponse:
    """
    Build a structured multi-layer supply chain map for a giant or theme anchor.
    Returns layered nodes with country/exchange tags, evidence, and ADR proxies.
    """
    anchor_type: str
    anchor: str
    layers: List[ChainLayer]

    if req.anchor and req.anchor.upper() in {k.upper() for k in GIANT_MAP}:
        anchor = req.anchor
        anchor_type = "giant"
        layers = get_chain_for_giant(
            req.anchor,
            max_depth=req.max_depth,
            themes_filter=([req.theme_id] if req.theme_id else None),
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    elif req.theme_id:
        anchor = req.theme_id
        anchor_type = "theme"
        layers = get_chain_for_theme(
            req.theme_id,
            max_depth=req.max_depth,
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    elif req.anchor:
        # anchor is a theme_id
        anchor = req.anchor
        anchor_type = "theme"
        layers = get_chain_for_theme(
            req.anchor,
            max_depth=req.max_depth,
            include_foreign=req.include_foreign,
            country_filters=req.country_filters,
        )
    else:
        return SupplyChainMapResponse(
            anchor="unknown",
            anchor_type="unknown",
            layers=[],
            meta={"error": "No anchor or theme_id provided"},
        )

    # Build companies_by_layer
    companies_by_layer: Dict[str, List[str]] = {}
    all_countries: Set[str] = set()
    adr_etf_proxies: Dict[str, str] = {}

    for cl in layers:
        label = f"layer_{cl.layer_index}"
        companies_by_layer[label] = [n.ticker or n.company_name for n in cl.nodes]
        for node in cl.nodes:
            if node.country:
                all_countries.add(node.country)
            if node.us_access_proxy and node.country != "US":
                adr_etf_proxies[node.ticker or node.company_name] = node.us_access_proxy

    # Add ETF proxies for themes
    if anchor_type == "theme":
        etfs = get_etf_proxies_for_theme(anchor)
        for etf in etfs[:2]:
            adr_etf_proxies[f"ETF_{anchor}"] = etf

    evidence_sources = ["Curated NODE_REGISTRY (primary)", "GIANT_MAP theme associations"]
    if req.include_foreign:
        evidence_sources.append("FOREIGN_ACCESS_MAP for ADR/proxy guidance")

    return SupplyChainMapResponse(
        anchor=anchor,
        anchor_type=anchor_type,
        theme=req.theme_id,
        layers=layers,
        companies_by_layer=companies_by_layer,
        country_tags=sorted(all_countries),
        confidence="high",
        evidence_sources=evidence_sources,
        adr_etf_proxies=adr_etf_proxies,
        meta={
            "max_depth":       req.max_depth,
            "include_foreign": req.include_foreign,
            "country_filters": req.country_filters,
            "total_nodes":     sum(len(cl.nodes) for cl in layers),
        },
    )
