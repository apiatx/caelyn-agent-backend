"""
Prompt-aware automatic routing layer for Caelyn AI.

Classifies each request into a complexity RouteLabel using heuristic rules
(no LLM call — pure Python, <1ms), resolves the minimum viable model, and
supports tier escalation when a response fails.

Integration: called from _build_prompt in claude_agent.py immediately after
the static category→model selection block, to potentially downgrade the
assigned model when prompt complexity doesn't warrant premium tier.

Design principles:
  - Never makes an external call — classification is heuristic only
  - Never silently upgrades the model (except for explicit deep-research keywords)
  - Never removes extended thinking from categories that need it
  - Routing failure must never break a user request (all callers try/except)
  - Logs every routing decision via [ROUTE] + [MODEL_POLICY] structured lines

RouteLabel hierarchy (cheap → expensive):
  simple_lookup → short_summary → structured_extraction → ranking_scoring
  → standard_synthesis → multi_source_synthesis → deep_research
"""

from __future__ import annotations

import json as _json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from agent.model_policy import (
    log_ai_call,
    MODEL_CLAUDE_FAST,
    MODEL_CLAUDE_BALANCED,
    MODEL_CLAUDE_PREMIUM,
)


# ── Route complexity labels ───────────────────────────────────────────────────

class RouteLabel(str, Enum):
    """Complexity label assigned to each request by the heuristic classifier."""
    SIMPLE_LOOKUP          = "simple_lookup"          # single factual lookup, < 80 chars
    SHORT_SUMMARY          = "short_summary"          # explicitly asked for brief/summary
    STRUCTURED_EXTRACTION  = "structured_extraction"  # list / table / JSON / parse
    RANKING_SCORING        = "ranking_scoring"        # rank / score / compare / top-N
    STANDARD_SYNTHESIS     = "standard_synthesis"     # multi-factor analysis, normal query
    MULTI_SOURCE_SYNTHESIS = "multi_source_synthesis" # combining outputs from multiple models
    DEEP_RESEARCH          = "deep_research"          # exhaustive, premium-tier research


# ── Route decision ────────────────────────────────────────────────────────────

@dataclass
class RouteDecision:
    label: RouteLabel
    provider: str           # "claude" (others added when call infra is ready)
    tier: str               # "fast" | "balanced" | "premium"
    model: str              # exact model ID
    max_tokens: int
    rationale: str          # human-readable routing explanation
    escalation_level: int = 0   # 0=initial, 1=first retry, 2=max
    used_escalation: bool = False
    feature: str = ""
    category: str = ""
    downgraded_from: Optional[str] = None   # original model before downgrade


# ── Tier mappings ─────────────────────────────────────────────────────────────

_TIER_TO_MODEL: dict[str, str] = {
    "fast":     MODEL_CLAUDE_FAST,
    "balanced": MODEL_CLAUDE_BALANCED,
    "premium":  MODEL_CLAUDE_PREMIUM,
}
_TIER_RANK  = {"fast": 0, "balanced": 1, "premium": 2}
_RANK_TIER  = {0: "fast", 1: "balanced", 2: "premium"}


# ── Default tier + max_tokens per label ──────────────────────────────────────

_LABEL_TIER: dict[RouteLabel, str] = {
    RouteLabel.SIMPLE_LOOKUP:          "fast",
    RouteLabel.SHORT_SUMMARY:          "fast",
    RouteLabel.STRUCTURED_EXTRACTION:  "fast",
    RouteLabel.RANKING_SCORING:        "fast",
    RouteLabel.STANDARD_SYNTHESIS:     "balanced",
    RouteLabel.MULTI_SOURCE_SYNTHESIS: "balanced",
    RouteLabel.DEEP_RESEARCH:          "premium",
}

_LABEL_TOKENS: dict[RouteLabel, int] = {
    RouteLabel.SIMPLE_LOOKUP:          1000,
    RouteLabel.SHORT_SUMMARY:          800,
    RouteLabel.STRUCTURED_EXTRACTION:  2000,
    RouteLabel.RANKING_SCORING:        2000,
    RouteLabel.STANDARD_SYNTHESIS:     6000,
    RouteLabel.MULTI_SOURCE_SYNTHESIS: 8000,
    RouteLabel.DEEP_RESEARCH:          10000,
}


# ── Category floor tiers ──────────────────────────────────────────────────────
# Routes can never go below the floor tier for these categories, regardless of
# how simple the prompt appears. Protects high-value structured outputs.

_CATEGORY_FLOOR: dict[str, str] = {
    # Always premium — deep structured JSON output, extended thinking eligible
    "ticker_analysis":      "premium",
    "portfolio_review":     "premium",
    "best_trades":          "premium",
    "cross_market":         "premium",
    "cross_asset_trending": "premium",
    # Always at least balanced — produce structured cards / data tables
    "earnings_catalyst":    "balanced",
    "prediction_markets":   "balanced",
    "daily_briefing":       "balanced",
    "briefing":             "balanced",
    "investments":          "balanced",
    "csv_analysis":         "balanced",
    # Extended-thinking categories: allow one step down for clearly simple
    # prompts, but never to fast (extended thinking only runs at premium)
    "crypto":               "balanced",     # can be balanced for standard queries
    "chat":                 "balanced",
    "sector_rotation":      "balanced",
    "social_momentum":      "balanced",
    "thematic":             "balanced",
    # followup and other non-listed categories: no floor (fast OK)
}

# Categories where extended thinking is budgeted. Downgrading these to balanced
# or fast disables thinking. We allow balanced for simple lookups only.
_EXTENDED_THINKING_CATS = frozenset({
    "ticker_analysis", "best_trades", "cross_market", "crypto",
    "portfolio_review", "chat", "sector_rotation",
})


# ── Heuristic keyword patterns ────────────────────────────────────────────────

_RE_DEEP = re.compile(
    r"\b(deep[- ]?dive|deep research|deep analysis|comprehensive analysis|"
    r"in[- ]depth|in depth|full analysis|thorough analysis|thorough report|"
    r"detailed analysis|exhaustive|full report|everything about|"
    r"tell me everything|walk me through|complete overview|full breakdown|"
    r"full picture)\b",
    re.IGNORECASE,
)
_RE_ESCALATE = re.compile(
    r"\b(deeper|more detail|go deeper|more thorough|more comprehensive|"
    r"expand on|elaborate on|full picture|complete picture|explain more|"
    r"more context|more information)\b",
    re.IGNORECASE,
)
_RE_SUMMARY = re.compile(
    r"\b(summarize|summary|brief|quick overview|quick take|quick summary|"
    r"tl;?dr|tldr|in one sentence|in a sentence|short version|"
    r"key points|main points|brief overview|brief summary|give me the gist|"
    r"what happened|what's the deal|bottom line)\b",
    re.IGNORECASE,
)
_RE_RANKING = re.compile(
    r"\b(rank|ranking|rankings|score|scoring|rate |rated|top \d|top-\d|"
    r"top five|top ten|top three|best \d|worst \d|strongest|weakest|"
    r"compare|vs\.?\s|versus|which is better|which one|pick the best|"
    r"highest|lowest|most|least|sort by|order by)\b",
    re.IGNORECASE,
)
_RE_EXTRACTION = re.compile(
    r"\b(extract|parse|list out|give me a list|table of|in json|"
    r"as json|structure|enumerate|bullet points|bullet list|"
    r"key fields|list all|list the|show me all|what are all)\b",
    re.IGNORECASE,
)
_RE_SIMPLE_START = re.compile(
    r"^(what\s+is|what\'s|how\s+much|what\s+price|what\s+are\s+the\s+price|"
    r"is\s+\w+\s+bullish|is\s+\w+\s+bearish|current\s+price|price\s+of|"
    r"does\s+\w+|when\s+is|who\s+is|how\s+many|define\s+|what\s+does)\b",
    re.IGNORECASE,
)
_SYNTHESIS_KEYWORDS = frozenset({
    "analyze", "analysis", "research", "synthesis", "synthesize",
    "explain", "elaborate", "breakdown", "break down", "evaluate",
    "assess", "assess", "projection", "outlook",
})


# ── Core classifier ───────────────────────────────────────────────────────────

def classify_prompt(
    text: str,
    category: str = "",
    preset_intent: str = "",
) -> RouteLabel:
    """
    Classify a user prompt into a RouteLabel using heuristic rules.

    Pure Python — no LLM call, no I/O. Returns in <1ms.

    Priority order (first match wins):
      DEEP_RESEARCH > SIMPLE_LOOKUP > SHORT_SUMMARY >
      RANKING_SCORING > STRUCTURED_EXTRACTION > STANDARD_SYNTHESIS
    """
    # Normalize: use preset_intent as fallback text when prompt is empty
    effective = (text or "").strip()
    if not effective and preset_intent:
        effective = preset_intent.replace("_", " ")
    if not effective:
        return RouteLabel.STANDARD_SYNTHESIS

    length = len(effective)

    # 1. DEEP_RESEARCH — explicit deep-research keywords trump everything
    if _RE_DEEP.search(effective):
        return RouteLabel.DEEP_RESEARCH

    # 2. SIMPLE_LOOKUP — short factual question, no analysis keywords
    if (
        length < 80
        and effective.count("?") <= 1
        and not any(kw in effective.lower() for kw in _SYNTHESIS_KEYWORDS)
        and (_RE_SIMPLE_START.match(effective) or "?" in effective)
    ):
        return RouteLabel.SIMPLE_LOOKUP

    # 3. SHORT_SUMMARY — explicit brevity/summary request
    if _RE_SUMMARY.search(effective):
        return RouteLabel.SHORT_SUMMARY

    # 4. RANKING_SCORING — ranking / comparison / top-N
    if _RE_RANKING.search(effective):
        return RouteLabel.RANKING_SCORING

    # 5. STRUCTURED_EXTRACTION — list / table / JSON extraction
    if _RE_EXTRACTION.search(effective):
        return RouteLabel.STRUCTURED_EXTRACTION

    # 6. MULTI_SOURCE_SYNTHESIS — multi-model collab synthesis context
    if category == "multi_source_synthesis":
        return RouteLabel.MULTI_SOURCE_SYNTHESIS

    # 7. Default
    return RouteLabel.STANDARD_SYNTHESIS


# ── Route resolver ────────────────────────────────────────────────────────────

def resolve_route(
    text: str,
    category: str = "",
    preset_intent: str = "",
    current_model: str = "",
    current_tokens: int = 0,
    feature: str = "",
) -> RouteDecision:
    """
    Resolve the minimum viable (provider, tier, model) for this request.

    Logic:
      1. Classify the prompt into a RouteLabel
      2. Map the label to a desired tier
      3. Apply the category floor (never go below a category's minimum)
      4. For extended-thinking categories: apply special floor rules
      5. Compare against the current (statically-assigned) model:
           - If desired tier is cheaper: downgrade (cost saving)
           - If desired tier is more expensive AND label is DEEP_RESEARCH: upgrade
           - Otherwise: keep current (don't silently upgrade)
      6. Return RouteDecision with full rationale
    """
    label = classify_prompt(text, category, preset_intent)

    # Target tier from label
    target_tier = _LABEL_TIER[label]
    target_tokens = _LABEL_TOKENS[label]

    # Category floor
    floor_tier = _CATEGORY_FLOOR.get(category, "fast")

    # Extended-thinking categories — soften the floor for clearly simple prompts
    if category in _EXTENDED_THINKING_CATS:
        if label in (RouteLabel.SIMPLE_LOOKUP, RouteLabel.SHORT_SUMMARY):
            # Allow balanced (disables thinking but saves ~80% cost on trivial queries)
            floor_tier = max(floor_tier, "balanced",
                             key=lambda t: _TIER_RANK.get(t, 0))
        elif label != RouteLabel.DEEP_RESEARCH:
            # Keep premium for anything non-trivial in thinking categories
            floor_tier = "premium"

    # Effective tier = max(target, floor)
    effective_tier = _RANK_TIER[
        max(_TIER_RANK[target_tier], _TIER_RANK[floor_tier])
    ]

    # Compare against current model
    current_tier = _model_to_tier(current_model)
    if current_tier is not None:
        cur_rank = _TIER_RANK[current_tier]
        eff_rank = _TIER_RANK[effective_tier]

        if label == RouteLabel.DEEP_RESEARCH:
            # Explicit deep-research always warrants premium
            effective_tier = "premium"
        elif cur_rank <= eff_rank:
            # Current is already at or below the desired tier — don't upgrade
            effective_tier = current_tier
        # else: current is over-provisioned → downgrade to effective_tier

    effective_model = _TIER_TO_MODEL[effective_tier]

    # Token limit: keep at least the current allocation to avoid truncation
    effective_tokens = max(target_tokens, current_tokens) if current_tokens else target_tokens

    # Build rationale
    downgraded_from: Optional[str] = None
    orig_tier = current_tier or "unknown"
    if current_model and effective_model != current_model:
        if _TIER_RANK.get(effective_tier, 1) < _TIER_RANK.get(orig_tier, 2):
            downgraded_from = current_model
            rationale = (
                f"Downgraded {orig_tier}→{effective_tier} "
                f"(label={label.value}, category={category or 'none'})"
            )
        else:
            rationale = (
                f"Upgraded {orig_tier}→{effective_tier} "
                f"(label={label.value}, deep_keywords=True)"
            )
    else:
        rationale = (
            f"No change (label={label.value}, tier={effective_tier}, "
            f"category={category or 'none'})"
        )

    return RouteDecision(
        label=label,
        provider="claude",
        tier=effective_tier,
        model=effective_model,
        max_tokens=effective_tokens,
        rationale=rationale,
        feature=feature,
        category=category,
        downgraded_from=downgraded_from,
    )


# ── Escalation ────────────────────────────────────────────────────────────────

def escalate_route(decision: RouteDecision) -> RouteDecision:
    """
    Escalate to the next stronger tier after a failed/empty response.
    Returns a new RouteDecision at the next tier (or stays at premium if already there).
    """
    cur_rank = _TIER_RANK.get(decision.tier, 0)
    next_rank = min(cur_rank + 1, 2)
    next_tier = _RANK_TIER[next_rank]
    next_model = _TIER_TO_MODEL[next_tier]
    # Ensure enough tokens for the escalated attempt
    next_tokens = max(
        decision.max_tokens,
        _LABEL_TOKENS.get(RouteLabel.STANDARD_SYNTHESIS, 6000),
    )
    return RouteDecision(
        label=decision.label,
        provider=decision.provider,
        tier=next_tier,
        model=next_model,
        max_tokens=next_tokens,
        rationale=f"Escalated {decision.tier}→{next_tier}: empty/failed response",
        escalation_level=decision.escalation_level + 1,
        used_escalation=True,
        feature=decision.feature,
        category=decision.category,
    )


# ── Observability ─────────────────────────────────────────────────────────────

def log_route(
    decision: RouteDecision,
    latency_ms: Optional[float] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    success: bool = True,
) -> None:
    """
    Emit a [ROUTE] structured log line (parse these for routing analytics)
    plus a [MODEL_POLICY] cost/latency log line.

    [ROUTE] example:
        [ROUTE] {"feature":"build_prompt/chat","category":"chat",
                 "classifier_label":"simple_lookup","provider":"claude",
                 "tier":"balanced","model":"claude-sonnet-4-20250514",
                 "downgraded_from":"claude-sonnet-4-5-20250929","latency_ms":412.3}
    """
    record: dict = {
        "feature":          decision.feature or decision.category,
        "category":         decision.category,
        "classifier_label": decision.label.value,
        "provider":         decision.provider,
        "tier":             decision.tier,
        "model":            decision.model,
    }
    if decision.downgraded_from:
        record["downgraded_from"] = decision.downgraded_from
    if decision.used_escalation:
        record["escalation_level"] = decision.escalation_level
    if latency_ms is not None:
        record["latency_ms"] = round(latency_ms, 1)
    if not success:
        record["success"] = False
    print(f"[ROUTE] {_json.dumps(record)}")

    # Also emit a MODEL_POLICY cost/latency record
    log_ai_call(
        task_type=decision.category or "freeform_query",
        provider=decision.provider,
        model=decision.model,
        feature=decision.feature or decision.category,
        latency_ms=latency_ms,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        escalation_used=decision.used_escalation,
        success=success,
    )


def is_escalation_trigger(text: str) -> bool:
    """Return True if the prompt contains explicit escalation keywords."""
    return bool(_RE_ESCALATE.search(text)) if text else False


# ── Internal helpers ──────────────────────────────────────────────────────────

def _model_to_tier(model_id: str) -> Optional[str]:
    """Reverse-map a model ID to its tier name. Returns None for unknown models."""
    for tier, mid in _TIER_TO_MODEL.items():
        if mid == model_id:
            return tier
    return None
