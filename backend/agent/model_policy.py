"""
Model Policy Registry — single source of truth for all AI model selection in Caelyn AI.

Phases 2-7 of the centralized model routing initiative.

USAGE:
    from agent.model_policy import resolve as mp_resolve, log_ai_call
    model_id = mp_resolve("claude", "query_classification")
    # → "claude-haiku-4-5-20251001"

    # Convenience constants (import once, use everywhere):
    from agent.model_policy import (
        MODEL_CLAUDE_FAST, MODEL_CLAUDE_BALANCED, MODEL_CLAUDE_PREMIUM,
        MODEL_GPT4O, MODEL_GPT4O_MINI,
        MODEL_GROK,
        MODEL_GEMINI, MODEL_GEMINI_25_FLASH,
        MODEL_SONAR, MODEL_SONAR_PRO,
        MODEL_DEEPSEEK,
    )

    # Extended-thinking guard:
    from agent.model_policy import supports_extended_thinking
    use_thinking = budget > 0 and supports_extended_thinking(model)

FEATURE FLAG:
    Set MODEL_POLICY_ENABLED=0 in env to disable and fall back to legacy model strings.
    Default: enabled (1).

TASK TAXONOMY:
    Internal / utility (fast, low-cost, not user-visible):
        query_classification  — classify user intent
        followup_suggestions  — generate follow-up question list
        short_summary         — brief text summary
        backtest_summary      — backtest results table summary
        notification_digest   — daily AI notification digest
        hyperliquid_summary   — Hyperliquid screener data summary
        dashboard_scoring     — AI-scored dashboard sections (precomputed)
        whale_discovery       — Perplexity whale/investor discovery
        discovery_validation  — Perplexity shortlist candidate validation (fast sonar)
        news_fetch            — Perplexity fast news fetch per ticker (fast sonar)

    Orchestration:
        orchestrator_routing  — smart orchestration JSON dispatch

    User-facing synthesis (medium complexity, balanced):
        options_analysis      — options flow chat response
        freeform_query        — free-form user chat
        multi_source_synthesis — multi-model collab synthesis
        watchlist_ranking     — watchlist per-ticker scoring
        watchlist_synthesis   — watchlist final structured synthesis

    User-facing deep analysis (premium, extended-thinking eligible):
        preset_prompt         — preset button structured response
        fundamental_analysis  — per-ticker deep fundamental analysis
        ticker_analysis       — ticker deep-dive
        high_complexity_research — deep research / CSV analysis

    Social / X sentiment (Grok-only):
        x_sentiment           — X/Twitter social sentiment search

    Web grounding (collaborator roles):
        news_grounding        — web/news search (Perplexity sonar-pro)
        web_research          — web research (Gemini 3 Flash + Google Search)
        watchlist_web_research — watchlist Gemini grounding (Gemini 2.5 Flash)

    Background / cached:
        sector_rotation_ai    — weekly sector rotation (Gemini 3 Flash + Google Search)
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

# ── Feature flag ──────────────────────────────────────────────────────────────
_POLICY_ENABLED: bool = os.getenv("MODEL_POLICY_ENABLED", "1").strip() not in ("0", "false", "no")

# ── Provider family → tiered exact model IDs ─────────────────────────────────
# Tiers:
#   fast     — lightweight, low-latency utility (classification, short summaries, notifications)
#   balanced — standard reasoning, good speed/quality (orchestration, most synthesis)
#   premium  — deeper reasoning, extended thinking eligible (complex user-facing analysis)

PROVIDER_REGISTRY: dict[str, dict[str, str]] = {
    "claude": {
        "fast":     "claude-haiku-4-5-20251001",   # classification, notifications, backtest digest
        "balanced": "claude-sonnet-4-5-20250929",  # orchestration, simpler synthesis, followup
        "premium":  "claude-sonnet-4-5-20250929",  # deep analysis, extended thinking
    },
    "gpt-4o": {
        "fast":     "gpt-4o-mini",
        "balanced": "gpt-4o",
        "premium":  "gpt-4o",
    },
    "grok": {
        "fast":     "grok-4-1-fast-reasoning",
        "balanced": "grok-4-1-fast-reasoning",
        "premium":  "grok-4-1-fast-reasoning",
    },
    "gemini": {
        "fast":     "gemini-3-flash-preview",
        "balanced": "gemini-3-flash-preview",
        "premium":  "gemini-3-flash-preview",
    },
    "perplexity": {
        "fast":     "sonar",
        "balanced": "sonar-pro",
        "premium":  "sonar-pro",
    },
    "deepseek": {
        "fast":     "deepseek-chat",
        "balanced": "deepseek-chat",
        "premium":  "deepseek-chat",
    },
}

# ── Task taxonomy → default policy ───────────────────────────────────────────
TASK_POLICY: dict[str, dict] = {
    # ── Internal / utility ───────────────────────────────────────────────────
    "query_classification":     {"provider": "claude",      "tier": "fast",     "max_tokens": 200},
    "followup_suggestions":     {"provider": "claude",      "tier": "fast",     "max_tokens": 200},
    "short_summary":            {"provider": "claude",      "tier": "fast",     "max_tokens": 400},
    "backtest_summary":         {"provider": "claude",      "tier": "fast",     "max_tokens": 400},
    "notification_digest":      {"provider": "claude",      "tier": "fast",     "max_tokens": 1400},
    "hyperliquid_summary":      {"provider": "claude",      "tier": "fast",     "max_tokens": 600},
    "dashboard_scoring":        {"provider": "claude",      "tier": "fast",     "max_tokens": 1400},
    "whale_discovery":          {"provider": "perplexity",  "tier": "fast",     "max_tokens": 1000},
    "discovery_validation":     {"provider": "perplexity",  "tier": "fast",     "max_tokens": 300},
    "news_fetch":               {"provider": "perplexity",  "tier": "fast",     "max_tokens": 600},
    # ── Orchestration ────────────────────────────────────────────────────────
    "orchestrator_routing":     {"provider": "claude",      "tier": "balanced", "max_tokens": 500},
    # ── User-facing synthesis (medium) ───────────────────────────────────────
    "options_analysis":         {"provider": "claude",      "tier": "balanced", "max_tokens": 2000},
    "freeform_query":           {"provider": "claude",      "tier": "balanced", "max_tokens": 8000},
    "multi_source_synthesis":   {"provider": "claude",      "tier": "balanced", "max_tokens": 8000},
    "watchlist_ranking":        {"provider": "claude",      "tier": "balanced", "max_tokens": 4096},
    "watchlist_synthesis":      {"provider": "claude",      "tier": "balanced", "max_tokens": 4096},
    # ── User-facing deep analysis (premium, extended-thinking eligible) ──────
    "preset_prompt":            {"provider": "claude",      "tier": "premium",  "max_tokens": 10000},
    "fundamental_analysis":     {"provider": "claude",      "tier": "premium",  "max_tokens": 10000},
    "ticker_analysis":          {"provider": "claude",      "tier": "premium",  "max_tokens": 10000},
    "high_complexity_research": {"provider": "claude",      "tier": "premium",  "max_tokens": 16384},
    # ── Social / X sentiment ─────────────────────────────────────────────────
    "x_sentiment":              {"provider": "grok",        "tier": "balanced", "max_tokens": 2000},
    # ── Web grounding (collaborators) ────────────────────────────────────────
    "news_grounding":           {"provider": "perplexity",  "tier": "balanced", "max_tokens": 2000},
    "web_research":             {"provider": "gemini",      "tier": "balanced", "max_tokens": 2000},
    "watchlist_web_research":   {"provider": "gemini",      "tier": "balanced", "max_tokens": 8192},
    # ── Background / cached ──────────────────────────────────────────────────
    "sector_rotation_ai":       {"provider": "gemini",      "tier": "balanced", "max_tokens": 4096},
}

# ── Legacy model strings (used when POLICY_ENABLED=0) ────────────────────────
_LEGACY: dict[str, str] = {
    "query_classification":     "claude-haiku-4-5-20251001",
    "followup_suggestions":     "claude-haiku-4-5-20251001",
    "short_summary":            "claude-haiku-4-5-20251001",
    "backtest_summary":         "claude-haiku-4-5-20251001",
    "notification_digest":      "claude-haiku-4-5-20251001",
    "hyperliquid_summary":      "claude-3-haiku-20240307",
    "dashboard_scoring":        "claude-haiku-4-5-20251001",
    "whale_discovery":          "sonar",
    "discovery_validation":     "sonar",
    "news_fetch":               "sonar",
    "orchestrator_routing":     "claude-sonnet-4-5-20250929",
    "options_analysis":         "claude-sonnet-4-5-20250929",
    "freeform_query":           "claude-sonnet-4-5-20250929",
    "multi_source_synthesis":   "claude-sonnet-4-5-20250929",
    "watchlist_ranking":        "claude-opus-4-5",
    "watchlist_synthesis":      "claude-opus-4-5",
    "preset_prompt":            "claude-sonnet-4-5-20250929",
    "fundamental_analysis":     "claude-sonnet-4-5-20250929",
    "ticker_analysis":          "claude-sonnet-4-5-20250929",
    "high_complexity_research": "claude-sonnet-4-5-20250929",
    "x_sentiment":              "grok-4-1-fast-reasoning",
    "news_grounding":           "sonar-pro",
    "web_research":             "gemini-3-flash-preview",
    "watchlist_web_research":   "gemini-2.5-flash",
    "sector_rotation_ai":       "gemini-3-flash-preview",
}

# ── Cost lookup table: (input $/1M tokens, output $/1M tokens) ───────────────
# Used by log_ai_call() to emit estimated_cost_usd. Best-effort approximation;
# update as provider pricing changes. Omitting a model → no cost estimate.
_COST_PER_MILLION: dict[str, tuple[float, float]] = {
    # Claude
    "claude-haiku-4-5-20251001":  (0.80,   4.00),
    "claude-sonnet-4-5-20250929": (3.00,  15.00),
    # OpenAI
    "gpt-4o":                     (2.50,  10.00),
    "gpt-4o-mini":                (0.15,   0.60),
    # xAI
    "grok-4-1-fast-reasoning":    (5.00,  15.00),
    # Gemini (approximate)
    "gemini-3-flash-preview":     (0.075,  0.30),
    "gemini-2.5-flash":           (0.15,   0.60),
    # Perplexity
    "sonar":                      (1.00,   1.00),
    "sonar-pro":                  (3.00,   3.00),
    # DeepSeek
    "deepseek-chat":              (0.14,   0.28),
}


# ── Core resolution functions ─────────────────────────────────────────────────

def resolve(provider: str, task_type: str, tier_override: Optional[str] = None) -> str:
    """
    Resolve a (provider, task_type) pair to an exact model ID string.

    Args:
        provider:      Provider family: "claude", "gpt-4o", "grok", "gemini",
                       "perplexity", "deepseek"
        task_type:     One of the TASK_POLICY keys (see module docstring)
        tier_override: Optional override — "fast" | "balanced" | "premium"

    Returns:
        Exact model ID string, e.g. "claude-sonnet-4-5-20250929"
    """
    if not _POLICY_ENABLED:
        return _LEGACY.get(task_type, PROVIDER_REGISTRY.get(provider, {}).get("balanced", provider))

    registry = PROVIDER_REGISTRY.get(provider)
    if not registry:
        return provider  # pass-through for unknown providers

    if tier_override:
        tier = tier_override
    else:
        task = TASK_POLICY.get(task_type, {})
        tier = task.get("tier", "balanced") if task.get("provider") == provider else "balanced"

    return registry.get(tier, registry["balanced"])


def resolve_for_task(
    task_type: str,
    provider_override: Optional[str] = None,
    tier_override: Optional[str] = None,
) -> tuple[str, str, int]:
    """
    Fully resolve a task to (provider, model_id, max_tokens).

    Args:
        task_type:         One of the TASK_POLICY keys
        provider_override: Override the default provider for this task
        tier_override:     Override the default tier

    Returns:
        (provider, model_id, max_tokens)
    """
    task = TASK_POLICY.get(task_type, {"provider": "claude", "tier": "balanced", "max_tokens": 4096})
    provider = provider_override or task["provider"]
    tier = tier_override or task["tier"]
    model_id = resolve(provider, task_type, tier_override=tier)
    return provider, model_id, task.get("max_tokens", 4096)


def supports_extended_thinking(model_id: str) -> bool:
    """
    Return True if this model supports Claude extended thinking (budget_tokens).

    Only claude-sonnet-4-5 (MODEL_CLAUDE_PREMIUM) supports extended thinking.
    Using this function instead of an in-string check means a model ID change
    in PROVIDER_REGISTRY automatically propagates here.
    """
    premium_model = PROVIDER_REGISTRY.get("claude", {}).get("premium", "")
    return bool(premium_model) and model_id == premium_model


def estimate_cost_usd(
    model: str,
    input_tokens: Optional[int],
    output_tokens: Optional[int],
) -> Optional[float]:
    """
    Estimate API cost in USD given a model ID and token counts.
    Returns None if the model is not in the cost table or tokens are unknown.
    """
    if input_tokens is None and output_tokens is None:
        return None
    costs = _COST_PER_MILLION.get(model)
    if costs is None:
        return None
    in_cost, out_cost = costs
    total = (input_tokens or 0) / 1_000_000 * in_cost + (output_tokens or 0) / 1_000_000 * out_cost
    return round(total, 6)


def log_ai_call(
    task_type: str,
    provider: str,
    model: str,
    feature: str,
    latency_ms: Optional[float] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    fallback_used: bool = False,
    escalation_used: bool = False,
    cache_hit: bool = False,
    success: bool = True,
    extra: Optional[dict] = None,
) -> None:
    """
    Emit a structured [MODEL_POLICY] log line for every AI call.
    Parse from logs to audit latency, cost, and over-modelling.

    Example output:
        [MODEL_POLICY] {"task_type":"query_classification","provider":"claude",
                        "model":"claude-haiku-4-5-20251001","feature":"classify_query",
                        "latency_ms":312.4,"input_tokens":450,"output_tokens":80,
                        "estimated_cost_usd":0.000356}
    """
    record: dict = {
        "task_type": task_type,
        "provider": provider,
        "model": model,
        "feature": feature,
        "policy_enabled": _POLICY_ENABLED,
    }
    if latency_ms is not None:
        record["latency_ms"] = round(latency_ms, 1)
    if input_tokens is not None:
        record["input_tokens"] = input_tokens
    if output_tokens is not None:
        record["output_tokens"] = output_tokens
    cost = estimate_cost_usd(model, input_tokens, output_tokens)
    if cost is not None:
        record["estimated_cost_usd"] = cost
    if fallback_used:
        record["fallback_used"] = True
    if escalation_used:
        record["escalation_used"] = True
    if cache_hit:
        record["cache_hit"] = True
    if not success:
        record["success"] = False
    if extra:
        record.update(extra)
    print(f"[MODEL_POLICY] {json.dumps(record)}")


# ── Convenience model-ID constants ───────────────────────────────────────────
# Import these into call sites to avoid string literals.
# These always reflect the current PROVIDER_REGISTRY values, respecting the flag.
MODEL_CLAUDE_FAST:      str = resolve("claude",      "query_classification")
MODEL_CLAUDE_BALANCED:  str = resolve("claude",      "orchestrator_routing")
MODEL_CLAUDE_PREMIUM:   str = resolve("claude",      "preset_prompt")
MODEL_GPT4O:            str = resolve("gpt-4o",      "orchestrator_routing")
MODEL_GPT4O_MINI:       str = PROVIDER_REGISTRY["gpt-4o"]["fast"]
MODEL_GROK:             str = resolve("grok",        "x_sentiment")
MODEL_GEMINI:           str = resolve("gemini",      "web_research")
MODEL_GEMINI_25_FLASH:  str = _LEGACY["watchlist_web_research"]  # "gemini-2.5-flash"
MODEL_SONAR:            str = PROVIDER_REGISTRY["perplexity"]["fast"]   # "sonar"
MODEL_SONAR_PRO:        str = resolve("perplexity",  "news_grounding")
MODEL_DEEPSEEK:         str = resolve("deepseek",    "orchestrator_routing")
