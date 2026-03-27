"""
Phase 0 — Mode name normalization layer.

Maps frontend display labels ("caelyn", "customize", legacy strings) to the
internal reasoning_model identifiers the backend pipeline uses.

INTERNAL IDENTIFIERS (never change these — business logic depends on them):
  "agent_collab"  — Caelyn automatic smart mode (Grok + Perplexity data sources
                    + proprietary pipeline → single reasoning model synthesizes).
  "all_agents"    — Full fan-out (every agent runs independently, then synthesizes).
  "claude"        — Solo Claude.
  "gpt-4o"        — Solo GPT-4o.
  "grok"          — Solo Grok.
  "gemini"        — Solo Gemini.
  "perplexity"    — Solo Perplexity.

UI CONCEPTS (frontend display only):
  "caelyn"        — Maps to agent_collab (automatic smart mode).
  "customize"     — No single internal model; used to signal the user is in the
                    Customize panel. The actual reasoning_model comes from the
                    frontend preset/collab selection, defaulting to agent_collab.
"""

# ── Public concept names ─────────────────────────────────────
CONCEPT_CAELYN    = "caelyn"
CONCEPT_CUSTOMIZE = "customize"
CONCEPT_SOLO      = "solo"

# ── Internal reasoning_model identifiers ─────────────────────
MODEL_AGENT_COLLAB = "agent_collab"
MODEL_ALL_AGENTS   = "all_agents"
_SOLO_MODELS       = {"claude", "gpt-4o", "grok", "gemini", "perplexity"}
_ALL_VALID_MODELS  = {MODEL_AGENT_COLLAB, MODEL_ALL_AGENTS} | _SOLO_MODELS

# ── Inbound normalization map ─────────────────────────────────
# Maps any string the frontend might send → internal identifier.
# Old strings are preserved as aliases so existing payloads never break.
_INBOUND: dict[str, str] = {
    # New frontend display labels
    "caelyn":           MODEL_AGENT_COLLAB,
    "caelyn_mode":      MODEL_AGENT_COLLAB,
    "auto":             MODEL_AGENT_COLLAB,
    "automatic":        MODEL_AGENT_COLLAB,
    "smart":            MODEL_AGENT_COLLAB,
    # Legacy / alternate spellings
    "default":          MODEL_AGENT_COLLAB,
    "default_collab":   MODEL_AGENT_COLLAB,
    "collab":           MODEL_AGENT_COLLAB,
    # Customize panel sends the actual model or all_agents; pass through.
    # "customize" itself defaults to agent_collab if no collab_agents supplied.
    "customize":        MODEL_AGENT_COLLAB,
    "custom":           MODEL_AGENT_COLLAB,
    "custom_collab":    MODEL_AGENT_COLLAB,
    # Full fan-out aliases
    "full_collab":      MODEL_ALL_AGENTS,
    "full":             MODEL_ALL_AGENTS,
    "all":              MODEL_ALL_AGENTS,
}


def normalize_reasoning_model(value: str | None) -> str:
    """
    Normalize any inbound reasoning_model / mode string to an internal
    identifier. Safe to call at every API entry point.

    Returns "agent_collab" (Caelyn) as the default for unknown/None inputs.
    Never raises — worst case returns the fallback.
    """
    if not value:
        return MODEL_AGENT_COLLAB

    cleaned = str(value).strip().lower()

    # Already a valid internal identifier — pass through unchanged.
    if cleaned in _ALL_VALID_MODELS:
        return cleaned

    # Map via alias table.
    mapped = _INBOUND.get(cleaned)
    if mapped:
        return mapped

    # Unknown string — log and return default so we never break.
    print(f"[MODE_NORMALIZER] Unknown reasoning_model '{value}', defaulting to agent_collab")
    return MODEL_AGENT_COLLAB


def mode_concept(reasoning_model: str | None) -> str:
    """
    Map an internal reasoning_model to a UI concept string.
    Used to populate response metadata so the frontend can display
    "Caelyn" or "Customize" without hard-coding model IDs.
    """
    rm = normalize_reasoning_model(reasoning_model)
    if rm == MODEL_AGENT_COLLAB:
        return CONCEPT_CAELYN
    if rm == MODEL_ALL_AGENTS:
        return CONCEPT_CUSTOMIZE
    if rm in _SOLO_MODELS:
        return CONCEPT_SOLO
    return CONCEPT_CAELYN


def mode_display_label(reasoning_model: str | None) -> str:
    """
    Human-readable label for a given reasoning_model.
    Used in response metadata and history records.
    """
    rm = normalize_reasoning_model(reasoning_model)
    _LABELS: dict[str, str] = {
        MODEL_AGENT_COLLAB: "Caelyn",
        MODEL_ALL_AGENTS:   "Full Collaboration",
        "claude":           "Claude",
        "gpt-4o":           "ChatGPT",
        "grok":             "Grok",
        "gemini":           "Gemini",
        "perplexity":       "Perplexity",
    }
    return _LABELS.get(rm, "Caelyn")
