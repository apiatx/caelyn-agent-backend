"""
Phase 0 — Mode name normalization layer.

Maps frontend display labels ("caelyn", "customize", legacy strings) to the
internal reasoning_model identifiers the backend pipeline uses.

INTERNAL IDENTIFIERS (never change these — business logic depends on them):
  "agent_collab"  — Caelyn automatic smart mode (data pipeline + chosen collab mix).
  "all_agents"    — Full fan-out (every agent runs independently, then synthesizes).
  "claude"        — Solo Claude.
  "gpt-4o"        — Solo GPT-4o.
  "grok"          — Solo Grok.
  "gemini"        — Solo Gemini.
  "perplexity"    — Solo Perplexity.
  "deepseek"      — Solo DeepSeek.

UI CONCEPTS (frontend display only):
  "caelyn"        — Maps to agent_collab (automatic smart mode).
  "customize"     — No single internal model; used to signal the user is in the
                    Customize panel. The actual reasoning_model comes from the
                    frontend preset/collab selection, defaulting to agent_collab.

COLLAB PRESET IDENTIFIERS (new — added for Default vs Auto distinction):
  "default"  — Fixed Default preset: Claude primary + Grok + Gemini collaborators.
               Provider families are locked. Backend may optimize model tier
               within each family but MUST NOT switch families.
  "auto"     — Dynamic Auto preset: backend/router chooses collaborators freely
               based on prompt and category. Only preset that allows family
               auto-routing.
  "full"     — Full Collaboration: all agents run, user picks synthesis model.
               Provider families locked.
  "custom"   — Custom Collaboration: user explicitly picked collaborators.
               Provider families locked to user's exact choices.
  None/""    — Legacy / solo mode: inferred from reasoning_model + collab_agents.
"""

# ── Public concept names ─────────────────────────────────────
CONCEPT_CAELYN    = "caelyn"
CONCEPT_CUSTOMIZE = "customize"
CONCEPT_SOLO      = "solo"

# ── Internal reasoning_model identifiers ─────────────────────
MODEL_AGENT_COLLAB = "agent_collab"
MODEL_ALL_AGENTS   = "all_agents"
_SOLO_MODELS       = {"claude", "gpt-4o", "grok", "gemini", "perplexity", "deepseek"}
_ALL_VALID_MODELS  = {MODEL_AGENT_COLLAB, MODEL_ALL_AGENTS} | _SOLO_MODELS

# ── Collab preset identifiers ─────────────────────────────────
# These are used as discriminators in the new collab_preset field.
# Values are stable internal identifiers — do not change.
COLLAB_PRESET_DEFAULT = "default"   # fixed: Claude + Grok + Gemini, routing LOCKED
COLLAB_PRESET_AUTO    = "auto"      # dynamic: routing ALLOWED, only preset with auto-family-routing
COLLAB_PRESET_FULL    = "full"      # full fan-out: all agents, routing LOCKED
COLLAB_PRESET_CUSTOM  = "custom"    # user-defined: explicit agents, routing LOCKED

# Fixed collaborator set for Default preset — do not change without product approval.
DEFAULT_PRESET_COLLABORATORS: list[str] = ["grok", "gemini"]
DEFAULT_PRESET_PRIMARY: str = "claude"

_VALID_COLLAB_PRESETS = {COLLAB_PRESET_DEFAULT, COLLAB_PRESET_AUTO, COLLAB_PRESET_FULL, COLLAB_PRESET_CUSTOM}

# Aliases the frontend might send for collab_preset
_COLLAB_PRESET_INBOUND: dict[str, str] = {
    "default":      COLLAB_PRESET_DEFAULT,
    "default_collab": COLLAB_PRESET_DEFAULT,
    "default_preset": COLLAB_PRESET_DEFAULT,
    "fixed":        COLLAB_PRESET_DEFAULT,
    "auto":         COLLAB_PRESET_AUTO,
    "automatic":    COLLAB_PRESET_AUTO,
    "caelyn":       COLLAB_PRESET_AUTO,
    "full":         COLLAB_PRESET_FULL,
    "full_collab":  COLLAB_PRESET_FULL,
    "full_collaboration": COLLAB_PRESET_FULL,
    "all":          COLLAB_PRESET_FULL,
    "custom":       COLLAB_PRESET_CUSTOM,
    "custom_collab": COLLAB_PRESET_CUSTOM,
    "custom_collaboration": COLLAB_PRESET_CUSTOM,
    "customize":    COLLAB_PRESET_CUSTOM,
}

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


def normalize_collab_preset(value: str | None) -> str | None:
    """
    Normalize any inbound collab_preset string to a known internal identifier.

    Returns None for unknown/None inputs so legacy solo-mode inference still works.
    Never raises.

    Values:
      "default" — Fixed Default preset (Claude + Grok + Gemini, routing LOCKED)
      "auto"    — Dynamic Auto preset (routing ALLOWED — only preset with family routing)
      "full"    — Full Collaboration (all agents, routing LOCKED)
      "custom"  — Custom Collaboration (explicit agents, routing LOCKED)
      None      — Unknown / not provided; caller should infer from reasoning_model + collab_agents
    """
    if not value:
        return None
    cleaned = str(value).strip().lower()
    if cleaned in _VALID_COLLAB_PRESETS:
        return cleaned
    mapped = _COLLAB_PRESET_INBOUND.get(cleaned)
    if mapped:
        return mapped
    print(f"[MODE_NORMALIZER] Unknown collab_preset '{value}', treating as None (legacy inference)")
    return None


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


def mode_display_label(reasoning_model: str | None, collab_preset: str | None = None) -> str:
    """
    Human-readable label for a given reasoning_model + optional collab_preset.
    When collab_preset is provided, it takes precedence for Customize-panel presets.
    Used in response metadata and history records.
    """
    if collab_preset:
        _PRESET_LABELS: dict[str, str] = {
            COLLAB_PRESET_DEFAULT: "Default (Claude + Grok + Gemini)",
            COLLAB_PRESET_AUTO:    "Auto",
            COLLAB_PRESET_FULL:    "Full Collaboration",
            COLLAB_PRESET_CUSTOM:  "Custom Collaboration",
        }
        label = _PRESET_LABELS.get(collab_preset)
        if label:
            return label

    rm = normalize_reasoning_model(reasoning_model)
    _LABELS: dict[str, str] = {
        MODEL_AGENT_COLLAB: "Caelyn",
        MODEL_ALL_AGENTS:   "Full Collaboration",
        "claude":           "Claude",
        "gpt-4o":           "ChatGPT",
        "grok":             "Grok",
        "gemini":           "Gemini",
        "perplexity":       "Perplexity",
        "deepseek":         "DeepSeek",
    }
    return _LABELS.get(rm, "Caelyn")


def collab_preset_display_label(collab_preset: str | None) -> str:
    """
    Human-readable label for a collab_preset identifier.
    Used for structured logging to make Default vs Auto unmistakable.
    """
    _LABELS: dict[str, str] = {
        COLLAB_PRESET_DEFAULT: "Default[FIXED: Claude+Grok+Gemini]",
        COLLAB_PRESET_AUTO:    "Auto[DYNAMIC: routing_allowed]",
        COLLAB_PRESET_FULL:    "Full[FIXED: all_agents]",
        COLLAB_PRESET_CUSTOM:  "Custom[FIXED: user_defined]",
    }
    return _LABELS.get(collab_preset or "", f"legacy_inferred[{collab_preset!r}]")
