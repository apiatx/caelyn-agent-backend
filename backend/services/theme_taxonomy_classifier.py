"""
Theme Taxonomy v2 — AI Classification & Dry-Run Engine
=======================================================

Produces AI-assisted classification proposals mapping each ticker to:
  - proposed_primary_theme_id   (one canonical assignable ID or null)
  - proposed_additional_theme_ids (zero or more canonical IDs)

Design principles:
  - DRY RUN ONLY — this module never writes to Neon.
  - Reads from existing cached fundamentals; no new provider calls per ticker.
  - Authorized providers: SOL56 (SOL56_MODEL_ID env var) or OpenAI with an
    explicit THEME_CLASSIFIER_MODEL env var. No auto-detection of Anthropic or
    Gemini; if neither authorized provider is configured the run stops with a
    config report and makes zero model calls.
  - Input completeness gate: tickers whose company name or business description
    is missing are quarantined as INPUT_INCOMPLETE before any model call.
  - Identity guard: proposals whose rationale exhibits identity confusion (e.g.
    "or similar", ticker-only guess with no evidence) are quarantined.
  - Manual overrides are never overwritten — existing theme_ticker_overrides
    rows with source "manual_admin" are marked manual_override_protected=True.
  - Checkpointed, resumable, idempotent: skip tickers already in proposal file.
  - Structured output only; invalid model responses are quarantined.
  - Max 3 additional memberships before a warning + requires_manual_review=True.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
TAXONOMY_VERSION       = "v2.0"
PROMPT_VERSION         = "v2.0"
MAX_ADDITIONAL         = 3       # additional memberships beyond primary
BATCH_SIZE             = 10      # tickers per LLM call
MAX_RETRIES            = 2       # per batch
DRY_RUN_APPLY          = False   # never write to DB in this module

_PROPOSALS_JSON = Path("data/theme-taxonomy-v2-proposals.json")
_PROPOSALS_CSV  = Path("data/theme-taxonomy-v2-proposals.csv")
_CHECKPOINT_KEY = "taxonomy_classifier_checkpoint"

_CSV_FIELDS = [
    "ticker", "company", "actual_sector", "current_primary", "current_additional",
    "proposed_primary", "proposed_additional", "commodity_exposures",
    "confidence", "rationale", "evidence", "warnings",
    "manual_override_protected", "requires_manual_review", "change_type",
    "taxonomy_version", "prompt_version", "run_timestamp", "model_id",
]

# ── Provider configuration ─────────────────────────────────────────────────────
# Authorized providers: SOL56 or explicit OpenAI only.
# Anthropic and Gemini are NOT authorized for this classifier regardless of
# which API keys are present in the environment.
_SOL56_MODEL_ID = os.getenv("SOL56_MODEL_ID") or os.getenv("SOL_MODEL_ID")
_SOL56_MISSING  = _SOL56_MODEL_ID is None
SOL56_FINDING   = (
    "SOL 5.6 model alias not found. Searched env vars: SOL56_MODEL_ID, SOL_MODEL_ID. "
    "OpenAI is also authorized when both OPENAI_API_KEY and THEME_CLASSIFIER_MODEL are set. "
    "Anthropic and Gemini are NOT authorized fallbacks for this classifier."
)

# Identity-guard red-flag phrases in model rationales
_IDENTITY_RED_FLAGS: list[str] = [
    "or similar",
    "or equivalent",
    "ticker suggests",
    "name suggests",
    "symbol suggests",
    "appears to be a",
    "likely a ",
    "guessing from",
]


def _get_assignable_registry() -> dict:
    """Return only assignable canonical theme/sub_theme nodes."""
    from services.theme_rs_universe import THEME_RS_UNIVERSE
    return {
        k: v for k, v in THEME_RS_UNIVERSE.items()
        if v.get("assignable", True)
        and v.get("classification") in ("theme", "sub_theme")
    }


def _get_current_assignments() -> dict[str, dict]:
    """
    Return {ticker: {primary_theme_id, additional_theme_ids, source, manual}}
    from current theme_ticker_overrides + category_overrides.
    """
    result: dict[str, dict] = {}
    try:
        from data.pg_storage import get_theme_ticker_overrides
        rows = get_theme_ticker_overrides() or []
        for r in rows:
            sym = r.get("symbol", "").upper()
            if not sym:
                continue
            if sym not in result:
                result[sym] = {
                    "primary_theme_id":     None,
                    "additional_theme_ids": [],
                    "manual":               False,
                    "source":               r.get("source", ""),
                }
            src = r.get("source", "")
            if src == "manual_admin":
                result[sym]["manual"] = True
            tid = r.get("theme_id", "")
            if r.get("action") == "add" and tid:
                if result[sym]["primary_theme_id"] is None:
                    result[sym]["primary_theme_id"] = tid
                elif tid not in result[sym]["additional_theme_ids"]:
                    result[sym]["additional_theme_ids"].append(tid)
    except Exception as exc:
        log.warning("_get_current_assignments: %s", exc)
    return result


def _get_fundamentals_cache() -> dict[str, dict]:
    """Load bulk fundamentals from disk cache (no new provider calls)."""
    cache_path = Path("data/watchlist_fundamentals_store.json")
    if not cache_path.exists():
        return {}
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return raw
    except Exception as exc:
        log.warning("_get_fundamentals_cache: %s", exc)
    return {}


def _get_llm_overrides() -> dict[str, dict]:
    """Load LLM theme override file (existing cached AI classifications)."""
    override_path = Path("data/llm_theme_overrides.json")
    if not override_path.exists():
        return {}
    try:
        return json.loads(override_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _detect_provider() -> tuple[str, str]:
    """
    Detect authorized LLM provider for taxonomy classification.
    Returns (provider_name, model_id).

    ONLY authorized providers:
      1. SOL56: SOL56_MODEL_ID (or SOL_MODEL_ID) env var must be set.
      2. OpenAI: OPENAI_API_KEY **and** THEME_CLASSIFIER_MODEL must both be set.
         Key alone is insufficient — an explicit model opt-in is required.

    Anthropic and Gemini are NOT authorized, regardless of which API keys are
    present. If neither authorized provider is configured, returns ("none", "none")
    and the caller must stop with a config report — zero model calls must be made.
    """
    if _SOL56_MODEL_ID:
        return "sol56", _SOL56_MODEL_ID

    if os.getenv("OPENAI_API_KEY") and os.getenv("THEME_CLASSIFIER_MODEL"):
        return "openai", os.getenv("THEME_CLASSIFIER_MODEL")

    return "none", "none"


def _build_taxonomy_prompt(registry: dict) -> str:
    """Build the deterministic taxonomy description for the classification prompt."""
    lines = [
        f"# Canonical Theme Taxonomy {TAXONOMY_VERSION}",
        "",
        "## Parent Themes (classification=theme)",
    ]
    parents = {k: v for k, v in registry.items() if v.get("classification") == "theme"}
    for pid, meta in sorted(parents.items()):
        lines.append(f"- {pid}: {meta.get('display_name','')}")

    lines.append("")
    lines.append("## Subthemes (classification=sub_theme, parent_theme_id=...)")
    subs = {k: v for k, v in registry.items() if v.get("classification") == "sub_theme"}
    for sid, meta in sorted(subs.items()):
        parent = meta.get("parent_theme_id", "?")
        lines.append(f"- {sid} (under {parent}): {meta.get('display_name','')} — {meta.get('description','')[:100]}")

    return "\n".join(lines)


def _taxonomy_prompt_hash(registry: dict) -> str:
    content = _build_taxonomy_prompt(registry)
    return hashlib.sha1(content.encode()).hexdigest()[:8]


def _build_batch_prompt(tickers_data: list[dict], registry: dict) -> str:
    """Build the full classification prompt for a batch of tickers."""
    taxonomy_desc = _build_taxonomy_prompt(registry)
    ticker_blocks = []
    for td in tickers_data:
        ticker_blocks.append(
            f"TICKER: {td['ticker']}\n"
            f"Company: {td.get('company','?')}\n"
            f"Sector: {td.get('sector','?')}\n"
            f"Industry: {td.get('industry','?')}\n"
            f"Description: {td.get('description','?')[:400]}\n"
            f"Current Primary Theme: {td.get('current_primary','?')}\n"
            f"Current Additional Themes: {td.get('current_additional','?')}"
        )

    return f"""You are a financial analyst performing thematic stock classification.

{taxonomy_desc}

## Classification Rules
- Only use canonical IDs from the taxonomy above.
- primary_theme_id = the company's defining business/value-chain identity.
  A subtheme (e.g. dc_connectivity_silicon under semiconductors) is preferred when it fits.
- additional_theme_ids = meaningful secondary exposures (max 3; more = requires_manual_review=true).
- No sectors in theme fields. No deprecated IDs. No market-lens IDs (gold, silver, copper_miners).
- If primary exposure is unclear, set confidence < 0.6 and requires_manual_review=true.
- Do not change actual_sector_id.
- Do not include redundant parent when a child is assigned (e.g. don't include "semiconductors"
  if "dc_connectivity_silicon" is already primary — it rolls up automatically).

## Tickers to Classify
{chr(10).join(ticker_blocks)}

## Output
Return ONLY a JSON array — one object per ticker:
[
  {{
    "ticker": "ALAB",
    "actual_sector_id": "technology",
    "proposed_primary_theme_id": "dc_connectivity_silicon",
    "proposed_additional_theme_ids": ["networking_fabric_infra"],
    "commodity_exposures": [],
    "confidence": 0.97,
    "rationale": "Primary business is PCIe/CXL retimer and memory-connectivity silicon for AI data centers.",
    "evidence": ["CXL connectivity products", "PCIe retimers for HPC/AI"],
    "warnings": [],
    "requires_manual_review": false
  }}
]"""


def _call_llm(prompt: str, provider: str, model_id: str) -> list[dict]:
    """Call LLM and return parsed list of classification proposals."""
    if provider == "none":
        raise RuntimeError("No LLM provider configured.")

    raw_text = ""
    if provider == "anthropic":
        try:
            import anthropic as _anth
            client = _anth.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            msg = client.messages.create(
                model=model_id,
                max_tokens=4096,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = msg.content[0].text if msg.content else ""
        except Exception as exc:
            raise RuntimeError(f"Anthropic call failed: {exc}") from exc

    elif provider == "gemini":
        try:
            import google.generativeai as genai
            genai.configure(api_key=os.environ["GEMINI_API_KEY"])
            model = genai.GenerativeModel(model_id)
            resp = model.generate_content(
                prompt,
                generation_config={"temperature": 0.1, "max_output_tokens": 4096},
            )
            raw_text = resp.text or ""
        except Exception as exc:
            raise RuntimeError(f"Gemini call failed: {exc}") from exc

    elif provider == "gemini_rest":
        try:
            import httpx
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_id}:generateContent?key={os.environ['GEMINI_API_KEY']}"
            )
            payload = {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 4096},
            }
            r = httpx.post(url, json=payload, timeout=90)
            r.raise_for_status()
            data = r.json()
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as exc:
            raise RuntimeError(f"Gemini REST call failed: {exc}") from exc

    elif provider in ("openai", "sol56"):
        try:
            import openai as _oai
            base_url = os.getenv("SOL56_BASE_URL") if provider == "sol56" else None
            api_key  = os.getenv("SOL56_API_KEY") if provider == "sol56" else os.environ.get("OPENAI_API_KEY", "")
            client   = _oai.OpenAI(api_key=api_key, **({"base_url": base_url} if base_url else {}))
            resp     = client.chat.completions.create(
                model=model_id,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=4096,
            )
            raw_text = resp.choices[0].message.content or ""
        except Exception as exc:
            raise RuntimeError(f"OpenAI-compatible call failed: {exc}") from exc

    # Parse JSON from response
    # Strip markdown fences if present
    raw_text = raw_text.strip()
    if raw_text.startswith("```"):
        raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
        raw_text = re.sub(r"\n?```$", "", raw_text)

    try:
        return json.loads(raw_text)
    except Exception:
        # Try to find JSON array within response
        match = re.search(r"\[[\s\S]*\]", raw_text)
        if match:
            return json.loads(match.group(0))
        raise ValueError(f"Could not parse JSON from model response: {raw_text[:300]}")


def _validate_proposal(
    proposal: dict,
    registry: dict,
    current_assignments: dict[str, dict],
    company_name: str = "",
) -> tuple[dict, list[str]]:
    """
    Validate and normalize one AI proposal. Returns (normalized_proposal, quarantine_reasons).
    quarantine_reasons is empty if the proposal is valid.

    company_name (optional): expected company name for this ticker, used by the
    identity guard. When supplied, the guard rejects proposals whose rationale
    shows signs of identity confusion (red-flag phrases, no company reference,
    no supporting evidence). When omitted, the identity guard is skipped.
    """
    quarantine: list[str] = []
    ticker = str(proposal.get("ticker", "")).upper()
    if not ticker:
        quarantine.append("missing ticker")
        return proposal, quarantine

    # ── Identity guard ────────────────────────────────────────────────────────
    if company_name:
        rationale_lower = str(proposal.get("rationale") or "").lower()
        evidence = list(proposal.get("evidence") or [])

        # Red-flag phrases that indicate the model is guessing rather than classifying
        for flag in _IDENTITY_RED_FLAGS:
            if flag in rationale_lower:
                quarantine.append(
                    f"IDENTITY_GUARD: rationale contains suspect phrase {flag!r} "
                    f"suggesting identity confusion (ticker={ticker}, expected company={company_name!r})"
                )
                break

        # If no red-flag phrase, check whether the rationale references the expected
        # company and whether there is any supporting evidence. Quarantine if the model
        # neither named the company nor provided any evidence (ticker-only guess).
        if not quarantine:
            company_words = [w.lower() for w in company_name.split() if len(w) > 3]
            rationale_mentions_company = any(w in rationale_lower for w in company_words)
            if not rationale_mentions_company and not evidence:
                quarantine.append(
                    f"IDENTITY_GUARD: rationale does not reference company {company_name!r} "
                    f"and no evidence was provided — possible ticker-only guess "
                    f"(ticker={ticker})"
                )

    assignable_ids = set(registry.keys())

    def _check_id(tid: str, field: str) -> Optional[str]:
        if not tid:
            return None
        if tid not in assignable_ids:
            quarantine.append(f"{field}={tid!r} not in assignable registry")
            return None
        return tid

    primary = _check_id(proposal.get("proposed_primary_theme_id") or "", "proposed_primary_theme_id")
    raw_additional = proposal.get("proposed_additional_theme_ids") or []
    additional: list[str] = []
    for t in raw_additional:
        valid_t = _check_id(t, f"proposed_additional_theme_ids[{t!r}]")
        if valid_t and valid_t != primary:
            additional.append(valid_t)

    additional = sorted(set(additional))

    warnings = list(proposal.get("warnings") or [])
    manual_review = bool(proposal.get("requires_manual_review", False))

    if len(additional) > MAX_ADDITIONAL:
        warnings.append(f"More than {MAX_ADDITIONAL} additional memberships proposed ({len(additional)}); manual review required.")
        manual_review = True

    # Check manual override protection
    current = current_assignments.get(ticker, {})
    manual_protected = current.get("manual", False)
    if manual_protected:
        warnings.append("Existing manual override detected — proposal is informational only.")
        manual_review = True

    # Determine change_type
    current_primary     = current.get("primary_theme_id")
    current_additional  = sorted(current.get("additional_theme_ids") or [])

    if not primary and not additional:
        change_type = "MANUAL_REVIEW" if manual_review else "UNCHANGED"
    elif primary == current_primary and sorted(additional) == current_additional:
        change_type = "UNCHANGED"
    elif primary != current_primary and current_primary is None:
        change_type = "NEW_PRIMARY"
    elif primary != current_primary:
        change_type = "ONE_TO_ONE_MIGRATION"
    elif sorted(additional) != current_additional:
        change_type = "NEW_ADDITIONAL"
    else:
        change_type = "UNCHANGED"

    # Flag if primary represents an ambiguous split (old node being retired)
    from services.theme_rs_universe import THEME_RS_UNIVERSE
    if current_primary and THEME_RS_UNIVERSE.get(current_primary, {}).get("classification") == "deprecated":
        change_type = "AMBIGUOUS_SPLIT"
        manual_review = True

    normalized = {
        "ticker":                    ticker,
        "actual_sector_id":          proposal.get("actual_sector_id", ""),
        "current_primary_theme_id":  current_primary,
        "current_additional_theme_ids": current_additional,
        "proposed_primary_theme_id":    primary,
        "proposed_additional_theme_ids": additional,
        "commodity_exposures":       proposal.get("commodity_exposures") or [],
        "confidence":                float(proposal.get("confidence") or 0.0),
        "rationale":                 str(proposal.get("rationale") or ""),
        "evidence":                  list(proposal.get("evidence") or []),
        "warnings":                  warnings,
        "manual_override_protected": manual_protected,
        "requires_manual_review":    manual_review,
        "change_type":               change_type,
    }
    return normalized, quarantine


def run_sample(
    tickers: Optional[list[str]] = None,
    *,
    write_artifacts: bool = True,
) -> dict:
    """
    Run a dry-run classification on the representative sample (or supplied tickers).
    Returns {proposals, quarantined, summary, sol56_finding}.
    Never writes to Neon.
    """
    if tickers is None:
        tickers = [
            "ALAB", "CRDO", "AEHR", "TER", "AMKR", "POWI",
            "BE", "LPTH", "LWLG", "IREN", "NBIS", "VRT",
        ]

    registry   = _get_assignable_registry()
    fund_cache = _get_fundamentals_cache()
    llm_overrides = _get_llm_overrides()
    current_assignments = _get_current_assignments()
    provider, model_id = _detect_provider()
    prompt_hash = _taxonomy_prompt_hash(registry)
    run_ts = datetime.now(timezone.utc).isoformat()

    proposals: list[dict] = []
    quarantined: list[dict] = []

    # ── Config-error gate: no authorized provider → stop immediately ──────────
    if provider == "none":
        config_report = (
            "CONFIG ERROR: No authorized LLM provider found for taxonomy classification.\n"
            "  Authorized providers:\n"
            "    SOL56:  set SOL56_MODEL_ID (and optionally SOL56_BASE_URL + SOL56_API_KEY)\n"
            "    OpenAI: set both OPENAI_API_KEY and THEME_CLASSIFIER_MODEL\n"
            "  NOT authorized: Anthropic, Gemini, or any auto-detected provider.\n"
            "Zero model calls were made."
        )
        log.warning("run_sample: no authorized provider — returning config_error")
        return {
            "proposals":     [],
            "quarantined":   [],
            "provider":      "none",
            "model_id":      "none",
            "config_error":  config_report,
            "sol56_finding": SOL56_FINDING,
            "sol56_missing": _SOL56_MISSING,
            "prompt_hash":   prompt_hash,
            "run_timestamp": run_ts,
            "summary":       _summarize([], []),
        }

    # ── Build ticker input data + input completeness gate ─────────────────────
    tickers_data: list[dict] = []
    for ticker in tickers:
        fund = fund_cache.get(ticker) or fund_cache.get(ticker.upper()) or {}
        fields = fund.get("fields") or {}
        profile = (fields.get("profile") or {})
        desc = (
            profile.get("description")
            or fields.get("description")
            or llm_overrides.get(ticker, {}).get("display_name", "")
            or ""
        )
        company = profile.get("companyName") or fields.get("companyName") or ""

        # Input completeness gate: quarantine before any model call
        if not company or company.strip().upper() == ticker.upper():
            quarantined.append({
                "ticker": ticker,
                "reason": (
                    "INPUT_INCOMPLETE: company name missing or defaults to ticker symbol — "
                    "cannot classify without confirmed company identity"
                ),
            })
            continue
        if not desc or len(desc.strip()) < 20:
            quarantined.append({
                "ticker": ticker,
                "reason": (
                    "INPUT_INCOMPLETE: business description missing or too short — "
                    "cannot classify without a description of core operations"
                ),
            })
            continue

        current_a = current_assignments.get(ticker, {})
        tickers_data.append({
            "ticker":          ticker,
            "company":         company,
            "sector":          (profile.get("sector") or fields.get("sector") or ""),
            "industry":        (profile.get("industry") or fields.get("industry") or ""),
            "description":     desc[:500],
            "current_primary": current_a.get("primary_theme_id") or "",
            "current_additional": current_a.get("additional_theme_ids") or [],
        })

    # Process in batches
    for i in range(0, len(tickers_data), BATCH_SIZE):
        batch = tickers_data[i:i + BATCH_SIZE]
        batch_tickers = [b["ticker"] for b in batch]

        prompt = _build_batch_prompt(batch, registry)
        raw_proposals: list[dict] = []
        last_exc: Optional[Exception] = None

        for attempt in range(1 + MAX_RETRIES):
            try:
                raw_proposals = _call_llm(prompt, provider, model_id)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                log.warning("Batch %s attempt %d failed: %s", batch_tickers, attempt + 1, exc)
                time.sleep(2 ** attempt)

        if last_exc:
            for td in batch:
                quarantined.append({
                    "ticker": td["ticker"],
                    "reason": f"All retries failed: {last_exc}",
                    "batch_tickers": batch_tickers,
                })
            continue

        # Index proposals by ticker
        proposal_index = {p.get("ticker", "").upper(): p for p in raw_proposals if isinstance(p, dict)}

        for td in batch:
            ticker = td["ticker"]
            raw = proposal_index.get(ticker)
            if not raw:
                quarantined.append({"ticker": ticker, "reason": "Model did not return proposal for this ticker."})
                continue

            try:
                # Pass company_name to enable identity guard
                normalized, quarantine_reasons = _validate_proposal(
                    raw, registry, current_assignments,
                    company_name=td.get("company", ""),
                )
                if quarantine_reasons:
                    quarantined.append({
                        "ticker": ticker,
                        "reason": "; ".join(quarantine_reasons),
                        "raw": raw,
                    })
                else:
                    normalized.update({
                        "taxonomy_version": TAXONOMY_VERSION,
                        "prompt_version":   PROMPT_VERSION,
                        "run_timestamp":    run_ts,
                        "model_id":         model_id,
                    })
                    proposals.append(normalized)
            except Exception as exc:
                quarantined.append({"ticker": ticker, "reason": f"Validation error: {exc}", "raw": raw})

    if write_artifacts:
        _write_artifacts(proposals, quarantined, run_ts, model_id, prompt_hash)

    return {
        "proposals":    proposals,
        "quarantined":  quarantined,
        "sol56_finding": SOL56_FINDING,
        "sol56_missing": _SOL56_MISSING,
        "provider":     provider,
        "model_id":     model_id,
        "prompt_hash":  prompt_hash,
        "run_timestamp": run_ts,
        "summary":       _summarize(proposals, quarantined),
    }


def run_full_watchlist_dry_run(*, sample_only: bool = False) -> dict:
    """
    Run classification dry run on every current Primary Watchlist ticker.

    sample_only=True → only the 12-ticker representative sample.
    Never applies proposals to Neon.
    """
    if sample_only:
        return run_sample()

    # Attempt to load watchlist tickers from Neon
    tickers: list[str] = []
    try:
        from data.pg_storage import watchlist_read
        wl = watchlist_read("default")
        if wl:
            tickers = [t for t in (wl.get("tickers") or []) if isinstance(t, str)]
    except Exception as exc:
        log.warning("Could not load watchlist: %s", exc)

    # Fall back to candidate symbols from the registry if watchlist is empty
    if not tickers:
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        seen: set[str] = set()
        for meta in THEME_RS_UNIVERSE.values():
            for sym in (meta.get("candidate_symbols") or []):
                if ":" not in sym and sym not in seen:
                    seen.add(sym)
                    tickers.append(sym)
        tickers = sorted(seen)
        log.info("Watchlist empty; using %d registry candidate symbols for dry run.", len(tickers))

    return run_sample(tickers=tickers)


def _summarize(proposals: list[dict], quarantined: list[dict]) -> dict:
    """Produce summary counts over proposal list."""
    total = len(proposals) + len(quarantined)
    unchanged = sum(1 for p in proposals if p.get("change_type") == "UNCHANGED")
    changed   = len(proposals) - unchanged
    high_conf = sum(1 for p in proposals if p.get("confidence", 0) >= 0.85)
    med_conf  = sum(1 for p in proposals if 0.6 <= p.get("confidence", 0) < 0.85)
    low_conf  = sum(1 for p in proposals if p.get("confidence", 0) < 0.6)
    protected = sum(1 for p in proposals if p.get("manual_override_protected"))
    review    = sum(1 for p in proposals if p.get("requires_manual_review"))
    multi     = sum(1 for p in proposals if (p.get("proposed_additional_theme_ids") or []))
    unassigned = sum(1 for p in proposals if not p.get("proposed_primary_theme_id"))

    by_primary: dict[str, int] = {}
    for p in proposals:
        pid = p.get("proposed_primary_theme_id") or "_unassigned"
        by_primary[pid] = by_primary.get(pid, 0) + 1

    change_types: dict[str, int] = {}
    for p in proposals:
        ct = p.get("change_type", "UNKNOWN")
        change_types[ct] = change_types.get(ct, 0) + 1

    return {
        "total_tickers":           total,
        "valid_proposals":         len(proposals),
        "quarantined":             len(quarantined),
        "unchanged":               unchanged,
        "changed":                 changed,
        "high_confidence":         high_conf,
        "medium_confidence":       med_conf,
        "low_confidence":          low_conf,
        "protected_manual_overrides": protected,
        "requires_manual_review":  review,
        "proposed_multi_membership": multi,
        "unassigned":              unassigned,
        "proposals_by_primary_theme": dict(sorted(by_primary.items())),
        "proposals_by_change_type":  change_types,
    }


def _write_artifacts(
    proposals: list[dict],
    quarantined: list[dict],
    run_ts: str,
    model_id: str,
    prompt_hash: str,
) -> None:
    """Write JSON and CSV proposal artifacts (not staged, not committed)."""
    artifact = {
        "taxonomy_version": TAXONOMY_VERSION,
        "prompt_version":   PROMPT_VERSION,
        "prompt_hash":      prompt_hash,
        "model_id":         model_id,
        "run_timestamp":    run_ts,
        "sol56_finding":    SOL56_FINDING,
        "summary":          _summarize(proposals, quarantined),
        "proposals":        proposals,
        "quarantined":      quarantined,
        "dry_run":          True,
        "applied_to_db":    False,
    }

    _PROPOSALS_JSON.parent.mkdir(parents=True, exist_ok=True)
    _PROPOSALS_JSON.write_text(json.dumps(artifact, indent=2, default=str), encoding="utf-8")
    log.info("Wrote proposals JSON: %s (%d proposals)", _PROPOSALS_JSON, len(proposals))

    with _PROPOSALS_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for p in proposals:
            row = {k: p.get(k, "") for k in _CSV_FIELDS}
            row["current_additional"]    = "|".join(p.get("current_additional_theme_ids") or [])
            row["proposed_additional"]   = "|".join(p.get("proposed_additional_theme_ids") or [])
            row["commodity_exposures"]   = "|".join(p.get("commodity_exposures") or [])
            row["evidence"]              = "|".join(p.get("evidence") or [])
            row["warnings"]              = "|".join(p.get("warnings") or [])
            row["current_primary"]       = p.get("current_primary_theme_id") or ""
            row["proposed_primary"]      = p.get("proposed_primary_theme_id") or ""
            writer.writerow(row)
    log.info("Wrote proposals CSV: %s", _PROPOSALS_CSV)
