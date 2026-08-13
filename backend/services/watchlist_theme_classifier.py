"""
watchlist_theme_classifier.py — Background batch LLM theme classifier.

Classifies watchlist symbols that have no canonical theme mapping into the
THEME_RS_UNIVERSE taxonomy using a cheap/fast configurable LLM provider.

Provider config (env vars):
  THEME_CLASSIFIER_PROVIDER=gemini  (default if GEMINI_API_KEY set)
  THEME_CLASSIFIER_PROVIDER=openai  (fallback if OPENAI_API_KEY set)
  THEME_CLASSIFIER_MODEL=<model>    (optional override)
  Default models: gemini-2.0-flash-lite, gpt-4o-mini

Rules:
  - Never called on page render.
  - Never called on normal GET.
  - Only classifies symbols with no existing mapping.
  - Capped batch size per run.
  - Job-level lock — one run at a time.
  - Saves results via register_llm_classified_tickers() (disk + in-memory).
  - Marks permanently unresolvable symbols in theme_needs_review.json.
"""
from __future__ import annotations

import asyncio
import json as _json
import os
import time
from pathlib import Path
from typing import Any

# ── Storage ───────────────────────────────────────────────────────────────────

_NEEDS_REVIEW_PATH = Path(__file__).parent.parent / "data" / "theme_needs_review.json"

# ── Config ────────────────────────────────────────────────────────────────────

_BATCH_CAP        = int(os.getenv("THEME_CLASSIFIER_BATCH_CAP", "40"))
_PROVIDER_ENV     = "THEME_CLASSIFIER_PROVIDER"
_MODEL_ENV        = "THEME_CLASSIFIER_MODEL"

_DEFAULT_MODELS = {
    "gemini":   "gemini-2.0-flash-lite",
    "openai":   "gpt-4o-mini",
    "deepseek": "deepseek-v4-flash",
}

_DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# Per-ticker singleton job tracking: set of tickers currently being classified.
# Prevents duplicate classification jobs for the same ticker.
_INFLIGHT_TICKERS: set[str] = set()
_INFLIGHT_LOCK = asyncio.Lock()

# ── Job state ─────────────────────────────────────────────────────────────────

_CLASSIFIER_STATE: dict[str, Any] = {
    "running":                 False,
    "watchlist_id":            None,
    "started_at":              None,
    "updated_at":              None,
    "total_symbols":           0,
    "mapped_existing":         0,
    "mapped_from_fmp_industry":0,
    "mapped_from_llm":         0,
    "needs_theme":             0,
    "queued":                  0,
    "processed":               0,
    "failed":                  0,
    "current_batch":           [],
    "failures":                [],
    "last_provider":           None,
    "last_model":              None,
}

_CLASSIFIER_LOCK: asyncio.Lock | None = None


def _lock() -> asyncio.Lock:
    global _CLASSIFIER_LOCK
    if _CLASSIFIER_LOCK is None:
        _CLASSIFIER_LOCK = asyncio.Lock()
    return _CLASSIFIER_LOCK


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _upd(**kwargs: Any) -> None:
    _CLASSIFIER_STATE.update(kwargs)
    _CLASSIFIER_STATE["updated_at"] = _ts()


# ── Needs-review helpers ───────────────────────────────────────────────────────

def _load_needs_review() -> dict[str, str]:
    try:
        if _NEEDS_REVIEW_PATH.exists():
            return _json.loads(_NEEDS_REVIEW_PATH.read_text()) or {}
    except Exception:
        pass
    return {}


def _save_needs_review(data: dict[str, str]) -> None:
    try:
        _NEEDS_REVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
        _NEEDS_REVIEW_PATH.write_text(_json.dumps(data, indent=2))
    except Exception as exc:
        print(f"[THEME_CLASSIFIER] needs_review save error: {exc}")


def mark_needs_review(symbols: list[str], reason: str = "llm_failed") -> None:
    data = _load_needs_review()
    for sym in symbols:
        data[sym] = reason
    _save_needs_review(data)
    print(f"[THEME_CLASSIFIER] marked {len(symbols)} symbols needs_review ({reason})")


def get_needs_review() -> dict[str, str]:
    return _load_needs_review()


# ── LLM provider ──────────────────────────────────────────────────────────────

def _detect_provider() -> tuple[str, str]:
    """Return (provider_name, model_name) based on env config and available keys."""
    provider = os.getenv(_PROVIDER_ENV, "").lower().strip()
    model    = os.getenv(_MODEL_ENV, "").strip()

    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
    gemini_key   = os.getenv("GEMINI_API_KEY", "")
    openai_key   = os.getenv("OPENAI_API_KEY", "")

    # DeepSeek V4 Flash — authorised for automatic Watchlist theme assignment
    if provider in ("deepseek", "") and deepseek_key:
        return "deepseek", model or _DEFAULT_MODELS["deepseek"]
    if provider in ("gemini", "") and gemini_key:
        return "gemini", model or _DEFAULT_MODELS["gemini"]
    if provider in ("openai", "") and openai_key:
        return "openai", model or _DEFAULT_MODELS["openai"]
    return "none", ""


async def _call_llm(ticker_lines: list[str], theme_list: list[str]) -> list[dict]:
    """
    Send ticker_lines to the configured LLM and return parsed classifications.
    Returns [] if LLM is unavailable or parsing fails.
    """
    provider, model_name = _detect_provider()
    _upd(last_provider=provider, last_model=model_name)

    if provider == "none":
        print("[THEME_CLASSIFIER] No LLM provider configured — skipping LLM classification.")
        return []

    themes_str = "\n".join(f"- {t}" for t in theme_list)
    tickers_str = "\n".join(ticker_lines)

    prompt = (
        "You are an expert investment theme classifier.\n"
        "Assign each ticker to the MOST APPROPRIATE theme from the list below.\n\n"
        f"AVAILABLE THEMES:\n{themes_str}\n\n"
        f"TICKERS TO CLASSIFY:\n{tickers_str}\n\n"
        "Rules:\n"
        "1. Assign each ticker to exactly ONE theme from the list above.\n"
        "2. Choose the most specific match based on primary business.\n"
        "3. confidence: 'high' (>80%), 'medium' (50-80%), 'low' (<50% — still pick a theme).\n"
        "4. Return ONLY valid JSON — no markdown fences, no explanation.\n\n"
        'Return exactly: {"classifications": ['
        '{"ticker": "SYM", "theme": "Exact Theme Name", "confidence": "high", "reasoning": "one sentence"}'
        "]}"
    )

    raw_text = ""
    try:
        if provider == "gemini":
            import google.generativeai as genai
            gemini_key = os.getenv("GEMINI_API_KEY", "")
            genai.configure(api_key=gemini_key)
            mdl = genai.GenerativeModel(model_name)
            resp = await asyncio.to_thread(mdl.generate_content, prompt)
            raw_text = resp.text or ""

        elif provider == "openai":
            import openai as _oai
            openai_key = os.getenv("OPENAI_API_KEY", "")
            client = _oai.OpenAI(api_key=openai_key)
            resp = await asyncio.to_thread(
                client.chat.completions.create,
                model=model_name,
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = resp.choices[0].message.content or ""

    except Exception as exc:
        print(f"[THEME_CLASSIFIER] LLM call error ({provider}/{model_name}): {exc}")
        return []

    # Parse response
    try:
        clean = raw_text.strip()
        if clean.startswith("```"):
            parts = clean.split("```")
            clean = parts[1] if len(parts) > 1 else clean
            if clean.lower().startswith("json"):
                clean = clean[4:]
            clean = clean.strip()
        parsed = _json.loads(clean)
        return parsed.get("classifications") or []
    except Exception as exc:
        print(f"[THEME_CLASSIFIER] JSON parse error: {exc} | raw={raw_text[:200]}")
        return []


# ── Main classifier ───────────────────────────────────────────────────────────

async def classify_watchlist_themes(
    watchlist_id: str | None = None,
    missing_only: bool = True,
    cap: int | None = None,
) -> dict:
    """
    Background job: classify unmapped watchlist symbols into canonical themes.

    Skips symbols already in canonical_map, industry_fallback, or LLM overrides.
    Returns the final _CLASSIFIER_STATE snapshot.
    """
    if not _lock().locked():
        asyncio.ensure_future(_run_classifier(watchlist_id, missing_only, cap))
    else:
        print("[THEME_CLASSIFIER] Already running — ignoring duplicate trigger.")
    return get_classifier_status()


async def _run_classifier(
    watchlist_id: str | None,
    missing_only: bool,
    cap: int | None,
) -> None:
    async with _lock():
        _CLASSIFIER_STATE.update({
            "running":                  True,
            "watchlist_id":             watchlist_id,
            "started_at":               _ts(),
            "updated_at":               _ts(),
            "total_symbols":            0,
            "mapped_existing":          0,
            "mapped_from_fmp_industry": 0,
            "mapped_from_llm":          0,
            "needs_theme":              0,
            "queued":                   0,
            "processed":                0,
            "failed":                   0,
            "current_batch":            [],
            "failures":                 [],
        })

        try:
            await _do_classify(watchlist_id, missing_only, cap)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            _upd(failures=_CLASSIFIER_STATE["failures"] + [f"top-level error: {exc}"])
        finally:
            _upd(running=False)
            print(f"[THEME_CLASSIFIER] done: {_CLASSIFIER_STATE}")


async def _do_classify(
    watchlist_id: str | None,
    missing_only: bool,
    cap: int | None,
) -> None:
    from services.watchlist_service import load_watchlist
    from services.theme_ticker_mapper import (
        map_ticker_to_primary_theme,
        map_industry_to_theme,
        get_all_theme_names,
        register_llm_classified_tickers,
    )

    # ── 1. Load watchlist tickers ─────────────────────────────────────────────
    store = load_watchlist(watchlist_id)
    if store is None:
        print(f"[THEME_CLASSIFIER] watchlist not found: {watchlist_id}")
        return

    csv_data: list[dict] = store.get("csv_data", [])
    tickers: list[str]   = store.get("tickers", []) or []

    # Build symbol → CSV row mapping for industry fallback
    csv_map: dict[str, dict] = {}
    for row in csv_data:
        sym = (row.get("Symbol") or row.get("symbol") or row.get("Ticker") or "").strip().upper()
        if sym:
            csv_map[sym] = row

    # Exclude foreign-exchange-prefixed symbols
    _FX_PREFIXES = ("AIM:", "STO:", "FRA:", "TYO:", "HKG:", "NSE:", "TSX:", "ASX:",
                    "LON:", "ETR:", "EPA:", "BIT:", "ELI:", "CPH:", "OTC:")
    eligible = [
        t.upper().strip() for t in tickers
        if t.strip() and not any(t.upper().startswith(p) for p in _FX_PREFIXES)
    ]

    _upd(total_symbols=len(eligible))
    print(f"[THEME_CLASSIFIER] eligible={len(eligible)} watchlist_id={watchlist_id}")

    # ── 2. Split: already mapped vs needs LLM ────────────────────────────────
    mapped_existing      = 0
    mapped_fmp_industry  = 0
    to_classify: list[str] = []
    fmp_persist_batch: list[dict] = []  # persist industry-mapped tickers to disk

    for sym in eligible:
        theme = map_ticker_to_primary_theme(sym)
        if theme and theme not in ("Other / Uncategorized", "Other/Uncategorized"):
            mapped_existing += 1
            continue

        # Try CSV/FMP industry fallback first (deterministic, no LLM)
        row     = csv_map.get(sym, {})
        csv_ind = (row.get("Industry") or row.get("industry") or "").strip()
        fmp_ind = csv_ind  # already in csv_map from upload

        if fmp_ind:
            ind_result = map_industry_to_theme(fmp_ind)
            if ind_result:
                _ind_display, _ind_theme_id = ind_result
                mapped_fmp_industry += 1
                # Persist so provenance/status can track this mapping
                fmp_persist_batch.append({
                    "ticker":     sym,
                    "theme":      _ind_display,
                    "confidence": "fmp_industry",
                })
                continue

        if not missing_only:
            to_classify.append(sym)
        else:
            to_classify.append(sym)

    # Persist FMP-industry mappings in one batch (disk + in-memory mapper update)
    if fmp_persist_batch:
        try:
            n_fmp = register_llm_classified_tickers(fmp_persist_batch)
            print(f"[THEME_CLASSIFIER] fmp_industry: persisted {n_fmp}/{len(fmp_persist_batch)} to mapper")
        except Exception as _fmp_err:
            print(f"[THEME_CLASSIFIER] fmp_industry persist error (non-fatal): {_fmp_err}")

    _upd(
        mapped_existing=mapped_existing,
        mapped_from_fmp_industry=mapped_fmp_industry,
        queued=len(to_classify),
    )
    print(f"[THEME_CLASSIFIER] already_mapped={mapped_existing} fmp_industry={mapped_fmp_industry} to_classify={len(to_classify)}")

    if not to_classify:
        _upd(needs_theme=0)
        return

    # ── 3. Apply batch cap ────────────────────────────────────────────────────
    effective_cap = cap if cap is not None else _BATCH_CAP
    batch = to_classify[:effective_cap]
    _upd(queued=len(batch))

    # ── 4. Gather FMP profiles from cache (no new FMP calls) ─────────────────
    profiles: dict[str, dict] = {}
    try:
        from services.fmp_cache_service import get_company_profiles_bulk_cached
        profiles = get_company_profiles_bulk_cached(batch) or {}
    except Exception as exc:
        print(f"[THEME_CLASSIFIER] profile cache error (non-fatal): {exc}")

    # Build ticker description lines
    ticker_lines: list[str] = []
    for sym in batch:
        row  = csv_map.get(sym, {})
        prof = profiles.get(sym) or {}
        name     = prof.get("companyName") or prof.get("name") or row.get("Name") or sym
        sector   = prof.get("sector") or row.get("Sector") or ""
        industry = prof.get("industry") or row.get("Industry") or ""
        desc     = (prof.get("description") or "")[:120]
        line     = f"- {sym}: {name}"
        if sector:
            line += f" | Sector: {sector}"
        if industry:
            line += f" | Industry: {industry}"
        if desc:
            line += f" | {desc}"
        ticker_lines.append(line)

    # ── 5. Call LLM ───────────────────────────────────────────────────────────
    all_themes = sorted(get_all_theme_names())
    _upd(current_batch=batch)

    raw_classifications = await _call_llm(ticker_lines, all_themes)

    # ── 6. Validate and normalise ─────────────────────────────────────────────
    theme_lower_map = {t.lower(): t for t in all_themes}
    valid_classifications: list[dict] = []
    classified_syms: set[str] = set()

    for item in raw_classifications:
        sym       = (item.get("ticker") or "").upper().strip()
        theme_raw = (item.get("theme") or "").strip()
        conf      = item.get("confidence") or "medium"
        reason    = item.get("reasoning") or ""

        exact = theme_lower_map.get(theme_raw.lower(), theme_raw)
        if exact and sym:
            valid_classifications.append({
                "ticker":     sym,
                "theme":      exact,
                "confidence": conf,
                "reasoning":  reason,
            })
            classified_syms.add(sym)

    # Symbols LLM silently skipped
    unresolved = [s for s in batch if s not in classified_syms]

    _upd(
        processed=len(valid_classifications),
        failed=len(unresolved),
        current_batch=[],
    )

    # ── 7. Persist valid classifications ──────────────────────────────────────
    n_llm = 0
    if valid_classifications:
        try:
            n_llm = register_llm_classified_tickers(valid_classifications)
            print(f"[THEME_CLASSIFIER] registered {n_llm} LLM classifications")
        except Exception as exc:
            print(f"[THEME_CLASSIFIER] register error: {exc}")

    # ── 8. Mark unresolved as needs_review ────────────────────────────────────
    provider, _ = _detect_provider()
    if unresolved:
        if provider == "none":
            mark_needs_review(unresolved, "no_provider")
        else:
            mark_needs_review(unresolved, "llm_unresolved")

    _upd(
        mapped_from_llm=n_llm,
        needs_theme=len(unresolved),
        failures=unresolved,
    )


# ── Status + provenance ───────────────────────────────────────────────────────

def get_classifier_status() -> dict:
    """Return current classifier job state."""
    return dict(_CLASSIFIER_STATE)


def get_theme_provenance(symbol: str) -> dict:
    """
    Return full theme resolution trace for a single symbol.
    Shows which source determined the final theme and what the chain looks like.
    """
    sym = symbol.upper().strip()

    from services.theme_ticker_mapper import (
        map_ticker_to_primary_theme,
        map_ticker_to_theme_id,
    )

    # LLM overrides
    from services.theme_ticker_mapper import _LLM_OVERRIDES_PATH
    import json as _j
    llm_overrides: dict = {}
    try:
        if _LLM_OVERRIDES_PATH.exists():
            llm_overrides = _j.loads(_LLM_OVERRIDES_PATH.read_text()) or {}
    except Exception:
        pass

    # Neon manual override (category_overrides) — highest priority
    neon_override: str | None = None
    try:
        from services.category_overrides import get_overrides as _get_overrides
        neon_override = _get_overrides("default").get(sym)
    except Exception:
        pass

    canonical_theme = map_ticker_to_primary_theme(sym)
    canonical_id    = map_ticker_to_theme_id(sym) if canonical_theme else None
    llm_entry       = llm_overrides.get(sym)
    needs_review    = _load_needs_review().get(sym)

    # Themes-page store: check if this symbol is in ENRICHED_THEME_RS_UNIVERSE
    themes_page_theme: str | None = None
    themes_page_theme_id: str | None = None
    try:
        from services.theme_merge_layer import ENRICHED_THEME_RS_UNIVERSE as _etrs
        for _tid, _tmeta in _etrs.items():
            if sym in _tmeta.get("proxy_symbols", []) or sym in _tmeta.get("candidate_symbols", []):
                themes_page_theme    = _tmeta.get("display_name")
                themes_page_theme_id = _tid
                break
    except Exception:
        pass

    # Final source: manual_override wins, then themes_page membership, then llm, then canonical
    final_theme    = neon_override or themes_page_theme or canonical_theme
    final_theme_id = None
    if neon_override:
        source = "neon_manual_override"
        try:
            from services.theme_rs_universe import THEME_RS_UNIVERSE as _trs
            final_theme_id = next(
                (tid for tid, m in _trs.items()
                 if m.get("display_name", "").lower() == neon_override.lower()),
                canonical_id,
            )
        except Exception:
            final_theme_id = canonical_id
    elif themes_page_theme:
        source         = "themes_page_membership"
        final_theme_id = themes_page_theme_id
    elif llm_entry:
        source         = "llm_classified"
        final_theme_id = canonical_id
    elif canonical_theme and canonical_theme not in ("Other / Uncategorized",):
        source         = "canonical_map_or_industry"
        final_theme_id = canonical_id
    else:
        source = "none"

    return {
        "symbol":              sym,
        "final_theme":         final_theme,
        "final_theme_id":      final_theme_id,
        "canonical_theme":     canonical_theme,
        "canonical_theme_id":  canonical_id,
        "source":              source,
        "llm_override":        llm_entry,
        "neon_override":       {"category": neon_override} if neon_override else None,
        "themes_page_theme":   themes_page_theme,
        "themes_page_theme_id":themes_page_theme_id,
        "needs_review":        needs_review,
        "is_mapped":           bool(final_theme and final_theme not in ("Other / Uncategorized",)),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Canonical taxonomy single-ticker classifier — DeepSeek V4 Flash
# ═══════════════════════════════════════════════════════════════════════════════
#
#  Triggered on Watchlist ticker ADD (not save). Runs as a background
#  asyncio.create_task — the add response does not wait for classification.
#
#  Classifies against the CURRENT canonical taxonomy (THEME_RS_UNIVERSE),
#  validates returned IDs, and persists through atomic_taxonomy_write_db()
#  (the same canonical write path as manual taxonomy assignment).
#
#  Confidence threshold for auto-persist:  >= 0.75
#  Low-confidence / no-valid-theme / provider failure → ticker stays unassigned.
# ═══════════════════════════════════════════════════════════════════════════════

_CONFIDENCE_THRESHOLD = 0.75
_MIN_DESCRIPTION_LEN  = 20

# Background metadata hydration retry: fundamentals hydration for a freshly
# added ticker can take tens of seconds.  The classifier task retries the
# cache read (never a provider call) until company_name + description are
# available, then gives up with an observable metadata_insufficient outcome.
_MAX_HYDRATE_ATTEMPTS = 10
_HYDRATE_RETRY_DELAY  = 20.0

# ── Bounded classifier concurrency ───────────────────────────────────────────
# Bulk-add schedules one background task per new ticker.  Without a bound,
# 100 bulk-added tickers would fan out into 100 concurrent DeepSeek calls,
# hydration DB reads and canonical writes.  This in-process semaphore bounds
# the expensive single-ticker classification execution (metadata hydration
# reads, the DeepSeek call, and canonical persistence) to a small, safe number
# while the HTTP add response never awaits it.  Sleeps between hydration
# retries happen OUTSIDE the semaphore so waiting tasks hold no resource.
_MAX_CONCURRENT_CLASSIFICATIONS = 3
_CLASSIFY_SEMAPHORE: asyncio.Semaphore | None = None


def _get_classify_semaphore() -> asyncio.Semaphore:
    global _CLASSIFY_SEMAPHORE
    if _CLASSIFY_SEMAPHORE is None:
        _CLASSIFY_SEMAPHORE = asyncio.Semaphore(_MAX_CONCURRENT_CLASSIFICATIONS)
    return _CLASSIFY_SEMAPHORE

# ── Per-ticker single-ticker classifier observability ────────────────────────
# In-memory only: records when a ticker was scheduled and its latest result so
# status queries can distinguish never_scheduled / scheduled / provider_failed /
# metadata_insufficient / no_valid_theme / classified / etc.
_SCHEDULED_AT:  dict[str, str]  = {}   # sym -> ISO ts when task was scheduled
_TICKER_RESULTS: dict[str, dict] = {}  # sym -> latest result dict


def _record_scheduled(sym: str) -> None:
    _SCHEDULED_AT[sym] = _ts()


def _record_result(sym: str, result: dict) -> None:
    result["updated_at"] = _ts()
    _TICKER_RESULTS[sym] = dict(result)


def get_ticker_classification_status(symbol: str) -> dict:
    """Observability: latest single-ticker classification state for a symbol."""
    sym = (symbol or "").upper().strip()
    entry = _TICKER_RESULTS.get(sym)
    if entry:
        return {"ticker": sym, **dict(entry)}
    if sym in _SCHEDULED_AT:
        return {
            "ticker":      sym,
            "action":      "scheduled",
            "scheduled_at": _SCHEDULED_AT.get(sym),
            "updated_at":  None,
        }
    return {
        "ticker":      sym,
        "action":      "never_scheduled",
        "scheduled_at": None,
        "updated_at":  None,
    }


def _get_assignable_registry() -> dict:
    """Return only assignable canonical theme/sub_theme nodes."""
    from services.theme_rs_universe import THEME_RS_UNIVERSE
    return {
        k: v for k, v in THEME_RS_UNIVERSE.items()
        if v.get("assignable", True)
        and v.get("classification") in ("theme", "sub_theme")
    }


def _build_taxonomy_prompt(registry: dict) -> str:
    """
    Build a COMPACT deterministic taxonomy description for the LLM prompt.

    One canonical `id: display name` line per assignable theme/subtheme node.
    No per-node metadata, no parent references, no headings — the model only
    needs canonical IDs + concise display context to select memberships, and
    a smaller prompt reduces DeepSeek output-budget pressure (see
    _call_deepseek_taxonomy length-truncation handling).
    """
    lines = ["Assignable theme/subtheme IDs (use ONLY these):"]
    for tid, meta in sorted(registry.items()):
        lines.append(f"- {tid}: {meta.get('display_name', '')}")
    return "\n".join(lines)


def _build_single_ticker_prompt(
    ticker: str,
    company_name: str,
    description: str,
    sector: str,
    registry: dict,
) -> str:
    taxonomy_desc = _build_taxonomy_prompt(registry)
    return f"""You are a financial analyst performing thematic stock classification.

{taxonomy_desc}

Rules:
- primary_theme_id: the company's defining business identity. Prefer a subtheme
  when it fits better than a parent theme.
- additional_theme_ids: up to 3 material, direct secondary exposures (no
  tangential technology or supply-chain adjacency).
- Use only the IDs above — never invent IDs or use sector names as themes.
- If no theme/subtheme genuinely represents this company, set no_valid_theme=true.
- If the primary exposure is unclear, set confidence below 0.6.

Ticker to classify:
TICKER: {ticker}
Company: {company_name}
Sector: {sector or 'Unknown'}
Description: {description[:400]}

Return ONLY a single JSON object — no markdown, no explanation:
{{
  "ticker": "{ticker}",
  "company_name": "{company_name}",
  "primary_theme_id": "canonical_id_or_null",
  "primary_subtheme_id": "canonical_id_or_null",
  "additional_theme_ids": [],
  "confidence": 0.0,
  "rationale": "one-sentence business rationale",
  "no_valid_theme": false
}}"""


# DeepSeek output budget: the compacted prompt (see _build_taxonomy_prompt)
# needs far less than 2048 output tokens for the required JSON.  Live evidence
# showed deepseek-v4-flash can still return EMPTY content with
# finish_reason='length' (reasoning tokens consuming the budget) — exactly one
# bounded retry with a larger budget is allowed in that case only.
_DEEPSEEK_MAX_TOKENS          = 2048
_DEEPSEEK_RETRY_MAX_TOKENS    = 4096


async def _call_deepseek_taxonomy(prompt: str, model_name: str) -> dict | None:
    """
    Call DeepSeek V4 Flash for single-ticker taxonomy classification.

    Retry policy (bounded, background-task only — never on a request path):
      - first attempt: max_tokens = _DEEPSEEK_MAX_TOKENS
      - if the response is empty with finish_reason == 'length' (explicit
        budget truncation), exactly ONE retry with _DEEPSEEK_RETRY_MAX_TOKENS.
      - any other failure (API error, invalid JSON, empty content with any
        other finish_reason) stays observable and is NOT retried.
    """
    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not deepseek_key:
        print("[THEME_CLASSIFIER] DeepSeek API key not found")
        return None

    async def _attempt(max_tokens: int) -> tuple[str, str | None]:
        import openai as _oai
        client = _oai.OpenAI(api_key=deepseek_key, base_url=_DEEPSEEK_BASE_URL)
        resp = await asyncio.to_thread(
            client.chat.completions.create,
            model=model_name,
            max_tokens=max_tokens,
            temperature=0.1,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.choices[0].message.content or ""
        finish = getattr(resp.choices[0], "finish_reason", None)
        return raw, finish

    raw_text = ""
    finish = None
    try:
        raw_text, finish = await _attempt(_DEEPSEEK_MAX_TOKENS)
    except Exception as exc:
        print(f"[THEME_CLASSIFIER] DeepSeek call error: {exc}")
        return None

    if not raw_text.strip() and finish == "length":
        print(
            "[THEME_CLASSIFIER] DeepSeek length-truncated to empty content "
            f"(max_tokens={_DEEPSEEK_MAX_TOKENS}) — single bounded retry "
            f"with max_tokens={_DEEPSEEK_RETRY_MAX_TOKENS}"
        )
        try:
            raw_text, finish = await _attempt(_DEEPSEEK_RETRY_MAX_TOKENS)
        except Exception as exc:
            print(f"[THEME_CLASSIFIER] DeepSeek retry call error: {exc}")
            return None
        if not raw_text.strip():
            print(
                f"[THEME_CLASSIFIER] DeepSeek retry also returned empty content "
                f"(finish_reason={finish!r}) — observable provider failure"
            )
            return None
    elif not raw_text.strip():
        print(
            f"[THEME_CLASSIFIER] DeepSeek returned empty content "
            f"(finish_reason={finish!r}) — observable provider failure, no retry"
        )
        return None

    try:
        clean = raw_text.strip()
        if clean.startswith("```"):
            clean = clean.split("```")[1]
            if clean.lower().startswith("json"):
                clean = clean[4:]
            clean = clean.strip()
        return _json.loads(clean)
    except Exception as exc:
        print(f"[THEME_CLASSIFIER] DeepSeek JSON parse error: {exc}")
        return None


def _validate_taxonomy_result(
    result: dict,
    requested_ticker: str,
    registry: dict,
) -> tuple[dict | None, str]:
    """
    Validate a DeepSeek classification result against the canonical taxonomy.
    Returns (validated_result, error_reason).
    """
    ticker = (result.get("ticker") or "").upper().strip()
    if ticker != requested_ticker.upper():
        return None, f"Ticker mismatch: got {ticker!r}, expected {requested_ticker!r}"

    no_valid = bool(result.get("no_valid_theme", False))
    if no_valid:
        return {
            "ticker":              ticker,
            "no_valid_theme":      True,
            "confidence":          0.0,
            "rationale":           str(result.get("rationale", "")),
        }, ""

    assignable_ids = set(registry.keys())

    def _check_id(tid: str | None) -> str | None:
        if not tid:
            return None
        if tid not in assignable_ids:
            return None
        return tid

    primary = _check_id(result.get("primary_theme_id") or None)
    subtheme = _check_id(result.get("primary_subtheme_id") or None)
    raw_additional = result.get("additional_theme_ids") or []
    additional: list[str] = []
    for aid in raw_additional:
        valid_aid = _check_id(aid)
        if valid_aid and valid_aid not in (primary, subtheme):
            additional.append(valid_aid)

    # Validate subtheme parent relationship — correct if mismatched
    effective_primary = subtheme or primary
    if subtheme and subtheme in registry:
        parent_from_registry = registry[subtheme].get("parent_theme_id", "")
        if parent_from_registry and primary and parent_from_registry != primary:
            print(
                f"[THEME_CLASSIFIER] validator: subtheme {subtheme!r} parent is "
                f"{parent_from_registry!r}, correcting primary from {primary!r}"
            )
            primary = parent_from_registry

    confidence = float(result.get("confidence", 0.0) or 0.0)

    if not effective_primary and not additional:
        return {
            "ticker":              ticker,
            "no_valid_theme":      True,
            "confidence":          confidence,
            "rationale":           str(result.get("rationale", "")),
        }, ""

    return {
        "ticker":                ticker,
        "company_name":           str(result.get("company_name", "")),
        "primary_theme_id":       primary,
        "primary_subtheme_id":    subtheme,
        "additional_theme_ids":   sorted(set(additional)),
        "confidence":             confidence,
        "rationale":              str(result.get("rationale", "")),
        "no_valid_theme":         False,
    }, ""


async def _persist_classification(validated: dict) -> bool:
    """
    Persist a validated classification through the canonical atomic taxonomy
    write path. Returns True on success.
    """
    ticker = validated["ticker"]
    effective_primary = validated.get("primary_subtheme_id") or validated.get("primary_theme_id")
    additional = validated.get("additional_theme_ids") or []

    try:
        from data.pg_storage import atomic_taxonomy_write_db

        ticker_overrides: list[dict] = []

        if effective_primary:
            ticker_overrides.append({
                "theme_id": effective_primary,
                "symbol":   ticker,
                "action":   "add",
                "source":   "deepseek_auto_classify",
                "note":     f"confidence={validated['confidence']:.2f} rationale={validated.get('rationale', '')[:120]}",
            })

        for aid in additional:
            ticker_overrides.append({
                "theme_id": aid,
                "symbol":   ticker,
                "action":   "add",
                "source":   "deepseek_auto_classify",
                "note":     f"confidence={validated['confidence']:.2f}",
            })

        if not ticker_overrides:
            return False

        primary_op: dict | None = None
        if effective_primary:
            primary_op = {
                "action":  "set",
                "user_id": "default",
                "ticker":  ticker,
                "category": effective_primary,
                "source":  "deepseek_auto_classify",
                "reason":  f"auto-classified by DeepSeek V4 Flash (confidence={validated['confidence']:.2f})",
            }

        txn_result = atomic_taxonomy_write_db(
            ticker_overrides=ticker_overrides,
            primary_operation=primary_op,
        )

        if not txn_result.get("ok"):
            print(f"[THEME_CLASSIFIER] atomic write failed: {txn_result.get('error')}")
            return False

        # Cache invalidation — same pattern as manual taxonomy assignment
        try:
            from services.category_overrides import invalidate_overrides_cache
            invalidate_overrides_cache("default")
        except Exception as _inv_err:
            print(f"[THEME_CLASSIFIER] overrides cache invalidation error (non-fatal): {_inv_err}")

        try:
            from services.watchlist_router import invalidate_bulk_lkg_for_ticker
            invalidate_bulk_lkg_for_ticker(ticker)
        except Exception as _lkg_err:
            print(f"[THEME_CLASSIFIER] LKG invalidation error (non-fatal): {_lkg_err}")

        return True

    except Exception as exc:
        print(f"[THEME_CLASSIFIER] persist error: {exc}")
        return False


async def _hydrate_ticker_metadata(
    sym: str,
    company_name: str,
    description: str,
    sector: str,
) -> tuple[str, str, str]:
    """
    Read cached company metadata for sym from public.watchlist_fundamentals_cache
    (the canonical FMP profile store).  Cache-read only — never adds provider
    calls.  Returns (company_name, description, sector) with any previously
    supplied non-empty values preserved.
    """
    try:
        def _read_fundamentals() -> tuple[str, str, str]:
            from data.pg_storage import _get_conn as _pg_c, _put_conn as _pg_p
            conn = _pg_c()
            if conn is None:
                return company_name, description, sector
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT fields FROM public.watchlist_fundamentals_cache WHERE symbol = %s",
                        (sym,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        fields = row[0] if isinstance(row[0], dict) else {}
                        profile = fields.get("profile", {})
                        _c = (
                            profile.get("companyName")
                            or fields.get("companyName")
                            or profile.get("name")
                            or ""
                        ).strip()
                        _d = (profile.get("description") or fields.get("description") or "").strip()
                        _s = (profile.get("sector") or fields.get("sector") or "").strip()
                        return (
                            _c if _c and not company_name else company_name,
                            _d if _d and not description else description,
                            _s if _s and not sector else sector,
                        )
                return company_name, description, sector
            finally:
                _pg_p(conn)

        return await asyncio.to_thread(_read_fundamentals)
    except Exception:
        return company_name, description, sector


async def classify_and_assign_ticker(
    ticker: str,
    company_name: str = "",
    description: str = "",
    sector: str = "",
    hydrate_attempts: int | None = None,
    hydrate_delay: float | None = None,
) -> dict:
    """
    Classify a SINGLE ticker against the canonical taxonomy using DeepSeek V4 Flash,
    validate the result, and persist high-confidence assignments through the
    canonical atomic taxonomy write path.

    Idempotent / exactly-once-ish via per-ticker in-flight tracking.

    Metadata hydration retry: company_name/description/sector are hydrated from
    the cached FMP fundamentals store (cache-read only, never a provider call).
    Because the add-time priority hydration runs concurrently, the classifier
    retries the cache read a bounded number of times with short sleeps before
    giving up with an observable metadata_insufficient outcome.  Retry counts
    are injectable for tests; production defaults are module constants.

    Returns a result dict with keys:
      ticker, action (classified/input_incomplete/already_assigned/skipped_inflight/
      provider_unavailable/metadata_insufficient/provider_failed/parse_failed/
      validation_failed/low_confidence/no_valid_theme/persisted successfully/
      persistence failed),
      primary_theme_id, confidence, rationale, error (if any).
    """
    sym = ticker.upper().strip()
    _record_scheduled(sym)
    result = {
        "ticker":      sym,
        "action":      "unknown",
        "error":       None,
        "confidence":  0.0,
        "rationale":   "",
    }

    # ── 1. In-flight guard ────────────────────────────────────────────────────
    async with _INFLIGHT_LOCK:
        if sym in _INFLIGHT_TICKERS:
            result["action"] = "skipped_inflight"
            result["error"]  = "Classification already in progress for this ticker"
            return result
        _INFLIGHT_TICKERS.add(sym)

    try:
        # ── 2. Already assigned check ──────────────────────────────────────────
        try:
            from services.category_overrides import get_overrides
            category = get_overrides("default").get(sym)
            if category:
                result["action"] = "already_assigned"
                result["error"]  = f"Already has category override: {category}"
                return result
        except Exception:
            pass

        # Check if ticker has a canonical theme_ticker_overrides assignment
        try:
            from data.pg_storage import get_theme_ticker_overrides
            rows = get_theme_ticker_overrides() or []
            existing = [r for r in rows if r.get("symbol", "").upper() == sym and r.get("action") == "add"]
            if existing:
                result["action"] = "already_assigned"
                result["error"]  = "Already has theme_ticker_overrides assignments"
                return result
        except Exception:
            pass

        # ── 2b/3. Background metadata hydration + completeness gate ─────────────
        # If description/sector/company_name were not supplied, read them from
        # the existing watchlist_fundamentals_cache.  Runs in the background via
        # asyncio.to_thread() so the event loop is never blocked.  Never adds
        # provider calls.  Retries a bounded number of times so a concurrently
        # running add-time hydration can complete first.
        #
        # Concurrency: each hydration DB read is taken under the shared
        # classification semaphore so bulk adds cannot fan out unlimited
        # concurrent cache reads; the sleep between retries happens OUTSIDE
        # the semaphore so waiting tasks hold no resource.
        attempts = hydrate_attempts if hydrate_attempts is not None else _MAX_HYDRATE_ATTEMPTS
        delay    = hydrate_delay if hydrate_delay is not None else _HYDRATE_RETRY_DELAY

        company = (company_name or "").strip()
        desc    = (description or "").strip()
        sect    = (sector or "").strip()

        def _meta_ready() -> bool:
            return bool(company and company.upper() != sym and len(desc) >= _MIN_DESCRIPTION_LEN)

        for _attempt in range(attempts + 1):
            if not _meta_ready():
                async with _get_classify_semaphore():
                    company, desc, sect = await _hydrate_ticker_metadata(sym, company, desc, sect)
                company = (company or "").strip()
                desc    = (desc or "").strip()
                sect    = (sect or "").strip()
            if _meta_ready():
                break
            if _attempt < attempts:
                await asyncio.sleep(delay)

        if not company or company.strip().upper() == sym:
            result["action"] = "metadata_insufficient"
            result["error"]  = "METADATA_INSUFFICIENT: company_name missing or equals ticker"
            mark_needs_review([sym], "metadata_insufficient")
            return result

        desc_len = len(desc)
        if desc_len < _MIN_DESCRIPTION_LEN:
            result["action"] = "metadata_insufficient"
            result["error"]  = f"METADATA_INSUFFICIENT: description too short ({desc_len} chars, need {_MIN_DESCRIPTION_LEN})"
            mark_needs_review([sym], "metadata_insufficient")
            return result

        # ── 4. Provider check ──────────────────────────────────────────────────
        provider, model_name = _detect_provider()
        if provider != "deepseek" and not os.getenv("DEEPSEEK_API_KEY"):
            result["action"] = "provider_unavailable"
            result["error"]  = "DeepSeek API key not configured"
            return result

        provider, model_name = "deepseek", _DEFAULT_MODELS["deepseek"]

        # ── 5. Build prompt and call DeepSeek (bounded concurrency) ───────────
        registry = _get_assignable_registry()
        if not registry:
            result["action"] = "provider_unavailable"
            result["error"]  = "Taxonomy registry empty — cannot classify"
            return result

        prompt = _build_single_ticker_prompt(
            ticker=sym,
            company_name=company,
            description=desc,
            sector=sect,
            registry=registry,
        )

        _upd(last_provider="deepseek", last_model=model_name)
        async with _get_classify_semaphore():
            raw_result = await _call_deepseek_taxonomy(prompt, model_name)

        if raw_result is None:
            result["action"] = "provider_failed"
            result["error"]  = "DeepSeek API call failed or returned invalid JSON"
            mark_needs_review([sym], "deepseek_api_failure")
            return result

        # ── 6. Validate ────────────────────────────────────────────────────────
        validated, validation_error = _validate_taxonomy_result(
            raw_result, sym, registry,
        )

        if validation_error:
            result["action"]      = "validation_failed"
            result["error"]       = validation_error
            result["confidence"]  = float(raw_result.get("confidence", 0.0) or 0.0)
            result["rationale"]   = str(raw_result.get("rationale", "") or "")[:200]
            mark_needs_review([sym], f"validation_failed: {validation_error}")
            return result

        result["confidence"] = validated["confidence"]
        result["rationale"]  = validated.get("rationale", "")[:200]

        # ── 7. No valid theme ──────────────────────────────────────────────────
        if validated.get("no_valid_theme"):
            result["action"] = "no_valid_theme"
            result["error"]  = None
            result["rationale"] = validated.get("rationale", "")[:200]
            print(
                f"[THEME_CLASSIFIER] {sym}: no_valid_theme (successful outcome, "
                f"no theme persisted) rationale={result['rationale'][:80]!r}"
            )
            return result

        # ── 8. Confidence gate ─────────────────────────────────────────────────
        if validated["confidence"] < _CONFIDENCE_THRESHOLD:
            result["action"] = "low_confidence"
            result["error"]  = (
                f"Confidence {validated['confidence']:.2f} below threshold "
                f"{_CONFIDENCE_THRESHOLD} — needs manual review"
            )
            mark_needs_review([sym], f"low_confidence_{validated['confidence']:.2f}")
            return result

        # ── 9. Persist (bounded concurrency — canonical write under semaphore) ──
        async with _get_classify_semaphore():
            persisted = await _persist_classification(validated)
        if persisted:
            effective_primary = validated.get("primary_subtheme_id") or validated.get("primary_theme_id")
            result["action"]            = "classified"
            result["primary_theme_id"]  = effective_primary
            result["subtheme_id"]       = validated.get("primary_subtheme_id")
            result["additional_ids"]    = validated.get("additional_theme_ids", [])
            return result
        else:
            result["action"] = "persist_failed"
            result["error"]  = "Canonical write transaction failed"
            return result

    except Exception as exc:
        import traceback
        traceback.print_exc()
        result["action"] = "internal_error"
        result["error"]  = str(exc)
        return result

    finally:
        _record_result(sym, result)
        _detail = ""
        if result.get("primary_theme_id"):
            _detail = f" primary={result['primary_theme_id']}"
        if result.get("error"):
            _detail += f" err={result['error'][:120]!r}"
        print(f"[THEME_CLASSIFIER] single-ticker {sym}: action={result['action']}{_detail}")
        async with _INFLIGHT_LOCK:
            _INFLIGHT_TICKERS.discard(sym)
