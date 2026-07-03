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
    "gemini": "gemini-2.0-flash-lite",
    "openai": "gpt-4o-mini",
}

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

    gemini_key = os.getenv("GEMINI_API_KEY", "")
    openai_key = os.getenv("OPENAI_API_KEY", "")

    if provider in ("gemini", "") and gemini_key:
        return "gemini", model or _DEFAULT_MODELS["gemini"]
    if provider in ("openai", "") and openai_key:
        return "openai", model or _DEFAULT_MODELS["openai"]
    if provider == "gemini":
        return "gemini", model or _DEFAULT_MODELS["gemini"]
    if provider == "openai":
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
