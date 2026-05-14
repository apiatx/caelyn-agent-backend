"""
portfolio_categorize.py — POST /api/portfolio/categorize-themes

Uses Claude to classify unclassified portfolio tickers into named investment
themes, then persists the result so future GET /api/caelyn-terminal responses
place those tickers in named theme buckets instead of "Unclassified".

Flow:
  1. Fetch FMP company profiles for each ticker (bulk cache — zero network)
  2. Pull all known theme names from theme_ticker_mapper
  3. Single Claude call with all tickers + theme list → JSON classifications
  4. Register results via register_llm_classified_tickers (in-memory + disk)
  5. Invalidate terminal cache so next GET picks up the new theme assignments
  6. Return {classified, unresolved, saved}
"""
from __future__ import annotations

import asyncio
import json as _json

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

router = APIRouter(tags=["portfolio"])

_AUTH_HEADER = "X-API-Key"


def _check_key(api_key):
    from config import AGENT_API_KEY
    if AGENT_API_KEY and api_key != AGENT_API_KEY:
        return JSONResponse(status_code=401, content={"error": "Invalid or missing API key"})
    return None


class CategorizeRequest(BaseModel):
    tickers: list[str]


@router.post("/api/portfolio/categorize-themes")
async def categorize_themes(
    body: CategorizeRequest,
    api_key: str = Header(None, alias=_AUTH_HEADER),
):
    """
    Classify unclassified portfolio tickers into investment themes via LLM.

    Accepts {"tickers": ["SIVEF", "PENG", ...]} and returns:
      {"classified": [{ticker, theme, confidence}], "unresolved": [], "saved": true}

    Side-effect: persists classifications to data/llm_theme_overrides.json and
    updates the live in-memory theme index — next caelyn-terminal load uses new themes.
    Response time: 15–60s (single LLM call for all tickers).
    """
    err = _check_key(api_key)
    if err:
        return err

    tickers = [t.upper().strip() for t in body.tickers if t.strip()]
    if not tickers:
        return {"classified": [], "unresolved": [], "saved": False,
                "error": "No tickers provided"}

    # ── 1. Fetch company profiles (FMP bulk cache — no network calls) ──────────
    profiles: dict[str, dict] = {}
    try:
        from services.fmp_cache_service import get_company_profiles_bulk_cached  # type: ignore
        profiles = get_company_profiles_bulk_cached(tickers) or {}
    except Exception as _pe:
        print(f"[categorize_themes] profile fetch error (non-fatal): {_pe}")

    ticker_lines: list[str] = []
    for sym in tickers:
        prof     = profiles.get(sym) or {}
        name     = prof.get("name") or prof.get("companyName") or sym
        sector   = prof.get("sector") or ""
        industry = prof.get("industry") or prof.get("industryDescription") or ""
        desc     = (prof.get("description") or prof.get("longBusinessSummary") or "")[:160]
        line     = f"- {sym}: {name}"
        if sector:
            line += f" | Sector: {sector}"
        if industry:
            line += f" | Industry: {industry}"
        if desc:
            line += f" | {desc}"
        ticker_lines.append(line)

    # ── 2. Get all known theme names ───────────────────────────────────────────
    from services.theme_ticker_mapper import get_all_theme_names  # type: ignore
    all_themes: list[str] = sorted(get_all_theme_names())
    themes_str = "\n".join(f"- {t}" for t in all_themes)

    # ── 3. Build LLM prompt ────────────────────────────────────────────────────
    prompt = (
        "You are an expert investment theme classifier for a deep-tech portfolio.\n"
        "Assign each ticker to the MOST APPROPRIATE theme from the list below.\n\n"
        f"AVAILABLE THEMES:\n{themes_str}\n\n"
        f"TICKERS TO CLASSIFY:\n" + "\n".join(ticker_lines) + "\n\n"
        "Rules:\n"
        "1. Assign each ticker to exactly ONE theme from the list above.\n"
        "2. Choose the most specific match. If a company fits multiple themes, pick its primary business.\n"
        "3. confidence: 'high' (>80% sure), 'medium' (50-80%), 'low' (<50% — but still pick a theme).\n"
        "4. Return ONLY valid JSON — no markdown fences, no explanation outside the JSON.\n\n"
        'Return exactly this structure:\n'
        '{"classifications": ['
        '{"ticker": "SYM", "theme": "Exact Theme Name", "confidence": "high", "reasoning": "one sentence"}'
        "]}"
    )

    # ── 4. Call Claude ─────────────────────────────────────────────────────────
    from config import ANTHROPIC_API_KEY  # type: ignore
    from agent.model_policy import MODEL_CLAUDE_BALANCED  # type: ignore
    import anthropic

    raw_text = ""
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = await asyncio.to_thread(
            client.messages.create,
            model=MODEL_CLAUDE_BALANCED,
            max_tokens=1800,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = resp.content[0].text if resp.content else ""
        print(f"[categorize_themes] Claude response length={len(raw_text)}")
    except Exception as _ce:
        return JSONResponse(
            status_code=503,
            content={
                "error":      f"LLM call failed: {_ce}",
                "classified": [],
                "unresolved": tickers,
                "saved":      False,
            },
        )

    # ── 5. Parse Claude response ───────────────────────────────────────────────
    try:
        clean = raw_text.strip()
        # Strip markdown code fences if present
        if clean.startswith("```"):
            parts = clean.split("```")
            clean = parts[1] if len(parts) > 1 else clean
            if clean.lower().startswith("json"):
                clean = clean[4:]
            clean = clean.strip()
        parsed         = _json.loads(clean)
        classifications = parsed.get("classifications") or []
    except Exception as _pe:
        return JSONResponse(
            status_code=500,
            content={
                "error":      f"JSON parse failed: {_pe}",
                "raw":        raw_text[:400],
                "classified": [],
                "unresolved": tickers,
                "saved":      False,
            },
        )

    # ── 6. Validate themes + normalise case ───────────────────────────────────
    all_themes_lower = {t.lower(): t for t in all_themes}   # lower → exact
    classified: list[dict] = []
    unresolved: list[str]  = []

    for item in classifications:
        sym        = (item.get("ticker") or "").upper().strip()
        theme_raw  = (item.get("theme") or "").strip()
        confidence = item.get("confidence") or "medium"
        reasoning  = item.get("reasoning") or ""

        theme_exact = all_themes_lower.get(theme_raw.lower(), theme_raw)
        if theme_exact:
            classified.append({
                "ticker":     sym,
                "theme":      theme_exact,
                "confidence": confidence,
                "reasoning":  reasoning,
            })
        else:
            unresolved.append(sym)

    # Any tickers the LLM silently skipped
    classified_syms = {c["ticker"] for c in classified}
    for sym in tickers:
        if sym not in classified_syms:
            unresolved.append(sym)

    # ── 7. Persist to theme mapper (in-memory + disk) ─────────────────────────
    saved = False
    n_registered = 0
    if classified:
        try:
            from services.theme_ticker_mapper import register_llm_classified_tickers  # type: ignore
            n_registered = register_llm_classified_tickers(classified)
            saved = n_registered > 0
        except Exception as _re:
            print(f"[categorize_themes] register_llm_classified_tickers failed: {_re}")

    # ── 8. Invalidate terminal cache so next GET picks up new themes ───────────
    try:
        from data.cache import cache  # type: ignore
        from data.caelyn_terminal import CaelynTerminalProvider  # type: ignore
        from data.portfolio_store import canonical_file as _cf  # type: ignore
        ck = CaelynTerminalProvider.cache_key_for(_cf())
        cache.delete(ck)
        print(f"[categorize_themes] terminal cache invalidated: {ck}")
    except Exception as _ie:
        print(f"[categorize_themes] terminal cache invalidation error (non-fatal): {_ie}")

    print(
        f"[categorize_themes] done: classified={len(classified)} "
        f"unresolved={unresolved} registered={n_registered} saved={saved}"
    )

    return {
        "classified": classified,
        "unresolved": unresolved,
        "saved":      saved,
    }
