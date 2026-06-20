"""
Anchor Research Service — Monthly OpenAI-web-search grounded supply-chain research.

Design:
- Uses OpenAI Responses API with hosted web_search_preview (tool_choice=required).
- Runs at most ONE research call per anchor per 30-day cycle (unless force=True).
- Calls run SEQUENTIALLY — never in parallel — to avoid rate-limit saturation.
- Results are written to anchor_supply_chain_research_nodes (Neon).
- Weekly Chain Reaction scoring reads approved cached nodes; it NEVER calls any LLM.
- Page-load endpoints NEVER call any LLM.

Anchors configured for overlay research:
  SPCX      / SpaceX
  OPENAI    / OpenAI
  ANTHROPIC / Anthropic

Model: configured via OPENAI_ANCHOR_RESEARCH_MODEL env var, default gpt-4o.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

_RESEARCH_MODEL: str = os.getenv("OPENAI_ANCHOR_RESEARCH_MODEL", "gpt-4o")
_MAX_TOKENS: int = 16000
_RESEARCH_INTERVAL_DAYS: int = 30
_MIN_NODES_REQUIRED: int = 5
_MAX_NODES_ACCEPTED: int = 40
_RESEARCH_METHOD: str = "openai_responses_web_search"

OVERLAY_ANCHOR_KEYS: list[str] = ["SPCX", "OPENAI", "ANTHROPIC"]


# ── Lazy imports ───────────────────────────────────────────────────────────────

def _get_prompt_module():
    from services.playbook.prompts.serenity_anchor_bottleneck_research_v1 import (
        build_research_prompt,
        get_anchor_name,
        is_configured_anchor,
        PROMPT_VERSION,
        PROMPT_HASH,
    )
    return build_research_prompt, get_anchor_name, is_configured_anchor, PROMPT_VERSION, PROMPT_HASH


def _get_store():
    from data.screener_hub_store import (
        ensure_anchor_research_tables,
        get_anchor_research_status,
        upsert_anchor_research_nodes,
        quarantine_anchor_research_nodes,
        log_research_run,
        finish_research_run,
    )
    return (
        ensure_anchor_research_tables,
        get_anchor_research_status,
        upsert_anchor_research_nodes,
        quarantine_anchor_research_nodes,
        log_research_run,
        finish_research_run,
    )


def _get_openai_client():
    try:
        import openai
        from config import OPENAI_API_KEY
        if not OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY is not set")
        return openai.OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        raise RuntimeError(f"Cannot initialize OpenAI client: {e}") from e


def _get_quote_validators():
    from data.screener_hub_store import get_quotes, get_fundamentals
    return get_quotes, get_fundamentals


# ── Freshness check ────────────────────────────────────────────────────────────

def anchor_needs_research(anchor_key: str) -> dict:
    """Return freshness status for the anchor."""
    (
        ensure_anchor_research_tables,
        get_anchor_research_status,
        *_,
    ) = _get_store()

    ensure_anchor_research_tables()
    status = get_anchor_research_status(anchor_key.upper())

    if not status or status.get("node_count", 0) == 0:
        return {
            "needs_research": True,
            "reason": "no_research_exists",
            "last_researched_at": None,
            "next_research_due_at": None,
            "node_count": 0,
        }

    next_due_str = status.get("next_research_due_at")
    if not next_due_str:
        return {
            "needs_research": True,
            "reason": "next_research_due_missing",
            "last_researched_at": status.get("last_researched_at"),
            "next_research_due_at": None,
            "node_count": status.get("node_count", 0),
        }

    try:
        next_due = datetime.fromisoformat(str(next_due_str).replace("Z", "+00:00"))
        if next_due.tzinfo is None:
            next_due = next_due.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if now >= next_due:
            return {
                "needs_research": True,
                "reason": "research_stale",
                "last_researched_at": status.get("last_researched_at"),
                "next_research_due_at": next_due_str,
                "node_count": status.get("node_count", 0),
            }
        return {
            "needs_research": False,
            "reason": "research_fresh",
            "last_researched_at": status.get("last_researched_at"),
            "next_research_due_at": next_due_str,
            "node_count": status.get("node_count", 0),
        }
    except Exception as e:
        return {
            "needs_research": True,
            "reason": f"date_parse_error: {e}",
            "last_researched_at": status.get("last_researched_at"),
            "next_research_due_at": next_due_str,
            "node_count": status.get("node_count", 0),
        }


# ── JSON extraction ────────────────────────────────────────────────────────────

def _extract_json(raw_text: str) -> dict:
    """
    Parse the LLM response as JSON.
    Falls back to partial node extraction on truncation.
    """
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    # Attempt 1: full parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Attempt 2: trim trailing garbage
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    # Attempt 3: partial node recovery from truncated response
    nodes_key_pos = text.find('"nodes"')
    if nodes_key_pos >= 0:
        bracket_open = text.find("[", nodes_key_pos)
        if bracket_open >= 0:
            pos = bracket_open + 1
            depth = 0
            node_start: Optional[int] = None
            complete_nodes: list = []
            while pos < len(text):
                ch = text[pos]
                if ch == "{":
                    if depth == 0:
                        node_start = pos
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0 and node_start is not None:
                        try:
                            complete_nodes.append(json.loads(text[node_start:pos + 1]))
                        except json.JSONDecodeError:
                            pass
                        node_start = None
                elif ch == "]" and depth == 0:
                    break
                pos += 1

            if complete_nodes:
                prefix = text[:nodes_key_pos]
                ak = re.search(r'"anchor_key"\s*:\s*"([^"]+)"', prefix)
                an = re.search(r'"anchor_name"\s*:\s*"([^"]+)"', prefix)
                print(
                    f"[ANCHOR_RESEARCH] partial JSON recovery: "
                    f"{len(complete_nodes)} complete nodes recovered from truncated response"
                )
                return {
                    "anchor_key": ak.group(1) if ak else "",
                    "anchor_name": an.group(1) if an else "",
                    "node_count": len(complete_nodes),
                    "nodes": complete_nodes,
                    "_recovered_partial": True,
                }

    raise ValueError(f"Cannot parse response as JSON. First 300 chars: {text[:300]!r}")


# ── Ticker validation ──────────────────────────────────────────────────────────

_INVALID_TICKER_PATTERNS = re.compile(
    r"^$|CONFIRM|PRIVATE|N/A|UNKNOWN|TBD|TBA|XXX", re.IGNORECASE
)


def _looks_valid_ticker(ticker: str) -> bool:
    """Basic pattern check: non-empty, 1-6 uppercase alpha/digit chars, no bad patterns."""
    if not ticker:
        return False
    if _INVALID_TICKER_PATTERNS.search(ticker):
        return False
    # Allow letters, digits, dots (BRK.A), hyphens (BF-B)
    if not re.match(r"^[A-Z][A-Z0-9.\-]{0,9}$", ticker.upper()):
        return False
    return True


def _validate_tickers_batch_sync(tickers: list[str]) -> dict[str, bool]:
    """
    Batch-validate tickers using existing market data cache.
    Returns {ticker: is_validated}.

    Validated = get_quotes returns a price OR get_fundamentals returns a profile.
    Unvalidated = cache miss (ticker may still be real; don't reject outright).
    """
    if not tickers:
        return {}

    result: dict[str, bool] = {t: False for t in tickers}

    try:
        get_quotes, get_fundamentals = _get_quote_validators()

        # Round 1: get_quotes
        quotes = get_quotes(tickers)
        for t in tickers:
            q = (quotes.get(t) or {}).get("quote") or {}
            price = (
                q.get("last")
                or q.get("close")
                or q.get("price")
                or q.get("regularMarketPrice")
            )
            if price:
                result[t] = True

        # Round 2: fundamentals for cache misses
        misses = [t for t, v in result.items() if not v]
        if misses:
            funds = get_fundamentals(misses)
            for t in misses:
                profile = (funds.get(t) or {}).get("profile") or {}
                if profile.get("companyName") or profile.get("symbol"):
                    result[t] = True

    except Exception as e:
        print(f"[ANCHOR_RESEARCH] ticker validation error (non-fatal): {e}")

    return result


# ── Node validation ────────────────────────────────────────────────────────────

def _validate_node(
    node: Any,
    anchor_key: str,
    web_search_sources: list[dict],
    ticker_validation: dict[str, bool],
) -> dict:
    """
    Validate and normalise a single researched node dict.
    Raises ValueError if the node fails hard validation.
    Returns a clean dict ready to upsert.
    """
    if not isinstance(node, dict):
        raise ValueError(f"Node is not a dict: {type(node)}")

    # Ticker — must be non-empty and pass pattern check
    ticker = str(node.get("ticker") or "").strip().upper()
    if not _looks_valid_ticker(ticker):
        raw = node.get("ticker")
        raise ValueError(
            f"Node rejected — invalid ticker {raw!r} "
            f"(company: {node.get('company_name')!r})"
        )

    company_name = str(node.get("company_name") or "").strip()
    if not company_name:
        raise ValueError(f"Node {ticker!r} missing company_name")

    supply_chain_role = str(node.get("supply_chain_role") or "").strip()
    if not supply_chain_role:
        raise ValueError(f"Node {ticker!r} missing supply_chain_role")

    # Evidence — require at least 1 non-empty string
    evidence = node.get("evidence") or []
    if not isinstance(evidence, list):
        evidence = [str(evidence)]
    evidence = [str(e).strip() for e in evidence if str(e).strip()]
    if not evidence:
        raise ValueError(f"Node {ticker!r} has no evidence strings")

    # Source URLs — require at least 1 from either node JSON or web_search annotations
    source_urls = node.get("source_urls") or []
    if not isinstance(source_urls, list):
        source_urls = [str(source_urls)]
    source_urls = [str(u).strip() for u in source_urls if str(u).strip()]

    source_titles = node.get("source_titles") or []
    if not isinstance(source_titles, list):
        source_titles = [str(source_titles)]
    source_titles = [str(t).strip() for t in source_titles if str(t).strip()]

    # Supplement source_urls from web_search annotations if node didn't include any
    if not source_urls and web_search_sources:
        source_urls = [s["url"] for s in web_search_sources if s.get("url")]
        source_titles = [s["title"] for s in web_search_sources if s.get("title")]

    if not source_urls:
        raise ValueError(f"Node {ticker!r} has no source URLs")

    # Ticker validated via market data cache?
    ticker_validated = bool(ticker_validation.get(ticker, False))

    giant_anchors = node.get("giant_anchors") or []
    if not isinstance(giant_anchors, list):
        giant_anchors = [str(giant_anchors)]
    if anchor_key.upper() not in [g.upper() for g in giant_anchors]:
        giant_anchors = [anchor_key.upper()] + [
            g for g in giant_anchors if g.upper() != anchor_key.upper()
        ]

    themes = node.get("themes") or []
    if not isinstance(themes, list):
        themes = [str(themes)]

    bottleneck_score = node.get("bottleneck_score")
    try:
        bottleneck_score = max(0, min(100, int(float(str(bottleneck_score or 60)))))
    except (TypeError, ValueError):
        bottleneck_score = 60

    layer = node.get("layer")
    try:
        layer = max(0, min(4, int(str(layer or 2))))
    except (TypeError, ValueError):
        layer = 2

    confidence = str(node.get("confidence") or "medium").lower()
    if confidence not in ("high", "medium", "low"):
        confidence = "medium"

    relationship_type = str(node.get("relationship_type") or "direct").lower()
    if relationship_type not in ("direct", "indirect", "infrastructure", "public_proxy", "inferred"):
        relationship_type = "inferred"

    tradingview_symbol = str(node.get("tradingview_symbol") or ticker).strip().upper()

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "anchor_key":                     anchor_key.upper(),
        "anchor_name":                    str(node.get("anchor_name") or "").strip(),
        "ticker":                         ticker,
        "company_name":                   company_name,
        "is_public":                      True,   # always True — public companies only
        "exchange":                       str(node.get("exchange") or "").strip() or None,
        "tradingview_symbol":             tradingview_symbol,
        "supply_chain_role":              supply_chain_role,
        "relationship_type":              relationship_type,
        "themes":                         themes,
        "layer":                          layer,
        "bottleneck_score":               bottleneck_score,
        "confidence":                     confidence,
        "evidence":                       evidence,
        "source_urls":                    source_urls,
        "source_titles":                  source_titles,
        "web_search_sources":             web_search_sources,
        "giant_anchors":                  giant_anchors,
        "why_it_matters":                 str(node.get("why_it_matters") or "").strip() or None,
        "why_hidden":                     str(node.get("why_hidden") or "").strip() or None,
        "why_now":                        str(node.get("why_now") or "").strip() or None,
        "what_would_break_thesis":        str(node.get("what_would_break_thesis") or "").strip() or None,
        "public_market_proxy_reason":     str(node.get("public_market_proxy_reason") or "").strip() or None,
        "overlap_existing_node_registry": False,
        "ticker_validated":               ticker_validated,
        "last_researched_at":             now_iso,
    }


# ── Core research runner ───────────────────────────────────────────────────────

async def run_anchor_research(
    anchor_key: str,
    force: bool = False,
) -> dict:
    """
    Run one OpenAI-web-search research call for the given anchor.

    Parameters
    ----------
    anchor_key : e.g. "OPENAI", "SPCX", "ANTHROPIC"
    force      : If True, skip the 30-day freshness check.

    Returns
    -------
    {
      "status": "ok" | "skipped" | "error",
      "anchor_key": str,
      "anchor_name": str,
      "nodes_written": int,
      "nodes_rejected": int,
      "tickers_validated": list[str],
      "tickers_unvalidated": list[str],
      "web_searches_fired": int,
      "last_researched_at": str | None,
      "next_research_due_at": str | None,
      "model": str,
      "prompt_version": str,
      "prompt_hash": str,
      "research_method": str,
      "elapsed_s": float,
      "error": str | None,
      "skipped_reason": str | None,
    }
    """
    import time as _time
    t0 = _time.monotonic()
    anchor_key = anchor_key.upper()

    build_research_prompt, get_anchor_name, is_configured_anchor, PROMPT_VERSION, PROMPT_HASH = _get_prompt_module()
    (
        ensure_anchor_research_tables,
        get_anchor_research_status,
        upsert_anchor_research_nodes,
        quarantine_anchor_research_nodes,
        log_research_run,
        finish_research_run,
    ) = _get_store()

    ensure_anchor_research_tables()
    anchor_name = get_anchor_name(anchor_key) or anchor_key

    def _base_result(**kwargs) -> dict:
        return {
            "anchor_key":          anchor_key,
            "anchor_name":         anchor_name,
            "nodes_written":       0,
            "nodes_rejected":      0,
            "tickers_validated":   [],
            "tickers_unvalidated": [],
            "web_searches_fired":  0,
            "last_researched_at":  None,
            "next_research_due_at": None,
            "model":               _RESEARCH_MODEL,
            "prompt_version":      PROMPT_VERSION,
            "prompt_hash":         PROMPT_HASH,
            "research_method":     _RESEARCH_METHOD,
            "elapsed_s":           round(_time.monotonic() - t0, 2),
            "error":               None,
            "skipped_reason":      None,
            **kwargs,
        }

    if not is_configured_anchor(anchor_key):
        return _base_result(
            status="error",
            error=f"Anchor {anchor_key!r} has no research configuration.",
        )

    # ── Freshness gate ────────────────────────────────────────────────────────
    if not force:
        freshness = anchor_needs_research(anchor_key)
        if not freshness["needs_research"]:
            print(
                f"[ANCHOR_RESEARCH] {anchor_key}: skipping — research fresh "
                f"(next_due={freshness['next_research_due_at']}, nodes={freshness['node_count']})"
            )
            return _base_result(
                status="skipped",
                last_researched_at=freshness["last_researched_at"],
                next_research_due_at=freshness["next_research_due_at"],
                skipped_reason=f"fresh: {freshness['reason']}",
            )

    # ── Quarantine old approved rows before re-running ────────────────────────
    quarantine_anchor_research_nodes(anchor_key)

    # ── Log run start ─────────────────────────────────────────────────────────
    run_id = log_research_run(
        anchor_key=anchor_key,
        anchor_name=anchor_name,
        model=_RESEARCH_MODEL,
        prompt_version=PROMPT_VERSION,
        prompt_hash=PROMPT_HASH,
    )
    print(
        f"[ANCHOR_RESEARCH] {anchor_key}: starting web-search research "
        f"(run_id={run_id}, model={_RESEARCH_MODEL}, force={force})"
    )

    # ── Build prompt ──────────────────────────────────────────────────────────
    system_prompt, user_prompt = build_research_prompt(anchor_key)

    # ── OpenAI Responses API call with forced web_search ─────────────────────
    raw_text: str = ""
    web_search_sources: list[dict] = []
    web_searches_fired: int = 0

    try:
        client = _get_openai_client()

        def _call_openai():
            return client.responses.create(
                model=_RESEARCH_MODEL,
                instructions=system_prompt,
                tools=[{"type": "web_search_preview"}],
                tool_choice="required",
                input=user_prompt,
                max_output_tokens=_MAX_TOKENS,
            )

        response = await asyncio.to_thread(_call_openai)

        # Extract text and web-search metadata from response output
        for item in response.output:
            item_type = getattr(item, "type", None)
            if item_type == "web_search_call":
                web_searches_fired += 1
            elif item_type == "message":
                for block in (getattr(item, "content", None) or []):
                    if getattr(block, "type", None) == "output_text":
                        raw_text += getattr(block, "text", "")
                        for ann in (getattr(block, "annotations", None) or []):
                            if getattr(ann, "type", "") == "url_citation":
                                web_search_sources.append({
                                    "url":   getattr(ann, "url", ""),
                                    "title": getattr(ann, "title", ""),
                                })

        print(
            f"[ANCHOR_RESEARCH] {anchor_key}: OpenAI responded "
            f"({len(raw_text)} chars, {web_searches_fired} web searches, "
            f"{len(web_search_sources)} citations)"
        )

        if web_searches_fired == 0:
            print(
                f"[ANCHOR_RESEARCH] {anchor_key}: WARNING — no web_search_call item "
                f"in response; results may not be web-grounded"
            )

    except Exception as llm_err:
        err_str = str(llm_err)
        print(f"[ANCHOR_RESEARCH] {anchor_key}: OpenAI call failed: {err_str}")
        finish_research_run(run_id, status="error", nodes_written=0, error=err_str)
        return _base_result(status="error", error=err_str)

    # ── Parse JSON ────────────────────────────────────────────────────────────
    try:
        parsed = _extract_json(raw_text)
    except ValueError as parse_err:
        err_str = str(parse_err)
        print(f"[ANCHOR_RESEARCH] {anchor_key}: JSON parse failed: {err_str}")
        finish_research_run(run_id, status="parse_error", nodes_written=0, error=err_str)
        return _base_result(status="error", error=err_str)

    raw_nodes = parsed.get("nodes") or []
    if not isinstance(raw_nodes, list):
        raw_nodes = []

    # ── Ticker validation (batch, cache-based) ────────────────────────────────
    candidate_tickers = []
    for n in raw_nodes[:_MAX_NODES_ACCEPTED]:
        t = str(n.get("ticker") or "").strip().upper()
        if t and _looks_valid_ticker(t):
            candidate_tickers.append(t)

    ticker_validation: dict[str, bool] = {}
    if candidate_tickers:
        ticker_validation = await asyncio.to_thread(
            _validate_tickers_batch_sync, candidate_tickers
        )
        validated_list = [t for t, v in ticker_validation.items() if v]
        unvalidated_list = [t for t, v in ticker_validation.items() if not v]
        print(
            f"[ANCHOR_RESEARCH] {anchor_key}: ticker validation — "
            f"validated={validated_list}, unvalidated={unvalidated_list}"
        )

    # ── Validate nodes ────────────────────────────────────────────────────────
    validated: list[dict] = []
    rejected: list[dict] = []

    for raw_node in raw_nodes[:_MAX_NODES_ACCEPTED]:
        try:
            clean = _validate_node(
                raw_node, anchor_key, web_search_sources, ticker_validation
            )
            validated.append(clean)
        except ValueError as ve:
            rejected.append({"node": raw_node, "reason": str(ve)})
            print(f"[ANCHOR_RESEARCH] {anchor_key}: node rejected — {ve}")

    if len(validated) < _MIN_NODES_REQUIRED:
        err_str = (
            f"Only {len(validated)} valid nodes returned (min {_MIN_NODES_REQUIRED}). "
            f"Rejected {len(rejected)}."
        )
        print(f"[ANCHOR_RESEARCH] {anchor_key}: {err_str}")
        finish_research_run(run_id, status="insufficient_nodes", nodes_written=0, error=err_str)
        return _base_result(
            status="error",
            nodes_rejected=len(rejected),
            web_searches_fired=web_searches_fired,
            error=err_str,
        )

    # ── Write to DB ───────────────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    next_due = now + timedelta(days=_RESEARCH_INTERVAL_DAYS)
    ok = upsert_anchor_research_nodes(
        anchor_key=anchor_key,
        anchor_name=anchor_name,
        nodes=validated,
        model=_RESEARCH_MODEL,
        prompt_version=PROMPT_VERSION,
        prompt_hash=PROMPT_HASH,
        last_researched_at=now.isoformat(),
        next_research_due_at=next_due.isoformat(),
    )

    if not ok:
        err_str = "DB upsert failed"
        finish_research_run(run_id, status="db_error", nodes_written=0, error=err_str)
        return _base_result(
            status="error",
            nodes_rejected=len(rejected),
            web_searches_fired=web_searches_fired,
            error=err_str,
        )

    finish_research_run(run_id, status="ok", nodes_written=len(validated), error=None)
    elapsed = round(_time.monotonic() - t0, 2)
    tickers_validated   = [n["ticker"] for n in validated if n.get("ticker_validated")]
    tickers_unvalidated = [n["ticker"] for n in validated if not n.get("ticker_validated")]

    print(
        f"[ANCHOR_RESEARCH] {anchor_key}: done — {len(validated)} nodes written, "
        f"{len(rejected)} rejected, {web_searches_fired} web searches, {elapsed}s"
    )
    return {
        "status":              "ok",
        "anchor_key":          anchor_key,
        "anchor_name":         anchor_name,
        "nodes_written":       len(validated),
        "nodes_rejected":      len(rejected),
        "rejection_reasons":   [r["reason"] for r in rejected],
        "tickers_validated":   tickers_validated,
        "tickers_unvalidated": tickers_unvalidated,
        "web_searches_fired":  web_searches_fired,
        "last_researched_at":  now.isoformat(),
        "next_research_due_at": next_due.isoformat(),
        "model":               _RESEARCH_MODEL,
        "prompt_version":      PROMPT_VERSION,
        "prompt_hash":         PROMPT_HASH,
        "research_method":     _RESEARCH_METHOD,
        "elapsed_s":           elapsed,
        "error":               None,
        "skipped_reason":      None,
    }


# ── Monthly batch runner ───────────────────────────────────────────────────────

async def run_monthly_refresh(force: bool = False) -> dict:
    """Run monthly research for all configured overlay anchors, sequentially."""
    import time as _time
    t0 = _time.monotonic()
    results: list[dict] = []

    print(f"[ANCHOR_RESEARCH] monthly refresh starting (force={force}) anchors={OVERLAY_ANCHOR_KEYS}")

    for anchor_key in OVERLAY_ANCHOR_KEYS:
        print(f"[ANCHOR_RESEARCH] monthly: starting {anchor_key}")
        result = await run_anchor_research(anchor_key, force=force)
        results.append(result)
        if result["status"] not in ("ok", "skipped"):
            print(f"[ANCHOR_RESEARCH] monthly: {anchor_key} failed — {result.get('error')}")
        if anchor_key != OVERLAY_ANCHOR_KEYS[-1]:
            await asyncio.sleep(2.0)

    n_ok      = sum(1 for r in results if r["status"] == "ok")
    n_skipped = sum(1 for r in results if r["status"] == "skipped")
    n_error   = sum(1 for r in results if r["status"] == "error")
    elapsed   = round(_time.monotonic() - t0, 2)

    print(
        f"[ANCHOR_RESEARCH] monthly refresh done: ok={n_ok} skipped={n_skipped} "
        f"error={n_error} elapsed={elapsed}s"
    )
    return {
        "status":    "ok" if n_error == 0 else ("partial" if n_ok > 0 else "error"),
        "anchors":   results,
        "n_ok":      n_ok,
        "n_skipped": n_skipped,
        "n_error":   n_error,
        "elapsed_s": elapsed,
    }
