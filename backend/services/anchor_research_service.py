"""
Anchor Research Service — Monthly LLM-driven supply-chain research.

Design:
- Runs at most ONE LLM call per anchor per 30-day cycle (unless force=True).
- Calls run SEQUENTIALLY — never in parallel — to avoid rate-limit saturation.
- Results are written to anchor_supply_chain_research_nodes (Neon).
- Weekly Chain Reaction scoring reads approved cached nodes; it never calls the LLM.
- Page-load endpoints never call the LLM.

Anchors configured for overlay research:
  SPCX  / SpaceX
  OPENAI / OpenAI
  ANTHROPIC / Anthropic

LLM used: claude-3-5-sonnet-20241022 (Anthropic) via synchronous client in asyncio.to_thread.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

_RESEARCH_MODEL = "claude-3-5-sonnet-20241022"
_MAX_TOKENS = 8192
_RESEARCH_INTERVAL_DAYS = 30
_MIN_NODES_REQUIRED = 5
_MAX_NODES_ACCEPTED = 40

OVERLAY_ANCHOR_KEYS: list[str] = ["SPCX", "OPENAI", "ANTHROPIC"]


# ── Lazy imports (avoid circular / startup cost) ───────────────────────────────

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
        log_research_run,
        finish_research_run,
    )
    return (
        ensure_anchor_research_tables,
        get_anchor_research_status,
        upsert_anchor_research_nodes,
        log_research_run,
        finish_research_run,
    )


def _get_anthropic_client():
    try:
        import anthropic
        from config import ANTHROPIC_API_KEY
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    except Exception as e:
        raise RuntimeError(f"Cannot initialize Anthropic client: {e}") from e


# ── Freshness check ────────────────────────────────────────────────────────────

def anchor_needs_research(anchor_key: str) -> dict:
    """
    Return a dict describing whether this anchor needs a research run.

    Returns
    -------
    {
      "needs_research": bool,
      "reason": str,
      "last_researched_at": str | None,
      "next_research_due_at": str | None,
      "node_count": int,
    }
    """
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


# ── JSON extraction helpers ────────────────────────────────────────────────────

def _extract_json(raw_text: str) -> dict:
    """
    Parse the LLM response as JSON.  Handles minor formatting artifacts.
    Raises ValueError on parse failure.
    """
    text = raw_text.strip()
    # Strip optional markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Try to find the outermost JSON object
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"Cannot parse LLM response as JSON. First 300 chars: {text[:300]!r}")


def _validate_node(node: Any, anchor_key: str) -> dict:
    """
    Validate and normalise a single researched node dict.
    Raises ValueError if the node is too malformed to save.
    """
    if not isinstance(node, dict):
        raise ValueError(f"Node is not a dict: {type(node)}")

    ticker = str(node.get("ticker") or "").strip().upper()
    company_name = str(node.get("company_name") or "").strip()
    if not company_name:
        raise ValueError("Node missing company_name")

    supply_chain_role = str(node.get("supply_chain_role") or "").strip()
    if not supply_chain_role:
        raise ValueError(f"Node {ticker or company_name!r} missing supply_chain_role")

    evidence = node.get("evidence") or []
    if not isinstance(evidence, list):
        evidence = [str(evidence)]

    giant_anchors = node.get("giant_anchors") or []
    if not isinstance(giant_anchors, list):
        giant_anchors = [str(giant_anchors)]
    if anchor_key.upper() not in [g.upper() for g in giant_anchors]:
        giant_anchors = [anchor_key.upper()] + [g for g in giant_anchors if g.upper() != anchor_key.upper()]

    themes = node.get("themes") or []
    if not isinstance(themes, list):
        themes = [str(themes)]

    source_urls = node.get("source_urls") or []
    if not isinstance(source_urls, list):
        source_urls = [str(source_urls)]

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
    if relationship_type not in ("direct", "indirect", "infrastructure", "proxy", "inferred"):
        relationship_type = "inferred"

    now_iso = datetime.now(timezone.utc).isoformat()

    return {
        "anchor_key":                   anchor_key.upper(),
        "anchor_name":                  str(node.get("anchor_name") or "").strip(),
        "ticker":                       ticker,
        "company_name":                 company_name,
        "is_public":                    bool(node.get("is_public", True)),
        "exchange":                     str(node.get("exchange") or "").strip() or None,
        "supply_chain_role":            supply_chain_role,
        "relationship_type":            relationship_type,
        "themes":                       themes,
        "layer":                        layer,
        "bottleneck_score":             bottleneck_score,
        "confidence":                   confidence,
        "evidence":                     evidence,
        "source_urls":                  source_urls,
        "giant_anchors":                giant_anchors,
        "why_it_matters":               str(node.get("why_it_matters") or "").strip() or None,
        "why_hidden":                   str(node.get("why_hidden") or "").strip() or None,
        "why_now":                      str(node.get("why_now") or "").strip() or None,
        "what_would_break_thesis":      str(node.get("what_would_break_thesis") or "").strip() or None,
        "public_market_proxy_reason":   str(node.get("public_market_proxy_reason") or "").strip() or None,
        "overlap_existing_node_registry": bool(node.get("overlap_existing_node_registry", False)),
        "last_researched_at":           now_iso,
    }


# ── Core research runner ───────────────────────────────────────────────────────

async def run_anchor_research(
    anchor_key: str,
    force: bool = False,
) -> dict:
    """
    Run one LLM research call for the given anchor.

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
      "last_researched_at": str | None,
      "next_research_due_at": str | None,
      "model": str,
      "prompt_version": str,
      "prompt_hash": str,
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
        log_research_run,
        finish_research_run,
    ) = _get_store()

    ensure_anchor_research_tables()

    anchor_name = get_anchor_name(anchor_key) or anchor_key

    if not is_configured_anchor(anchor_key):
        return {
            "status": "error",
            "anchor_key": anchor_key,
            "anchor_name": anchor_name,
            "nodes_written": 0,
            "nodes_rejected": 0,
            "last_researched_at": None,
            "next_research_due_at": None,
            "model": _RESEARCH_MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "elapsed_s": round(_time.monotonic() - t0, 2),
            "error": f"Anchor {anchor_key!r} has no research configuration in the prompt file.",
            "skipped_reason": None,
        }

    # ── Freshness gate ────────────────────────────────────────────────────────
    if not force:
        freshness = anchor_needs_research(anchor_key)
        if not freshness["needs_research"]:
            print(
                f"[ANCHOR_RESEARCH] {anchor_key}: skipping — research fresh "
                f"(next_due={freshness['next_research_due_at']}, nodes={freshness['node_count']})"
            )
            return {
                "status": "skipped",
                "anchor_key": anchor_key,
                "anchor_name": anchor_name,
                "nodes_written": 0,
                "nodes_rejected": 0,
                "last_researched_at": freshness["last_researched_at"],
                "next_research_due_at": freshness["next_research_due_at"],
                "model": _RESEARCH_MODEL,
                "prompt_version": PROMPT_VERSION,
                "prompt_hash": PROMPT_HASH,
                "elapsed_s": round(_time.monotonic() - t0, 2),
                "error": None,
                "skipped_reason": f"fresh: {freshness['reason']}",
            }

    # ── Log run start ─────────────────────────────────────────────────────────
    run_id = log_research_run(
        anchor_key=anchor_key,
        anchor_name=anchor_name,
        model=_RESEARCH_MODEL,
        prompt_version=PROMPT_VERSION,
        prompt_hash=PROMPT_HASH,
    )
    print(f"[ANCHOR_RESEARCH] {anchor_key}: starting LLM research (run_id={run_id}, force={force})")

    # ── Build prompt ──────────────────────────────────────────────────────────
    system_prompt, user_prompt = build_research_prompt(anchor_key)

    # ── LLM call ──────────────────────────────────────────────────────────────
    raw_text: str = ""
    try:
        client = _get_anthropic_client()
        response = await asyncio.to_thread(
            client.messages.create,
            model=_RESEARCH_MODEL,
            max_tokens=_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw_text = response.content[0].text if response.content else ""
        print(f"[ANCHOR_RESEARCH] {anchor_key}: LLM responded ({len(raw_text)} chars)")
    except Exception as llm_err:
        err_str = str(llm_err)
        print(f"[ANCHOR_RESEARCH] {anchor_key}: LLM call failed: {err_str}")
        finish_research_run(run_id, status="error", nodes_written=0, error=err_str)
        return {
            "status": "error",
            "anchor_key": anchor_key,
            "anchor_name": anchor_name,
            "nodes_written": 0,
            "nodes_rejected": 0,
            "last_researched_at": None,
            "next_research_due_at": None,
            "model": _RESEARCH_MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "elapsed_s": round(_time.monotonic() - t0, 2),
            "error": err_str,
            "skipped_reason": None,
        }

    # ── Parse response ────────────────────────────────────────────────────────
    try:
        parsed = _extract_json(raw_text)
    except ValueError as parse_err:
        err_str = str(parse_err)
        print(f"[ANCHOR_RESEARCH] {anchor_key}: JSON parse failed: {err_str}")
        finish_research_run(run_id, status="parse_error", nodes_written=0, error=err_str)
        return {
            "status": "error",
            "anchor_key": anchor_key,
            "anchor_name": anchor_name,
            "nodes_written": 0,
            "nodes_rejected": 0,
            "last_researched_at": None,
            "next_research_due_at": None,
            "model": _RESEARCH_MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "elapsed_s": round(_time.monotonic() - t0, 2),
            "error": err_str,
            "skipped_reason": None,
        }

    raw_nodes = parsed.get("nodes") or []
    if not isinstance(raw_nodes, list):
        raw_nodes = []

    # ── Validate nodes ────────────────────────────────────────────────────────
    validated: list[dict] = []
    rejected: list[dict] = []
    for raw_node in raw_nodes[:_MAX_NODES_ACCEPTED]:
        try:
            clean = _validate_node(raw_node, anchor_key)
            validated.append(clean)
        except ValueError as ve:
            rejected.append({"node": raw_node, "reason": str(ve)})
            print(f"[ANCHOR_RESEARCH] {anchor_key}: node rejected — {ve}")

    if len(validated) < _MIN_NODES_REQUIRED:
        err_str = (
            f"Only {len(validated)} valid nodes returned (min {_MIN_NODES_REQUIRED}). "
            f"Rejected {len(rejected)}. LLM response may be malformed."
        )
        print(f"[ANCHOR_RESEARCH] {anchor_key}: {err_str}")
        finish_research_run(run_id, status="insufficient_nodes", nodes_written=0, error=err_str)
        return {
            "status": "error",
            "anchor_key": anchor_key,
            "anchor_name": anchor_name,
            "nodes_written": 0,
            "nodes_rejected": len(rejected),
            "last_researched_at": None,
            "next_research_due_at": None,
            "model": _RESEARCH_MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "elapsed_s": round(_time.monotonic() - t0, 2),
            "error": err_str,
            "skipped_reason": None,
        }

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
        err_str = "DB upsert failed — check screener_hub_store logs"
        finish_research_run(run_id, status="db_error", nodes_written=0, error=err_str)
        return {
            "status": "error",
            "anchor_key": anchor_key,
            "anchor_name": anchor_name,
            "nodes_written": 0,
            "nodes_rejected": len(rejected),
            "last_researched_at": None,
            "next_research_due_at": None,
            "model": _RESEARCH_MODEL,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": PROMPT_HASH,
            "elapsed_s": round(_time.monotonic() - t0, 2),
            "error": err_str,
            "skipped_reason": None,
        }

    finish_research_run(run_id, status="ok", nodes_written=len(validated), error=None)
    elapsed = round(_time.monotonic() - t0, 2)
    print(
        f"[ANCHOR_RESEARCH] {anchor_key}: done — {len(validated)} nodes written, "
        f"{len(rejected)} rejected, {elapsed}s"
    )
    return {
        "status": "ok",
        "anchor_key": anchor_key,
        "anchor_name": anchor_name,
        "nodes_written": len(validated),
        "nodes_rejected": len(rejected),
        "last_researched_at": now.isoformat(),
        "next_research_due_at": next_due.isoformat(),
        "model": _RESEARCH_MODEL,
        "prompt_version": PROMPT_VERSION,
        "prompt_hash": PROMPT_HASH,
        "elapsed_s": elapsed,
        "error": None,
        "skipped_reason": None,
    }


# ── Monthly batch runner ───────────────────────────────────────────────────────

async def run_monthly_refresh(force: bool = False) -> dict:
    """
    Run monthly research for all configured overlay anchors, sequentially.

    One anchor at a time — never in parallel.  Skips anchors that are still fresh
    unless force=True.

    Returns a summary dict with per-anchor results.
    """
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
        # Brief pause between anchors to avoid rate-limit saturation
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
