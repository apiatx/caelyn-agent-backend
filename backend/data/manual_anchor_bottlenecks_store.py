"""
Manual anchor bottleneck overlay — PostgreSQL-backed CRUD store.
Uses the same pg_storage pool pattern as screener_hub_store.
No LLM. No web search. Deterministic.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

try:
    from data.pg_storage import _get_conn, _put_conn, is_available  # type: ignore
except Exception:
    _get_conn = lambda: None          # type: ignore
    _put_conn = lambda c: None        # type: ignore
    def is_available() -> bool:       # type: ignore
        return False

# ── DDL ────────────────────────────────────────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS public.manual_anchor_bottlenecks (
    id                       BIGSERIAL PRIMARY KEY,
    anchor_key               TEXT         NOT NULL,
    ticker                   TEXT         NOT NULL,
    company_name             TEXT         NOT NULL,
    tradingview_symbol       TEXT         NULL,
    supply_chain_role        TEXT         NOT NULL,
    bottleneck_score         NUMERIC(6,2) NOT NULL DEFAULT 60,
    evidence_grade           TEXT         NOT NULL DEFAULT 'B',
    relationship_specificity TEXT         NOT NULL DEFAULT 'direct',
    evidence                 JSONB        NOT NULL DEFAULT '[]'::jsonb,
    source_urls              JSONB        NOT NULL DEFAULT '[]'::jsonb,
    notes                    TEXT         NULL,
    deal_signed_date         DATE         NULL,
    added_by                 TEXT         NOT NULL DEFAULT 'admin',
    ticker_validated         BOOLEAN      NOT NULL DEFAULT FALSE,
    is_active                BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_manual_anchor_bottlenecks_anchor
    ON public.manual_anchor_bottlenecks (anchor_key, is_active);

CREATE INDEX IF NOT EXISTS idx_manual_anchor_bottlenecks_ticker
    ON public.manual_anchor_bottlenecks (ticker, is_active);
"""

_DDL_APPLIED = False


def ensure_manual_anchor_table() -> bool:
    global _DDL_APPLIED
    if _DDL_APPLIED:
        return True
    conn = _get_conn()
    if conn is None:
        print("[MANUAL_ANCHOR] DB not available — skipping table ensure")
        return False
    try:
        cur = conn.cursor()
        cur.execute(_DDL)
        conn.commit()
        cur.close()
        _DDL_APPLIED = True
        print("[MANUAL_ANCHOR] table ensured OK")
        return True
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] ensure_table error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        _put_conn(conn)


# ── Helpers ────────────────────────────────────────────────────────────────────

_COLS = [
    "id", "anchor_key", "ticker", "company_name", "tradingview_symbol",
    "supply_chain_role", "bottleneck_score", "evidence_grade",
    "relationship_specificity", "evidence", "source_urls", "notes",
    "deal_signed_date", "added_by", "ticker_validated", "is_active",
    "created_at", "updated_at",
]

_SELECT = f"""
    SELECT {', '.join(_COLS)}
    FROM public.manual_anchor_bottlenecks
"""


def _row_to_dict(row) -> dict:
    d = dict(zip(_COLS, row))
    if d.get("bottleneck_score") is not None:
        d["bottleneck_score"] = float(d["bottleneck_score"])
    for f in ("evidence", "source_urls"):
        v = d.get(f)
        if isinstance(v, str):
            try:
                d[f] = json.loads(v)
            except Exception:
                d[f] = []
        elif v is None:
            d[f] = []
    for f in ("deal_signed_date", "created_at", "updated_at"):
        v = d.get(f)
        if v is not None and hasattr(v, "isoformat"):
            d[f] = v.isoformat()
    return d


# ── Reads ──────────────────────────────────────────────────────────────────────

def get_manual_nodes(
    anchor_key: Optional[str] = None,
    active_only: bool = True,
) -> list[dict]:
    ensure_manual_anchor_table()
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        clauses: list[str] = []
        params:  list[Any] = []
        if active_only:
            clauses.append("is_active = TRUE")
        if anchor_key:
            clauses.append("UPPER(anchor_key) = %s")
            params.append(anchor_key.upper())
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        cur.execute(
            f"{_SELECT} {where} ORDER BY bottleneck_score DESC, created_at DESC",
            params,
        )
        rows = [_row_to_dict(r) for r in cur.fetchall()]
        cur.close()
        return rows
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] get_manual_nodes error: {exc}")
        return []
    finally:
        _put_conn(conn)


def get_manual_node_by_id(node_id: int) -> Optional[dict]:
    ensure_manual_anchor_table()
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(f"{_SELECT} WHERE id = %s", (node_id,))
        row = cur.fetchone()
        cur.close()
        return _row_to_dict(row) if row else None
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] get_by_id error: {exc}")
        return None
    finally:
        _put_conn(conn)


# ── Writes ─────────────────────────────────────────────────────────────────────

def insert_manual_node(data: dict) -> dict:
    """
    Insert a new manual node.
    Required: anchor_key, ticker, company_name, supply_chain_role.
    Returns the full inserted row as a dict.
    """
    ensure_manual_anchor_table()
    now      = datetime.now(timezone.utc)
    evidence = json.dumps(data.get("evidence") or [])
    src_urls = json.dumps(data.get("source_urls") or [])
    tv_sym   = data.get("tradingview_symbol") or data.get("ticker") or ""

    conn = _get_conn()
    if conn is None:
        raise RuntimeError("DB not available")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.manual_anchor_bottlenecks
                (anchor_key, ticker, company_name, tradingview_symbol,
                 supply_chain_role, bottleneck_score, evidence_grade,
                 relationship_specificity, evidence, source_urls, notes,
                 deal_signed_date, added_by, ticker_validated, is_active,
                 created_at, updated_at)
            VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s)
            RETURNING id
            """,
            (
                data["anchor_key"].upper(),
                data["ticker"].upper(),
                data["company_name"],
                tv_sym,
                data["supply_chain_role"],
                float(data.get("bottleneck_score") or 60),
                data.get("evidence_grade") or "B",
                data.get("relationship_specificity") or "direct",
                evidence,
                src_urls,
                data.get("notes"),
                data.get("deal_signed_date"),
                data.get("added_by") or "admin",
                bool(data.get("ticker_validated", False)),
                True,
                now,
                now,
            ),
        )
        row_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] insert error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _put_conn(conn)

    return get_manual_node_by_id(row_id) or {"id": row_id}


def update_manual_node(node_id: int, data: dict) -> Optional[dict]:
    """
    Update allowed fields of an existing manual node.
    Returns the updated row, or None if not found.
    """
    ensure_manual_anchor_table()

    _UPDATABLE = {
        "company_name", "tradingview_symbol", "supply_chain_role",
        "bottleneck_score", "evidence_grade", "relationship_specificity",
        "evidence", "source_urls", "notes", "deal_signed_date",
        "ticker_validated", "is_active",
    }

    set_clauses: list[str] = []
    params:      list[Any] = []

    for field in _UPDATABLE:
        if field not in data:
            continue
        val = data[field]
        if field in ("evidence", "source_urls"):
            val = json.dumps(val if isinstance(val, list) else [])
        elif field == "bottleneck_score" and val is not None:
            val = float(val)
        set_clauses.append(f"{field} = %s")
        params.append(val)

    if not set_clauses:
        return get_manual_node_by_id(node_id)

    set_clauses.append("updated_at = %s")
    params.append(datetime.now(timezone.utc))
    params.append(node_id)

    conn = _get_conn()
    if conn is None:
        raise RuntimeError("DB not available")
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE public.manual_anchor_bottlenecks SET {', '.join(set_clauses)} WHERE id = %s",
            params,
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] update error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _put_conn(conn)

    return get_manual_node_by_id(node_id)


def disable_manual_node(node_id: int) -> bool:
    """Soft-delete: sets is_active=False. Returns True if a row was updated."""
    ensure_manual_anchor_table()
    conn = _get_conn()
    if conn is None:
        raise RuntimeError("DB not available")
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE public.manual_anchor_bottlenecks SET is_active=FALSE, updated_at=NOW() WHERE id=%s",
            (node_id,),
        )
        affected = cur.rowcount
        conn.commit()
        cur.close()
        return affected > 0
    except Exception as exc:
        print(f"[MANUAL_ANCHOR] disable error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        _put_conn(conn)


# ── Shape converter (manual DB row → Chain Reaction–compatible row) ────────────

def manual_node_to_cr_row(row: dict) -> dict:
    """Convert a manual DB row to the same shape as curated_anchor_bottlenecks rows."""
    from services.playbook.curated_anchor_bottlenecks import (
        _LAYER_NAMES, _GRADE_WHY_HIDDEN, _rel_type,
        _why_it_matters, _why_now, _what_breaks, LAST_CURATED_AT,
    )
    ticker     = row["ticker"]
    co_name    = row["company_name"]
    anchor_key = row["anchor_key"]
    role       = row["supply_chain_role"]
    score      = float(row.get("bottleneck_score") or 60)
    grade      = row.get("evidence_grade") or "B"
    rel_spec   = row.get("relationship_specificity") or "direct"
    conf       = "high" if score >= 70 else "medium" if score >= 50 else "low"

    layer   = int(row.get("layer") or 2)
    themes  = row.get("themes") or []
    primary_theme = themes[0] if themes else anchor_key.lower()

    return {
        # ── Chain Reaction–compatible fields ───────────────────────────────────
        "bottleneck_ticker":        ticker,
        "company_name":             co_name,
        "anchor_ticker":            anchor_key,
        "giant_anchors":            [anchor_key],
        "supply_chain_role":        role,
        "layer":                    layer,
        "themes":                   themes,
        "bottleneck_score":         score,
        "confidence":               conf,
        "evidence":                 row.get("evidence") or [],
        "relationship_type":        _rel_type(rel_spec),
        "source_urls":              row.get("source_urls") or [],
        "why_it_matters":           _why_it_matters(anchor_key, co_name, layer, role, int(score)),
        "why_hidden":               _GRADE_WHY_HIDDEN.get(grade, _GRADE_WHY_HIDDEN["B"]),
        "why_now":                  _why_now(anchor_key, role),
        "what_would_break_thesis":  _what_breaks(rel_spec, anchor_key, ticker),
        # ── Fallback fields to match /api/bottlenecks/current row shape ────────
        "final_score":              score,
        "theme_alignment_score":    100.0,
        "bottleneck_type":          "supply_chain",
        "bottleneckReason":         role,
        "anchor_theme":             primary_theme,
        "theme":                    primary_theme,
        "discovery_sources":        ["manual"],
        "lastUpdated":              LAST_CURATED_AT,
        "momentum_score":           None,
        "volume_score":             None,
        "fundamental_score":        None,
        "social_score":             None,
        "options_score":            None,
        "change_percent_1d":        None,
        "revenueSignal":            None,
        "exchange":                 None,
        "country":                  None,
        "market_cap":               None,
        "marketCap":                None,
        "marketCapBucket":          None,
        # ── Curated-specific fields ────────────────────────────────────────────
        "anchor_key":               anchor_key,
        "anchor_name":              anchor_key,
        "tradingview_symbol":       row.get("tradingview_symbol") or ticker,
        "layer_name":               _LAYER_NAMES.get(layer, "Key Component"),
        "evidence_grade":           grade,
        "evidence_grade_reason":    row.get("notes") or "",
        "relationship_specificity": rel_spec,
        "source_type":              "manual",
        "manual_added":             True,
        "last_curated_at":          LAST_CURATED_AT,
        "manual_id":                row.get("id"),
        "deal_signed_date":         row.get("deal_signed_date"),
        "added_by":                 row.get("added_by"),
    }
