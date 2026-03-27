"""
PostgreSQL storage backend for prompt history and chat history.
Prefers NEON_DATABASE_URL (external cloud DB, works in dev + production).
Falls back to DATABASE_URL (Replit internal Helium DB, dev-only).
Auto-creates tables on first use. Survives all deploys and autoscale events.
"""

import json
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


def _sanitize_database_url(url: str | None) -> str | None:
    """Strip channel_binding from Neon pooler URLs — psycopg2-binary doesn't
    always handle SCRAM channel binding correctly with connection poolers."""
    if not url:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "channel_binding" in qs:
            del qs["channel_binding"]
            new_query = urlencode(qs, doseq=True)
            url = urlunparse(parsed._replace(query=new_query))
    except Exception:
        pass
    return url


# NEON_DATABASE_URL is the externally-accessible cloud DB (works in both dev and
# production deployments). DATABASE_URL points to Replit's internal Helium host
# which is only reachable from the dev workspace, not from deployed containers.
_RAW_DATABASE_URL = os.environ.get("NEON_DATABASE_URL") or os.environ.get("DATABASE_URL")
_DATABASE_URL = _sanitize_database_url(_RAW_DATABASE_URL)
_pool = None
_available = False
# Track last connection error for diagnostics
_last_conn_error: str | None = None


@traceable(name="pg_storage.to_jsonb")
def _to_jsonb(value):
    if not isinstance(value, dict):
        return None
    try:
        from psycopg2.extras import Json
        return Json(value)
    except Exception:
        return json.dumps(value, default=str)


def _destroy_pool():
    """Tear down the connection pool so the next _get_conn() rebuilds it."""
    global _pool, _available
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
    _pool = None
    _available = False


@traceable(name="pg_storage.get_conn")
def _get_conn():
    """Get a healthy connection from the pool (lazy-initialized).

    If a pooled connection is stale (Neon kills idle connections aggressively),
    discard it, destroy the pool, and rebuild once.  This guarantees callers
    always receive a usable connection or an explicit None.
    """
    global _pool, _available, _last_conn_error
    if not _DATABASE_URL:
        _last_conn_error = "No NEON_DATABASE_URL or DATABASE_URL set"
        return None

    for attempt in range(2):  # at most one retry after pool rebuild
        if _pool is None:
            try:
                import psycopg2
                from psycopg2 import pool as _pg_pool
                _pool = _pg_pool.SimpleConnectionPool(1, 5, _DATABASE_URL)
                _available = True
                _last_conn_error = None
            except Exception as e:
                _last_conn_error = f"Pool creation failed: {e}"
                print(f"[PG_STORAGE] {_last_conn_error}")
                _available = False
                return None

        conn = None
        try:
            conn = _pool.getconn()
        except Exception as e:
            _last_conn_error = f"getconn failed: {e}"
            print(f"[PG_STORAGE] {_last_conn_error}")
            _destroy_pool()
            continue

        # Health check: verify the connection is alive
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.execute("SET search_path TO public")
            conn.commit()
            cur.close()
            _last_conn_error = None
            return conn
        except Exception as e:
            _last_conn_error = f"Health check failed (attempt {attempt+1}): {e}"
            print(f"[PG_STORAGE] {_last_conn_error}")
            # Connection is dead — drop it and rebuild pool
            try:
                _pool.putconn(conn, close=True)
            except Exception:
                pass
            _destroy_pool()
            continue

    return None


@traceable(name="pg_storage.put_conn")
def _put_conn(conn):
    """Return a connection to the pool."""
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


@traceable(name="pg_storage.is_available")
def is_available() -> bool:
    """Check if PostgreSQL is available."""
    if not _DATABASE_URL:
        return False
    conn = _get_conn()
    if conn is None:
        return False
    _put_conn(conn)
    return True


def get_last_conn_error() -> str | None:
    """Return the last connection error message (for diagnostics)."""
    return _last_conn_error




@traceable(name="pg_storage.startup_probe")
def startup_probe() -> dict:
    """Startup diagnostic for PostgreSQL connectivity/schema visibility."""
    info = {"database_url_detected": bool(_DATABASE_URL), "connected": False, "database": None, "schema": None, "tables": []}
    if not _DATABASE_URL:
        return info
    conn = _get_conn()
    if conn is None:
        return info
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_schema()")
        db_row = cur.fetchone()
        if db_row:
            info["database"] = db_row[0]
            info["schema"] = db_row[1]
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name ASC
        """)
        info["tables"] = [r[0] for r in cur.fetchall()]
        info["connected"] = True
        cur.close()
    except Exception as e:
        info["error"] = str(e)
    finally:
        _put_conn(conn)
    return info

@traceable(name="pg_storage.init_tables")
def init_tables():
    """Create tables if they don't exist. Safe to call multiple times."""
    print("[PG_STORAGE] init_tables starting (target schema=public)")
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.prompt_history (
                user_id TEXT NOT NULL,
                bucket_key TEXT NOT NULL,
                data JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, bucket_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.chat_conversations (
                conv_id TEXT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated
            ON public.chat_conversations (updated_at DESC)
        """)

        # New normalized chat schema (source of truth going forward)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.conversations (
                id TEXT PRIMARY KEY,
                session_id TEXT NULL,
                title TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.messages (
                id BIGSERIAL PRIMARY KEY,
                conversation_id TEXT NOT NULL REFERENCES public.conversations(id) ON DELETE CASCADE,
                role TEXT NOT NULL,
                message_type TEXT NOT NULL DEFAULT 'chat',
                content TEXT NOT NULL DEFAULT '',
                structured_payload JSONB NULL,
                preset_key TEXT NULL,
                model_used TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_conversation_created
            ON public.messages (conversation_id, created_at ASC, id ASC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
            ON public.conversations (updated_at DESC)
        """)

        # Ticker mention snapshots — one row per ticker per assistant message
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.ticker_mentions (
                id BIGSERIAL PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                message_id BIGINT NULL,
                ticker TEXT NOT NULL,
                mentioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                mention_price NUMERIC(20, 6) NULL,
                asset_type TEXT NULL,
                source TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_mentions_conv
            ON public.ticker_mentions (conversation_id, mentioned_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ticker_mentions_message
            ON public.ticker_mentions (message_id)
        """)

        # ── Historic options data (Polygon EOD) ─────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_history (
                id BIGSERIAL PRIMARY KEY,
                underlying TEXT NOT NULL,
                option_ticker TEXT NOT NULL,
                expiration DATE NOT NULL,
                strike NUMERIC(12, 4) NOT NULL,
                option_type TEXT NOT NULL,
                trade_date DATE NOT NULL,
                open NUMERIC(12, 4),
                high NUMERIC(12, 4),
                low NUMERIC(12, 4),
                close NUMERIC(12, 4),
                volume BIGINT,
                vwap NUMERIC(12, 4),
                num_trades INT,
                fetched_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (option_ticker, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_history_underlying
            ON public.options_history (underlying, trade_date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_history_expiration
            ON public.options_history (underlying, expiration, trade_date)
        """)

        # ── Technical indicators for underlying stocks (Polygon) ────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.stock_technicals (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                indicator TEXT NOT NULL,
                trade_date DATE NOT NULL,
                value NUMERIC(20, 6),
                signal_value NUMERIC(20, 6),
                histogram NUMERIC(20, 6),
                fetched_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (ticker, indicator, trade_date)
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_technicals_ticker
            ON public.stock_technicals (ticker, trade_date)
        """)

        # ── Fetch progress tracking for background ingestion ────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_fetch_progress (
                ticker TEXT PRIMARY KEY,
                last_fetched_date DATE,
                contracts_fetched INT DEFAULT 0,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # ── Live options flow snapshots for intraday signal history ───────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.options_flow_snapshots (
                id BIGSERIAL PRIMARY KEY,
                underlying TEXT NOT NULL,
                contract_symbol TEXT NOT NULL,
                expiration DATE NULL,
                option_type TEXT NULL,
                strike NUMERIC(12, 4) NULL,
                underlying_price NUMERIC(12, 4) NULL,
                bid NUMERIC(12, 4) NULL,
                ask NUMERIC(12, 4) NULL,
                last NUMERIC(12, 4) NULL,
                midpoint NUMERIC(12, 4) NULL,
                volume BIGINT NULL,
                open_interest BIGINT NULL,
                implied_volatility NUMERIC(12, 6) NULL,
                delta NUMERIC(12, 6) NULL,
                gamma NUMERIC(12, 6) NULL,
                theta NUMERIC(12, 6) NULL,
                vega NUMERIC(12, 6) NULL,
                spread_pct NUMERIC(12, 4) NULL,
                premium_traded_estimate NUMERIC(16, 2) NULL,
                expected_move_pct NUMERIC(12, 4) NULL,
                captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_flow_snapshots_contract
            ON public.options_flow_snapshots (contract_symbol, captured_at DESC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_options_flow_snapshots_underlying
            ON public.options_flow_snapshots (underlying, captured_at DESC)
        """)

        conn.commit()
        cur.close()
        print("[PG_STORAGE] init_tables completed (CREATE TABLE IF NOT EXISTS executed)")
        return True
    except Exception as e:
        print(f"[PG_STORAGE] Table creation error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


# ── Prompt History ───────────────────────────────────────────

@traceable(name="pg_storage.ph_read")
def ph_read(user_id: str) -> dict:
    """Read all prompt history for a user."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT bucket_key, data FROM public.prompt_history WHERE user_id = %s",
            (user_id,),
        )
        result = {}
        for row in cur.fetchall():
            bucket_key, data = row
            if isinstance(data, str):
                data = json.loads(data)
            result[bucket_key] = data
        cur.close()
        return result
    except Exception as e:
        print(f"[PG_STORAGE] ph_read error for {user_id}: {e}")
        return {}
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.ph_write")
def ph_write(user_id: str, data: dict):
    """Write all prompt history for a user (full replace by bucket)."""
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        for bucket_key, bucket_data in data.items():
            json_data = json.dumps(bucket_data, default=str)
            cur.execute("""
                INSERT INTO public.prompt_history (user_id, bucket_key, data, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (user_id, bucket_key)
                DO UPDATE SET data = %s::jsonb, updated_at = NOW()
            """, (user_id, bucket_key, json_data, json_data))
        # Remove buckets that are no longer in data
        if data:
            cur.execute(
                "DELETE FROM public.prompt_history WHERE user_id = %s AND bucket_key != ALL(%s)",
                (user_id, list(data.keys())),
            )
        else:
            cur.execute(
                "DELETE FROM public.prompt_history WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] ph_write error for {user_id}: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.ph_read_bucket")
def ph_read_bucket(user_id: str, bucket_key: str) -> dict:
    """Read a single bucket for a user."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT data FROM public.prompt_history WHERE user_id = %s AND bucket_key = %s",
            (user_id, bucket_key),
        )
        row = cur.fetchone()
        cur.close()
        if row is None:
            return {}
        data = row[0]
        if isinstance(data, str):
            data = json.loads(data)
        return data
    except Exception as e:
        print(f"[PG_STORAGE] ph_read_bucket error for {user_id}/{bucket_key}: {e}")
        return {}
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.ph_write_bucket")
def ph_write_bucket(user_id: str, bucket_key: str, bucket_data: dict):
    """Write a single bucket (more efficient for single-intent updates)."""
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        json_data = json.dumps(bucket_data, default=str)
        cur.execute("""
            INSERT INTO public.prompt_history (user_id, bucket_key, data, updated_at)
            VALUES (%s, %s, %s::jsonb, NOW())
            ON CONFLICT (user_id, bucket_key)
            DO UPDATE SET data = %s::jsonb, updated_at = NOW()
        """, (user_id, bucket_key, json_data, json_data))
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] ph_write_bucket error: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)


# ── Chat History ─────────────────────────────────────────────

@traceable(name="pg_storage.chat_read")
def chat_read(conv_id: str) -> dict | None:
    """Read a single conversation."""
    return chat_get_conversation(conv_id)


@traceable(name="pg_storage.chat_write")
def chat_write(conv_id: str, data: dict):
    """Write/update a conversation."""
    # Keep compatibility with existing callers while storing in normalized schema.
    title = data.get("title") if isinstance(data, dict) else None
    messages = data.get("messages", []) if isinstance(data, dict) else []
    chat_replace_messages(conv_id, messages=messages, title=title)


@traceable(name="pg_storage.chat_delete")
def chat_delete(conv_id: str) -> bool:
    """Delete a conversation."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM public.conversations WHERE id = %s", (conv_id,))
        deleted_new = cur.rowcount > 0
        # Also clean legacy row if present
        cur.execute("DELETE FROM public.chat_conversations WHERE conv_id = %s", (conv_id,))
        deleted_legacy = cur.rowcount > 0
        deleted = deleted_new or deleted_legacy
        conn.commit()
        cur.close()
        return deleted
    except Exception as e:
        print(f"[PG_STORAGE] chat_delete error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.chat_list")
def chat_list() -> list:
    """List all conversations (summary only), sorted by updated_at desc."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                c.id,
                c.title,
                c.created_at,
                c.updated_at,
                COUNT(m.id) AS message_count
            FROM public.conversations c
            LEFT JOIN public.messages m ON m.conversation_id = c.id
            GROUP BY c.id, c.title, c.created_at, c.updated_at
            ORDER BY c.updated_at DESC, c.created_at DESC
        """)
        results = [
            {
                "id": row[0],
                "title": row[1] or "",
                "created_at": row[2].isoformat() if row[2] else "",
                "updated_at": row[3].isoformat() if row[3] else "",
                "message_count": int(row[4] or 0),
            }
            for row in cur.fetchall()
        ]
        cur.close()
        return results
    except Exception as e:
        print(f"[PG_STORAGE] chat_list error: {e}")
        return []
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.chat_create_conversation")
def chat_create_conversation(conv_id: str, title: str | None = None, session_id: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, session_id, title, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (id)
            DO UPDATE SET title = COALESCE(EXCLUDED.title, public.conversations.title), updated_at = NOW()
            """,
            (conv_id, session_id, title),
        )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_create_conversation error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.chat_append_message")
def chat_append_message(
    conv_id: str,
    role: str,
    content: str,
    message_type: str = "chat",
    structured_payload: dict | None = None,
    preset_key: str | None = None,
    model_used: str | None = None,
) -> int | None:
    """Append a message and return its BIGINT message_id, or None on failure."""
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, created_at, updated_at)
            VALUES (%s, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            (conv_id,),
        )
        cur.execute(
            """
            INSERT INTO public.messages (
                conversation_id, role, message_type, content,
                structured_payload, preset_key, model_used, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id
            """,
            (conv_id, role, message_type or "chat", content or "", _to_jsonb(structured_payload), preset_key, model_used),
        )
        row = cur.fetchone()
        message_id = row[0] if row else None
        if role == "user":
            trimmed = (content or "").strip()
            title = (trimmed[:60] + "...") if len(trimmed) > 60 else trimmed
            if title:
                cur.execute("UPDATE public.conversations SET title = COALESCE(NULLIF(title, ''), %s), updated_at = NOW() WHERE id = %s", (title, conv_id))
            else:
                cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        else:
            cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return message_id
    except Exception as e:
        print(f"[PG_STORAGE] chat_append_message error for {conv_id}: {e}")
        conn.rollback()
        return None
    finally:
        _put_conn(conn)


def add_ticker_mentions(
    conversation_id: str,
    message_id: int | None,
    mentions: list[dict],
) -> bool:
    """
    Persist ticker mention snapshots for a given assistant message.
    Each mention dict: {ticker, mention_price, asset_type, source}
    """
    if not mentions:
        return True
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        for m in mentions:
            ticker = (m.get("ticker") or "").upper().strip()
            if not ticker:
                continue
            price = m.get("mention_price") or m.get("price")
            asset_type = m.get("asset_type")
            source = m.get("source")
            cur.execute(
                """
                INSERT INTO public.ticker_mentions
                    (conversation_id, message_id, ticker, mention_price, asset_type, source, mentioned_at, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                """,
                (conversation_id, message_id, ticker, price, asset_type, source),
            )
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] add_ticker_mentions error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def get_ticker_mentions_by_conv(conversation_id: str) -> list[dict]:
    """Return all ticker mention snapshots for a conversation, newest first."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, message_id, ticker, mentioned_at, mention_price, asset_type, source
            FROM public.ticker_mentions
            WHERE conversation_id = %s
            ORDER BY mentioned_at DESC
            """,
            (conversation_id,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "message_id": r[1],
                "ticker": r[2],
                "mentioned_at": r[3].isoformat() if r[3] else None,
                "mention_price": float(r[4]) if r[4] is not None else None,
                "asset_type": r[5],
                "source": r[6],
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[PG_STORAGE] get_ticker_mentions_by_conv error: {e}")
        return []
    finally:
        _put_conn(conn)


def chat_list_recent(limit: int = 5) -> list:
    """Return the N most recently updated conversations (lightweight, for sidebar)."""
    conn = _get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.id,
                c.title,
                c.created_at,
                c.updated_at,
                (
                    SELECT m.model_used
                    FROM public.messages m
                    WHERE m.conversation_id = c.id
                    ORDER BY m.created_at DESC
                    LIMIT 1
                ) AS last_model_used
            FROM public.conversations c
            ORDER BY c.updated_at DESC
            LIMIT %s
            """,
            (max(1, min(limit, 50)),),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "id": r[0],
                "title": r[1] or "",
                "created_at": r[2].isoformat() if r[2] else "",
                "updated_at": r[3].isoformat() if r[3] else "",
                "last_model_used": r[4],
            }
            for r in rows
        ]
    except Exception as e:
        print(f"[PG_STORAGE] chat_list_recent error: {e}")
        return []
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.chat_replace_messages")
def chat_replace_messages(conv_id: str, messages: list, title: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO public.conversations (id, title, created_at, updated_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET title = COALESCE(EXCLUDED.title, public.conversations.title), updated_at = NOW()
            """,
            (conv_id, title),
        )
        cur.execute("DELETE FROM public.messages WHERE conversation_id = %s", (conv_id,))
        for msg in messages or []:
            cur.execute(
                """
                INSERT INTO public.messages (conversation_id, role, message_type, content, structured_payload, preset_key, model_used, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    conv_id,
                    msg.get("role", "assistant"),
                    msg.get("message_type", "chat"),
                    msg.get("content", ""),
                    _to_jsonb(msg.get("structured_payload")),
                    msg.get("preset_key"),
                    msg.get("model_used"),
                ),
            )
        cur.execute("UPDATE public.conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_replace_messages error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.chat_get_conversation")
def chat_get_conversation(conv_id: str) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title, created_at, updated_at FROM public.conversations WHERE id = %s", (conv_id,))
        conv_row = cur.fetchone()
        if not conv_row:
            cur.close()
            return None
        cur.execute(
            """
            SELECT id, role, message_type, content, structured_payload, preset_key, model_used, created_at
            FROM public.messages
            WHERE conversation_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (conv_id,),
        )
        messages = []
        msg_id_list = []
        for row in cur.fetchall():
            msg_id_list.append(row[0])
            messages.append({
                "id": row[0],
                "role": row[1],
                "message_type": row[2],
                "content": row[3] or "",
                "structured_payload": row[4],
                "preset_key": row[5],
                "model_used": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
                "ticker_mentions": [],
            })

        # Attach ticker mentions per message (single batch query)
        if msg_id_list:
            cur.execute(
                """
                SELECT message_id, ticker, mentioned_at, mention_price, asset_type, source
                FROM public.ticker_mentions
                WHERE message_id = ANY(%s)
                ORDER BY mentioned_at ASC
                """,
                (msg_id_list,),
            )
            mentions_by_msg: dict[int, list] = {}
            for mr in cur.fetchall():
                mid = mr[0]
                mentions_by_msg.setdefault(mid, []).append({
                    "ticker": mr[1],
                    "mentioned_at": mr[2].isoformat() if mr[2] else None,
                    "mention_price": float(mr[3]) if mr[3] is not None else None,
                    "asset_type": mr[4],
                    "source": mr[5],
                })
            for msg in messages:
                msg["ticker_mentions"] = mentions_by_msg.get(msg["id"], [])

        cur.close()
        return {
            "id": conv_row[0],
            "title": conv_row[1] or "",
            "created_at": conv_row[2].isoformat() if conv_row[2] else "",
            "updated_at": conv_row[3].isoformat() if conv_row[3] else "",
            "messages": messages,
        }
    except Exception as e:
        print(f"[PG_STORAGE] chat_get_conversation error for {conv_id}: {e}")
        return None
    finally:
        _put_conn(conn)


@traceable(name="pg_storage.storage_info")
def storage_info() -> dict:
    """Return diagnostic info about PostgreSQL storage."""
    conn = _get_conn()
    if conn is None:
        return {"available": False, "reason": "No DATABASE_URL or connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM public.prompt_history")
        ph_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.conversations")
        conv_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM public.messages")
        msg_count = cur.fetchone()[0]
        cur.close()
        return {
            "available": True,
            "prompt_history_rows": ph_count,
            "conversations": conv_count,
            "messages": msg_count,
        }
    except Exception as e:
        return {"available": False, "reason": str(e)}
    finally:
        _put_conn(conn)
