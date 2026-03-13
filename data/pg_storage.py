"""
PostgreSQL storage backend for prompt history and chat history.
Uses the DATABASE_URL environment variable set by Replit's PostgreSQL add-on.
Auto-creates tables on first use. Survives all deploys and autoscale events.
"""

import json
import os

_DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("REPLIT_DB_URL_POSTGRES")
_pool = None
_available = False


def _to_jsonb(value):
    if not isinstance(value, dict):
        return None
    try:
        from psycopg2.extras import Json
        return Json(value)
    except Exception:
        return json.dumps(value, default=str)


def _get_conn():
    """Get a connection from the pool (lazy-initialized)."""
    global _pool, _available
    if _pool is None:
        if not _DATABASE_URL:
            return None
        try:
            import psycopg2
            from psycopg2 import pool as _pg_pool
            _pool = _pg_pool.SimpleConnectionPool(1, 5, _DATABASE_URL)
            _available = True
        except Exception as e:
            print(f"[PG_STORAGE] Failed to create connection pool: {e}")
            _available = False
            return None
    try:
        return _pool.getconn()
    except Exception as e:
        print(f"[PG_STORAGE] Failed to get connection: {e}")
        return None


def _put_conn(conn):
    """Return a connection to the pool."""
    if _pool and conn:
        try:
            _pool.putconn(conn)
        except Exception:
            pass


def is_available() -> bool:
    """Check if PostgreSQL is available."""
    if not _DATABASE_URL:
        return False
    conn = _get_conn()
    if conn is None:
        return False
    _put_conn(conn)
    return True


def init_tables():
    """Create tables if they don't exist. Safe to call multiple times."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prompt_history (
                user_id TEXT NOT NULL,
                bucket_key TEXT NOT NULL,
                data JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, bucket_key)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_conversations (
                conv_id TEXT PRIMARY KEY,
                data JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_chat_conversations_updated
            ON chat_conversations (updated_at DESC)
        """)

        # New normalized chat schema (source of truth going forward)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                session_id TEXT NULL,
                title TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id BIGSERIAL PRIMARY KEY,
                conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
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
            ON messages (conversation_id, created_at ASC, id ASC)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
            ON conversations (updated_at DESC)
        """)
        conn.commit()
        cur.close()
        print("[PG_STORAGE] Tables initialized successfully")
        return True
    except Exception as e:
        print(f"[PG_STORAGE] Table creation error: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


# ── Prompt History ───────────────────────────────────────────

def ph_read(user_id: str) -> dict:
    """Read all prompt history for a user."""
    conn = _get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT bucket_key, data FROM prompt_history WHERE user_id = %s",
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
                INSERT INTO prompt_history (user_id, bucket_key, data, updated_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (user_id, bucket_key)
                DO UPDATE SET data = %s::jsonb, updated_at = NOW()
            """, (user_id, bucket_key, json_data, json_data))
        # Remove buckets that are no longer in data
        if data:
            cur.execute(
                "DELETE FROM prompt_history WHERE user_id = %s AND bucket_key != ALL(%s)",
                (user_id, list(data.keys())),
            )
        else:
            cur.execute(
                "DELETE FROM prompt_history WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[PG_STORAGE] ph_write error for {user_id}: {e}")
        conn.rollback()
    finally:
        _put_conn(conn)


def ph_write_bucket(user_id: str, bucket_key: str, bucket_data: dict):
    """Write a single bucket (more efficient for single-intent updates)."""
    conn = _get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        json_data = json.dumps(bucket_data, default=str)
        cur.execute("""
            INSERT INTO prompt_history (user_id, bucket_key, data, updated_at)
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

def chat_read(conv_id: str) -> dict | None:
    """Read a single conversation."""
    return chat_get_conversation(conv_id)


def chat_write(conv_id: str, data: dict):
    """Write/update a conversation."""
    # Keep compatibility with existing callers while storing in normalized schema.
    title = data.get("title") if isinstance(data, dict) else None
    messages = data.get("messages", []) if isinstance(data, dict) else []
    chat_replace_messages(conv_id, messages=messages, title=title)


def chat_delete(conv_id: str) -> bool:
    """Delete a conversation."""
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM conversations WHERE id = %s", (conv_id,))
        deleted_new = cur.rowcount > 0
        # Also clean legacy row if present
        cur.execute("DELETE FROM chat_conversations WHERE conv_id = %s", (conv_id,))
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
            FROM conversations c
            LEFT JOIN messages m ON m.conversation_id = c.id
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


def chat_create_conversation(conv_id: str, title: str | None = None, session_id: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO conversations (id, session_id, title, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (id)
            DO UPDATE SET title = COALESCE(EXCLUDED.title, conversations.title), updated_at = NOW()
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


def chat_append_message(
    conv_id: str,
    role: str,
    content: str,
    message_type: str = "chat",
    structured_payload: dict | None = None,
    preset_key: str | None = None,
    model_used: str | None = None,
) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO conversations (id, created_at, updated_at)
            VALUES (%s, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            (conv_id,),
        )
        cur.execute(
            """
            INSERT INTO messages (
                conversation_id, role, message_type, content,
                structured_payload, preset_key, model_used, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (conv_id, role, message_type or "chat", content or "", _to_jsonb(structured_payload), preset_key, model_used),
        )
        if role == "user":
            trimmed = (content or "").strip()
            title = (trimmed[:60] + "...") if len(trimmed) > 60 else trimmed
            if title:
                cur.execute("UPDATE conversations SET title = COALESCE(NULLIF(title, ''), %s), updated_at = NOW() WHERE id = %s", (title, conv_id))
            else:
                cur.execute("UPDATE conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        else:
            cur.execute("UPDATE conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_append_message error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def chat_replace_messages(conv_id: str, messages: list, title: str | None = None) -> bool:
    conn = _get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO conversations (id, title, created_at, updated_at)
            VALUES (%s, %s, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET title = COALESCE(EXCLUDED.title, conversations.title), updated_at = NOW()
            """,
            (conv_id, title),
        )
        cur.execute("DELETE FROM messages WHERE conversation_id = %s", (conv_id,))
        for msg in messages or []:
            cur.execute(
                """
                INSERT INTO messages (conversation_id, role, message_type, content, structured_payload, preset_key, model_used, created_at)
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
        cur.execute("UPDATE conversations SET updated_at = NOW() WHERE id = %s", (conv_id,))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"[PG_STORAGE] chat_replace_messages error for {conv_id}: {e}")
        conn.rollback()
        return False
    finally:
        _put_conn(conn)


def chat_get_conversation(conv_id: str) -> dict | None:
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title, created_at, updated_at FROM conversations WHERE id = %s", (conv_id,))
        conv_row = cur.fetchone()
        if not conv_row:
            cur.close()
            return None
        cur.execute(
            """
            SELECT id, role, message_type, content, structured_payload, preset_key, model_used, created_at
            FROM messages
            WHERE conversation_id = %s
            ORDER BY created_at ASC, id ASC
            """,
            (conv_id,),
        )
        messages = []
        for row in cur.fetchall():
            messages.append({
                "id": row[0],
                "role": row[1],
                "message_type": row[2],
                "content": row[3] or "",
                "structured_payload": row[4],
                "preset_key": row[5],
                "model_used": row[6],
                "created_at": row[7].isoformat() if row[7] else None,
            })
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


def storage_info() -> dict:
    """Return diagnostic info about PostgreSQL storage."""
    conn = _get_conn()
    if conn is None:
        return {"available": False, "reason": "No DATABASE_URL or connection failed"}
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM prompt_history")
        ph_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM conversations")
        conv_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM messages")
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
