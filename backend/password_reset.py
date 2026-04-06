"""
Password-reset flow for CaelynAI.

Flow:
  1. POST /api/auth/request-reset  — generates a short-lived token and emails it
  2. GET  /api/auth/reset-password  — serves the reset page (HTML redirect)
  3. POST /api/auth/reset-password  — validates token, sets new AUTH_PASSWORD_HASH

Token store: in-memory dict (keyed by token, value is expiry timestamp).
Tokens expire after 30 minutes and are single-use.
"""

import os
import secrets
import time
import bcrypt
from typing import Optional

# ── Token store ───────────────────────────────────────────────────────────────
# { token_hex: expires_at_epoch_float }
_reset_tokens: dict[str, float] = {}
_TOKEN_TTL_SECONDS = 1800  # 30 minutes


def generate_reset_token() -> str:
    token = secrets.token_urlsafe(32)
    _reset_tokens[token] = time.time() + _TOKEN_TTL_SECONDS
    # Prune expired tokens
    expired = [t for t, exp in _reset_tokens.items() if time.time() > exp]
    for t in expired:
        _reset_tokens.pop(t, None)
    return token


def validate_and_consume_token(token: str) -> bool:
    exp = _reset_tokens.get(token)
    if exp is None:
        return False
    if time.time() > exp:
        _reset_tokens.pop(token, None)
        return False
    _reset_tokens.pop(token, None)  # single-use
    return True


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


# ── Resend email sender ───────────────────────────────────────────────────────

def send_reset_email(reset_url: str) -> bool:
    """Send reset link via Resend.  Returns True on success."""
    try:
        import resend
        resend.api_key = os.getenv("RESEND_API_KEY", "")
        if not resend.api_key:
            print("[RESET] RESEND_API_KEY not configured")
            return False

        from_addr  = os.getenv("RESET_EMAIL_FROM", "noreply@caelynai.com")
        to_addr    = os.getenv("RESET_EMAIL_TO",   "")
        if not to_addr:
            print("[RESET] RESET_EMAIL_TO not configured")
            return False

        params = {
            "from": from_addr,
            "to": [to_addr],
            "subject": "Caelyn AI — Password Reset",
            "html": f"""
<div style="font-family:monospace;background:#090909;color:#e0e0e0;padding:32px;max-width:480px;margin:0 auto;border:1px solid #2a2a2a;border-radius:4px">
  <p style="font-size:11px;letter-spacing:2px;color:#888;margin:0 0 16px">CAELYN AI</p>
  <h2 style="font-size:16px;font-weight:bold;color:#00e676;margin:0 0 16px">Password Reset Request</h2>
  <p style="font-size:13px;color:#bbb;margin:0 0 24px;line-height:1.6">
    Someone requested a password reset for your Caelyn AI account.<br>
    Click the link below to set a new password. This link expires in <strong style="color:#ffd600">30 minutes</strong>.
  </p>
  <a href="{reset_url}" style="display:inline-block;padding:12px 24px;background:#1b3a25;border:1px solid #00e676;color:#00e676;text-decoration:none;font-size:12px;letter-spacing:1px;border-radius:3px">
    ▶ RESET MY PASSWORD
  </a>
  <p style="font-size:11px;color:#555;margin:24px 0 0;line-height:1.5">
    If you didn't request this, you can safely ignore this email.<br>
    Link: <span style="color:#888">{reset_url}</span>
  </p>
</div>""",
        }
        response = resend.Emails.send(params)
        print(f"[RESET] Email sent — id={getattr(response, 'id', response)}")
        return True
    except Exception as exc:
        print(f"[RESET] Email error: {exc}")
        return False


def get_app_base_url(request) -> str:
    """Best-effort app base URL from request or env."""
    domain = os.getenv("REPLIT_DOMAINS", "")
    if domain:
        # REPLIT_DOMAINS may be comma-separated; use the first
        domain = domain.split(",")[0].strip()
        return f"https://{domain}"
    # Fallback: derive from request
    return str(request.base_url).rstrip("/")
