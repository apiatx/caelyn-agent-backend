"""
JWT authentication utilities for CaelynAI backend.
Uses python-jose for JWT signing and bcrypt for password hashing.
"""

import os
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
import bcrypt

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop


JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "caelyn_default_jwt_secret_change_in_production")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRY_REMEMBER = timedelta(days=30)
TOKEN_EXPIRY_SESSION = timedelta(hours=24)

AUTH_USERNAME = os.getenv("AUTH_USERNAME", "admin")
AUTH_PASSWORD_HASH = os.getenv("AUTH_PASSWORD_HASH", "")


@traceable(name="auth.hash_password")
def hash_password(password: str) -> str:
    """Hash a password with bcrypt. Use this once to generate AUTH_PASSWORD_HASH."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


@traceable(name="auth.verify_password")
def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plain password against a bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


@traceable(name="auth.create_token")
def create_token(user_id: str, remember_me: bool = False) -> str:
    """Create a signed JWT token."""
    expiry = TOKEN_EXPIRY_REMEMBER if remember_me else TOKEN_EXPIRY_SESSION
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + expiry,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)


@traceable(name="auth.verify_token")
def verify_token(token: str) -> dict:
    """Verify and decode a JWT token. Returns payload dict or raises JWTError."""
    return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])


@traceable(name="auth.validate_credentials")
def validate_credentials(username: str, password: str) -> bool:
    """Validate username and password against environment-stored credentials.

    Priority:
    1. If ADMIN_PASSWORD secret is set, accept that plaintext password directly
       (bootstrap / password-reset mode — no need to pre-hash).
    2. Otherwise fall back to bcrypt verification against AUTH_PASSWORD_HASH.
    """
    if username != AUTH_USERNAME:
        return False
    # ── Bootstrap path: ADMIN_PASSWORD overrides the hash ──────────────────
    admin_password = os.getenv("ADMIN_PASSWORD", "")
    if admin_password:
        return password == admin_password
    # ── Normal path: bcrypt hash ────────────────────────────────────────────
    if not AUTH_PASSWORD_HASH:
        return False
    return verify_password(password, AUTH_PASSWORD_HASH)
