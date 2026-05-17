"""
JWT authentication utilities for CaelynAI backend.
Uses python-jose for JWT signing and bcrypt for password hashing.
"""

import os
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
import bcrypt



JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "caelyn_default_jwt_secret_change_in_production")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRY_REMEMBER = timedelta(days=3650)   # 10 years — effectively permanent
TOKEN_EXPIRY_SESSION = timedelta(days=3650)    # 10 years — effectively permanent

AUTH_USERNAME = os.getenv("AUTH_USERNAME", "admin")
AUTH_PASSWORD_HASH = os.getenv("AUTH_PASSWORD_HASH", "")


# ── Startup diagnostics ────────────────────────────────────────────────────────

def _startup_auth_diagnostics() -> None:
    """
    Run auth config validation at startup and print findings to server logs.
    Does NOT expose any secret values.
    """
    issues: list[str] = []

    # 1. Username
    if not AUTH_USERNAME:
        issues.append("AUTH_USERNAME is not set (defaulting to 'admin')")
    else:
        print(f"[AUTH] AUTH_USERNAME configured: '{AUTH_USERNAME}'")

    # 2. JWT secret strength check
    jwt_key = os.getenv("JWT_SECRET_KEY", "")
    if not jwt_key or jwt_key == "caelyn_default_jwt_secret_change_in_production":
        issues.append("JWT_SECRET_KEY is using the insecure default — set a strong random secret")

    # 3. ADMIN_PASSWORD bootstrap mode
    admin_pw = os.getenv("ADMIN_PASSWORD", "")
    if admin_pw:
        print(
            "[AUTH] ADMIN_PASSWORD is SET — this takes PRIORITY over AUTH_PASSWORD_HASH. "
            "Login uses plaintext comparison. Clear ADMIN_PASSWORD once you have a valid hash."
        )
        # Generate a bcrypt hash from ADMIN_PASSWORD and print it — safe to log
        generated_hash = bcrypt.hashpw(admin_pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
        print(
            f"[AUTH] Generated bcrypt hash from ADMIN_PASSWORD "
            f"(copy this into AUTH_PASSWORD_HASH secret, then clear ADMIN_PASSWORD):\n"
            f"  {generated_hash}"
        )
    else:
        # 4. AUTH_PASSWORD_HASH integrity check
        pw_hash = AUTH_PASSWORD_HASH
        if not pw_hash:
            issues.append(
                "AUTH_PASSWORD_HASH is empty — set it to a bcrypt hash or set ADMIN_PASSWORD for bootstrap"
            )
        elif not pw_hash.startswith("$2b$") and not pw_hash.startswith("$2a$"):
            issues.append(
                f"AUTH_PASSWORD_HASH appears invalid: length={len(pw_hash)}, "
                "expected a bcrypt hash starting with '$2b$' or '$2a$'"
            )
        elif len(pw_hash) < 55:
            issues.append(
                f"AUTH_PASSWORD_HASH is too short (length={len(pw_hash)}); "
                "a valid bcrypt hash is 60 characters — the value may be truncated"
            )
        else:
            print(f"[AUTH] AUTH_PASSWORD_HASH: configured, length={len(pw_hash)} ✓")

    if issues:
        for issue in issues:
            print(f"[AUTH] ⚠ WARNING: {issue}")
    else:
        print("[AUTH] Auth configuration looks healthy.")


# Run diagnostics on import (fires at startup when main.py imports auth)
try:
    _startup_auth_diagnostics()
except Exception as _diag_err:
    print(f"[AUTH] Startup diagnostics error (non-fatal): {_diag_err}")


# ── Core auth functions ────────────────────────────────────────────────────────
def hash_password(password: str) -> str:
    """Hash a password with bcrypt. Use this once to generate AUTH_PASSWORD_HASH."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plain password against a bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False
def create_token(user_id: str, remember_me: bool = False) -> str:
    """Create a signed JWT token."""
    expiry = TOKEN_EXPIRY_REMEMBER if remember_me else TOKEN_EXPIRY_SESSION
    payload = {
        "sub": user_id,
        "exp": datetime.now(timezone.utc) + expiry,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
def verify_token(token: str) -> dict:
    """Verify and decode a JWT token. Returns payload dict or raises JWTError."""
    return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
def validate_credentials(username: str, password: str) -> bool:
    """
    Validate username and password against environment-stored credentials.

    Priority:
    1. If ADMIN_PASSWORD secret is set, accept that plaintext password directly
       (bootstrap / password-reset mode — no need to pre-hash).
    2. Otherwise fall back to bcrypt verification against AUTH_PASSWORD_HASH.

    Debug logging: logs config state and failure reason on every failed attempt.
    Does NOT log plaintext passwords or hash values.
    """
    # ── Debug: log config state ────────────────────────────────────────────
    _auth_username_set = bool(AUTH_USERNAME)
    _hash_set          = bool(AUTH_PASSWORD_HASH)
    _hash_valid_fmt    = (
        AUTH_PASSWORD_HASH.startswith(("$2b$", "$2a$")) and len(AUTH_PASSWORD_HASH) >= 55
    ) if AUTH_PASSWORD_HASH else False
    _admin_pw_set      = bool(os.getenv("ADMIN_PASSWORD", ""))
    _username_matched  = (username == AUTH_USERNAME)

    # ── Username check ─────────────────────────────────────────────────────
    if not _username_matched:
        print(
            f"[AUTH] Login FAILED — username_received='{username}' "
            f"expected='{AUTH_USERNAME}' username_matched=False"
        )
        return False

    # ── Bootstrap path: ADMIN_PASSWORD overrides the hash ─────────────────
    admin_password = os.getenv("ADMIN_PASSWORD", "")
    if admin_password:
        result = (password == admin_password)
        if not result:
            print(
                f"[AUTH] Login FAILED — username='{username}' "
                f"auth_mode=ADMIN_PASSWORD "
                f"auth_username_set={_auth_username_set} "
                f"admin_password_set=True "
                f"hash_set={_hash_set} hash_valid_format={_hash_valid_fmt} "
                f"username_matched=True password_matched=False"
            )
        else:
            print(f"[AUTH] Login OK — username='{username}' auth_mode=ADMIN_PASSWORD")
        return result

    # ── Normal path: bcrypt hash ───────────────────────────────────────────
    if not AUTH_PASSWORD_HASH:
        print(
            f"[AUTH] Login FAILED — username='{username}' "
            f"auth_mode=BCRYPT "
            f"auth_username_set={_auth_username_set} "
            f"admin_password_set=False "
            f"hash_set=False "
            f"reason=AUTH_PASSWORD_HASH_empty"
        )
        return False

    if not _hash_valid_fmt:
        print(
            f"[AUTH] Login FAILED — username='{username}' "
            f"auth_mode=BCRYPT "
            f"auth_username_set={_auth_username_set} "
            f"admin_password_set=False "
            f"hash_set=True hash_valid_format=False "
            f"hash_length={len(AUTH_PASSWORD_HASH)} "
            f"reason=AUTH_PASSWORD_HASH_invalid_format"
        )
        return False

    result = verify_password(password, AUTH_PASSWORD_HASH)
    if not result:
        print(
            f"[AUTH] Login FAILED — username='{username}' "
            f"auth_mode=BCRYPT "
            f"auth_username_set={_auth_username_set} "
            f"admin_password_set=False "
            f"hash_set=True hash_valid_format=True "
            f"username_matched=True password_hash_verification=FAILED"
        )
    else:
        print(f"[AUTH] Login OK — username='{username}' auth_mode=BCRYPT")
    return result
