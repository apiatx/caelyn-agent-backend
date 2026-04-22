"""
Subscription / paywall guard for CaelynAI.

Usage in any endpoint:
    from subscription import require_subscription
    ...
    async def my_endpoint(request: Request, _: None = Depends(require_subscription)):
        ...

Owner (OWNER_USERNAME env var) is always allowed through.
Requests carrying a valid AGENT_API_KEY are also allowed through (same as
the X-API-Key header used throughout the rest of the app).
All other users get 402 with a JSON body that the frontend maps to the paywall page.
"""

import os
from fastapi import Request, Depends, HTTPException
from fastapi.responses import JSONResponse

# Fall back to AUTH_USERNAME so the login user is always the owner.
OWNER_USERNAME: str = os.getenv("OWNER_USERNAME") or os.getenv("AUTH_USERNAME", "admin")
_AGENT_API_KEY: str = os.getenv("AGENT_API_KEY", "")

def _get_user_id(request: Request) -> str | None:
    """Extract user_id from the Bearer JWT if present."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:]
    try:
        from auth import verify_token
        payload = verify_token(token)
        return payload.get("sub")
    except Exception:
        return None


def _has_valid_api_key(request: Request) -> bool:
    """Return True when the request carries the server's AGENT_API_KEY."""
    if not _AGENT_API_KEY:
        return False
    sent = (
        request.headers.get("X-API-Key", "")
        or request.headers.get("x-api-key", "")
    )
    return bool(sent) and sent == _AGENT_API_KEY


def require_subscription(request: Request) -> None:
    """
    FastAPI dependency.  Passes through silently for the owner.
    Also passes through for requests authenticated with AGENT_API_KEY
    (the X-API-Key header used throughout the frontend).
    Raises HTTP 402 for everyone else so the frontend can redirect to /subscribe.
    """
    # ── API-key auth (covers Social page and other X-API-Key callers) ──
    if _has_valid_api_key(request):
        return

    # ── JWT-owner auth ──────────────────────────────────────────────────
    user_id = _get_user_id(request)
    if user_id and user_id.lower() == OWNER_USERNAME.lower():
        return

    raise HTTPException(
        status_code=402,
        detail={
            "error": "SUBSCRIPTION_REQUIRED",
            "message": "This feature requires an active subscription.",
            "redirect": "/subscribe",
        },
    )
