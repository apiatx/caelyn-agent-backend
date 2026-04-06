"""
Subscription / paywall guard for CaelynAI.

Usage in any endpoint:
    from subscription import require_subscription
    ...
    async def my_endpoint(request: Request, _: None = Depends(require_subscription)):
        ...

Owner (OWNER_USERNAME env var) is always allowed through.
All other users get 402 with a JSON body that the frontend maps to the paywall page.
"""

import os
from fastapi import Request, Depends, HTTPException
from fastapi.responses import JSONResponse

# Fall back to AUTH_USERNAME so the login user is always the owner.
OWNER_USERNAME: str = os.getenv("OWNER_USERNAME") or os.getenv("AUTH_USERNAME", "admin")

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


def require_subscription(request: Request) -> None:
    """
    FastAPI dependency.  Passes through silently for the owner.
    Raises HTTP 402 for everyone else so the frontend can redirect to /subscribe.
    """
    user_id = _get_user_id(request)
    if user_id and user_id.lower() == OWNER_USERNAME.lower():
        return  # owner — always allowed
    raise HTTPException(
        status_code=402,
        detail={
            "error": "SUBSCRIPTION_REQUIRED",
            "message": "This feature requires an active subscription.",
            "redirect": "/subscribe",
        },
    )
