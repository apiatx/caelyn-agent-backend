---
name: Chart Radar user_id resolution
description: JWTAuthMiddleware is a no-op; correct pattern for extracting user_id from requests
---

JWTAuthMiddleware in main.py is explicitly disabled (pure pass-through, line ~151).
`request.state.user_id` is NEVER populated. All endpoints that need user_id must
parse the Bearer JWT directly.

**Rule:** Use this helper pattern (mirrors subscription.py):
```python
def _get_user_id(request: Request) -> str:
    uid = getattr(request.state, "user_id", None)   # future-proof
    if uid:
        return str(uid)
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        from auth import verify_token
        payload = verify_token(auth[7:])
        sub = payload.get("sub")
        if sub:
            return str(sub)
    return "default"
```

**Why:** Middleware disabled to avoid breaking StreamingResponse (keepalive /api/query).
Auth is handled per-endpoint via _jwt_or_key() or Bearer parsing directly.

**How to apply:** Any new router that saves user-specific data to Neon must use this
pattern, not getattr(request.state, "user_id", "default").
