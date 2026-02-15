from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from pydantic import BaseModel
from typing import List, Optional

from config import ANTHROPIC_API_KEY, POLYGON_API_KEY, AGENT_API_KEY, FMP_API_KEY, COINGECKO_API_KEY, CMC_API_KEY
from data.market_data_service import MarketDataService
from agent.claude_agent import TradingAgent

# ============================================================
# Initialize the app
# ============================================================
app = FastAPI(title="Trading Agent API")

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS: Allow your website to call this backend.
# IMPORTANT: Replace these URLs with YOUR actual website URLs.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Wire up the services
# ============================================================
data_service = MarketDataService(polygon_key=POLYGON_API_KEY, fmp_key=FMP_API_KEY, coingecko_key=COINGECKO_API_KEY, cmc_key=CMC_API_KEY)
agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service)

# ============================================================
# API Routes
# ============================================================


@app.get("/")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Verify the API key sent in the X-API-Key header."""
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Include X-API-Key header.",
        )
    if x_api_key != AGENT_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key.",
        )
    return x_api_key


class ChatMessage(BaseModel):
    role: str
    content: str

class QueryRequest(BaseModel):
    prompt: str
    history: Optional[List[ChatMessage]] = None

@app.post("/api/query")
@limiter.limit("10/minute")
async def query_agent(
    request: Request,
    body: QueryRequest,
    api_key: str = Header(None, alias="X-API-Key"),
):
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key.",
        )
    print(f"[API] Received query: prompt={body.prompt[:100]}")
    try:
        result = await agent.handle_query(
            body.prompt,
            history=[h.dict() for h in body.history] if body.history else None,
        )
        return result
    except Exception as e:
        import traceback
        print(f"[API] Error in /api/query: {e}")
        traceback.print_exc()
        return {"error": str(e), "type": "chat", "analysis": f"Error: {str(e)}"}


@app.post("/api/cache/clear")
@limiter.limit("5/minute")
async def clear_cache(request: Request, api_key: str = Header(None, alias="X-API-Key")):
    if not api_key or api_key != AGENT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    from data.cache import cache
    cache.clear()
    return {"status": "Cache cleared"}


@app.get("/api/health")
@limiter.limit("30/minute")
async def health_check(request: Request):
    """Detailed health check — tests if API keys are configured."""
    return {
        "anthropic_key_set": bool(ANTHROPIC_API_KEY),
        "polygon_key_set": bool(POLYGON_API_KEY),
    }