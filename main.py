from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

from config import ANTHROPIC_API_KEY, POLYGON_API_KEY
from data.market_data_service import MarketDataService
from agent.claude_agent import TradingAgent

# ============================================================
# Initialize the app
# ============================================================
app = FastAPI(title="Trading Agent API")

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
data_service = MarketDataService(polygon_key=POLYGON_API_KEY)
agent = TradingAgent(api_key=ANTHROPIC_API_KEY, data_service=data_service)

# ============================================================
# API Routes
# ============================================================


@app.get("/")
async def root():
    """Health check — visit this URL to confirm the backend is running."""
    return {"status": "running", "message": "Trading Agent API is live"}


class ChatMessage(BaseModel):
    role: str
    content: str

class QueryRequest(BaseModel):
    prompt: str
    history: Optional[List[ChatMessage]] = None

@app.post("/api/query")
async def query_agent(request: QueryRequest):
    try:
        result = await agent.handle_query(
            request.prompt,
            history=[h.dict() for h in request.history] if request.history else None,
        )
        return result
    except Exception as e:
        print(f"Error in /api/query: {e}")
        return {"error": str(e), "type": "chat", "analysis": f"Error: {str(e)}"}


@app.get("/api/health")
async def health_check():
    """Detailed health check — tests if API keys are configured."""
    return {
        "anthropic_key_set": ANTHROPIC_API_KEY is not None,
        "polygon_key_set": POLYGON_API_KEY is not None,
    }