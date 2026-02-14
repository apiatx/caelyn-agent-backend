from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
    allow_origins=[
        "https://your-website.yourusername.repl.co",  # Your Replit website URL
        "https://yourcustomdomain.com",  # Your custom domain if you have one
        "http://localhost:3000",  # For local testing
    ],
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


@app.post("/api/query")
async def handle_query(request: dict):
    """
    Main endpoint. Your frontend sends POST requests here.
    Body: {"prompt": "best trades today"}
    Returns: {"type": "screener|analysis|chat", "analysis": "...", "data": [...]}
    """
    prompt = request.get("prompt", "")

    if not prompt:
        return {"error": "No prompt provided"}

    try:
        result = await agent.query(prompt)
        return result
    except Exception as e:
        print(f"Agent error: {e}")
        return {
            "type": "chat",
            "analysis": (
                "Sorry, I encountered an error processing your request. "
                "Please try again."
            ),
            "data": None,
            "tickers": None,
        }


@app.get("/api/health")
async def health_check():
    """Detailed health check — tests if API keys are configured."""
    return {
        "anthropic_key_set": ANTHROPIC_API_KEY is not None,
        "polygon_key_set": POLYGON_API_KEY is not None,
    }