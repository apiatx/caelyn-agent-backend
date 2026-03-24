import os

# ── LangSmith tracing ──────────────────────────────────────────────────
# Setting these env vars enables automatic trace export to LangSmith.
# The API key should be stored in Replit Secrets as LANGSMITH_API_KEY.
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
os.environ.setdefault("LANGCHAIN_PROJECT", "CaelynAI")
if os.getenv("LANGSMITH_API_KEY"):
    os.environ.setdefault("LANGCHAIN_API_KEY", os.getenv("LANGSMITH_API_KEY"))

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")
AGENT_API_KEY = os.getenv("AGENT_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
CMC_API_KEY = os.getenv("CMC_API_KEY")
ALTFINS_API_KEY = os.getenv("ALTFINS_API_KEY")
XAI_API_KEY = os.getenv("XAI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY")
BRAVE_API_KEY = os.getenv("BRAVE_API_KEY")
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
PUBLIC_COM_API_KEY = os.getenv("PUBLIC_COM_API_KEY")
TRADIER_API_KEY = os.getenv("TRADIER_API_KEY")
TRADIER_SANDBOX = os.getenv("TRADIER_SANDBOX", "false").lower() in ("true", "1", "yes")
