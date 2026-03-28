"""
Quick test: Does the XAI API + x_search actually work?
Run this on Replit: python test_xai.py
"""
import asyncio
import os
import json
import sys
from datetime import datetime, timedelta

async def test_xai():
    api_key = os.getenv("XAI_API_KEY")
    if not api_key:
        print("ERROR: XAI_API_KEY not set!")
        sys.exit(1)

    print(f"XAI_API_KEY: {api_key[:8]}...{api_key[-4:]} (length: {len(api_key)})")

    from data.xai_sentiment_provider import XAISentimentProvider
    xai = XAISentimentProvider(api_key)

    print(f"\nModels:")
    print(f"  Fast (default): {xai.model}")
    print(f"  Deep (reasoning): {xai.deep_model}")

    # Test 1: Fast model, basic x_search
    print("\n" + "="*60)
    print("TEST 1: Fast model - single ticker ($IREN)")
    print("="*60)
    try:
        result = await asyncio.wait_for(
            xai._call_grok_with_x_search(
                "Search X for recent posts about $IREN stock. What are people saying?",
                timeout=30.0,
                raw_mode=True,
            ),
            timeout=35.0,
        )
        if isinstance(result, dict) and "error" in result:
            print(f"ERROR: {result['error']}")
            if "detail" in result:
                print(f"DETAIL: {result['detail'][:300]}")
        elif isinstance(result, dict) and "_raw_analysis" in result:
            text = result["_raw_analysis"]
            print(f"SUCCESS! Got {len(text)} chars")
            print(f"Preview: {text[:500]}")
        else:
            print(f"Unexpected result: {result}")
    except Exception as e:
        print(f"EXCEPTION: {type(e).__name__}: {e}")

    # Test 2: Reasoning model with from_date
    print("\n" + "="*60)
    print("TEST 2: Reasoning model + from_date ($IONQ, $RKLB, $ASTS)")
    print("="*60)
    from_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        result = await asyncio.wait_for(
            xai._call_grok_with_x_search(
                "Search X for recent posts about $IONQ, $RKLB, and $ASTS. "
                "For each ticker, tell me: How much buzz? What are people saying? "
                "Any specific catalysts being discussed? Be specific.",
                timeout=120.0,
                raw_mode=True,
                use_deep_model=True,
                x_search_config={"from_date": from_date},
            ),
            timeout=125.0,
        )
        if isinstance(result, dict) and "error" in result:
            print(f"ERROR: {result['error']}")
            if "detail" in result:
                print(f"DETAIL: {result['detail'][:500]}")
        elif isinstance(result, dict) and "_raw_analysis" in result:
            text = result["_raw_analysis"]
            print(f"SUCCESS! Got {len(text)} chars")
            print(f"Preview: {text[:1000]}")
        else:
            print(f"Unexpected result: {str(result)[:500]}")
    except Exception as e:
        print(f"EXCEPTION: {type(e).__name__}: {e}")

    # Test 3: JSON mode (structured sentiment)
    print("\n" + "="*60)
    print("TEST 3: JSON mode - structured sentiment ($IREN)")
    print("="*60)
    try:
        result = await asyncio.wait_for(
            xai.get_ticker_sentiment("IREN"),
            timeout=45.0,
        )
        if isinstance(result, dict) and "error" in result:
            print(f"ERROR: {result['error']}")
            if "detail" in result:
                print(f"DETAIL: {result['detail'][:300]}")
        else:
            print(f"SUCCESS! Keys: {list(result.keys())}")
            print(f"Result: {json.dumps(result, indent=2, default=str)[:800]}")
    except Exception as e:
        print(f"EXCEPTION: {type(e).__name__}: {e}")

    print("\n" + "="*60)
    print("DONE")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(test_xai())
