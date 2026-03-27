"""
DeFiLlama provider — free public API, no key required.
Provides: global DeFi TVL, top protocols, chain TVL flows, DEX volumes, stablecoin data.
"""
import asyncio
import httpx
from data.cache import TTLCache

_cache = TTLCache()
_TTL = 300  # 5-minute cache

BASE = "https://api.llama.fi"
STABLE_BASE = "https://stablecoins.llama.fi"


class DeFiLlamaProvider:
    TIMEOUT = 12.0

    async def _get(self, url: str) -> dict | list | None:
        try:
            async with httpx.AsyncClient(timeout=self.TIMEOUT) as client:
                r = await client.get(url, headers={"Accept": "application/json"})
            if r.status_code == 200:
                return r.json()
            print(f"[DEFILLAMA] HTTP {r.status_code} for {url}")
            return None
        except Exception as e:
            print(f"[DEFILLAMA] Error fetching {url}: {e}")
            return None

    async def get_defi_overview(self) -> dict:
        cached = _cache.get("defillama:overview")
        if cached is not None:
            return cached

        protocols_task = asyncio.create_task(self._get(f"{BASE}/protocols"))
        chains_task = asyncio.create_task(self._get(f"{BASE}/v2/chains"))
        dex_task = asyncio.create_task(
            self._get(f"{BASE}/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true")
        )
        stable_task = asyncio.create_task(
            self._get(f"{STABLE_BASE}/stablecoins?includePrices=true")
        )

        protocols_raw, chains_raw, dex_raw, stable_raw = await asyncio.gather(
            protocols_task, chains_task, dex_task, stable_task,
            return_exceptions=True,
        )

        result: dict = {}

        if isinstance(protocols_raw, list) and protocols_raw:
            sorted_protos = sorted(
                [p for p in protocols_raw if isinstance(p, dict) and p.get("tvl")],
                key=lambda p: p.get("tvl", 0), reverse=True,
            )
            result["total_tvl_usd"] = sum(p.get("tvl", 0) for p in sorted_protos)
            result["top_protocols"] = [
                {
                    "name": p.get("name"),
                    "symbol": p.get("symbol"),
                    "category": p.get("category"),
                    "chain": p.get("chain"),
                    "tvl": p.get("tvl"),
                    "change_1d": p.get("change_1d"),
                    "change_7d": p.get("change_7d"),
                }
                for p in sorted_protos[:12]
            ]

        if isinstance(chains_raw, list) and chains_raw:
            sorted_chains = sorted(
                [c for c in chains_raw if isinstance(c, dict) and c.get("tvl")],
                key=lambda c: c.get("tvl", 0), reverse=True,
            )
            result["top_chains"] = [
                {
                    "name": c.get("name"),
                    "tvl": c.get("tvl"),
                    "tokenSymbol": c.get("tokenSymbol"),
                }
                for c in sorted_chains[:8]
            ]

        if isinstance(dex_raw, dict):
            result["dex_volume_24h"] = dex_raw.get("total24h")
            result["dex_volume_7d"] = dex_raw.get("total7d")
            result["dex_change_1d"] = dex_raw.get("change_1d")
            top_dexs = dex_raw.get("protocols") or []
            result["top_dexs"] = [
                {
                    "name": d.get("name"),
                    "volume_24h": d.get("total24h"),
                    "change_1d": d.get("change_1d"),
                    "chain": d.get("chain"),
                }
                for d in sorted(top_dexs, key=lambda x: x.get("total24h") or 0, reverse=True)[:6]
                if isinstance(d, dict) and d.get("total24h")
            ]

        if isinstance(stable_raw, dict):
            pegs = stable_raw.get("peggedAssets") or []
            total_mcap = sum(
                (p.get("circulating", {}) or {}).get("peggedUSD", 0) or 0
                for p in pegs if isinstance(p, dict)
            )
            result["stablecoin_total_mcap"] = total_mcap
            sorted_stable = sorted(
                [p for p in pegs if isinstance(p, dict)],
                key=lambda p: (p.get("circulating", {}) or {}).get("peggedUSD", 0) or 0,
                reverse=True,
            )
            result["top_stablecoins"] = [
                {
                    "name": s.get("name"),
                    "symbol": s.get("symbol"),
                    "mcap": (s.get("circulating", {}) or {}).get("peggedUSD"),
                    "change_24h": s.get("change_24h"),
                    "peg_type": s.get("pegType"),
                }
                for s in sorted_stable[:6]
            ]

        print(
            f"[DEFILLAMA] overview: total_tvl=${result.get('total_tvl_usd', 0)/1e9:.1f}B, "
            f"protocols={len(result.get('top_protocols', []))}, "
            f"chains={len(result.get('top_chains', []))}, "
            f"stable_mcap=${result.get('stablecoin_total_mcap', 0)/1e9:.1f}B"
        )

        _cache.set("defillama:overview", result, _TTL)
        return result
