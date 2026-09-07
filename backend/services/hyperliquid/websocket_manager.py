"""
Hyperliquid Screener — WebSocket consumer + boot sequence.

Boot sequence (fast path — sets is_ready in ~30-60s):
  1. REST: fetch metaAndAssetCtxs → initialize crypto perp assets
  2. REST: fetch spotMetaAndAssetCtxs → extend with spot assets
  2b. Disk: preload HIP-3 cache → stocks/commodities/pre-IPO available instantly
  3. REST: fetch allMids → patch mid prices
  4. REST: fetch 1h candles for top-40 assets → volatility/momentum
  5. REST: fetch 5m candles for top-20 assets → short-term vol/momentum
  6. REST: fetch L2 books for top-20 assets → book depth features
  7. Run full feature pass → compute all signals
  8. Mark state.is_ready = True  ← fast: crypto perps + HIP-3 from cache
  9. Connect WebSocket → subscribe to allMids + activeAssetCtx + bbo + trades

Post-boot enrichment (background, non-blocking):
 10. Load HIP-3 DEX universes fresh from API + save to disk cache
 11. Load 1d candles for top-50 crypto perps (TSMOM signals)
 12. Run feature pass to include refreshed HIP-3 assets

Background tasks (continuous):
 13. Periodic candle refresh (every 5 min) — refreshes 1h, 5m, 1d
 14. Periodic feature recompute (every 60s)
 15. Periodic universe refresh (every 5 min) — reconciles perps, spot, and HIP-3
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Optional

import websockets
import websockets.exceptions

from .client import HyperliquidRestClient
from .feature_engine import run_full_feature_pass
from .models import ScreenerAsset
from .normalizer import (
    build_hip3_universe,
    build_perp_universe,
    build_spot_universe,
    patch_from_active_asset_ctx,
    patch_from_all_mids,
    patch_from_bbo,
    patch_from_l2,
    patch_trade_flow,
)
from .state import HyperliquidState

_WS_URL = "wss://api.hyperliquid.xyz/ws"

# ── Disk cache for HIP-3 assets ───────────────────────────────────────────────
_HIP3_CACHE_PATH = Path(__file__).parent.parent.parent / "data" / "hyperliquid_hip3_cache.json"
_HIP3_CACHE_MAX_AGE_S = 86400   # 24 hours — still useful even if stale; API refresh overwrites

# ── Disk snapshot for OI history (enables OI Δ on first cycle after restart) ──
_SIGNAL_SNAPSHOT_PATH = Path(__file__).parent.parent.parent / "data" / "hyperliquid_signal_snapshots.json"
_SIGNAL_SNAPSHOT_MAX_AGE_S = 7200   # discard if older than 2h — history would be useless


def _save_signal_snapshots(state: HyperliquidState) -> None:
    """
    Persist per-coin OI history snapshots to disk.

    Saves up to the last 10 (ts, oi_usd) pairs per coin from state.oi_history.
    On next restart, _load_signal_snapshots() restores these into state.oi_history
    so OI Δ fields (oi_delta_5m/15m/1h) become available after the first 60s cycle
    rather than requiring an hour of warm-up.

    Atomic write: .tmp → replace to prevent corrupt reads on crash.
    """
    try:
        snapshots: dict = {}
        for coin, history in state.oi_history.items():
            snaps = list(history)
            if not snaps:
                continue
            snapshots[coin] = [
                {"ts": ts, "oi_usd": oi_usd}
                for ts, oi_usd in snaps[-10:]   # last 10 ≈ ~10 min at 60s cadence
            ]
        if not snapshots:
            return
        _SIGNAL_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {"saved_at": time.time(), "coin_count": len(snapshots), "snapshots": snapshots}
        tmp = _SIGNAL_SNAPSHOT_PATH.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(payload, f)
        tmp.replace(_SIGNAL_SNAPSHOT_PATH)
    except Exception as exc:
        print(f"[HL][signal_snapshots] Save error: {exc}")


def _load_signal_snapshots(state: HyperliquidState) -> int:
    """
    Restore per-coin OI history from disk into state.oi_history.

    Called during the boot sequence (before the initial feature pass) so that
    _compute_oi_changes() can immediately produce OI Δ values on the first 60s cycle.
    Returns the number of coins whose histories were restored.
    Snapshots older than _SIGNAL_SNAPSHOT_MAX_AGE_S are silently discarded.
    """
    try:
        if not _SIGNAL_SNAPSHOT_PATH.exists():
            return 0
        with open(_SIGNAL_SNAPSHOT_PATH) as f:
            data = json.load(f)
        age_s = time.time() - data.get("saved_at", 0)
        if age_s > _SIGNAL_SNAPSHOT_MAX_AGE_S:
            print(f"[HL][signal_snapshots] Snapshot too old ({age_s / 3600:.1f}h), discarding")
            return 0
        snapshots = data.get("snapshots", {})
        restored = 0
        for coin, snaps in snapshots.items():
            for entry in snaps:
                ts     = entry.get("ts")
                oi_usd = entry.get("oi_usd")
                if ts and oi_usd:
                    state.oi_history[coin].append((ts, oi_usd))
            if snaps:
                restored += 1
        print(
            f"[HL][signal_snapshots] Restored OI history for {restored} coins "
            f"({sum(len(s) for s in snapshots.values())} points, age={age_s / 60:.1f} min)"
        )
        return restored
    except Exception as exc:
        print(f"[HL][signal_snapshots] Load error: {exc}")
        return 0


def _save_hip3_cache(state: HyperliquidState) -> None:
    """Persist all HIP-3 assets (coin contains ':') to disk after each enrich cycle."""
    try:
        hip3_assets = {
            coin: asset.model_dump()
            for coin, asset in state.assets.items()
            if ":" in coin
        }
        if not hip3_assets:
            return
        _HIP3_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "saved_at": time.time(),
            "asset_count": len(hip3_assets),
            "assets": hip3_assets,
        }
        tmp = _HIP3_CACHE_PATH.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(payload, f)
        tmp.replace(_HIP3_CACHE_PATH)
        print(f"[HL][hip3_cache] Saved {len(hip3_assets)} HIP-3 assets to disk")
    except Exception as exc:
        print(f"[HL][hip3_cache] Save error: {exc}")


def _preload_hip3_cache(state: HyperliquidState) -> int:
    """
    Load HIP-3 assets from disk if the cache exists and is < 24 h old.
    Returns the number of assets loaded (0 if cache missing/stale/error).
    Must be called after perp + spot universe is built so we don't overwrite crypto.
    """
    try:
        if not _HIP3_CACHE_PATH.exists():
            print("[HL][hip3_cache] No disk cache found — HIP-3 will load from API after boot")
            return 0
        with open(_HIP3_CACHE_PATH) as f:
            payload = json.load(f)
        age_s = time.time() - payload.get("saved_at", 0)
        if age_s > _HIP3_CACHE_MAX_AGE_S:
            print(f"[HL][hip3_cache] Cache too old ({age_s / 3600:.1f}h) — will refresh from API")
            return 0
        assets_raw = payload.get("assets", {})
        count = 0
        for coin, data in assets_raw.items():
            if ":" not in coin:
                continue   # safety: only accept HIP-3 prefixed coins
            if coin in state.assets:
                continue   # never overwrite a crypto perp
            try:
                state.assets[coin] = ScreenerAsset(**data)
                state.universe_allowlist.add(coin)
                count += 1
            except Exception:
                pass
        print(f"[HL][hip3_cache] Preloaded {count} HIP-3 assets from disk (age={age_s / 60:.1f} min)")
        return count
    except Exception as exc:
        print(f"[HL][hip3_cache] Load error: {exc}")
        return 0

# Subscription thresholds
_CTX_SUBS   = 50   # activeAssetCtx subscriptions (top N by OI)
_BBO_SUBS   = 30   # BBO subscriptions
_TRADE_SUBS = 30   # trades subscriptions

# Reconnect backoff
_RECONNECT_MIN_S = 3.0
_RECONNECT_MAX_S = 60.0

# Heartbeat interval
_PING_INTERVAL_S = 20.0

_shutdown = False


async def boot_and_run(state: HyperliquidState):
    """
    Top-level background task entry point.
    Runs the boot sequence, then starts the WebSocket consumer
    and periodic background tasks concurrently.
    """
    client = HyperliquidRestClient()
    try:
        print("[HL] Starting boot sequence...")
        await _boot_sequence(state, client)
        print(f"[HL] Boot complete — {len(state.assets)} assets ready. Starting WS...")
        state.is_ready = True
        state.boot_ts = time.time()

        # Run all long-lived tasks concurrently.
        # _post_boot_enrich loads fresh HIP-3 data from API + saves disk cache.
        # _periodic_hip3_refresh then keeps HIP-3 prices live every 5 min.
        await asyncio.gather(
            _ws_consumer(state),
            _periodic_candle_refresh(state, client),
            _periodic_feature_recompute(state),
            _post_boot_enrich(state, client),
            _periodic_hip3_refresh(state, client),
            _periodic_oi_cap_refresh(state, client),
            return_exceptions=True,
        )
    except Exception as e:
        print(f"[HL] boot_and_run fatal error: {e}")
    finally:
        await client.close()


# ─────────────────────────────────────────────────────────────────────────────
# Boot sequence
# ─────────────────────────────────────────────────────────────────────────────

async def _boot_sequence(state: HyperliquidState, client: HyperliquidRestClient):
    # 1. Perp universe
    print("[HL][boot] Fetching perp universe...")
    try:
        meta_ctxs = await client.get_meta_and_asset_ctxs()
        perp_assets = build_perp_universe(meta_ctxs)
        for coin, asset in perp_assets.items():
            state.assets[coin] = asset
            state.meta[coin] = {}
        # Build perp allowlist from admitted assets
        state.perp_allowlist = set(perp_assets.keys())
        state.universe_allowlist.update(state.perp_allowlist)
        print(f"[HL][boot] Loaded {len(perp_assets)} perp assets | allowlist size={len(state.perp_allowlist)}")
    except Exception as e:
        print(f"[HL][boot] Perp universe error: {e}")

    # 2. Spot universe
    print("[HL][boot] Fetching spot universe...")
    try:
        spot_ctxs = await client.get_spot_meta_and_asset_ctxs()
        spot_assets = build_spot_universe(spot_ctxs)
        for coin, asset in spot_assets.items():
            if coin not in state.assets:   # don't overwrite a perp with same name
                state.assets[coin] = asset
        # Build spot allowlist from canonical admitted assets only
        state.spot_allowlist = set(spot_assets.keys())
        state.universe_allowlist.update(state.spot_allowlist)
        print(f"[HL][boot] Loaded {len(spot_assets)} spot assets | universe total={len(state.universe_allowlist)}")
    except Exception as e:
        print(f"[HL][boot] Spot universe error: {e}")

    # 2b. Preload HIP-3 from disk cache — stocks/commodities/pre-IPO available before WS connects
    print("[HL][boot] Preloading HIP-3 disk cache...")
    hip3_cached = _preload_hip3_cache(state)
    if hip3_cached > 0:
        print(f"[HL][boot] HIP-3 disk cache: {hip3_cached} assets preloaded (full API refresh in background)")

    # 3. All mids
    try:
        mids = await client.get_all_mids()
        patch_from_all_mids(state, mids)
        print(f"[HL][boot] Patched mids for {len(mids)} coins")
    except Exception as e:
        print(f"[HL][boot] allMids error: {e}")

    # 4. 1h candles for top-40 by volume
    top40 = state.top_coins_by_volume(40)
    print(f"[HL][boot] Fetching 1h candles for {len(top40)} assets...")
    try:
        candles_1h = await client.get_candles_multi(top40, "1h", n_bars=50)
        for coin, bars in candles_1h.items():
            if bars:
                state.add_candles(coin, "1h", bars)
        print(f"[HL][boot] 1h candles loaded for {sum(1 for b in candles_1h.values() if b)} coins")
    except Exception as e:
        print(f"[HL][boot] 1h candle error: {e}")

    # 5. 5m candles for top-20
    top20 = top40[:20]
    print(f"[HL][boot] Fetching 5m candles for {len(top20)} assets...")
    try:
        candles_5m = await client.get_candles_multi(top20, "5m", n_bars=50)
        for coin, bars in candles_5m.items():
            if bars:
                state.add_candles(coin, "5m", bars)
    except Exception as e:
        print(f"[HL][boot] 5m candle error: {e}")

    # 5a. 4h candles for top-40 (used by Relative Strength signal module)
    print(f"[HL][boot] Fetching 4h candles for {len(top40)} assets...")
    try:
        candles_4h = await client.get_candles_multi(top40, "4h", n_bars=12)
        for coin, bars in candles_4h.items():
            if bars:
                state.add_candles(coin, "4h", bars)
        print(f"[HL][boot] 4h candles loaded for {sum(1 for b in candles_4h.values() if b)} coins")
    except Exception as e:
        print(f"[HL][boot] 4h candle error: {e}")

    # 5b. 1d candles for top-15 crypto perps — loaded at boot so TSMOM is
    #     ready immediately when is_ready flips True (no waiting for post-boot).
    top15_crypto = [c for c in top20 if ":" not in c][:15]
    print(f"[HL][boot] Fetching 1d candles for {len(top15_crypto)} TSMOM assets...")
    try:
        candles_1d_boot = await client.get_candles_multi(top15_crypto, "1d", n_bars=120)
        loaded_1d = sum(1 for bars in candles_1d_boot.values() if bars)
        for coin, bars in candles_1d_boot.items():
            if bars:
                state.add_candles(coin, "1d", bars)
        print(f"[HL][boot] 1d candles loaded for {loaded_1d} / {len(top15_crypto)} coins")
    except Exception as e:
        print(f"[HL][boot] 1d candle error: {e}")

    # 6. L2 books for top-20
    print(f"[HL][boot] Fetching L2 books for {len(top20)} assets...")
    try:
        books = await client.get_l2_books_multi(top20)
        for coin, book in books.items():
            levels = book.get("levels") or []
            if levels:
                patch_from_l2(state, coin, levels)
                state.set_book(coin, book)
    except Exception as e:
        print(f"[HL][boot] L2 books error: {e}")

    # 6b. OI caps for HIP-3 DEXes + main crypto perps at cap
    print("[HL][boot] Fetching OI caps...")
    await _refresh_oi_caps(state, client)

    # 6c. Restore OI history from disk — enables OI Δ on first 60s cycle after restart
    print("[HL][boot] Loading signal snapshots...")
    n_restored = _load_signal_snapshots(state)
    if n_restored > 0:
        print(f"[HL][boot] OI history restored for {n_restored} coins — OI Δ ready on first cycle")

    # 7. Initial feature pass
    print("[HL][boot] Running feature pass...")
    n = run_full_feature_pass(state)
    print(f"[HL][boot] Features computed for {n} assets")


# ─────────────────────────────────────────────────────────────────────────────
# OI cap data (HIP-3 perpDexLimits + perpsAtOpenInterestCap)
# ─────────────────────────────────────────────────────────────────────────────

# Known HIP-3 DEX prefixes to query for OI caps
_HIP3_DEX_PREFIXES = ["xyz", "vntl", "km", "flx", "cash", "hyna", "abcd", "para"]


async def _refresh_oi_caps(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Fetch OI caps from two sources:
    1. perpDexLimits for each HIP-3 DEX → per-coin caps in USD notional
    2. perpsAtOpenInterestCap → list of main crypto perps at their cap
    Merges into state.oi_caps and state.perps_at_oi_cap.
    """
    caps: dict[str, float] = {}
    dex_defaults: dict[str, float] = {}  # dex prefix → default per-perp cap

    # 1. Fetch HIP-3 DEX limits concurrently
    results = await asyncio.gather(
        *[client.get_perp_dex_limits(dex) for dex in _HIP3_DEX_PREFIXES],
        return_exceptions=True,
    )
    for dex, result in zip(_HIP3_DEX_PREFIXES, results):
        if isinstance(result, Exception) or result is None:
            continue
        default_cap = float(result.get("oiSzCapPerPerp", 0))
        dex_defaults[dex] = default_cap
        for coin, cap_str in result.get("coinToOiCap", []):
            try:
                caps[coin] = float(cap_str)
            except (TypeError, ValueError):
                pass

    # For HIP-3 assets without a specific cap entry, use the DEX default
    for coin in state.assets:
        if ":" in coin and coin not in caps:
            prefix = coin.split(":")[0]
            if prefix in dex_defaults:
                caps[coin] = dex_defaults[prefix]

    # 2. Fetch main crypto perps at their OI cap
    try:
        at_cap = await client.get_perps_at_oi_cap()
        state.perps_at_oi_cap = set(at_cap)
    except Exception as e:
        print(f"[HL][oi_caps] perpsAtOpenInterestCap error: {e}")

    state.oi_caps = caps
    state.oi_caps_ts = time.time()
    print(
        f"[HL][oi_caps] Loaded {len(caps)} HIP-3 caps across {len(dex_defaults)} DEXes, "
        f"{len(state.perps_at_oi_cap)} main perps at cap"
    )


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket consumer
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Post-boot enrichment (runs once, non-blocking)
# ─────────────────────────────────────────────────────────────────────────────

async def _post_boot_enrich(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Non-blocking post-boot enrichment. Runs once after is_ready=True.
    HIP-3 DEX loading and extended 1d candle fetching run concurrently so
    TSMOM signals are not blocked waiting for HIP-3 DEX calls to finish.
    """
    await asyncio.sleep(3)  # let WS subscribe first
    print("[HL][enrich] Starting post-boot enrichment (HIP-3 + 1d candles in parallel)...")
    await asyncio.gather(
        _enrich_hip3(state, client),
        _enrich_1d_candles(state, client),
        return_exceptions=True,
    )
    print("[HL][enrich] Post-boot enrichment complete.")


async def _enrich_hip3(state: HyperliquidState, client: HyperliquidRestClient):
    """Load HIP-3 DEX assets (equity/commodity/index/pre-IPO perps)."""
    await _refresh_discovered_universe(state, client, include_main_spot=False)


def _minimal_hip3_asset(dex_prefix: str, asset_meta: dict) -> ScreenerAsset:
    """Represent active HIP-3 membership even when quote enrichment is absent."""
    coin = asset_meta["name"]
    stripped = coin.split(":", 1)[1] if ":" in coin else coin
    return ScreenerAsset(
        coin=coin,
        display_name=stripped,
        canonical_coin_id=coin,
        display_symbol=stripped,
        is_listed_on_hyperliquid=True,
        market_type="perp",
        dex=f"hl-{dex_prefix}",
        tags=["perp", "hip3"],
        max_leverage=asset_meta.get("maxLeverage"),
        only_isolated=asset_meta.get("onlyIsolated", False),
        sz_decimals=asset_meta.get("szDecimals", 0),
        market_status="active",
        last_updated_ts=time.time(),
    )


def _complete_hip3_membership(
    dex_prefix: str,
    meta_and_ctxs: list,
    existing: dict[str, ScreenerAsset],
) -> dict[str, ScreenerAsset]:
    """
    Build every non-delisted HIP-3 listing.

    Existing enriched rows survive a temporarily missing per-market context;
    brand-new listings receive a nullable membership row until enrichment arrives.
    """
    enriched = build_hip3_universe(dex_prefix, meta_and_ctxs)
    meta_block = meta_and_ctxs[0] if meta_and_ctxs else {}
    for asset_meta in meta_block.get("universe", []):
        coin = asset_meta.get("name", "")
        if not coin or asset_meta.get("isDelisted") or coin in enriched:
            continue
        prior = existing.get(coin)
        enriched[coin] = prior or _minimal_hip3_asset(dex_prefix, asset_meta)
    return enriched


def _validated_meta_ctxs(result) -> Optional[list]:
    """
    Return a normalized [metadata, contexts] discovery response, or None.

    Metadata-only responses are valid membership snapshots with nullable
    enrichment. Missing/malformed metadata is not authoritative and must not
    destructively replace the source's LKG.
    """
    if isinstance(result, Exception) or not isinstance(result, (list, tuple)) or not result:
        return None
    meta = result[0]
    if not isinstance(meta, dict) or not isinstance(meta.get("universe"), list):
        return None
    contexts = result[1] if len(result) > 1 and isinstance(result[1], list) else []
    return [meta, contexts]


async def _refresh_discovered_universe(
    state: HyperliquidState,
    client: HyperliquidRestClient,
    *,
    include_main_spot: bool = True,
):
    """
    Reconcile the current Hyperliquid listing metadata into canonical state.

    Main perp, spot, and each HIP-3 namespace reconcile independently. A failed
    source preserves that source's last-known-good membership, while a successful
    source removes listings no longer present or marked delisted.
    """
    try:
        discovery_calls = [client.get_all_perp_metas()]
        if include_main_spot:
            discovery_calls.extend([
                client.get_meta_and_asset_ctxs(),
                client.get_spot_meta_and_asset_ctxs(),
            ])
        discovered = await asyncio.gather(*discovery_calls, return_exceptions=True)
        all_metas = discovered[0]
        next_assets = dict(state.assets)
        hip3_prefixes: list[str] = []
        successful_prefixes: set[str] = set()
        valid_all_metas = (
            isinstance(all_metas, list)
            and bool(all_metas)
            and all(
                isinstance(block, dict) and isinstance(block.get("universe"), list)
                for block in all_metas
            )
        )
        if not valid_all_metas:
            print(f"[HL][universe_refresh] allPerpMetas error: {all_metas}")
        else:
            for dex_meta in all_metas[1:]:
                for asset_meta in dex_meta.get("universe", []):
                    name = asset_meta.get("name", "")
                    if ":" in name:
                        prefix = name.split(":", 1)[0]
                        if prefix not in hip3_prefixes:
                            hip3_prefixes.append(prefix)
                        break

            hip3_ctxs_list = await asyncio.gather(
                *[client.get_dex_meta_and_asset_ctxs(p) for p in hip3_prefixes],
                return_exceptions=True,
            )
            known_prefixes = {
                coin.split(":", 1)[0]
                for coin in next_assets
                if ":" in coin
            }
            removed_prefixes = known_prefixes - set(hip3_prefixes)
            for coin in [
                c for c in next_assets
                if ":" in c and c.split(":", 1)[0] in removed_prefixes
            ]:
                del next_assets[coin]

            for prefix, result in zip(hip3_prefixes, hip3_ctxs_list):
                valid_result = _validated_meta_ctxs(result)
                if valid_result is None:
                    print(f"[HL][universe_refresh] HIP-3 DEX '{prefix}' error: {result}")
                    continue
                successful_prefixes.add(prefix)
                for coin in [
                    c for c in next_assets
                    if c.startswith(f"{prefix}:")
                ]:
                    del next_assets[coin]
                next_assets.update(
                    _complete_hip3_membership(prefix, valid_result, state.assets)
                )

        if include_main_spot:
            main_result, spot_result = discovered[1], discovered[2]
            valid_main = _validated_meta_ctxs(main_result)
            if valid_main is None:
                print(f"[HL][universe_refresh] main perp error: {main_result}")
            else:
                main_assets = build_perp_universe(valid_main)
                for coin, asset in list(next_assets.items()):
                    if asset.market_type == "perp" and ":" not in coin:
                        del next_assets[coin]
                next_assets.update(main_assets)
                state.perp_allowlist = set(main_assets)

            valid_spot = _validated_meta_ctxs(spot_result)
            if valid_spot is None:
                print(f"[HL][universe_refresh] spot error: {spot_result}")
            else:
                spot_assets = build_spot_universe(valid_spot)
                for coin, asset in list(next_assets.items()):
                    if asset.market_type == "spot":
                        del next_assets[coin]
                for coin, asset in spot_assets.items():
                    if coin not in next_assets:
                        next_assets[coin] = asset
                state.spot_allowlist = set(spot_assets)

        state.assets = next_assets
        state.universe_allowlist = set(next_assets)
        state.lkg_assets = dict(next_assets)
        state.lkg_pass_ts = time.time()
        print(
            f"[HL][universe_refresh] canonical={len(next_assets)} "
            f"perp={len(state.perp_allowlist)} spot={len(state.spot_allowlist)} "
            f"hip3={sum(1 for c in next_assets if ':' in c)} "
            f"successful_hip3={len(successful_prefixes)}/{len(hip3_prefixes)}"
        )

        try:
            mids = await client.get_all_mids()
            patch_from_all_mids(state, mids)
        except Exception as exc:
            print(f"[HL][universe_refresh] allMids enrichment error: {exc}")
        try:
            run_full_feature_pass(state)
            print("[HL][enrich] Feature pass complete after universe refresh")
        except Exception as exc:
            print(f"[HL][universe_refresh] feature enrichment error: {exc}")

        # Persist to disk so next boot has HIP-3 immediately (no API wait)
        _save_hip3_cache(state)
    except Exception as e:
        print(f"[HL][enrich] HIP-3 error: {e}")


async def _enrich_1d_candles(state: HyperliquidState, client: HyperliquidRestClient):
    """Load extended 1d candles for top-50 crypto perps (TSMOM breadth)."""
    await asyncio.sleep(2)  # let boot mids settle
    try:
        tsmom_coins = [
            c for c in state.top_coins_by_volume(60)
            if ":" not in c
        ][:50]
        print(f"[HL][enrich] Fetching extended 1d candles for {len(tsmom_coins)} coins...")
        candles_1d = await client.get_candles_multi(tsmom_coins, "1d", n_bars=120)
        loaded = sum(1 for bars in candles_1d.values() if bars)
        for coin, bars in candles_1d.items():
            if bars:
                state.add_candles(coin, "1d", bars)
        print(f"[HL][enrich] Extended 1d candles: {loaded} / {len(tsmom_coins)} coins")
    except Exception as e:
        print(f"[HL][enrich] 1d candle error: {e}")



async def _ws_consumer(state: HyperliquidState):
    """
    Connect to Hyperliquid WebSocket, subscribe to live feeds,
    and process incoming messages indefinitely with auto-reconnect.
    """
    backoff = _RECONNECT_MIN_S
    while not _shutdown:
        try:
            print("[HL][ws] Connecting...")
            async with websockets.connect(
                _WS_URL,
                ping_interval=None,     # we handle pings manually
                open_timeout=15,
                close_timeout=10,
            ) as ws:
                state.ws_connected = True
                backoff = _RECONNECT_MIN_S
                print("[HL][ws] Connected. Subscribing...")
                await _subscribe_all(ws, state)
                print("[HL][ws] Subscriptions sent. Consuming messages...")

                ping_task = asyncio.create_task(_ping_loop(ws))
                try:
                    async for raw in ws:
                        await _handle_message(state, raw)
                finally:
                    ping_task.cancel()

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[HL][ws] Connection closed: {e}. Reconnecting in {backoff}s...")
        except Exception as e:
            print(f"[HL][ws] Error: {e}. Reconnecting in {backoff}s...")
        finally:
            state.ws_connected = False

        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, _RECONNECT_MAX_S)


async def _subscribe_all(ws, state: HyperliquidState):
    """Send all subscription requests on a fresh connection."""
    # 1. allMids — one sub covers every asset
    await _subscribe(ws, {"type": "allMids"})

    # 2. activeAssetCtx for top-N perps (funding, OI, mark updates)
    top_ctx = state.top_coins_by_oi(_CTX_SUBS)
    for coin in top_ctx:
        await _subscribe(ws, {"type": "activeAssetCtx", "coin": coin})

    # 3. BBO for top-N
    top_bbo = state.top_coins_by_volume(_BBO_SUBS)
    for coin in top_bbo:
        await _subscribe(ws, {"type": "bbo", "coin": coin})

    # 4. Trades for top-N
    top_trades = state.top_coins_by_volume(_TRADE_SUBS)
    for coin in top_trades:
        await _subscribe(ws, {"type": "trades", "coin": coin})


async def _subscribe(ws, subscription: dict):
    await ws.send(json.dumps({"method": "subscribe", "subscription": subscription}))


async def _ping_loop(ws):
    """Send periodic pings to keep the WS alive."""
    while True:
        await asyncio.sleep(_PING_INTERVAL_S)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            break


# ─────────────────────────────────────────────────────────────────────────────
# Message dispatch
# ─────────────────────────────────────────────────────────────────────────────

async def _handle_message(state: HyperliquidState, raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return

    channel = msg.get("channel", "")
    data    = msg.get("data", {})

    if channel == "allMids":
        mids = data.get("mids", {}) if isinstance(data, dict) else data
        if isinstance(mids, dict):
            patch_from_all_mids(state, mids)

    elif channel == "activeAssetCtx":
        coin = data.get("coin", "")
        ctx  = data.get("ctx", {})
        if coin and ctx:
            patch_from_active_asset_ctx(state, coin, ctx)

    elif channel == "bbo":
        # data = {"coin": "BTC", "data": {...}} or {"coin": "BTC", "bid": [...], ...}
        if isinstance(data, dict):
            coin = data.get("coin", "")
            inner = data.get("data", data)
            if coin and inner:
                patch_from_bbo(state, coin, inner)

    elif channel == "l2Book":
        coin   = data.get("coin", "")
        levels = data.get("levels", [])
        if coin and levels:
            patch_from_l2(state, coin, levels)

    elif channel == "trades":
        # data = list of trade dicts
        trades = data if isinstance(data, list) else []
        affected: set[str] = set()
        for trade in trades:
            coin = trade.get("coin", "")
            if coin:
                state.add_trades(coin, [trade])
                affected.add(coin)
        for coin in affected:
            patch_trade_flow(state, coin)

    elif channel == "candle":
        # data = {coin, interval, candle_data}
        if isinstance(data, dict):
            coin     = data.get("coin", "") or data.get("s", "")
            interval = data.get("interval", "") or data.get("i", "")
            candle   = data.get("data", data)
            if coin and interval and candle:
                state.upsert_candle(coin, interval, candle)

    elif channel in ("pong", "subscriptionResponse"):
        pass   # ignore heartbeats and ack messages

    # else: unknown channel — silently ignore


# ─────────────────────────────────────────────────────────────────────────────
# Periodic background tasks
# ─────────────────────────────────────────────────────────────────────────────

async def _periodic_candle_refresh(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Every 5 minutes: refresh 1h and 5m candles for the full top-40 universe.
    This keeps volatility and momentum features fresh even for assets without
    WS candle subscriptions.
    """
    while not _shutdown:
        await asyncio.sleep(300)
        if not state.assets:
            continue
        try:
            top40 = state.top_coins_by_volume(40)
            candles = await client.get_candles_multi(top40, "1h", n_bars=50)
            for coin, bars in candles.items():
                if bars:
                    state.add_candles(coin, "1h", bars)

            top20 = top40[:20]
            candles5 = await client.get_candles_multi(top20, "5m", n_bars=50)
            for coin, bars in candles5.items():
                if bars:
                    state.add_candles(coin, "5m", bars)

            # Refresh 4h candles for Relative Strength signals
            candles4h = await client.get_candles_multi(top40, "4h", n_bars=12)
            for coin, bars in candles4h.items():
                if bars:
                    state.add_candles(coin, "4h", bars)

            # Refresh L2 books for Order Book Pressure signals
            books = await client.get_l2_books_multi(top20)
            for coin, book in books.items():
                levels = book.get("levels") or []
                if levels:
                    patch_from_l2(state, coin, levels)
                    state.set_book(coin, book)

            # Refresh 1d candles for TSMOM
            tsmom_coins = [c for c in top40 if ":" not in c][:50]
            candles1d = await client.get_candles_multi(tsmom_coins, "1d", n_bars=120)
            for coin, bars in candles1d.items():
                if bars:
                    state.add_candles(coin, "1d", bars)
        except Exception as e:
            print(f"[HL][candle_refresh] Error: {e}")


async def _periodic_feature_recompute(state: HyperliquidState):
    """
    Every 60 seconds: save OI snapshots, compute OI changes, recompute all features.
    This ensures composite scores, percentile ranks, and flags stay current
    even for assets that haven't received a live WS update recently.
    """
    while not _shutdown:
        await asyncio.sleep(60)
        if not state.is_ready:
            continue
        try:
            _save_oi_snapshots(state)
            _compute_oi_changes(state)
            run_full_feature_pass(state)
            _save_score_snapshots(state)
            _save_signal_snapshots(state)   # persist OI history for fast recovery after restart
        except Exception as e:
            print(f"[HL][feature_recompute] Error: {e}")


async def _periodic_oi_cap_refresh(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Every 10 minutes: refresh OI cap data from HIP-3 DEX limits and main perp caps.
    OI caps change infrequently so a 10-min interval is sufficient.
    Waits 5 minutes initially to avoid overlapping with boot sequence.
    """
    await asyncio.sleep(300)   # 5 min head-start
    while not _shutdown:
        if state.is_ready:
            try:
                await _refresh_oi_caps(state, client)
            except Exception as exc:
                print(f"[HL][oi_cap_periodic] Error: {exc}")
        await asyncio.sleep(600)   # 10 minutes


async def _periodic_hip3_refresh(state: HyperliquidState, client: HyperliquidRestClient):
    """
    Every 5 minutes: reconcile main perp, spot, and HIP-3 membership and prices.
    This keeps the complete canonical universe current and ensures the HIP-3
    cache is warm for instant availability on the next server restart.
    Waits 8 minutes initially to avoid overlapping with _post_boot_enrich.
    """
    await asyncio.sleep(480)   # 8 min head-start for post_boot_enrich to finish first
    while not _shutdown:
        if state.is_ready:
            try:
                await _refresh_discovered_universe(state, client)
            except Exception as exc:
                print(f"[HL][hip3_periodic] Error: {exc}")
        await asyncio.sleep(300)   # 5 minutes


def _save_oi_snapshots(state: HyperliquidState):
    """Record current OI for all perp assets for change computation."""
    now = time.time()
    for asset in state.perp_assets():
        if asset.open_interest_usd:
            state.oi_history[asset.coin].append((now, asset.open_interest_usd))


def _compute_oi_changes(state: HyperliquidState):
    """Compute 5m, 15m, and 1h OI changes from stored history and patch assets."""
    now = time.time()
    for coin, history in state.oi_history.items():
        asset = state.get_asset(coin)
        if asset is None:
            continue
        snaps = list(history)
        if not snaps:
            continue
        current_oi = asset.open_interest_usd
        if not current_oi:
            continue

        def _find(lo, hi):
            return next((s for s in reversed(snaps) if lo <= (now - s[0]) <= hi), None)

        snap_5m  = _find(270,  450)    # ~5 min ago
        snap_15m = _find(810, 1200)    # ~15 min ago
        snap_1h  = _find(3300, 4500)   # ~1 hour ago

        def pct(new, old):
            return round((new - old) / old, 6) if old and old != 0 else None

        patch = {}
        if (v := pct(current_oi, snap_5m[1]  if snap_5m  else None)) is not None: patch["oi_change_5m"]  = v
        if (v := pct(current_oi, snap_15m[1] if snap_15m else None)) is not None: patch["oi_change_15m"] = v
        if (v := pct(current_oi, snap_1h[1]  if snap_1h  else None)) is not None: patch["oi_change_1h"]  = v

        if patch:
            state.assets[coin] = asset.model_copy(update=patch)


def _save_score_snapshots(state: HyperliquidState):
    """Record current composite score for all assets and compute score_change."""
    now = time.time()
    for coin, asset in state.assets.items():
        score = asset.composite_signal_score
        if score is None:
            continue
        hist = state.score_history[coin]
        # Compute score_change vs prior snapshot
        score_change = None
        if hist:
            last_ts, last_score = hist[-1]
            if now - last_ts >= 50:  # at least 50s between snapshots
                score_change = round(score - last_score, 2)
                hist.append((now, score))
        else:
            hist.append((now, score))
            score_change = None

        if score_change is not None:
            state.assets[coin] = asset.model_copy(update={"score_change": score_change})
