# CONFLUENCE AUDIT — CODEBASE-GROUNDED FINDINGS
*Audit date: 2026-07-08. Zero code changes made. Read-only.*

---

## CRITICAL ARCHITECTURAL FINDING (read before Part A)

The terms **"Golden Zone," "High Conviction Trade Zone,"** and **"High Conviction Investment Zone"** do **not exist** as separately coded scoring engines anywhere in the codebase. There are no functions, classes, Neon tables, routes, or LKG files named for these zones. They are **section labels** in the AI synthesis output produced by `backend/services/watchlist_analysis.py → run_analysis_pipeline()`. The synthesis sections are: `best_entries`, `momentum_plays`, `catalyst_watch`, `sector_rotation`, `high_conviction`, `contrarian_value`. The user-visible section *titles* (including the zone names) are LLM-generated strings embedded in the Neon-persisted analysis store.

This means Parts A, B, C treat the Watchlist Zone systems as the LLM pipeline that produces them, not as independent algorithmic engines.

---

## PART A — SYSTEM TRACE

### SYSTEM 1 — Daily Alpha Board

**A. Frontend section:** Home page "Daily Alpha Board" panel

**B. Backend endpoints:**
- `GET /api/home/daily-alpha-board`
- `GET /api/home/daily-alpha-board/diagnostics`

**C. Route file:** `backend/main.py` (direct `@app.get` — not in a router file)

**D. Main service/function:** `backend/services/daily_alpha_board_service.py`
- Entry: `build_daily_alpha_board_safe(...)` (LKG-fallback wrapper)
- Core: `build_daily_alpha_board(...)`

**E. Exact call chain:**
```
GET /api/home/daily-alpha-board
  → build_daily_alpha_board_safe()
      → checks _BOARD_CACHE (TTL: 60s market hours / 300s off-hours)
          HIT  → return cached result
          MISS → build_daily_alpha_board()
                  → 8 synchronous collector calls (all cache/disk reads, zero provider calls):
                      collect_watchlist_cache_candidates()       → Neon analysis.sections rows
                      collect_portfolio_cache_candidates()       → Neon portfolio_holdings
                      collect_social_screener_cache_candidates() → x_consensus_weekly.json + social LKG
                      collect_themes_cache_candidates()          → themes_rs_lkg.json
                      collect_strategy_cache_candidates()        → strategy_screener_lkg.json
                      collect_catalyst_cache_candidates()        → earnings_snap_*.json (glob)
                      collect_options_cache_candidates()         → options_master_lkg_v1.json (+ fallbacks)
                      collect_hyperliquid_cache_candidates()     → hyperliquid_signal_snapshots.json
                      collect_macro_cache_context()              → regime:current_v1 in-memory TTLCache
                  → _merge_candidates(all_raw)          → deduplicate; union signals across sources
                  → _set_stock_long_fields(c) each      → enforce long bias; set entry_quality
                  → _apply_filters(...)                 → asset type / scope filters
                  → _score_candidate(c, regime) each    → weighted normalized score 0–100
                  → sort by score desc; timing_signal candidates fill top-N first
                  → cap at top-N (default 10 ideas)
                  → store in _BOARD_CACHE
                  → return {"ideas": [...], "meta": {...}}
```

**F. Cache/LKG/Neon sources:**
- In-memory `_BOARD_CACHE` (TTL 60s/300s)
- Neon: watchlist store (`load_watchlist()`), portfolio_holdings (portfolio manager)
- Disk JSON: `strategy_screener_lkg.json`, `themes_rs_lkg.json`, `hyperliquid_signal_snapshots.json`, `x_consensus_weekly.json`, `options_master_lkg_v1.json`, `options_lkg_v1_large_cap.json`, `options_lkg_v1_small_cap.json`, `earnings_snap_*.json`
- In-memory TTLCache: `regime:current_v1`

**G. Refresh cadence:** Request-triggered with TTL cache. No dedicated background refresh loop. The 8 source files are refreshed by their own separate background services.

**H. Calculation type:** Request-time (TTL-cached)

**I. History/snapshots required:** NO — point-in-time only

**J. Fields returned:**
```
ideas[]:
  symbol, name, asset_type, theme, sector
  score (0–100), confidence (high/medium/low), status, has_timing_signal
  direction (long/watch), timeframe, setup_type, summary, trigger, invalidation
  signals: { ta, fundamentals, catalysts, social, news, options, theme, macro,
             hyperliquid, momentum, rel_volume }
  evidence: [str], risks: [str]
  source_pages: [str]
  updated_at, long_bias
  entry_quality, preferred_action, extension_risk
  catalyst_window, days_to_earnings, earnings_result, eps_surprise_pct
  setup_bucket, timing_quality
  [crypto-only] hyperliquid_quality_gate, tsm_quality, matrix_signal
meta: { sources, build_ms, regime, ... }
```

---

### EXACT INPUT SIGNALS — DAILY ALPHA BOARD (STOCKS)

**Weight table (`_STOCK_WEIGHTS`):**

| Signal slot | Weight |
|---|---|
| theme | 0.20 |
| ta | 0.18 |
| rel_volume | 0.15 |
| catalyst | 0.12 |
| options | 0.10 |
| social | 0.10 |
| news | 0.08 |
| fundamentals | 0.05 |
| relevance (macro) | 0.02 |

**Weight table (`_CRYPTO_WEIGHTS`):**

| Signal slot | Weight |
|---|---|
| momentum | 0.25 |
| oi | 0.18 |
| volume_velocity | 0.15 |
| funding_quality | 0.12 |
| liquidation | 0.10 |
| volatility_expansion | 0.08 |
| macro | 0.07 |
| social_news | 0.05 |

**Full signal detail table (stocks):**

| Signal | Returned Field | Internal Var | Source Provider | Source File/Cache | Formula | Weight | Gate | Missing-Data Behavior | Can +? | Can −? | Display Only? |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Theme RS | `signals.theme` | `theme_sig` | theme_rs_service | `themes_rs_lkg.json` | `rs_score/100 * staleness_factor` | 0.20 | staleness_factor>0 | Excluded from normalization | YES | NO | NO |
| Technical/Stage | `signals.ta` | `ta_sig` | watchlist_stage2_service | Neon watchlist store | `technical_score/100`; fallback: action-label heuristic (0.25–0.82); stage modifier: S2 +0.12, S1 +0.06, S3 −0.10, S4 −0.20 | 0.18 | None | Excluded | YES | Via stage modifier | NO |
| Relative Volume | `signals.rel_volume` | `rel_vol_sig` | Neon watchlist store / social LKG | Neon + `x_consensus_weekly.json` | `vol_ratio/4.0` OR `vol_mc_pct/0.30` (clamped 0–1) | 0.15 | None | Excluded | YES | NO | NO |
| Catalyst/Earnings | `signals.catalysts` | `cat_sig` | earnings_snap_*.json | Disk JSON | Earnings within [−10, +21] days; score from snapshot | 0.12 | None | Excluded | YES | NO | NO |
| Options | `signals.options` | `opt_sig` | Options master LKG | `options_master_lkg_v1.json` | `composite_score/100 * staleness_factor` | 0.10 | staleness_factor>0 | Excluded | YES | NO | NO |
| Social | `signals.social` | `social_sig` | x_consensus_cache / watchlist | `x_consensus_weekly.json` | `consensus_score/100` OR `rs_score/100` | 0.10 | None | Excluded | YES | NO | NO |
| News | `signals.news` | `news_sig` | news_signal_scorer | Neon watchlist store | `major_news_score/100` (deterministic keyword-rule engine) | 0.08 | None | Excluded | YES | NO | NO |
| Fundamentals | `signals.fundamentals` | `fund_sig` | Claude/CSV LLM analysis | Neon watchlist store | Normalized 0–1 from LLM section data | 0.05 | None | Excluded | YES | NO | NO |
| Macro/Regime | `signals.macro` | `regime_sig` | regime_engine | `regime:current_v1` TTLCache | Regime alignment 0–1 | 0.02 | None | Excluded | YES | NO | NO |

**Score formula:**
```python
# Available-weight normalization
avail_weight = sum(weights[k] for k in present)
raw   = sum(float(sig_map[k]) * weights[k] for k in present)
score_01  = raw / avail_weight          # 0–1
score_100 = score_01 * 100             # 0–100 on candidate["score"]
```

**Cross-cutting score modifiers (not signal slots):**

| Modifier | Condition | Formula |
|---|---|---|
| Regime risk_on + crypto | label=="risk_on" | `score *= (1 + 0.06 * conf)` |
| Regime risk_on + themed stock | label=="risk_on" and theme | `score *= (1 + 0.04 * conf)` |
| Regime risk_off + crypto | label=="risk_off" | `score *= (1 − 0.08 * conf)` |
| Regime risk_off + extended stock | label=="risk_off" and status=="extended" | `score *= (1 − 0.05 * conf)` |
| Major macro event + no catalyst | label=="major_macro_event_soon" and catalysts==None | `score *= 0.92` |
| RS boost in weak market | _rs_boost==True and label in (risk_off, neutral) | `score *= 1.05` |
| Theme-only guard | Only source="themes", no external timing | `score *= 0.85`; forced watch_only |
| Staleness | Data age | <2h→1.0, 2–12h→0.90, 12–48h→0.70, >48h→0.0 (hard exclude) |
| Watchlist staleness floor | Watchlist source only | `max(staleness_factor, 0.55)` — never fully excluded |

**Crypto anti-pump modifiers:**
- OI spike >15% in 5m without TSM: `penalty *= 0.58`
- OI spike >20% in 15m without TSM: `penalty *= 0.55`
- 24h move >20% without structural quality: `penalty *= 0.65`
- Pump detected AND accumulated penalty < 0.40: hard exclude
- Crowded funding (no TSM): `penalty *= 0.80`
- Exhaustion signal for long: `penalty *= 0.75`
- Signal cap if OI-only fallback: `_OI_ONLY_MAX_SIGNAL = 0.68` (~69/100 max score)

**Confidence thresholds:**
- `has_timing_signal=False`: max "medium" (≥50) or "low" (<50)
- `has_timing_signal=True` + score≥72 → "high"
- `has_timing_signal=True` + score≥50 → "medium"
- `has_timing_signal=True` + score<50 → "low"

---

### SYSTEMS 2–4 — Golden Zone / HC Trade Zone / HC Investment Zone

**A. Frontend sections:** Named sections within the Watchlist analysis panel. Section titles are LLM-generated strings — they are NOT hardcoded enum values in the backend.

**B. Backend endpoints:**
- Trigger analysis: `POST /api/watchlist/strategy-report/generate`
- Read report: `GET /api/watchlist/strategy-report/{report_id}`
- Report history: `GET /api/watchlist/strategy-report/history`
- Live section data: also embedded in Neon watchlist store; served on watchlist GET path (enriched on-request by `_enrich_store_with_quotes()`)

**C. Route file:** `backend/services/watchlist_router.py`

**D. Main service/function:** `backend/services/watchlist_analysis.py → run_analysis_pipeline()`

**E. Exact call chain:**
```
POST /api/watchlist/strategy-report/generate
  → run_analysis_pipeline(tickers, agent, data_service)
      → asyncio.gather() — 5 parallel branches:
          _collect_grok_sentiment(tickers)      → xAI Grok; X/Twitter posts last 48h
          _collect_gemini_news(tickers)          → Gemini web-grounded search; last 2 weeks
          _collect_claude_csv_analysis(csv_data) → Claude / DeepSeek on user-uploaded CSV
          _collect_sec_edgar(tickers)            → SEC EDGAR API (10-K, 10-Q, 8-K, Form 4)
          _collect_technical_analysis(tickers)   → ta_signal_engine.py via Tradier/FMP
      → _build_synthesis_prompt(all_collected)   → master prompt construction
      → _synthesize_all_sources(prompt)          → Claude → 6-section JSON output:
          Sections: best_entries, momentum_plays, catalyst_watch,
                    sector_rotation, high_conviction, contrarian_value
      → persist to disk (data/strategy_reports/) + Neon watchlist store
      → return
```

**F. Cache/LKG/Neon:** Strategy reports persisted to `backend/data/strategy_reports/`. Watchlist analysis persisted in Neon watchlist store under `analysis.sections`.

**G. Refresh cadence:** User-triggered (POST). No background auto-refresh.

**H. Calculation type:** Precomputed on user request; LLM-driven; non-deterministic across runs.

**I. History:** Report history via `GET /api/watchlist/strategy-report/history`.

**J. Fields per section ticker:**
```
symbol, name, price, change_pct
technical_setup (text)
catalyst (text)
sentiment (text)
key_insight (text)
risk_level (low/moderate/high)
action_note (levels: support/resistance/entry zone)

Enriched on-read by _enrich_store_with_quotes():
  stage, stage_label, setup_type, action, action_note, conviction
  technical_score, rs_score, vol_ratio, vol_mc_pct
  rv_rank, volmc_rank, rv_percentile, volmc_percentile
  fundamentals fields (from watchlist_fundamentals_store)
  theme/sector classification
  nf_* options flow fields (where available)
```

---

## PART B — DAILY ALPHA BOARD DEEP AUDIT

**1. Initial ticker universe:** Dynamically unioned from all 8 collectors at runtime. No static universe file.
- Themes: top 20 themes by `rs_score ≥ 40`
- Strategy: all tickers in `strategy_screener_lkg.json`
- Catalysts: earnings within [−10, +21] days
- Options: all tickers in `options_master_lkg_v1.json`
- Hyperliquid: all active perp snapshots
- Watchlist: up to 5 watchlists × all `analysis.sections[].tickers[]`
- Portfolio: all `portfolio_holdings` rows

**2. Contributing pages:** Watchlist, Portfolio, Social Screener, Themes, Strategy Screener, Catalyst Calendar, Options Flow, Hyperliquid

**3. Aggregation:** UNION across all collectors, then deduplicated by symbol via `_merge_candidates()`. Same symbol across multiple collectors → signal slots merged (best non-null value wins; source_pages accumulates)

**4. Filter order:** collect → merge → `_set_stock_long_fields()` → `_apply_filters()` (asset type, scope) → `_score_candidate()` → sort → rank

**5–6. Signals and weights:** See Part A table. Score formula: available-weight normalization (denominator = sum of weights for *present* signals only).

**7. Hard gates before scoring:**
- `len(present) < 2` → score=0, candidate excluded
- `not (present & timing_keys)` → score=0, candidate excluded
- `staleness_factor == 0.0` for an entire source → that collector returns []

**8. Negative scores/penalties:** No true negative raw scores. All penalties are multiplicative multipliers applied to the final normalized score. There is no "subtraction" path in `_score_candidate()`. Minimum result is 0.0.

**9. Missing signals:** Excluded from both numerator AND denominator (available-weight normalization). A 1-signal candidate is excluded by the 2-signal gate.

**10. Tie-breaking:** `has_timing_signal=True` candidates fill top-N slots first. Within same group, sort by `score` descending. No secondary tiebreaker defined.

**11. Theme/sector alignment:** YES — weighted at 0.20. Source: `themes_rs_lkg.json`.

**12. Ticker stage:** YES — incorporated into the `ta` signal slot via stage modifier. Stage is not a separate slot; it adjusts ta_sig by ±0.06–0.20.
- Stage 2: +0.12
- Stage 1: +0.06
- Stage 3: −0.10
- Stage 4: −0.20

**13. Options signal:** YES — weighted at 0.10. Source: `composite_score` from `options_master_lkg_v1.json`.

**14. Net options flow:** NO. The options collector reads `composite_score`, `confidence_score`, `side_bias`, `primary_signal`. Net flow premium fields (`net_premium_24h`, `net_premium_7d`) from the Options Flow sectors system are NOT consumed.

**15. Options-flow acceleration/delta:** NO.

**16. VolX:** NO for stocks. VolX is a Hyperliquid/crypto concept. For crypto, the `oi` and `volume_velocity` signal slots capture OI spike and volume momentum from the Hyperliquid snapshot. The standalone Options Flow VolX metric from `options_flow_sectors.py` is not consumed.

**17. Volume/market cap:** YES — `rel_volume` slot (weight 0.15). Formula: `vol_ratio/4.0` (volume vs 20d avg from watchlist store) OR `vol_mc_pct/0.30` (daily vol as % of market cap from social screener).

**18. Relative strength:** Partial. `rs_score` from `themes_rs_lkg.json` feeds `theme_sig`. `rs_score` from the watchlist store feeds `social_sig` in the watchlist collector. `_rs_boost` flag applies +5% multiplier in risk_off/neutral regime. RS is not a standalone weighted slot.

**19. Social consensus:** YES — weighted at 0.10 (`social` slot).

**20. Social acceleration/freshness:** NO as a separate slot. The staleness factor (0.90/0.70 multiplier) is the only freshness mechanism. `acceleration_score` from `social_screener_service.py` is computed but not consumed in the Daily Alpha scoring.

**21. News:** YES — weighted at 0.08. Source: `news_signal_scorer.py` deterministic keyword-rule engine. `major_news_score/100`.

**22. Catalysts:** YES — weighted at 0.12. Source: `earnings_snap_*.json`. Earnings within [−10, +21] day window.

**23. Earnings:** YES — same as catalysts above. `days_to_earnings` computed and returned in idea output.

**24. Fundamentals:** YES — weighted at 0.05. Source: normalized 0–1 from Claude/CSV LLM analysis data stored in Neon watchlist store. Only present for watchlist tickers that have been through a strategy report generation.

**25. Analyst ratings/targets/estimates:** NO. Not consumed by any collector or signal slot.

**26. Macro/market regime:** YES — `regime_engine` label and confidence modify the final score via cross-cutting multipliers (not a weighted slot).

**27. VIX:** Not directly as a standalone signal. Incorporated into the regime engine output (regime label reflects VIX state), but VIX value itself is not a signal slot.

**28. Prediction market signals:** NO.

**29. Portfolio/watchlist membership:** Indirect only. Portfolio/watchlist symbols enter the candidate pool via their respective collectors. Watchlist staleness floor = 0.55 (never fully excluded). No explicit "membership = +N points" weight.

**30. Price entry quality/support/pullback/breakout/extension:** YES — `setup_bucket` (stage_1_to_2_base, early_breakout, dip_reversal_watch, etc.), `entry_quality` (low/medium/high), `extension_risk` (bool/text), `timing_quality` (high/medium) are computed and returned. An "extended" status in a `risk_off` regime applies a score penalty (`score *= (1 − 0.05 * confidence)`). These affect both scoring and display fields.

---

**Worked Example — AMD (using hypothetical but realistic cached values)**

```
CANDIDATE SOURCES:
  - watchlist collector: stage=2, technical_score=78, vol_ratio=2.1, rs_score=72
  - themes collector:    Semiconductors theme, rs_score=68
  - options collector:   composite_score=71 in options_master_lkg_v1

MERGE:
  present = { ta, rel_volume, social, theme, options }

SIGNAL VALUES:
  ta_sig      = 78/100 = 0.78; stage=2 → min(1.0, 0.78+0.12) = 0.90
  rel_vol_sig = min(1.0, 2.1/4.0) = 0.525
  social_sig  = 72/100 = 0.72
  theme_sig   = 68/100 = 0.68
  opt_sig     = 71/100 = 0.71

GATES:
  len(present)=5 ≥ 2 ✅
  timing={ta, rel_volume, social, options} ≥ 1 ✅
  external_timing={rel_volume, social, options} → has_timing_signal=True ✅

SCORING:
  avail_weight = 0.18+0.15+0.10+0.20+0.10 = 0.73
  raw = (0.90×0.18)+(0.525×0.15)+(0.72×0.10)+(0.68×0.20)+(0.71×0.10)
      = 0.162+0.079+0.072+0.136+0.071 = 0.520
  score_raw = 0.520/0.73 = 0.712
  score_100 = 71.2

REGIME MODIFIER (assume risk_on, conf=0.7, AMD is themed):
  score_100 = 71.2 × (1 + 0.04×0.7) = 71.2 × 1.028 = 73.2

CONFIDENCE: 73.2 ≥ 72 → "high"
DIRECTION: long (long bias enforced for stocks)
RANK: sorted among all candidates by score desc; 73.2 competes for top-N
```

---

## PART C — GOLDEN / HC ZONE DEEP COMPARISON

**CRITICAL FINDING:** Golden Zone, HC Trade Zone, and HC Investment Zone are LLM-synthesized section labels. They do NOT have independent signal weights, gates, or thresholds. The analysis is non-deterministic across runs. The table below reflects what each section's LLM synthesis instructions *target*, not a coded scoring formula.

| Signal / Gate | Golden Zone (`best_entries`) | HC Trade Zone (`high_conviction`) | HC Investment Zone (`contrarian_value`) | Daily Alpha Board |
|---|---|---|---|---|
| **Stage** | YES — display (dip-in-uptrend requires Stage 2 context) | YES — display | YES — display | YES — weighted (in ta slot) |
| **Revenue growth** | YES — display (from CSV/EDGAR) | YES — display | YES — display | YES — weighted (0.05, via watchlist fundamentals) |
| **EPS growth** | YES — display | YES — display | YES — display | YES — weighted (0.05) |
| **Forward revenue expectations** | NO | NO | NO | NO |
| **Forward EPS expectations** | NO | NO | NO | NO |
| **Gross margin** | YES — display (if in CSV) | YES — display | YES — display | NO |
| **FCF / FCF margin** | YES — display | YES — display | YES — display | NO |
| **Profitability** | YES — display | YES — display | YES — display | NO |
| **Analyst signal** | NO | NO | NO | NO |
| **Analyst target upside** | NO | NO | NO | NO |
| **VolX** | NO | NO | NO | NO |
| **Volume / market cap** | YES — display | YES — display | NO | YES — weighted (0.15) |
| **Relative volume momentum** | YES — gate (declining vol on dip preferred) | YES — display | NO | YES — weighted (0.15) |
| **Options score** | NO | NO | NO | YES — weighted (0.10) |
| **Premium put/call** | NO | NO | NO | NO |
| **Net flow** | NO | NO | NO | NO |
| **Theme alignment** | YES — display | YES — display | YES — display | YES — weighted (0.20) |
| **Theme stage** | YES — display | YES — display | YES — display | YES — weighted (in ta) |
| **Relative strength** | YES — display (ta_signal_engine RS) | YES — display | YES — display | Partial (rs_boost only) |
| **Social signal (Grok)** | YES — display | YES — gate (convergence req) | YES — gate (depressed ok) | YES — weighted (0.10) |
| **News/catalysts** | YES — display (from Gemini) | YES — display | YES — display | YES — weighted (0.08+0.12) |
| **Price structure** | YES — gate (dip in uptrend) | YES — display | NO explicit gate | YES — partial (setup_bucket) |
| **Support proximity** | YES — display (action_note) | YES — display | YES — display | YES — display only |
| **Extension risk** | YES — implied (dip preference) | NO explicit gate | NO | YES — regime penalty |

**Zone comparison answers:**

1. **Is Golden Zone literally an intersection of HC Trade + HC Investment?** NO. All three are independent LLM section prompts. No code intersects the two HC zones to produce Golden Zone.

2. **Does it independently recalculate its own score?** Not applicable — no scoring formula exists for any zone. All three are produced by a single LLM synthesis pass.

3. **Does HC Trade use the canonical Options Flow signal?** NO. The watchlist analysis pipeline uses `ta_signal_engine.py` (RSI, MACD, SMA, volume ratio via Tradier/FMP). It does NOT consume `options_master_lkg_v1.json` or the Options Flow sectors system.

4. **Does HC Investment use all current fundamental columns?** NO. Only whatever is in the user-uploaded CSV (`_collect_claude_csv_analysis()`) plus whatever SEC EDGAR returns (`_collect_sec_edgar()`). Not normalized against the `fundamentals_enricher.py` FMP schema.

5. **Are analyst estimates/targets currently part of any zone calculation?** NO. Not in Daily Alpha, not in any zone.

6. **Are there duplicated calculations between these systems?** YES — Stage analysis, technical score, social/RS signal. See Part G for full list.

7. **Can two systems return materially different values for the same underlying signal?** YES. The `ta` signal in Daily Alpha uses the Neon watchlist store (potentially weeks-old LLM analysis). The HC Trade zone calls `ta_signal_engine.py` with fresh bars at report generation time. They can diverge significantly.

---

## PART D — ALERT ENGINE AUDIT

**Endpoints/routes:**
- `GET /api/alerts/stream` — SSE real-time push
- `GET /api/alerts/recent` — polling (`since`, `limit`, `popup_only` params)
- `GET /api/alerts/diagnostics` — in-memory state + 7-day DB counts
- `GET /api/alerts/history` — paged 7-day history, up to 30-day lookback
- `GET /api/alerts/{alert_id}` — metadata
- `GET /api/alerts/{alert_id}/detail` — full detail with reasons, source tags
- `POST /api/alerts/{alert_id}/ack`
- `POST /api/alerts/{alert_id}/dismiss`

**Monitoring service:** `backend/services/alert_signal_bus.py` — `AlertSignalBus` class. **Passive bus only** — has no internal fetch loop. Receives data via `record_signal_snapshot()` called by other services.

**Scheduler/loop:** NONE of its own. Piggybacks on:
- `_watchlist_rank_snapshot_loop` (main.py, 5-minute interval)
- `_x_consensus_loop` (main.py, daily 10:00 AM CT)
- `_itype_classify_loop` (main.py, 30-minute)

**Ticker universe:** Not statically defined. Processes any ticker passed via `record_signal_snapshot()`. Categorized into lanes by source.

**Signal inputs and thresholds:**

`full_activity` lane — requires ≥ 2 independent signals; total score ≥ 75 to fire:
- Price move: 3% → +20 pts, 5% → +30 pts, 8% → +40 pts
- VolX: 2× → +20 pts, 5× → +35 pts, 10× → +45 pts
- Vol/MC: 5% → +15 pts, 10% → +25 pts, 15% → +30 pts
- Rel Vol z-score: 2σ → +20 pts, 3σ → +30 pts

`options_first` lane — single-signal, score ≥ 75 to fire:
- Base: `options_score × 0.85`
- Rank boost: top 3 → +12 pts, top 5 → +8 pts
- Unusual put/call bias → +15 pts

`hyperliquid` lane:
- Volume: $5M → +25 pts, $20M → +40 pts
- Liquidations: $1M → +30 pts, $5M → +45 pts
- Funding >0.1%/hr → +20 pts
- OI spike 2σ → +20 pts

`cross_confirmed` upgrade: Same ticker fires `options_first` AND `full_activity` within 15 minutes → lane upgraded, +20 pts added.

**Baseline/history source:** `public.ticker_signal_snapshots` Neon table (7-day retention). Z-score computed from last 20 snapshots per `(ticker, source)`.

**Cooldown:** 15 minutes (`_COOLDOWN_SECS = 900`) per `(user, ticker, alert_type)`. Escalation bypass: higher severity overrides active cooldown immediately.

**Snapshot debounce:** Same `(ticker, source)` seen < 5 minutes ago → skip ingestion.

**Persistence:**
- Snapshots: `public.ticker_signal_snapshots`, 7-day retention
- Alert events: `public.ticker_alert_events`, 90-day retention

**Frontend delivery:** SSE push via `/api/alerts/stream` + polling via `/api/alerts/recent?popup_only=true`. Detail fetched via `GET /api/alerts/{id}/detail`.

**Alert engine answers:**

1. **Do alerts rank confluence?** NO. Each lane scores its own signals independently. Cross-confirmation is an upgrade, not full confluence ranking.

2. **Can multiple aligned signals combine into one alert?** YES — `full_activity` requires ≥ 2 signals; `cross_confirmed` combines options + price/volume. But theme, social, news, fundamentals do NOT contribute to alert scoring.

3. **Does an alert know that a ticker is also in Daily Alpha?** NO.

4. **Does an alert know Golden/HC Zone membership?** NO.

5. **Does an alert consume theme state?** NO.

6. **Does an alert consume options signal?** YES — `options_first` and `cross_confirmed` lanes.

7. **Does an alert consume portfolio/watchlist context?** The `full_activity` lane is triggered by watchlist/portfolio tickers, but alert content is not enriched with zone/stage/portfolio context at fire time.

8. **Is there any concept resembling TRADE_IDEA / ENTRY_WINDOW / ADD_SIGNAL / HOLD / TRIM / EXIT / THEME_ROTATION / RISK_REGIME?** NO. None of these concepts exist in the alert engine. Alert types are: unusual_volume, price_move, oi_spike, options_unusual, liquidation_spike.

---

## PART E — STAGE AND THEME INPUT PROVENANCE

### Stage Analysis

**Implementation file:** `backend/services/stage_analysis.py`

**Functions:**
- `analyze_symbol_stage()` — primary entry point for single ticker
- `analyze_theme_stage()` — entry point for thematic index (adds member breadth)
- `_classify_stage()` — core deterministic mapping of indicators to Weinstein stages
- `_compute_stage_score()` — continuous 0–100 scoring for fallback
- `compute_technical_metrics()` — daily-bar-focused: SMA 20/50/200, extension risk, breakout
- `_breadth_only_result()` — fallback when no price index available (theme-only)

**Required historical data:** Minimum 35 weekly bars (35 weeks). Service fetches 400 daily bars (Tradier primary, FMP fallback), then aggregates to weekly.

**Provider/cache source:** Tradier `/markets/history`; FMP historical prices (fallback). Results cached in `backend/data/watchlist_stage2_lkg.json`. In-memory LKG via `get_stage2(sym)` (zero I/O on read).

**Lookback:**
- 30-week SMA: primary Weinstein MA
- 4 weeks: slope calculation window
- 26 weeks: prior-trend context (`prior_26w`)
- 8 weeks: RS calculation vs SPY

**Moving averages used:**
- Weinstein primary: **30-week SMA** (weekly closes derived from daily bar aggregation)
- Technical metrics: **20-day SMA, 50-day SMA, 200-day SMA** (daily bars)
- RS: 8-week ticker performance vs SPY

**Slope calculation:**
```python
slope = (MA_current - MA_4w_ago) / MA_4w_ago * 100
# Rising:  slope > 0.25
# Falling: slope < -0.25
# Flat:    -0.25 <= slope <= 0.25
```

**Price-relative-to-MA rules:**
- Above MA: `price_vs_ma > 0`
- Near MA: `-6.0% <= price_vs_ma <= 8.0%`
- Extended: `price_vs_ma > 20%` (triggers Stage 3m)

**Breakout/base rules:**
- Stage 1 base: price below/near flat MA; `prior_26w < -10%`
- Stage 1→2 watch: price near MA; flat/rising MA; prior downtrend
- Stage 2b breakout: price crosses above flat/rising MA from prior downtrend context
- Daily fresh breakout: price within 3% above the 20-day high (compute_technical_metrics)

**Stage classification precedence (evaluated in order):**
```
1. Stage 4:    price below + falling MA
2. Stage 34:   price below + flat MA + prior uptrend
3. Stage 3:    price near/above + flat MA + prior uptrend
4. Stage 3m:   price above + rising MA + price_vs_ma > 20% + prior uptrend   ← FALSE POSITIVE SOURCE
5. Stage 2b:   price above + flat/rising MA + prior downtrend
6. Stage 2-3:  price above + rising MA
7. Stage 12:   price near + flat/rising MA + prior downtrend
8. Stage 1:    price below/near + flat MA + no prior uptrend
9. Fallback:   _compute_stage_score() → 0–100 mapped to nearest stage
```

**Fallback behavior:**
- Insufficient bars → `_fallback_result` with "Unknown" label
- Theme without price series → `_breadth_only_result` (member % above own MAs)
- LKG degraded-run guard: if bulk update yields <20% valid labels → preserve existing LKG, mark only failures for retry

**Freshness:** "OK" entries: 20-hour TTL. "fetch_failed": 2-hour TTL for rapid retry.

**Do Watchlist and Themes use the same canonical stage result?**
- Watchlist: reads from `watchlist_stage2_lkg.json` via `get_stage2(sym)` — same `_classify_stage()` engine
- Themes: `analyze_theme_stage()` — same engine, but inputs theme proxy ETF + member breadth as additional factor
- They are the same engine applied to different input series. A single stock's stage from the Watchlist path and the Themes path can differ because the theme path also weighs member breadth.

**Likely reasons for false Stage 3 classifications:**

1. **Extension rule fires on recently strong breakouts (Stage 3m):** Any stock where `price_vs_ma > 20%` AND MA is rising AND prior 26-week trend is positive gets classified "3m." A stock emerging from Stage 1→2 that runs quickly can be instantly mis-labeled Stage 3m before it has truly extended. The 20% threshold is absolute — not volatility-adjusted and not duration-qualified.

2. **Flat MA ambiguity:** A recovering stock can have a flat MA (slope between −0.25 and +0.25) while price is above it AND `prior_26w > 0`. This routes to rule 3 (Stage 3), not rule 5 (Stage 2b), even if the MA has only recently turned from down to flat.

3. **Prior-trend detection sensitivity:** `prior_26w` is the raw 26-week price change. A stock that fell 5% then recovered strongly can have `prior_26w > 0` (net positive) — routes to Stage 3 path instead of Stage 2b/1 path, even if the stock is genuinely in early Stage 2.

4. **Weekly aggregation granularity:** Daily prices aggregated to weekly closes. A stock that started a breakout mid-week may show "near MA" in the weekly close, triggering Stage 12 (watch) when it should be Stage 2b.

---

### Theme State Calculation

**Implementation file:** `backend/services/theme_rs_service.py`

**Exact state calculation:**
```python
# State = function of 1D performance vs median of ALL themes' 1D performance
# median = median(all_theme_1D_performances)

if perf_1d > 0 and perf_1d > (median + 0.005):      → "leading"
if perf_1d > 0 and abs(perf_1d - median) < 0.005:   → "emerging"
if perf_1d <= 0 and abs(perf_1d - median) < 0.005:  → "neutral"
if perf_1d <= 0 and perf_1d < (median - 0.005):     → "lagging"
```

**Timeframes used:** 1D, 7D, 30D, YTD, 1Y, 5Y. State classification (`leading`/`emerging`/`neutral`/`lagging`) is based on **1D performance ONLY**. Longer timeframes are computed and returned but do not affect state.

**RS acceleration:** `perf_current − perf_prior`. For 1D: latest intraday vs prior 10-minute snapshot.

**ETF proxy:** Primary proxy ETF via `etf_holdings_service`; top 10 holdings used for basket performance. Performance averaged across holdings.

**Average theme-member performance:** YES — breadth = `% of tickers with positive 1D change`.

**Options flow integration:** YES — `net_premium_24h` and `sentiment_score` for the proxy ETF incorporated.

**Social signal integration:** YES — `consensus_score` and `acceleration_score` from the social pipeline for the proxy ticker.

**NOT currently used in theme state calculation:**
- Anchor performance (curated anchor bottleneck anchors not consumed)
- News/catalysts (news scorer output not consumed)
- Longer timeframe windows for state classification (7D/30D data exists but state uses 1D only)

---

## PART F — CANONICAL SIGNAL INVENTORY

| Signal Name | Canonical Field | Provider | Canonical Backend Source | Calculation Function | Current Consumers | History? | Stored Where | Freshness | Duplicate Impls? | Conflicting Formulas? | Safe for V2? |
|---|---|---|---|---|---|---|---|---|---|---|---|
| Market Regime | `label`, `confidence` | regime_engine | `regime:current_v1` TTLCache | `backend/core/regime_engine.py` | Daily Alpha | NO | In-memory TTLCache | Real-time | NO | NO | YES |
| VIX State | embedded in regime label | regime_engine | regime engine | regime_engine | Daily Alpha (via regime) | NO | In-memory | Real-time | NO | NO | Partial |
| Theme State | `state` | theme_rs_service | `themes_rs_lkg.json` | `_classify_theme_state()` | Daily Alpha, Themes page | NO | `themes_rs_lkg.json` | ~hourly | NO | NO | YES |
| Theme RS Score | `rs_score` | theme_rs_service | `themes_rs_lkg.json` | theme_rs_service | Daily Alpha, Themes | YES (YTD/1Y/5Y) | `themes_rs_lkg.json` | ~hourly | NO | NO | YES |
| Theme RS Acceleration | `rs_acceleration` | theme_rs_service | `themes_rs_lkg.json` | `perf_current - perf_prior` | Themes page | NO | `themes_rs_lkg.json` | ~hourly | NO | NO | YES |
| Theme Breadth | `breadth` | theme_rs_service | `themes_rs_lkg.json` | `% members with positive 1D` | Themes page | NO | `themes_rs_lkg.json` | ~hourly | NO | NO | YES |
| Ticker Stage (Weinstein) | `stage`, `stage_label` | stage_analysis | `watchlist_stage2_lkg.json` | `_classify_stage()` | Daily Alpha (via watchlist), Watchlist page | NO | `watchlist_stage2_lkg.json` | 20h TTL | NO | NO | YES |
| Ticker Technical Score | `technical_score` | ta_signal_engine | Neon watchlist store | `ta_signal_engine.py` | Daily Alpha (via watchlist), HC Trade zone | NO | Neon watchlist store | On-analysis | YES (two call paths) | Potentially | Fix staleness gating |
| Ticker RS vs SPY (8w) | `rs_score` (stage LKG) | stage_analysis | `watchlist_stage2_lkg.json` | `analyze_symbol_stage()` | Watchlist, Daily Alpha | NO | Stage LKG | 20h TTL | YES — field name collision | YES — same field name as social rank | Fix naming |
| Social Rank (consensus) | `rs_score` (watchlist store) | social_screener | `x_consensus_weekly.json` | `social_screener_service.py` | Daily Alpha (social slot) | NO | `x_consensus_weekly.json` | Daily | YES — field name collision | YES | Fix naming |
| Social Consensus Score | `consensus_score` | x_consensus_cache | `x_consensus_weekly.json` | `social_screener_service.py` | Daily Alpha, Watchlist analysis | NO | `x_consensus_weekly.json` | Daily | YES (two consumers, different sources) | Different sources (weekly file vs live Grok) | Fix sourcing |
| Social Freshness Score | `freshness_score` | social_screener | `x_consensus_weekly.json` | score map: 0d→100, 1d→90, 3d→75, 7d→50, 14d→25, 30d+→5 | Not consumed in scoring | NO | `x_consensus_weekly.json` | Daily | NO | NO | YES |
| Social Acceleration Score | `acceleration_score` | social_screener | `x_consensus_weekly.json` | `ratio=mentions_1d/max(mentions_7d/7,1); (min(ratio,4)/4*80)+(20 if in_accel)` | Not consumed in any scoring | NO | `x_consensus_weekly.json` | Daily | NO | NO | YES — add to V2 |
| VolX (Hyperliquid) | `volx` | Hyperliquid API | hyperliquid snapshot | `hyperliquid/router.py` | Daily Alpha (crypto only), Alert bus | NO | `hyperliquid_signal_snapshots.json` | Real-time | NO | NO | YES (crypto only) |
| Volume / Market Cap % | `vol_mc_pct` | social_screener | `x_consensus_weekly.json` | `(daily_volume / market_cap) * 100` | Daily Alpha (rel_volume slot) | NO | Weekly social LKG | Daily | NO | NO | YES |
| Volume Ratio (vs 20d avg) | `vol_ratio` | watchlist store | Neon watchlist store | ta_signal_engine | Daily Alpha, Alert bus | NO | Neon watchlist store | On-analysis | YES | NO | YES |
| Options Composite Score | `composite_score` | Options LKG | `options_master_lkg_v1.json` | sectors_chain_summarizer | Daily Alpha, Alert bus | NO | `options_master_lkg_v1.json` | ~2–4h | NO | NO | YES |
| Options Side Bias | `side_bias` | Options LKG | `options_master_lkg_v1.json` | sectors_chain_summarizer | Daily Alpha (display) | NO | `options_master_lkg_v1.json` | ~2–4h | NO | NO | YES |
| Premium Put/Call | `premium_pcr` | Options Flow | Options supplement LKG | `options_flow_sectors.py` | Options Flow page | YES (35-day, `options_net_premium_daily`) | Supplement LKG + Neon | Per-scan | NO | NO | YES |
| Net Flow (7–60 DTE canonical) | `net_premium_24h` | Tradier | Supplement LKG + Neon snapshot | `sectors_chain_summarizer.py` | Options Flow page | YES (35-day) | `options_net_premium_daily` Neon | Per-scan | NO | NO | YES |
| Net Flow Delta 1D | `delta_1d` | `options_net_premium_history.py` | `options_net_premium_daily` Neon | `get_net_premium_delta()` | Options Flow page (injected in tree) | YES | `options_net_premium_daily` | Daily snapshot | NO | NO | YES — add to V2 |
| Net Flow Delta 7D | `delta_7d` | same | same | same | Options Flow page | YES | same | Daily | NO | NO | YES — add to V2 |
| Net Flow Delta 30D | `delta_30d` | same | same | same | Options Flow page | YES | same | Daily | NO | NO | YES — add to V2 |
| News / Catalyst Score | `major_news_score` | news_signal_scorer | Neon watchlist store + RSS | `news_signal_scorer.py` keyword rules | Daily Alpha, Watchlist news panel | NO | Neon watchlist / RSS cache | 30-min TTL | NO | NO | YES |
| Earnings Proximity | `days_to_earnings` | FMP / earnings_snap | `earnings_snap_*.json` | `earnings_clean_service.py` | Daily Alpha (catalyst slot), Watchlist | NO | `earnings_snap_*.json` | Weekly | NO | NO | YES |
| Revenue Growth (YoY) | `revenue_growth` | SEC EDGAR | `fundamentals_enricher.py` | `(Rev_yr − Rev_prev) / |Rev_prev| * 100` (10-K, 330–400 days apart) | Watchlist analysis, watchlist_fundamentals_store | NO | Neon `watchlist_fundamentals_cache` | Weekly | YES (EDGAR vs FMP vs CSV) | YES | Fix sourcing |
| EPS Growth | `eps_growth` | SEC EDGAR / FMP | `fundamentals_enricher.py` | Annual EPS from EDGAR or FMP | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | YES (EDGAR vs FMP) | YES | Fix sourcing |
| Gross Margin | `gross_margin` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `grossProfitMarginTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| FCF Margin | `fcf_margin` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `(fcfPerShareTTM / price) * priceToSalesRatioTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | Verify formula |
| FCF (absolute) | Not explicitly stored | FMP | `fundamentals_enricher.py` | Per-share × shares (not directly stored) | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | Partial |
| Operating Income / EBIT | Not stored | Not consumed | — | — | None | NO | — | — | NO | NO | Missing |
| P/E Ratio | `pe_ratio` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `priceEarningsRatioTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| P/S Ratio | `ps_ratio` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `priceToSalesRatioTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| P/B Ratio | `pb_ratio` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `priceToBookRatioTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| Debt/Equity | `debt_equity` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `debtToEquityRatioTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| ROE | `roe` | FMP `/ratios-ttm` | `fundamentals_enricher.py` | `returnOnEquityTTM` | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| Net Margin | `net_margin` | SEC EDGAR | `fundamentals_enricher.py` | `(NetIncomeLoss / Revenue) * 100` from latest annual filing | Watchlist analysis | NO | Neon `watchlist_fundamentals_cache` | Weekly | NO | NO | YES |
| EV/EBITDA | Not stored | Not consumed | — | — | None | NO | — | — | NO | NO | MISSING |
| Analyst Recommendation | Not consumed | None | — | — | None | NO | — | — | NO | NO | MISSING |
| Analyst Target Upside | Not consumed | None | — | — | None | NO | — | — | NO | NO | MISSING |
| Forward Estimates | Not consumed | None | — | — | None | NO | — | — | NO | NO | MISSING |
| Portfolio Membership | implicit via collector | Neon portfolio_holdings | `portfolio_ledger.py` | `load_active_holdings()` | Daily Alpha (candidate source) | NO | Neon | Real-time | NO | NO | YES |
| Watchlist Membership | implicit via collector | Neon watchlist | `watchlist_service.py` | `list_watchlists()` | Daily Alpha (candidate source) | NO | Neon | Real-time | NO | NO | YES |
| Close Watch / Favorite | Not a scored field | — | — | — | None | NO | — | — | NO | NO | MISSING |
| Insider Activity (Form 4) | `recent_filings` | SEC EDGAR | `_collect_sec_edgar()` | EDGAR API Form 4 | Watchlist HC Investment zone (LLM interpretation) | NO | Not persisted separately | On-analysis | NO | NO | Partial (LLM-consumed only) |
| Smart Options (HL gap) | `gap_pct`, `signal_direction` | Hyperliquid + Tradier | `smart_options_service.py` | `((HL price - actual) / actual) * 100` | Not consumed by scoring engines | NO | In-memory | Real-time | NO | NO | NOT the same as options flow — label carefully |

---

## PART G — DUPLICATION AND ARCHITECTURAL DRIFT

### G1 — `rs_score` field name collision
**SEVERITY: CRITICAL**

- **Path A:** `watchlist_stage2_lkg.json` → `rs_score` = 8-week relative performance vs SPY (returns-based, typically −20 to +20 range)
- **Path B:** `x_consensus_weekly.json` → `rs_score` = social rank within screener universe (rank-based, 0–100 range)
- **Path C:** Neon watchlist store → `row.get("rs_score")` — which value this contains depends on which system last wrote it

`collect_watchlist_cache_candidates()` reads `row.get("rs_score")` and maps it to `social_sig = _clamp01(float(rs_score) / 100.0)`. If the stage-LKG value (−5 to +15 typical) is consumed here, `social_sig ≈ 0.05–0.15` — systematically understating social signal. If the social rank value (0–100) is consumed as RS, it overstates RS.

**Why it matters for V2:** V2 cannot normalize RS and social signals correctly until the field naming is disambiguated. All V2 signal tables must use `price_rs_8w_spy` and `social_rank_score` as separate canonical names.

---

### G2 — Dual `technical_score` computation paths
**SEVERITY: HIGH**

- **Path A:** Daily Alpha reads `technical_score` from Neon watchlist store — written by `ta_signal_engine.py` at the time the user ran a strategy report (could be days or weeks ago)
- **Path B:** Watchlist analysis (`_collect_technical_analysis()`) calls `ta_signal_engine.py` with fresh bar data at report generation time

A watchlist analysis that is 3 days old means Daily Alpha is scoring TA state from 3 days ago. After the next report generation, Daily Alpha updates only on next TTL expiry.

**Why it matters for V2:** Cannot trust `technical_score` from a Neon row without checking `updated_at`. Must define a max-age threshold (suggest ≤24h) before falling back to stage-only TA signal.

---

### G3 — Social signal: weekly file vs live Grok call
**SEVERITY: HIGH**

- **Path A:** Daily Alpha consumes `consensus_score` from `x_consensus_weekly.json` — updated once daily via `_x_consensus_loop`
- **Path B:** Watchlist analysis calls `_collect_grok_sentiment()` — live xAI Grok API call at report time

Within a single trading session, a ticker that trended on X in the last 2 hours appears with yesterday's social state in Daily Alpha and with current social state in the HC Trade zone analysis. These can be materially different.

**Why it matters for V2:** Two consumers of "social signal" return different values within the same session. V2 must define a canonical social source (weekly file is faster/cheaper; Grok is more current).

---

### G4 — Fundamentals: SEC EDGAR vs FMP vs user CSV — triple sourcing
**SEVERITY: HIGH**

- **Path A:** `fundamentals_enricher.py` — Revenue growth from SEC EDGAR 10-K (annual only, 330–400 day gap rule). Ratios (PE, PS, gross margin, FCF margin) from FMP `/ratios-ttm`
- **Path B:** Watchlist analysis `_collect_sec_edgar()` — Revenue, net margin, EPS from SEC EDGAR (separate code path, different filing selector)
- **Path C:** `_collect_claude_csv_analysis()` — User-uploaded CSV with unformatted, unvalidated financial data fed to LLM

Three different fundamental paths can produce three different revenue growth figures for the same company in the same period.

**Why it matters for V2:** Cannot aggregate fundamental signals across systems without a canonical normalization layer. Must pick one source per field and document where overrides apply.

---

### G5 — "Options signal" means three different things
**SEVERITY: HIGH**

- **Meaning 1 (Daily Alpha):** `composite_score` from `options_master_lkg_v1.json` — the chain summarizer output (canonical put/call premium, net flow, unusual contracts)
- **Meaning 2 (`smart_options_service.py`):** Hyperliquid/Tradier price gap — a cross-market arbitrage signal (`((HL_price - actual) / actual) * 100`). Has nothing to do with options premium flow.
- **Meaning 3 (Watchlist HC Trade zone):** LLM-interpreted options mentions from Grok/Gemini text — no structured signal at all

**Why it matters for V2:** V2 must name these three signals explicitly: `options_flow_composite` (chain-scanner output), `hl_price_gap_signal` (arbitrage), and exclude the LLM-text path from scored inputs.

---

### G6 — Stage result: symbol-level vs theme-level breadth
**SEVERITY: MEDIUM**

- Symbol stage: `analyze_symbol_stage()` — pure 30w MA price classification for a single ticker
- Theme stage: `analyze_theme_stage()` — same engine + `member_breadth` (% of theme members above own MAs)

A ticker can be Stage 2 (individual classification) while its theme is classified Stage 3 (breadth deteriorating). Daily Alpha reads symbol-level stage only. Themes page displays theme-level stage.

**Why it matters for V2:** Must specify which stage definition applies. A ticker individually Stage 2 in a theme-level Stage 3 classification produces mixed signals. V2 should use both: `ticker_stage` (symbol) and `theme_stage` (breadth-adjusted) as distinct inputs.

---

### G7 — Alert engine is architecturally decoupled from scoring systems
**SEVERITY: MEDIUM**

The alert engine is a passive bus. A ticker generating a high-confidence Daily Alpha idea and simultaneously generating an alert — but neither system knows about the other. Alerts fire on price/volume/OI/options. They do NOT fire on: theme RS breakout, stage transition, social acceleration, earnings proximity, news catalyst.

**Why it matters for V2:** V2 confluence cannot route signals to the alert surface without a new integration bridge. The alert engine would need a new lane (`confluence_signal`) that accepts a pre-computed V2 score.

---

### G8 — Zone scoring is non-deterministic
**SEVERITY: MEDIUM**

The same ticker can rank #1 in HC Trade one run and be absent the next depending on LLM output variation (temperature, context window, model version). There is no fallback deterministic scoring for zones.

**Why it matters for V2:** V2 must NOT inherit the zone LLM pipeline as its scoring backbone. V2 must be deterministic and reproducible. Zone sections can continue to exist as a separate LLM-driven display layer.

---

## PART H — FINAL RECOMMENDATIONS

**1. Safest system to clone as V2 experiment:**
`backend/services/daily_alpha_board_service.py`

Reasons: fully deterministic (no LLM in scoring loop), multi-source (8 collectors), available-weight normalized (handles missing signals gracefully), TTL cache architecture already proven, clear signal → weight → score → rank pipeline.

**2. Functions/services that should remain canonical and reused unchanged:**
- `stage_analysis.py → _classify_stage()` — deterministic, well-tested
- `theme_rs_service.py` — canonical theme state and RS (including breadth and acceleration)
- `news_signal_scorer.py` — deterministic keyword-rule scorer, O(1), no LLM
- `alert_signal_bus.py` — keep as passive bus; do not add V2 scoring to it
- `options_net_premium_history.py` — canonical net flow delta history
- `sectors_chain_summarizer.py → scan_batch_for_sectors()` — canonical options data producer
- `earnings_clean_service.py` — canonical catalyst/earnings snapshot

**3. Current signals safe to use immediately in V2:**
- Market regime (label + confidence) — `regime_engine`
- Theme RS score and state — `themes_rs_lkg.json`
- Theme RS acceleration and breadth — `themes_rs_lkg.json`
- Ticker stage (Weinstein) — `watchlist_stage2_lkg.json`
- Options composite score — `options_master_lkg_v1.json`
- Net flow (canonical 7–60 DTE scope only) — supplement LKG + `options_net_premium_daily`
- Net flow deltas (1D/7D/30D) — `options_net_premium_daily` Neon table
- Earnings proximity / `days_to_earnings` — `earnings_snap_*.json`
- News score (keyword-rule) — `news_signal_scorer.py`
- Volume/market cap ratio — `x_consensus_weekly.json`

**4. Signals that must be fixed before V2 can trust them:**
1. **`rs_score` naming collision** — rename to `price_rs_8w_spy` (stage LKG) and `social_rank` (social screener) before V2 consumes either
2. **`technical_score` staleness** — must check `updated_at` age before trusting; define max-age (suggest 24h); fallback to stage-only signal after expiry
3. **Social signal canonical source** — define whether canonical social is the weekly file or live Grok; align both consumers to the same source
4. **Fundamental data provenance** — pick one canonical source per field (FMP TTM for ratios; EDGAR for growth rates); document CSV override scope
5. **Stage 3m false positive rate** — the 20% extension threshold is absolute and not volatility-adjusted; needs confirmation period or volatility normalization before V2 applies penalty scoring

**5. Major signal categories genuinely missing:**
- Analyst recommendations and price targets (no source wired to any scoring engine)
- Forward revenue/EPS consensus estimates
- EV/EBITDA (not stored)
- Options-flow acceleration (delta-of-delta over time — deltas exist in Neon but not surfaced in any scoring engine)
- Social acceleration as a standalone scored signal (computed in `social_screener_service.py` but not consumed in Daily Alpha or zones)
- Theme rotation / state-transition signal (no alert or score for theme moving from Neutral→Leading or Leading→Lagging)
- Support/resistance proximity as a numeric signal (only in LLM-generated text; no structured level)
- Prediction market signals (Polymarket/Kalshi output not consumed by any scoring engine)
- Watchlist/portfolio membership as an explicit scored weight (currently only determines entry to candidate pool, not score)
- Insider activity as a structured scored signal (Form 4 consumed only by LLM in the watchlist analysis path)

**6. Should Daily Alpha V1 remain frozen during V2 experiment?**
YES. Daily Alpha V1 is deterministic, fast (TTL-cached), has no LLM in the scoring loop, and is currently the most reliable signal surface. V2 should be a *separate* endpoint returning a parallel ranked list. Daily Alpha V1 must not be touched until V2 has been validated side-by-side for ≥ 2 weeks.

**7. Smallest architectural boundary for Confluence V2 that avoids changing existing behavior:**
New module `backend/services/confluence_v2_service.py` with its own LKG file `confluence_v2_lkg.json`, exposed on a new endpoint `/api/home/confluence-v2`. Reads from the same canonical source files with no new providers. Does not write to, invalidate, or modify any existing LKG, cache, Neon table, or scheduled loop.

---

```
CONFLUENCE AUDIT VERDICT
════════════════════════════════════════════════════════════════

BEST BASE TO CLONE:
  backend/services/daily_alpha_board_service.py
  (deterministic, multi-source, available-weight normalized, TTL-cached,
  no LLM in scoring, proven pipeline architecture)

FREEZE UNCHANGED:
  - GET /api/home/daily-alpha-board and its entire scoring pipeline
  - All 8 collect_*_cache_candidates() functions (read-only; do not modify)
  - alert_signal_bus.py (keep as passive bus)
  - All existing LKG files and their producer services
  - All existing Neon table schemas
  - options_net_premium_daily table and upsert logic
  - All scheduled background loops in main.py

SAFE INPUTS NOW:
  - Market regime (label + confidence) — regime_engine
  - Theme RS score and state (Leading/Emerging/Neutral/Lagging) — themes_rs_lkg.json
  - Theme RS acceleration — themes_rs_lkg.json
  - Theme breadth — themes_rs_lkg.json
  - Ticker stage (Weinstein 30w SMA) — watchlist_stage2_lkg.json
  - Options composite score — options_master_lkg_v1.json
  - Net flow canonical (7–60 DTE scope, net_flow_single_expiry_7_60dte_v1 only)
  - Net flow deltas 1D/7D/30D — options_net_premium_daily Neon
  - Earnings proximity / days_to_earnings — earnings_snap_*.json
  - News score (keyword-rule) — news_signal_scorer.py
  - Volume/market cap ratio — x_consensus_weekly.json
  - Social consensus score (define ONE canonical source first)

FIX BEFORE V2:
  1. rs_score naming collision (stage LKG vs social screener — same name, different units,
     different scales, currently consumed interchangeably in Daily Alpha)
  2. technical_score staleness gating (must check updated_at age ≤24h before trusting;
     fallback to stage-only if stale)
  3. Social signal canonical source definition (weekly file vs live Grok — pick one;
     currently two consumers read two different sources)
  4. Fundamental data provenance (FMP vs EDGAR vs CSV — pick winner per field; document
     where each override applies)
  5. Stage 3m false positive rate (20% absolute extension threshold fires on strong breakouts;
     needs volatility normalization or duration qualification before V2 applies penalties)

GENUINELY MISSING:
  - Analyst recommendations and price targets
  - Forward revenue/EPS consensus estimates
  - EV/EBITDA
  - Options-flow acceleration (delta-of-delta) as scored signal
  - Social acceleration as standalone scored signal (computed but not consumed in scoring)
  - Theme rotation / state-transition signal
  - Support/resistance proximity as numeric scored input
  - Prediction market signals in any scoring engine
  - Watchlist/portfolio membership as explicit scored weight
  - Insider activity (Form 4) as structured scored signal
  - Close Watch / Favorite membership signal

TOP 5 ARCHITECTURAL DRIFT RISKS:
  1. rs_score name collision — Daily Alpha may be scoring social rank as relative strength
     silently on every build_daily_alpha_board() call
  2. Watchlist LLM output feeding Daily Alpha — if watchlist analysis is weeks old,
     the ta/fundamental/news signals in Daily Alpha reflect stale data with no visible warning
  3. "Options signal" has three incompatible definitions across three systems — composite
     score, HL price gap, and LLM-text mention — V2 must name all three explicitly
  4. Alert engine is architecturally decoupled — a V2 confluence score has no path to the
     alert surface without a new integration bridge
  5. Zone scoring is non-deterministic — same ticker can be absent or ranked #1 across
     consecutive runs; V2 cannot inherit this as its scoring backbone

RECOMMENDED NEXT IMPLEMENTATION:
  Add `social_acceleration_score` and `net_flow_delta_1d` as two new explicit signal slots
  to a cloned `confluence_v2_service.py`, behind `/api/home/confluence-v2`, reading
  exclusively from already-computed canonical sources:
    - x_consensus_weekly.json → social_acceleration_score (currently computed, not scored)
    - options_net_premium_daily Neon → net_flow_delta_1d (exists, not consumed in scoring)
  No new providers. No new scheduled loops. No LLM calls.
  This is the smallest verifiable step that adds two genuinely missing confluence dimensions
  to a proven scoring architecture without touching any existing behavior.
```

---
*End of audit. Zero code changes made. All findings read-only.*
