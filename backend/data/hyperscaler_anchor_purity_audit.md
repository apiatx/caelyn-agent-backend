
# HYPERSCALER ANCHOR CATALYST SOURCE PURITY REPORT
Generated: 2026-07-15T19:12:52Z | Source: catalyst_alignment_lkg.json, caelyn_confluence_v42.py, news_signal_scorer.py

---

## PART 1 — SOURCE TRACE FOR HYPERSCALER_ANCHOR

### Storage format note

`catalyst_primary_event.event_type` is stored **lowercase** (`hyperscaler_anchor`) in the LKG JSON.
At score time, `caelyn_confluence_v42.py` line 936 normalises it:

```python
et_raw = (primary_event.get("event_type") or "").upper().strip()
```

The TIER_A set uses uppercase — the case conversion is confirmed correct.

### All HYPERSCALER_ANCHOR symbols (n=8)

| Symbol | Cat pts | Source | PE source | Published at | Age (h) | Age (d) | Freshness bucket |
|---|---|---|---|---|---|---|---|
| ANET | 15.00 | rss_v2 | rss | 2026-07-15T08:13:05Z | 11.0 | 0.46 | **last_48h** |
| COHR | 15.00 | rss_v2 | rss | 2026-07-14T21:02:23Z | 22.2 | 0.92 | **last_48h** |
| ELVA | 15.00 | rss_v2 | rss | 2026-07-15T18:04:57Z | 1.1 | 0.05 | **last_48h** |
| ORCL | 15.00 | rss_v2 | rss | 2026-07-15T11:10:26Z | 8.0 | 0.34 | **last_48h** |
| AMAT | 14.82 | rss_v2 | rss | 2026-07-14T14:13:58Z | 29.0 | 1.21 | **last_48h** |
| TSM | 14.73 | rss_v2 | rss | 2026-07-13T10:58:11Z | 56.2 | 2.34 | **last_3d** |
| NOK | 13.27 | rss_v2 | rss | 2026-07-15T16:40:59Z | 2.5 | 0.11 | **last_48h** |
| IREN | 13.68 | rss_v2_plus_theme_policy | rss | 2026-07-12T07:59:12Z | 83.2 | 3.47 | **last_14d** |

### Full per-symbol detail

**ANET (15.00 pts)**
- Title: "Arista Networks (ANET) Is Up 9.7% After Boosting 2024 Revenue Outlook On Surging AI Demand"
- Source: rss_v2 | mat=0.87 | conf=0.75 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR, COMMERCIAL_CONTRACT
- Modifier breakdown: base=80, mat=+7.4, conf=+2.5, relev=+6, fresh=+6, art=+0 → direct=100 → 15/15
- Hyperscaler entity matched: Arista itself is the anchor; revenue guidance driven by hyperscaler demand

**COHR (15.00 pts)**
- Title: "Coherent (COHR) Is Down 8.4% After NVIDIA-Backed AI Optics Expansion News - Has The Bull Case..."
- Source: rss_v2 | mat=0.87 | conf=0.60 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR, COMMERCIAL_CONTRACT, STRATEGIC_PARTNERSHIP
- Modifier breakdown: base=80, mat=+7.4, conf=+1.0, relev=+6, fresh=+6, art=+0 → direct=100 → 15/15
- Hyperscaler entity matched: NVIDIA partnership

**ELVA (15.00 pts)**
- Title: "Electrovaya Stock (ELVA) Soars on a New Amazon (AMZN) Battery Partnership - TipRanks"
- Source: rss_v2 | mat=0.87 | conf=0.79 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=2
- Reason codes: HYPERSCALER_ANCHOR
- Modifier breakdown: base=80, mat=+7.4, conf=+2.9, relev=+6, fresh=+6, art=+2 → direct=100 → 15/15
- Hyperscaler entity matched: Amazon partnership (explicit named deal)

**ORCL (15.00 pts)**
- Title: "Oracle shares gain after report it leads race for Japanese government cloud contract (ORCL)"
- Source: rss_v2 | mat=0.87 | conf=0.75 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR, EARNINGS_GUIDANCE, MNA
- Modifier breakdown: base=80, mat=+7.4, conf=+2.5, relev=+6, fresh=+6, art=+0 → direct=100 → 15/15
- Hyperscaler entity matched: Oracle is itself the hyperscaler anchor

**AMAT (14.82 pts)**
- Title: "Applied Materials (AMAT) Lands 10 Year TSMC AI Packaging Partnership"
- Source: rss_v2 | mat=0.714 | conf=0.75 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR, EARNINGS_GUIDANCE
- Modifier breakdown: base=80, mat=+4.3, conf=+2.5, relev=+6, fresh=+6, art=+0 → direct=98.8 → 14.82/15
- Hyperscaler entity matched: TSMC partnership (TSMC is in hyperscaler anchor entity list)

**TSM (14.73 pts)**
- Title: "TSMC Delivers Record Second-Quarter Revenue as AI Chip Demand Accelerates (TSM)"
- Source: rss_v2 | mat=0.566 | conf=0.79 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=2
- Reason codes: HYPERSCALER_ANCHOR, EARNINGS_GUIDANCE
- Modifier breakdown: base=80, mat=+1.3, conf=+2.9, relev=+6, fresh=+6, art=+2 → direct=98.2 → 14.73/15
- Hyperscaler entity matched: TSMC itself; earnings growth driven by hyperscaler demand

**NOK (13.27 pts)**
- Title: "Nokia Unveils AI-Powered Network Platform in Major Nvidia Partnership"
- Source: rss_v2 | mat=0.399 | conf=0.75 | relev=0.5 (AMBIGUOUS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR, STRATEGIC_PARTNERSHIP, COMMERCIAL_CONTRACT
- Modifier breakdown: base=80, mat=−2.0, conf=+2.5, relev=+2 (ambiguous match), fresh=+6, art=+0 → direct=88.5 → 13.27/15
- Note: relevance=0.5 because "Nokia" not found as explicit ticker symbol in title — correctly penalised

**IREN (13.68 pts)**
- Title: "IREN (IREN), BE Networks Partner to Simulate NVIDIA Blackwell GPU Infrastructure"
- Source: rss_v2_plus_theme_policy | mat=0.435 | conf=0.75 | relev=1.0 (EXPLICIT_SYM_PARENS) | articles=1
- Reason codes: HYPERSCALER_ANCHOR
- Modifier breakdown: base=80, mat=−1.3, conf=+2.5, relev=+6, fresh=+4 (4–14d window), art=+0 → direct=91.2 → 13.68/15
- Note: 83h old → freshness drops to +4 instead of +6; correctly differentiated from sub-48h events

### Is HYPERSCALER_ANCHOR coming from rss_v2/news/event data?

**YES — 100% of 8 HYPERSCALER_ANCHOR events are sourced from rss_v2 (7) or rss_v2_plus_theme_policy (1). All have real news headlines and published_at timestamps. No static or hardcoded source found.**

---

## PART 2 — STATIC BOTTLENECK LEAKAGE CHECK

### Code paths searched

| Pattern | Searched in | Result |
|---|---|---|
| `bottleneck` | `backend/services/*catalyst*` | **0 matches** |
| `supply_chain` | `backend/services/*catalyst*` | **0 matches** |
| `anchor_supplier` | `backend/services/*catalyst*` | **0 matches** |
| `hyperscaler_supplier` | `backend/services/*catalyst*` | **0 matches** |
| `anchor_count` | `backend/services/*catalyst*` | **0 matches** |
| `known_supplier` | `backend/services/*catalyst*` | **0 matches** |
| `bottleneck` | `backend/*catalyst*` (glob) | **0 matches** |
| `curated_anchor` | `backend/*catalyst*` (glob) | **0 matches** |

### `_HYPERSCALER_ANCHOR_SYMBOLS` in `news_signal_scorer.py` — not a catalyst creator

`backend/services/news_signal_scorer.py` lines 59–78 defines `_HYPERSCALER_ANCHOR_SYMBOLS`:
```python
_HYPERSCALER_ANCHOR_SYMBOLS: dict[str, str] = {
    "Arista": "ANET",  "TSMC": "TSM",  "Oracle": "ORCL",  ...
}
```
This dict's **only** function is `resolve_anchor_symbols()` — a display helper that maps entity names to public tickers for `hyperscaler_articles` in the watchlist news panel. It is NOT called by any catalyst scoring function and does NOT write to `catalyst_primary_event`, `catalyst_event_type`, or `catalyst_alignment_points`.

The `HYPERSCALER_ANCHOR` event type is assigned by the **upstream catalyst classifier service** (not in this repo) when a news article contains a hyperscaler entity mention (e.g. NVIDIA, Amazon, TSMC) in the context of a deal or partnership.

### Bottleneck bonus pathway (confirmed separate)

`_score_bottleneck_bonus_v42()` in `caelyn_confluence_v42.py`:
- Reads `bottleneck_map` parameter (dict of symbol → bottleneck data)
- Writes to `bn_bonus["points"]` (max 10)
- Is summed into `bonus_score`, never into `catalyst_alignment_points`
- Has zero code overlap with `_score_catalyst_alignment_v42()`

**No leakage found. The two paths are completely separate.**

---

## PART 3 — EVENT RECENCY CHECK

### Per-symbol age and freshness modifier

| Symbol | Published at | Age (hours) | Age (days) | Bucket | Freshness mod applied |
|---|---|---|---|---|---|
| ELVA | 2026-07-15T18:04Z | **1.1h** | 0.05d | last_48h | **+6** |
| ORCL | 2026-07-15T11:10Z | **8.0h** | 0.34d | last_48h | **+6** |
| ANET | 2026-07-15T08:13Z | **11.0h** | 0.46d | last_48h | **+6** |
| NOK | 2026-07-15T16:40Z | **2.5h** | 0.11d | last_48h | **+6** |
| COHR | 2026-07-14T21:02Z | **22.2h** | 0.92d | last_48h | **+6** |
| AMAT | 2026-07-14T14:13Z | **29.0h** | 1.21d | last_48h | **+6** |
| TSM | 2026-07-13T10:58Z | **56.2h** | 2.34d | **last_3d** | **+6** |
| IREN | 2026-07-12T07:59Z | **83.2h** | 3.47d | **last_14d** | **+4** |

*Freshness window: Phase B uses 72-hour cutoff for +6 (not 48h). TSM at 56h → still gets +6. IREN at 83h → drops to +4.*

### Does HYPERSCALER_ANCHOR mean "last 48 hours"?

**NO — it means "recent cached news event where the catalyst classifier matched a hyperscaler entity in the article."**

6 of 8 symbols happen to be within 48h today (2026-07-15 snapshot), but:
- TSM is 56h old → last_3d, still gets +6 freshness (72h window)
- IREN is 83h old → last_14d, gets +4 freshness, scores 13.68 (not 15)

The freshness modifier is continuous, not a binary gate. A HYPERSCALER_ANCHOR event at 10 days old would still score +2 (15–45d window) and reach ~12.0 pts. At 46+ days it would receive +0 and score ~11.2 pts. There is no hard cutoff that suppresses the event entirely.

---

## PART 4 — PURITY RULE RECOMMENDATION

### Current state

The system already enforces clean separation in practice:
- HYPERSCALER_ANCHOR is set by the upstream catalyst classifier (news-based)
- Bottleneck bonus is set by `_score_bottleneck_bonus_v42()` (bottleneck_map data)
- No code path allows static anchor membership to write to `catalyst_event_type`

### Recommended explicit rule (for code comment / future-proofing)

Add the following as a docstring to `_catalyst_phb_direct_score()`:

> **HYPERSCALER_ANCHOR purity rule:**
> `event_type = HYPERSCALER_ANCHOR` is only valid when sourced from `rss_v2`, `rss_v2_plus_theme_policy`, or `scheduled_event` with a populated `published_at` or `catalyst_date`.
> Static bottleneck/anchor-map membership must never set this field.
> Static membership → `bottleneck_bonus` only.

### Additional guard (not currently needed but recommended if classifier ever changes)

At the top of `_catalyst_phb_direct_score()`, add a source purity assertion for TIER_A events:

```python
# PURITY GUARD — TIER_A events must have a news source and a date
if et_raw in _TIER_A and source not in ("rss_v2", "rss_v2_plus_theme_policy", "scheduled"):
    # Demote to TIER_C if no news backing
    et_raw = "COMMERCIAL_CONTRACT"
    reason_codes.append("TIER_A_SOURCE_DEMOTION")
```

This guard would fire silently if the upstream classifier ever started producing HYPERSCALER_ANCHOR from a static/non-news source. Currently: **not needed** — all 8 symbols pass the check.

### What belongs where

| Signal | Correct home | Must NOT appear in |
|---|---|---|
| Company news mentions hyperscaler entity | `catalyst_event_type = HYPERSCALER_ANCHOR` | Bottleneck bonus |
| Confirmed supply-chain bottleneck | `bottleneck_bonus.points` (max 10) | `catalyst_alignment_points` |
| Hyperscaler is a known customer | `investment_alignment` or `theme_alignment` | `catalyst_event_type` |
| Static anchor map membership | `bottleneck_bonus`, `reason_codes` | `catalyst_primary_event` |

---

## PART 5 — FINAL REPORT SUMMARY TABLES

### HYPERSCALER_ANCHOR symbols (n=8, 2026-07-15 snapshot)

| Symbol | Cat pts | Tier | Source | Age | Freshness | mat | conf | relev | Headline excerpt |
|---|---|---|---|---|---|---|---|---|---|
| ANET | 15.00 | TIER_A | rss_v2 | 11h | +6 | 0.87 | 0.75 | 1.0 | Revenue outlook raised on AI demand |
| COHR | 15.00 | TIER_A | rss_v2 | 22h | +6 | 0.87 | 0.60 | 1.0 | NVIDIA-backed AI optics expansion |
| ELVA | 15.00 | TIER_A | rss_v2 | 1h | +6 | 0.87 | 0.79 | 1.0 | New Amazon battery partnership |
| ORCL | 15.00 | TIER_A | rss_v2 | 8h | +6 | 0.87 | 0.75 | 1.0 | Japanese government cloud contract |
| AMAT | 14.82 | TIER_A | rss_v2 | 29h | +6 | 0.71 | 0.75 | 1.0 | 10-year TSMC AI packaging deal |
| TSM | 14.73 | TIER_A | rss_v2 | 56h | +6 | 0.57 | 0.79 | 1.0 | Record Q2 revenue on AI demand |
| IREN | 13.68 | TIER_A | rss_v2+ | 83h | **+4** | 0.44 | 0.75 | 1.0 | NVIDIA Blackwell GPU simulation partner |
| NOK | 13.27 | TIER_A | rss_v2 | 2.5h | +6 | 0.40 | 0.75 | **0.5** | AI platform, Nvidia partnership |

### Source trace summary

All 8 HYPERSCALER_ANCHOR events originate from `rss_v2` news articles with populated `published_at` timestamps. The event_type is assigned by the upstream catalyst classifier based on article content (hyperscaler entity mention + deal/partnership context). No static data source contributes to `catalyst_event_type`.

### Bottleneck leakage check summary

Zero matches across all bottleneck/supply-chain/anchor field names in any catalyst file. The `_HYPERSCALER_ANCHOR_SYMBOLS` dict in `news_signal_scorer.py` is a display-only entity→ticker resolver, not a catalyst writer. Bottleneck bonus and catalyst alignment are computed by completely separate functions with no shared state.

### Event recency summary

6/8 events are within 48h. 1 (TSM) is 56h (last_3d). 1 (IREN) is 83h (last_14d). The freshness modifier differentiates them: +6 for ≤72h, +4 for 4–14d. There is no binary 48h gate — the system is continuous and correctly penalises older events.

---

## HYPERSCALER ANCHOR PURITY VERDICT

```
HYPERSCALER_ANCHOR_IS_NEWS_BASED:
YES

STATIC_BOTTLENECK_DATA_USED_IN_CATALYST:
NO

ANY_LEAKAGE_FOUND:
NO

MEANS_LAST_48_HOURS:
NO — means "recent cached news event where upstream classifier matched hyperscaler entity in article";
6/8 happen to be within 48h in today's snapshot; the scoring window is 72h for +6 freshness bonus,
not a hard 48h gate; IREN at 83h correctly scores +4 (not +6) and reaches 13.68 not 15.

MEANS_RECENT_CACHED_EVENT:
YES

CATALYST_AND_BOTTLENECK_PROPERLY_SEPARATED:
YES — zero code overlap between _score_catalyst_alignment_v42() and _score_bottleneck_bonus_v42();
separate input sources, separate output fields, no shared state

PATCH_NEEDED:
NO — recommend adding a docstring purity rule and an optional source guard
as a future-proofing measure, but no functional bug exists today
```
