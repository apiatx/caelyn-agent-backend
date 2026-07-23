---
name: EI Materials EFTS attachment index
description: EDGAR filing index endpoint is 404; correct source is efts.sec.gov; primary-doc fallback; is_primary_body classifier guard
---

## Rule
Use EFTS full-text search to get filing attachment lists. Never call `{acc_clean}-index.json` — it returns 404 NoSuchKey on all accessions.

**Why:** EDGAR does not serve a filing-level JSON index at the `-index.json` suffix. The only programmatic sources are (a) EFTS, (b) the HTML `-index.htm` (unreliable, often 503), and (c) the submissions JSON which only includes `primaryDocument`.

## EFTS endpoint
```
GET https://efts.sec.gov/LATEST/search-index?q="{accession_with_dashes}"
# e.g. q="0001679788-26-000053"
```
Response: `hits.hits[*]._id = "{accession}:{filename}"`, `_source.file_description = "EX-99.1"`, `_source.sequence = "2"`.

Construct URL: `https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{filename}`.

**Rate limit:** 2 req/s (same as SEC policy). Returns 403 (not 429) when over-requested during testing.

## Primary-doc fallback (EFTS unavailable)
When EFTS returns 403/empty, synthesize a single attachment from `primaryDocument` in the submissions JSON:
```python
raw_atts = [{"filename": pri_name, "type": f["form"], "url": pri_url, ...}]
```
Set `type = f["form"]` (e.g., "8-K") so `is_primary_body=True` in the classifier.

**How to apply:** Fallback in `fetch_and_cache_materials` after `_fetch_filing_index` returns `[]`. Gives correct earnings_release + transcript state even with EFTS blocked.

## is_primary_body classifier guard
In `_classify_attachment`, for 8-K/8-K/A/6-K:
```python
is_primary_body = (atype.upper() in (form.upper(), form.upper().rstrip("/A")))
```
Skip `investor_presentation` keyword check when `is_primary_body=True`.

**Why:** XBRL/iXBRL-formatted 8-K primary bodies embed "slide", "deck" in XML metadata, causing false `investor_presentation` classifications. Only exhibit docs (EX-99.1 etc.) should be checked for investor_presentation keywords.

## investor_presentation keywords
Include "deck", "slides", "earnings deck", "earnings presentation", "quarterly presentation", "q1/q2/q3/q4 deck". Combined with `is_primary_body` guard, these safely catch exhibit decks without misclassifying XBRL bodies.

## Data path in ticker-detail
`earnings_intelligence.materials` (NOT top-level `ei_materials`).
Admin backfill: `POST /api/watchlist/admin/ei-materials/backfill`.
Frontend contract at `backend/data/ei_frontend_contract.md`.

## Webcast URL limitation
XBRL iXBRL 8-K bodies lose href attributes during BeautifulSoup text parsing. Webcast URLs only appear in:
- Plain-text EX-99 exhibits (EFTS-dependent)
- Pre-earnings 8-K filed under item 7.01/8.01 (not the 2.02 filing itself)
`webcast_url` will be null for most XBRL-formatted 8-Ks.
