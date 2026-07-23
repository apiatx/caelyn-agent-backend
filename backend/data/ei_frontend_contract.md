# Earnings Intelligence — Frontend API Contract
**Generated:** 2026-07-23  
**Schema version:** `ei_v1`

---

## Endpoint

```
GET /api/watchlist/ticker-detail/{symbol}
```

The EI materials live under `earnings_intelligence.materials` in the ticker-detail response.

---

## Response shape

```jsonc
{
  "symbol": "COIN",
  "coverage": {
    "earnings_intelligence": true,   // whole EI block present
    "ei_materials": true             // SEC materials block present + filing data found
  },
  "earnings_intelligence": {
    "schema_version": "ei_v1",
    "source_status": { ... },        // top-level EI aggregate

    // ── SEC Materials sub-block ─────────────────────────────────────────
    "materials": {

      // ── Latest earnings packet ──────────────────────────────────────
      "latest_earnings_packet": {
        "earnings_date":   "2026-05-07",      // ISO date of the earnings 8-K filing
        "detected_at":     "2026-07-23T...",  // ISO timestamp we built the packet
        "days_since_filing": 77,              // integer days since earnings_date

        // Primary filing metadata
        "primary_filing": {
          "form":             "8-K",
          "accession_number": "0001679788-26-000053",
          "filed_date":       "2026-05-07",
          "filing_index_url": "https://www.sec.gov/Archives/edgar/data/..."
        },

        // ── Earnings release (press release / shareholder letter) ──────
        // null when no earnings-release attachment found
        "earnings_release": {
          "filename":                  "coin-20260507.htm",
          "document_type":             "8-K",          // EDGAR file_description (EX-99.1 etc.)
          "description":               "8-K",
          "document_url":              "https://www.sec.gov/Archives/edgar/...",
          "classification":            "earnings_release",
          "classification_confidence": "high",          // high|medium|low
          "classification_method":     "item202_earnings_kw",
          "text_inspected":            true             // body text was fetched & parsed
        },

        // ── Investor presentation (deck / slides) ─────────────────────
        // null when no presentation attachment found  
        // NOTE: requires EFTS (efts.sec.gov) to be available; falls back to null
        //       when EFTS is rate-limited. Full exhibit list is only available via EFTS.
        "investor_presentation": null,

        // ── Supplemental tables ────────────────────────────────────────
        "supplemental_tables": [],    // array of attachment docs (same shape as earnings_release)

        // ── Guidance documents ─────────────────────────────────────────
        "guidance_documents": [],

        // ── Prepared remarks ──────────────────────────────────────────
        "prepared_remarks": null,

        // ── Related financial report (10-Q / 10-K / 20-F) ────────────
        "related_financial_report": {
          "form":             "10-Q",
          "accession_number": "0001679788-26-000041",
          "filed_date":       "2026-05-07",
          "accepted_at":      "2026-05-07T20:45:00",
          "filing_index_url": "https://www.sec.gov/Archives/edgar/data/...",
          "classification":   "financial_report",
          "source":           "sec_edgar"
        },

        // ── Webcast URL ───────────────────────────────────────────────
        // null when not found in the filing body text.
        // XBRL/iXBRL formatted 8-K bodies do not expose hrefs in parsed text;
        // webcast URLs are more reliably present in plain-text EX-99 exhibits
        // (EFTS-dependent). This field will populate for companies that include
        // the URL in the earnings press release body.
        "webcast_url":                    null,
        "webcast_source_document":        null,
        "webcast_extraction_confidence":  "none",   // "high"|"medium"|"low"|"none"

        // ── Transcript ────────────────────────────────────────────────
        "transcript": {
          // status values:
          //   "available_sec_exhibit"   — transcript found as SEC filing attachment
          //   "not_yet_available"       — earnings filed < 5 calendar days ago
          //   "unavailable"             — ≥ 5 days since filing, no transcript found
          //   "unknown"                 — filing date could not be parsed
          "status":      "unavailable",     // or "not_yet_available" if recent
          "source_type": null,              // "sec_exhibit" | "sec_exhibit_unconfirmed" | null
          "source_url":  null
        }
      },

      // ── Recent filings (most-recent first, up to 40) ────────────────
      "recent_filings": [
        {
          "form":             "8-K",
          "category":         "earnings",    // earnings|current_reports|insider|financial_report|...
          "filed_date":       "2026-07-23",
          "accepted_at":      "2026-07-23T07:30:00",
          "accession_number": "0000764065-26-000097",
          "title":            "RESULTS OF OPERATIONS AND FINANCIAL CONDITION",
          "items":            "2.02,9.01",   // null for non-8-K forms
          "filing_index_url": "https://www.sec.gov/Archives/edgar/data/...",
          "primary_document_url": "https://www.sec.gov/Archives/edgar/data/...",
          "attachments": [
            {
              "filename":                  "clf-20260723.htm",
              "document_type":             "8-K",
              "description":               "RESULTS OF OPERATIONS...",
              "document_url":              "https://www.sec.gov/Archives/edgar/data/...",
              "classification":            "earnings_release",
              "classification_confidence": "high",
              "classification_method":     "item202_earnings_kw",
              "text_inspected":            true
            }
            // Additional EX-99.1, EX-99.2 etc. appear here when EFTS is available
          ]
        }
        // ... up to 40 filings
      ],

      // ── Source status ─────────────────────────────────────────────────
      "source_status": {
        "cik":                   "0000764065",   // zero-padded CIK
        "fetched_at":            "2026-07-23T...",
        "coverage":              true,
        "filing_count":          31,             // total in lookback window
        "earnings_8k_count":     3,              // 8-Ks with item 2.02
        "errors":                {},             // empty on success

        // Aggregate classification counts across all attachments in all filings
        "classification_counts": {
          "earnings_release":     3,
          "financial_report":     2,
          "investor_presentation": 0,
          "other":                2
        },

        // Transcript state counts from all earnings 8-Ks in window
        "transcript_state_counts": {
          "not_yet_available": 1    // or "unavailable": N, "available_sec_exhibit": N
        }
      }
    },

    // ── EI aggregate fields (populated by earnings_intelligence service) ──
    "reaction_summary": { ... },    // pre/post price reaction stats
    "earnings_history": [ ... ],    // historical EPS/revenue beats
    "ratings":          { ... },    // analyst ratings snapshot
    "sec_filings":      [ ... ]     // other SEC filings (pre-existing)
  }
}
```

---

## Classification vocabulary

| `classification`       | Meaning |
|------------------------|---------|
| `earnings_release`     | Press release or 8-K body with results of operations (item 2.02) |
| `investor_presentation`| Deck / slides / investor day materials |
| `supplemental_tables`  | Supplemental financial data tables |
| `transcript`           | Earnings call transcript |
| `prepared_remarks`     | Pre-released management remarks |
| `corporate_guidance`   | Forward guidance document |
| `webcast_or_replay`    | Webcast link / replay document |
| `financial_report`     | 10-K / 10-Q / 20-F / annual report |
| `ownership_filing`     | SC 13D / SC 13G |
| `proxy`                | DEF 14A / proxy statement |
| `offering_document`    | S-1 / S-3 / 424B* |
| `transaction_material` | 425 / M&A related |
| `other`                | Unclassified |

## Confidence levels

| `classification_confidence` | Meaning |
|-----------------------------|---------|
| `high`   | Text inspected + strong keyword match or deterministic form-type match |
| `medium` | Keyword match without text, or exhibit-type-only signal |
| `low`    | Fallback rule (e.g. item 2.02 with no confirming keywords) |

## Category vocabulary (recent_filings[*].category)

`earnings` | `current_reports` | `financial_report` | `insider` | `offerings` | `proxy` | `ownership_filings` | `transactions` | `other`

---

## Transcript status semantics

| `status`                  | When set |
|---------------------------|----------|
| `available_sec_exhibit`   | Transcript found as EDGAR exhibit in an 8-K |
| `not_yet_available`       | Earnings filing < 5 calendar days ago (still in processing window) |
| `unavailable`             | ≥ 5 days since earnings filing, no transcript found |
| `unknown`                 | Filing date could not be parsed |

---

## Known limitations (as of 2026-07-23)

1. **EFTS dependency for exhibit lists**: The full attachment list (EX-99.1, EX-99.2 etc.) requires `efts.sec.gov` (EDGAR full-text search). When EFTS is rate-limited (403), only the primary document is classified via the submissions JSON fallback. `investor_presentation` and `supplemental_tables` will be `null`/`[]` in degraded mode; `earnings_release` and transcript state remain correct.

2. **Webcast URL**: Most companies using XBRL/iXBRL-formatted 8-Ks do not expose webcast URLs in the body text (hrefs are embedded in XML namespaces and lost during HTML parsing). The URL is more reliably found in plain-text EX-99 exhibits when EFTS is available. The webcast is often in a separate 8-K filed under item 7.01/8.01 (pre-announcement). Future enhancement: scan related pre-earnings 8-Ks for webcast URLs.

3. **Cache TTL**: Disk cache at `backend/data/edgar_disk_cache/ei_materials.json`. TTL is 24h. Background loop refreshes all watchlist symbols daily. Admin endpoint `POST /api/watchlist/admin/ei-materials/backfill` forces immediate refresh.

4. **Foreign issuers**: Symbols with `:` prefix (e.g. `LON:SHEL`) are excluded from EI materials — they require different EDGAR form types (20-F, 6-K) and may not have a US CIK. The CIK resolver returns null and the materials block is absent.

5. **Ineligible symbols**: ETFs, indices, crypto, and symbols for which EDGAR CIK resolution fails will have `coverage.ei_materials = false` and `earnings_intelligence.materials` absent.

---

## Admin endpoints

```
POST /api/watchlist/admin/ei-materials/backfill
Authorization: Bearer {ADMIN_PASSWORD}
Content-Type: application/json

Body:
{
  "symbols": ["COIN", "AAPL"],   // optional — omit for all watchlist symbols
  "force": true                   // bypass TTL check
}

Response:
{
  "status": "started",
  "total": 2,
  "symbols": ["COIN", "AAPL"]
}
```
