"""
EI Materials Service — SEC filing fetch, classify, and cache for earnings intelligence.

Architecture:
  Background refresh only — never called at request time.
  Uses SecEdgarProvider for CIK resolution and filing fetch.
  Writes to ei_materials_cache.py (disk, keyed by symbol).
  ticker_detail_endpoint reads from disk cache — zero provider calls.

Forms monitored:
  10-K, 10-K/A, 10-Q, 10-Q/A, 8-K, 8-K/A, Form 4,
  SC 13D, SC 13D/A, SC 13G, SC 13G/A, 6-K, 20-F, 20-F/A,
  DEF 14A, S-1, S-1/A, 425, 424B1–B5

Classification uses deterministic keyword+form+items scoring.
LLM fallback is NOT used by default.
"""
from __future__ import annotations

import asyncio
import math
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

_EDGAR_HEADERS = {
    "User-Agent": "TradingAnalysisPlatform/1.0 (contact: apixbt@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json",
}
_DATA_URL = "https://data.sec.gov"
_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
_TOKEN_BUCKET_RATE = 2.0     # 2 req/sec — conservative vs SEC's 10 req/sec limit
_INDEX_LOOKBACK_DAYS = 180   # fetch attachment index for filings in this window
_RECENT_FILINGS_LIMIT = 40   # max filing rows from submissions API

# ── Form-type mappings ───────────────────────────────────────────────────────

_FORM_CATEGORY_MAP: dict[str, str] = {
    "10-K":     "financial_reports",
    "10-K/A":   "financial_reports",
    "10-Q":     "financial_reports",
    "10-Q/A":   "financial_reports",
    "20-F":     "financial_reports",
    "20-F/A":   "financial_reports",
    "8-K":      "current_reports",
    "8-K/A":    "current_reports",
    "6-K":      "current_reports",
    "4":        "insider",
    "4/A":      "insider",
    "SC 13D":   "ownership",
    "SC 13D/A": "ownership",
    "SC 13G":   "ownership",
    "SC 13G/A": "ownership",
    "S-1":      "offerings",
    "S-1/A":    "offerings",
    "S-3":      "offerings",
    "S-3/A":    "offerings",
    "424B1":    "offerings",
    "424B2":    "offerings",
    "424B3":    "offerings",
    "424B4":    "offerings",
    "424B5":    "offerings",
    "DEF 14A":  "governance",
    "DEFA14A":  "governance",
    "425":      "transactions",
}

_MONITORED_FORMS = frozenset(_FORM_CATEGORY_MAP.keys())

# Forms for which we fetch the full attachment index from EDGAR
_DEEP_INDEX_FORMS = frozenset({
    "8-K", "8-K/A", "6-K", "10-K", "10-K/A", "10-Q", "10-Q/A",
    "20-F", "20-F/A", "DEF 14A", "S-1", "S-1/A",
})

# ── Keyword classifiers ──────────────────────────────────────────────────────

_EARNINGS_RELEASE_KW = [
    "earnings release", "quarterly results", "financial results",
    "quarterly financial", "results of operations", "q1 results", "q2 results",
    "q3 results", "q4 results", "fourth quarter results", "third quarter results",
    "second quarter results", "first quarter results", "annual results",
    "full year results", "fiscal year results",
]
_INVESTOR_PRESENTATION_KW = [
    "investor presentation", "corporate presentation", "company presentation",
    "investor day", "analyst day", "capital markets day",
]
_SUPPLEMENTAL_KW = [
    "supplemental", "financial supplement", "data supplement",
    "statistical supplement", "key metrics", "supplemental data",
    "operating statistics", "supplemental financial",
]
_TRANSCRIPT_KW = [
    "transcript", "earnings call transcript", "conference call transcript",
    "earnings conference", "call transcript",
]
_GUIDANCE_KW = [
    "guidance", "outlook", "forecast", "forward-looking", "full year outlook",
    "annual guidance", "revenue guidance", "updated guidance",
]
_WEBCAST_KW = [
    "webcast", "conference call", "live audio", "listen to call",
    "replay", "earnings call", "investor call", "live broadcast",
]
_PREPARED_REMARKS_KW = [
    "prepared remarks", "prepared statement", "conference call remarks",
    "management remarks",
]
_PROXY_KW = [
    "proxy statement", "definitive proxy", "annual meeting", "def 14a",
]
_OFFERING_KW = [
    "prospectus", "offering", "underwriting", "registration statement",
    "424b", "s-1", "s-3",
]
_TRANSACTION_KW = [
    "merger", "acquisition", "business combination", "tender offer",
    "transaction", "proposed transaction", "425",
]


def _kw_score(text: str, keywords: list[str]) -> int:
    """Count how many keywords appear in text (case-insensitive)."""
    t = text.lower()
    return sum(1 for kw in keywords if kw in t)


def _classify_attachment(
    filing_form: str,
    items_str: str,          # "2.02,9.01" etc — only meaningful for 8-K/6-K
    att_type: str,           # exhibit type field from filing index
    att_desc: str,           # attachment description
    filename: str,
    doc_title: str = "",
) -> tuple[str, str, str]:
    """
    Deterministically classify one attachment.

    Returns (classification, confidence, method):
      classification: one of the supported attachment classification labels
      confidence: "high" | "medium" | "low"
      method: human-readable explanation of the rule that fired
    """
    form = (filing_form or "").upper().strip()
    items = items_str or ""
    atype = (att_type or "").upper().strip()
    desc = (att_desc or "").lower().strip()
    fname = (filename or "").lower().strip()
    title = (doc_title or "").lower().strip()
    combined = f"{desc} {title} {fname}"

    # ── Form-level overrides: entire filing belongs to one category ──────────
    if form in ("4", "4/A"):
        return "insider_filing", "high", "form_type_4"
    if form in ("SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A"):
        return "ownership_filing", "high", "form_type_sc13"
    if form in ("DEF 14A", "DEFA14A"):
        return "proxy", "high", "form_type_def14a"
    if form == "425":
        return "transaction_material", "high", "form_type_425"
    if form.startswith("424B"):
        return "offering_document", "high", "form_type_424b"
    if form in ("S-1", "S-1/A", "S-3", "S-3/A"):
        return "offering_document", "high", "form_type_s1_s3"

    # ── 8-K / 8-K/A / 6-K — items-driven classification ────────────────────
    if form in ("8-K", "8-K/A", "6-K"):
        has_202 = "2.02" in items  # Results of Operations
        has_701 = "7.01" in items  # Regulation FD
        has_801 = "8.01" in items  # Other Events

        # Earnings-related: item 2.02 present AND keyword confirms
        if has_202:
            er_score = _kw_score(combined, _EARNINGS_RELEASE_KW)
            tr_score = _kw_score(combined, _TRANSCRIPT_KW)
            pr_score = _kw_score(combined, _PREPARED_REMARKS_KW)
            sup_score = _kw_score(combined, _SUPPLEMENTAL_KW)
            pres_score = _kw_score(combined, _INVESTOR_PRESENTATION_KW)

            if tr_score >= 1:
                return "transcript", "medium", "item202_transcript_kw"
            if pr_score >= 1:
                return "prepared_remarks", "medium", "item202_remarks_kw"
            if sup_score >= 1:
                return "supplemental_tables", "medium", "item202_supplemental_kw"
            if pres_score >= 1:
                return "investor_presentation", "medium", "item202_presentation_kw"
            if er_score >= 1:
                return "earnings_release", "high", "item202_earnings_kw"
            # item 2.02 but no confirming keyword — still likely earnings-related
            return "earnings_release", "medium", "item202_no_confirming_kw"

        # Non-2.02 8-K — classify by keywords
        pres_score = _kw_score(combined, _INVESTOR_PRESENTATION_KW)
        tr_score = _kw_score(combined, _TRANSCRIPT_KW)
        guid_score = _kw_score(combined, _GUIDANCE_KW)
        wc_score = _kw_score(combined, _WEBCAST_KW)
        er_score = _kw_score(combined, _EARNINGS_RELEASE_KW)

        if tr_score >= 1:
            return "transcript", "medium", "8k_non202_transcript_kw"
        if pres_score >= 1:
            return "investor_presentation", "medium", "8k_non202_presentation_kw"
        if guid_score >= 2:
            return "corporate_guidance", "medium", "8k_non202_guidance_kw"
        if er_score >= 1:
            return "earnings_release", "low", "8k_non202_earnings_kw"
        return "other", "low", "8k_non202_no_match"

    # ── 10-K / 10-Q / 20-F primary document ─────────────────────────────────
    if form in ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"):
        if atype in ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"):
            return "financial_report", "high", "form_type_match"
        # Exhibits within a 10-K/Q
        sup_score = _kw_score(combined, _SUPPLEMENTAL_KW)
        if sup_score >= 1:
            return "supplemental_tables", "medium", "10kq_supplemental_kw"
        return "financial_report", "low", "10kq_exhibit_fallback"

    # ── Generic keyword scoring for remaining forms ──────────────────────────
    pres_score = _kw_score(combined, _INVESTOR_PRESENTATION_KW)
    er_score = _kw_score(combined, _EARNINGS_RELEASE_KW)
    tr_score = _kw_score(combined, _TRANSCRIPT_KW)
    sup_score = _kw_score(combined, _SUPPLEMENTAL_KW)
    guid_score = _kw_score(combined, _GUIDANCE_KW)
    wc_score = _kw_score(combined, _WEBCAST_KW)
    pr_score = _kw_score(combined, _PREPARED_REMARKS_KW)
    off_score = _kw_score(combined, _OFFERING_KW)

    scores = [
        ("earnings_release",       er_score),
        ("investor_presentation",  pres_score),
        ("transcript",             tr_score),
        ("supplemental_tables",    sup_score),
        ("corporate_guidance",     guid_score),
        ("webcast_or_replay",      wc_score),
        ("prepared_remarks",       pr_score),
        ("offering_document",      off_score),
    ]
    best_cls, best_score = max(scores, key=lambda x: x[1])
    if best_score >= 1:
        conf = "medium" if best_score >= 2 else "low"
        return best_cls, conf, f"keyword_score_{best_score}"

    return "other", "low", "no_rule_matched"


def _category_from_form(form: str) -> str:
    """Map a form type to its broad category."""
    f = (form or "").upper().strip()
    return _FORM_CATEGORY_MAP.get(f, "other")


def _is_earnings_8k(form: str, items_str: str) -> bool:
    """True when this 8-K filing is likely related to an earnings release."""
    return form.upper() in ("8-K", "8-K/A", "6-K") and "2.02" in (items_str or "")


# ── Rate limiter (module-level token bucket) ─────────────────────────────────

_tb_tokens: float = 2.0
_tb_last: float = 0.0


async def _acquire() -> None:
    global _tb_tokens, _tb_last
    now = time.time()
    if _tb_last > 0:
        _tb_tokens = min(2.0, _tb_tokens + (now - _tb_last) * _TOKEN_BUCKET_RATE)
    _tb_last = now
    if _tb_tokens >= 1.0:
        _tb_tokens -= 1.0
        return
    wait = (1.0 - _tb_tokens) / _TOKEN_BUCKET_RATE
    await asyncio.sleep(wait)
    _tb_tokens = max(0.0, _tb_tokens - 1.0)


async def _get_json(client: httpx.AsyncClient, url: str, timeout: float = 15.0) -> dict | list | None:
    """Fetch JSON with rate limiting. Returns None on error."""
    try:
        await _acquire()
        resp = await client.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            print(f"[EI_MATERIALS] Rate limited on {url} — sleeping 30s")
            await asyncio.sleep(30)
        return None
    except Exception as exc:
        print(f"[EI_MATERIALS] fetch error {url}: {exc}")
        return None


# ── CIK resolution ───────────────────────────────────────────────────────────

_cik_cache: dict[str, str] = {}
_cik_cache_ts: float = 0.0
_CIK_MAP_TTL = 86400  # 24h


async def _resolve_cik(symbol: str, client: httpx.AsyncClient) -> str | None:
    """Resolve symbol → zero-padded CIK string. Caches the full mapping for 24h."""
    global _cik_cache, _cik_cache_ts
    sym = symbol.upper().strip()
    now = time.time()

    if _cik_cache and now - _cik_cache_ts < _CIK_MAP_TTL:
        return _cik_cache.get(sym)

    try:
        await _acquire()
        resp = await client.get(
            "https://www.sec.gov/files/company_tickers.json",
            timeout=15.0,
        )
        if resp.status_code == 200:
            raw = resp.json()
            mapping: dict[str, str] = {}
            for entry in raw.values():
                t = (entry.get("ticker") or "").upper()
                c = str(entry.get("cik_str") or "").zfill(10)
                if t and c:
                    mapping[t] = c
            _cik_cache = mapping
            _cik_cache_ts = now
            return mapping.get(sym)
    except Exception as exc:
        print(f"[EI_MATERIALS] CIK map load error: {exc}")

    return _cik_cache.get(sym)


# ── Filing index fetch ───────────────────────────────────────────────────────

async def _fetch_filing_index(
    client: httpx.AsyncClient,
    cik_num: str,           # CIK without leading zeros
    accession: str,         # raw accession number "0001234567-24-000001"
) -> list[dict]:
    """
    Fetch the EDGAR filing index JSON for one accession.
    Returns list of attachment dicts with: name, type, size, description, url.
    """
    acc_clean = accession.replace("-", "")
    url = f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{acc_clean}-index.json"
    data = await _get_json(client, url)
    if not data or not isinstance(data, dict):
        return []

    items = []
    directory = data.get("directory", {})
    for item in (directory.get("item") or []):
        if not isinstance(item, dict):
            continue
        name = item.get("name") or ""
        if not name or name.endswith("/"):
            continue
        items.append({
            "filename":    name,
            "type":        item.get("type") or "",
            "size":        item.get("size") or "",
            "description": item.get("description") or "",
            "url":         f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{name}",
        })
    return items


# ── Attachment builder ───────────────────────────────────────────────────────

def _build_attachment(
    filing_form: str,
    items_str: str,
    raw_att: dict,
    cik_num: str,
    accession: str,
    acc_clean: str,
) -> dict:
    """Convert a raw filing index item into a classified attachment dict."""
    filename = raw_att.get("filename") or ""
    att_type = raw_att.get("type") or ""
    att_desc = raw_att.get("description") or ""
    url = raw_att.get("url") or f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{filename}"

    cls, conf, method = _classify_attachment(
        filing_form, items_str, att_type, att_desc, filename
    )
    return {
        "filename":                filename,
        "document_type":           att_type,
        "description":             att_desc,
        "document_url":            url,
        "classification":          cls,
        "classification_confidence": conf,
        "classification_method":   method,
    }


# ── Latest earnings packet assembler ─────────────────────────────────────────

_TRANSCRIPT_STATUS_ENUM = (
    "available_official",
    "available_sec_exhibit",
    "available_licensed",
    "processing",
    "not_yet_available",
    "unavailable",
    "unknown",
)


def _assemble_earnings_packet(
    filings_with_atts: list[dict],
) -> dict | None:
    """
    From a list of enriched filing dicts, find and assemble the latest
    earnings packet (most recent 8-K with item 2.02).
    Returns None when no earnings 8-K is present.
    """
    # Find the most recent earnings-related 8-K
    earnings_8k: dict | None = None
    for f in filings_with_atts:
        if _is_earnings_8k(f.get("form", ""), f.get("items", "")):
            earnings_8k = f
            break  # already sorted newest-first

    if not earnings_8k:
        return None

    atts = earnings_8k.get("attachments") or []

    def _first_by_cls(*classifications) -> dict | None:
        for cls in classifications:
            for a in atts:
                if a.get("classification") == cls:
                    return a
        return None

    def _all_by_cls(cls: str) -> list[dict]:
        return [a for a in atts if a.get("classification") == cls]

    primary_filing = {
        "form":             earnings_8k.get("form"),
        "accession_number": earnings_8k.get("accession_number"),
        "filed_date":       earnings_8k.get("filed_date"),
        "accepted_at":      earnings_8k.get("accepted_at"),
        "filing_index_url": earnings_8k.get("filing_index_url"),
        "items":            earnings_8k.get("items"),
    }

    er_att  = _first_by_cls("earnings_release")
    pres_att = _first_by_cls("investor_presentation")
    sup_atts = _all_by_cls("supplemental_tables")
    guid_atts = _all_by_cls("corporate_guidance")
    rem_att  = _first_by_cls("prepared_remarks")
    tr_att   = _first_by_cls("transcript")
    wc_att   = _first_by_cls("webcast_or_replay")

    def _att_doc(a: dict | None) -> dict | None:
        if not a:
            return None
        return {
            "form":            earnings_8k.get("form"),
            "accession_number": earnings_8k.get("accession_number"),
            "filed_date":      earnings_8k.get("filed_date"),
            "accepted_at":     earnings_8k.get("accepted_at"),
            "document_type":   a.get("document_type"),
            "description":     a.get("description"),
            "filename":        a.get("filename"),
            "document_url":    a.get("document_url"),
            "filing_index_url": earnings_8k.get("filing_index_url"),
            "classification":  a.get("classification"),
            "classification_confidence": a.get("classification_confidence"),
            "source":          "sec_edgar",
        }

    # Webcast URL: only from first-party attachment
    webcast_url: str | None = None
    if wc_att:
        webcast_url = wc_att.get("document_url")

    # Transcript status
    if tr_att:
        transcript = {
            "status":      "available_sec_exhibit",
            "source_type": "sec_exhibit",
            "source_url":  tr_att.get("document_url"),
        }
    else:
        transcript = {
            "status":      "unavailable",
            "source_type": None,
            "source_url":  None,
        }

    # Related financial report (latest 10-K or 10-Q within 180 days of 8-K)
    related_fr: dict | None = None
    e8k_date = earnings_8k.get("filed_date") or ""
    for f in filings_with_atts:
        if f.get("form", "").upper() in ("10-Q", "10-K", "20-F") and f.get("filed_date", "") <= e8k_date:
            related_fr = {
                "form":             f.get("form"),
                "accession_number": f.get("accession_number"),
                "filed_date":       f.get("filed_date"),
                "accepted_at":      f.get("accepted_at"),
                "filing_index_url": f.get("filing_index_url"),
                "classification":   "financial_report",
                "source":           "sec_edgar",
            }
            break

    return {
        "earnings_date":       earnings_8k.get("filed_date"),
        "detected_at":         datetime.now(timezone.utc).isoformat(),
        "primary_filing":      primary_filing,
        "earnings_release":    _att_doc(er_att),
        "investor_presentation": _att_doc(pres_att),
        "supplemental_tables": [_att_doc(a) for a in sup_atts if a],
        "guidance_documents":  [_att_doc(a) for a in guid_atts if a],
        "prepared_remarks":    _att_doc(rem_att),
        "related_financial_report": related_fr,
        "webcast_url":         webcast_url,
        "transcript":          transcript,
    }


# ── Main fetch-and-cache function ─────────────────────────────────────────────

async def fetch_and_cache_materials(
    symbol: str,
    lookback_days: int = _INDEX_LOOKBACK_DAYS,
    deep_index: bool = True,
) -> dict | None:
    """
    Fetch SEC materials for one symbol and write to disk cache.
    Returns the materials dict or None on total failure.
    Called by background refresh — never at request time.
    """
    from data.ei_materials_cache import set_materials

    sym = symbol.upper().strip()
    if not sym or ":" in sym:
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    errors: dict = {}

    async with httpx.AsyncClient(headers=_EDGAR_HEADERS, timeout=20.0, follow_redirects=True) as client:
        # 1. Resolve CIK
        cik = await _resolve_cik(sym, client)
        if not cik:
            result = {
                "latest_earnings_packet": None,
                "recent_filings": [],
                "source_status": {
                    "fetched_at": now_iso,
                    "coverage": False,
                    "errors": {"cik": "not_found"},
                },
            }
            set_materials(sym, result)
            return result

        cik_num = cik.lstrip("0") or "0"
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        # 2. Fetch submissions (filing list)
        sub_url = f"{_DATA_URL}/submissions/CIK{cik}.json"
        sub_data = await _get_json(client, sub_url)
        if not sub_data or not isinstance(sub_data, dict):
            errors["submissions"] = "fetch_failed"
            result = {
                "latest_earnings_packet": None,
                "recent_filings": [],
                "source_status": {
                    "fetched_at": now_iso,
                    "coverage": False,
                    "errors": errors,
                },
            }
            set_materials(sym, result)
            return result

        recent = sub_data.get("filings", {}).get("recent", {})
        forms       = recent.get("form", [])
        filed_dates = recent.get("filingDate", [])
        acc_numbers = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        primary_descs = recent.get("primaryDocDescription", [])
        accepted_ats  = recent.get("acceptedDate", [])
        report_dates  = recent.get("reportDate", [])
        items_list    = recent.get("items", [])

        # 3. Build filing records (filtered to monitored forms + cutoff)
        raw_filings: list[dict] = []
        for i in range(min(len(forms), _RECENT_FILINGS_LIMIT * 2)):
            if i >= len(filed_dates):
                break
            form       = (forms[i] or "").upper().strip()
            filed_date = str(filed_dates[i] or "")[:10]
            accession  = str(acc_numbers[i] if i < len(acc_numbers) else "")
            accepted   = str(accepted_ats[i] if i < len(accepted_ats) else "")
            items_str  = str(items_list[i] if i < len(items_list) else "")
            pri_doc    = str(primary_docs[i] if i < len(primary_docs) else "")
            pri_desc   = str(primary_descs[i] if i < len(primary_descs) else "")
            rep_date   = str(report_dates[i] if i < len(report_dates) else "")

            if filed_date < cutoff:
                continue
            if form not in _MONITORED_FORMS and not any(form.startswith(p) for p in ("424B",)):
                continue

            acc_clean = accession.replace("-", "")
            pri_url = (
                f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{pri_doc}"
                if acc_clean and pri_doc else ""
            )
            idx_url = (
                f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{acc_clean}-index.htm"
                if acc_clean else ""
            )

            category = _category_from_form(form)

            # Re-classify 8-K as earnings or transactions based on items
            if form in ("8-K", "8-K/A") and "2.02" in items_str:
                category = "earnings"
            elif form in ("8-K", "8-K/A") and any(k in items_str for k in ("1.01", "1.02", "5.01")):
                category = "transactions"

            raw_filings.append({
                "form":             form,
                "category":         category,
                "filed_date":       filed_date,
                "accepted_at":      accepted,
                "accession_number": accession,
                "title":            pri_desc,
                "report_date":      rep_date,
                "items":            items_str,
                "primary_document_url": pri_url,
                "filing_index_url": idx_url,
                "attachments":      [],     # populated below if deep_index
            })

            if len(raw_filings) >= _RECENT_FILINGS_LIMIT:
                break

        # Sort newest first
        raw_filings.sort(key=lambda f: f["filed_date"], reverse=True)

        # 4. Deep-index: fetch attachment list for relevant filings
        if deep_index:
            # Limit deep fetches to avoid excessive SEC requests:
            # - All earnings 8-Ks within lookback
            # - Most recent 10-K and 10-Q (one each)
            # - Total cap: 12 index fetches
            fetch_targets: list[dict] = []
            saw_10k = saw_10q = False
            for f in raw_filings:
                form = f["form"]
                if form in ("8-K", "8-K/A", "6-K"):
                    fetch_targets.append(f)
                elif form in ("10-K", "10-K/A") and not saw_10k:
                    fetch_targets.append(f)
                    saw_10k = True
                elif form in ("10-Q", "10-Q/A") and not saw_10q:
                    fetch_targets.append(f)
                    saw_10q = True
                if len(fetch_targets) >= 12:
                    break

            for f in fetch_targets:
                if not f["accession_number"]:
                    continue
                try:
                    raw_atts = await _fetch_filing_index(
                        client, cik_num, f["accession_number"]
                    )
                    f["attachments"] = [
                        _build_attachment(
                            f["form"], f["items"], a,
                            cik_num, f["accession_number"],
                            f["accession_number"].replace("-", ""),
                        )
                        for a in raw_atts
                    ]
                except Exception as exc:
                    errors[f"index_{f['accession_number']}"] = str(exc)[:120]

        # 5. Assemble latest earnings packet
        try:
            latest_packet = _assemble_earnings_packet(raw_filings)
        except Exception as exc:
            errors["packet_assembly"] = str(exc)[:120]
            latest_packet = None

        # 6. Build recent_filings response (clean up internal fields)
        recent_filings_out: list[dict] = []
        for f in raw_filings:
            recent_filings_out.append({
                "form":                 f["form"],
                "category":             f["category"],
                "filed_date":           f["filed_date"],
                "accepted_at":          f["accepted_at"],
                "accession_number":     f["accession_number"],
                "title":                f["title"],
                "summary":              None,
                "items":                f["items"] or None,
                "filing_index_url":     f["filing_index_url"],
                "primary_document_url": f["primary_document_url"],
                "attachments":          f["attachments"],
            })

        result: dict = {
            "latest_earnings_packet": latest_packet,
            "recent_filings":         recent_filings_out,
            "source_status": {
                "fetched_at": now_iso,
                "coverage":   bool(raw_filings),
                "cik":        cik,
                "errors":     errors,
            },
        }

        set_materials(sym, result)
        return result


# ── Bulk backfill ─────────────────────────────────────────────────────────────

async def backfill_materials(
    symbols: list[str],
    force: bool = False,
    progress_state: dict | None = None,
) -> dict:
    """
    Refresh materials for a list of symbols.
    Skips symbols with a fresh cache entry unless force=True.
    Returns summary statistics.
    """
    from data.ei_materials_cache import needs_refresh

    refreshed = 0
    skipped = 0
    failed = 0
    failed_syms: list[str] = []

    for sym in symbols:
        if progress_state is not None:
            progress_state["current_symbol"] = sym

        if not force and not needs_refresh(sym):
            skipped += 1
            continue

        try:
            result = await fetch_and_cache_materials(sym)
            if result is not None:
                refreshed += 1
            else:
                failed += 1
                failed_syms.append(sym)
        except Exception as exc:
            failed += 1
            failed_syms.append(sym)
            print(f"[EI_MATERIALS] backfill error {sym}: {exc}")

        # Brief pause between symbols to respect SEC rate limits
        await asyncio.sleep(0.6)

    return {
        "refreshed":    refreshed,
        "skipped":      skipped,
        "failed":       failed,
        "failed_symbols": failed_syms,
        "total":        len(symbols),
    }
