"""
EI Materials Service — SEC filing fetch, classify, and cache for earnings intelligence.

Architecture:
  Background refresh only — never called at request time.
  Writes to ei_materials_cache.py (disk, keyed by symbol).
  ticker_detail_endpoint reads from disk cache — zero provider calls at request time.

Classification pipeline (in order):
  1. Form type override (4, SC 13D, DEF 14A, etc.)
  2. Filing item codes (8-K item 2.02 = earnings)
  3. Attachment metadata (type, description, filename)
  4. Document body text (HTML/text only, capped at 8 000 chars)
  5. Keyword scoring across all available signals

Text fetch:
  - Only .htm / .html / .txt files fetched (identified by filename extension)
  - Cap: 150 KB download → 8 000 clean text chars passed to classifier
  - Uses BeautifulSoup html.parser for HTML stripping
  - PDFs, XLS, ZIP → metadata-only classification, confidence capped at "low"

Webcast URL extraction:
  - Extracted from earnings-release and presentation attachment text
  - Regex pattern list targets known IR / webcast platforms
  - Stores webcast_url, webcast_source_document, extraction_confidence
  - Never returns tracking pixels, social links, or commercial transcript links

Transcript state semantics:
  - available_sec_exhibit: attachment text confirmed as transcript
  - not_yet_available:     no transcript source; filing < 5 days ago
  - unavailable:           no transcript source; filing >= 5 days ago

Forms monitored (exact EDGAR strings):
  10-K, 10-K/A, 10-Q, 10-Q/A, 20-F, 20-F/A,
  8-K, 8-K/A, 6-K,
  4, 4/A,
  SC 13D, SC 13D/A, SC 13G, SC 13G/A,
  DEF 14A, DEFA14A,
  S-1, S-1/A, S-3, S-3/A,
  424B1, 424B2, 424B3, 424B4, 424B5,
  425

LLM fallback is NOT used.
"""
from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

_EDGAR_HEADERS = {
    "User-Agent": "TradingAnalysisPlatform/1.0 (contact: apixbt@gmail.com)",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, text/plain, */*",
}
_DATA_URL      = "https://data.sec.gov"
_ARCHIVE_URL   = "https://www.sec.gov/Archives/edgar/data"
_EFTS_URL      = "https://efts.sec.gov/LATEST/search-index"
_TOKEN_BUCKET_RATE   = 2.0   # 2 req/s — well under SEC 10 req/s limit
_INDEX_LOOKBACK_DAYS = 180
_RECENT_FILINGS_LIMIT = 40
_TEXT_FETCH_MAX_BYTES = 150_000   # 150 KB download cap
_TEXT_CLEAN_MAX_CHARS = 8_000     # chars fed to classifier after HTML strip

# ── Fetch-eligible extensions (filename-based) ────────────────────────────────
_TEXT_EXTENSIONS = frozenset({".htm", ".html", ".txt"})
_SKIP_EXTENSIONS = frozenset({
    ".pdf", ".xls", ".xlsx", ".xlsm", ".zip", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".doc", ".docx", ".ppt", ".pptx",
})

# ── Form-type mappings ────────────────────────────────────────────────────────
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

# Forms for which we fetch the full attachment index
_DEEP_INDEX_FORMS = frozenset({
    "8-K", "8-K/A", "6-K", "10-K", "10-K/A", "10-Q", "10-Q/A",
    "20-F", "20-F/A", "DEF 14A", "S-1", "S-1/A",
})

# ── Keyword classifiers ───────────────────────────────────────────────────────
_EARNINGS_RELEASE_KW = [
    "earnings release", "quarterly results", "financial results",
    "quarterly financial", "results of operations", "q1 results", "q2 results",
    "q3 results", "q4 results", "fourth quarter", "third quarter",
    "second quarter", "first quarter", "annual results",
    "full year results", "fiscal year results", "revenue of", "net income",
    "diluted eps", "non-gaap", "adjusted eps",
]
_INVESTOR_PRESENTATION_KW = [
    "investor presentation", "corporate presentation", "company presentation",
    "investor day", "analyst day", "capital markets day",
    "investment highlights", "business overview",
    "slide",     # "slides", "slide deck"
    "deck",      # "earnings deck", "q1deck", filenames like "earningsdeck-*.htm"
    "slides",    # plural — PowerPoint-style titles
    "earnings deck", "q1 deck", "q2 deck", "q3 deck", "q4 deck",
    "quarterly presentation", "earnings presentation",
]
_SUPPLEMENTAL_KW = [
    "supplemental", "financial supplement", "data supplement",
    "statistical supplement", "key metrics", "supplemental data",
    "operating statistics", "supplemental financial",
]
_TRANSCRIPT_KW = [
    "transcript", "earnings call transcript", "conference call transcript",
    "earnings conference call", "questions and answers",
    "operator:", "moderator:", "q&a session", "analyst:", "good morning, everyone",
    "good afternoon, everyone", "the following is a transcript",
]
_GUIDANCE_KW = [
    "guidance", "outlook", "forecast", "forward-looking", "full year outlook",
    "annual guidance", "revenue guidance", "updated guidance", "expects revenue",
    "expects earnings", "fiscal year 20", "we expect",
]
_WEBCAST_KW = [
    "webcast", "conference call", "live audio", "listen to call",
    "replay", "earnings call", "investor call", "live broadcast",
    "listen live", "dial-in", "dial in",
]
_PREPARED_REMARKS_KW = [
    "prepared remarks", "prepared statement", "conference call remarks",
    "management remarks", "opening remarks", "ceo remarks", "cfo remarks",
]

# ── Webcast URL extraction ────────────────────────────────────────────────────
# Match known IR / streaming / webcast domains — avoids social links and tracking pixels.
_WEBCAST_URL_RE = re.compile(
    r'https?://[^\s\'"<>]+'
    r'(?:webcast|listen|replay|earnings(?:-call)?|'
    r'q4inc\.com|west\.com|streetevents|on24\.com|'
    r'verint\.com|arkadin\.com|meetup\.com/|'
    r'zoom\.us/webinar|teams\.microsoft\.com|'
    r'chorus\.ai|gong\.io|'
    r'ir\.[a-z0-9-]+\.com|investors\.[a-z0-9-]+\.com|'
    r'investor\.relations|ir-room|corporate\.ir\.net)',
    re.IGNORECASE,
)
# Block known noise / tracker / social domains
_WEBCAST_BLOCKLIST_RE = re.compile(
    r'(?:facebook|twitter|linkedin|instagram|tiktok|youtube|'
    r'google-analytics|googletagmanager|doubleclick|'
    r'pixel\.|tracking\.|cdn\.|fonts\.googleapis)',
    re.IGNORECASE,
)
# Earnings release anchor patterns — used to limit URL search to nearby context
_WEBCAST_CONTEXT_KW = re.compile(
    r'(?:webcast|conference.?call|listen.?live|dial.?in|replay)',
    re.IGNORECASE,
)


def _kw_score(text: str, keywords: list[str]) -> int:
    t = text.lower()
    return sum(1 for kw in keywords if kw in t)


# ── HTML/text cleaning ────────────────────────────────────────────────────────

def _clean_html(raw: bytes | str, max_chars: int = _TEXT_CLEAN_MAX_CHARS) -> str:
    """Strip HTML tags and collapse whitespace. Returns up to max_chars characters."""
    try:
        text_input = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        soup = BeautifulSoup(text_input, "html.parser")
        for tag in soup(["script", "style", "head"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace"))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _extract_webcast_url_from_text(
    body_text: str,
    source_url: str = "",
) -> tuple[str | None, str | None, str]:
    """
    Extract a webcast / listen-live URL from cleaned attachment text.

    Returns: (webcast_url, extraction_method, confidence)
      confidence: "high" | "medium" | "low" | "none"
    """
    if not body_text:
        return None, None, "none"

    # Find all candidate https:// URLs
    all_urls = re.findall(r'https?://[^\s\'"<>\)]+', body_text)

    # Filter: remove blocklist + keep only webcast-like
    candidates: list[str] = []
    for url in all_urls:
        if _WEBCAST_BLOCKLIST_RE.search(url):
            continue
        if _WEBCAST_URL_RE.search(url):
            candidates.append(url)

    if not candidates:
        return None, None, "none"

    # Prefer URLs that appear within 200 chars of a webcast context keyword
    for url in candidates:
        idx = body_text.find(url)
        if idx >= 0:
            context = body_text[max(0, idx - 200): idx + 200]
            if _WEBCAST_CONTEXT_KW.search(context):
                # Clean trailing punctuation
                url_clean = url.rstrip(".,;:)'\"")
                return url_clean, source_url, "high"

    # Fall back to first candidate regardless of context
    url_clean = candidates[0].rstrip(".,;:)'\"")
    return url_clean, source_url, "medium"


# ── Rate limiter ──────────────────────────────────────────────────────────────

_tb_tokens: float = 2.0
_tb_last:   float = 0.0


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
    try:
        await _acquire()
        resp = await client.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            print(f"[EI_MATERIALS] Rate limited — sleeping 30s")
            await asyncio.sleep(30)
        return None
    except Exception as exc:
        print(f"[EI_MATERIALS] fetch error {url}: {exc}")
        return None


async def _fetch_attachment_text(
    client: httpx.AsyncClient,
    url: str,
    filename: str = "",
) -> str:
    """
    Fetch and clean attachment text for HTML/text files only.
    Returns "" for PDFs, XLS, ZIP, or on any error.
    Capped at _TEXT_FETCH_MAX_BYTES download.
    """
    # Determine extension
    fname = (filename or url).lower().split("?")[0]
    ext = ""
    if "." in fname:
        ext = "." + fname.rsplit(".", 1)[-1]

    if ext in _SKIP_EXTENSIONS:
        return ""
    if ext and ext not in _TEXT_EXTENSIONS:
        return ""  # unknown extension — skip

    try:
        await _acquire()
        async with client.stream("GET", url, timeout=15.0) as resp:
            if resp.status_code != 200:
                return ""
            ct = resp.headers.get("content-type", "").lower()
            # Skip non-text content types
            if any(x in ct for x in ("pdf", "excel", "zip", "octet-stream", "image")):
                return ""
            raw_chunks: list[bytes] = []
            total = 0
            async for chunk in resp.aiter_bytes(chunk_size=8192):
                raw_chunks.append(chunk)
                total += len(chunk)
                if total >= _TEXT_FETCH_MAX_BYTES:
                    break
            raw = b"".join(raw_chunks)
    except Exception:
        return ""

    return _clean_html(raw)


# ── Attachment classifier ─────────────────────────────────────────────────────

def _classify_attachment(
    filing_form: str,
    items_str: str,
    att_type: str,
    att_desc: str,
    filename: str,
    doc_title: str = "",
    body_text: str = "",     # cleaned attachment text (empty = metadata-only)
) -> tuple[str, str, str]:
    """
    Deterministically classify one attachment.

    Returns (classification, confidence, method).
    Classification candidates:
      earnings_release, investor_presentation, supplemental_tables,
      transcript, prepared_remarks, corporate_guidance,
      webcast_or_replay, financial_report, insider_filing,
      ownership_filing, proxy, offering_document, transaction_material,
      other
    """
    form   = (filing_form or "").upper().strip()
    items  = items_str or ""
    atype  = (att_type or "").upper().strip()
    desc   = (att_desc or "").lower().strip()
    fname  = (filename or "").lower().strip()
    title  = (doc_title or "").lower().strip()
    # Combine metadata + body text for keyword scoring
    combined_meta = f"{desc} {title} {fname}"
    combined_full = f"{combined_meta} {body_text.lower()[:4000]}"

    # ── Form-level overrides ──────────────────────────────────────────────────
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

    # ── 8-K / 8-K/A / 6-K ────────────────────────────────────────────────────
    if form in ("8-K", "8-K/A", "6-K"):
        has_202 = "2.02" in items

        # Primary body doc: atype matches the parent form type (e.g. atype="8-K"
        # for the main 8-K HTML body).  Exhibit docs have atype like "EX-99.1".
        # We skip investor_presentation keyword check for primary body docs because
        # XBRL/iXBRL content frequently embeds "slide", "deck" etc. in XBRL metadata,
        # causing false investor_presentation classifications.
        is_primary_body = (atype.upper() in (form.upper(), form.upper().rstrip("/A")))

        if has_202:
            # Body-text checks first (highest signal)
            if _kw_score(combined_full, _TRANSCRIPT_KW) >= 2:
                conf = "high" if body_text else "medium"
                return "transcript", conf, "item202_transcript_body" if body_text else "item202_transcript_meta"
            if _kw_score(combined_full, _PREPARED_REMARKS_KW) >= 1:
                return "prepared_remarks", "medium", "item202_remarks_kw"
            if _kw_score(combined_full, _SUPPLEMENTAL_KW) >= 1:
                return "supplemental_tables", "medium", "item202_supplemental_kw"
            # Only classify as investor_presentation for exhibits (EX-*), not the
            # primary 8-K body — avoids false positives from XBRL metadata content.
            if not is_primary_body and _kw_score(combined_full, _INVESTOR_PRESENTATION_KW) >= 1:
                return "investor_presentation", "medium", "item202_presentation_kw"
            if _kw_score(combined_full, _EARNINGS_RELEASE_KW) >= 1:
                conf = "high" if body_text else "medium"
                return "earnings_release", conf, "item202_earnings_kw"
            # item 2.02 with no confirming keyword → still likely earnings release
            return "earnings_release", "low", "item202_no_confirming_kw"

        # Non-2.02 8-K
        if _kw_score(combined_full, _TRANSCRIPT_KW) >= 2:
            return "transcript", "medium", "8k_non202_transcript_kw"
        if not is_primary_body and _kw_score(combined_full, _INVESTOR_PRESENTATION_KW) >= 1:
            return "investor_presentation", "medium", "8k_non202_presentation_kw"
        if _kw_score(combined_full, _GUIDANCE_KW) >= 2:
            return "corporate_guidance", "medium", "8k_non202_guidance_kw"
        if _kw_score(combined_full, _EARNINGS_RELEASE_KW) >= 1:
            return "earnings_release", "low", "8k_non202_earnings_kw"
        return "other", "low", "8k_non202_no_match"

    # ── 10-K / 10-Q / 20-F ───────────────────────────────────────────────────
    if form in ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"):
        if atype in ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A"):
            return "financial_report", "high", "form_type_match"
        if _kw_score(combined_full, _SUPPLEMENTAL_KW) >= 1:
            return "supplemental_tables", "medium", "10kq_supplemental_kw"
        return "financial_report", "low", "10kq_exhibit_fallback"

    # ── Generic keyword scoring ───────────────────────────────────────────────
    scores = [
        ("earnings_release",       _kw_score(combined_full, _EARNINGS_RELEASE_KW)),
        ("investor_presentation",  _kw_score(combined_full, _INVESTOR_PRESENTATION_KW)),
        ("transcript",             _kw_score(combined_full, _TRANSCRIPT_KW)),
        ("supplemental_tables",    _kw_score(combined_full, _SUPPLEMENTAL_KW)),
        ("corporate_guidance",     _kw_score(combined_full, _GUIDANCE_KW)),
        ("webcast_or_replay",      _kw_score(combined_full, _WEBCAST_KW)),
        ("prepared_remarks",       _kw_score(combined_full, _PREPARED_REMARKS_KW)),
    ]
    best_cls, best_score = max(scores, key=lambda x: x[1])
    if best_score >= 1:
        conf = "medium" if best_score >= 2 else "low"
        return best_cls, conf, f"keyword_score_{best_score}"

    return "other", "low", "no_rule_matched"


def _category_from_form(form: str) -> str:
    f = (form or "").upper().strip()
    return _FORM_CATEGORY_MAP.get(f, "other")


def _is_earnings_8k(form: str, items_str: str) -> bool:
    return form.upper() in ("8-K", "8-K/A", "6-K") and "2.02" in (items_str or "")


def _ext_of(filename: str) -> str:
    fname = (filename or "").lower().split("?")[0]
    if "." in fname:
        return "." + fname.rsplit(".", 1)[-1]
    return ""


# ── CIK resolution ────────────────────────────────────────────────────────────

_cik_cache: dict[str, str] = {}
_cik_cache_ts: float = 0.0
_CIK_MAP_TTL = 86400


async def _resolve_cik(symbol: str, client: httpx.AsyncClient) -> str | None:
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


# ── Filing index fetch ────────────────────────────────────────────────────────

async def _fetch_filing_index(
    client: httpx.AsyncClient,
    cik_num: str,
    accession: str,
) -> list[dict]:
    """
    Fetch the list of documents for one accession via the EFTS search API.

    EDGAR's `-index.json` does not exist as a stable endpoint (returns 404).
    The EFTS full-text search at efts.sec.gov is the authoritative source:
      GET https://efts.sec.gov/LATEST/search-index?q="{accession_with_dashes}"

    Response hit structure:
      _id  = "{accession}:{filename}"
      _source.file_description = "EX-99.1" | "8-K" | etc.
      _source.sequence         = document sequence number
    """
    acc_clean = accession.replace("-", "")
    try:
        await _acquire()
        resp = await client.get(
            _EFTS_URL,
            params={"q": f'"{accession}"'},
            timeout=12.0,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception as exc:
        print(f"[EI_MATERIALS] EFTS fetch error {accession}: {exc}")
        return []

    hits = (data.get("hits") or {}).get("hits") or []
    items = []
    for hit in hits:
        hit_id  = hit.get("_id") or ""          # "{accession}:{filename}"
        src     = hit.get("_source") or {}
        # Extract filename from id
        if ":" in hit_id:
            filename = hit_id.split(":", 1)[1]
        else:
            continue
        if not filename or filename.endswith("/"):
            continue
        att_type = src.get("file_description") or ""   # "EX-99.1", "8-K", etc.
        seq      = str(src.get("sequence") or "")
        items.append({
            "filename":    filename,
            "type":        att_type,
            "size":        "",
            "description": att_type,
            "sequence":    seq,
            "url":         f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{filename}",
        })

    # Sort by sequence (primary document first)
    try:
        items.sort(key=lambda x: int(x["sequence"]) if x["sequence"].isdigit() else 99)
    except Exception:
        pass

    return items


# ── Attachment builder (async — fetches text for eligible files) ──────────────

async def _build_attachment_async(
    client: httpx.AsyncClient,
    filing_form: str,
    items_str: str,
    raw_att: dict,
    cik_num: str,
    accession: str,
    acc_clean: str,
    fetch_text: bool = True,
) -> dict:
    filename = raw_att.get("filename") or ""
    att_type = raw_att.get("type") or ""
    att_desc = raw_att.get("description") or ""
    url      = raw_att.get("url") or f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{filename}"

    # Fetch body text for HTML/text eligible files
    body_text = ""
    text_fetched = False
    if fetch_text:
        ext = _ext_of(filename)
        if ext in _TEXT_EXTENSIONS:
            body_text = await _fetch_attachment_text(client, url, filename)
            text_fetched = bool(body_text)

    cls, conf, method = _classify_attachment(
        filing_form, items_str, att_type, att_desc, filename,
        body_text=body_text,
    )

    # Lower confidence for non-text files where we couldn't inspect content
    ext = _ext_of(filename)
    if ext in _SKIP_EXTENSIONS and conf == "high":
        conf = "medium"

    return {
        "filename":                filename,
        "document_type":           att_type,
        "description":             att_desc,
        "document_url":            url,
        "classification":          cls,
        "classification_confidence": conf,
        "classification_method":   method,
        "text_inspected":          text_fetched,
        # Internal: keep body_text for webcast extraction; stripped from final response
        "_body_text":              body_text,
    }


# ── Earnings packet assembler ─────────────────────────────────────────────────

def _assemble_earnings_packet(
    filings_with_atts: list[dict],
    today_str: str,
) -> dict | None:
    earnings_8k: dict | None = None
    for f in filings_with_atts:
        if _is_earnings_8k(f.get("form", ""), f.get("items", "")):
            earnings_8k = f
            break

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

    er_att   = _first_by_cls("earnings_release")
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
            "form":              earnings_8k.get("form"),
            "accession_number":  earnings_8k.get("accession_number"),
            "filed_date":        earnings_8k.get("filed_date"),
            "accepted_at":       earnings_8k.get("accepted_at"),
            "document_type":     a.get("document_type"),
            "description":       a.get("description"),
            "filename":          a.get("filename"),
            "document_url":      a.get("document_url"),
            "filing_index_url":  earnings_8k.get("filing_index_url"),
            "classification":    a.get("classification"),
            "classification_confidence": a.get("classification_confidence"),
            "text_inspected":    a.get("text_inspected", False),
            "source":            "sec_edgar",
        }

    # ── Webcast URL extraction ─────────────────────────────────────────────────
    # Scan: earnings release body, then presentation body, then webcast attachment
    webcast_url: str | None = None
    webcast_source_document: str | None = None
    webcast_extraction_confidence: str = "none"

    for candidate_att in [er_att, pres_att, wc_att]:
        if not candidate_att:
            continue
        txt = candidate_att.get("_body_text") or ""
        if txt:
            wc_url, wc_src, wc_conf = _extract_webcast_url_from_text(
                txt, candidate_att.get("document_url", "")
            )
            if wc_url:
                webcast_url = wc_url
                webcast_source_document = wc_src
                webcast_extraction_confidence = wc_conf
                break
        elif candidate_att.get("classification") == "webcast_or_replay":
            # No text inspected — use the attachment URL directly as the webcast reference
            webcast_url = candidate_att.get("document_url")
            webcast_source_document = candidate_att.get("document_url")
            webcast_extraction_confidence = "low"
            break

    # ── Transcript state ───────────────────────────────────────────────────────
    # not_yet_available → filing < 5 calendar days ago (still in processing window)
    # unavailable      → filing >= 5 calendar days ago, no transcript found
    filed_str = earnings_8k.get("filed_date") or ""
    days_since_filing: int | None = None
    if filed_str and today_str:
        try:
            delta = datetime.strptime(today_str, "%Y-%m-%d") - datetime.strptime(filed_str, "%Y-%m-%d")
            days_since_filing = delta.days
        except Exception:
            pass

    if tr_att:
        tr_body = tr_att.get("_body_text") or ""
        if tr_body and _kw_score(tr_body, _TRANSCRIPT_KW) >= 2:
            transcript_status = "available_sec_exhibit"
            transcript_source_type = "sec_exhibit"
        else:
            # Attachment classified as transcript by metadata but not confirmed by text
            transcript_status = "available_sec_exhibit"
            transcript_source_type = "sec_exhibit_unconfirmed"
        transcript = {
            "status":      transcript_status,
            "source_type": transcript_source_type,
            "source_url":  tr_att.get("document_url"),
        }
    else:
        # Determine not_yet_available vs unavailable by date
        if days_since_filing is not None and days_since_filing < 5:
            ts = "not_yet_available"
        elif days_since_filing is None:
            ts = "unknown"
        else:
            ts = "unavailable"
        transcript = {
            "status":      ts,
            "source_type": None,
            "source_url":  None,
        }

    # ── Related financial report ───────────────────────────────────────────────
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
        "earnings_date":       filed_str,
        "detected_at":         datetime.now(timezone.utc).isoformat(),
        "days_since_filing":   days_since_filing,
        "primary_filing":      primary_filing,
        "earnings_release":    _att_doc(er_att),
        "investor_presentation": _att_doc(pres_att),
        "supplemental_tables": [_att_doc(a) for a in sup_atts if a],
        "guidance_documents":  [_att_doc(a) for a in guid_atts if a],
        "prepared_remarks":    _att_doc(rem_att),
        "related_financial_report": related_fr,
        "webcast_url":               webcast_url,
        "webcast_source_document":   webcast_source_document,
        "webcast_extraction_confidence": webcast_extraction_confidence,
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
    Background refresh only — never called at request time.
    """
    from data.ei_materials_cache import set_materials

    sym = symbol.upper().strip()
    if not sym or ":" in sym:
        return None

    now_iso = datetime.now(timezone.utc).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    errors: dict = {}

    async with httpx.AsyncClient(
        headers=_EDGAR_HEADERS, timeout=20.0, follow_redirects=True
    ) as client:

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
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d")

        # 2. Fetch submissions
        sub_url = f"{_DATA_URL}/submissions/CIK{cik}.json"
        sub_data = await _get_json(client, sub_url)
        if not sub_data or not isinstance(sub_data, dict):
            errors["submissions"] = "fetch_failed"
            result = {
                "latest_earnings_packet": None,
                "recent_filings": [],
                "source_status": {
                    "fetched_at": now_iso, "coverage": False, "errors": errors,
                },
            }
            set_materials(sym, result)
            return result

        recent        = sub_data.get("filings", {}).get("recent", {})
        forms         = recent.get("form", [])
        filed_dates   = recent.get("filingDate", [])
        acc_numbers   = recent.get("accessionNumber", [])
        primary_docs  = recent.get("primaryDocument", [])
        primary_descs = recent.get("primaryDocDescription", [])
        accepted_ats  = recent.get("acceptedDate", [])
        items_list    = recent.get("items", [])

        # 3. Build filing records
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

            if filed_date < cutoff:
                continue

            # Match exact EDGAR form strings plus 424B* prefix
            if form not in _MONITORED_FORMS and not form.startswith("424B"):
                continue

            acc_clean = accession.replace("-", "")
            pri_url   = (
                f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{pri_doc}"
                if acc_clean and pri_doc else ""
            )
            idx_url = (
                f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{acc_clean}-index.htm"
                if acc_clean else ""
            )

            category = _category_from_form(form)
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
                "items":            items_str,
                "primary_document_url": pri_url,
                "filing_index_url": idx_url,
                "attachments":      [],
            })

            if len(raw_filings) >= _RECENT_FILINGS_LIMIT:
                break

        raw_filings.sort(key=lambda f: f["filed_date"], reverse=True)

        # 4. Deep-index: fetch attachment list + text for key filings
        if deep_index:
            fetch_targets: list[dict] = []
            saw_10k = saw_10q = False
            for f in raw_filings:
                frm = f["form"]
                if frm in ("8-K", "8-K/A", "6-K"):
                    fetch_targets.append(f)
                elif frm in ("10-K", "10-K/A") and not saw_10k:
                    fetch_targets.append(f)
                    saw_10k = True
                elif frm in ("10-Q", "10-Q/A") and not saw_10q:
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
                    acc_clean_f = f["accession_number"].replace("-", "")

                    # Fallback: EFTS returned empty (403 / rate-limit / transient).
                    # Synthesize a single attachment from the primary document URL
                    # that is already known from the submissions JSON.  The primary
                    # body gets atype == form type, so the is_primary_body guard in
                    # _classify_attachment routes it correctly (no false investor_pres).
                    if not raw_atts and f.get("primary_document_url"):
                        pri_url  = f["primary_document_url"]
                        pri_name = (pri_url or "").rsplit("/", 1)[-1] or ""
                        raw_atts = [{
                            "filename":    pri_name,
                            "type":        f["form"],   # e.g. "8-K" — primary body signal
                            "size":        "",
                            "description": f.get("title") or f["form"],
                            "sequence":    "1",
                            "url":         pri_url,
                        }]

                    # Build attachments asynchronously (fetches text where eligible)
                    # Only fetch text for earnings-8K and 10-K/Q (not every filing)
                    should_fetch_text = f["form"] in (
                        "8-K", "8-K/A", "6-K", "10-K", "10-K/A", "10-Q", "10-Q/A"
                    )
                    tasks = [
                        _build_attachment_async(
                            client, f["form"], f["items"], a,
                            cik_num, f["accession_number"], acc_clean_f,
                            fetch_text=should_fetch_text,
                        )
                        for a in raw_atts
                    ]
                    built = await asyncio.gather(*tasks, return_exceptions=True)
                    f["attachments"] = [
                        a for a in built if isinstance(a, dict)
                    ]
                except Exception as exc:
                    errors[f"index_{f['accession_number']}"] = str(exc)[:120]

        # 5. Assemble earnings packet
        try:
            latest_packet = _assemble_earnings_packet(raw_filings, today_str)
        except Exception as exc:
            errors["packet_assembly"] = str(exc)[:120]
            latest_packet = None

        # 6. Build response — strip internal _body_text before persisting
        recent_filings_out: list[dict] = []
        for f in raw_filings:
            clean_atts = []
            for a in f["attachments"]:
                a2 = {k: v for k, v in a.items() if k != "_body_text"}
                clean_atts.append(a2)
            recent_filings_out.append({
                "form":                 f["form"],
                "category":             f["category"],
                "filed_date":           f["filed_date"],
                "accepted_at":          f["accepted_at"],
                "accession_number":     f["accession_number"],
                "title":                f["title"],
                "items":                f["items"] or None,
                "filing_index_url":     f["filing_index_url"],
                "primary_document_url": f["primary_document_url"],
                "attachments":          clean_atts,
            })

        # Compute classification counts across all attachments
        cls_counts: dict[str, int] = {}
        transcript_states: dict[str, int] = {}
        for f in recent_filings_out:
            for a in f["attachments"]:
                c = a.get("classification", "other")
                cls_counts[c] = cls_counts.get(c, 0) + 1
        if latest_packet:
            ts = (latest_packet.get("transcript") or {}).get("status", "unknown")
            transcript_states[ts] = transcript_states.get(ts, 0) + 1

        result: dict = {
            "latest_earnings_packet": latest_packet,
            "recent_filings":         recent_filings_out,
            "source_status": {
                "fetched_at":         now_iso,
                "coverage":           bool(raw_filings),
                "cik":                cik,
                "filing_count":       len(recent_filings_out),
                "earnings_8k_count":  sum(1 for f in recent_filings_out if f.get("category") == "earnings"),
                "classification_counts": cls_counts,
                "transcript_state_counts": transcript_states,
                "errors":             errors,
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
    from data.ei_materials_cache import needs_refresh

    refreshed = 0
    skipped   = 0
    failed    = 0
    no_cik    = 0
    no_packet = 0
    failed_syms: list[str] = []
    cls_counts: dict[str, int] = {}
    ts_counts:  dict[str, int] = {}

    for sym in symbols:
        if progress_state is not None:
            progress_state["current_symbol"] = sym

        if not force and not needs_refresh(sym):
            skipped += 1
            continue

        try:
            result = await fetch_and_cache_materials(sym)
            if result is None:
                failed += 1
                failed_syms.append(sym)
                continue

            ss = result.get("source_status") or {}
            if not ss.get("cik") or (ss.get("errors") or {}).get("cik") == "not_found":
                no_cik += 1

            if not result.get("latest_earnings_packet"):
                no_packet += 1

            for c, n in (ss.get("classification_counts") or {}).items():
                cls_counts[c] = cls_counts.get(c, 0) + n
            for t, n in (ss.get("transcript_state_counts") or {}).items():
                ts_counts[t] = ts_counts.get(t, 0) + n

            refreshed += 1
        except Exception as exc:
            failed += 1
            failed_syms.append(sym)
            print(f"[EI_MATERIALS] backfill error {sym}: {exc}")

        await asyncio.sleep(0.6)   # 1.67 req/s — under SEC 10 req/s cap

    return {
        "refreshed":               refreshed,
        "skipped":                 skipped,
        "failed":                  failed,
        "no_cik":                  no_cik,
        "no_earnings_packet":      no_packet,
        "failed_symbols":          failed_syms,
        "total":                   len(symbols),
        "classification_counts":   cls_counts,
        "transcript_state_counts": ts_counts,
    }
