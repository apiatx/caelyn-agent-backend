"""
EI Materials Service — SEC filing fetch, classify, and cache for earnings intelligence.

Architecture:
  Background refresh only — never called at request time.
  Writes to ei_materials_cache.py (disk, keyed by symbol).
  ticker_detail_endpoint reads from disk cache — zero provider calls at request time.

Canonical attachment-discovery hierarchy (per filing):
  1. GET {directory}index.json          → discover filenames + locate -index.html
  2. Parse {acc_dashes}-index.html      → Document Format Files table (seq/type/desc/url)
  3. Parse {acc_dashes}.txt (SGML)      → <DOCUMENT>/<TYPE>/<FILENAME>/<DESCRIPTION> headers
  4. EFTS efts.sec.gov/LATEST/search-index → bounded last resort, sets efts_fallback
  5. Primary-document synthetic fallback → primary_filing classification, attachments_complete=False

Classification pipeline (in order per attachment):
  1. Form type override (4, SC 13D, DEF 14A, etc.)
  2. Filing item codes (8-K item 2.02 = earnings-related)
  3. is_primary_body detection (atype == form → skip presentation check)
  4. Attachment metadata (type, description, filename)
  5. Document body text (HTML/text only, capped at 8 000 chars)
  6. Keyword scoring across all available signals

Text fetch:
  - Only .htm / .html / .txt files fetched (identified by filename extension)
  - Cap: 150 KB download → 8 000 clean text chars passed to classifier
  - Uses BeautifulSoup html.parser for HTML stripping
  - Hrefs extracted BEFORE stripping — appended to text for webcast URL detection
  - PDFs, XLS, ZIP → metadata-only classification, confidence capped at "low"

Webcast URL extraction:
  - Extracted from earnings-release and presentation attachment hrefs + text
  - Hrefs are preserved separately from get_text() for accurate URL recovery
  - Regex pattern list targets known IR / webcast platforms
  - Stores webcast_url, webcast_source_document, extraction_confidence
  - Never returns tracking pixels, social links, or commercial transcript links

Primary-document fallback mode:
  - When canonical discovery fails entirely, synthesize one attachment from
    the submissions JSON primaryDocument field
  - Primary body classified as primary_filing (not earnings_release)
  - Upgraded to earnings_release only when body text clearly confirms it
  - attachments_complete: false in this mode
  - discovery_method: primary_document_fallback

Transcript state semantics:
  - available_sec_exhibit: attachment text confirmed as transcript
  - not_yet_available:     no transcript source; filing < 5 days ago
  - unavailable:           no transcript source; filing >= 5 days ago
  - unknown:               filing date could not be parsed

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

# ── Discovery / classifier versions ───────────────────────────────────────────
# Bump DISCOVERY_VERSION when the attachment enumeration method changes.
# Bump CLASSIFIER_VERSION when classification rules change.
# Cache entries with lower versions are considered stale regardless of TTL.
_DISCOVERY_VERSION = 2
_CLASSIFIER_VERSION = 3

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
_SGML_RANGE_BYTES    = 40_960     # 40 KB — enough for all DOCUMENT headers

# ── Fetch-eligible extensions (filename-based) ─────────────────────────────
_TEXT_EXTENSIONS = frozenset({".htm", ".html", ".txt"})
_SKIP_EXTENSIONS = frozenset({
    ".pdf", ".xls", ".xlsx", ".xlsm", ".zip", ".gz",
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".doc", ".docx", ".ppt", ".pptx", ".xsd", ".xml",
})

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
    "slide presentation", "investor deck",
    "earnings deck", "earnings presentation",
    "quarterly presentation", "q1 deck", "q2 deck", "q3 deck", "q4 deck",
    "deck",   # catches filenames like "earningsdeck-*.htm"; guard prevents XBRL body match
    "slide",  # catches "slides" in filenames/descriptions
    "slides",
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

# ── Webcast URL extraction ───────────────────────────────────────────────────
_WEBCAST_URL_RE = re.compile(
    r'https?://[^\s\'"<>)&]+'
    r'(?:webcast|listen|replay|earnings(?:-call)?|'
    r'q4inc\.com|west\.com|streetevents|on24\.com|'
    r'verint\.com|arkadin\.com|'
    r'zoom\.us/webinar|teams\.microsoft\.com|'
    r'chorus\.ai|gong\.io|'
    r'ir\.[a-z0-9-]+\.com|investors?\.[a-z0-9-]+\.com|'
    r'investor\.relations|ir-room|corporate\.ir\.net)',
    re.IGNORECASE,
)
_WEBCAST_BLOCKLIST_RE = re.compile(
    r'(?:facebook|twitter|linkedin|instagram|tiktok|youtube|'
    r'google-analytics|googletagmanager|doubleclick|'
    r'pixel\.|tracking\.|cdn\.|fonts\.googleapis|'
    r'seeking[aA]lpha|motleyfool|streetinsider|briefing\.com)',
    re.IGNORECASE,
)
_WEBCAST_CONTEXT_KW = re.compile(
    r'(?:webcast|conference.?call|listen.?live|dial.?in|replay|earn(?:ings)?.?call)',
    re.IGNORECASE,
)


def _kw_score(text: str, keywords: list[str]) -> int:
    t = text.lower()
    return sum(1 for kw in keywords if kw in t)


# ── HTML / text utilities ───────────────────────────────────────────────────

def _extract_hrefs_text(soup: BeautifulSoup) -> str:
    """
    Extract absolute HTTP href URLs from anchor tags before stripping HTML.
    Returns a space-separated string of 'URL [anchor_text]' pairs, capped at 100 links.
    This is appended to cleaned body text so webcast URL extraction can find
    href-only URLs that get lost during HTML stripping.
    """
    parts: list[str] = []
    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if not href.startswith("http"):
            continue
        # Skip SEC/EDGAR navigation links
        if "sec.gov" in href and ("/Archives/edgar" not in href):
            continue
        anchor = a_tag.get_text(strip=True)[:80]
        parts.append(f"{href} {anchor}")
        if len(parts) >= 100:
            break
    return " ".join(parts)


def _clean_html(raw: bytes | str, max_chars: int = _TEXT_CLEAN_MAX_CHARS) -> str:
    """
    Strip HTML tags and collapse whitespace. Returns up to max_chars characters.
    Also extracts href URLs from anchor tags and appends them after the text
    (separated by __HREFS__) so webcast URL extraction can find URLs even
    from href= attributes that get lost during tag stripping.
    """
    try:
        text_input = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
        soup = BeautifulSoup(text_input, "html.parser")
        # Extract hrefs BEFORE decomposing tags
        hrefs_text = _extract_hrefs_text(soup)
        for tag in soup(["script", "style", "head"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
    except Exception:
        text = re.sub(r"<[^>]+>", " ", raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace"))
        hrefs_text = ""
    text = re.sub(r"\s+", " ", text).strip()
    combined = text[:max_chars]
    if hrefs_text:
        combined += f" __HREFS__ {hrefs_text}"
    return combined


def _extract_webcast_url_from_text(
    body_text: str,
    source_url: str = "",
) -> tuple[str | None, str | None, str]:
    """
    Extract a webcast / listen-live URL from attachment text (which includes
    hrefs appended by _clean_html via the __HREFS__ separator).

    Returns: (webcast_url, source_url, confidence)
      confidence: "high" | "medium" | "low" | "none"
    """
    if not body_text:
        return None, None, "none"

    # Find all candidate https:// URLs in text + appended hrefs
    all_urls = re.findall(r'https?://[^\s\'"<>\)&]+', body_text)

    candidates: list[str] = []
    for url in all_urls:
        if _WEBCAST_BLOCKLIST_RE.search(url):
            continue
        if _WEBCAST_URL_RE.search(url):
            candidates.append(url)

    if not candidates:
        return None, None, "none"

    # Prefer URLs appearing within 300 chars of a webcast context keyword
    for url in candidates:
        idx = body_text.find(url)
        if idx >= 0:
            context = body_text[max(0, idx - 300): idx + 300]
            if _WEBCAST_CONTEXT_KW.search(context):
                url_clean = url.rstrip(".,;:)'\">")
                return url_clean, source_url, "high"

    # No context match — return first candidate with lower confidence
    url_clean = candidates[0].rstrip(".,;:)'\">")
    return url_clean, source_url, "medium"


# ── Rate limiter ─────────────────────────────────────────────────────────────

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
    Hrefs are extracted before stripping and appended after __HREFS__ separator.
    """
    fname = (filename or url).lower().split("?")[0]
    ext = ("." + fname.rsplit(".", 1)[-1]) if "." in fname else ""

    if ext in _SKIP_EXTENSIONS:
        return ""
    if ext and ext not in _TEXT_EXTENSIONS:
        return ""

    try:
        await _acquire()
        async with client.stream("GET", url, timeout=15.0) as resp:
            if resp.status_code != 200:
                return ""
            ct = resp.headers.get("content-type", "").lower()
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


# ── Canonical filing index parsers ───────────────────────────────────────────


# ── Types to skip when parsing the HTML filing index ─────────────────────────
# These are EDGAR meta-files, XBRL inline viewers, or binary assets — not
# documents we can meaningfully classify.
_SKIP_ATT_TYPES: frozenset[str] = frozenset({
    "GRAPHIC", "XBRL INSTANCE DOCUMENT", "XBRL TAXONOMY EXTENSION SCHEMA DOCUMENT",
    "XBRL TAXONOMY EXTENSION CALCULATION LINKBASE DOCUMENT",
    "XBRL TAXONOMY EXTENSION DEFINITION LINKBASE DOCUMENT",
    "XBRL TAXONOMY EXTENSION LABEL LINKBASE DOCUMENT",
    "XBRL TAXONOMY EXTENSION PRESENTATION LINKBASE DOCUMENT",
    "XBRL TAXONOMY EXTENSION REFERENCES LINKBASE DOCUMENT",
    "XBRL SCHEMA WITH EMBEDDED LINKBASE DOCUMENTS",
    "R-FILE",  # inline XBRL viewer fragment
})


def _is_meta_filename(fname: str, acc_dashes: str) -> bool:
    """True for EDGAR archive/meta filenames that are not content documents."""
    fl = fname.lower()
    # Complete submission archive: 0001679788-26-000053.txt
    if fl == f"{acc_dashes.lower()}.txt":
        return True
    # Filing index pages themselves
    if fl.endswith("-index.html") or fl.endswith("-index.htm") or fl.endswith("-index-headers.html"):
        return True
    # XBRL zip
    if fl.endswith("-xbrl.zip"):
        return True
    # Raw XBRL schema / instance / taxonomy
    if fl.endswith(".xsd"):
        return True
    # Inline XBRL viewer fragments (R2.htm, R3.htm … R50.htm)
    if re.match(r'^r\d+\.htm$', fl):
        return True
    return False


def _parse_index_html(
    html: str,
    cik_num: str,
    acc_clean: str,
    dir_url: str,
    acc_dashes: str = "",
) -> list[dict]:
    """
    Parse the Document Format Files table from an EDGAR {acc_dashes}-index.html page.
    Returns list of attachment dicts with sequence/description/filename/type/url.
    Filters out EDGAR meta-files, XBRL schema/instance files, and inline viewer fragments.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        items: list[dict] = []
        for table in soup.find_all("table", class_="tableFile"):
            rows = table.find_all("tr")
            for row in rows[1:]:   # skip header
                cells = row.find_all(["td", "th"])
                if len(cells) < 4:
                    continue
                seq  = cells[0].get_text(strip=True)
                desc = cells[1].get_text(strip=True)
                a_tag = cells[2].find("a")
                if not a_tag:
                    continue
                href  = (a_tag.get("href") or "").strip()
                fname = a_tag.get_text(strip=True)
                att_type = cells[3].get_text(strip=True)
                size = cells[4].get_text(strip=True) if len(cells) > 4 else ""
                if not fname:
                    continue

                # Filter meta-files and XBRL noise
                if att_type.upper() in _SKIP_ATT_TYPES:
                    continue
                if _is_meta_filename(fname, acc_dashes):
                    continue
                # Skip raw XBRL / JSON / ZIP files by extension
                ext = ("." + fname.rsplit(".", 1)[-1]).lower() if "." in fname else ""
                if ext in {".xsd", ".json", ".zip", ".gz", ".xml"}:
                    continue

                # Resolve URL — strip iXBRL viewer wrapper if present
                if "/ix?doc=" in href:
                    href = href.split("/ix?doc=", 1)[1]
                if href.startswith("/Archives/"):
                    url = f"https://www.sec.gov{href}"
                elif href.startswith("/"):
                    url = f"https://www.sec.gov{href}"
                else:
                    url = f"{dir_url}{fname}"

                items.append({
                    "sequence":    seq,
                    "description": desc,
                    "filename":    fname,
                    "type":        att_type,
                    "size":        size,
                    "url":         url,
                })
        return items
    except Exception as exc:
        print(f"[EI_MATERIALS] HTML index parse error: {exc}")
        return []


def _parse_sgml_headers(text: str, cik_num: str, acc_clean: str) -> list[dict]:
    """
    Parse <DOCUMENT> sections from SGML complete-submission header text.
    Returns list of attachment dicts with sequence/description/filename/type/url.
    """
    try:
        items: list[dict] = []
        doc_sections = re.split(r'<DOCUMENT>', text, flags=re.IGNORECASE)[1:]
        for section in doc_sections[:80]:
            text_match = re.search(r'<TEXT>', section, re.IGNORECASE)
            header = section[:text_match.start()] if text_match else section[:800]

            m_type = re.search(r'<TYPE>([^\n<]+)', header, re.IGNORECASE)
            m_seq  = re.search(r'<SEQUENCE>([^\n<]+)', header, re.IGNORECASE)
            m_fn   = re.search(r'<FILENAME>([^\n<]+)', header, re.IGNORECASE)
            m_desc = re.search(r'<DESCRIPTION>([^\n<]+)', header, re.IGNORECASE)

            att_type = m_type.group(1).strip() if m_type else ""
            seq      = m_seq.group(1).strip() if m_seq else ""
            fname    = m_fn.group(1).strip() if m_fn else ""
            desc     = m_desc.group(1).strip() if m_desc else ""

            if not fname:
                continue
            url = f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{fname}"
            items.append({
                "sequence":    seq,
                "description": desc,
                "filename":    fname,
                "type":        att_type,
                "size":        "",
                "url":         url,
            })
        return items
    except Exception as exc:
        print(f"[EI_MATERIALS] SGML parse error: {exc}")
        return []


# ── Filing index fetch (canonical hierarchy) ─────────────────────────────────

async def _fetch_filing_index(
    client: httpx.AsyncClient,
    cik_num: str,
    accession: str,
) -> tuple[list[dict], str, bool, str]:
    """
    Fetch the list of documents for one accession.

    Canonical EDGAR attachment discovery hierarchy:
      1. GET {directory}index.json           → find index HTML filename
      2. Parse {acc_dashes}-index.html/.htm  → Document Format Files table
      3. GET {acc_dashes}.txt (Range: 0-40KB) → parse SGML DOCUMENT headers
      4. EFTS efts.sec.gov/LATEST/search-index (bounded last resort)

    Returns: (items, discovery_method, attachments_complete, filing_index_url)
      discovery_method: directory_index_html | sgml_headers | efts_fallback | failed
      attachments_complete: True if a complete exhibit manifest was retrieved
      filing_index_url: URL of the HTML index page (or directory) for UI reference
    """
    acc_dashes = accession        # e.g., "0001679788-26-000053"
    acc_clean  = accession.replace("-", "")
    dir_url    = f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/"
    index_html_url = dir_url      # fallback — will be updated if we find the HTML index

    # ── Tier 1: directory/index.json ────────────────────────────────────────
    index_html_name: str | None = None
    dir_filenames: set[str] = set()
    try:
        await _acquire()
        r = await client.get(f"{dir_url}index.json", timeout=12.0)
        if r.status_code == 200:
            raw_items = r.json().get("directory", {}).get("item", [])
            for it in raw_items:
                name = it.get("name", "")
                if not name:
                    continue
                dir_filenames.add(name)
                if index_html_name is None:
                    nl = name.lower()
                    if nl.endswith("-index.html") or nl.endswith("-index.htm"):
                        index_html_name = name
    except Exception as exc:
        print(f"[EI_MATERIALS] dir/index.json error {accession}: {exc}")

    # ── Tier 2: HTML filing index ────────────────────────────────────────────
    if index_html_name:
        html_url = f"{dir_url}{index_html_name}"
        index_html_url = html_url
        try:
            await _acquire()
            r = await client.get(html_url, timeout=12.0)
            if r.status_code == 200:
                items = _parse_index_html(r.text, cik_num, acc_clean, dir_url, acc_dashes)
                if items:
                    return items, "directory_index_html", True, index_html_url
        except Exception as exc:
            print(f"[EI_MATERIALS] HTML index error {accession}: {exc}")
    else:
        # dir/index.json succeeded but couldn't identify index HTML name — try SGML directly
        pass

    # ── Tier 3: Complete-submission SGML headers ─────────────────────────────
    txt_name = f"{acc_dashes}.txt"
    # Only attempt SGML if we got a directory listing (confirms file exists) or as fallback
    if txt_name in dir_filenames or not dir_filenames:
        txt_url = f"{dir_url}{txt_name}"
        try:
            await _acquire()
            range_end = _SGML_RANGE_BYTES - 1
            r = await client.get(
                txt_url, timeout=15.0,
                headers={**_EDGAR_HEADERS, "Range": f"bytes=0-{range_end}"},
            )
            if r.status_code in (200, 206) and r.text:
                items = _parse_sgml_headers(r.text, cik_num, acc_clean)
                if items:
                    return items, "sgml_headers", True, index_html_url
        except Exception as exc:
            print(f"[EI_MATERIALS] SGML error {accession}: {exc}")

    # ── Tier 4: EFTS (bounded last resort) ───────────────────────────────────
    try:
        await _acquire()
        r = await client.get(
            _EFTS_URL,
            params={"q": f'"{acc_dashes}"'},
            timeout=12.0,
        )
        if r.status_code == 200:
            hits = (r.json().get("hits") or {}).get("hits") or []
            items: list[dict] = []
            for hit in hits:
                hit_id  = hit.get("_id") or ""
                src     = hit.get("_source") or {}
                if ":" not in hit_id:
                    continue
                filename = hit_id.split(":", 1)[1]
                if not filename or filename.endswith("/"):
                    continue
                att_type = src.get("file_description") or ""
                seq      = str(src.get("sequence") or "")
                items.append({
                    "filename":    filename,
                    "type":        att_type,
                    "size":        "",
                    "description": att_type,
                    "sequence":    seq,
                    "url":         f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{filename}",
                })
            if items:
                try:
                    items.sort(key=lambda x: int(x["sequence"]) if x["sequence"].isdigit() else 99)
                except Exception:
                    pass
                return items, "efts_fallback", True, index_html_url
        elif r.status_code == 403:
            print(f"[EI_MATERIALS] EFTS 403 for {accession} — all tiers failed")
    except Exception as exc:
        print(f"[EI_MATERIALS] EFTS error {accession}: {exc}")

    return [], "failed", False, index_html_url


# ── Attachment classifier ────────────────────────────────────────────────────

def _classify_attachment(
    filing_form: str,
    items_str: str,
    att_type: str,
    att_desc: str,
    filename: str,
    doc_title: str = "",
    body_text: str = "",       # cleaned attachment text (includes hrefs section)
    is_fallback_mode: bool = False,  # True when this is the only attachment (primary doc)
) -> tuple[str, str, str]:
    """
    Deterministically classify one attachment.

    Returns (classification, confidence, method).
    Classification candidates:
      primary_filing, earnings_release, investor_presentation, supplemental_tables,
      transcript, prepared_remarks, corporate_guidance,
      webcast_or_replay, financial_report, insider_filing,
      ownership_filing, proxy, offering_document, transaction_material, other

    primary_filing: the 8-K/6-K primary body when exhibit identity cannot be confirmed.
    """
    form   = (filing_form or "").upper().strip()
    items  = items_str or ""
    atype  = (att_type or "").upper().strip()
    desc   = (att_desc or "").lower().strip()
    fname  = (filename or "").lower().strip()
    title  = (doc_title or "").lower().strip()
    # Combine metadata + first 4000 chars of body text for keyword scoring
    combined_meta = f"{desc} {title} {fname}"
    combined_full = f"{combined_meta} {body_text.lower()[:4000]}"

    # ── Form-level overrides (always high-confidence) ─────────────────────
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
        # We avoid investor_presentation keyword check for primary body docs because
        # XBRL/iXBRL content embeds "slide", "deck" etc. in XBRL metadata.
        is_primary_body = (atype.upper() in (form.upper(), form.upper().rstrip("/A"), "8-K", "6-K"))

        # Transcript check first (strong keywords, not form-specific)
        if _kw_score(combined_full, _TRANSCRIPT_KW) >= 2:
            conf = "high" if body_text else "medium"
            meth = "transcript_body_kw" if body_text else "transcript_meta_kw"
            return "transcript", conf, meth

        if _kw_score(combined_full, _PREPARED_REMARKS_KW) >= 1:
            return "prepared_remarks", "medium", "prepared_remarks_kw"

        if has_202:
            if _kw_score(combined_full, _SUPPLEMENTAL_KW) >= 1:
                return "supplemental_tables", "medium", "item202_supplemental_kw"

            # Investor presentation — exhibits only (not the primary body)
            if not is_primary_body and _kw_score(combined_full, _INVESTOR_PRESENTATION_KW) >= 1:
                return "investor_presentation", "medium", "item202_presentation_kw"

            if _kw_score(combined_full, _EARNINGS_RELEASE_KW) >= 1:
                conf = "high" if body_text else "medium"
                return "earnings_release", conf, "item202_earnings_kw"

            # item 2.02 but no confirming text:
            # - For an exhibit (not primary body): earnings_release with low confidence
            # - For the primary body (XBRL cover form or only-available attachment):
            #   classify as primary_filing per spec — item 2.02 establishes earnings
            #   context, not the specific attachment role.
            if not is_primary_body:
                return "earnings_release", "low", "item202_exhibit_no_kw"
            else:
                return "primary_filing", "low", "item202_primary_body"

        # Non-2.02 8-K
        if not is_primary_body and _kw_score(combined_full, _INVESTOR_PRESENTATION_KW) >= 1:
            return "investor_presentation", "medium", "8k_non202_presentation_kw"
        if _kw_score(combined_full, _GUIDANCE_KW) >= 2:
            return "corporate_guidance", "medium", "8k_non202_guidance_kw"
        if _kw_score(combined_full, _EARNINGS_RELEASE_KW) >= 1:
            return "earnings_release", "low", "8k_non202_earnings_kw"
        if is_primary_body:
            return "primary_filing", "low", "8k_primary_body"
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
    is_fallback_mode: bool = False,
) -> dict:
    filename = raw_att.get("filename") or ""
    att_type = raw_att.get("type") or ""
    att_desc = raw_att.get("description") or ""
    url      = raw_att.get("url") or f"{_ARCHIVE_URL}/{cik_num}/{acc_clean}/{filename}"

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
        is_fallback_mode=is_fallback_mode,
    )

    # Cap confidence for non-text files we could not inspect
    ext = _ext_of(filename)
    if ext in _SKIP_EXTENSIONS and conf == "high":
        conf = "medium"

    return {
        "filename":                  filename,
        "document_type":             att_type,
        "description":               att_desc,
        "document_url":              url,
        "classification":            cls,
        "classification_confidence": conf,
        "classification_method":     method,
        "text_inspected":            text_fetched,
        "_body_text":                body_text,   # stripped before persisting
    }


# ── Earnings packet assembler ─────────────────────────────────────────────────

def _assemble_earnings_packet(
    filings_with_atts: list[dict],
    today_str: str,
    discovery_method: str = "",
    attachments_complete: bool = True,
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

    # When discovery is incomplete (primary_document_fallback), treat primary_filing
    # attachment as a best-available fallback for the earnings release slot.
    er_is_fallback = False
    if er_att is None and not attachments_complete:
        candidate = _first_by_cls("primary_filing")
        if candidate:
            er_att = candidate
            er_is_fallback = True

    def _att_doc(a: dict | None, is_fallback: bool = False) -> dict | None:
        if not a:
            return None
        d = {
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
        if is_fallback:
            d["_discovery_incomplete"] = True
        return d

    # ── Webcast URL extraction ─────────────────────────────────────────────────
    webcast_url: str | None = None
    webcast_source_document: str | None = None
    webcast_extraction_confidence: str = "none"

    for candidate_att in [er_att, pres_att, wc_att, rem_att]:
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
            webcast_url = candidate_att.get("document_url")
            webcast_source_document = candidate_att.get("document_url")
            webcast_extraction_confidence = "low"
            break

    # ── Transcript state ───────────────────────────────────────────────────────
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
            transcript_status  = "available_sec_exhibit"
            transcript_source_type = "sec_exhibit"
        else:
            transcript_status  = "available_sec_exhibit"
            transcript_source_type = "sec_exhibit_unconfirmed"
        transcript = {
            "status":      transcript_status,
            "source_type": transcript_source_type,
            "source_url":  tr_att.get("document_url"),
        }
    else:
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
        "attachments_complete": attachments_complete,
        "discovery_method":    discovery_method,
        "primary_filing":      primary_filing,
        "earnings_release":    _att_doc(er_att, is_fallback=er_is_fallback),
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

    now_iso   = datetime.now(timezone.utc).isoformat()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    errors: dict = {}
    # Aggregate discovery method counts across all fetched filings
    method_counts: dict[str, int] = {}

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
                    "fetched_at":        now_iso,
                    "coverage":          False,
                    "discovery_version": _DISCOVERY_VERSION,
                    "classifier_version": _CLASSIFIER_VERSION,
                    "errors":            {"cik": "not_found"},
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
                    "fetched_at":        now_iso,
                    "coverage":          False,
                    "discovery_version": _DISCOVERY_VERSION,
                    "classifier_version": _CLASSIFIER_VERSION,
                    "errors":            errors,
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
            if form not in _MONITORED_FORMS and not form.startswith("424B"):
                continue

            acc_clean = accession.replace("-", "")
            cik_num_path = cik_num   # integer-form CIK, no leading zeros
            pri_url = (
                f"{_ARCHIVE_URL}/{cik_num_path}/{acc_clean}/{pri_doc}"
                if acc_clean and pri_doc else ""
            )
            # Use the directory URL as placeholder; updated after index fetch below
            idx_url = (
                f"{_ARCHIVE_URL}/{cik_num_path}/{acc_clean}/"
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
                "_primary_doc_url": pri_url,
                "_primary_doc_name": pri_doc,
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
                    raw_atts, disc_method, att_complete, idx_url = await _fetch_filing_index(
                        client, cik_num, f["accession_number"]
                    )
                    # Update filing_index_url with the discovered HTML index URL
                    if idx_url:
                        f["filing_index_url"] = idx_url

                    method_counts[disc_method] = method_counts.get(disc_method, 0) + 1

                    acc_clean_f = f["accession_number"].replace("-", "")

                    if not raw_atts and f.get("_primary_doc_url"):
                        # Primary-document-only fallback
                        disc_method  = "primary_document_fallback"
                        att_complete = False
                        method_counts[disc_method] = method_counts.get(disc_method, 0) + 1
                        pri_name = (f["_primary_doc_url"] or "").rsplit("/", 1)[-1] or ""
                        raw_atts = [{
                            "filename":    pri_name,
                            "type":        f["form"],   # primary body signal
                            "size":        "",
                            "description": f.get("title") or f["form"],
                            "sequence":    "1",
                            "url":         f["_primary_doc_url"],
                        }]

                    f["_discovery_method"]      = disc_method
                    f["_attachments_complete"]  = att_complete

                    should_fetch_text = f["form"] in (
                        "8-K", "8-K/A", "6-K", "10-K", "10-K/A", "10-Q", "10-Q/A"
                    )
                    tasks = [
                        _build_attachment_async(
                            client, f["form"], f["items"], a,
                            cik_num, f["accession_number"], acc_clean_f,
                            fetch_text=should_fetch_text,
                            is_fallback_mode=not att_complete,
                        )
                        for a in raw_atts
                    ]
                    built = await asyncio.gather(*tasks, return_exceptions=True)
                    f["attachments"] = [a for a in built if isinstance(a, dict)]

                except Exception as exc:
                    errors[f"index_{f['accession_number']}"] = str(exc)[:120]

        # 5. Assemble earnings packet
        # Find the first earnings 8-K's discovery metadata for the packet
        packet_disc_method = ""
        packet_att_complete = True
        for f in raw_filings:
            if _is_earnings_8k(f.get("form", ""), f.get("items", "")):
                packet_disc_method   = f.get("_discovery_method", "")
                packet_att_complete  = f.get("_attachments_complete", True)
                break

        try:
            latest_packet = _assemble_earnings_packet(
                raw_filings, today_str,
                discovery_method=packet_disc_method,
                attachments_complete=packet_att_complete,
            )
        except Exception as exc:
            errors["packet_assembly"] = str(exc)[:120]
            latest_packet = None

        # 6. Build response — strip internal _body_text before persisting
        recent_filings_out: list[dict] = []
        for f in raw_filings:
            clean_atts = []
            for a in f["attachments"]:
                a2 = {k: v for k, v in a.items() if not k.startswith("_")}
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
                "discovery_method":     f.get("_discovery_method", ""),
                "attachments_complete": f.get("_attachments_complete", False),
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
                "fetched_at":          now_iso,
                "coverage":            bool(raw_filings),
                "cik":                 cik,
                "filing_count":        len(recent_filings_out),
                "earnings_8k_count":   sum(1 for f in recent_filings_out if f.get("category") == "earnings"),
                "classification_counts":   cls_counts,
                "transcript_state_counts": transcript_states,
                "discovery_method_counts": method_counts,
                "discovery_version":       _DISCOVERY_VERSION,
                "classifier_version":      _CLASSIFIER_VERSION,
                "errors":                  errors,
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

    refreshed   = 0
    skipped     = 0
    failed      = 0
    no_cik      = 0
    no_packet   = 0
    failed_syms: list[str] = []
    cls_counts: dict[str, int] = {}
    ts_counts:  dict[str, int] = {}
    method_counts: dict[str, int] = {}

    for sym in symbols:
        if progress_state is not None:
            progress_state["current_symbol"] = sym

        if not force and not needs_refresh(sym):
            skipped += 1
            if progress_state is not None:
                progress_state["skipped"] = progress_state.get("skipped", 0) + 1
            continue

        try:
            result = await fetch_and_cache_materials(sym)
            if result is None:
                failed += 1
                failed_syms.append(sym)
                if progress_state is not None:
                    progress_state["failed"] = progress_state.get("failed", 0) + 1
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
            for m, n in (ss.get("discovery_method_counts") or {}).items():
                method_counts[m] = method_counts.get(m, 0) + n

            refreshed += 1
            if progress_state is not None:
                progress_state["refreshed"] = progress_state.get("refreshed", 0) + 1

        except Exception as exc:
            failed += 1
            failed_syms.append(sym)
            if progress_state is not None:
                progress_state["failed"] = progress_state.get("failed", 0) + 1
            print(f"[EI_MATERIALS] backfill error {sym}: {exc}")

        await asyncio.sleep(0.6)   # ~1.67 req/s — SEC-compliant

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
        "discovery_method_counts": method_counts,
    }
