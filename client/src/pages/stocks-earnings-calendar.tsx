import { useState, useEffect, useCallback, useRef } from "react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { ExternalLink, Loader2, Sparkles, Calendar, ChevronLeft, ChevronRight, CalendarDays, X, Clock, Send, MessageSquare } from "lucide-react";

// ─── DATA FLOW AUDIT (March 2026) ─────────────────────────────
//
// API CALLS ON PAGE LOAD:
//   1. Polymarket Gamma API (free, no key) — fetch 50 earnings events
//   2. Finnhub /earnings_calendar (free tier, included in API key) — current week
//   Both are lightweight list calls, no per-ticker enrichment.
//
// API CALLS ON SCROLL:
//   None. The entry list is paginated in batches of 15 (render-only, no API calls).
//
// API CALLS ON CLICK (per-ticker detail):
//   1. Finnhub company_profile (24hr cache) — name, sector, market_cap, logo
//   2. Finnhub earnings_surprises (1hr cache) — past 4 quarters beat/miss
//   3. Finnhub earnings_calendar per-ticker (no cache) — upcoming dates
//   4. Finnhub recommendation_trends (10min cache) — analyst buy/hold/sell
//   5. Finnhub quote (1min cache) — current price
//   6. Finnhub company_news (no cache) — recent articles
//   7. SEC EDGAR XBRL (free, 6hr cache) — revenue, financials
//   Total: ~7 Finnhub calls + 1-2 EDGAR calls per click (all free tier).
//
// PERPLEXITY / LLM CALLS:
//   NONE. Zero Perplexity, zero LLM calls anywhere in this flow.
//   News sentiment is a simple keyword heuristic (backend lines 713-719).
//   news_summary field exists but is always empty string — no LLM generates it.
//
// RATE LIMITS:
//   /api/earnings/calendar: 10/minute (slowapi)
//   /api/earnings/detail:   30/minute (slowapi)
//   Finnhub free tier: 60 calls/minute (shared across all endpoints)
//   SEC EDGAR: Token bucket 2 req/sec, circuit breaker on 429
//
// CACHING (backend in-memory TTL):
//   Calendar response: 5 min
//   Detail response:   10 min
//   Company profile:   24 hr
//   Earnings history:  1 hr
//   EDGAR financials:  6 hr
//   EDGAR CIK map:     7 days
// ───────────────────────────────────────────────────────────────

// ─── Constants ────────────────────────────────────────────────────
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";
const POLYMARKET_PROXY = `${AGENT_BACKEND_URL}/api/polymarket/events`;
const GAMMA_API = "https://gamma-api.polymarket.com/events";
const REFRESH_INTERVAL = 60_000;

// ─── Types ────────────────────────────────────────────────────────
interface PolyMarket {
  id: string;
  question: string;
  slug: string;
  outcomes: string;
  outcomePrices: string;
  volume24hr: number;
  liquidity: string;
  active: boolean;
  closed: boolean;
}

interface PolyEvent {
  id: string;
  title: string;
  slug: string;
  description: string;
  active: boolean;
  closed: boolean;
  volume24hr: number;
  volume: number;
  liquidity: number;
  competitive: number;
  commentCount: number;
  endDate?: string;
  tags: Array<{ id: string; label: string; slug: string }>;
  markets: PolyMarket[];
}

interface ParsedMarket {
  eventId: string;
  eventTitle: string;
  eventSlug: string;
  marketId: string;
  question: string;
  description: string;
  yesPrice: number;
  noPrice: number;
  volume24hr: number;
  totalVolume: number;
  liquidity: number;
  tags: string[];
  endDate?: string;
}

interface FinnhubEarning {
  ticker: string;
  date: string;
  eps_estimate: number | null;
  eps_actual: number | null;
  revenue_estimate: number | null;
  revenue_actual: number | null;
  hour: string;  // "bmo", "amc", or ""
  quarter: number | null;
  year: number | null;
}

interface EarningsEntry {
  market: ParsedMarket | null;
  ticker: string;
  company: string;
  eps: string | null;
  quarter: string | null;
  time: string | null;
  exchange: string | null;
  beatPct: number;
  revenueEstimate: string | null;
  source: "polymarket" | "finnhub" | "both";
  earningsDate: string | null;
}

interface EarningsDetailData {
  ticker: string;
  company_name?: string;
  sector?: string;
  industry?: string;
  market_cap?: number;
  current_price?: number;
  price_change_pct?: number;
  logo?: string;
  beat_rate?: string;
  beat_pct?: number;
  avg_surprise_pct?: number;
  earnings_history?: { period: string; actual_eps: number | null; estimate_eps: number | null; surprise_percent: number | null; beat: boolean | null }[];
  analyst_consensus?: { buy: number; hold: number; sell: number; total: number; rating: string };
  news_articles?: { title: string; source: string; content: string; url: string }[];
  news_summary?: string;
  news_sentiment?: string;
}

interface SmartTicker {
  ticker: string;
  date: string;
  eps_estimate: number | null;
  revenue_estimate: number | null;
  hour: string;
  quarter: number | null;
  year: number | null;
  buzz_level: number;
  sentiment: string;
  news_signal: string;
  analyst_focus: boolean;
  one_line: string;
  score: number;
}

interface SmartDayData {
  tickers: SmartTicker[];
  count: number;
  cached_at: number;
  fallback?: boolean;
  scanning?: boolean;
  cache_status?: { status: string; last_updated: string | null; age_hours?: number };
}

// ─── Helpers ──────────────────────────────────────────────────────
function parsePriceArray(raw: string): number[] {
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map(Number) : [];
  } catch {
    return [];
  }
}

function parseTagEvents(events: PolyEvent[]): ParsedMarket[] {
  const results: ParsedMarket[] = [];
  for (const ev of events) {
    if (!ev.active || ev.closed) continue;
    for (const m of ev.markets || []) {
      if (!m.active || m.closed) continue;
      const prices = parsePriceArray(m.outcomePrices || "[]");
      const yesPrice = prices[0] ?? 0;
      const noPrice = prices[1] ?? 1 - yesPrice;
      results.push({
        eventId: ev.id,
        eventTitle: ev.title,
        eventSlug: ev.slug,
        marketId: m.id,
        question: m.question || ev.title,
        description: ev.description || "",
        yesPrice,
        noPrice,
        volume24hr: m.volume24hr || ev.volume24hr || 0,
        totalVolume: ev.volume || 0,
        liquidity: parseFloat(m.liquidity || "0") || ev.liquidity || 0,
        tags: (ev.tags || []).map((t) => t.label),
        endDate: ev.endDate,
      });
    }
  }
  results.sort((a, b) => b.volume24hr - a.volume24hr);
  return results;
}

function formatVolume(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function formatMktCap(v: number | undefined): string {
  if (!v) return "N/A";
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${v.toLocaleString()}`;
}

function extractTicker(question: string): string | null {
  const match = question.match(/\(([A-Z]{1,5})\)/);
  return match ? match[1] : null;
}

function extractCompanyName(question: string): string {
  const match = question.match(/^Will\s+(.+?)\s+(?:\(|beat|miss|report)/i);
  if (match) {
    let name = match[1].trim();
    if (name.endsWith("'s")) name = name.slice(0, -2);
    return name;
  }
  const ticker = extractTicker(question);
  if (ticker) return ticker;
  return question.length > 40 ? question.slice(0, 37) + "..." : question;
}

function extractEPS(description: string): string | null {
  const m = description.match(/(?:consensus|EPS|earnings)\s+(?:estimate|forecast)\s+of\s+\$?([\-\d.]+)/i)
    || description.match(/\$?([\-\d.]+)\s+(?:EPS|per share)/i)
    || description.match(/estimate\s+of\s+\$?([\-\d.]+)/i);
  return m ? `$${m[1]}` : null;
}

function extractQuarter(text: string): string | null {
  const m = text.match(/\b(Q[1-4])\s*(?:FY|CY|)\s*['"]?(\d{4})/i)
    || text.match(/\b(Q[1-4])\s+(\d{4})/i)
    || text.match(/\b(Q[1-4])\b/i);
  if (m) return m[2] ? `${m[1].toUpperCase()} ${m[2]}` : m[1].toUpperCase();
  return null;
}

function extractTime(description: string): string | null {
  if (/before\s+(the\s+)?market\s+open/i.test(description) || /pre[\s-]?market/i.test(description) || /BMO/i.test(description))
    return "Pre-Market";
  if (/after\s+(the\s+)?market\s+close/i.test(description) || /post[\s-]?market/i.test(description) || /AMC/i.test(description))
    return "After Hours";
  const timeMatch = description.match(/(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm))/);
  if (timeMatch) return timeMatch[1];
  return null;
}

function extractExchange(description: string): string | null {
  const m = description.match(/\b(NASDAQ|NYSE|AMEX|TSX|LSE)\b/i);
  return m ? m[1].toUpperCase() : null;
}

function buildEntry(m: ParsedMarket): EarningsEntry {
  const combined = `${m.question} ${m.description}`;
  const revMatch = m.description.match(/revenue\s+(?:estimate|forecast|consensus)?\s*(?:of\s+)?\$?([\d.]+\s*(?:B|M|billion|million))/i);
  return {
    market: m,
    ticker: extractTicker(m.question) || "???",
    company: extractCompanyName(m.question),
    eps: extractEPS(m.description),
    quarter: extractQuarter(combined),
    time: extractTime(m.description),
    exchange: extractExchange(m.description),
    beatPct: Math.round(m.yesPrice * 100),
    revenueEstimate: revMatch ? `$${revMatch[1]}` : null,
    source: "polymarket",
    earningsDate: m.endDate || null,
  };
}

function buildFinnhubEntry(e: FinnhubEarning): EarningsEntry {
  const hour = e.hour === "bmo" ? "Pre-Market" : e.hour === "amc" ? "After Hours" : null;
  const qtr = e.quarter && e.year ? `Q${e.quarter} ${e.year}` : e.quarter ? `Q${e.quarter}` : null;
  const epsStr = e.eps_estimate != null ? `$${e.eps_estimate.toFixed(2)}` : null;
  const revStr = e.revenue_estimate != null
    ? (e.revenue_estimate >= 1e9 ? `$${(e.revenue_estimate / 1e9).toFixed(1)}B` : e.revenue_estimate >= 1e6 ? `$${(e.revenue_estimate / 1e6).toFixed(0)}M` : `$${e.revenue_estimate.toLocaleString()}`)
    : null;
  return {
    market: null,
    ticker: e.ticker,
    company: e.ticker,  // Will be enriched later
    eps: epsStr,
    quarter: qtr,
    time: hour,
    exchange: null,
    beatPct: -1,  // -1 = no Polymarket data
    revenueEstimate: revStr,
    source: "finnhub",
    earningsDate: e.date,
  };
}

function formatRevenue(v: number | null | undefined): string {
  if (!v) return "N/A";
  if (v >= 1e12) return `$${(v / 1e12).toFixed(1)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${v.toLocaleString()}`;
}

function buildBullets(entry: EarningsEntry): string[] {
  const bullets: string[] = [];
  const desc = entry.market?.description || "";

  if (entry.eps) {
    const q = entry.quarter || "quarterly";
    bullets.push(`Wall Street consensus EPS estimate of ${entry.eps} for ${q} earnings`);
  }

  if (entry.revenueEstimate) {
    bullets.push(`Revenue estimate: ${entry.revenueEstimate}`);
  }

  if (entry.beatPct >= 0) {
    const beatLabel = entry.beatPct >= 70 ? "strongly favored to beat" : entry.beatPct >= 55 ? "favored to beat" : entry.beatPct <= 30 ? "expected to miss" : entry.beatPct <= 45 ? "at risk of missing" : "near a coin flip on beating";
    bullets.push(`Polymarket crowd: ${beatLabel} estimates (${entry.beatPct}% chance of beat)`);
  }

  const revMatch = desc.match(/revenue\s+(?:estimate|forecast|consensus)?\s*(?:of\s+)?\$?([\d.]+\s*(?:B|M|billion|million))/i);
  if (revMatch) {
    bullets.push(`Revenue estimate: $${revMatch[1]}`);
  }

  const sentences = desc.split(/[.!]\s+/).filter((s) => s.length > 20 && s.length < 200);
  for (const s of sentences) {
    if (bullets.length >= 3) break;
    const lower = s.toLowerCase();
    if (lower.includes("resolve to") || lower.includes("this market")) continue;
    if (lower.includes("reports") || lower.includes("announces") || lower.includes("expects") || lower.includes("growth") || lower.includes("decline") || lower.includes("revenue") || lower.includes("sector")) {
      const cleaned = s.trim().replace(/^\W+/, "");
      if (cleaned && !bullets.some((b) => b.includes(cleaned.slice(0, 30)))) {
        bullets.push(cleaned.endsWith(".") ? cleaned : cleaned + ".");
      }
    }
  }

  if (bullets.length < 2 && entry.market) {
    bullets.push(`Trading volume: ${formatVolume(entry.market.totalVolume)} total on Polymarket`);
  }

  return bullets.slice(0, 3);
}

const TICKER_COLORS = [
  "from-blue-500 to-blue-600",
  "from-purple-500 to-purple-600",
  "from-emerald-500 to-emerald-600",
  "from-orange-500 to-orange-600",
  "from-rose-500 to-rose-600",
  "from-cyan-500 to-cyan-600",
  "from-yellow-500 to-yellow-600",
  "from-indigo-500 to-indigo-600",
  "from-pink-500 to-pink-600",
  "from-teal-500 to-teal-600",
];
function tickerColor(ticker: string): string {
  let h = 0;
  for (let i = 0; i < ticker.length; i++) h = (h * 31 + ticker.charCodeAt(i)) | 0;
  return TICKER_COLORS[Math.abs(h) % TICKER_COLORS.length];
}

function getSunday(d: Date): Date {
  const dt = new Date(d);
  const day = dt.getDay();
  dt.setDate(dt.getDate() - day);
  dt.setHours(0, 0, 0, 0);
  return dt;
}

function addDays(d: Date, n: number): Date {
  const dt = new Date(d);
  dt.setDate(dt.getDate() + n);
  return dt;
}

function dateKey(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

const MONTH_NAMES_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const MONTH_NAMES = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
const DAY_NAMES_FULL = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded bg-white/5 ${className}`} />;
}

async function fetchPolymarketByTag(tagSlug: string): Promise<PolyEvent[] | null> {
  try {
    const proxyRes = await fetch(
      `${POLYMARKET_PROXY}?limit=50&tag_slug=${encodeURIComponent(tagSlug)}`
    );
    if (proxyRes.ok) {
      const json = await proxyRes.json();
      if (Array.isArray(json)) return json;
    }
  } catch { /* fall through */ }

  try {
    const directRes = await fetch(
      `${GAMMA_API}?limit=50&active=true&closed=false&order=volume24hr&ascending=false&tag_slug=${encodeURIComponent(tagSlug)}`
    );
    if (directRes.ok) {
      const json = await directRes.json();
      if (Array.isArray(json)) return json;
    }
  } catch { /* fall through */ }

  return null;
}

async function fetchFinnhubCalendar(fromDate: string, toDate: string): Promise<FinnhubEarning[]> {
  try {
    const res = await fetch(
      `${AGENT_BACKEND_URL}/api/earnings/calendar?from_date=${encodeURIComponent(fromDate)}&to_date=${encodeURIComponent(toDate)}`
    );
    if (res.ok) {
      const data = await res.json();
      return data.earnings || [];
    }
  } catch (e) {
    console.warn("[FINNHUB_CALENDAR] fetch failed:", e);
  }
  return [];
}

async function fetchSmartEarnings(date: string): Promise<SmartDayData | null> {
  try {
    const res = await fetch(
      `${AGENT_BACKEND_URL}/api/earnings/smart/${encodeURIComponent(date)}`
    );
    if (res.ok) {
      return await res.json();
    }
  } catch (e) {
    console.warn("[SMART_EARNINGS] fetch failed:", e);
  }
  return null;
}

// ─── Earnings Detail Modal ────────────────────────────────────────

function EarningsModal({ entry, onClose, prefetchedDetail }: { entry: EarningsEntry; onClose: () => void; prefetchedDetail?: EarningsDetailData | null }) {
  const bullets = buildBullets(entry);
  const beatPct = entry.beatPct;
  const missPct = 100 - beatPct;
  const [detail, setDetail] = useState<EarningsDetailData | null>(prefetchedDetail || null);
  const [loading, setLoading] = useState(!prefetchedDetail);
  const [fetchError, setFetchError] = useState(false);

  useEffect(() => {
    if (prefetchedDetail) { setDetail(prefetchedDetail); setLoading(false); return; }
    if (!entry.ticker || entry.ticker === "???") {
      setLoading(false);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(
          `${AGENT_BACKEND_URL}/api/earnings/detail?ticker=${encodeURIComponent(entry.ticker)}`
        );
        if (!cancelled) {
          if (res.ok) {
            const data = await res.json();
            setDetail(data);
          } else {
            setFetchError(true);
          }
        }
      } catch (e) {
        console.warn("[EarningsModal] detail fetch failed:", e);
        if (!cancelled) setFetchError(true);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [entry.ticker, prefetchedDetail]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  const companyName = detail?.company_name || entry.company;
  const history = detail?.earnings_history || [];
  const consensus = detail?.analyst_consensus;
  const articles = detail?.news_articles || [];

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" />
      <div
        className="relative w-full max-w-2xl max-h-[90vh] bg-[#0c0c0f] border border-white/10 rounded-2xl shadow-2xl overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-6 pt-5 pb-4 border-b border-white/[0.06] sticky top-0 bg-[#0c0c0f] z-10">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              {detail?.logo ? (
                <img src={detail.logo} alt={entry.ticker} className="w-10 h-10 rounded-xl object-contain bg-white/5 p-1" />
              ) : (
                <div className={`w-10 h-10 rounded-xl bg-gradient-to-br ${tickerColor(entry.ticker)} flex items-center justify-center`}>
                  <span className="text-sm font-bold text-white">{entry.ticker.slice(0, 2)}</span>
                </div>
              )}
              <div>
                <h3 className="text-base font-bold text-white">{companyName}</h3>
                <div className="flex items-center gap-2 mt-0.5">
                  <span className="text-xs font-mono text-white/50">{entry.ticker}</span>
                  {entry.exchange && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-white/[0.06] text-white/30">{entry.exchange}</span>
                  )}
                  {entry.quarter && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400/70">{entry.quarter}</span>
                  )}
                  {detail?.sector && (
                    <span className="text-[9px] px-1.5 py-0.5 rounded bg-white/[0.04] text-white/25">{detail.sector}</span>
                  )}
                </div>
              </div>
            </div>
            <div className="flex items-center gap-3">
              {detail?.current_price && (
                <div className="text-right">
                  <p className="text-sm font-bold text-white">${detail.current_price.toFixed(2)}</p>
                  {detail.price_change_pct != null && (
                    <p className={`text-[10px] font-semibold ${detail.price_change_pct >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                      {detail.price_change_pct >= 0 ? "+" : ""}{detail.price_change_pct.toFixed(2)}%
                    </p>
                  )}
                </div>
              )}
              <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-white/5 transition-colors">
                <X className="w-4 h-4 text-white/40" />
              </button>
            </div>
          </div>
        </div>

        {/* Polymarket Beat / Miss probability */}
        {beatPct >= 0 && (
        <div className="px-6 py-4 border-b border-white/[0.06]">
          <div className="flex items-center justify-between mb-2">
            <span className="text-[11px] font-semibold text-white/50 uppercase tracking-wider">Polymarket: Chance of Beat</span>
            <span className={`text-lg font-bold ${beatPct >= 50 ? "text-emerald-400" : "text-red-400"}`}>
              {beatPct}%
            </span>
          </div>
          <div className="h-3 rounded-full bg-white/5 overflow-hidden flex">
            <div
              className="h-full rounded-l-full transition-all duration-500"
              style={{ width: `${beatPct}%`, background: "linear-gradient(90deg, #22c55e, #4ade80)" }}
            />
            <div
              className="h-full rounded-r-full transition-all duration-500"
              style={{ width: `${missPct}%`, background: "linear-gradient(90deg, #ef4444, #f87171)" }}
            />
          </div>
          <div className="flex justify-between mt-1.5 text-[10px] font-semibold">
            <span className="text-emerald-400/70">Beat {beatPct}%</span>
            <span className="text-red-400/70">Miss {missPct}%</span>
          </div>
        </div>
        )}

        {/* Stats grid */}
        <div className="px-6 py-4 border-b border-white/[0.06]">
          <div className="grid grid-cols-3 sm:grid-cols-5 gap-2.5">
            {entry.eps && (
              <div className="bg-white/[0.03] rounded-lg p-2.5 text-center">
                <p className="text-[9px] text-white/30 uppercase tracking-wider mb-1">EPS Est.</p>
                <p className="text-sm font-bold text-white">{entry.eps}</p>
              </div>
            )}
            {entry.time && (
              <div className="bg-white/[0.03] rounded-lg p-2.5 text-center">
                <p className="text-[9px] text-white/30 uppercase tracking-wider mb-1">Report Time</p>
                <p className="text-sm font-bold text-white">{entry.time}</p>
              </div>
            )}
            {detail?.beat_rate && (
              <div className="bg-white/[0.03] rounded-lg p-2.5 text-center">
                <p className="text-[9px] text-white/30 uppercase tracking-wider mb-1">Beat Record</p>
                <p className="text-sm font-bold text-emerald-400">{detail.beat_rate}</p>
              </div>
            )}
            {detail?.avg_surprise_pct != null && (
              <div className="bg-white/[0.03] rounded-lg p-2.5 text-center">
                <p className="text-[9px] text-white/30 uppercase tracking-wider mb-1">Avg Surprise</p>
                <p className={`text-sm font-bold ${detail.avg_surprise_pct >= 0 ? "text-emerald-400" : "text-red-400"}`}>
                  {detail.avg_surprise_pct >= 0 ? "+" : ""}{detail.avg_surprise_pct}%
                </p>
              </div>
            )}
            {detail?.market_cap && (
              <div className="bg-white/[0.03] rounded-lg p-2.5 text-center">
                <p className="text-[9px] text-white/30 uppercase tracking-wider mb-1">Mkt Cap</p>
                <p className="text-sm font-bold text-white">{formatMktCap(detail.market_cap)}</p>
              </div>
            )}
          </div>
        </div>

        {/* Earnings History */}
        {history.length > 0 && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider mb-3">Earnings History</h4>
            <div className="grid grid-cols-4 gap-2">
              {history.slice(0, 4).map((h, i) => (
                <div key={i} className={`rounded-lg p-2.5 text-center border ${
                  h.beat === true ? "bg-emerald-500/5 border-emerald-500/15" :
                  h.beat === false ? "bg-red-500/5 border-red-500/15" :
                  "bg-white/[0.02] border-white/[0.06]"
                }`}>
                  <p className="text-[9px] text-white/40 mb-1">{h.period || `Q${4 - i}`}</p>
                  <p className={`text-xs font-bold ${h.beat === true ? "text-emerald-400" : h.beat === false ? "text-red-400" : "text-white/50"}`}>
                    {h.beat === true ? "BEAT" : h.beat === false ? "MISS" : "N/A"}
                  </p>
                  {h.actual_eps != null && h.estimate_eps != null && (
                    <p className="text-[9px] text-white/30 mt-0.5">
                      ${h.actual_eps.toFixed(2)} vs ${h.estimate_eps.toFixed(2)}
                    </p>
                  )}
                  {h.surprise_percent != null && (
                    <p className={`text-[9px] font-semibold mt-0.5 ${(h.surprise_percent || 0) >= 0 ? "text-emerald-400/60" : "text-red-400/60"}`}>
                      {h.surprise_percent >= 0 ? "+" : ""}{h.surprise_percent.toFixed(1)}%
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Analyst Consensus */}
        {consensus && consensus.total > 0 && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <div className="flex items-center justify-between mb-2.5">
              <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider">Analyst Consensus</h4>
              <span className={`text-xs font-bold px-2 py-0.5 rounded ${
                consensus.rating === "Buy" ? "bg-emerald-500/10 text-emerald-400" :
                consensus.rating === "Sell" ? "bg-red-500/10 text-red-400" :
                "bg-yellow-500/10 text-yellow-400"
              }`}>
                {consensus.rating}
              </span>
            </div>
            <div className="h-2.5 rounded-full bg-white/5 overflow-hidden flex">
              <div className="h-full bg-emerald-500/70" style={{ width: `${(consensus.buy / consensus.total) * 100}%` }} />
              <div className="h-full bg-yellow-500/50" style={{ width: `${(consensus.hold / consensus.total) * 100}%` }} />
              <div className="h-full bg-red-500/70" style={{ width: `${(consensus.sell / consensus.total) * 100}%` }} />
            </div>
            <div className="flex justify-between mt-1.5 text-[9px]">
              <span className="text-emerald-400/70">Buy {consensus.buy}</span>
              <span className="text-yellow-400/70">Hold {consensus.hold}</span>
              <span className="text-red-400/70">Sell {consensus.sell}</span>
            </div>
          </div>
        )}

        {/* AI News Summary + Sentiment */}
        {detail?.news_summary && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <div className="flex items-center justify-between mb-2.5">
              <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider flex items-center gap-1.5">
                <Sparkles className="w-3 h-3 text-blue-400" /> Earnings Context
              </h4>
              {detail.news_sentiment && (
                <span className={`text-[9px] font-bold px-2 py-0.5 rounded ${
                  detail.news_sentiment === "Bullish" ? "bg-emerald-500/10 text-emerald-400" :
                  detail.news_sentiment === "Bearish" ? "bg-red-500/10 text-red-400" :
                  "bg-white/[0.06] text-white/40"
                }`}>
                  {detail.news_sentiment}
                </span>
              )}
            </div>
            <p className="text-xs text-white/60 leading-relaxed">{detail.news_summary}</p>
          </div>
        )}

        {/* Recent News Articles */}
        {articles.length > 0 && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider mb-3">Recent News</h4>
            <div className="space-y-2.5">
              {articles.slice(0, 4).map((a, i) => (
                <a
                  key={i}
                  href={a.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="block rounded-lg p-3 bg-white/[0.02] border border-white/[0.04] hover:bg-white/[0.04] hover:border-white/[0.08] transition-all group"
                >
                  <p className="text-xs font-semibold text-white/80 group-hover:text-blue-400 transition-colors leading-snug">
                    {a.title}
                  </p>
                  {a.content && (
                    <p className="text-[10px] text-white/40 mt-1 leading-relaxed line-clamp-2">
                      {a.content.slice(0, 180)}
                    </p>
                  )}
                  {a.source && (
                    <p className="text-[9px] text-white/20 mt-1">{a.source}</p>
                  )}
                </a>
              ))}
            </div>
          </div>
        )}

        {/* Loading / Error / Polymarket fallback bullets */}
        {loading && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <div className="flex items-center justify-center py-4 gap-2">
              <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />
              <span className="text-xs text-white/40">Loading earnings data from Finnhub...</span>
            </div>
          </div>
        )}
        {!loading && fetchError && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <div className="rounded-lg p-3 bg-red-500/5 border border-red-500/15">
              <p className="text-[11px] text-red-400/80 font-semibold">Unable to load enriched data</p>
              <p className="text-[10px] text-white/30 mt-1">Make sure the backend is running with the latest code. The /api/earnings/detail endpoint is required.</p>
            </div>
            <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider mb-3 mt-3">Polymarket Data</h4>
            <ul className="space-y-2">
              {bullets.map((b, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="w-1 h-1 rounded-full bg-blue-400 mt-1.5 flex-shrink-0" />
                  <span className="text-xs text-white/70 leading-relaxed">{b}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
        {!loading && !fetchError && !detail && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider mb-3">Key Details</h4>
            <ul className="space-y-2">
              {bullets.map((b, i) => (
                <li key={i} className="flex items-start gap-2">
                  <span className="w-1 h-1 rounded-full bg-blue-400 mt-1.5 flex-shrink-0" />
                  <span className="text-xs text-white/70 leading-relaxed">{b}</span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Polymarket volume stats */}
        {entry.market && (
        <div className="px-6 py-3 border-b border-white/[0.06]">
          <div className="flex items-center gap-4 text-[10px] text-white/30">
            <span><span className="text-white/50 font-semibold">24h Vol:</span> {formatVolume(entry.market.volume24hr)}</span>
            <span><span className="text-white/50 font-semibold">Total Vol:</span> {formatVolume(entry.market.totalVolume)}</span>
            <span><span className="text-white/50 font-semibold">Liquidity:</span> {formatVolume(entry.market.liquidity)}</span>
          </div>
        </div>
        )}

        {/* Footer */}
        <div className="px-6 py-4 flex items-center justify-between">
          <span className="text-[9px] text-white/20">{entry.source === "polymarket" || entry.source === "both" ? "Polymarket + Finnhub" : "Finnhub"}</span>
          {entry.market && (
          <a
            href={`https://polymarket.com/event/${entry.market.eventSlug}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-[11px] font-semibold text-blue-400 hover:text-blue-300 transition-colors"
          >
            Trade on Polymarket <ExternalLink className="w-3 h-3" />
          </a>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Earnings Calendar Component ──────────────────────────────────

function EarningsCalendarWidget({ markets }: { markets: ParsedMarket[] }) {
  const [weekStart, setWeekStart] = useState<Date>(() => getSunday(new Date()));
  const [selectedDayKey, setSelectedDayKey] = useState<string>(dateKey(new Date()));
  const [modalEntry, setModalEntry] = useState<EarningsEntry | null>(null);
  const [enrichments, setEnrichments] = useState<Record<string, EarningsDetailData>>({});
  const [enrichLoading, setEnrichLoading] = useState<Set<string>>(new Set());
  const [finnhubEntries, setFinnhubEntries] = useState<Map<string, EarningsEntry[]>>(new Map());
  const [finnhubLoading, setFinnhubLoading] = useState(false);
  const finnhubFetchedWeeks = useRef<Set<string>>(new Set());

  // Smart View state
  const [viewMode, setViewMode] = useState<"smart" | "all">("smart");
  const [smartData, setSmartData] = useState<Record<string, SmartDayData>>({});
  const [smartLoading, setSmartLoading] = useState(false);
  const [smartOverflowCount, setSmartOverflowCount] = useState(0);
  const SMART_MORE_BATCH = 10;

  // Build Polymarket date map
  const polyDateMap = new Map<string, EarningsEntry[]>();
  const undated: EarningsEntry[] = [];
  for (const m of markets) {
    const entry = buildEntry(m);
    if (m.endDate) {
      const d = new Date(m.endDate);
      if (!isNaN(d.getTime())) {
        const key = dateKey(d);
        if (!polyDateMap.has(key)) polyDateMap.set(key, []);
        polyDateMap.get(key)!.push(entry);
        continue;
      }
    }
    undated.push(entry);
  }

  // Fetch Finnhub calendar for current week
  useEffect(() => {
    const weekKey = dateKey(weekStart);
    if (finnhubFetchedWeeks.current.has(weekKey)) return;
    finnhubFetchedWeeks.current.add(weekKey);
    setFinnhubLoading(true);

    const sunday = weekStart;
    const saturday = addDays(sunday, 6);
    const fromStr = dateKey(sunday);
    const toStr = dateKey(saturday);

    fetchFinnhubCalendar(fromStr, toStr).then((earnings) => {
      const map = new Map<string, EarningsEntry[]>();
      for (const e of earnings) {
        if (!e.date || !e.ticker) continue;
        const key = e.date;
        if (!map.has(key)) map.set(key, []);
        map.get(key)!.push(buildFinnhubEntry(e));
      }
      setFinnhubEntries(prev => {
        const merged = new Map(prev);
        for (const [k, v] of map) {
          merged.set(k, v);
        }
        return merged;
      });
    }).catch(() => {}).finally(() => setFinnhubLoading(false));
  }, [weekStart]);

  // Fetch smart earnings data for the selected day
  const smartFetchedRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    if (viewMode !== "smart") return;
    if (selectedDayKey === "undated") return;
    if (smartFetchedRef.current.has(selectedDayKey)) return;
    smartFetchedRef.current.add(selectedDayKey);
    setSmartLoading(true);
    fetchSmartEarnings(selectedDayKey).then((data) => {
      if (data) {
        setSmartData(prev => ({ ...prev, [selectedDayKey]: data }));
      }
    }).finally(() => setSmartLoading(false));
  }, [selectedDayKey, viewMode]);

  // Merge: Polymarket entries take priority, Finnhub fills in the rest
  const dateMap = new Map<string, EarningsEntry[]>();
  // First, add all Polymarket entries
  for (const [key, entries] of polyDateMap) {
    dateMap.set(key, [...entries]);
  }
  // Then merge Finnhub entries (skip if ticker already present from Polymarket)
  for (const [key, fhEntries] of finnhubEntries) {
    const existing = dateMap.get(key) || [];
    const existingTickers = new Set(existing.map(e => e.ticker.toUpperCase()));
    for (const fhEntry of fhEntries) {
      if (existingTickers.has(fhEntry.ticker.toUpperCase())) {
        // Enrich existing Polymarket entry with Finnhub data
        const polyEntry = existing.find(e => e.ticker.toUpperCase() === fhEntry.ticker.toUpperCase());
        if (polyEntry) {
          if (!polyEntry.eps && fhEntry.eps) polyEntry.eps = fhEntry.eps;
          if (!polyEntry.quarter && fhEntry.quarter) polyEntry.quarter = fhEntry.quarter;
          if (!polyEntry.time && fhEntry.time) polyEntry.time = fhEntry.time;
          if (!polyEntry.revenueEstimate && fhEntry.revenueEstimate) polyEntry.revenueEstimate = fhEntry.revenueEstimate;
          polyEntry.source = "both";
        }
      } else {
        existing.push(fhEntry);
        existingTickers.add(fhEntry.ticker.toUpperCase());
      }
    }
    dateMap.set(key, existing);
  }
  // Sort: Polymarket entries first (by beatPct desc), then Finnhub entries alphabetically
  for (const entries of dateMap.values()) {
    entries.sort((a, b) => {
      if (a.beatPct >= 0 && b.beatPct < 0) return -1;
      if (a.beatPct < 0 && b.beatPct >= 0) return 1;
      if (a.beatPct >= 0 && b.beatPct >= 0) return b.beatPct - a.beatPct;
      return a.ticker.localeCompare(b.ticker);
    });
  }

  // ── On-demand enrichment: detail is fetched only when user clicks a ticker ──
  // No batch prefetch on day selection — prevents rate limit exhaustion and ensures
  // click-through always has budget available. Detail endpoint is cached 10min backend-side.
  const fetchTickerDetail = useCallback(async (ticker: string): Promise<EarningsDetailData | null> => {
    if (!ticker || ticker === "???") return null;
    // Return cached if already fetched
    if (enrichments[ticker]) return enrichments[ticker];
    setEnrichLoading(prev => new Set([...prev, ticker]));
    try {
      const res = await fetch(
        `${AGENT_BACKEND_URL}/api/earnings/detail?ticker=${encodeURIComponent(ticker)}`
      );
      if (res.ok) {
        const data = await res.json();
        setEnrichments(prev => ({ ...prev, [ticker]: data }));
        return data;
      }
    } catch (e) {
      console.warn(`[EARNINGS] detail fetch failed for ${ticker}:`, e);
    } finally {
      setEnrichLoading(prev => { const n = new Set(prev); n.delete(ticker); return n; });
    }
    return null;
  }, [enrichments]);

  const weekDays = Array.from({ length: 7 }, (_, i) => addDays(weekStart, i));
  const weekMonth = MONTH_NAMES[weekStart.getMonth()];
  const weekYear = weekStart.getFullYear();

  const prevWeek = () => {
    const newStart = addDays(weekStart, -7);
    setWeekStart(newStart);
    setSelectedDayKey(dateKey(newStart));
  };
  const nextWeek = () => {
    const newStart = addDays(weekStart, 7);
    setWeekStart(newStart);
    setSelectedDayKey(dateKey(newStart));
  };
  const goToday = () => {
    setWeekStart(getSunday(new Date()));
    setSelectedDayKey(dateKey(new Date()));
  };

  const selectedEntries = dateMap.get(selectedDayKey) || [];
  const selectedDate = weekDays.find((d) => dateKey(d) === selectedDayKey) || new Date();
  const totalThisWeek = weekDays.reduce((sum, d) => sum + (dateMap.get(dateKey(d))?.length || 0), 0);

  const showUndated = selectedDayKey === "undated";
  const displayEntries = showUndated ? undated : selectedEntries;

  // ── Lazy rendering: paginate in batches of 15 ──
  const BATCH_SIZE = 15;
  const [visibleCount, setVisibleCount] = useState(BATCH_SIZE);
  const loadMoreRef = useRef<HTMLDivElement>(null);

  // Reset visible count and smart show-more when day changes
  useEffect(() => {
    setVisibleCount(BATCH_SIZE);
    setSmartOverflowCount(0);
  }, [selectedDayKey]);

  // IntersectionObserver to load more entries as user scrolls
  useEffect(() => {
    const el = loadMoreRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && visibleCount < displayEntries.length) {
          setVisibleCount(prev => Math.min(prev + BATCH_SIZE, displayEntries.length));
        }
      },
      { rootMargin: '200px' }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [visibleCount, displayEntries.length]);

  const visibleEntries = displayEntries.slice(0, visibleCount);

  // Click handler: fetch detail on demand then open modal
  const handleEntryClick = useCallback(async (entry: EarningsEntry) => {
    setModalEntry(entry);
    // Trigger on-demand fetch (non-blocking — modal will show loading state)
    fetchTickerDetail(entry.ticker);
  }, [fetchTickerDetail]);

  return (
    <div>
      {/* Calendar header */}
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-gradient-to-br from-yellow-500 to-orange-500 rounded-lg flex items-center justify-center flex-shrink-0">
            <CalendarDays className="w-5 h-5 text-white" />
          </div>
          <div>
            <h3 className="text-base font-bold text-white">
              Earnings Calendar
            </h3>
            <p className="text-[10px] text-white/30 mt-0.5 leading-tight">
              Complete earnings calendar with Polymarket predictions &amp; Finnhub fundamentals
            </p>
            <p className="text-[10px] text-white/20 mt-0.5">
              {weekMonth} {weekYear} &middot; {totalThisWeek} earnings call{totalThisWeek !== 1 ? "s" : ""} this week
              {finnhubLoading && <span className="ml-1.5 text-blue-400/50"><Loader2 className="w-2.5 h-2.5 animate-spin inline" /></span>}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={goToday}
            className="px-3 py-1.5 rounded-lg text-[11px] font-semibold text-white/50 border border-white/[0.08] hover:bg-white/5 hover:text-white/70 transition-all mr-1"
          >
            Today
          </button>
          <button onClick={prevWeek} className="p-1.5 rounded-lg border border-white/[0.08] hover:bg-white/5 transition-all">
            <ChevronLeft className="w-4 h-4 text-white/50" />
          </button>
          <button onClick={nextWeek} className="p-1.5 rounded-lg border border-white/[0.08] hover:bg-white/5 transition-all">
            <ChevronRight className="w-4 h-4 text-white/50" />
          </button>
        </div>
      </div>

      {/* Smart/All toggle + cache status */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="flex rounded-lg border border-white/[0.08] overflow-hidden">
            <button
              onClick={() => setViewMode("smart")}
              className={`px-3 py-1 text-[10px] font-bold transition-all ${
                viewMode === "smart"
                  ? "bg-purple-500/20 text-purple-400 border-r border-white/[0.08]"
                  : "bg-transparent text-white/35 border-r border-white/[0.08] hover:text-white/50"
              }`}
            >
              <Sparkles className="w-3 h-3 inline mr-1 -mt-0.5" />Smart
            </button>
            <button
              onClick={() => setViewMode("all")}
              className={`px-3 py-1 text-[10px] font-bold transition-all ${
                viewMode === "all"
                  ? "bg-blue-500/20 text-blue-400"
                  : "bg-transparent text-white/35 hover:text-white/50"
              }`}
            >
              All
            </button>
          </div>
          {viewMode === "smart" && (() => {
            const sd = smartData[selectedDayKey];
            const cs = sd?.cache_status;
            if (smartLoading || sd?.scanning) {
              return (
                <span className="text-[9px] text-purple-400/60 flex items-center gap-1">
                  <Loader2 className="w-2.5 h-2.5 animate-spin" /> Scanning...
                </span>
              );
            }
            if (cs?.last_updated) {
              return (
                <span className="text-[9px] text-white/20">
                  Updated: {new Date(cs.last_updated).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                </span>
              );
            }
            return null;
          })()}
        </div>
        {viewMode === "smart" && (
          <span className="text-[9px] text-white/15">
            AI-curated via Grok + Perplexity
          </span>
        )}
      </div>

      {/* Week day selector row */}
      <div className="grid grid-cols-7 gap-1.5 mb-5">
        {weekDays.map((day, i) => {
          const key = dateKey(day);
          const entries = dateMap.get(key) || [];
          const isToday = dateKey(new Date()) === key;
          const isSelected = selectedDayKey === key;
          const callCount = entries.length;
          return (
            <button
              key={key}
              onClick={() => setSelectedDayKey(key)}
              className={`rounded-xl p-2.5 text-center transition-all border ${
                isSelected
                  ? "bg-blue-500/10 border-blue-500/30 ring-1 ring-blue-500/20"
                  : isToday
                  ? "bg-white/[0.03] border-blue-500/15 hover:bg-white/[0.05]"
                  : "bg-white/[0.015] border-white/[0.05] hover:bg-white/[0.04] hover:border-white/[0.08]"
              }`}
            >
              <p className={`text-[10px] font-semibold mb-0.5 ${isSelected ? "text-blue-400" : "text-white/40"}`}>
                {DAY_NAMES_FULL[i]}
              </p>
              <p className={`text-xs font-bold ${isSelected ? "text-white" : isToday ? "text-blue-400" : "text-white/70"}`}>
                {MONTH_NAMES_SHORT[day.getMonth()]} {day.getDate()}
              </p>
              <p className={`text-[9px] mt-1 ${callCount > 0 ? (isSelected ? "text-blue-400/70" : "text-white/40") : "text-white/20"}`}>
                {callCount > 0 ? `${callCount} Call${callCount > 1 ? "s" : ""}` : "No Calls"}
              </p>
              {callCount > 0 && (
                <div className="flex justify-center gap-0.5 mt-1.5">
                  {entries.slice(0, 4).map((e) => (
                    <div key={e.market?.marketId || `fh-${e.ticker}`} className={`w-4 h-4 rounded-sm bg-gradient-to-br ${tickerColor(e.ticker)} flex items-center justify-center`}>
                      <span className="text-[6px] font-bold text-white">{e.ticker.slice(0, 1)}</span>
                    </div>
                  ))}
                  {callCount > 4 && (
                    <div className="w-4 h-4 rounded-sm bg-white/[0.06] flex items-center justify-center">
                      <span className="text-[6px] font-bold text-white/40">+{callCount - 4}</span>
                    </div>
                  )}
                </div>
              )}
            </button>
          );
        })}
      </div>

      {/* Selected day header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <h4 className="text-sm font-bold text-white/90">
            {showUndated ? "Date TBD" : (
              <>
                {DAY_NAMES_FULL[selectedDate.getDay()]}, {MONTH_NAMES[selectedDate.getMonth()]} {selectedDate.getDate()}
              </>
            )}
          </h4>
          <span className="text-[10px] text-white/30">
            {displayEntries.length} earning{displayEntries.length !== 1 ? "s" : ""} call{displayEntries.length !== 1 ? "s" : ""}
          </span>
        </div>
        {undated.length > 0 && !showUndated && (
          <button
            onClick={() => setSelectedDayKey("undated")}
            className="text-[10px] text-white/30 hover:text-white/50 transition-colors"
          >
            {undated.length} with dates TBD
          </button>
        )}
      </div>

      {/* Smart View */}
      {viewMode === "smart" && (() => {
        const sd = smartData[selectedDayKey];
        const smartTickers = sd?.tickers || [];
        if (smartLoading && !sd) {
          return (
            <div className="text-center py-10 border border-white/[0.04] rounded-xl bg-white/[0.01]">
              <Loader2 className="w-6 h-6 text-purple-400/40 mx-auto mb-2 animate-spin" />
              <p className="text-sm text-white/25">Scanning earnings with AI...</p>
              <p className="text-[10px] text-white/15 mt-1">Analyzing social buzz and news signals</p>
            </div>
          );
        }
        if (smartTickers.length === 0) {
          return (
            <div className="text-center py-10 border border-white/[0.04] rounded-xl bg-white/[0.01]">
              <Calendar className="w-6 h-6 text-white/10 mx-auto mb-2" />
              <p className="text-sm text-white/25">No high-signal earnings for this day</p>
              <p className="text-[10px] text-white/15 mt-1">Switch to "All" view to see every ticker</p>
            </div>
          );
        }
        return (
          <div className="space-y-2">
            {smartTickers.map((st) => {
              const enrich = enrichments[st.ticker];
              const isEnrichLoading = enrichLoading.has(st.ticker);
              const epsStr = st.eps_estimate != null ? `$${st.eps_estimate.toFixed(2)}` : null;
              const revStr = st.revenue_estimate != null
                ? (st.revenue_estimate >= 1e9 ? `$${(st.revenue_estimate / 1e9).toFixed(1)}B` : st.revenue_estimate >= 1e6 ? `$${(st.revenue_estimate / 1e6).toFixed(0)}M` : `$${st.revenue_estimate.toLocaleString()}`)
                : null;
              const qtr = st.quarter && st.year ? `Q${st.quarter} ${st.year}` : st.quarter ? `Q${st.quarter}` : null;
              const timeStr = st.hour === "bmo" ? "Pre-Market" : st.hour === "amc" ? "After Hours" : null;
              const sentimentEmoji = st.sentiment === "bullish" ? "\uD83D\uDFE2" : st.sentiment === "bearish" ? "\uD83D\uDD34" : "\uD83D\uDFE1";
              const buzzIcon = st.buzz_level >= 7 ? "\uD83D\uDD25" : st.buzz_level >= 4 ? "\u3030\uFE0F" : "";
              // Look up Polymarket beat probability from the already-fetched full day list
              const polyEntry = displayEntries.find(e => e.ticker.toUpperCase() === st.ticker.toUpperCase());
              const polyBeatPct = polyEntry && polyEntry.beatPct > 0 ? polyEntry.beatPct : null;
              if (polyEntry) {
                console.log(`[SMART_PM] ${st.ticker}: found in displayEntries, beatPct=${polyEntry.beatPct}, source=${polyEntry.source}, showing badge=${polyBeatPct != null}`);
              }

              return (
                <div
                  key={`smart-${st.ticker}`}
                  className="rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all group cursor-pointer"
                  onClick={() => {
                    // Build a minimal EarningsEntry for the modal
                    const entry: EarningsEntry = polyEntry || {
                      market: null,
                      ticker: st.ticker,
                      company: enrich?.company_name || st.ticker,
                      eps: epsStr,
                      quarter: qtr,
                      time: timeStr,
                      exchange: null,
                      beatPct: -1,
                      revenueEstimate: revStr,
                      source: "finnhub",
                      earningsDate: st.date,
                    };
                    setModalEntry(entry);
                    fetchTickerDetail(st.ticker);
                  }}
                >
                  <div className="flex items-start gap-4 p-4">
                    {enrich?.logo ? (
                      <img src={enrich.logo} alt={st.ticker} className="w-10 h-10 rounded-xl object-contain bg-white/5 p-1 flex-shrink-0 mt-0.5" />
                    ) : (
                      <div className={`w-10 h-10 rounded-xl bg-gradient-to-br ${tickerColor(st.ticker)} flex items-center justify-center flex-shrink-0 mt-0.5`}>
                        <span className="text-xs font-bold text-white">{st.ticker.slice(0, 2)}</span>
                      </div>
                    )}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <p className="text-sm font-bold text-white group-hover:text-blue-400 transition-colors">
                            {enrich?.company_name || st.ticker}
                          </p>
                          <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                            <span className="text-[11px] font-mono text-white/40">{st.ticker}</span>
                            {qtr && <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-blue-500/10 text-blue-400/70">{qtr}</span>}
                            {timeStr && (
                              <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-white/[0.04] text-white/30 flex items-center gap-0.5">
                                <Clock className="w-2.5 h-2.5" /> {timeStr}
                              </span>
                            )}
                            {enrich?.sector && <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-purple-500/10 text-purple-400/60">{enrich.sector}</span>}
                          </div>
                          {/* AI one-line thesis */}
                          {st.one_line && (
                            <p className="text-[10px] text-white/40 leading-relaxed mt-1.5 line-clamp-2">
                              <Sparkles className="w-3 h-3 text-purple-400/50 inline mr-1 -mt-0.5" />
                              {st.one_line}
                            </p>
                          )}
                        </div>
                        {/* Right side: Market Cap + Sentiment + Buzz */}
                        <div className="flex-shrink-0 flex items-center gap-2">
                          <span className="text-[10px] font-semibold text-white/35 font-mono min-w-[48px] text-right">
                            {enrich?.market_cap ? formatMktCap(enrich.market_cap) : ""}
                          </span>
                          {/* Sentiment badge */}
                          <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-lg text-[10px] font-bold ${
                            st.sentiment === "bullish" ? "bg-emerald-500/10 border border-emerald-500/20 text-emerald-400" :
                            st.sentiment === "bearish" ? "bg-red-500/10 border border-red-500/20 text-red-400" :
                            "bg-yellow-500/10 border border-yellow-500/20 text-yellow-400"
                          }`}>
                            <span>{sentimentEmoji}</span>
                            <span className="capitalize">{st.sentiment}</span>
                          </div>
                          {/* Polymarket beat probability badge */}
                          {polyBeatPct != null && (
                            <div className={`inline-flex items-center gap-1 px-2 py-1 rounded-lg text-[10px] font-bold ${
                              polyBeatPct >= 60 ? "bg-emerald-500/10 border border-emerald-500/20 text-emerald-400" :
                              polyBeatPct <= 40 ? "bg-red-500/10 border border-red-500/20 text-red-400" :
                              "bg-yellow-500/10 border border-yellow-500/20 text-yellow-400"
                            }`}>
                              <span className="text-white/50">PM:</span> {polyBeatPct}% beat
                            </div>
                          )}
                        </div>
                      </div>

                      {/* Signal indicators row */}
                      <div className="flex items-center gap-2 mt-2 flex-wrap">
                        {/* Buzz level */}
                        {st.buzz_level > 0 && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-orange-500/8 border border-orange-500/15 text-orange-400/80 font-semibold flex items-center gap-1">
                            {buzzIcon} Buzz: {st.buzz_level}/10
                          </span>
                        )}
                        {/* News signal */}
                        {st.news_signal === "high" && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-purple-500/8 border border-purple-500/15 text-purple-400/80 font-semibold">
                            High News Signal
                          </span>
                        )}
                        {/* Analyst focus */}
                        {st.analyst_focus && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-blue-500/8 border border-blue-500/15 text-blue-400/80 font-semibold">
                            Analyst Focus
                          </span>
                        )}
                        {enrich?.beat_rate && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-emerald-500/8 border border-emerald-500/15 text-emerald-400/80 font-semibold">
                            Beat Record: {enrich.beat_rate}
                          </span>
                        )}
                      </div>
                      {isEnrichLoading && (
                        <div className="flex items-center gap-1.5 mt-2">
                          <Loader2 className="w-3 h-3 text-blue-400/50 animate-spin" />
                          <span className="text-[9px] text-white/25">Loading context...</span>
                        </div>
                      )}

                      {/* Bottom row: EPS, Rev, Score */}
                      <div className="flex items-center gap-4 mt-2.5">
                        {epsStr && (
                          <span className="text-[10px] text-white/30">
                            <span className="text-white/50 font-semibold">EPS Est:</span> {epsStr}
                          </span>
                        )}
                        {revStr && (
                          <span className="text-[10px] text-white/30">
                            <span className="text-white/50 font-semibold">Rev Est:</span> {revStr}
                          </span>
                        )}
                        <span className="text-[10px] text-blue-400/60 group-hover:text-blue-400 transition-colors ml-auto">
                          View full details
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
            {/* Show more earnings — non-curated tickers from the full day list */}
            {(() => {
              const smartTickerSet = new Set(smartTickers.map(s => s.ticker.toUpperCase()));
              const overflowEntries = displayEntries.filter(e => !smartTickerSet.has(e.ticker.toUpperCase()));
              if (overflowEntries.length === 0) return null;
              const visibleOverflow = overflowEntries.slice(0, smartOverflowCount);
              const hasMore = smartOverflowCount < overflowEntries.length;
              return (
                <>
                  {visibleOverflow.map((e) => (
                    <div
                      key={`overflow-${e.market?.marketId || `fh-${e.ticker}`}`}
                      className="rounded-xl border border-white/[0.04] bg-white/[0.01] hover:bg-white/[0.02] hover:border-white/[0.06] transition-all group cursor-pointer opacity-60"
                      onClick={() => handleEntryClick(e)}
                    >
                      <div className="flex items-center gap-3 px-4 py-3">
                        <div className={`w-8 h-8 rounded-lg bg-gradient-to-br ${tickerColor(e.ticker)} flex items-center justify-center flex-shrink-0`}>
                          <span className="text-[9px] font-bold text-white">{e.ticker.slice(0, 2)}</span>
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-bold text-white/70 group-hover:text-blue-400/70 transition-colors">{e.company}</span>
                            <span className="text-[10px] font-mono text-white/30">{e.ticker}</span>
                            {e.quarter && <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-blue-500/10 text-blue-400/50">{e.quarter}</span>}
                            {e.time && <span className="text-[9px] text-white/20">{e.time}</span>}
                          </div>
                        </div>
                        <div className="flex items-center gap-2 flex-shrink-0">
                          {e.eps && <span className="text-[10px] text-white/25">EPS: {e.eps}</span>}
                          {e.beatPct >= 0 && (
                            <span className={`text-[9px] px-1.5 py-0.5 rounded-md font-semibold ${
                              e.beatPct >= 60 ? "bg-emerald-500/8 text-emerald-400/60" :
                              e.beatPct <= 40 ? "bg-red-500/8 text-red-400/60" :
                              "bg-yellow-500/8 text-yellow-400/60"
                            }`}>
                              PM: {e.beatPct}%
                            </span>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                  {smartOverflowCount > 0 && (
                    <button
                      onClick={() => setSmartOverflowCount(0)}
                      className="w-full py-2.5 rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all text-[11px] font-semibold text-white/40 hover:text-white/60"
                    >
                      Show less
                    </button>
                  )}
                  {hasMore && (
                    <button
                      onClick={() => setSmartOverflowCount(prev => prev + SMART_MORE_BATCH)}
                      className="w-full py-2.5 rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all text-[11px] font-semibold text-white/40 hover:text-white/60"
                    >
                      Show more earnings ({overflowEntries.length - smartOverflowCount} more)
                    </button>
                  )}
                </>
              );
            })()}
          </div>
        );
      })()}

      {/* All View — Company list for selected day (existing logic, unchanged) */}
      {viewMode === "all" && displayEntries.length === 0 ? (
        <div className="text-center py-10 border border-white/[0.04] rounded-xl bg-white/[0.01]">
          <Calendar className="w-6 h-6 text-white/10 mx-auto mb-2" />
          <p className="text-sm text-white/25">No earnings calls scheduled</p>
          <p className="text-[10px] text-white/15 mt-1">Try another day or navigate to a different week</p>
        </div>
      ) : viewMode === "all" && (
        <div className="space-y-2">
          {visibleEntries.map((e) => {
            const isHigh = e.beatPct >= 60;
            const isLow = e.beatPct <= 40;
            const enrich = enrichments[e.ticker];
            const isEnrichLoading = enrichLoading.has(e.ticker);
            const consensus = enrich?.analyst_consensus;
            const articles = enrich?.news_articles || [];
            const topArticle = articles[0];
            return (
              <div
                key={e.market?.marketId || `fh-${e.ticker}`}
                className="rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all group cursor-pointer"
                onClick={() => handleEntryClick(e)}
              >
                <div className="flex items-start gap-4 p-4">
                  {enrich?.logo ? (
                    <img src={enrich.logo} alt={e.ticker} className="w-10 h-10 rounded-xl object-contain bg-white/5 p-1 flex-shrink-0 mt-0.5" />
                  ) : (
                    <div className={`w-10 h-10 rounded-xl bg-gradient-to-br ${tickerColor(e.ticker)} flex items-center justify-center flex-shrink-0 mt-0.5`}>
                      <span className="text-xs font-bold text-white">{e.ticker.slice(0, 2)}</span>
                    </div>
                  )}

                  <div className="flex-1 min-w-0">
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="text-sm font-bold text-white group-hover:text-blue-400 transition-colors">
                          {enrich?.company_name || e.company}
                        </p>
                        <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                          <span className="text-[11px] font-mono text-white/40">{e.ticker}</span>
                          {e.quarter && (
                            <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-blue-500/10 text-blue-400/70">{e.quarter}</span>
                          )}
                          {e.time && (
                            <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-white/[0.04] text-white/30 flex items-center gap-0.5">
                              <Clock className="w-2.5 h-2.5" /> {e.time}
                            </span>
                          )}
                          {enrich?.sector && (
                            <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-purple-500/10 text-purple-400/60">{enrich.sector}</span>
                          )}
                          {enrich?.current_price && (
                            <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-white/[0.04] text-white/40 font-semibold">
                              ${enrich.current_price.toFixed(2)}
                              {enrich.price_change_pct != null && (
                                <span className={enrich.price_change_pct >= 0 ? "text-emerald-400 ml-1" : "text-red-400 ml-1"}>
                                  {enrich.price_change_pct >= 0 ? "+" : ""}{enrich.price_change_pct.toFixed(1)}%
                                </span>
                              )}
                            </span>
                          )}
                        </div>
                      </div>

                      {/* Right side: Market Cap + Beat % */}
                      <div className="flex-shrink-0 flex items-center gap-2">
                        {/* Market Cap — always visible when enrichment loaded, "—" otherwise */}
                        <span className="text-[10px] font-semibold text-white/35 font-mono min-w-[48px] text-right">
                          {enrich ? (enrich.market_cap ? formatMktCap(enrich.market_cap) : "\u2014") : ""}
                        </span>
                        {e.beatPct >= 0 ? (
                        <div className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg ${
                          isHigh ? "bg-emerald-500/10 border border-emerald-500/20" : isLow ? "bg-red-500/10 border border-red-500/20" : "bg-yellow-500/10 border border-yellow-500/20"
                        }`}>
                          <span className={`w-1.5 h-1.5 rounded-full ${
                            isHigh ? "bg-emerald-400" : isLow ? "bg-red-400" : "bg-yellow-400"
                          }`} />
                          <span className={`text-xs font-bold ${
                            isHigh ? "text-emerald-400" : isLow ? "text-red-400" : "text-yellow-400"
                          }`}>
                            {e.beatPct}%
                          </span>
                          <span className={`text-[9px] ${
                            isHigh ? "text-emerald-400/60" : isLow ? "text-red-400/60" : "text-yellow-400/60"
                          }`}>beat</span>
                        </div>
                        ) : (
                        <div className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg bg-white/[0.04] border border-white/[0.08]">
                          <span className="text-[9px] text-white/30 font-semibold">Finnhub</span>
                        </div>
                        )}
                      </div>
                    </div>

                    {enrich && (
                      <div className="flex items-center gap-2 mt-2 flex-wrap">
                        {enrich.beat_rate && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-emerald-500/8 border border-emerald-500/15 text-emerald-400/80 font-semibold">
                            Beat Record: {enrich.beat_rate}
                          </span>
                        )}
                        {consensus && consensus.total > 0 && (
                          <span className={`text-[9px] px-1.5 py-0.5 rounded font-semibold ${
                            consensus.rating === "Buy" ? "bg-emerald-500/8 border border-emerald-500/15 text-emerald-400/80" :
                            consensus.rating === "Sell" ? "bg-red-500/8 border border-red-500/15 text-red-400/80" :
                            "bg-yellow-500/8 border border-yellow-500/15 text-yellow-400/80"
                          }`}>
                            Analysts: {consensus.rating} ({consensus.buy}B/{consensus.hold}H/{consensus.sell}S)
                          </span>
                        )}
                        {enrich.news_sentiment && enrich.news_sentiment !== "Neutral" && (
                          <span className={`text-[9px] px-1.5 py-0.5 rounded font-semibold ${
                            enrich.news_sentiment === "Bullish" ? "bg-emerald-500/8 border border-emerald-500/15 text-emerald-400/80" :
                            "bg-red-500/8 border border-red-500/15 text-red-400/80"
                          }`}>
                            {enrich.news_sentiment}
                          </span>
                        )}
                      </div>
                    )}
                    {isEnrichLoading && (
                      <div className="flex items-center gap-1.5 mt-2">
                        <Loader2 className="w-3 h-3 text-blue-400/50 animate-spin" />
                        <span className="text-[9px] text-white/25">Loading context...</span>
                      </div>
                    )}

                    {enrich?.news_summary && (
                      <p className="text-[10px] text-white/45 leading-relaxed mt-2 line-clamp-2">
                        <Sparkles className="w-3 h-3 text-blue-400/50 inline mr-1 -mt-0.5" />
                        {enrich.news_summary}
                      </p>
                    )}

                    {topArticle && (
                      <div className="mt-2 flex items-start gap-1.5">
                        <ExternalLink className="w-3 h-3 text-blue-400/40 flex-shrink-0 mt-0.5" />
                        <span className="text-[10px] text-blue-400/60 leading-snug line-clamp-1">{topArticle.title}</span>
                        {topArticle.source && <span className="text-[8px] text-white/20 flex-shrink-0 mt-0.5">{topArticle.source}</span>}
                      </div>
                    )}

                    <div className="flex items-center gap-4 mt-2.5">
                      {e.eps && (
                        <span className="text-[10px] text-white/30">
                          <span className="text-white/50 font-semibold">EPS Est:</span> {e.eps}
                        </span>
                      )}
                      {e.revenueEstimate && (
                        <span className="text-[10px] text-white/30">
                          <span className="text-white/50 font-semibold">Rev Est:</span> {e.revenueEstimate}
                        </span>
                      )}
                      {e.market && (
                      <span className="text-[10px] text-white/30">
                        <span className="text-white/50 font-semibold">Vol:</span> {formatVolume(e.market.totalVolume)}
                      </span>
                      )}
                      <span className="text-[10px] text-blue-400/60 group-hover:text-blue-400 transition-colors ml-auto">
                        View full details
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
          {/* Lazy load sentinel — triggers next batch when scrolled into view */}
          {visibleCount < displayEntries.length && (
            <div ref={loadMoreRef} className="flex items-center justify-center py-4">
              <Loader2 className="w-4 h-4 text-white/20 animate-spin mr-2" />
              <span className="text-[10px] text-white/20">Loading more ({visibleCount} of {displayEntries.length})...</span>
            </div>
          )}
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 flex items-center justify-between">
        <span className="text-[10px] text-white/20">
          Finnhub earnings calendar &middot; Polymarket predictions
        </span>
      </div>

      {modalEntry && <EarningsModal entry={modalEntry} onClose={() => setModalEntry(null)} prefetchedDetail={enrichments[modalEntry.ticker] || null} />}
    </div>
  );
}

// ─── Earnings Agent Chatbar ───────────────────────────────────────

interface EarningsAgentMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
}

const EARNINGS_SUGGESTED_PROMPTS = [
  "Which upcoming earnings have the highest beat probability?",
  "What are the best earnings plays this week based on sentiment and technicals?",
  "Which earnings could cause the biggest surprise moves?",
  "Analyze the highest-volume earnings bets on Polymarket right now",
];

function EarningsAgent() {
  const [messages, setMessages] = useState<EarningsAgentMessage[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg: EarningsAgentMessage = { role: "user", content: text.trim(), timestamp: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const payload: Record<string, unknown> = {
        query: text.trim(),
        preset_intent: "earnings_catalyst",
        history: history.length > 0 ? history : undefined,
        conversation_id: conversationId,
      };

      const res = await fetch(`${AGENT_BACKEND_URL}/api/query`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-API-Key": AGENT_API_KEY,
        },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => "");
        throw new Error(`Backend returned ${res.status}: ${errText.slice(0, 200)}`);
      }

      const rawText = (await res.text()).trim();
      const data = JSON.parse(rawText);
      const convId = data.conversation_id || conversationId;
      if (convId) setConversationId(convId);

      let analysisText = "";
      if (data.analysis) {
        analysisText = data.analysis;
      } else if (data.structured?.message) {
        analysisText = data.structured.message;
      } else if (data.structured?.analysis) {
        analysisText = data.structured.analysis;
      } else if (typeof data.message === "string") {
        analysisText = data.message;
      } else {
        analysisText = "Received response but couldn't extract analysis. Raw: " + JSON.stringify(data).slice(0, 500);
      }

      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: analysisText, timestamp: Date.now() },
      ]);
    } catch (err) {
      console.error("[EARNINGS_AGENT]", err);
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: `Error: ${err instanceof Error ? err.message : "Failed to reach agent. Please try again."}`,
          timestamp: Date.now(),
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [loading, messages, conversationId]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  return (
    <GlassCard className="p-5 mb-6">
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #f59e0b 0%, #f97316 50%, #ef4444 100%)' }}>
          <Sparkles className="w-4 h-4 text-white" />
        </div>
        <div>
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            Ask Caelyn
          </h2>
          <p className="text-[10px] text-white/30">
            Ask about upcoming earnings, beat odds, sentiment, and trading setups
          </p>
        </div>
      </div>

      {/* Suggested prompts (only show when no messages) */}
      {messages.length === 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-4">
          {EARNINGS_SUGGESTED_PROMPTS.map((prompt) => (
            <button
              key={prompt}
              onClick={() => sendMessage(prompt)}
              disabled={loading}
              className="text-left text-[11px] text-white/50 bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2.5 hover:bg-white/[0.06] hover:text-white/70 hover:border-white/10 transition-all disabled:opacity-40"
            >
              <MessageSquare className="w-3 h-3 inline mr-1.5 opacity-40" />
              {prompt}
            </button>
          ))}
        </div>
      )}

      {/* Messages */}
      {messages.length > 0 && (
        <div className="mb-4 max-h-[500px] overflow-y-auto space-y-3 scrollbar-hide">
          {messages.map((msg, i) => (
            <div
              key={i}
              className={`rounded-lg px-4 py-3 text-xs leading-relaxed ${
                msg.role === "user"
                  ? "bg-orange-500/10 border border-orange-500/20 text-orange-100"
                  : "bg-white/[0.03] border border-white/[0.06] text-white/80"
              }`}
            >
              <div className="flex items-center gap-1.5 mb-1.5">
                <span className={`text-[9px] font-bold uppercase tracking-wider ${
                  msg.role === "user" ? "text-orange-400" : "text-yellow-400"
                }`}>
                  {msg.role === "user" ? "You" : "Agent"}
                </span>
              </div>
              <div className="whitespace-pre-wrap">{msg.content}</div>
            </div>
          ))}
          {loading && (
            <div className="rounded-lg px-4 py-3 bg-white/[0.03] border border-white/[0.06] text-xs text-white/40">
              <div className="flex items-center gap-2">
                <Loader2 className="w-3 h-3 animate-spin" />
                Analyzing earnings data with sentiment and technicals...
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      )}

      {/* Input */}
      <form onSubmit={handleSubmit} className="flex gap-2">
        <textarea
          ref={inputRef}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask about earnings... (e.g., &quot;Which stocks have the best beat odds this week?&quot;)"
          disabled={loading}
          rows={1}
          className="flex-1 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2.5 text-xs text-white placeholder-white/25 resize-none focus:outline-none focus:border-white/20 focus:ring-1 focus:ring-white/10 disabled:opacity-40 transition-all"
        />
        <Button
          type="submit"
          disabled={loading || !input.trim()}
          className="text-white px-3 py-2 rounded-lg transition-all disabled:opacity-30 flex-shrink-0" style={{ background: 'linear-gradient(135deg, #f59e0b, #f97316, #ef4444)' }}
        >
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
        </Button>
      </form>

      {/* Clear conversation */}
      {messages.length > 0 && (
        <button
          onClick={() => { setMessages([]); setConversationId(null); }}
          className="mt-2 text-[9px] text-white/20 hover:text-white/40 transition-colors"
        >
          Clear conversation
        </button>
      )}
    </GlassCard>
  );
}

// ─── Main Page ────────────────────────────────────────────────────

export default function StocksEarningsCalendarPage() {
  const [earningsMarkets, setEarningsMarkets] = useState<ParsedMarket[]>([]);
  const [earningsLoading, setEarningsLoading] = useState(true);

  const fetchEarnings = useCallback(async () => {
    setEarningsLoading(true);
    try {
      const data = await fetchPolymarketByTag("earnings");
      if (data && data.length > 0) {
        setEarningsMarkets(parseTagEvents(data));
      }
    } catch { /* silent */ }
    finally {
      setEarningsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchEarnings();
    const iv = setInterval(fetchEarnings, REFRESH_INTERVAL);
    return () => clearInterval(iv);
  }, [fetchEarnings]);

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <EarningsAgent />
        <GlassCard className="p-5">
          {earningsLoading && earningsMarkets.length === 0 ? (
            <div>
              <div className="flex items-center gap-3 mb-1">
                <div className="w-9 h-9 bg-gradient-to-br from-yellow-500 to-orange-500 rounded-lg flex items-center justify-center flex-shrink-0">
                  <CalendarDays className="w-5 h-5 text-white" />
                </div>
                <div>
                  <h3 className="text-base font-bold text-white">
                    Earnings Calendar
                    <span className="text-white/30 font-normal text-xs ml-2">/ Loading...</span>
                  </h3>
                  <p className="text-[10px] text-white/30 leading-tight">Complete earnings calendar with Polymarket predictions &amp; Finnhub fundamentals</p>
                </div>
              </div>
              <div className="grid grid-cols-5 gap-2">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-[200px] rounded-xl" />
                ))}
              </div>
            </div>
          ) : (
            <EarningsCalendarWidget markets={earningsMarkets} />
          )}
        </GlassCard>
      </main>
    </div>
  );
}
