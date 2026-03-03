import { useState, useEffect, useCallback, useRef } from "react";
import { GlassCard } from "@/components/ui/glass-card";
import { ExternalLink, Loader2, Sparkles, Calendar, ChevronLeft, ChevronRight, CalendarDays, X, Clock } from "lucide-react";

// ─── Constants ────────────────────────────────────────────────────
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
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

interface EarningsEntry {
  market: ParsedMarket;
  ticker: string;
  company: string;
  eps: string | null;
  quarter: string | null;
  time: string | null;
  exchange: string | null;
  beatPct: number;
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
  return {
    market: m,
    ticker: extractTicker(m.question) || "???",
    company: extractCompanyName(m.question),
    eps: extractEPS(m.description),
    quarter: extractQuarter(combined),
    time: extractTime(m.description),
    exchange: extractExchange(m.description),
    beatPct: Math.round(m.yesPrice * 100),
  };
}

function buildBullets(entry: EarningsEntry): string[] {
  const bullets: string[] = [];
  const desc = entry.market.description || "";

  if (entry.eps) {
    const q = entry.quarter || "quarterly";
    bullets.push(`Wall Street consensus EPS estimate of ${entry.eps} for ${q} earnings`);
  }

  const beatLabel = entry.beatPct >= 70 ? "strongly favored to beat" : entry.beatPct >= 55 ? "favored to beat" : entry.beatPct <= 30 ? "expected to miss" : entry.beatPct <= 45 ? "at risk of missing" : "near a coin flip on beating";
  bullets.push(`Polymarket crowd: ${beatLabel} estimates (${entry.beatPct}% chance of beat)`);

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

  if (bullets.length < 2) {
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
        <div className="px-6 py-3 border-b border-white/[0.06]">
          <div className="flex items-center gap-4 text-[10px] text-white/30">
            <span><span className="text-white/50 font-semibold">24h Vol:</span> {formatVolume(entry.market.volume24hr)}</span>
            <span><span className="text-white/50 font-semibold">Total Vol:</span> {formatVolume(entry.market.totalVolume)}</span>
            <span><span className="text-white/50 font-semibold">Liquidity:</span> {formatVolume(entry.market.liquidity)}</span>
          </div>
        </div>

        {/* Footer */}
        <div className="px-6 py-4 flex items-center justify-between">
          <span className="text-[9px] text-white/20">Polymarket + Finnhub</span>
          <a
            href={`https://polymarket.com/event/${entry.market.eventSlug}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1.5 text-[11px] font-semibold text-blue-400 hover:text-blue-300 transition-colors"
          >
            Trade on Polymarket <ExternalLink className="w-3 h-3" />
          </a>
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

  const dateMap = new Map<string, EarningsEntry[]>();
  const undated: EarningsEntry[] = [];
  for (const m of markets) {
    const entry = buildEntry(m);
    if (m.endDate) {
      const d = new Date(m.endDate);
      if (!isNaN(d.getTime())) {
        const key = dateKey(d);
        if (!dateMap.has(key)) dateMap.set(key, []);
        dateMap.get(key)!.push(entry);
        continue;
      }
    }
    undated.push(entry);
  }
  for (const entries of dateMap.values()) {
    entries.sort((a, b) => b.beatPct - a.beatPct);
  }

  const fetchedDaysRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    if (fetchedDaysRef.current.has(selectedDayKey)) return;
    const dateEntries = selectedDayKey === "undated" ? undated : (dateMap.get(selectedDayKey) || []);
    const tickers = dateEntries.map(e => e.ticker).filter(t => t && t !== "???");
    if (tickers.length === 0) return;
    fetchedDaysRef.current.add(selectedDayKey);
    setEnrichLoading(prev => new Set([...prev, ...tickers]));
    tickers.slice(0, 10).forEach(async (ticker) => {
      try {
        const res = await fetch(
          `${AGENT_BACKEND_URL}/api/earnings/detail?ticker=${encodeURIComponent(ticker)}`
        );
        if (res.ok) {
          const data = await res.json();
          setEnrichments(prev => ({ ...prev, [ticker]: data }));
        }
      } catch { /* silent */ }
      finally {
        setEnrichLoading(prev => { const n = new Set(prev); n.delete(ticker); return n; });
      }
    });
  }, [selectedDayKey]);

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

  return (
    <div>
      {/* Calendar header */}
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <CalendarDays className="w-5 h-5 text-yellow-400" />
          <div>
            <h3 className="text-base font-bold text-white">
              Earnings Calendar
            </h3>
            <p className="text-[10px] text-white/30 mt-0.5">
              {weekMonth} {weekYear} &middot; {totalThisWeek} earnings call{totalThisWeek !== 1 ? "s" : ""} this week
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
                    <div key={e.market.marketId} className={`w-4 h-4 rounded-sm bg-gradient-to-br ${tickerColor(e.ticker)} flex items-center justify-center`}>
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

      {/* Company list for selected day */}
      {displayEntries.length === 0 ? (
        <div className="text-center py-10 border border-white/[0.04] rounded-xl bg-white/[0.01]">
          <Calendar className="w-6 h-6 text-white/10 mx-auto mb-2" />
          <p className="text-sm text-white/25">No earnings calls scheduled</p>
          <p className="text-[10px] text-white/15 mt-1">Try another day or navigate to a different week</p>
        </div>
      ) : (
        <div className="space-y-2">
          {displayEntries.map((e) => {
            const isHigh = e.beatPct >= 60;
            const isLow = e.beatPct <= 40;
            const enrich = enrichments[e.ticker];
            const isEnrichLoading = enrichLoading.has(e.ticker);
            const consensus = enrich?.analyst_consensus;
            const articles = enrich?.news_articles || [];
            const topArticle = articles[0];
            return (
              <div
                key={e.market.marketId}
                className="rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all group cursor-pointer"
                onClick={() => setModalEntry(e)}
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

                      <div className="flex-shrink-0 text-right">
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
                        {enrich.market_cap && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded bg-white/[0.04] border border-white/[0.06] text-white/40 font-semibold">
                            {formatMktCap(enrich.market_cap)}
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
                      <span className="text-[10px] text-white/30">
                        <span className="text-white/50 font-semibold">Vol:</span> {formatVolume(e.market.totalVolume)}
                      </span>
                      <span className="text-[10px] text-blue-400/60 group-hover:text-blue-400 transition-colors ml-auto">
                        View full details
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Footer */}
      <div className="mt-4 flex items-center justify-between">
        <span className="text-[10px] text-white/20">
          Polymarket odds &middot; Finnhub fundamentals &amp; news
        </span>
        <a
          href="https://polymarket.com/earnings"
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 text-[10px] text-blue-400/60 hover:text-blue-400 transition-colors"
        >
          View on Polymarket <ExternalLink className="w-2.5 h-2.5" />
        </a>
      </div>

      {modalEntry && <EarningsModal entry={modalEntry} onClose={() => setModalEntry(null)} prefetchedDetail={enrichments[modalEntry.ticker] || null} />}
    </div>
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
    <div className="min-h-screen text-white" style={{background: 'linear-gradient(135deg, hsl(0, 0%, 0%) 0%, hsl(0, 0%, 10%) 50%, hsl(0, 0%, 0%) 100%)'}}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center relative mb-8">
          <div className="absolute inset-0 bg-gradient-to-r from-yellow-500/20 via-orange-500/20 to-red-500/20 blur-3xl -z-10"></div>
          <div className="flex justify-center items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-gradient-to-br from-yellow-500 to-orange-500 rounded-xl flex items-center justify-center">
              <CalendarDays className="w-6 h-6 text-white" />
            </div>
            <h2 className="text-2xl lg:text-3xl font-bold bg-gradient-to-r from-white via-yellow-200 to-orange-200 bg-clip-text text-transparent">Earnings Calendar</h2>
          </div>
          <p className="text-sm text-white/50">Polymarket-powered earnings predictions with Finnhub fundamentals &amp; company news</p>
          <div className="w-32 h-1 bg-gradient-to-r from-yellow-500 to-orange-500 mx-auto mt-4 rounded-full"></div>
        </div>
      </div>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pb-12">
        <GlassCard className="p-5">
          {earningsLoading && earningsMarkets.length === 0 ? (
            <div>
              <div className="flex items-center gap-3 mb-4">
                <CalendarDays className="w-5 h-5 text-yellow-400" />
                <h3 className="text-sm font-bold text-white/90">
                  Earnings Calendar
                  <span className="text-white/30 font-normal ml-2">/ Loading...</span>
                </h3>
              </div>
              <div className="grid grid-cols-5 gap-2">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Skeleton key={i} className="h-[200px] rounded-xl" />
                ))}
              </div>
            </div>
          ) : earningsMarkets.length === 0 ? (
            <div className="flex items-center gap-3">
              <CalendarDays className="w-5 h-5 text-yellow-400" />
              <h3 className="text-sm font-bold text-white/90">Earnings Calendar</h3>
              <span className="text-[10px] text-white/30 ml-2">No earnings markets found. Check back closer to earnings season.</span>
            </div>
          ) : (
            <EarningsCalendarWidget markets={earningsMarkets} />
          )}
        </GlassCard>
      </main>
    </div>
  );
}
