import { useState, useEffect, useCallback, useRef } from "react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { TrendingUp, ExternalLink, Activity, BarChart3, RefreshCw, Users, DollarSign, MessageSquare, Send, Loader2, Sparkles, Calendar, ChevronLeft, ChevronRight, CalendarDays, X, Clock } from "lucide-react";
import { openSecureLink } from "@/utils/security";
import diceImage from "@assets/istockphoto-1252690598-612x612_1756665072306.jpg";

// ─── Constants ────────────────────────────────────────────────────
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";
const POLYMARKET_PROXY = `${AGENT_BACKEND_URL}/api/polymarket/events`;
const GAMMA_API = "https://gamma-api.polymarket.com/events";
const REFRESH_INTERVAL = 60_000;

const MACRO_INCLUDE = [
  "fed", "rate", "rates", "inflation", "gdp", "recession", "bitcoin", "btc",
  "ethereum", "eth", "crypto", "stock", "s&p", "nasdaq", "tariff", "trade war",
  "election", "president", "congress", "treasury", "employment", "jobs", "cpi",
  "ppi", "oil", "gold", "commodities", "economy", "economic", "debt", "deficit",
  "housing", "interest", "monetary", "fiscal", "central bank", "dollar", "yen",
  "euro", "currency", "bond", "yield", "market", "dow", "default", "shutdown",
  "geopolitical", "war", "sanctions", "china", "iran", "russia", "ukraine",
  "opec", "regulation", "sec", "etf", "ipo", "ai ", "artificial intelligence",
  "semiconductor", "solana", "xrp", "dogecoin",
  // Finance
  "bank", "banking", "jpmorgan", "goldman", "morgan stanley", "credit", "loan",
  "mortgage", "fintech", "insurance", "hedge fund", "private equity", "venture capital",
  "ipo", "merger", "acquisition", "m&a", "bankruptcy", "bailout",
  // Tech
  "apple", "google", "meta", "microsoft", "amazon", "nvidia", "tesla", "openai",
  "chatgpt", "tech", "technology", "software", "hardware", "chip", "semiconductor",
  "cloud", "saas", "startup", "silicon valley", "antitrust",
  // Earnings
  "earnings", "revenue", "profit", "eps", "quarterly", "guidance", "forecast",
  "beat", "miss", "report", "q1", "q2", "q3", "q4", "annual",
];

const MACRO_EXCLUDE = [
  "nfl", "nba", "mlb", "nhl", "soccer", "football", "basketball", "baseball",
  "hockey", "tennis", "golf", "cricket", "ufc", "mma", "boxing", "f1",
  "formula 1", "oscar", "grammy", "emmy", "tony", "bachelor", "bachelorette",
  "love island", "survivor", "big brother", "celebrity", "kardashian", "swift",
  "drake", "beyonce", "tiktok", "youtube", "twitch", "streamer", "influencer",
  "weather", "hurricane", "tornado", "earthquake", "super bowl", "world series",
  "stanley cup", "premier league", "champions league", "fifa", "olympics",
  "paralympics",
];

type CategoryTab = "all" | "crypto" | "fed" | "elections" | "economy" | "geopolitics" | "finance" | "tech";

// Categories that use Polymarket tag_slug API for direct fetching
const TAG_SLUG_CATEGORIES: Partial<Record<CategoryTab, string>> = {
  finance: "finance",
  tech: "tech",
};

const CATEGORY_KEYWORDS: Record<Exclude<CategoryTab, "all" | "finance" | "tech">, string[]> = {
  crypto: ["bitcoin", "btc", "ethereum", "eth", "crypto", "solana", "xrp", "dogecoin", "defi", "nft", "blockchain"],
  fed: ["fed", "rate", "rates", "interest", "monetary", "central bank", "fomc", "powell", "inflation", "cpi", "ppi", "yield", "bond"],
  elections: ["election", "president", "congress", "senate", "house", "vote", "governor", "democrat", "republican", "trump", "biden"],
  economy: ["gdp", "recession", "economy", "economic", "jobs", "employment", "housing", "debt", "deficit", "fiscal", "stock", "s&p", "nasdaq", "dow", "market", "tariff", "trade war", "commodity", "commodities", "oil", "gold"],
  geopolitics: ["war", "ukraine", "russia", "china", "iran", "sanctions", "geopolitical", "nato", "opec", "nuclear", "taiwan", "middle east"],
};

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

// ─── Helpers ──────────────────────────────────────────────────────
function parsePriceArray(raw: string): number[] {
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.map(Number) : [];
  } catch {
    return [];
  }
}

// All unique category keywords flattened — events matching ANY category pass the gate
const ALL_CATEGORY_KEYWORDS = Array.from(
  new Set(Object.values(CATEGORY_KEYWORDS).flat())
);

function isMacroEvent(ev: PolyEvent): boolean {
  const text = `${ev.title} ${ev.description || ""}`.toLowerCase();
  const tagLabels = (ev.tags || []).map((t) => t.label.toLowerCase()).join(" ");
  const combined = `${text} ${tagLabels}`;
  const excluded = MACRO_EXCLUDE.some((kw) => combined.includes(kw));
  if (excluded) return false;
  // Pass if it matches any MACRO_INCLUDE keyword OR any category keyword
  if (MACRO_INCLUDE.some((kw) => combined.includes(kw))) return true;
  return ALL_CATEGORY_KEYWORDS.some((kw) => combined.includes(kw));
}

function matchesCategory(m: ParsedMarket, cat: CategoryTab): boolean {
  if (cat === "all") return true;
  // Tag-slug categories are fetched separately; don't keyword-filter them
  if (cat in TAG_SLUG_CATEGORIES) return true;
  const keywords = CATEGORY_KEYWORDS[cat as keyof typeof CATEGORY_KEYWORDS];
  if (!keywords) return true;
  const text = `${m.eventTitle} ${m.question} ${m.tags.join(" ")}`.toLowerCase();
  return keywords.some((kw) => text.includes(kw));
}

function formatVolume(v: number): string {
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function parseEventsCore(events: PolyEvent[], applyMacroFilter: boolean): ParsedMarket[] {
  const results: ParsedMarket[] = [];
  for (const ev of events) {
    if (!ev.active || ev.closed) continue;
    if (applyMacroFilter && !isMacroEvent(ev)) continue;
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

function parseEvents(events: PolyEvent[]): ParsedMarket[] {
  return parseEventsCore(events, true);
}

/** Parse events from a tag-specific API call (no macro keyword filter needed) */
function parseTagEvents(events: PolyEvent[]): ParsedMarket[] {
  return parseEventsCore(events, false);
}

// ─── Skeleton Loader ──────────────────────────────────────────────
function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded bg-white/5 ${className}`} />;
}

function CardSkeleton() {
  return (
    <GlassCard className="p-4">
      <Skeleton className="h-4 w-3/4 mb-3" />
      <Skeleton className="h-3 w-full mb-2" />
      <Skeleton className="h-6 w-full mb-3 rounded-full" />
      <div className="flex gap-3">
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-16" />
      </div>
    </GlassCard>
  );
}

function TickerSkeleton() {
  return (
    <div className="flex gap-4 overflow-hidden">
      {Array.from({ length: 5 }).map((_, i) => (
        <Skeleton key={i} className="h-14 w-56 flex-shrink-0 rounded-lg" />
      ))}
    </div>
  );
}

// ─── Sub-Components ───────────────────────────────────────────────

function LiveBadge() {
  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] font-bold tracking-widest uppercase text-emerald-400">
      <span className="relative flex h-2 w-2">
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
        <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-400" />
      </span>
      LIVE
    </span>
  );
}

function PriceBar({ yesPrice }: { yesPrice: number }) {
  const pct = Math.round(yesPrice * 100);
  const isHigh = pct >= 70;
  const isLow = pct <= 30;
  return (
    <div className="w-full">
      <div className="flex justify-between text-[10px] font-bold mb-1">
        <span className={isHigh ? "text-emerald-400" : isLow ? "text-red-400" : "text-blue-400"}>
          YES {pct}%
        </span>
        <span className="text-white/40">NO {100 - pct}%</span>
      </div>
      <div className="h-2 rounded-full bg-white/5 overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-700"
          style={{
            width: `${pct}%`,
            background: isHigh
              ? "linear-gradient(90deg, #22c55e, #4ade80)"
              : isLow
              ? "linear-gradient(90deg, #ef4444, #f87171)"
              : "linear-gradient(90deg, #3b82f6, #60a5fa)",
          }}
        />
      </div>
    </div>
  );
}

function MarketPulseBar({ markets }: { markets: ParsedMarket[] }) {
  const top = markets.slice(0, 5);
  if (top.length === 0) return null;
  return (
    <div className="mb-6">
      <div className="flex items-center gap-2 mb-3">
        <Activity className="w-4 h-4 text-emerald-400" />
        <h3 className="text-sm font-bold text-white/90 tracking-wide uppercase">Market Pulse</h3>
        <LiveBadge />
      </div>
      <div className="flex gap-3 overflow-x-auto pb-2 scrollbar-hide">
        {top.map((m) => {
          const pct = Math.round(m.yesPrice * 100);
          const isHigh = pct >= 60;
          return (
            <a
              key={m.marketId}
              href={`https://polymarket.com/event/${m.eventSlug}`}
              target="_blank"
              rel="noopener noreferrer"
              className="flex-shrink-0 min-w-[220px] max-w-[280px] bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2.5 hover:bg-white/[0.06] hover:border-white/10 transition-all cursor-pointer group"
            >
              <p className="text-[11px] text-white/70 font-medium leading-tight mb-2 line-clamp-2 group-hover:text-white/90 transition-colors">
                {m.question.length > 65 ? m.question.slice(0, 62) + "..." : m.question}
              </p>
              <div className="flex items-center gap-2">
                <span className={`text-sm font-bold ${isHigh ? "text-emerald-400" : "text-red-400"}`}>
                  {pct}%
                </span>
                <div className="flex-1 h-1.5 rounded-full bg-white/5 overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${pct}%`,
                      background: isHigh ? "#22c55e" : "#ef4444",
                    }}
                  />
                </div>
                <span className="text-[9px] text-white/30 font-mono">{formatVolume(m.volume24hr)}</span>
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
}

function MarketCard({ market }: { market: ParsedMarket }) {
  return (
    <a
      href={`https://polymarket.com/event/${market.eventSlug}`}
      target="_blank"
      rel="noopener noreferrer"
      className="block"
    >
      <GlassCard className="p-4 hover:bg-white/[0.06] hover:border-white/10 transition-all cursor-pointer h-full">
        <p className="text-xs font-semibold text-white/90 leading-tight mb-3 line-clamp-3 min-h-[3rem]">
          {market.question}
        </p>
        <PriceBar yesPrice={market.yesPrice} />
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1 mt-3 text-[10px] text-white/40">
          <span className="flex items-center gap-1">
            <BarChart3 className="w-3 h-3" />
            24h: {formatVolume(market.volume24hr)}
          </span>
          <span className="flex items-center gap-1">
            <DollarSign className="w-3 h-3" />
            Liq: {formatVolume(market.liquidity)}
          </span>
          {market.totalVolume > 0 && (
            <span className="flex items-center gap-1">
              <Users className="w-3 h-3" />
              Vol: {formatVolume(market.totalVolume)}
            </span>
          )}
        </div>
        {market.tags.length > 0 && (
          <div className="flex flex-wrap gap-1 mt-2">
            {market.tags.slice(0, 3).map((t) => (
              <span key={t} className="text-[9px] px-1.5 py-0.5 rounded bg-white/[0.04] text-white/30 border border-white/[0.04]">
                {t}
              </span>
            ))}
          </div>
        )}
      </GlassCard>
    </a>
  );
}

function MoversSection({ markets }: { markets: ParsedMarket[] }) {
  // Show markets with extreme probabilities (strong conviction) or near-50/50 (contested)
  const movers = [...markets]
    .filter((m) => m.volume24hr > 0)
    .sort((a, b) => {
      // Prioritize markets with highest volume (proxy for biggest activity/moves)
      // and extreme or highly contested probabilities
      const aScore = Math.abs(a.yesPrice - 0.5) * a.volume24hr;
      const bScore = Math.abs(b.yesPrice - 0.5) * b.volume24hr;
      return bScore - aScore;
    })
    .slice(0, 6);

  if (movers.length === 0) return null;
  return (
    <div className="mb-6">
      <div className="flex items-center gap-2 mb-3">
        <TrendingUp className="w-4 h-4 text-orange-400" />
        <h3 className="text-sm font-bold text-white/90 tracking-wide uppercase">Movers & Shakers</h3>
        <span className="text-[10px] text-white/30">Highest conviction bets</span>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
        {movers.map((m) => {
          const pct = Math.round(m.yesPrice * 100);
          const deviation = pct - 50;
          const isYesFavored = deviation > 0;
          return (
            <a
              key={m.marketId}
              href={`https://polymarket.com/event/${m.eventSlug}`}
              target="_blank"
              rel="noopener noreferrer"
              className="block"
            >
              <GlassCard className="p-3 hover:bg-white/[0.06] transition-all cursor-pointer">
                <div className="flex items-start justify-between gap-2 mb-2">
                  <p className="text-[11px] text-white/80 font-medium leading-tight line-clamp-2 flex-1">
                    {m.question.length > 80 ? m.question.slice(0, 77) + "..." : m.question}
                  </p>
                  <span
                    className={`text-xs font-bold px-2 py-0.5 rounded-full flex-shrink-0 ${
                      isYesFavored
                        ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
                        : "bg-red-500/10 text-red-400 border border-red-500/20"
                    }`}
                  >
                    {isYesFavored ? "+" : ""}
                    {deviation}%
                  </span>
                </div>
                <div className="flex items-center justify-between text-[10px] text-white/40">
                  <span>YES {pct}%</span>
                  <span>{formatVolume(m.volume24hr)} 24h</span>
                </div>
              </GlassCard>
            </a>
          );
        })}
      </div>
    </div>
  );
}

// ─── Earnings Calendar (Perplexity-style) ─────────────────────────

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

function buildBullets(entry: EarningsEntry): string[] {
  const bullets: string[] = [];
  const desc = entry.market.description || "";

  // EPS consensus
  if (entry.eps) {
    const q = entry.quarter || "quarterly";
    bullets.push(`Wall Street consensus EPS estimate of ${entry.eps} for ${q} earnings`);
  }

  // Beat probability
  const beatLabel = entry.beatPct >= 70 ? "strongly favored to beat" : entry.beatPct >= 55 ? "favored to beat" : entry.beatPct <= 30 ? "expected to miss" : entry.beatPct <= 45 ? "at risk of missing" : "near a coin flip on beating";
  bullets.push(`Polymarket crowd: ${beatLabel} estimates (${entry.beatPct}% chance of beat)`);

  // Try to extract revenue or other context from description
  const revMatch = desc.match(/revenue\s+(?:estimate|forecast|consensus)?\s*(?:of\s+)?\$?([\d.]+\s*(?:B|M|billion|million))/i);
  if (revMatch) {
    bullets.push(`Revenue estimate: $${revMatch[1]}`);
  }

  // Extract any sentence about the company from description that's informative
  const sentences = desc.split(/[.!]\s+/).filter((s) => s.length > 20 && s.length < 200);
  for (const s of sentences) {
    if (bullets.length >= 3) break;
    const lower = s.toLowerCase();
    // Skip boilerplate resolution sentences
    if (lower.includes("resolve to") || lower.includes("this market")) continue;
    if (lower.includes("reports") || lower.includes("announces") || lower.includes("expects") || lower.includes("growth") || lower.includes("decline") || lower.includes("revenue") || lower.includes("sector")) {
      const cleaned = s.trim().replace(/^\W+/, "");
      if (cleaned && !bullets.some((b) => b.includes(cleaned.slice(0, 30)))) {
        bullets.push(cleaned.endsWith(".") ? cleaned : cleaned + ".");
      }
    }
  }

  // Ensure at least 2 bullets
  if (bullets.length < 2) {
    bullets.push(`Trading volume: ${formatVolume(entry.market.totalVolume)} total on Polymarket`);
  }

  return bullets.slice(0, 3);
}

// Ticker → deterministic background color
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

// ─── Earnings Detail Modal ────────────────────────────────────────

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

function formatMktCap(v: number | undefined): string {
  if (!v) return "N/A";
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${v.toLocaleString()}`;
}

function EarningsModal({ entry, onClose }: { entry: EarningsEntry; onClose: () => void }) {
  const bullets = buildBullets(entry);
  const beatPct = entry.beatPct;
  const missPct = 100 - beatPct;
  const [detail, setDetail] = useState<EarningsDetailData | null>(null);
  const [loading, setLoading] = useState(true);

  // Fetch enriched data from backend
  useEffect(() => {
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
        if (res.ok && !cancelled) {
          const data = await res.json();
          setDetail(data);
        }
      } catch (e) {
        console.warn("[EarningsModal] detail fetch failed:", e);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [entry.ticker]);

  // Close on Escape
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
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70 backdrop-blur-sm" />

      {/* Modal */}
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

        {/* Stats grid — key metrics */}
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

        {/* Earnings History — past quarters */}
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

        {/* Polymarket key details (fallback bullets) */}
        {(!detail || loading) && (
          <div className="px-6 py-4 border-b border-white/[0.06]">
            {loading ? (
              <div className="flex items-center justify-center py-4 gap-2">
                <Loader2 className="w-4 h-4 text-blue-400 animate-spin" />
                <span className="text-xs text-white/40">Loading earnings data...</span>
              </div>
            ) : (
              <>
                <h4 className="text-[11px] font-semibold text-white/50 uppercase tracking-wider mb-3">Key Details</h4>
                <ul className="space-y-2">
                  {bullets.map((b, i) => (
                    <li key={i} className="flex items-start gap-2">
                      <span className="w-1 h-1 rounded-full bg-blue-400 mt-1.5 flex-shrink-0" />
                      <span className="text-xs text-white/70 leading-relaxed">{b}</span>
                    </li>
                  ))}
                </ul>
              </>
            )}
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
          <span className="text-[9px] text-white/20">Polymarket + Finnhub + Tavily</span>
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

function EarningsCalendar({ markets }: { markets: ParsedMarket[] }) {
  const [weekStart, setWeekStart] = useState<Date>(() => getSunday(new Date()));
  const [selectedDayKey, setSelectedDayKey] = useState<string>(dateKey(new Date()));
  const [modalEntry, setModalEntry] = useState<EarningsEntry | null>(null);

  // Build date → entries map
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

  // Combine selected + undated for display when viewing undated
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
              {/* Mini avatars */}
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
            const bullets = buildBullets(e);
            return (
              <div
                key={e.market.marketId}
                className="flex items-start gap-4 p-4 rounded-xl border border-white/[0.06] bg-white/[0.015] hover:bg-white/[0.03] hover:border-white/[0.1] transition-all group"
              >
                {/* Avatar */}
                <div className={`w-10 h-10 rounded-xl bg-gradient-to-br ${tickerColor(e.ticker)} flex items-center justify-center flex-shrink-0 mt-0.5`}>
                  <span className="text-xs font-bold text-white">{e.ticker.slice(0, 2)}</span>
                </div>

                {/* Main content */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      {/* Company name — clickable for modal */}
                      <button
                        onClick={() => setModalEntry(e)}
                        className="text-sm font-bold text-white hover:text-blue-400 transition-colors text-left"
                      >
                        {e.company}
                      </button>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-[11px] font-mono text-white/40">{e.ticker}</span>
                        {e.quarter && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-white/[0.04] text-white/30">{e.quarter}</span>
                        )}
                        {e.time && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-md bg-white/[0.04] text-white/30 flex items-center gap-0.5">
                            <Clock className="w-2.5 h-2.5" /> {e.time}
                          </span>
                        )}
                      </div>
                    </div>

                    {/* Beat % badge */}
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
                        }`}>chance of beat</span>
                      </div>
                    </div>
                  </div>

                  {/* Bullet points */}
                  <ul className="mt-2.5 space-y-1">
                    {bullets.map((b, i) => (
                      <li key={i} className="flex items-start gap-1.5">
                        <span className="w-1 h-1 rounded-full bg-white/20 mt-1.5 flex-shrink-0" />
                        <span className="text-[11px] text-white/50 leading-relaxed">{b}</span>
                      </li>
                    ))}
                  </ul>

                  {/* Quick stats row */}
                  <div className="flex items-center gap-4 mt-2.5">
                    {e.eps && (
                      <span className="text-[10px] text-white/30">
                        <span className="text-white/50 font-semibold">EPS Est:</span> {e.eps}
                      </span>
                    )}
                    <span className="text-[10px] text-white/30">
                      <span className="text-white/50 font-semibold">Vol:</span> {formatVolume(e.market.totalVolume)}
                    </span>
                    <button
                      onClick={() => setModalEntry(e)}
                      className="text-[10px] text-blue-400/60 hover:text-blue-400 transition-colors ml-auto"
                    >
                      More details
                    </button>
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
          Odds powered by Polymarket prediction markets
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

      {/* Modal */}
      {modalEntry && <EarningsModal entry={modalEntry} onClose={() => setModalEntry(null)} />}
    </div>
  );
}

// ─── Fetch helper for tag-specific Polymarket data ────────────────

async function fetchPolymarketByTag(tagSlug: string): Promise<PolyEvent[] | null> {
  // Attempt 1: Backend proxy with tag_slug
  try {
    const proxyRes = await fetch(
      `${POLYMARKET_PROXY}?limit=50&tag_slug=${encodeURIComponent(tagSlug)}`
    );
    if (proxyRes.ok) {
      const json = await proxyRes.json();
      if (Array.isArray(json)) return json;
    }
  } catch { /* fall through */ }

  // Attempt 2: Direct Gamma API
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

// ─── Dashboard Component ──────────────────────────────────────────

function PolymarketDashboard() {
  const [markets, setMarkets] = useState<ParsedMarket[]>([]);
  const [tagCache, setTagCache] = useState<Record<string, ParsedMarket[]>>({});
  const [earningsMarkets, setEarningsMarkets] = useState<ParsedMarket[]>([]);
  const [loading, setLoading] = useState(true);
  const [tagLoading, setTagLoading] = useState(false);
  const [earningsLoading, setEarningsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<CategoryTab>("all");
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Fetch the main macro markets (for all/crypto/fed/elections/economy/geopolitics)
  const fetchData = useCallback(async () => {
    try {
      let data: PolyEvent[] | null = null;

      try {
        const proxyRes = await fetch(`${POLYMARKET_PROXY}?limit=100`);
        if (proxyRes.ok) {
          const json = await proxyRes.json();
          if (Array.isArray(json)) data = json;
        }
      } catch { /* fall through */ }

      if (!data) {
        try {
          const directRes = await fetch(
            `${GAMMA_API}?limit=100&active=true&closed=false&order=volume24hr&ascending=false`
          );
          if (directRes.ok) {
            const json = await directRes.json();
            if (Array.isArray(json)) data = json;
          }
        } catch { /* fall through */ }
      }

      if (!data || !Array.isArray(data) || data.length === 0) {
        setError("Unable to fetch Polymarket data. Markets may be unavailable.");
        return;
      }
      const parsed = parseEvents(data);
      setMarkets(parsed);
      setError(null);
      setLastUpdated(new Date());
    } catch (err) {
      console.error("[POLYMARKET] Unexpected error:", err);
      setError("Failed to load prediction markets.");
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch tag-specific markets — caches results so switching tabs is instant
  const fetchTagData = useCallback(async (tab: CategoryTab) => {
    const tagSlug = TAG_SLUG_CATEGORIES[tab];
    if (!tagSlug) return;
    // If already cached, don't show loading
    if (!tagCache[tagSlug]) setTagLoading(true);
    try {
      const data = await fetchPolymarketByTag(tagSlug);
      if (data && data.length > 0) {
        const parsed = parseTagEvents(data);
        setTagCache((prev) => ({ ...prev, [tagSlug]: parsed }));
      }
    } catch { /* keep cached data */ }
    finally {
      setTagLoading(false);
    }
  }, [tagCache]);

  // Fetch earnings data on mount (pre-loaded, always visible)
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
    fetchData();
    fetchEarnings();
    const iv = setInterval(() => { fetchData(); fetchEarnings(); }, REFRESH_INTERVAL);
    return () => clearInterval(iv);
  }, [fetchData, fetchEarnings]);

  // When switching to a tag-slug category, fetch if not cached
  useEffect(() => {
    const tagSlug = TAG_SLUG_CATEGORIES[activeTab];
    if (tagSlug) {
      fetchTagData(activeTab);
    }
  }, [activeTab, fetchTagData]);

  const handleRefresh = () => {
    setLoading(true);
    fetchData();
    fetchEarnings();
    const tagSlug = TAG_SLUG_CATEGORIES[activeTab];
    if (tagSlug) {
      // Clear cache for active tag to force reload
      setTagCache((prev) => { const n = { ...prev }; delete n[tagSlug]; return n; });
      fetchTagData(activeTab);
    }
  };

  // For tag-slug categories, use cached tag data; for others, filter the main markets
  const isTagCategory = activeTab in TAG_SLUG_CATEGORIES;
  const tagSlug = TAG_SLUG_CATEGORIES[activeTab];
  const filtered = isTagCategory
    ? (tagSlug ? tagCache[tagSlug] || [] : [])
    : markets.filter((m) => matchesCategory(m, activeTab));

  const tabs: { key: CategoryTab; label: string }[] = [
    { key: "all", label: "All Macro" },
    { key: "crypto", label: "Crypto" },
    { key: "fed", label: "Fed & Rates" },
    { key: "elections", label: "Elections" },
    { key: "economy", label: "Economy" },
    { key: "geopolitics", label: "Geopolitics" },
    { key: "finance", label: "Finance" },
    { key: "tech", label: "Tech" },
  ];

  return (
    <>
    <GlassCard className="p-5 mb-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-5 flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-gradient-to-br from-purple-500 to-blue-500 rounded-lg flex items-center justify-center">
            <BarChart3 className="w-4 h-4 text-white" />
          </div>
          <div>
            <h2 className="text-base font-bold text-white flex items-center gap-2">
              Prediction Markets Dashboard
              <LiveBadge />
            </h2>
            <p className="text-[10px] text-white/30">
              Live prediction market odds for macro, economics & investing
              {lastUpdated && (
                <> &middot; Updated {lastUpdated.toLocaleTimeString()}</>
              )}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <a
            href="https://polymarket.com/crypto"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-white/10 hover:bg-white/5 transition-colors text-xs text-white/50 hover:text-white/80"
          >
            <ExternalLink className="w-3 h-3" />
            Open Polymarket
          </a>
          <button
            onClick={handleRefresh}
            disabled={loading}
            className="p-2 rounded-lg border border-white/10 hover:bg-white/5 transition-colors disabled:opacity-40"
          >
            <RefreshCw className={`w-3.5 h-3.5 text-white/50 ${loading ? "animate-spin" : ""}`} />
          </button>
        </div>
      </div>

      {/* Error state */}
      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-500/5 border border-red-500/10 text-xs text-red-400 flex items-center gap-2">
          <span>Data unavailable — {error}</span>
          <button onClick={handleRefresh} className="text-red-300 underline hover:text-red-200">
            Retry
          </button>
        </div>
      )}

      {/* Loading */}
      {loading && markets.length === 0 ? (
        <div>
          <TickerSkeleton />
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 mt-6">
            {Array.from({ length: 6 }).map((_, i) => (
              <CardSkeleton key={i} />
            ))}
          </div>
        </div>
      ) : (
        <>
          {/* Market Pulse Ticker Tape — always uses main markets */}
          <MarketPulseBar markets={markets} />

          {/* Movers & Shakers — always uses main markets */}
          <MoversSection markets={markets} />

          {/* Category Tabs */}
          <div className="flex items-center gap-1.5 mb-4 overflow-x-auto pb-1 scrollbar-hide">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => setActiveTab(tab.key)}
                className={`px-3 py-1.5 rounded-full text-[11px] font-semibold transition-all whitespace-nowrap border ${
                  activeTab === tab.key
                    ? "bg-blue-500/15 text-blue-400 border-blue-500/30"
                    : "bg-transparent text-white/40 border-white/[0.06] hover:text-white/60 hover:border-white/10"
                }`}
              >
                {tab.label}
                {activeTab === tab.key && filtered.length > 0 && (
                  <span className="ml-1.5 text-[9px] text-blue-400/60">
                    {filtered.length}
                  </span>
                )}
              </button>
            ))}
          </div>

          {/* Tag category: show skeleton only on first load (no cache yet) */}
          {isTagCategory && tagLoading && filtered.length === 0 ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 mt-2">
              {Array.from({ length: 6 }).map((_, i) => (
                <CardSkeleton key={i} />
              ))}
            </div>
          ) : (
            <>
              <div className="flex items-center gap-2 mb-3">
                <TrendingUp className="w-4 h-4 text-blue-400" />
                <h3 className="text-sm font-bold text-white/90 tracking-wide uppercase">
                  {isTagCategory ? `${activeTab.charAt(0).toUpperCase() + activeTab.slice(1)} Markets` : "Top Macro Markets"}
                </h3>
                <span className="text-[10px] text-white/30">By 24h volume</span>
              </div>

              {filtered.length === 0 ? (
                <div className="text-center py-8 text-sm text-white/30">
                  No markets found for this category.
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                  {filtered.slice(0, 15).map((m) => (
                    <MarketCard key={m.marketId} market={m} />
                  ))}
                </div>
              )}

              {filtered.length > 15 && (
                <p className="text-center text-[10px] text-white/20 mt-3">
                  Showing top 15 of {filtered.length} markets
                </p>
              )}
            </>
          )}
        </>
      )}
    </GlassCard>

    {/* ═══ Earnings Calendar — always visible, pre-loaded ═══ */}
    <GlassCard className="p-5 mb-8">
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
        <EarningsCalendar markets={earningsMarkets} />
      )}
    </GlassCard>
    </>
  );
}

// ─── Prediction Markets Agent ─────────────────────────────────────

interface AgentMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: number;
}

const SUGGESTED_PROMPTS = [
  "How do the current Fed rate cut odds affect equity sectors?",
  "If the top crypto events play out, what's the best positioning?",
  "Which prediction market events have the biggest cross-asset implications?",
  "What are the most mispriced prediction markets right now?",
];

function PredictionAgent() {
  const [messages, setMessages] = useState<AgentMessage[]>([]);
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
    const userMsg: AgentMessage = { role: "user", content: text.trim(), timestamp: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const payload: Record<string, unknown> = {
        query: text.trim(),
        preset_intent: "prediction_markets",
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

      const data = await res.json();
      const convId = data.conversation_id || conversationId;
      if (convId) setConversationId(convId);

      // Extract the analysis text from the response
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
      console.error("[PREDICT_AGENT]", err);
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
    <GlassCard className="p-5 mb-8">
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'linear-gradient(135deg, #6366f1 0%, #3b82f6 50%, #06b6d4 100%)' }}>
          <Sparkles className="w-4 h-4 text-white" />
        </div>
        <div>
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            Caelyn Predicts
          </h2>
          <p className="text-[10px] text-white/30">
            Ask how prediction market odds affect investments, sectors, and positioning
          </p>
        </div>
      </div>

      {/* Suggested prompts (only show when no messages) */}
      {messages.length === 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-4">
          {SUGGESTED_PROMPTS.map((prompt) => (
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
                  ? "bg-blue-500/10 border border-blue-500/20 text-blue-100"
                  : "bg-white/[0.03] border border-white/[0.06] text-white/80"
              }`}
            >
              <div className="flex items-center gap-1.5 mb-1.5">
                <span className={`text-[9px] font-bold uppercase tracking-wider ${
                  msg.role === "user" ? "text-blue-400" : "text-orange-400"
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
                Analyzing prediction markets data with macro context...
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
          placeholder="Ask about prediction market implications... (e.g., &quot;How do rate cut odds affect tech stocks?&quot;)"
          disabled={loading}
          rows={1}
          className="flex-1 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2.5 text-xs text-white placeholder-white/25 resize-none focus:outline-none focus:border-white/20 focus:ring-1 focus:ring-white/10 disabled:opacity-40 transition-all"
        />
        <Button
          type="submit"
          disabled={loading || !input.trim()}
          className="text-white px-3 py-2 rounded-lg transition-all disabled:opacity-30 flex-shrink-0" style={{ background: 'linear-gradient(135deg, #6366f1, #3b82f6, #06b6d4)' }}
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

// ─── Lazy Iframe ──────────────────────────────────────────────────

function LazyIframe({ src, title, sandbox, referrerPolicy, scrolling }: {
  src: string;
  title: string;
  sandbox?: string;
  referrerPolicy?: string;
  scrolling?: string;
}) {
  const [loaded, setLoaded] = useState(false);
  if (!loaded) {
    return (
      <div
        className="w-full h-[600px] rounded-lg border border-white/10 flex flex-col items-center justify-center gap-3 bg-white/[0.02] cursor-pointer hover:bg-white/[0.04] transition-colors"
        onClick={() => setLoaded(true)}
      >
        <ExternalLink className="w-6 h-6 text-white/20" />
        <p className="text-xs text-white/30">Click to load {title}</p>
        <p className="text-[10px] text-white/15">Loads on demand to avoid auto-popups</p>
      </div>
    );
  }
  return (
    <iframe
      src={src}
      className="w-full h-[600px] rounded-lg border border-crypto-silver/20"
      title={title}
      frameBorder="0"
      sandbox={sandbox}
      referrerPolicy={referrerPolicy as React.IframeHTMLAttributes<HTMLIFrameElement>["referrerPolicy"]}
      scrolling={scrolling}
    />
  );
}

// ─── Existing Components ──────────────────────────────────────────

const SmallLink = ({ href, label }: { href: string; label: string }) => (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    className="text-xs text-white/40 hover:text-white/70 transition-colors flex items-center gap-1"
  >
    {label} <ExternalLink className="w-3 h-3" />
  </a>
);

const openInNewTab = (url: string) => {
  openSecureLink(url);
};

// ─── Main Page ────────────────────────────────────────────────────

export default function PredictPage() {
  return (
    <div className="min-h-screen text-white relative" style={{ background: '#050510', fontFamily: "'Outfit', sans-serif" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
      `}</style>
      {/* Blue radial gradient background — same as Social page */}
      <div style={{
        position: 'fixed', top: '-40%', left: '-20%', width: '140%', height: '140%',
        background: 'radial-gradient(ellipse 800px 600px at 20% 15%, rgba(99,102,241,0.06) 0%, transparent 70%), radial-gradient(ellipse 600px 500px at 80% 70%, rgba(6,182,212,0.04) 0%, transparent 70%), radial-gradient(ellipse 900px 400px at 50% 50%, rgba(59,130,246,0.03) 0%, transparent 60%)',
        pointerEvents: 'none', zIndex: 0
      }} />
      {/* Header */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 relative" style={{ zIndex: 1 }}>
        <div className="text-center relative">
          <div className="absolute inset-0 bg-gradient-to-r from-indigo-500/10 via-blue-500/10 to-cyan-500/10 blur-3xl -z-10"></div>
          <div className="flex justify-center items-center gap-4 mb-4">
            <div className="w-28 h-28 rounded-full flex items-center justify-center border-2 shadow-2xl hover:scale-110 transition-transform duration-300 overflow-hidden" style={{ borderColor: '#38bdf8' }}>
              <img
                src={diceImage}
                alt="Prediction Markets"
                className="w-28 h-28 object-cover"
              />
            </div>
            <h2 className="text-2xl lg:text-3xl font-bold" style={{ background: 'linear-gradient(135deg, #6366f1 0%, #3b82f6 40%, #06b6d4 100%)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>Prediction Markets</h2>
          </div>
          <p className="text-base font-medium tracking-wide" style={{ color: '#64748b' }}>Decentralized Casino and Analytics</p>
          <div className="w-32 h-1 mx-auto mt-3 rounded-full" style={{ background: 'linear-gradient(135deg, #6366f1, #3b82f6, #06b6d4)' }}></div>
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 relative" style={{ zIndex: 1 }}>

        {/* ═══ Prediction Markets Agent ═══ */}
        <PredictionAgent />

        {/* ═══ Prediction Markets Dashboard ═══ */}
        <PolymarketDashboard />

        {/* ═══ Existing iframes & cards ═══ */}
        <GlassCard className="p-6">
          <div className="flex justify-end mb-1 mt-0">
            <SmallLink href="https://predictbase.app/" label="Open PredictBase" />
          </div>
          <LazyIframe
            src="https://predictbase.app/"
            title="PredictBase"
            sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
            referrerPolicy="no-referrer-when-downgrade"
          />

          <div className="flex justify-end mb-1 mt-6">
            <SmallLink href="https://betbase.xyz/" label="Open BetBase" />
          </div>
          <LazyIframe
            src="https://betbase.xyz/"
            title="BetBase"
            sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
            referrerPolicy="no-referrer-when-downgrade"
          />

          <div className="flex justify-end mb-1 mt-6">
            <SmallLink href="https://pmx.trade/markets" label="Open PMX Trading" />
          </div>
          <LazyIframe
            src="https://pmx.trade/markets"
            title="PMX Trading"
            scrolling="yes"
          />

          {/* Kalshi */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-indigo-500/10 to-purple-500/10 border border-indigo-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">Kalshi</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://kalshi.com/")}
                  className="bg-gradient-to-r from-indigo-500 to-purple-500 hover:from-indigo-600 hover:to-purple-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Kalshi
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                CFTC-regulated prediction markets where you can trade on real-world events.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-indigo-500/20">
                <p className="text-sm text-crypto-silver">
                  Trade on elections, economic data, and news events
                  <br />
                  CFTC-regulated and fully legal in the US
                  <br />
                  Real money trading with transparent odds
                </p>
              </div>
            </div>
          </div>

          {/* TrueMarkets */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-green-500/10 to-teal-500/10 border border-green-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-green-500 to-teal-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">TrueMarkets</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://app.truemarkets.org/en/markets")}
                  className="bg-gradient-to-r from-green-500 to-teal-500 hover:from-green-600 hover:to-teal-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open TrueMarkets
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                Decentralized prediction markets platform with transparent and trustless betting mechanisms.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-green-500/20">
                <p className="text-sm text-crypto-silver">
                  Decentralized prediction markets on blockchain
                  <br />
                  Trustless and transparent betting protocols
                  <br />
                  Wide range of prediction market categories
                </p>
              </div>
            </div>
          </div>

          {/* Cloudbet Sports Betting */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">Cloudbet Sports Betting</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://www.cloudbet.com/en/sports")}
                  className="bg-gradient-to-r from-blue-500 to-purple-500 hover:from-blue-600 hover:to-purple-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Cloudbet
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                Professional crypto sports betting platform with competitive odds and live betting options.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-blue-500/20">
                <p className="text-sm text-crypto-silver">
                  Crypto-first sportsbook with Bitcoin, Ethereum, and altcoin betting
                  <br />
                  Live betting on major sports events
                  <br />
                  Provably fair gaming and instant withdrawals
                </p>
              </div>
            </div>
          </div>

          {/* Betly.trade */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-purple-500/10 to-pink-500/10 border border-purple-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">Betly.trade</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://www.betly.trade/categories")}
                  className="bg-gradient-to-r from-purple-500 to-pink-500 hover:from-purple-600 hover:to-pink-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Betly
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                Social betting made simple. Swipe on prediction markets, not thots.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-purple-500/20">
                <p className="text-sm text-crypto-silver">
                  Swipe-based prediction market interface
                  <br />
                  Social betting on crypto and trending topics
                  <br />
                  Quick and simple market participation
                </p>
              </div>
            </div>
          </div>

          {/* Limitless Exchange */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-orange-500/10 to-red-500/10 border border-orange-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-orange-500 to-red-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">Limitless Exchange</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://limitless.exchange/advanced")}
                  className="bg-gradient-to-r from-orange-500 to-red-500 hover:from-orange-600 hover:to-red-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Limitless
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                Predict future crypto and stocks prices with sophisticated trading features and analytics.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-orange-500/20">
                <p className="text-sm text-crypto-silver">
                  Advanced prediction market trading platform
                  <br />
                  Crypto and stock price predictions
                  <br />
                  Sophisticated analytics and trading features
                </p>
              </div>
            </div>
          </div>

          {/* Overtime Markets */}
          <div className="mt-6">
            <div className="p-6 bg-gradient-to-r from-green-500/10 to-blue-500/10 border border-green-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-green-500 to-blue-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h3 className="text-xl font-semibold text-white">Overtime Markets</h3>
                </div>
                <Button
                  onClick={() => openInNewTab("https://www.overtimemarkets.xyz/markets?status=OpenMarkets&sport=Live")}
                  className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Overtime
                </Button>
              </div>
              <p className="text-crypto-silver mb-4">
                Decentralized sports prediction markets with live betting on major sports events.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-green-500/20">
                <p className="text-sm text-crypto-silver">
                  Live sports prediction markets
                  <br />
                  Decentralized betting on NBA, NFL, Soccer, and more
                  <br />
                  Real-time odds and transparent market mechanics
                </p>
              </div>
            </div>
          </div>
        </GlassCard>
      </main>
    </div>
  );
}
