import { useState, useEffect, useRef, useCallback, useMemo, Fragment } from "react";
import type { ReactNode } from "react";
import {
  RefreshCw,
  Send,
  Loader2,
  Zap,
  ChevronDown,
  ChevronUp,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Activity,
  BarChart3,
  Database,
  CircleAlert,
  Save,
  RotateCcw,
  X,
  Clock,
  Eye,
} from "lucide-react";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  ComposedChart,
  Area,
  ReferenceLine,
} from "recharts";

const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const API_BASE = `${AGENT_BACKEND_URL}/api/options`;

function getToken(): string | null {
  return localStorage.getItem("caelyn_token") || sessionStorage.getItem("caelyn_token");
}
function authHeaders(): Record<string, string> {
  const h: Record<string, string> = { "Content-Type": "application/json", "X-API-Key": AGENT_API_KEY };
  const t = getToken();
  if (t) h["Authorization"] = `Bearer ${t}`;
  return h;
}

const C = {
  bg: "#050510",
  card: "#08080f",
  cardAlt: "#0c0c1a",
  border: "#1a1a30",
  bright: "#e2e8f0",
  text: "#94a3b8",
  dim: "#475569",
  blue: "#38bdf8",
  green: "#4ade80",
  red: "#ef4444",
  yellow: "#fbbf24",
  orange: "#f97316",
  purple: "#a855f7",
  gold: "#f59e0b",
};
const font = "'JetBrains Mono', 'Fira Code', monospace";
const sans = "'Outfit', 'Inter', sans-serif";

type SortDir = "asc" | "desc";
type CatFilter = "all" | "stock" | "etf";
type SideFilter = "all" | "call" | "put";
type MainTab = "tickers" | "flow";

interface ScoreWeights {
  flow_score?: number | null;
  gamma_score?: number | null;
  asymmetry_score?: number | null;
  volatility_score?: number | null;
  sentiment_score?: number | null;
  stock_context_score?: number | null;
  [key: string]: string | number | null | undefined;
}

interface ModularScores {
  flow_score?: number | null;
  gamma_score?: number | null;
  asymmetry_score?: number | null;
  volatility_score?: number | null;
  sentiment_score?: number | null;
  stock_context_score?: number | null;
}

interface StockContext {
  stock_relative_volume?: number | null;
  stock_intraday_move_pct?: number | null;
  breakout_context?: string | null;
  compression_context?: string | null;
  reversal_context?: string | null;
  catalyst_context?: string | null;
  liquidity_context?: string | null;
  short_squeeze_context?: string | null;
  macro_context?: string | null;
}

interface OptionsContext {
  call_put_volume_ratio?: number | null;
  call_put_oi_ratio?: number | null;
  near_spot_oi_density?: number | null;
  near_spot_gamma_density?: number | null;
  iv_current?: number | null;
  expected_move_from_atm_straddle?: number | null;
  gamma_score_is_approximation?: boolean | null;
}

interface DataQuality {
  confidence?: string | null;
  confidence_score?: number | null;
  flags?: string[] | null;
  missing_data_flags?: string[] | null;
  approximate_metrics?: string[] | null;
  history_metrics_ready?: boolean | null;
}

interface OptionGreeks {
  delta?: number | null;
  gamma?: number | null;
  theta?: number | null;
  vega?: number | null;
}

interface OptionContract {
  contract_symbol?: string | null;
  symbol?: string | null;
  type?: string | null;
  side?: string | null;
  strike?: number | string | null;
  expiration?: string | null;
  dte?: number | null;
  bid?: number | null;
  ask?: number | null;
  last?: number | null;
  mid?: number | null;
  volume?: number | null;
  open_interest?: number | null;
  openInterest?: number | null;
  implied_volatility?: number | null;
  iv?: number | null;
  greeks?: OptionGreeks | null;
  delta?: number | null;
  gamma?: number | null;
  theta?: number | null;
  vega?: number | null;
  option_volume_to_oi_ratio?: number | null;
  vol_oi_ratio?: number | null;
  spread_pct?: number | null;
  premium_traded_estimate?: number | null;
  break_even?: number | null;
  break_even_distance_pct?: number | null;
  contract_liquidity_quality?: string | null;
  repeated_flow_score?: number | null;
  iv_rank?: number | null;
  iv_percentile?: number | null;
  contract_score?: number | null;
  flow_score?: number | null;
  asymmetry_score?: number | null;
  short_thesis?: string | null;
  underlying?: string | null;
  category?: string | null;
  confidence?: string | null;
  primary_signal?: string | null;
}

interface TickerResult {
  ticker: string;
  category?: string | null;
  underlying_price?: number | null;
  price_change_pct?: number | null;
  expiration_focus?: Array<string | number> | null;
  call_volume?: number | null;
  put_volume?: number | null;
  total_volume?: number | null;
  pc_ratio?: number | null;
  call_oi?: number | null;
  put_oi?: number | null;
  total_oi?: number | null;
  avg_call_iv?: number | null;
  avg_put_iv?: number | null;
  iv_skew?: number | null;
  max_pain?: number | null;
  primary_signal?: string | null;
  confidence?: string | null;
  confidence_score?: number | null;
  composite_score?: number | null;
  modular_scores?: ModularScores | null;
  stock_context_summary?: string | null;
  options_context_summary?: string | null;
  stock_context?: StockContext | null;
  options_context?: OptionsContext | null;
  top_contracts?: OptionContract[] | null;
  top_calls?: OptionContract[] | null;
  top_puts?: OptionContract[] | null;
  thesis?: string | string[] | null;
  risks?: string | string[] | null;
  data_quality?: DataQuality | null;
  technicals?: any;
  historic_volume?: any;
}

interface OptionsDashboardResponse {
  display_type?: string | null;
  scan_type?: string | null;
  filter_defaults?: Record<string, unknown> | null;
  score_weights?: ScoreWeights | null;
  pipeline_stats?: Record<string, unknown> | null;
  market_summary?: Record<string, unknown> | null;
  tickers?: TickerResult[] | null;
  all_contracts?: OptionContract[] | null;
}

const sideColor = (s?: string | null) => s?.toLowerCase() === "call" ? C.green : C.red;
const pcColor = (r: number | null) => r == null ? C.dim : r > 1.2 ? C.red : r < 0.8 ? C.green : C.yellow;
const voiColor = (r: number | null) => r == null ? C.dim : r > 10 ? C.red : r > 5 ? C.orange : r > 3 ? C.yellow : C.dim;
const skewColor = (s: number | null) => s == null ? C.dim : s > 0.05 ? C.red : s < -0.05 ? C.green : C.dim;
const rsiColor = (v: number | null | undefined) => v == null ? C.dim : v > 70 ? C.red : v < 30 ? C.green : C.text;
const macdColor = (v: number | null | undefined) => v == null ? C.dim : v > 0 ? C.green : C.red;
const scoreColor = (score: number | null) => {
  if (score == null) return C.dim;
  if (score >= 80) return C.green;
  if (score >= 65) return C.blue;
  if (score >= 50) return C.yellow;
  return C.dim;
};
const trendSignal = (sma20?: number | null, sma50?: number | null): { label: string; color: string } | null => {
  if (sma20 == null || sma50 == null) return null;
  return sma20 > sma50 ? { label: "Bullish", color: C.green } : { label: "Bearish", color: C.red };
};

const safeNum = (n: unknown): number | null => {
  if (n == null) return null;
  const v = typeof n === "string" ? parseFloat(n) : Number(n);
  return Number.isFinite(v) ? v : null;
};
const fmtVol = (n: unknown) => {
  const v = safeNum(n);
  if (v == null) return "—";
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(0)}K`;
  return String(Math.round(v));
};
const fmtNum = (n: unknown, d = 2) => { const v = safeNum(n); return v == null ? "—" : v.toFixed(d); };
const fmtMoney = (n: unknown, d = 2) => { const v = safeNum(n); return v == null ? "—" : `$${v.toFixed(d)}`; };
const fmtSmartPct = (n: unknown, d = 1) => {
  const v = safeNum(n);
  if (v == null) return "—";
  const value = Math.abs(v) <= 1 ? v * 100 : v;
  return `${value >= 0 ? "+" : ""}${value.toFixed(d)}%`;
};
const fmtRatioPct = (n: unknown, d = 1) => {
  const v = safeNum(n);
  if (v == null) return "—";
  return `${(v * 100).toFixed(d)}%`;
};
const fmtPlainPct = (n: unknown, d = 1) => { const v = safeNum(n); return v == null ? "—" : `${v.toFixed(d)}%`; };
const fmtMaybeText = (value: unknown) => {
  if (value == null || value === "") return "—";
  if (Array.isArray(value)) return value.filter(Boolean).join(", ");
  return String(value);
};
const ensureArray = (value: unknown): string[] => {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean).map(String);
  return [String(value)];
};
const normalizeScore = (value: number | null | undefined) => {
  if (value == null || Number.isNaN(value)) return null;
  if (Math.abs(value) <= 1) return Math.max(0, Math.min(100, value * 100));
  return Math.max(0, Math.min(100, value));
};
const getConfidence = (confidence?: string | null, confidenceScore?: number | null) => {
  const label = confidence?.toLowerCase() || (confidenceScore != null ? (normalizeScore(confidenceScore) ?? 0) >= 75 ? "high" : (normalizeScore(confidenceScore) ?? 0) >= 45 ? "medium" : "low" : "unknown");
  if (label.includes("high")) return { label: "High", color: C.green };
  if (label.includes("medium")) return { label: "Medium", color: C.yellow };
  if (label.includes("low")) return { label: "Low", color: C.red };
  return { label: confidence || "Unknown", color: C.dim };
};
const getSignalColor = (signal?: string | null) => {
  const s = (signal || "").toLowerCase();
  if (s.includes("gamma")) return C.purple;
  if (s.includes("breakout") || s.includes("bull")) return C.green;
  if (s.includes("sentiment") || s.includes("earnings")) return C.orange;
  if (s.includes("asym")) return C.blue;
  if (s.includes("vol")) return C.yellow;
  if (s.includes("put") || s.includes("bear")) return C.red;
  return C.blue;
};
const toTitleCase = (value: string) => value.split(/[_\s-]+/).filter(Boolean).map(part => part.charAt(0).toUpperCase() + part.slice(1)).join(" ");
const compactDate = (value?: string | null) => value ? String(value).slice(5, 10) : "—";
const normalizeContract = (contract: OptionContract) => {
  const side = (contract.type || contract.side || "").toLowerCase() || "call";
  const openInterest = safeNum(contract.open_interest ?? contract.openInterest);
  const iv = safeNum(contract.iv ?? contract.implied_volatility);
  const delta = safeNum(contract.greeks?.delta ?? contract.delta);
  const gamma = safeNum(contract.greeks?.gamma ?? contract.gamma);
  const theta = safeNum(contract.greeks?.theta ?? contract.theta);
  const vega = safeNum(contract.greeks?.vega ?? contract.vega);
  const volumeToOi = safeNum(contract.option_volume_to_oi_ratio ?? contract.vol_oi_ratio);
  return { ...contract, side, openInterest, iv, delta, gamma, theta, vega, volumeToOi };
};
const signalTagsForTicker = (ticker: TickerResult) => {
  const tags: Array<{ label: string; color: string }> = [];
  const primary = ticker.primary_signal?.toLowerCase() || "";
  const stockSummary = (ticker.stock_context_summary || "").toLowerCase();
  const optionsSummary = (ticker.options_context_summary || "").toLowerCase();
  const breakout = (ticker.stock_context?.breakout_context || "").toLowerCase();
  const catalyst = (ticker.stock_context?.catalyst_context || "").toLowerCase();
  const gammaApprox = !!ticker.options_context?.gamma_score_is_approximation;

  if (primary.includes("unusual") || optionsSummary.includes("unusual")) tags.push({ label: "Unusual Flow", color: C.blue });
  if (primary.includes("gamma") || optionsSummary.includes("gamma")) tags.push({ label: gammaApprox ? "Gamma Setup ~" : "Gamma Setup", color: C.purple });
  if (primary.includes("asym") || optionsSummary.includes("asym")) tags.push({ label: "Asymmetric R/R", color: C.green });
  if (primary.includes("sentiment") || optionsSummary.includes("sentiment")) tags.push({ label: "Sentiment Extreme", color: C.orange });
  if (primary.includes("vol") || optionsSummary.includes("volatility")) tags.push({ label: "Vol Expansion", color: C.yellow });
  if (catalyst.includes("earnings") || optionsSummary.includes("earnings")) tags.push({ label: "Earnings Move", color: C.orange });
  if (breakout.includes("breakout") || primary.includes("breakout") || stockSummary.includes("breakout")) tags.push({ label: "Breakout Confirm", color: C.green });

  return tags.slice(0, 4);
};

function Badge({ color, children, sm }: { color: string; children: ReactNode; sm?: boolean }) {
  return (
    <span
      style={{
        background: `${color}18`,
        color,
        border: `1px solid ${color}35`,
        borderRadius: 999,
        padding: sm ? "1px 7px" : "3px 9px",
        fontSize: sm ? 9 : 10,
        fontWeight: 700,
        fontFamily: font,
        textTransform: "uppercase",
        letterSpacing: "0.05em",
        display: "inline-flex",
        alignItems: "center",
        gap: 4,
      }}
    >
      {children}
    </span>
  );
}

function SectionCard({ children }: { children: ReactNode }) {
  return <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10 }}>{children}</div>;
}

function MetricBlock({ label, value, color = C.bright, subtext }: { label: string; value: ReactNode; color?: string; subtext?: ReactNode }) {
  return (
    <div style={{ padding: "10px 12px", borderRadius: 8, background: `${color}08`, border: `1px solid ${color}18`, minWidth: 110, transition: "opacity 0.15s ease, transform 0.2s ease" }}>
      <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 4 }}>{label}</div>
      <div style={{ color, fontSize: 14, fontFamily: font, fontWeight: 700, transition: "color 0.15s ease" }}>{value}</div>
      {subtext ? <div style={{ color: C.text, fontSize: 10, marginTop: 4 }}>{subtext}</div> : null}
    </div>
  );
}

function ScoreBar({ label, value }: { label: string; value?: number | null }) {
  const normalized = normalizeScore(value);
  const color = scoreColor(normalized);
  return (
    <div style={{ minWidth: 120 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 4 }}>
        <span style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase" }}>{label}</span>
        <span style={{ color, fontSize: 10, fontFamily: font, fontWeight: 700 }}>{normalized != null ? fmtNum(normalized, 0) : "—"}</span>
      </div>
      <div style={{ height: 7, borderRadius: 999, background: C.border, overflow: "hidden" }}>
        <div style={{ width: `${normalized ?? 0}%`, height: "100%", background: `linear-gradient(90deg, ${color}66, ${color})` }} />
      </div>
    </div>
  );
}

function DetailList({ title, items }: { title: string; items: Array<{ label: string; value: ReactNode; color?: string }> }) {
  const visible = items.filter(item => item.value !== null && item.value !== undefined && item.value !== "—" && item.value !== "");
  if (!visible.length) return null;
  return (
    <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
      <div style={{ color: C.bright, fontSize: 11, fontFamily: font, textTransform: "uppercase", marginBottom: 8 }}>{title}</div>
      <div style={{ display: "grid", gap: 8 }}>
        {visible.map(item => (
          <div key={item.label} style={{ display: "grid", gridTemplateColumns: "150px 1fr", gap: 8, alignItems: "start" }}>
            <div style={{ color: C.dim, fontSize: 10, fontFamily: font, textTransform: "uppercase" }}>{item.label}</div>
            <div style={{ color: item.color || C.text, fontSize: 12, lineHeight: 1.5 }}>{item.value}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function ArrayFlagGroup({ title, values, color }: { title: string; values?: string[] | null; color: string }) {
  const items = ensureArray(values);
  if (!items.length) return null;
  return (
    <div>
      <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase", marginBottom: 6 }}>{title}</div>
      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
        {items.map(item => (
          <Badge key={item} color={color} sm>{item}</Badge>
        ))}
      </div>
    </div>
  );
}

function TVChart({ symbol }: { symbol: string }) {
  const [ivl, setIvl] = useState("D");
  const ivls = [{ l: "1H", v: "60" }, { l: "4H", v: "240" }, { l: "1D", v: "D" }, { l: "1W", v: "W" }, { l: "1M", v: "M" }];
  return (
    <div style={{ margin: "12px 0" }}>
      <div style={{ display: "flex", gap: 4, marginBottom: 6, flexWrap: "wrap" }}>
        {ivls.map(iv => (
          <button
            key={iv.v}
            onClick={e => {
              e.stopPropagation();
              setIvl(iv.v);
            }}
            style={{
              padding: "2px 8px",
              fontSize: 9,
              fontWeight: 600,
              fontFamily: font,
              background: ivl === iv.v ? `${C.blue}20` : "transparent",
              color: ivl === iv.v ? C.blue : C.dim,
              border: `1px solid ${ivl === iv.v ? `${C.blue}40` : C.border}`,
              borderRadius: 3,
              cursor: "pointer",
            }}
          >
            {iv.l}
          </button>
        ))}
      </div>
      <div style={{ borderRadius: 8, overflow: "hidden", border: `1px solid ${C.border}` }}>
        <iframe
          src={`https://s.tradingview.com/widgetembed/?symbol=${encodeURIComponent(symbol)}&interval=${ivl}&theme=dark&style=1&locale=en&hide_top_toolbar=1&hide_side_toolbar=1&allow_symbol_change=0&save_image=0&width=100%25&height=220`}
          style={{ width: "100%", height: 220, border: "none", display: "block" }}
          title={`${symbol} chart`}
        />
      </div>
    </div>
  );
}

function Skeleton({ h = 16, mb = 8 }: { h?: number; mb?: number }) {
  return <div style={{ height: h, background: `${C.border}80`, borderRadius: 4, marginBottom: mb, animation: "pulse 1.5s ease-in-out infinite" }} />;
}

function SortIcon({ col, active, dir }: { col: string; active: string; dir: SortDir }) {
  if (col !== active) return <ArrowUpDown className="w-3 h-3" style={{ color: C.dim, opacity: 0.5 }} />;
  return dir === "asc" ? <ArrowUp className="w-3 h-3" style={{ color: C.blue }} /> : <ArrowDown className="w-3 h-3" style={{ color: C.blue }} />;
}

function useSortable<T extends Record<string, any>>(rows: T[], defaultCol: keyof T, defaultDir: SortDir = "desc") {
  const [col, setCol] = useState<keyof T>(defaultCol);
  const [dir, setDir] = useState<SortDir>(defaultDir);
  const toggle = (c: keyof T) => {
    if (c === col) setDir(d => d === "asc" ? "desc" : "asc");
    else {
      setCol(c);
      setDir("desc");
    }
  };
  const sorted = [...rows].sort((a, b) => {
    const av = a[col];
    const bv = b[col];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const cmp = typeof av === "number" && typeof bv === "number" ? av - bv : String(av).localeCompare(String(bv));
    return dir === "asc" ? cmp : -cmp;
  });
  return { sorted, col: col as string, dir, toggle: toggle as (c: string) => void };
}

function ContractsMini({ contracts, side }: { contracts: OptionContract[]; side: "call" | "put" }) {
  if (!contracts?.length) return null;
  const color = sideColor(side);
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ color, fontSize: 10, fontWeight: 700, fontFamily: font, textTransform: "uppercase", marginBottom: 6 }}>Legacy Top {side}s</div>
      <div style={{ background: C.cardAlt, border: `1px solid ${color}20`, borderRadius: 7, overflow: "hidden" }}>
        <div style={{ display: "grid", gridTemplateColumns: "70px 60px 70px 70px 55px 55px 50px", padding: "6px 10px", background: `${color}08`, fontSize: 9, fontFamily: font, textTransform: "uppercase", color: C.dim }}>
          <span>Strike</span><span>Expiry</span><span style={{ textAlign: "right" }}>Vol</span><span style={{ textAlign: "right" }}>OI</span><span style={{ textAlign: "right" }}>V/OI</span><span style={{ textAlign: "right" }}>IV</span><span style={{ textAlign: "right" }}>Δ</span>
        </div>
        {contracts.slice(0, 6).map((raw, i) => {
          const c = normalizeContract(raw);
          return (
            <div key={`${c.contract_symbol || c.symbol || i}`} style={{ display: "grid", gridTemplateColumns: "70px 60px 70px 70px 55px 55px 50px", padding: "5px 10px", borderTop: `1px solid ${C.border}`, fontSize: 11, fontFamily: font }}>
              <span style={{ color, fontWeight: 700 }}>${c.strike}</span>
              <span style={{ color: C.dim, fontSize: 10 }}>{compactDate(c.expiration)}</span>
              <span style={{ textAlign: "right", color: C.bright }}>{fmtVol(c.volume)}</span>
              <span style={{ textAlign: "right", color: C.text }}>{fmtVol(c.openInterest)}</span>
              <span style={{ textAlign: "right", color: voiColor(c.volumeToOi) }}>{c.volumeToOi != null ? `${fmtNum(c.volumeToOi, 1)}×` : "—"}</span>
              <span style={{ textAlign: "right", color: C.yellow }}>{c.iv != null ? fmtRatioPct(c.iv) : "—"}</span>
              <span style={{ textAlign: "right", color: C.text }}>{c.delta != null ? fmtNum(c.delta, 2) : "—"}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function TopContractsSection({ ticker, historyReady }: { ticker: TickerResult; historyReady: boolean | null | undefined }) {
  const primaryContracts = (ticker.top_contracts?.length ? ticker.top_contracts : [...(ticker.top_calls || []), ...(ticker.top_puts || [])]).slice(0, 6);
  if (!primaryContracts.length) {
    return (
      <div style={{ padding: 12, border: `1px dashed ${C.border}`, borderRadius: 8, color: C.dim, fontSize: 12 }}>
        No top contracts were returned for this ticker in the current scan.
      </div>
    );
  }

  return (
    <div style={{ display: "grid", gap: 10 }}>
      {primaryContracts.map((raw, index) => {
        const contract = normalizeContract(raw);
        const spreadWide = contract.spread_pct != null && contract.spread_pct > 15;
        const liquidityText = String(contract.contract_liquidity_quality || (spreadWide ? "wide spread" : contract.openInterest && contract.openInterest > 500 ? "strong liquidity" : "standard liquidity"));
        return (
          <div key={`${contract.contract_symbol || contract.symbol || index}`} style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start", marginBottom: 10, flexWrap: "wrap" }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                <div style={{ color: C.bright, fontFamily: font, fontWeight: 700, fontSize: 13 }}>{contract.underlying || ticker.ticker}</div>
                <Badge color={sideColor(contract.side)} sm>{contract.side || "contract"}</Badge>
                <Badge color={scoreColor(normalizeScore(contract.contract_score))} sm>Score {normalizeScore(contract.contract_score) != null ? fmtNum(normalizeScore(contract.contract_score), 0) : "—"}</Badge>
                {contract.contract_liquidity_quality ? <Badge color={liquidityText.toLowerCase().includes("strong") ? C.green : spreadWide ? C.red : C.blue} sm>{contract.contract_liquidity_quality}</Badge> : null}
                {spreadWide ? <Badge color={C.red} sm>Wide Spread</Badge> : null}
                {contract.repeated_flow_score != null ? <Badge color={historyReady ? C.purple : C.orange} sm>{historyReady ? `Repeated Flow ${fmtNum(normalizeScore(contract.repeated_flow_score), 0)}` : "Repeated Flow Limited"}</Badge> : null}
              </div>
              <div style={{ color: sideColor(contract.side), fontFamily: font, fontWeight: 700, fontSize: 13 }}>${contract.strike} · {compactDate(contract.expiration)}{contract.dte != null ? ` · ${contract.dte}D` : ""}</div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))", gap: 8 }}>
              <MetricBlock label="Bid / Ask" value={contract.bid != null && contract.ask != null ? `${fmtMoney(contract.bid)} / ${fmtMoney(contract.ask)}` : "—"} color={C.text} />
              <MetricBlock label="Last / Mid" value={`${fmtMoney(contract.last)} / ${fmtMoney(contract.mid)}`} color={C.bright} />
              <MetricBlock label="Volume / OI" value={`${fmtVol(contract.volume)} / ${fmtVol(contract.openInterest)}`} color={C.blue} subtext={contract.volumeToOi != null ? `V/OI ${fmtNum(contract.volumeToOi, 1)}×` : undefined} />
              <MetricBlock label="IV" value={contract.iv != null ? fmtRatioPct(contract.iv) : "—"} color={C.yellow} subtext={contract.iv_rank != null || contract.iv_percentile != null ? `IV Rank ${contract.iv_rank != null ? fmtNum(normalizeScore(contract.iv_rank), 0) : "—"} · IV %ile ${contract.iv_percentile != null ? fmtNum(normalizeScore(contract.iv_percentile), 0) : "—"}` : undefined} />
              <MetricBlock label="Greeks" value={contract.delta != null || contract.gamma != null ? `Δ ${fmtNum(contract.delta, 2)} · Γ ${fmtNum(contract.gamma, 3)}` : "Missing greeks"} color={contract.delta != null || contract.gamma != null ? C.green : C.dim} subtext={contract.theta != null || contract.vega != null ? `Θ ${fmtNum(contract.theta, 3)} · Vega ${fmtNum(contract.vega, 2)}` : undefined} />
              <MetricBlock label="Break-even" value={fmtMoney(contract.break_even)} color={C.orange} subtext={contract.break_even_distance_pct != null ? `Distance ${fmtSmartPct(contract.break_even_distance_pct)}` : undefined} />
              <MetricBlock label="Premium Traded" value={contract.premium_traded_estimate != null ? fmtMoney(contract.premium_traded_estimate, 0) : "—"} color={C.purple} subtext={contract.spread_pct != null ? `Spread ${fmtPlainPct(contract.spread_pct)}` : undefined} />
            </div>

            {(contract.short_thesis || contract.flow_score != null || contract.asymmetry_score != null) && (
              <div style={{ marginTop: 10, display: "grid", gap: 8 }}>
                {contract.short_thesis ? <div style={{ color: C.text, fontSize: 12, lineHeight: 1.6 }}>{contract.short_thesis}</div> : null}
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                  {contract.flow_score != null ? <Badge color={C.blue} sm>Flow {fmtNum(normalizeScore(contract.flow_score), 0)}</Badge> : null}
                  {contract.asymmetry_score != null ? <Badge color={C.green} sm>Asymmetry {fmtNum(normalizeScore(contract.asymmetry_score), 0)}</Badge> : null}
                  {contract.iv_rank != null && !historyReady ? <Badge color={C.orange} sm>History Limited</Badge> : null}
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function TickerDetailPanel({ symbol, ticker }: { symbol: string; ticker: TickerResult }) {
  const [technicals, setTechnicals] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [volumeSummary, setVolumeSummary] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    Promise.all([
      fetch(`${API_BASE}/technicals/${encodeURIComponent(symbol)}`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/history/${encodeURIComponent(symbol)}?limit=60`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`${API_BASE}/volume-summary/${encodeURIComponent(symbol)}?days=30`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([tech, hist, vol]) => {
      if (cancelled) return;
      setTechnicals(tech);
      setHistory(Array.isArray(hist?.bars || hist) ? (hist?.bars || hist) : []);
      setVolumeSummary(vol);
      setLoading(false);
    });
    return () => { cancelled = true; };
  }, [symbol]);

  const dataQuality = ticker.data_quality || {};
  const historyReady = dataQuality.history_metrics_ready;

  if (loading) {
    return (
      <div style={{ padding: 20, display: "flex", alignItems: "center", gap: 8, color: C.dim, fontSize: 11, fontFamily: font }}>
        <Loader2 className="w-3 h-3 animate-spin" /> Loading technicals & history for {symbol}...
      </div>
    );
  }

  const smaData = technicals?.sma_20 || technicals?.sma_50 ? (() => {
    const sma20List = Array.isArray(technicals?.sma_20) ? technicals.sma_20 : technicals?.sma_20 ? [technicals.sma_20] : [];
    const sma50List = Array.isArray(technicals?.sma_50) ? technicals.sma_50 : technicals?.sma_50 ? [technicals.sma_50] : [];
    const dateMap: Record<string, any> = {};
    sma20List.forEach((d: any) => { dateMap[d.date] = { ...dateMap[d.date], date: d.date, sma20: d.value }; });
    sma50List.forEach((d: any) => { dateMap[d.date] = { ...dateMap[d.date], date: d.date, sma50: d.value }; });
    return Object.values(dateMap).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })() : [];
  const rsiData = (() => {
    const rsiList = Array.isArray(technicals?.rsi_14) ? technicals.rsi_14 : technicals?.rsi_14 ? [technicals.rsi_14] : [];
    return rsiList.map((d: any) => ({ date: d.date, rsi: d.value })).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })();
  const macdData = (() => {
    const macdList = Array.isArray(technicals?.macd) ? technicals.macd : technicals?.macd ? [technicals.macd] : [];
    return macdList.map((d: any) => ({ date: d.date, macd: d.value, signal: d.signal, histogram: d.histogram })).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })();
  const volumeChartData = history.slice(-30).map((bar: any) => ({
    date: bar.date || bar.day,
    callVol: bar.call_volume || 0,
    putVol: bar.put_volume || 0,
    pcRatio: bar.call_volume && bar.put_volume ? bar.put_volume / bar.call_volume : null,
  }));
  const chartStyle = { background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 8px", marginBottom: 10 };
  const chartLabel = (text: string) => <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6, paddingLeft: 4 }}>{text}</div>;
  const thesisItems = ensureArray(ticker.thesis);
  const riskItems = ensureArray(ticker.risks);

  return (
    <div style={{ display: "grid", gap: 12 }} onClick={e => e.stopPropagation()}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 12 }}>
        <DetailList
          title="Signal thesis"
          items={[
            { label: "Primary signal", value: ticker.primary_signal || "—", color: getSignalColor(ticker.primary_signal) },
            { label: "Thesis", value: thesisItems.length ? thesisItems.join(" · ") : "—" },
            { label: "Risks", value: riskItems.length ? riskItems.join(" · ") : "—", color: riskItems.length ? C.orange : C.text },
            { label: "Expiration focus", value: fmtMaybeText(ticker.expiration_focus) },
          ]}
        />
        <DetailList
          title="Stock context"
          items={[
            { label: "Relative volume", value: ticker.stock_context?.stock_relative_volume != null ? `${fmtNum(ticker.stock_context.stock_relative_volume, 2)}×` : "—", color: C.blue },
            { label: "Intraday move", value: fmtSmartPct(ticker.stock_context?.stock_intraday_move_pct), color: C.text },
            { label: "Breakout", value: ticker.stock_context?.breakout_context || "—" },
            { label: "Compression", value: ticker.stock_context?.compression_context || "—" },
            { label: "Reversal", value: ticker.stock_context?.reversal_context || "—" },
            { label: "Catalyst", value: ticker.stock_context?.catalyst_context || "—" },
            { label: "Liquidity", value: ticker.stock_context?.liquidity_context || "—" },
            { label: "Short squeeze", value: ticker.stock_context?.short_squeeze_context || "—" },
            { label: "Macro", value: ticker.stock_context?.macro_context || "—" },
          ]}
        />
        <DetailList
          title="Options context"
          items={[
            { label: "Call/put volume", value: ticker.options_context?.call_put_volume_ratio != null ? `${fmtNum(ticker.options_context.call_put_volume_ratio, 2)}×` : "—", color: C.green },
            { label: "Call/put OI", value: ticker.options_context?.call_put_oi_ratio != null ? `${fmtNum(ticker.options_context.call_put_oi_ratio, 2)}×` : "—", color: C.blue },
            { label: "Near-spot OI density", value: fmtNum(ticker.options_context?.near_spot_oi_density, 2) },
            { label: "Near-spot gamma density", value: fmtNum(ticker.options_context?.near_spot_gamma_density, 2) },
            { label: "Current IV", value: ticker.options_context?.iv_current != null ? fmtRatioPct(ticker.options_context.iv_current) : "—", color: C.yellow },
            { label: "Expected move", value: fmtSmartPct(ticker.options_context?.expected_move_from_atm_straddle), color: C.orange },
            { label: "Gamma label", value: ticker.options_context?.gamma_score_is_approximation ? "Approximation used" : "Direct metric", color: ticker.options_context?.gamma_score_is_approximation ? C.orange : C.green },
          ]}
        />
        <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: 12 }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10, gap: 8, flexWrap: "wrap" }}>
            <div style={{ color: C.bright, fontSize: 11, fontFamily: font, textTransform: "uppercase" }}>Data quality</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              <Badge color={getConfidence(dataQuality.confidence, dataQuality.confidence_score).color} sm>{getConfidence(dataQuality.confidence, dataQuality.confidence_score).label} confidence</Badge>
              <Badge color={historyReady ? C.green : C.orange} sm>{historyReady ? "History Ready" : "History Limited"}</Badge>
            </div>
          </div>
          <ArrayFlagGroup title="Flags" values={dataQuality.flags} color={C.blue} />
          <ArrayFlagGroup title="Missing data" values={dataQuality.missing_data_flags} color={C.red} />
          <ArrayFlagGroup title="Approximate metrics" values={dataQuality.approximate_metrics} color={C.orange} />
          {!ensureArray(dataQuality.flags).length && !ensureArray(dataQuality.missing_data_flags).length && !ensureArray(dataQuality.approximate_metrics).length ? (
            <div style={{ color: C.dim, fontSize: 12 }}>No explicit quality flags returned.</div>
          ) : null}
        </div>
      </div>

      <div>
        <div style={{ color: C.bright, fontSize: 11, fontFamily: font, textTransform: "uppercase", marginBottom: 8 }}>Top contract ideas</div>
        <TopContractsSection ticker={ticker} historyReady={historyReady} />
      </div>

      <TVChart symbol={ticker.ticker} />

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        {smaData.length > 1 && (
          <div style={chartStyle}>
            {chartLabel("SMA 20 / 50")}
            <ResponsiveContainer width="100%" height={140}>
              <LineChart data={smaData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
                <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={["auto", "auto"]} />
                <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                <Line type="monotone" dataKey="sma20" stroke={C.blue} strokeWidth={1.5} dot={false} name="SMA 20" />
                <Line type="monotone" dataKey="sma50" stroke={C.orange} strokeWidth={1.5} dot={false} name="SMA 50" />
                <Legend iconSize={8} wrapperStyle={{ fontSize: 9, fontFamily: font }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {rsiData.length > 1 && (
          <div style={chartStyle}>
            {chartLabel("RSI (14)")}
            <ResponsiveContainer width="100%" height={140}>
              <ComposedChart data={rsiData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
                <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={[0, 100]} />
                <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                <ReferenceLine y={70} stroke={C.red} strokeDasharray="3 3" strokeOpacity={0.6} />
                <ReferenceLine y={30} stroke={C.green} strokeDasharray="3 3" strokeOpacity={0.6} />
                <Area type="monotone" dataKey="rsi" fill={`${C.purple}15`} stroke={C.purple} strokeWidth={1.5} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {macdData.length > 1 && (
          <div style={chartStyle}>
            {chartLabel("MACD")}
            <ResponsiveContainer width="100%" height={140}>
              <ComposedChart data={macdData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
                <YAxis tick={{ fontSize: 8, fill: C.dim }} />
                <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                <Bar dataKey="histogram" fill={C.blue} opacity={0.5} name="Histogram" />
                <Line type="monotone" dataKey="macd" stroke={C.blue} strokeWidth={1.5} dot={false} name="MACD" />
                <Line type="monotone" dataKey="signal" stroke={C.orange} strokeWidth={1.5} dot={false} name="Signal" />
                <Legend iconSize={8} wrapperStyle={{ fontSize: 9, fontFamily: font }} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        )}

        {volumeChartData.length > 0 && (
          <div style={chartStyle}>
            {chartLabel("Daily Options Volume (30d)")}
            <ResponsiveContainer width="100%" height={140}>
              <BarChart data={volumeChartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
                <YAxis tick={{ fontSize: 8, fill: C.dim }} />
                <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                <Bar dataKey="callVol" fill={C.green} opacity={0.8} name="Call Vol" />
                <Bar dataKey="putVol" fill={C.red} opacity={0.8} name="Put Vol" />
                <Legend iconSize={8} wrapperStyle={{ fontSize: 9, fontFamily: font }} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {volumeChartData.filter(d => d.pcRatio != null).length > 1 && (
          <div style={chartStyle}>
            {chartLabel("Put/Call Ratio Trend (30d)")}
            <ResponsiveContainer width="100%" height={140}>
              <LineChart data={volumeChartData.filter(d => d.pcRatio != null)} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
                <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={["auto", "auto"]} />
                <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                <ReferenceLine y={1} stroke={C.yellow} strokeDasharray="3 3" strokeOpacity={0.5} />
                <Line type="monotone" dataKey="pcRatio" stroke={C.yellow} strokeWidth={1.5} dot={false} name="P/C Ratio" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {volumeSummary && (
          <div style={chartStyle}>
            {chartLabel("30-Day Volume Summary")}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 6, padding: "4px 4px" }}>
              {[
                { label: "Call Total Vol", value: fmtVol(volumeSummary.call_total_volume), color: C.green },
                { label: "Put Total Vol", value: fmtVol(volumeSummary.put_total_volume), color: C.red },
                { label: "Call Avg Daily", value: fmtVol(volumeSummary.call_avg_daily_vol), color: C.green },
                { label: "Put Avg Daily", value: fmtVol(volumeSummary.put_avg_daily_vol), color: C.red },
                { label: "Call Contracts", value: fmtVol(volumeSummary.call_unique_contracts), color: C.blue },
                { label: "Put Contracts", value: fmtVol(volumeSummary.put_unique_contracts), color: C.purple },
              ].map(s => (
                <div key={s.label} style={{ padding: "5px 8px", background: `${s.color}08`, borderRadius: 5, border: `1px solid ${s.color}15` }}>
                  <div style={{ color: C.dim, fontSize: 8, fontFamily: font, textTransform: "uppercase", marginBottom: 2 }}>{s.label}</div>
                  <div style={{ color: s.color, fontSize: 13, fontWeight: 700, fontFamily: font }}>{s.value}</div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {smaData.length <= 1 && rsiData.length <= 1 && macdData.length <= 1 && volumeChartData.length === 0 && !volumeSummary && (
        <div style={{ gridColumn: "1 / -1", padding: "16px 0", color: C.dim, fontSize: 11, fontFamily: font, textAlign: "center" }}>
          <Activity className="w-4 h-4 inline-block" style={{ marginRight: 6 }} />
          Technical data not yet available — enrichment ingestion may still be in progress.
        </div>
      )}

      {!!ticker.top_calls?.length || !!ticker.top_puts?.length ? (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 4 }}>
          <ContractsMini contracts={ticker.top_calls || []} side="call" />
          <ContractsMini contracts={ticker.top_puts || []} side="put" />
        </div>
      ) : null}

      <TimeSalesPanel symbol={symbol} />
    </div>
  );
}

function DataIngestionWidget() {
  const [summary, setSummary] = useState<any>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/ingestion-summary`, { headers: authHeaders() });
      if (res.ok) setSummary(await res.json());
    } catch { /* ignore */ } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open && !summary) fetchStatus();
  }, [open, summary, fetchStatus]);

  const tickersIngested = summary?.tickers_ingested ?? "?";
  const tickersTotal = summary?.tickers_total ?? "?";
  const barsStored = summary?.total_bars ?? "?";
  const lastUpdated = summary?.last_updated;
  const formattedTime = lastUpdated ? new Date(lastUpdated).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }) : "unknown";

  return (
    <div style={{ marginBottom: 12 }}>
      <button onClick={() => setOpen(o => !o)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "5px 12px", fontSize: 10, fontWeight: 600, fontFamily: font, background: open ? `${C.purple}15` : "transparent", color: open ? C.purple : C.dim, border: `1px solid ${open ? `${C.purple}40` : C.border}`, borderRadius: 6, cursor: "pointer" }}>
        <Database className="w-3 h-3" />
        Ingestion Status
        {open ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
      </button>
      {open && (
        <div style={{ marginTop: 8, background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: "12px 16px", animation: "fadeIn 0.2s ease" }}>
          {loading ? (
            <div style={{ color: C.dim, fontSize: 11, fontFamily: font, display: "flex", alignItems: "center", gap: 6 }}>
              <Loader2 className="w-3 h-3 animate-spin" /> Fetching ingestion status...
            </div>
          ) : (
            <div style={{ display: "flex", gap: 20, alignItems: "center", flexWrap: "wrap" }}>
              <div style={{ color: C.text, fontSize: 12, fontFamily: font }}>
                <span style={{ color: C.bright, fontWeight: 700 }}>{tickersIngested}</span>
                <span style={{ color: C.dim }}> / {tickersTotal} tickers ingested</span>
              </div>
              <div style={{ color: C.text, fontSize: 12, fontFamily: font }}>
                <span style={{ color: C.blue, fontWeight: 700 }}>{typeof barsStored === "number" ? barsStored.toLocaleString() : barsStored}</span>
                <span style={{ color: C.dim }}> bars stored</span>
              </div>
              <div style={{ color: C.dim, fontSize: 11, fontFamily: font }}>
                Last updated: <span style={{ color: C.text }}>{formattedTime}</span>
              </div>
              <button onClick={fetchStatus} style={{ padding: "3px 8px", fontSize: 9, fontFamily: font, background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 4, color: C.blue, cursor: "pointer" }}>
                <RefreshCw className="w-3 h-3 inline-block" />
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TickerRows({ t, index, isExp, onToggle }: { t: TickerResult; index: number; isExp: boolean; onToggle: () => void }) {
  const confidence = getConfidence(t.confidence || t.data_quality?.confidence, t.confidence_score ?? t.data_quality?.confidence_score ?? null);
  const signalColor = getSignalColor(t.primary_signal);
  const tags = signalTagsForTicker(t);
  const modular = t.modular_scores || {};
  return (
    <Fragment>
      <tr onClick={onToggle} style={{ borderTop: `1px solid ${C.border}`, cursor: "pointer", background: isExp ? `${C.blue}06` : "transparent", verticalAlign: "top", transition: "opacity 0.15s ease, transform 0.2s ease" }}>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 6 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              <span style={{ color: C.dim, fontSize: 11, fontFamily: font }}>#{index + 1}</span>
              <span style={{ color: C.bright, fontFamily: font, fontWeight: 800, fontSize: 14 }}>{t.ticker}</span>
              {t.category ? <Badge color={t.category === "etf" ? C.purple : C.blue} sm>{t.category}</Badge> : null}
            </div>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", fontFamily: font, fontSize: 11 }}>
              <span style={{ color: C.bright }}>{fmtMoney(t.underlying_price)}</span>
              <span style={{ color: (safeNum(t.price_change_pct) ?? 0) >= 0 ? C.green : C.red }}>{fmtSmartPct(t.price_change_pct)}</span>
            </div>
            {t.expiration_focus?.length ? <div style={{ color: C.dim, fontSize: 10 }}>Focus: {fmtMaybeText(t.expiration_focus)}</div> : null}
          </div>
        </td>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {t.primary_signal ? <Badge color={signalColor}>{t.primary_signal}</Badge> : <Badge color={C.dim}>No primary signal</Badge>}
              <Badge color={confidence.color}>{confidence.label} confidence</Badge>
              {t.options_context?.gamma_score_is_approximation ? <Badge color={C.orange}>Gamma Approx.</Badge> : null}
            </div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {tags.length ? tags.map(tag => <Badge key={tag.label} color={tag.color} sm>{tag.label}</Badge>) : <span style={{ color: C.dim, fontSize: 11 }}>No secondary tags</span>}
            </div>
          </div>
        </td>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(72px, 1fr))", gap: 8 }}>
              <MetricBlock label="Total Vol" value={fmtVol(t.total_volume)} color={C.bright} />
              <MetricBlock label="P/C Ratio" value={t.pc_ratio != null ? fmtNum(t.pc_ratio, 2) : "—"} color={pcColor(t.pc_ratio ?? null)} />
              <MetricBlock label="Calls" value={fmtVol(t.call_volume)} color={C.green} />
              <MetricBlock label="Puts" value={fmtVol(t.put_volume)} color={C.red} />
            </div>
          </div>
        </td>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 10 }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
              <span style={{ color: scoreColor(normalizeScore(t.composite_score)), fontFamily: font, fontSize: 24, fontWeight: 800 }}>{normalizeScore(t.composite_score) != null ? fmtNum(normalizeScore(t.composite_score), 0) : "—"}</span>
              <span style={{ color: C.dim, fontSize: 10, fontFamily: font, textTransform: "uppercase" }}>Composite</span>
            </div>
            <div style={{ display: "grid", gap: 8 }}>
              <ScoreBar label="Flow" value={modular.flow_score} />
              <ScoreBar label="Gamma" value={modular.gamma_score} />
              <ScoreBar label="Asymmetry" value={modular.asymmetry_score} />
              <ScoreBar label="Volatility" value={modular.volatility_score} />
              <ScoreBar label="Sentiment" value={modular.sentiment_score} />
              <ScoreBar label="Stock Context" value={modular.stock_context_score} />
            </div>
          </div>
        </td>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ color: C.text, fontSize: 12, lineHeight: 1.55 }}>{t.stock_context_summary || "No stock context summary returned."}</div>
            <div style={{ color: C.text, fontSize: 12, lineHeight: 1.55 }}>{t.options_context_summary || "No options context summary returned."}</div>
            <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
              {ensureArray(t.data_quality?.missing_data_flags).slice(0, 2).map(item => <Badge key={item} color={C.red} sm>{item}</Badge>)}
              {ensureArray(t.data_quality?.approximate_metrics).slice(0, 2).map(item => <Badge key={item} color={C.orange} sm>{item}</Badge>)}
            </div>
          </div>
        </td>
        <td style={{ padding: "12px 10px" }}>
          <div style={{ display: "grid", gap: 6, fontFamily: font, fontSize: 11 }}>
            <div style={{ color: C.text }}>Rel Vol <span style={{ color: C.blue }}>{t.stock_context?.stock_relative_volume != null ? `${fmtNum(t.stock_context.stock_relative_volume, 2)}×` : "—"}</span></div>
            <div style={{ color: C.text }}>Vol Ratio <span style={{ color: C.green }}>{t.options_context?.call_put_volume_ratio != null ? `${fmtNum(t.options_context.call_put_volume_ratio, 2)}×` : "—"}</span></div>
            <div style={{ color: C.text }}>OI Ratio <span style={{ color: C.blue }}>{t.options_context?.call_put_oi_ratio != null ? `${fmtNum(t.options_context.call_put_oi_ratio, 2)}×` : "—"}</span></div>
            <div style={{ color: C.text }}>IV <span style={{ color: C.yellow }}>{t.options_context?.iv_current != null ? fmtRatioPct(t.options_context.iv_current) : "—"}</span></div>
            <div style={{ color: C.text }}>Exp Move <span style={{ color: C.orange }}>{fmtSmartPct(t.options_context?.expected_move_from_atm_straddle)}</span></div>
          </div>
        </td>
        <td style={{ padding: "12px 10px", textAlign: "right" }}>
          {isExp ? <ChevronUp className="w-3 h-3" style={{ color: C.dim }} /> : <ChevronDown className="w-3 h-3" style={{ color: C.dim }} />}
        </td>
      </tr>
      {isExp && (
        <tr>
          <td colSpan={7} style={{ padding: "14px 16px", background: C.cardAlt, borderTop: `1px solid ${C.border}` }} onClick={e => e.stopPropagation()}>
            <TickerDetailPanel symbol={t.ticker} ticker={t} />
          </td>
        </tr>
      )}
    </Fragment>
  );
}

function TickerSummaryTab({ tickers }: { tickers: TickerResult[] }) {
  const [catFilter, setCatFilter] = useState<CatFilter>("all");
  const [expanded, setExpanded] = useState<string | null>(null);

  const filtered = useMemo(() => {
    return tickers
      .filter(t => catFilter === "all" || t.category === catFilter)
      .map(t => ({
        ...t,
        _rsi: t.technicals?.rsi_14?.value ?? null,
        _trend: (() => {
          const s = trendSignal(t.technicals?.sma_20?.value, t.technicals?.sma_50?.value);
          return s ? (s.label === "Bullish" ? 1 : 0) : null;
        })(),
        _macd: t.technicals?.macd?.histogram ?? null,
        _histVol: ((t.historic_volume?.call_total_volume ?? 0) + (t.historic_volume?.put_total_volume ?? 0)) || null,
        _composite: normalizeScore(t.composite_score) ?? -1,
      }))
      .sort((a, b) => (b._composite ?? -1) - (a._composite ?? -1));
  }, [tickers, catFilter]);

  const TH = ({ label, width, right }: { label: string; width?: string | number; right?: boolean }) => (
    <th style={{ padding: "8px 10px", width, textAlign: right ? "right" : "left", fontSize: 9, fontFamily: font, textTransform: "uppercase", color: C.dim, whiteSpace: "nowrap" }}>{label}</th>
  );

  return (
    <div>
      <div style={{ display: "flex", gap: 6, marginBottom: 12, flexWrap: "wrap", alignItems: "center" }}>
        {(["all", "stock", "etf"] as CatFilter[]).map(f => (
          <button
            key={f}
            onClick={() => {
              setCatFilter(f);
              setExpanded(null);
            }}
            style={{ padding: "5px 14px", fontSize: 11, fontWeight: 600, fontFamily: font, background: catFilter === f ? `${C.blue}18` : "transparent", color: catFilter === f ? C.blue : C.dim, border: `1px solid ${catFilter === f ? `${C.blue}40` : C.border}`, borderRadius: 6, cursor: "pointer" }}
          >
            {f === "all" ? "All" : f === "stock" ? "Stocks" : "ETFs"}
          </button>
        ))}
        <span style={{ marginLeft: "auto", color: C.dim, fontSize: 11, fontFamily: font, alignSelf: "center" }}>{filtered.length} ranked tickers</span>
      </div>

      <SectionCard>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: `${C.border}50` }}>
                <TH label="# / Ticker" width={170} />
                <TH label="Signal / Confidence" width={260} />
                <TH label="Price / Flow" width={190} />
                <TH label="Composite + Modular Scores" width={300} />
                <TH label="Why It Ranks" />
                <TH label="Quick Metrics" width={220} />
                <th style={{ padding: "8px 10px", width: 30 }} />
              </tr>
            </thead>
            <tbody>
              {filtered.map((t, index) => (
                <TickerRows
                  key={t.ticker}
                  t={t}
                  index={index}
                  isExp={expanded === t.ticker}
                  onToggle={() => setExpanded(expanded === t.ticker ? null : t.ticker)}
                />
              ))}
            </tbody>
          </table>
        </div>
        {filtered.length === 0 && <div style={{ padding: 40, textAlign: "center", color: C.dim, fontSize: 13, fontFamily: sans }}>No tickers found for the selected filter.</div>}
      </SectionCard>
    </div>
  );
}

function FlowTab({ contracts, onContractClick }: { contracts: OptionContract[]; onContractClick?: (occSymbol: string) => void }) {
  const [catFilter, setCatFilter] = useState<CatFilter>("all");
  const [sideFilter, setSideFilter] = useState<SideFilter>("all");
  const [unusualOnly, setUnusualOnly] = useState(false);
  const [limit, setLimit] = useState(100);

  const normalizedContracts = useMemo(() => contracts.map(raw => {
    const c = normalizeContract(raw);
    return {
      ...c,
      category: c.category || "stock",
      contract_score_sort: normalizeScore(c.contract_score) ?? -1,
      flow_score_sort: normalizeScore(c.flow_score) ?? -1,
      asymmetry_score_sort: normalizeScore(c.asymmetry_score) ?? -1,
      premium_traded_estimate_sort: c.premium_traded_estimate ?? -1,
      option_volume_to_oi_ratio_sort: c.volumeToOi ?? -1,
      spread_pct_sort: c.spread_pct ?? -1,
      volume_sort: c.volume ?? -1,
      open_interest_sort: c.openInterest ?? -1,
      implied_volatility_sort: c.iv ?? -1,
      break_even_distance_pct_sort: c.break_even_distance_pct ?? -1,
      confidence_sort: normalizeScore(c.confidence ? ({ high: 90, medium: 60, low: 25 } as Record<string, number>)[c.confidence.toLowerCase()] : null) ?? -1,
      primary_signal_sort: c.primary_signal || "",
      underlying_sort: c.underlying || "",
    };
  }), [contracts]);

  const filtered = normalizedContracts.filter(c => {
    if (catFilter !== "all" && c.category !== catFilter) return false;
    if (sideFilter !== "all" && c.side !== sideFilter) return false;
    if (unusualOnly && (c.volumeToOi == null || c.volumeToOi < 3)) return false;
    return true;
  });
  const { sorted, col, dir, toggle } = useSortable(filtered, "contract_score_sort");
  const visible = sorted.slice(0, limit);

  const TH = ({ c, label, right }: { c: string; label: string; right?: boolean }) => (
    <th onClick={() => toggle(c)} style={{ padding: "7px 8px", textAlign: right ? "right" : "left", fontSize: 9, fontFamily: font, textTransform: "uppercase", color: col === c ? C.blue : C.dim, cursor: "pointer", whiteSpace: "nowrap", userSelect: "none" }}>
      <span style={{ display: "inline-flex", alignItems: "center", gap: 3 }}>{label} <SortIcon col={c} active={col} dir={dir} /></span>
    </th>
  );

  return (
    <div>
      <div style={{ display: "flex", gap: 6, marginBottom: 12, flexWrap: "wrap", alignItems: "center" }}>
        <div style={{ display: "flex", gap: 4 }}>
          {(["all", "stock", "etf"] as CatFilter[]).map(f => (
            <button key={f} onClick={() => setCatFilter(f)} style={{ padding: "4px 12px", fontSize: 10, fontWeight: 600, fontFamily: font, background: catFilter === f ? `${C.blue}18` : "transparent", color: catFilter === f ? C.blue : C.dim, border: `1px solid ${catFilter === f ? `${C.blue}40` : C.border}`, borderRadius: 5, cursor: "pointer" }}>
              {f === "all" ? "All" : f === "stock" ? "Stocks" : "ETFs"}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", gap: 4 }}>
          {(["all", "call", "put"] as SideFilter[]).map(f => (
            <button key={f} onClick={() => setSideFilter(f)} style={{ padding: "4px 12px", fontSize: 10, fontWeight: 600, fontFamily: font, background: sideFilter === f ? `${sideColor(f === "all" ? "call" : f)}18` : "transparent", color: sideFilter === f ? sideColor(f === "all" ? "call" : f) : C.dim, border: `1px solid ${sideFilter === f ? `${sideColor(f === "all" ? "call" : f)}40` : C.border}`, borderRadius: 5, cursor: "pointer" }}>
              {f === "all" ? "Both" : f === "call" ? "Calls" : "Puts"}
            </button>
          ))}
        </div>
        <button onClick={() => setUnusualOnly(u => !u)} style={{ padding: "4px 12px", fontSize: 10, fontWeight: 600, fontFamily: font, background: unusualOnly ? `${C.orange}18` : "transparent", color: unusualOnly ? C.orange : C.dim, border: `1px solid ${unusualOnly ? `${C.orange}40` : C.border}`, borderRadius: 5, cursor: "pointer" }}>
          V/OI &gt; 3×
        </button>
        <span style={{ marginLeft: "auto", color: C.dim, fontSize: 11, fontFamily: font }}>{filtered.length} contracts</span>
      </div>

      <SectionCard>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: `${C.border}50` }}>
                <TH c="underlying_sort" label="Ticker" />
                <TH c="side" label="Side" />
                <TH c="contract_score_sort" label="Contract Score" right />
                <TH c="flow_score_sort" label="Flow" right />
                <TH c="asymmetry_score_sort" label="Asymmetry" right />
                <TH c="premium_traded_estimate_sort" label="Premium" right />
                <TH c="option_volume_to_oi_ratio_sort" label="V/OI" right />
                <TH c="spread_pct_sort" label="Spread %" right />
                <TH c="volume_sort" label="Volume" right />
                <TH c="open_interest_sort" label="OI" right />
                <TH c="implied_volatility_sort" label="IV" right />
                <TH c="break_even_distance_pct_sort" label="BE Dist" right />
                <TH c="primary_signal_sort" label="Signal" />
                <TH c="confidence_sort" label="Conf." right />
              </tr>
            </thead>
            <tbody>
              {visible.map((c, i) => {
                const confidence = getConfidence(c.confidence, null);
                return (
                  <tr key={`${c.contract_symbol || c.symbol || i}`} style={{ borderTop: `1px solid ${C.border}`, cursor: onContractClick ? "pointer" : undefined }} onClick={() => { const sym = c.contract_symbol || c.symbol; if (onContractClick && sym) onContractClick(sym); }}>
                    <td style={{ padding: "8px 8px" }}>
                      <div style={{ fontFamily: font, fontWeight: 700, fontSize: 12, color: C.bright }}>{c.underlying || "—"}</div>
                      <div style={{ color: onContractClick ? C.blue : C.dim, fontSize: 10, textDecoration: onContractClick ? "underline" : "none" }}>{c.symbol || c.contract_symbol || ""}</div>
                    </td>
                    <td style={{ padding: "8px 8px" }}><Badge color={sideColor(c.side)} sm>{c.side || "—"}</Badge></td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: scoreColor(normalizeScore(c.contract_score)), fontWeight: 700 }}>{normalizeScore(c.contract_score) != null ? fmtNum(normalizeScore(c.contract_score), 0) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.blue }}>{normalizeScore(c.flow_score) != null ? fmtNum(normalizeScore(c.flow_score), 0) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.green }}>{normalizeScore(c.asymmetry_score) != null ? fmtNum(normalizeScore(c.asymmetry_score), 0) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.purple }}>{c.premium_traded_estimate != null ? fmtMoney(c.premium_traded_estimate, 0) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: voiColor(c.volumeToOi) }}>{c.volumeToOi != null ? `${fmtNum(c.volumeToOi, 1)}×` : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: c.spread_pct != null && c.spread_pct > 15 ? C.red : C.text }}>{c.spread_pct != null ? fmtPlainPct(c.spread_pct) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.bright }}>{fmtVol(c.volume)}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.text }}>{fmtVol(c.openInterest)}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.yellow }}>{c.iv != null ? fmtRatioPct(c.iv) : "—"}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right", fontFamily: font, color: C.orange }}>{fmtSmartPct(c.break_even_distance_pct)}</td>
                    <td style={{ padding: "8px 8px" }}>{c.primary_signal ? <Badge color={getSignalColor(c.primary_signal)} sm>{c.primary_signal}</Badge> : <span style={{ color: C.dim, fontSize: 11 }}>—</span>}</td>
                    <td style={{ padding: "8px 8px", textAlign: "right" }}>{c.confidence ? <Badge color={confidence.color} sm>{confidence.label}</Badge> : <span style={{ color: C.dim, fontSize: 11 }}>—</span>}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {visible.length === 0 && <div style={{ padding: 40, textAlign: "center", color: C.dim, fontSize: 13, fontFamily: sans }}>No contracts match your filters.</div>}
        {filtered.length > limit && (
          <div style={{ borderTop: `1px solid ${C.border}`, padding: "10px 16px", display: "flex", justifyContent: "center" }}>
            <button onClick={() => setLimit(l => l + 100)} style={{ padding: "6px 18px", background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 6, color: C.blue, fontSize: 11, fontFamily: font, cursor: "pointer" }}>
              Show more ({filtered.length - limit} remaining)
            </button>
          </div>
        )}
      </SectionCard>
    </div>
  );
}

/* ── Contract Detail Modal (Tradier-only) ── */
function ContractDetailModal({ occSymbol, onClose }: { occSymbol: string; onClose: () => void }) {
  const [detail, setDetail] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    fetch(`${API_BASE}/contract-detail/${encodeURIComponent(occSymbol)}`, { headers: authHeaders() })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(json => { if (!cancelled) setDetail(json); })
      .catch(e => { if (!cancelled) setError(e.message); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [occSymbol]);

  const quote = detail?.quote;
  const history = detail?.history?.bars || detail?.history || [];
  const timesales = detail?.timesales?.ticks || detail?.timesales || [];

  return (
    <div style={{ position: "fixed", inset: 0, zIndex: 1000, display: "flex", alignItems: "center", justifyContent: "center" }} onClick={onClose}>
      <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,0.7)" }} />
      <div style={{ position: "relative", background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, width: "90%", maxWidth: 800, maxHeight: "85vh", overflow: "auto", padding: 0 }} onClick={e => e.stopPropagation()}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 18px", borderBottom: `1px solid ${C.border}`, position: "sticky", top: 0, background: C.bg, zIndex: 1 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <Eye className="w-4 h-4" style={{ color: C.blue }} />
            <span style={{ color: C.bright, fontSize: 14, fontWeight: 800, fontFamily: font }}>{occSymbol}</span>
          </div>
          <button onClick={onClose} style={{ background: "none", border: "none", color: C.dim, cursor: "pointer", padding: 4 }}><X className="w-4 h-4" /></button>
        </div>

        <div style={{ padding: "16px 18px" }}>
          {loading && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, color: C.dim, fontSize: 12, fontFamily: font, padding: "30px 0", justifyContent: "center" }}>
              <Loader2 className="w-4 h-4 animate-spin" /> Loading contract detail...
            </div>
          )}
          {error && <div style={{ color: C.red, fontSize: 12, padding: "20px 0", textAlign: "center" }}>{error}</div>}

          {!loading && !error && detail && (
            <div style={{ display: "grid", gap: 16 }}>
              {/* Quote */}
              {quote && (
                <div>
                  <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase", marginBottom: 8 }}>Live Quote</div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(110px, 1fr))", gap: 8 }}>
                    <MetricBlock label="Last" value={fmtMoney(quote.last)} color={C.bright} />
                    <MetricBlock label="Bid / Ask" value={quote.bid != null && quote.ask != null ? `${fmtMoney(quote.bid)} / ${fmtMoney(quote.ask)}` : "—"} color={C.text} />
                    <MetricBlock label="Volume" value={fmtVol(quote.volume)} color={C.blue} />
                    <MetricBlock label="Open Int" value={fmtVol(quote.open_interest)} color={C.text} />
                    <MetricBlock label="Change" value={quote.change_percentage != null ? `${safeNum(quote.change_percentage)! >= 0 ? "+" : ""}${fmtNum(quote.change_percentage, 2)}%` : "—"} color={safeNum(quote.change_percentage) != null ? (safeNum(quote.change_percentage)! >= 0 ? C.green : C.red) : C.dim} />
                    {quote.greeks?.mid_iv != null && <MetricBlock label="Mid IV" value={fmtRatioPct(quote.greeks.mid_iv)} color={C.yellow} />}
                    {quote.greeks?.smv_vol != null && <MetricBlock label="SMV Vol" value={fmtRatioPct(quote.greeks.smv_vol)} color={C.orange} />}
                    {quote.greeks?.delta != null && <MetricBlock label="Greeks" value={`\u0394 ${fmtNum(quote.greeks.delta, 3)} \u00B7 \u0393 ${fmtNum(quote.greeks.gamma, 4)}`} color={C.green} subtext={`\u0398 ${fmtNum(quote.greeks.theta, 4)} \u00B7 \u03BD ${fmtNum(quote.greeks.vega, 3)}${quote.greeks.rho != null ? ` \u00B7 \u03C1 ${fmtNum(quote.greeks.rho, 4)}` : ""}`} />}
                  </div>
                </div>
              )}

              {/* Price History Chart */}
              {history.length > 1 && (
                <div>
                  <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase", marginBottom: 8 }}>90-Day Price History</div>
                  <div style={{ background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 8px" }}>
                    <ResponsiveContainer width="100%" height={180}>
                      <ComposedChart data={history.slice(-90)} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                        <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v?.slice(5) || ""} />
                        <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={["auto", "auto"]} />
                        <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                        <Area type="monotone" dataKey="close" fill={`${C.blue}15`} stroke={C.blue} strokeWidth={1.5} name="Close" />
                        <Bar dataKey="volume" fill={`${C.purple}40`} name="Volume" yAxisId="right" />
                        <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 8, fill: C.dim }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}

              {/* Intraday Time & Sales */}
              {timesales.length > 0 && (
                <div>
                  <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
                    <Clock className="w-3 h-3" style={{ color: C.purple }} />
                    <span style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase" }}>Intraday Time & Sales ({timesales.length} ticks)</span>
                  </div>
                  <div style={{ background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 8px", marginBottom: 8 }}>
                    <ResponsiveContainer width="100%" height={120}>
                      <LineChart data={timesales.slice(-120)} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
                        <XAxis dataKey="time" tick={{ fontSize: 7, fill: C.dim }} tickFormatter={(v: string) => v?.slice(11, 16) || ""} />
                        <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={["auto", "auto"]} />
                        <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
                        <Line type="monotone" dataKey="price" stroke={C.green} strokeWidth={1.5} dot={false} name="Price" />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  <div style={{ maxHeight: 200, overflowY: "auto", borderRadius: 8, border: `1px solid ${C.border}` }}>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                      <thead>
                        <tr style={{ background: `${C.border}50` }}>
                          <th style={{ padding: "5px 8px", fontSize: 9, fontFamily: font, textAlign: "left", color: C.dim }}>TIME</th>
                          <th style={{ padding: "5px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>PRICE</th>
                          <th style={{ padding: "5px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>SIZE</th>
                          <th style={{ padding: "5px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>VWAP</th>
                        </tr>
                      </thead>
                      <tbody>
                        {timesales.slice(-50).reverse().map((tick: any, i: number) => (
                          <tr key={i} style={{ borderTop: `1px solid ${C.border}` }}>
                            <td style={{ padding: "4px 8px", fontSize: 10, fontFamily: font, color: C.text }}>{tick.time?.slice(11, 19) || tick.timestamp || "—"}</td>
                            <td style={{ padding: "4px 8px", fontSize: 10, fontFamily: font, color: C.bright, textAlign: "right" }}>{fmtMoney(tick.price)}</td>
                            <td style={{ padding: "4px 8px", fontSize: 10, fontFamily: font, color: C.blue, textAlign: "right" }}>{fmtVol(tick.volume || tick.size)}</td>
                            <td style={{ padding: "4px 8px", fontSize: 10, fontFamily: font, color: C.purple, textAlign: "right" }}>{tick.vwap != null ? fmtMoney(tick.vwap) : "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Time & Sales Panel (for ticker detail, Tradier-only) ── */
function TimeSalesPanel({ symbol }: { symbol: string }) {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    fetch(`${API_BASE}/timesales/${encodeURIComponent(symbol)}?interval=5min`, { headers: authHeaders() })
      .then(r => r.ok ? r.json() : null)
      .then(json => {
        if (!cancelled) {
          const ticks = json?.ticks || json?.timesales || json?.data || [];
          setData(Array.isArray(ticks) ? ticks : []);
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [symbol]);

  if (loading) return <div style={{ padding: 12, color: C.dim, fontSize: 11, fontFamily: font, display: "flex", alignItems: "center", gap: 6 }}><Loader2 className="w-3 h-3 animate-spin" /> Loading time & sales...</div>;
  if (!data.length) return <div style={{ padding: 12, color: C.dim, fontSize: 11, fontFamily: font }}>No intraday time & sales data available.</div>;

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8 }}>
        <Clock className="w-3 h-3" style={{ color: C.purple }} />
        <span style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase" }}>Intraday Time & Sales — {symbol}</span>
      </div>
      <div style={{ background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 8px", marginBottom: 8 }}>
        <ResponsiveContainer width="100%" height={100}>
          <LineChart data={data.slice(-120)} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis dataKey="time" tick={{ fontSize: 7, fill: C.dim }} tickFormatter={(v: string) => v?.slice(11, 16) || ""} />
            <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={["auto", "auto"]} />
            <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
            <Line type="monotone" dataKey="price" stroke={C.green} strokeWidth={1.5} dot={false} name="Price" />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div style={{ maxHeight: 180, overflowY: "auto", borderRadius: 8, border: `1px solid ${C.border}` }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ background: `${C.border}50` }}>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "left", color: C.dim }}>TIME</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>OPEN</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>HIGH</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>LOW</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>CLOSE</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>VOL</th>
              <th style={{ padding: "4px 8px", fontSize: 9, fontFamily: font, textAlign: "right", color: C.dim }}>VWAP</th>
            </tr>
          </thead>
          <tbody>
            {data.slice(-30).reverse().map((tick: any, i: number) => (
              <tr key={i} style={{ borderTop: `1px solid ${C.border}` }}>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.text }}>{tick.time?.slice(11, 16) || tick.timestamp || "—"}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.text, textAlign: "right" }}>{fmtMoney(tick.open)}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.green, textAlign: "right" }}>{fmtMoney(tick.high)}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.red, textAlign: "right" }}>{fmtMoney(tick.low)}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.bright, textAlign: "right" }}>{fmtMoney(tick.close || tick.price)}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.blue, textAlign: "right" }}>{fmtVol(tick.volume)}</td>
                <td style={{ padding: "3px 8px", fontSize: 10, fontFamily: font, color: C.purple, textAlign: "right" }}>{tick.vwap != null ? fmtMoney(tick.vwap) : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

type ScanTab = "etf" | "megacap" | "large_cap" | "small_cap";
const SCAN_TAB_LABELS: Record<ScanTab, string> = { etf: "ETFs", megacap: "Megacap ($1T+)", large_cap: "Large Cap ($100B–$999B)", small_cap: "Small Cap ($500M–$99B)" };
const SCAN_TAB_ORDER: ScanTab[] = ["etf", "megacap", "large_cap", "small_cap"];

export default function OptionsPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [loadStage, setLoadStage] = useState("Initializing live scan...");
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [refreshError, setRefreshError] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [scanTab, setScanTab] = useState<ScanTab>("megacap");
  const [tab, setTab] = useState<MainTab>("tickers");
  const [chatInput, setChatInput] = useState("");
  const [chatMessages, setChatMessages] = useState<Array<{ role: "user" | "ai"; text: string }>>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const [showRankingInfo, setShowRankingInfo] = useState(false);
  const [contractDetailSymbol, setContractDetailSymbol] = useState<string | null>(null);
  const chatBottomRef = useRef<HTMLDivElement>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const tabCacheRef = useRef<Record<string, any>>({});

  const scanTabRef = useRef<ScanTab>(scanTab);
  scanTabRef.current = scanTab;

  // Track available tabs separately so they don't jump around when switching tabs
  const [knownAvailableTabs, setKnownAvailableTabs] = useState<ScanTab[]>(SCAN_TAB_ORDER);

  // Scan defaults state
  const [scanDefaults, setScanDefaults] = useState<Record<string, any>>({});
  const [scanDefaultsEditable, setScanDefaultsEditable] = useState<string[]>([]);
  const [scanDefaultsIsEditable, setScanDefaultsIsEditable] = useState(false);
  const [scanDefaultsOverrides, setScanDefaultsOverrides] = useState<Record<string, any>>({});
  const [tierMcapRange, setTierMcapRange] = useState<Record<string, any> | null>(null);
  const [scanDefaultsSaving, setScanDefaultsSaving] = useState(false);

  const fetchScanDefaults = useCallback(async (t: ScanTab) => {
    try {
      const res = await fetch(`${API_BASE}/scan-defaults?tab=${t}`, { headers: authHeaders() });
      if (!res.ok) return;
      const json = await res.json();
      setScanDefaults(json.defaults || {});
      setScanDefaultsEditable(json.editable_keys || []);
      setScanDefaultsIsEditable(json.editable !== false);
      setScanDefaultsOverrides({});
      setTierMcapRange(json.tier_mcap_range || null);
    } catch { /* ignore */ }
  }, []);

  useEffect(() => {
    fetchScanDefaults(scanTab);
  }, [scanTab, fetchScanDefaults]);

  const saveScanDefaults = async () => {
    setScanDefaultsSaving(true);
    try {
      const res = await fetch(`${API_BASE}/scan-defaults`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify({ tab: scanTab, overrides: scanDefaultsOverrides }),
      });
      if (res.ok) {
        const json = await res.json();
        setScanDefaults(json.defaults || {});
        setScanDefaultsOverrides({});
      }
    } catch { /* ignore */ } finally {
      setScanDefaultsSaving(false);
    }
  };

  const resetScanDefaults = async () => {
    setScanDefaultsSaving(true);
    try {
      const res = await fetch(`${API_BASE}/scan-defaults`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify({ tab: scanTab, reset: true }),
      });
      if (res.ok) {
        const json = await res.json();
        setScanDefaults(json.defaults || {});
        setScanDefaultsOverrides({});
      }
    } catch { /* ignore */ } finally {
      setScanDefaultsSaving(false);
    }
  };

  const fetchDashboard = useCallback(async (tabOverride?: ScanTab) => {
    const activeTab = tabOverride ?? scanTabRef.current;
    const hasExistingData = !!tabCacheRef.current[activeTab];

    // Background refresh: don't clear content, just show subtle indicator
    if (hasExistingData) {
      setIsRefreshing(true);
      setRefreshError(null);
    } else {
      // Initial load: show full loading state
      setLoading(true);
      setError("");
    }

    const stages = ["Running live scan...", "Scanning options chains...", "Aggregating flow & context...", "Scoring modular signals...", "Building market summary...", "Finalizing dashboard..."];
    let si = 0;
    if (!hasExistingData) setLoadStage(stages[0]);
    const stageTimer = !hasExistingData ? setInterval(() => {
      si = Math.min(si + 1, stages.length - 1);
      setLoadStage(stages[si]);
    }, 2500) : null;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 55_000);
    try {
      const url = `${API_BASE}/dashboard?tab=${encodeURIComponent(activeTab)}`;
      console.log(`[OptionsPage] fetchDashboard → ${url}  (tabOverride=${tabOverride ?? "none"}, scanTabRef=${scanTabRef.current}, background=${hasExistingData})`);
      const res = await fetch(url, { headers: authHeaders(), signal: controller.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      const resp = json?.response || {};
      console.log(`[OptionsPage] response keys:`, Object.keys(resp), `| tickers: ${(resp.tickers || []).length} | all_contracts: ${(resp.all_contracts || []).length} | tickers_scanned (seed, NOT used): ${resp.tickers_scanned ?? "n/a"}`);
      // Update cache and state in-place
      tabCacheRef.current[activeTab] = json;
      // Update known available tabs from API response (merged, not replaced)
      const apiTabs = json?.available_tabs as ScanTab[] | undefined;
      if (apiTabs?.length) {
        setKnownAvailableTabs(prev => {
          const merged = new Set([...prev, ...apiTabs]);
          return SCAN_TAB_ORDER.filter(t => merged.has(t));
        });
      }
      if (activeTab === scanTabRef.current) {
        setData(json);
      }
      setError("");
    } catch (e: any) {
      if (hasExistingData) {
        // Background refresh failed — keep old data, show toast
        setRefreshError("Refresh failed, showing cached data");
        setTimeout(() => setRefreshError(null), 5000);
      } else {
        if (e.name === "AbortError") {
          setError("Request timed out (55s). The backend may still be building the cache — click Refresh to try again.");
        } else {
          setError(e.message || "Failed to load options dashboard");
        }
      }
    } finally {
      clearTimeout(timeout);
      if (stageTimer) clearInterval(stageTimer);
      setLoading(false);
      setLoadStage("");
      setIsRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchDashboard();
    intervalRef.current = setInterval(() => fetchDashboard(), 120_000);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [fetchDashboard]);

  const switchScanTab = (newTab: ScanTab) => {
    if (newTab === scanTab) return;
    setScanTab(newTab);
    // Show cached data instantly if available, otherwise show loading
    const cached = tabCacheRef.current[newTab];
    if (cached) {
      setData(cached);
      setError("");
    } else {
      setData(null);
    }
    fetchDashboard(newTab);
  };

  useEffect(() => {
    chatBottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [chatMessages]);

  const askAgent = async () => {
    if (!chatInput.trim() || chatLoading) return;
    const q = chatInput.trim();
    setChatInput("");
    setChatMessages(prev => [...prev, { role: "user", text: q }]);
    setChatLoading(true);
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/options/query`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ query: q, tab: scanTab, context_data: data, conversation_id: null }),
      });
      const json = await res.json();
      const text = json.analysis || json.response?.analysis || json.structured?.summary || json.text || "No response.";
      setChatMessages(prev => [...prev, { role: "ai", text }]);
    } catch (e: any) {
      setChatMessages(prev => [...prev, { role: "ai", text: `Error: ${e.message}` }]);
    } finally {
      setChatLoading(false);
    }
  };

  const resp: OptionsDashboardResponse = data?.response || {};
  const tickers = (resp.tickers || []).slice().sort((a, b) => (normalizeScore(b.composite_score) ?? -1) - (normalizeScore(a.composite_score) ?? -1));
  const allContracts = resp.all_contracts || [];
  const mktSum = resp.market_summary || {};
  const pipelineStats = resp.pipeline_stats || {};
  const filterDefaults = resp.filter_defaults || {};
  const scoreWeights = resp.score_weights || {};
  const cacheAge: number | null = data?.cache_age_seconds ?? null;
  const fromCache: boolean = data?.from_cache ?? false;
  // Always use stable knownAvailableTabs so tabs never jump around on click
  const availableTabs: ScanTab[] = knownAvailableTabs;
  const hasData = tickers.length > 0;
  const degradedSources = ensureArray((pipelineStats as any)?.degraded_sources || (mktSum as any)?.degraded_sources);

  const filterEntries = Object.entries(filterDefaults).filter(([, value]) => value !== null && value !== undefined && value !== "").slice(0, 8);
  const scoreWeightEntries = Object.entries(scoreWeights).filter(([, value]) => typeof value === "number");

  return (
    <div style={{ background: C.bg, minHeight: "100vh", fontFamily: sans }}>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes pulse { 0%,100% { opacity:0.5; } 50% { opacity:1; } }
        @keyframes fadeIn { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:translateY(0); } }
        @keyframes refreshBar { from { width: 0%; } to { width: 100%; } }
        @keyframes toastIn { from { opacity:0; transform:translateY(-8px); } to { opacity:1; transform:translateY(0); } }
        @keyframes toastOut { from { opacity:1; } to { opacity:0; } }
      `}</style>

      <div style={{ padding: "16px 20px 10px", borderBottom: `1px solid ${C.border}`, position: "sticky", top: 0, background: C.bg, zIndex: 10 }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 8, marginBottom: hasData ? 10 : 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            <Zap className="w-5 h-5" style={{ color: C.green }} />
            <span style={{ color: C.bright, fontSize: 17, fontWeight: 800, fontFamily: font, letterSpacing: "-0.02em" }}>OPTIONS FLOW</span>
            {fromCache && cacheAge != null ? <span style={{ color: C.dim, fontSize: 10, fontFamily: font }}>Updated {cacheAge}s ago</span> : null}
            {isRefreshing && <Loader2 className="w-3 h-3 animate-spin" style={{ color: C.blue, opacity: 0.7 }} />}
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
            {scoreWeightEntries.length ? (
              <button onClick={() => setShowRankingInfo(v => !v)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 12px", background: `${C.purple}12`, border: `1px solid ${C.purple}30`, borderRadius: 7, color: C.purple, fontSize: 11, fontWeight: 600, fontFamily: font, cursor: "pointer" }}>
                <BarChart3 className="w-3 h-3" /> How ranking works
              </button>
            ) : null}
            <button onClick={() => fetchDashboard()} disabled={loading || isRefreshing} style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 14px", background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 7, color: (loading || isRefreshing) ? C.dim : C.blue, fontSize: 12, fontWeight: 600, fontFamily: font, cursor: (loading || isRefreshing) ? "not-allowed" : "pointer" }}>
              <RefreshCw className={`w-3 h-3 ${(loading || isRefreshing) ? "animate-spin" : ""}`} />
              {loading ? loadStage : isRefreshing ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>

        {/* Scan tab switcher */}
        <div style={{ display: "flex", gap: 4, marginBottom: 10 }}>
          {availableTabs.map(t => (
            <button key={t} onClick={() => switchScanTab(t)} disabled={loading && !hasData}
              style={{ padding: "5px 16px", fontSize: 11, fontWeight: 700, fontFamily: font, background: scanTab === t ? `${C.green}18` : "transparent", color: scanTab === t ? C.green : C.dim, border: `1px solid ${scanTab === t ? C.green + "40" : C.border}`, borderRadius: 6, cursor: (loading && !hasData) ? "not-allowed" : "pointer", opacity: (loading && !hasData) && scanTab !== t ? 0.5 : 1, transition: "all 0.15s ease" }}>
              {SCAN_TAB_LABELS[t] || toTitleCase(t)}
            </button>
          ))}
        </div>

        {showRankingInfo && scoreWeightEntries.length ? (
          <div style={{ marginBottom: 10, background: C.card, border: `1px solid ${C.border}`, borderRadius: 9, padding: 12, animation: "fadeIn 0.25s ease" }}>
            <div style={{ color: C.dim, fontSize: 10, fontFamily: font, textTransform: "uppercase", marginBottom: 8 }}>Score weights</div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              {scoreWeightEntries.map(([key, value]) => <Badge key={key} color={C.purple}>{toTitleCase(key)} {fmtNum(normalizeScore(value as number), 0)}</Badge>)}
            </div>
          </div>
        ) : null}

        {hasData && (
          <div style={{ display: "grid", gap: 10, animation: "fadeIn 0.35s ease" }}>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(135px, 1fr))", gap: 8 }}>
              <MetricBlock label="Most Active" value={fmtMaybeText((mktSum as any).most_active_ticker)} color={C.gold} />
              <MetricBlock label="Market P/C" value={(mktSum as any).market_pc_ratio != null ? fmtNum((mktSum as any).market_pc_ratio, 2) : "—"} color={pcColor((mktSum as any).market_pc_ratio ?? null)} />
              <MetricBlock label="Call Volume" value={fmtVol((mktSum as any).total_call_volume as number | null)} color={C.green} />
              <MetricBlock label="Put Volume" value={fmtVol((mktSum as any).total_put_volume as number | null)} color={C.red} />
              <MetricBlock label="Tickers Ranked" value={(mktSum as any).tickers_ranked ?? tickers.length} color={C.blue} />
              <MetricBlock label="Prefilter" value={(pipelineStats as any).prefilter_candidate_count ?? "—"} color={C.text} />
              <MetricBlock label="Inspected" value={(pipelineStats as any).options_inspection_count ?? "—"} color={C.text} />
              <MetricBlock label="Ranked Results" value={(pipelineStats as any).ranked_result_count ?? tickers.length} color={C.bright} />
              <MetricBlock label="Degraded Sources" value={degradedSources.length} color={degradedSources.length ? C.orange : C.green} subtext={degradedSources.length ? degradedSources.join(", ") : "All enrichment sources available"} />
            </div>

            {((mktSum as any).macro_context || filterEntries.length || degradedSources.length) && (
              <SectionCard>
                <div style={{ padding: 12, display: "grid", gap: 10 }}>
                  {(mktSum as any).macro_context ? <div style={{ color: C.text, fontSize: 12, lineHeight: 1.6 }}><span style={{ color: C.dim, fontFamily: font, fontSize: 10, textTransform: "uppercase" }}>Macro context:</span> {(mktSum as any).macro_context as ReactNode}</div> : null}
                  {Object.keys(scanDefaults).length ? (
                    <div>
                      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 6 }}>
                        <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: "uppercase" }}>Scan defaults</div>
                        {scanDefaultsIsEditable && (
                          <div style={{ display: "flex", gap: 6 }}>
                            {Object.keys(scanDefaultsOverrides).length > 0 && (
                              <button onClick={saveScanDefaults} disabled={scanDefaultsSaving} style={{ display: "flex", alignItems: "center", gap: 4, padding: "3px 8px", fontSize: 9, fontWeight: 600, fontFamily: font, background: `${C.green}15`, border: `1px solid ${C.green}30`, borderRadius: 4, color: C.green, cursor: scanDefaultsSaving ? "not-allowed" : "pointer" }}>
                                <Save className="w-3 h-3" /> Save
                              </button>
                            )}
                            <button onClick={resetScanDefaults} disabled={scanDefaultsSaving} style={{ display: "flex", alignItems: "center", gap: 4, padding: "3px 8px", fontSize: 9, fontWeight: 600, fontFamily: font, background: `${C.orange}15`, border: `1px solid ${C.orange}30`, borderRadius: 4, color: C.orange, cursor: scanDefaultsSaving ? "not-allowed" : "pointer" }}>
                              <RotateCcw className="w-3 h-3" /> Reset to Defaults
                            </button>
                          </div>
                        )}
                      </div>
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                        {tierMcapRange && (tierMcapRange.min != null || tierMcapRange.max != null || tierMcapRange.label) && (
                          <Badge key="__tier_mcap" color={C.dim} sm>Mcap Range: {tierMcapRange.label || `${tierMcapRange.min != null ? fmtMaybeText(tierMcapRange.min) : "—"} – ${tierMcapRange.max != null ? fmtMaybeText(tierMcapRange.max) : "—"}`}</Badge>
                        )}
                        {Object.entries(scanDefaults).filter(([k, v]) => v !== null && v !== undefined && v !== "" && !/(^|_)(min|max)_mcap$|^mcap_/i.test(k)).slice(0, 12).map(([key, value]) => {
                          const isEditable = scanDefaultsIsEditable && scanDefaultsEditable.includes(key);
                          const currentVal = scanDefaultsOverrides[key] ?? value;
                          if (isEditable) {
                            return (
                              <div key={key} style={{ display: "flex", alignItems: "center", gap: 4, background: `${C.blue}10`, border: `1px solid ${C.blue}25`, borderRadius: 5, padding: "2px 6px" }}>
                                <span style={{ color: C.dim, fontSize: 9, fontFamily: font }}>{toTitleCase(key)}:</span>
                                <input
                                  type={typeof value === "number" ? "number" : "text"}
                                  value={currentVal}
                                  onChange={e => {
                                    const v = typeof value === "number" ? (e.target.value === "" ? "" : Number(e.target.value)) : e.target.value;
                                    setScanDefaultsOverrides(prev => ({ ...prev, [key]: v }));
                                  }}
                                  style={{ width: 60, background: C.bg, border: `1px solid ${C.border}`, borderRadius: 3, padding: "1px 4px", color: C.bright, fontSize: 10, fontFamily: font, outline: "none" }}
                                />
                              </div>
                            );
                          }
                          return <Badge key={key} color={C.blue} sm>{toTitleCase(key)}: {fmtMaybeText(currentVal)}</Badge>;
                        })}
                      </div>
                    </div>
                  ) : null}
                  {degradedSources.length ? (
                    <div style={{ display: "flex", alignItems: "center", gap: 8, color: C.orange, fontSize: 12 }}>
                      <CircleAlert className="w-4 h-4" /> Some enrichment sources were unavailable, so certain scores or details may be approximate.
                    </div>
                  ) : null}
                </div>
              </SectionCard>
            )}
          </div>
        )}
      </div>

      {/* Thin refresh progress bar */}
      {isRefreshing && (
        <div style={{ height: 2, background: C.border, overflow: "hidden", position: "relative" }}>
          <div style={{ height: "100%", background: C.blue, animation: "refreshBar 8s ease-in-out infinite", opacity: 0.8 }} />
        </div>
      )}

      {/* Refresh error toast */}
      {refreshError && (
        <div style={{ position: "fixed", top: 12, right: 12, zIndex: 100, background: `${C.orange}18`, border: `1px solid ${C.orange}40`, borderRadius: 8, padding: "8px 14px", color: C.orange, fontSize: 12, fontFamily: font, animation: "toastIn 0.2s ease", display: "flex", alignItems: "center", gap: 6 }}>
          <CircleAlert className="w-3 h-3" /> {refreshError}
        </div>
      )}

      <div style={{ display: "flex", flexDirection: "column", height: "calc(100vh - 140px)" }}>
        <div style={{ flex: 1, overflowY: "auto", padding: "14px 20px" }}>
          {loading && !hasData && (
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 14, padding: "60px 20px" }}>
              <div style={{ width: 40, height: 40, border: `3px solid ${C.border}`, borderTop: `3px solid ${C.blue}`, borderRadius: "50%", animation: "spin 0.8s linear infinite" }} />
              <div style={{ color: C.blue, fontSize: 13, fontFamily: font }}>{loadStage}</div>
              {[1, 2, 3].map(i => <Skeleton key={i} h={48} mb={0} />)}
            </div>
          )}

          {error && !loading && !hasData && (
            <div style={{ background: `${C.red}10`, border: `1px solid ${C.red}30`, borderRadius: 10, padding: "14px 18px", color: C.red, fontSize: 13, fontFamily: sans }}>⚠ {error}</div>
          )}

          {!loading && !isRefreshing && !error && data && tickers.length === 0 && (
            <div style={{ background: `${C.yellow}08`, border: `1px solid ${C.yellow}25`, borderRadius: 10, padding: "24px 20px", textAlign: "center", animation: "fadeIn 0.3s ease" }}>
              <div style={{ color: C.yellow, fontSize: 14, fontWeight: 700, fontFamily: font, marginBottom: 8 }}>No tickers returned</div>
              <div style={{ color: C.text, fontSize: 12, lineHeight: 1.6, marginBottom: 14 }}>
                The scan completed but returned zero ranked tickers. This usually means the cache is still warming up after a fresh deploy.
                Click Refresh to retry once the backend cache is ready.
              </div>
              <button onClick={() => fetchDashboard()} style={{ padding: "8px 20px", background: `${C.blue}18`, border: `1px solid ${C.blue}40`, borderRadius: 7, color: C.blue, fontSize: 12, fontWeight: 600, fontFamily: font, cursor: "pointer" }}>
                <RefreshCw className="w-3 h-3" style={{ display: "inline-block", marginRight: 6, verticalAlign: "middle" }} />
                Refresh
              </button>
            </div>
          )}

          {hasData && <DataIngestionWidget />}

          {hasData && (
            <div style={{ animation: "fadeIn 0.35s ease" }}>
              <div style={{ display: "flex", gap: 6, marginBottom: 14 }}>
                {[
                  { id: "tickers" as MainTab, label: "Signal Board", count: tickers.length },
                  { id: "flow" as MainTab, label: "Contracts Table", count: allContracts.length },
                ].map(t => (
                  <button key={t.id} onClick={() => setTab(t.id)} style={{ display: "flex", alignItems: "center", gap: 6, padding: "6px 16px", fontSize: 11, fontWeight: 600, fontFamily: font, background: tab === t.id ? `${C.blue}18` : "transparent", color: tab === t.id ? C.blue : C.dim, border: `1px solid ${tab === t.id ? `${C.blue}40` : C.border}`, borderRadius: 6, cursor: "pointer" }}>
                    {t.label}
                    <span style={{ background: `${C.blue}25`, color: C.blue, borderRadius: 10, padding: "0 6px", fontSize: 9 }}>{t.count}</span>
                  </button>
                ))}
              </div>
              {tab === "tickers" && <TickerSummaryTab tickers={tickers} />}
              {tab === "flow" && <FlowTab contracts={allContracts} onContractClick={(sym) => setContractDetailSymbol(sym)} />}
            </div>
          )}
        </div>

        <div style={{ borderTop: `1px solid ${C.border}`, background: C.card, padding: "10px 20px 14px", flexShrink: 0 }}>
          {chatMessages.length > 0 && (
            <div style={{ maxHeight: 180, overflowY: "auto", marginBottom: 10, display: "flex", flexDirection: "column", gap: 8 }}>
              {chatMessages.map((m, i) => (
                <div key={i} style={{ display: "flex", justifyContent: m.role === "user" ? "flex-end" : "flex-start" }}>
                  <div style={{ maxWidth: "82%", padding: "8px 12px", borderRadius: 8, fontSize: 12, fontFamily: sans, lineHeight: 1.6, background: m.role === "user" ? `${C.blue}18` : C.cardAlt, color: m.role === "user" ? C.blue : C.text, border: `1px solid ${m.role === "user" ? `${C.blue}30` : C.border}` }}>
                    {m.text}
                  </div>
                </div>
              ))}
              {chatLoading && <div style={{ display: "flex", alignItems: "center", gap: 6, color: C.dim, fontSize: 11, fontFamily: font }}><Loader2 className="w-3 h-3 animate-spin" /> Analyzing...</div>}
              <div ref={chatBottomRef} />
            </div>
          )}
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <input value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); askAgent(); } }} placeholder={hasData ? "Ask about signal rank, thesis, risks, IV regime, or contract ideas..." : "Loading data..."} disabled={chatLoading || !hasData} style={{ flex: 1, background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: "9px 14px", color: C.bright, fontSize: 12, fontFamily: sans, outline: "none", opacity: hasData ? 1 : 0.5 }} />
            <button onClick={askAgent} disabled={chatLoading || !chatInput.trim() || !hasData} style={{ padding: "9px 14px", background: chatLoading || !chatInput.trim() || !hasData ? `${C.dim}18` : `${C.blue}20`, border: `1px solid ${chatLoading || !chatInput.trim() || !hasData ? C.border : `${C.blue}40`}`, borderRadius: 8, color: chatLoading || !chatInput.trim() || !hasData ? C.dim : C.blue, cursor: chatLoading || !chatInput.trim() || !hasData ? "not-allowed" : "pointer", display: "flex", alignItems: "center", gap: 5 }}>
              {chatLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            </button>
          </div>
        </div>
      </div>

      {/* Contract Detail Modal */}
      {contractDetailSymbol && (
        <ContractDetailModal
          occSymbol={contractDetailSymbol}
          onClose={() => setContractDetailSymbol(null)}
        />
      )}
    </div>
  );
}
