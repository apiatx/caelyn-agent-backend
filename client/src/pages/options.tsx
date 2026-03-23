import { useState, useEffect, useRef, useCallback, Fragment } from "react";
import { RefreshCw, Send, Loader2, Zap, ChevronDown, ChevronUp, ArrowUpDown, ArrowUp, ArrowDown, TrendingUp, TrendingDown, Activity, BarChart3, Database } from "lucide-react";
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, ComposedChart, Area, ReferenceLine } from 'recharts';

const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";

function getToken(): string | null {
  return localStorage.getItem('caelyn_token') || sessionStorage.getItem('caelyn_token');
}
function authHeaders(): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json', 'X-API-Key': AGENT_API_KEY };
  const t = getToken();
  if (t) h['Authorization'] = `Bearer ${t}`;
  return h;
}

const C = {
  bg: '#050510', card: '#08080f', cardAlt: '#0c0c1a', border: '#1a1a30',
  bright: '#e2e8f0', text: '#94a3b8', dim: '#475569',
  blue: '#38bdf8', green: '#4ade80', red: '#ef4444',
  yellow: '#fbbf24', orange: '#f97316', purple: '#a855f7', gold: '#f59e0b',
};
const font = "'JetBrains Mono', 'Fira Code', monospace";
const sans = "'Outfit', 'Inter', sans-serif";

// ─── Helpers ────────────────────────────────────────────────────────────────────
const sideColor = (s: string) => s?.toLowerCase() === 'call' ? C.green : C.red;
const pcColor = (r: number) => r > 1.2 ? C.red : r < 0.8 ? C.green : C.yellow;
const voiColor = (r: number | null) => r == null ? C.dim : r > 10 ? C.red : r > 5 ? C.orange : r > 3 ? C.yellow : C.dim;
const skewColor = (s: number | null) => s == null ? C.dim : s > 0.05 ? C.red : s < -0.05 ? C.green : C.dim;
const fmtVol = (n: number | null | undefined) => n == null ? '—' : n >= 1_000_000 ? `${(n / 1_000_000).toFixed(1)}M` : n >= 1_000 ? `${(n / 1_000).toFixed(0)}K` : String(n);
const fmtPct = (n: number | null | undefined) => n == null ? '—' : `${(n * 100).toFixed(1)}%`;
const fmtNum = (n: number | null | undefined, d = 2) => n == null ? '—' : n.toFixed(d);
const rsiColor = (v: number | null | undefined) => v == null ? C.dim : v > 70 ? C.red : v < 30 ? C.green : C.text;
const trendSignal = (sma20?: number | null, sma50?: number | null): { label: string; color: string } | null => {
  if (sma20 == null || sma50 == null) return null;
  return sma20 > sma50 ? { label: 'Bullish', color: C.green } : { label: 'Bearish', color: C.red };
};
const macdColor = (v: number | null | undefined) => v == null ? C.dim : v > 0 ? C.green : C.red;

function Badge({ color, children, sm }: { color: string; children: React.ReactNode; sm?: boolean }) {
  return <span style={{ background: `${color}18`, color, border: `1px solid ${color}35`, borderRadius: 4, padding: sm ? '1px 5px' : '2px 7px', fontSize: sm ? 9 : 10, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{children}</span>;
}

function TVChart({ symbol }: { symbol: string }) {
  const [ivl, setIvl] = useState('D');
  const ivls = [{ l: '1H', v: '60' }, { l: '4H', v: '240' }, { l: '1D', v: 'D' }, { l: '1W', v: 'W' }, { l: '1M', v: 'M' }];
  return (
    <div style={{ margin: '12px 0' }}>
      <div style={{ display: 'flex', gap: 4, marginBottom: 6 }}>
        {ivls.map(iv => <button key={iv.v} onClick={e => { e.stopPropagation(); setIvl(iv.v); }} style={{ padding: '2px 8px', fontSize: 9, fontWeight: 600, fontFamily: font, background: ivl === iv.v ? `${C.blue}20` : 'transparent', color: ivl === iv.v ? C.blue : C.dim, border: `1px solid ${ivl === iv.v ? C.blue + '40' : C.border}`, borderRadius: 3, cursor: 'pointer' }}>{iv.l}</button>)}
      </div>
      <div style={{ borderRadius: 8, overflow: 'hidden', border: `1px solid ${C.border}` }}>
        <iframe src={`https://s.tradingview.com/widgetembed/?symbol=${encodeURIComponent(symbol)}&interval=${ivl}&theme=dark&style=1&locale=en&hide_top_toolbar=1&hide_side_toolbar=1&allow_symbol_change=0&save_image=0&width=100%25&height=220`} style={{ width: '100%', height: 220, border: 'none', display: 'block' }} title={`${symbol} chart`} />
      </div>
    </div>
  );
}

function Skeleton({ h = 16, mb = 8 }: { h?: number; mb?: number }) {
  return <div style={{ height: h, background: `${C.border}80`, borderRadius: 4, marginBottom: mb, animation: 'pulse 1.5s ease-in-out infinite' }} />;
}

// ─── Sort control ────────────────────────────────────────────────────────────────
type SortDir = 'asc' | 'desc';
function SortIcon({ col, active, dir }: { col: string; active: string; dir: SortDir }) {
  if (col !== active) return <ArrowUpDown className="w-3 h-3" style={{ color: C.dim, opacity: 0.5 }} />;
  return dir === 'asc' ? <ArrowUp className="w-3 h-3" style={{ color: C.blue }} /> : <ArrowDown className="w-3 h-3" style={{ color: C.blue }} />;
}
function useSortable<T>(rows: T[], defaultCol: keyof T, defaultDir: SortDir = 'desc') {
  const [col, setCol] = useState<keyof T>(defaultCol);
  const [dir, setDir] = useState<SortDir>(defaultDir);
  const toggle = (c: keyof T) => { if (c === col) setDir(d => d === 'asc' ? 'desc' : 'asc'); else { setCol(c); setDir('desc'); } };
  const sorted = [...rows].sort((a, b) => {
    const av = a[col] as any;
    const bv = b[col] as any;
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const cmp = typeof av === 'number' ? av - bv : String(av).localeCompare(String(bv));
    return dir === 'asc' ? cmp : -cmp;
  });
  return { sorted, col: col as string, dir, toggle: toggle as (c: string) => void };
}

// ─── Contracts mini-table (inside expanded ticker row) ───────────────────────────
function ContractsMini({ contracts, side }: { contracts: any[]; side: 'call' | 'put' }) {
  if (!contracts?.length) return null;
  const color = sideColor(side);
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ color, fontSize: 10, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', marginBottom: 6 }}>Top {side}s</div>
      <div style={{ background: C.cardAlt, border: `1px solid ${color}20`, borderRadius: 7, overflow: 'hidden' }}>
        <div style={{ display: 'grid', gridTemplateColumns: '70px 60px 70px 70px 55px 55px 50px', padding: '6px 10px', background: `${color}08`, fontSize: 9, fontFamily: font, textTransform: 'uppercase', color: C.dim }}>
          <span>Strike</span><span>Expiry</span><span style={{ textAlign: 'right' }}>Vol</span><span style={{ textAlign: 'right' }}>OI</span><span style={{ textAlign: 'right' }}>V/OI</span><span style={{ textAlign: 'right' }}>IV</span><span style={{ textAlign: 'right' }}>Δ</span>
        </div>
        {contracts.slice(0, 8).map((c: any, i: number) => (
          <div key={i} style={{ display: 'grid', gridTemplateColumns: '70px 60px 70px 70px 55px 55px 50px', padding: '5px 10px', borderTop: `1px solid ${C.border}`, fontSize: 11, fontFamily: font }}>
            <span style={{ color: color, fontWeight: 700 }}>${c.strike}</span>
            <span style={{ color: C.dim, fontSize: 10 }}>{String(c.expiration || '').slice(5)}</span>
            <span style={{ textAlign: 'right', color: C.bright }}>{fmtVol(c.volume)}</span>
            <span style={{ textAlign: 'right', color: C.text }}>{fmtVol(c.openInterest)}</span>
            <span style={{ textAlign: 'right', color: voiColor(c.vol_oi_ratio) }}>{c.vol_oi_ratio != null ? fmtNum(c.vol_oi_ratio, 1) + '×' : '—'}</span>
            <span style={{ textAlign: 'right', color: C.yellow }}>{c.iv != null ? fmtPct(parseFloat(c.iv)) : '—'}</span>
            <span style={{ textAlign: 'right', color: C.text }}>{c.delta ?? '—'}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── TICKER DETAIL PANEL (expanded row with charts) ─────────────────────────────
function TickerDetailPanel({ symbol }: { symbol: string }) {
  const [technicals, setTechnicals] = useState<any>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [volumeSummary, setVolumeSummary] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    Promise.all([
      fetch(`/api/options/technicals/${encodeURIComponent(symbol)}`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`/api/options/history/${encodeURIComponent(symbol)}?limit=60`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(`/api/options/volume-summary/${encodeURIComponent(symbol)}?days=30`, { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
    ]).then(([tech, hist, vol]) => {
      if (cancelled) return;
      setTechnicals(tech);
      setHistory(Array.isArray(hist?.bars || hist) ? (hist?.bars || hist) : []);
      setVolumeSummary(vol);
      setLoading(false);
    });
    return () => { cancelled = true; };
  }, [symbol]);

  if (loading) {
    return (
      <div style={{ padding: 20, display: 'flex', alignItems: 'center', gap: 8, color: C.dim, fontSize: 11, fontFamily: font }}>
        <Loader2 className="w-3 h-3 animate-spin" /> Loading technicals & history for {symbol}...
      </div>
    );
  }

  // Extract SMA series for price chart overlay
  const smaData = technicals?.sma_20 || technicals?.sma_50 ? (() => {
    const sma20List = Array.isArray(technicals?.sma_20) ? technicals.sma_20 : technicals?.sma_20 ? [technicals.sma_20] : [];
    const sma50List = Array.isArray(technicals?.sma_50) ? technicals.sma_50 : technicals?.sma_50 ? [technicals.sma_50] : [];
    const dateMap: Record<string, any> = {};
    sma20List.forEach((d: any) => { dateMap[d.date] = { ...dateMap[d.date], date: d.date, sma20: d.value }; });
    sma50List.forEach((d: any) => { dateMap[d.date] = { ...dateMap[d.date], date: d.date, sma50: d.value }; });
    return Object.values(dateMap).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })() : [];

  // RSI data
  const rsiData = (() => {
    const rsiList = Array.isArray(technicals?.rsi_14) ? technicals.rsi_14 : technicals?.rsi_14 ? [technicals.rsi_14] : [];
    return rsiList.map((d: any) => ({ date: d.date, rsi: d.value })).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })();

  // MACD data
  const macdData = (() => {
    const macdList = Array.isArray(technicals?.macd) ? technicals.macd : technicals?.macd ? [technicals.macd] : [];
    return macdList.map((d: any) => ({ date: d.date, macd: d.value, signal: d.signal, histogram: d.histogram })).sort((a: any, b: any) => a.date.localeCompare(b.date));
  })();

  // Volume bar chart data from history
  const volumeChartData = history.slice(-30).map((bar: any) => ({
    date: bar.date || bar.day,
    callVol: bar.call_volume || 0,
    putVol: bar.put_volume || 0,
    pcRatio: (bar.call_volume && bar.put_volume) ? (bar.put_volume / bar.call_volume) : null,
  }));

  const chartStyle = { background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: '10px 8px', marginBottom: 10 };
  const chartLabel = (text: string) => <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 6, paddingLeft: 4 }}>{text}</div>;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10 }} onClick={e => e.stopPropagation()}>
      {/* SMA Price Overlay */}
      {smaData.length > 1 && (
        <div style={chartStyle}>
          {chartLabel('SMA 20 / 50')}
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={smaData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
              <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={['auto', 'auto']} />
              <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
              <Line type="monotone" dataKey="sma20" stroke={C.blue} strokeWidth={1.5} dot={false} name="SMA 20" />
              <Line type="monotone" dataKey="sma50" stroke={C.orange} strokeWidth={1.5} dot={false} name="SMA 50" />
              <Legend iconSize={8} wrapperStyle={{ fontSize: 9, fontFamily: font }} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* RSI Sub-Chart */}
      {rsiData.length > 1 && (
        <div style={chartStyle}>
          {chartLabel('RSI (14)')}
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

      {/* MACD Sub-Chart */}
      {macdData.length > 1 && (
        <div style={chartStyle}>
          {chartLabel('MACD')}
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

      {/* Options Volume Bar Chart (Call vs Put) */}
      {volumeChartData.length > 0 && (
        <div style={chartStyle}>
          {chartLabel('Daily Options Volume (30d)')}
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

      {/* P/C Ratio Trend from history */}
      {volumeChartData.filter(d => d.pcRatio != null).length > 1 && (
        <div style={chartStyle}>
          {chartLabel('Put/Call Ratio Trend (30d)')}
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={volumeChartData.filter(d => d.pcRatio != null)} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
              <XAxis dataKey="date" tick={{ fontSize: 8, fill: C.dim }} tickFormatter={(v: string) => v.slice(5)} />
              <YAxis tick={{ fontSize: 8, fill: C.dim }} domain={['auto', 'auto']} />
              <Tooltip contentStyle={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 6, fontSize: 10 }} />
              <ReferenceLine y={1} stroke={C.yellow} strokeDasharray="3 3" strokeOpacity={0.5} />
              <Line type="monotone" dataKey="pcRatio" stroke={C.yellow} strokeWidth={1.5} dot={false} name="P/C Ratio" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Volume Summary Stats */}
      {volumeSummary && (
        <div style={chartStyle}>
          {chartLabel(`30-Day Volume Summary`)}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6, padding: '4px 4px' }}>
            {[
              { label: 'Call Total Vol', value: fmtVol(volumeSummary.call_total_volume), color: C.green },
              { label: 'Put Total Vol', value: fmtVol(volumeSummary.put_total_volume), color: C.red },
              { label: 'Call Avg Daily', value: fmtVol(volumeSummary.call_avg_daily_vol), color: C.green },
              { label: 'Put Avg Daily', value: fmtVol(volumeSummary.put_avg_daily_vol), color: C.red },
              { label: 'Call Contracts', value: fmtVol(volumeSummary.call_unique_contracts), color: C.blue },
              { label: 'Put Contracts', value: fmtVol(volumeSummary.put_unique_contracts), color: C.purple },
            ].map((s, i) => (
              <div key={i} style={{ padding: '5px 8px', background: `${s.color}08`, borderRadius: 5, border: `1px solid ${s.color}15` }}>
                <div style={{ color: C.dim, fontSize: 8, fontFamily: font, textTransform: 'uppercase', marginBottom: 2 }}>{s.label}</div>
                <div style={{ color: s.color, fontSize: 13, fontWeight: 700, fontFamily: font }}>{s.value}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Fallback if no data */}
      {smaData.length <= 1 && rsiData.length <= 1 && macdData.length <= 1 && volumeChartData.length === 0 && !volumeSummary && (
        <div style={{ gridColumn: '1 / -1', padding: '16px 0', color: C.dim, fontSize: 11, fontFamily: font, textAlign: 'center' }}>
          <Activity className="w-4 h-4 inline-block" style={{ marginRight: 6 }} />
          Technical data not yet available — Polygon ingestion may still be in progress.
        </div>
      )}
    </div>
  );
}

// ─── DATA INGESTION STATUS WIDGET ────────────────────────────────────────────────
function DataIngestionWidget() {
  const [coverage, setCoverage] = useState<any>(null);
  const [progress, setProgress] = useState<any>(null);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);

  const fetchStatus = useCallback(async () => {
    setLoading(true);
    try {
      const [covRes, progRes] = await Promise.all([
        fetch('/api/options/data-coverage', { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
        fetch('/api/options/fetch-progress', { headers: authHeaders() }).then(r => r.ok ? r.json() : null).catch(() => null),
      ]);
      setCoverage(covRes);
      setProgress(progRes);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open && !coverage) fetchStatus();
  }, [open, coverage, fetchStatus]);

  const tickersIngested = progress?.tickers_completed ?? coverage?.tickers_ingested ?? '?';
  const tickersTotal = progress?.tickers_total ?? coverage?.tickers_total ?? '?';
  const barsStored = coverage?.total_bars ?? '?';
  const lastUpdated = coverage?.last_updated;
  const timeAgo = lastUpdated ? (() => {
    const diff = Date.now() - new Date(lastUpdated).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    return `${hrs}h ago`;
  })() : 'unknown';

  return (
    <div style={{ marginBottom: 12 }}>
      <button onClick={() => setOpen(o => !o)} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '5px 12px', fontSize: 10, fontWeight: 600, fontFamily: font, background: open ? `${C.purple}15` : 'transparent', color: open ? C.purple : C.dim, border: `1px solid ${open ? C.purple + '40' : C.border}`, borderRadius: 6, cursor: 'pointer' }}>
        <Database className="w-3 h-3" />
        Ingestion Status
        {open ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
      </button>
      {open && (
        <div style={{ marginTop: 8, background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: '12px 16px', animation: 'fadeIn 0.2s ease' }}>
          {loading ? (
            <div style={{ color: C.dim, fontSize: 11, fontFamily: font, display: 'flex', alignItems: 'center', gap: 6 }}>
              <Loader2 className="w-3 h-3 animate-spin" /> Fetching ingestion status...
            </div>
          ) : (
            <div style={{ display: 'flex', gap: 20, alignItems: 'center', flexWrap: 'wrap' }}>
              <div style={{ color: C.text, fontSize: 12, fontFamily: font }}>
                <span style={{ color: C.bright, fontWeight: 700 }}>{tickersIngested}</span>
                <span style={{ color: C.dim }}> / {tickersTotal} tickers ingested</span>
              </div>
              <div style={{ color: C.text, fontSize: 12, fontFamily: font }}>
                <span style={{ color: C.blue, fontWeight: 700 }}>{typeof barsStored === 'number' ? barsStored.toLocaleString() : barsStored}</span>
                <span style={{ color: C.dim }}> bars stored</span>
              </div>
              <div style={{ color: C.dim, fontSize: 11, fontFamily: font }}>
                Last updated: <span style={{ color: C.text }}>{timeAgo}</span>
              </div>
              <button onClick={fetchStatus} style={{ padding: '3px 8px', fontSize: 9, fontFamily: font, background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 4, color: C.blue, cursor: 'pointer' }}>
                <RefreshCw className="w-3 h-3 inline-block" />
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── TICKER SUMMARY TAB ──────────────────────────────────────────────────────────
type CatFilter = 'all' | 'stock' | 'etf';

function TickerSummaryTab({ tickers }: { tickers: any[] }) {
  const [catFilter, setCatFilter] = useState<CatFilter>('all');
  const [expanded, setExpanded] = useState<string | null>(null);
  const filtered = tickers.filter(t => catFilter === 'all' || t.category === catFilter).map(t => ({
    ...t,
    _rsi: t.technicals?.rsi_14?.value ?? null,
    _trend: (() => { const s = trendSignal(t.technicals?.sma_20?.value, t.technicals?.sma_50?.value); return s ? (s.label === 'Bullish' ? 1 : 0) : null; })(),
    _macd: t.technicals?.macd?.histogram ?? null,
    _histVol: ((t.historic_volume?.call_total_volume ?? 0) + (t.historic_volume?.put_total_volume ?? 0)) || null,
  }));
  const { sorted, col, dir, toggle } = useSortable(filtered, 'total_volume');

  const TH = ({ c, label, right }: { c: string; label: string; right?: boolean }) => (
    <th onClick={() => toggle(c)} style={{ padding: '8px 10px', textAlign: right ? 'right' : 'left', fontSize: 9, fontFamily: font, textTransform: 'uppercase', color: col === c ? C.blue : C.dim, cursor: 'pointer', whiteSpace: 'nowrap', userSelect: 'none' }}>
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3 }}>{label} <SortIcon col={c} active={col} dir={dir} /></span>
    </th>
  );

  return (
    <div>
      {/* Category filter */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 12 }}>
        {(['all', 'stock', 'etf'] as CatFilter[]).map(f => (
          <button key={f} onClick={() => { setCatFilter(f); setExpanded(null); }}
            style={{ padding: '5px 14px', fontSize: 11, fontWeight: 600, fontFamily: font, background: catFilter === f ? `${C.blue}18` : 'transparent', color: catFilter === f ? C.blue : C.dim, border: `1px solid ${catFilter === f ? C.blue + '40' : C.border}`, borderRadius: 6, cursor: 'pointer' }}>
            {f === 'all' ? 'All' : f === 'stock' ? 'Stocks' : 'ETFs'}
          </button>
        ))}
        <span style={{ marginLeft: 'auto', color: C.dim, fontSize: 11, fontFamily: font, alignSelf: 'center' }}>{sorted.length} tickers</span>
      </div>

      {/* Table */}
      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflow: 'hidden' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ background: `${C.border}50` }}>
                <TH c="ticker" label="Ticker" />
                <TH c="total_volume" label="Total Vol" right />
                <TH c="call_volume" label="Calls" right />
                <TH c="put_volume" label="Puts" right />
                <TH c="pc_ratio" label="P/C" right />
                <TH c="avg_call_iv" label="Call IV" right />
                <TH c="avg_put_iv" label="Put IV" right />
                <TH c="iv_skew" label="Skew" right />
                <TH c="max_pain" label="Max Pain" right />
                <TH c="total_oi" label="Total OI" right />
                <TH c="_rsi" label="RSI" right />
                <TH c="_trend" label="Trend" right />
                <TH c="_macd" label="MACD" right />
                <TH c="_histVol" label="Hist Vol" right />
                <th style={{ padding: '8px 10px', width: 30 }} />
              </tr>
            </thead>
            <tbody>
              {sorted.map((t: any) => {
                const isExp = expanded === t.ticker;
                return <Fragment key={t.ticker}>
                  <tr onClick={() => setExpanded(isExp ? null : t.ticker)}
                    style={{ borderTop: `1px solid ${C.border}`, cursor: 'pointer', background: isExp ? `${C.blue}05` : 'transparent' }}>
                    <td style={{ padding: '10px 10px', fontFamily: font, fontWeight: 800, fontSize: 13, color: C.bright }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                        {t.ticker}
                        <Badge color={t.category === 'etf' ? C.purple : C.blue} sm>{t.category}</Badge>
                      </div>
                    </td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 12, fontWeight: 700, color: C.bright }}>{fmtVol(t.total_volume)}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 12, color: C.green }}>{fmtVol(t.call_volume)}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 12, color: C.red }}>{fmtVol(t.put_volume)}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 12, fontWeight: 700, color: pcColor(t.pc_ratio ?? 1) }}>{t.pc_ratio != null ? fmtNum(t.pc_ratio, 2) : '—'}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.yellow }}>{t.avg_call_iv != null ? fmtPct(t.avg_call_iv) : '—'}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.yellow }}>{t.avg_put_iv != null ? fmtPct(t.avg_put_iv) : '—'}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: skewColor(t.iv_skew) }}>{t.iv_skew != null ? (t.iv_skew >= 0 ? '+' : '') + fmtPct(t.iv_skew) : '—'}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.gold }}>{t.max_pain != null ? `$${t.max_pain}` : '—'}</td>
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.text }}>{fmtVol(t.total_oi)}</td>
                    {/* RSI */}
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, fontWeight: t._rsi != null ? 700 : 400, color: rsiColor(t._rsi) }}>
                      {t._rsi != null ? fmtNum(t._rsi, 1) : '—'}
                    </td>
                    {/* Trend */}
                    <td style={{ padding: '10px 10px', textAlign: 'right' }}>
                      {(() => {
                        const s = trendSignal(t.technicals?.sma_20?.value, t.technicals?.sma_50?.value);
                        if (!s) return <span style={{ color: C.dim, fontFamily: font, fontSize: 11 }}>—</span>;
                        return (
                          <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3, color: s.color, fontFamily: font, fontSize: 10, fontWeight: 700 }}>
                            {s.label === 'Bullish' ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                            {s.label}
                          </span>
                        );
                      })()}
                    </td>
                    {/* MACD Histogram */}
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, fontWeight: t._macd != null ? 700 : 400, color: macdColor(t._macd) }}>
                      {t._macd != null ? (t._macd >= 0 ? '+' : '') + fmtNum(t._macd, 2) : '—'}
                    </td>
                    {/* Hist Vol (30d total) */}
                    <td style={{ padding: '10px 10px', textAlign: 'right', fontFamily: font, fontSize: 11, color: t._histVol ? C.bright : C.dim }}>
                      {t._histVol ? fmtVol(t._histVol) : '—'}
                    </td>
                    <td style={{ padding: '10px 10px', textAlign: 'right' }}>
                      {isExp ? <ChevronUp className="w-3 h-3" style={{ color: C.dim }} /> : <ChevronDown className="w-3 h-3" style={{ color: C.dim }} />}
                    </td>
                  </tr>
                  {isExp && (
                    <tr key={`${t.ticker}-exp`}>
                      <td colSpan={15} style={{ padding: '14px 16px', background: `${C.cardAlt}`, borderTop: `1px solid ${C.border}` }} onClick={e => e.stopPropagation()}>
                        <TVChart symbol={t.ticker} />
                        <TickerDetailPanel symbol={t.ticker} />
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginTop: 4 }}>
                          <ContractsMini contracts={t.top_calls} side="call" />
                          <ContractsMini contracts={t.top_puts} side="put" />
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>;
              })}
            </tbody>
          </table>
        </div>
        {sorted.length === 0 && <div style={{ padding: 40, textAlign: 'center', color: C.dim, fontSize: 13, fontFamily: sans }}>No tickers found.</div>}
      </div>
    </div>
  );
}

// ─── FLOW / CONTRACTS TAB ────────────────────────────────────────────────────────
type SideFilter = 'all' | 'call' | 'put';

function FlowTab({ contracts }: { contracts: any[] }) {
  const [catFilter, setCatFilter] = useState<CatFilter>('all');
  const [sideFilter, setSideFilter] = useState<SideFilter>('all');
  const [unusualOnly, setUnusualOnly] = useState(false);
  const [limit, setLimit] = useState(100);

  const filtered = contracts.filter(c => {
    if (catFilter !== 'all' && c.category !== catFilter) return false;
    if (sideFilter !== 'all' && c.side !== sideFilter) return false;
    if (unusualOnly && (c.vol_oi_ratio == null || c.vol_oi_ratio < 3)) return false;
    return true;
  });
  const { sorted, col, dir, toggle } = useSortable(filtered, 'volume');
  const visible = sorted.slice(0, limit);

  const TH = ({ c, label, right }: { c: string; label: string; right?: boolean }) => (
    <th onClick={() => toggle(c)} style={{ padding: '7px 8px', textAlign: right ? 'right' : 'left', fontSize: 9, fontFamily: font, textTransform: 'uppercase', color: col === c ? C.blue : C.dim, cursor: 'pointer', whiteSpace: 'nowrap', userSelect: 'none' }}>
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 3 }}>{label} <SortIcon col={c} active={col} dir={dir} /></span>
    </th>
  );

  return (
    <div>
      {/* Filters */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 12, flexWrap: 'wrap', alignItems: 'center' }}>
        <div style={{ display: 'flex', gap: 4 }}>
          {(['all', 'stock', 'etf'] as CatFilter[]).map(f => (
            <button key={f} onClick={() => setCatFilter(f)} style={{ padding: '4px 12px', fontSize: 10, fontWeight: 600, fontFamily: font, background: catFilter === f ? `${C.blue}18` : 'transparent', color: catFilter === f ? C.blue : C.dim, border: `1px solid ${catFilter === f ? C.blue + '40' : C.border}`, borderRadius: 5, cursor: 'pointer' }}>
              {f === 'all' ? 'All' : f === 'stock' ? 'Stocks' : 'ETFs'}
            </button>
          ))}
        </div>
        <div style={{ display: 'flex', gap: 4 }}>
          {(['all', 'call', 'put'] as SideFilter[]).map(f => (
            <button key={f} onClick={() => setSideFilter(f)} style={{ padding: '4px 12px', fontSize: 10, fontWeight: 600, fontFamily: font, background: sideFilter === f ? `${sideColor(f === 'all' ? 'call' : f)}18` : 'transparent', color: sideFilter === f ? sideColor(f === 'all' ? 'call' : f) : C.dim, border: `1px solid ${sideFilter === f ? sideColor(f === 'all' ? 'call' : f) + '40' : C.border}`, borderRadius: 5, cursor: 'pointer' }}>
              {f === 'all' ? 'Both' : f === 'call' ? 'Calls' : 'Puts'}
            </button>
          ))}
        </div>
        <button onClick={() => setUnusualOnly(u => !u)} style={{ padding: '4px 12px', fontSize: 10, fontWeight: 600, fontFamily: font, background: unusualOnly ? `${C.orange}18` : 'transparent', color: unusualOnly ? C.orange : C.dim, border: `1px solid ${unusualOnly ? C.orange + '40' : C.border}`, borderRadius: 5, cursor: 'pointer' }}>
          V/OI &gt; 3×
        </button>
        <span style={{ marginLeft: 'auto', color: C.dim, fontSize: 11, fontFamily: font }}>{filtered.length} contracts</span>
      </div>

      {/* Table */}
      <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 10, overflow: 'hidden' }}>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ background: `${C.border}50` }}>
                <TH c="underlying" label="Ticker" />
                <TH c="side" label="Side" />
                <TH c="strike" label="Strike" right />
                <TH c="expiration" label="Expiry" right />
                <TH c="volume" label="Volume" right />
                <TH c="openInterest" label="OI" right />
                <TH c="vol_oi_ratio" label="V/OI" right />
                <TH c="iv" label="IV" right />
                <TH c="delta" label="Δ Delta" right />
                <TH c="last" label="Last" right />
                <th style={{ padding: '7px 8px', fontSize: 9, fontFamily: font, textTransform: 'uppercase', color: C.dim, textAlign: 'right' }}>Bid/Ask</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((c: any, i: number) => {
                const voiNum = c.vol_oi_ratio != null ? parseFloat(c.vol_oi_ratio) : null;
                const ivNum = c.iv != null ? parseFloat(c.iv) : null;
                const dNum = c.delta != null ? parseFloat(c.delta) : null;
                return (
                  <tr key={i} style={{ borderTop: `1px solid ${C.border}` }}>
                    <td style={{ padding: '8px 8px', fontFamily: font, fontWeight: 700, fontSize: 12, color: C.bright }}>{c.underlying}</td>
                    <td style={{ padding: '8px 8px' }}><Badge color={sideColor(c.side)} sm>{c.side}</Badge></td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 12, color: sideColor(c.side), fontWeight: 700 }}>${c.strike}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.dim }}>{String(c.expiration || '').slice(5)}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 12, fontWeight: 700, color: C.bright }}>{fmtVol(c.volume)}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.text }}>{fmtVol(c.openInterest)}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, fontWeight: voiNum && voiNum > 3 ? 700 : 400, color: voiColor(voiNum) }}>{voiNum != null ? fmtNum(voiNum, 1) + '×' : '—'}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.yellow }}>{ivNum != null ? fmtPct(ivNum) : '—'}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, color: dNum != null && dNum > 0 ? C.green : C.red }}>{dNum != null ? fmtNum(dNum, 3) : '—'}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 11, color: C.bright }}>{c.last ?? '—'}</td>
                    <td style={{ padding: '8px 8px', textAlign: 'right', fontFamily: font, fontSize: 10, color: C.dim }}>{c.bid && c.ask ? `${c.bid} / ${c.ask}` : '—'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {visible.length === 0 && <div style={{ padding: 40, textAlign: 'center', color: C.dim, fontSize: 13, fontFamily: sans }}>No contracts match your filters.</div>}
        {filtered.length > limit && (
          <div style={{ borderTop: `1px solid ${C.border}`, padding: '10px 16px', display: 'flex', justifyContent: 'center' }}>
            <button onClick={() => setLimit(l => l + 100)} style={{ padding: '6px 18px', background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 6, color: C.blue, fontSize: 11, fontFamily: font, cursor: 'pointer' }}>
              Show more ({filtered.length - limit} remaining)
            </button>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── MAIN PAGE ───────────────────────────────────────────────────────────────────
type MainTab = 'tickers' | 'flow';

type ScanTab = 'megacap' | 'high_growth';
const SCAN_TAB_LABELS: Record<ScanTab, string> = { megacap: 'Megacap', high_growth: 'High Growth' };

export default function OptionsPage() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [loadStage, setLoadStage] = useState('Initializing live scan...');
  const [error, setError] = useState('');
  const [scanTab, setScanTab] = useState<ScanTab>('megacap');
  const [tab, setTab] = useState<MainTab>('tickers');
  const [chatInput, setChatInput] = useState('');
  const [chatMessages, setChatMessages] = useState<Array<{ role: 'user' | 'ai'; text: string }>>([]);
  const [chatLoading, setChatLoading] = useState(false);
  const chatBottomRef = useRef<HTMLDivElement>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const scanTabRef = useRef<ScanTab>(scanTab);
  scanTabRef.current = scanTab;

  const fetchDashboard = useCallback(async (tabOverride?: ScanTab) => {
    setLoading(true);
    setError('');
    const stages = ['Running live scan...', 'Scanning options chains...', 'Aggregating volume & OI...', 'Computing greeks & IV...', 'Building market summary...', 'Finalizing...'];
    let si = 0;
    setLoadStage(stages[0]);
    const stageTimer = setInterval(() => { si = Math.min(si + 1, stages.length - 1); setLoadStage(stages[si]); }, 2500);
    try {
      const res = await fetch('/api/options/dashboard', { method: 'POST', headers: authHeaders(), body: JSON.stringify({ tab: tabOverride ?? scanTabRef.current }) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e.message || 'Failed to load options dashboard');
    } finally {
      clearInterval(stageTimer);
      setLoading(false);
      setLoadStage('');
    }
  }, []);

  useEffect(() => {
    fetchDashboard();
    intervalRef.current = setInterval(() => fetchDashboard(), 120_000);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [fetchDashboard]);

  const switchScanTab = (newTab: ScanTab) => {
    if (newTab === scanTab || loading) return;
    setScanTab(newTab);
    setData(null);
    fetchDashboard(newTab);
  };

  useEffect(() => { chatBottomRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [chatMessages]);

  const askAgent = async () => {
    if (!chatInput.trim() || chatLoading) return;
    const q = chatInput.trim();
    setChatInput('');
    setChatMessages(prev => [...prev, { role: 'user', text: q }]);
    setChatLoading(true);
    try {
      const res = await fetch('/api/query', {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ query: q, preset_intent: 'options_flow', context_data: data, conversation_id: null, reasoning_model: 'claude' }),
      });
      const json = await res.json();
      const text = json.analysis || json.response?.analysis || json.structured?.summary || json.text || 'No response.';
      setChatMessages(prev => [...prev, { role: 'ai', text }]);
    } catch (e: any) {
      setChatMessages(prev => [...prev, { role: 'ai', text: `Error: ${e.message}` }]);
    } finally {
      setChatLoading(false);
    }
  };

  // ─── Extract data ──────────────────────────────────────────────────────────────
  const resp = data?.response || {};
  const tickers: any[] = resp.tickers || [];
  const allContracts: any[] = resp.all_contracts || [];
  const mktSum = resp.market_summary || {};
  const cacheAge: number | null = data?.cache_age_seconds ?? null;
  const fromCache: boolean = data?.from_cache ?? false;
  const hasData = !loading && tickers.length > 0;

  return (
    <div style={{ background: C.bg, minHeight: '100vh', fontFamily: sans }}>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        @keyframes pulse { 0%,100% { opacity:0.5; } 50% { opacity:1; } }
        @keyframes fadeIn { from { opacity:0; transform:translateY(4px); } to { opacity:1; transform:translateY(0); } }
      `}</style>

      {/* ── Header ── */}
      <div style={{ padding: '16px 20px 10px', borderBottom: `1px solid ${C.border}`, position: 'sticky', top: 0, background: C.bg, zIndex: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8, marginBottom: hasData ? 10 : 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Zap className="w-5 h-5" style={{ color: C.green }} />
            <span style={{ color: C.bright, fontSize: 17, fontWeight: 800, fontFamily: font, letterSpacing: '-0.02em' }}>OPTIONS FLOW</span>
            <span style={{ background: `${C.blue}15`, color: C.blue, border: `1px solid ${C.blue}30`, borderRadius: 4, padding: '2px 7px', fontSize: 10, fontWeight: 700, fontFamily: font }}>SCREENER</span>
            {fromCache && cacheAge != null && (
              <span style={{ color: C.dim, fontSize: 10, fontFamily: font }}>Updated {cacheAge}s ago</span>
            )}
          </div>
          <button onClick={fetchDashboard} disabled={loading} style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 14px', background: `${C.blue}12`, border: `1px solid ${C.blue}30`, borderRadius: 7, color: loading ? C.dim : C.blue, fontSize: 12, fontWeight: 600, fontFamily: font, cursor: loading ? 'not-allowed' : 'pointer' }}>
            <RefreshCw className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`} />
            {loading ? loadStage : 'Refresh'}
          </button>
        </div>

        {/* Scan tab switcher */}
        <div style={{ display: 'flex', gap: 4, marginBottom: 10 }}>
          {(['megacap', 'high_growth'] as ScanTab[]).map(t => (
            <button key={t} onClick={() => switchScanTab(t)} disabled={loading}
              style={{ padding: '5px 16px', fontSize: 11, fontWeight: 700, fontFamily: font, background: scanTab === t ? `${C.green}18` : 'transparent', color: scanTab === t ? C.green : C.dim, border: `1px solid ${scanTab === t ? C.green + '40' : C.border}`, borderRadius: 6, cursor: loading ? 'not-allowed' : 'pointer', opacity: loading && scanTab !== t ? 0.5 : 1, transition: 'all 0.15s ease' }}>
              {SCAN_TAB_LABELS[t]}
            </button>
          ))}
        </div>

        {/* Market summary banner */}
        {hasData && (
          <div style={{ display: 'flex', gap: 0, background: C.card, border: `1px solid ${C.border}`, borderRadius: 9, overflow: 'hidden', animation: 'fadeIn 0.4s ease', flexWrap: 'wrap' }}>
            {[
              { label: 'Call Vol', value: fmtVol(mktSum.total_call_volume), color: C.green },
              { label: 'Put Vol', value: fmtVol(mktSum.total_put_volume), color: C.red },
              { label: 'P/C Ratio', value: mktSum.market_pc_ratio != null ? fmtNum(mktSum.market_pc_ratio, 2) : '—', color: pcColor(mktSum.market_pc_ratio ?? 1) },
              { label: 'Contracts', value: mktSum.total_contracts != null ? mktSum.total_contracts.toLocaleString() : '—', color: C.blue },
              { label: 'Tickers', value: mktSum.tickers_scanned ?? tickers.length, color: C.text },
              { label: 'Most Active', value: mktSum.most_active_ticker ?? '—', color: C.gold },
            ].map((s, i) => (
              <div key={i} style={{ flex: 1, minWidth: 90, padding: '9px 14px', borderRight: i < 5 ? `1px solid ${C.border}` : 'none' }}>
                <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 3 }}>{s.label}</div>
                <div style={{ color: s.color, fontSize: 14, fontWeight: 700, fontFamily: font }}>{s.value}</div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── Body ── */}
      <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 140px)' }}>
        <div style={{ flex: 1, overflowY: 'auto', padding: '14px 20px' }}>

          {/* Loading */}
          {loading && !hasData && (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14, padding: '60px 20px' }}>
              <div style={{ width: 40, height: 40, border: `3px solid ${C.border}`, borderTop: `3px solid ${C.blue}`, borderRadius: '50%', animation: 'spin 0.8s linear infinite' }} />
              <div style={{ color: C.blue, fontSize: 13, fontFamily: font }}>{loadStage}</div>
              {[1, 2, 3].map(i => <Skeleton key={i} h={48} mb={0} />)}
            </div>
          )}

          {/* Error */}
          {error && !loading && (
            <div style={{ background: `${C.red}10`, border: `1px solid ${C.red}30`, borderRadius: 10, padding: '14px 18px', color: C.red, fontSize: 13, fontFamily: sans }}>⚠ {error}</div>
          )}

          {/* Data Ingestion Status */}
          {hasData && <DataIngestionWidget />}

          {/* Tabs */}
          {hasData && (
            <div style={{ animation: 'fadeIn 0.35s ease' }}>
              <div style={{ display: 'flex', gap: 6, marginBottom: 14 }}>
                {[
                  { id: 'tickers' as MainTab, label: `Ticker Summary`, count: tickers.length },
                  { id: 'flow' as MainTab, label: `Flow / Contracts`, count: allContracts.length },
                ].map(t => (
                  <button key={t.id} onClick={() => setTab(t.id)}
                    style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 16px', fontSize: 11, fontWeight: 600, fontFamily: font, background: tab === t.id ? `${C.blue}18` : 'transparent', color: tab === t.id ? C.blue : C.dim, border: `1px solid ${tab === t.id ? C.blue + '40' : C.border}`, borderRadius: 6, cursor: 'pointer' }}>
                    {t.label}
                    <span style={{ background: `${C.blue}25`, color: C.blue, borderRadius: 10, padding: '0 6px', fontSize: 9 }}>{t.count}</span>
                  </button>
                ))}
              </div>
              {tab === 'tickers' && <TickerSummaryTab tickers={tickers} />}
              {tab === 'flow' && <FlowTab contracts={allContracts} />}
            </div>
          )}
        </div>

        {/* ── Agent Chatbar ── */}
        <div style={{ borderTop: `1px solid ${C.border}`, background: C.card, padding: '10px 20px 14px', flexShrink: 0 }}>
          {chatMessages.length > 0 && (
            <div style={{ maxHeight: 180, overflowY: 'auto', marginBottom: 10, display: 'flex', flexDirection: 'column', gap: 8 }}>
              {chatMessages.map((m, i) => (
                <div key={i} style={{ display: 'flex', justifyContent: m.role === 'user' ? 'flex-end' : 'flex-start' }}>
                  <div style={{ maxWidth: '82%', padding: '8px 12px', borderRadius: 8, fontSize: 12, fontFamily: sans, lineHeight: 1.6, background: m.role === 'user' ? `${C.blue}18` : C.cardAlt, color: m.role === 'user' ? C.blue : C.text, border: `1px solid ${m.role === 'user' ? C.blue + '30' : C.border}` }}>
                    {m.text}
                  </div>
                </div>
              ))}
              {chatLoading && <div style={{ display: 'flex', alignItems: 'center', gap: 6, color: C.dim, fontSize: 11, fontFamily: font }}><Loader2 className="w-3 h-3 animate-spin" /> Analyzing...</div>}
              <div ref={chatBottomRef} />
            </div>
          )}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input value={chatInput} onChange={e => setChatInput(e.target.value)} onKeyDown={e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); askAgent(); } }}
              placeholder={hasData ? 'Ask about options flow, P/C ratios, IV skew, unusual volume...' : 'Loading data...'}
              disabled={chatLoading || !hasData}
              style={{ flex: 1, background: C.cardAlt, border: `1px solid ${C.border}`, borderRadius: 8, padding: '9px 14px', color: C.bright, fontSize: 12, fontFamily: sans, outline: 'none', opacity: hasData ? 1 : 0.5 }} />
            <button onClick={askAgent} disabled={chatLoading || !chatInput.trim() || !hasData}
              style={{ padding: '9px 14px', background: chatLoading || !chatInput.trim() || !hasData ? `${C.dim}18` : `${C.blue}20`, border: `1px solid ${chatLoading || !chatInput.trim() || !hasData ? C.border : C.blue + '40'}`, borderRadius: 8, color: chatLoading || !chatInput.trim() || !hasData ? C.dim : C.blue, cursor: chatLoading || !chatInput.trim() || !hasData ? 'not-allowed' : 'pointer', display: 'flex', alignItems: 'center', gap: 5 }}>
              {chatLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
