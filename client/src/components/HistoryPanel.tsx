import { useState, useEffect, useCallback, useRef } from 'react';
import { normalizeHistoryBuckets, normalizeNewHistoryApiResponse } from '@/lib/history';
const AGENT_BACKEND_URL = 'https://fast-api-server-trading-agent-aidanpilon.replit.app';
const AGENT_API_KEY = 'hippo_ak_7f3x9k2m4p8q1w5t';
function getToken(): string | null {
  return localStorage.getItem('caelyn_token') || sessionStorage.getItem('caelyn_token');
}
function authHeaders(): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json', 'X-API-Key': AGENT_API_KEY };
  const t = getToken();
  if (t) h['Authorization'] = `Bearer ${t}`;
  return h;
}
// Human-readable category title map (used for both predefined and dynamic categories)
const CATEGORY_TITLE_MAP: Record<string, string> = {
  terminal: 'Terminal Chat', overview: 'Overview', trades: 'Trades & Ideas',
  fundamental: 'Fundamental', sectors: 'Sectors', ta_screener: 'TA Screener',
  earnings_agent: 'Earnings', prediction_markets: 'Prediction Markets',
  news_intelligence: 'NotifAI',
};
function categoryTitle(id: string): string {
  if (CATEGORY_TITLE_MAP[id]) return CATEGORY_TITLE_MAP[id];
  return id.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}
// Section order: Terminal Chat first, NotifAI last
const CATEGORIES: { id: string; title: string; intents: { label: string; intent: string }[] }[] = [
  { id: 'terminal', title: 'Terminal Chat', intents: [
    { label: 'Terminal Query', intent: 'freeform_query' },
  ]},
  { id: 'overview', title: 'Overview', intents: [
    { label: 'Daily Briefing', intent: 'daily_briefing' },
    { label: 'Macro Overview', intent: 'macro_overview' },
    { label: 'Headlines', intent: 'headlines' },
    { label: 'Upcoming Catalysts', intent: 'upcoming_catalysts' },
    { label: 'Trending Now', intent: 'trending_now' },
    { label: 'Social Momentum', intent: 'social_momentum' },
    { label: 'Sector Rotation', intent: 'sector_rotation' },
  ]},
  { id: 'trades', title: 'Trades & Ideas', intents: [
    { label: 'Best Trades', intent: 'best_trades' },
    { label: 'Best Investments', intent: 'best_investments' },
    { label: 'Asymmetric R:R', intent: 'asymmetric_rr' },
    { label: 'Small Cap Spec', intent: 'small_cap_spec' },
    { label: 'Short Squeeze', intent: 'short_squeeze' },
  ]},
  { id: 'fundamental', title: 'Fundamental', intents: [
    { label: 'Fundamental Leaders', intent: 'fundamental_leaders' },
    { label: 'Rapidly Improving', intent: 'rapidly_improving' },
    { label: 'Earnings Watch', intent: 'earnings_watch' },
    { label: 'Insider Buying', intent: 'insider_buying' },
    { label: 'Revenue Reaccelerating', intent: 'revenue_reaccelerating' },
    { label: 'Margin Expansion', intent: 'margin_expansion' },
    { label: 'Undervalued Growth', intent: 'undervalued_growth' },
    { label: 'Institutional Accumulation', intent: 'institutional_accumulation' },
    { label: 'Free Cash Flow Leaders', intent: 'free_cash_flow_leaders' },
  ]},
  { id: 'sectors', title: 'Sectors', intents: [
    { label: 'Crypto', intent: 'crypto' },
    { label: 'Commodities', intent: 'commodities' },
    { label: 'Energy', intent: 'energy' },
    { label: 'Materials', intent: 'materials' },
    { label: 'Aerospace/Defense', intent: 'aerospace_defense' },
    { label: 'Tech', intent: 'tech' },
    { label: 'AI/Compute', intent: 'ai_compute' },
    { label: 'Quantum', intent: 'quantum' },
    { label: 'Fintech', intent: 'fintech' },
    { label: 'Biotech', intent: 'biotech' },
    { label: 'Real Estate', intent: 'real_estate' },
  ]},
  { id: 'ta_screener', title: 'TA Screener', intents: [
    { label: 'Oversold+Growing', intent: 'oversold_growing' },
    { label: 'Value+Momentum', intent: 'value_momentum' },
    { label: 'Insider+Breakout', intent: 'insider_breakout' },
    { label: 'High Growth Small Cap', intent: 'high_growth_sc' },
    { label: 'Dividend Value', intent: 'dividend_value' },
    { label: 'Stage 2 Breakouts', intent: 'technical_stage2' },
    { label: 'Bullish Breakouts', intent: 'technical_bullish_breakouts' },
    { label: 'Bearish Breakdowns', intent: 'technical_breakdowns' },
    { label: 'Bearish Setups', intent: 'technical_bearish_setups' },
    { label: 'Oversold Bounces', intent: 'technical_oversold' },
    { label: 'Overbought Warnings', intent: 'technical_overbought' },
    { label: 'Crossover Signals', intent: 'technical_crossovers' },
    { label: 'Momentum Shifts', intent: 'momentum_shift_scan' },
    { label: 'Volume & Movers', intent: 'volume_movers_scan' },
  ]},
  { id: 'earnings_agent', title: 'Earnings', intents: [
    { label: 'Earnings Agent', intent: 'earnings_agent' },
  ]},
  { id: 'prediction_markets', title: 'Prediction Markets', intents: [
    { label: 'Prediction Markets', intent: 'prediction_markets' },
  ]},
  { id: 'news_intelligence', title: 'NotifAI', intents: [
    { label: 'NotifAI', intent: 'news_intelligence' },
  ]},
];
interface TickerBacktest {
  ticker: string;
  rec_price: number;
  current_price: number;
  pct_change: number;
}
interface BacktestData {
  cumulative_pct: number;
  ticker_count: number;
  details: TickerBacktest[];
}
interface HistoryEntry {
  id: string;
  timestamp: number;
  content: string;
  display_type: string | null;
  model_used?: string;
  tickers?: { ticker: string; rec_price: number | null; current_price?: number | null; pct_change?: number | null }[];
  conversation?: { role: string; content: string }[];
}
interface HistoryData {
  [key: string]: {
    category: string;
    intent: string;
    entries: HistoryEntry[];
  };
}
type View =
  | { level: 'categories' }
  | { level: 'intents'; categoryId: string }
  | { level: 'entries'; categoryId: string; intent: string; label: string }
  | { level: 'detail'; categoryId: string; intent: string; label: string; entry: HistoryEntry; entryLabel: string };
const font = "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace";
const sansFont = "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
const C = {
  bg: '#0b0c10', card: '#111318', border: '#1a1d25', text: '#c9cdd6', bright: '#e8eaef',
  dim: '#6b7280', green: '#22c55e', red: '#ef4444', blue: '#3b82f6', gold: '#f59e0b',
  purple: '#a78bfa',
};
function formatDate(ts: number): string {
  const d = new Date(ts * 1000);
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}
function buildEntryLabels(entries: HistoryEntry[]): Map<string, string> {
  const labels = new Map<string, string>();
  const dateCounts = new Map<string, number>();
  for (const e of entries) {
    const dateStr = formatDate(e.timestamp);
    const count = (dateCounts.get(dateStr) || 0) + 1;
    dateCounts.set(dateStr, count);
    labels.set(e.id, count > 1 ? `${dateStr} \u2014 ${count}` : dateStr);
  }
  const dateFirstSeen = new Map<string, string[]>();
  for (const e of entries) {
    const dateStr = formatDate(e.timestamp);
    if (!dateFirstSeen.has(dateStr)) dateFirstSeen.set(dateStr, []);
    dateFirstSeen.get(dateStr)!.push(e.id);
  }
  for (const [dateStr, ids] of dateFirstSeen) {
    if (ids.length > 1) {
      ids.forEach((id, i) => {
        labels.set(id, `${dateStr} \u2014 ${i + 1}`);
      });
    }
  }
  return labels;
}
function VirtualList({ items, renderItem, itemHeight = 40 }: { items: any[]; renderItem: (item: any, index: number) => React.ReactNode; itemHeight?: number }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [containerHeight, setContainerHeight] = useState(400);
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    setContainerHeight(el.clientHeight);
    const resizeObs = new ResizeObserver(() => setContainerHeight(el.clientHeight));
    resizeObs.observe(el);
    return () => resizeObs.disconnect();
  }, []);
  if (items.length <= 20) {
    return <div>{items.map((item, i) => renderItem(item, i))}</div>;
  }
  const totalHeight = items.length * itemHeight;
  const startIdx = Math.max(0, Math.floor(scrollTop / itemHeight) - 2);
  const endIdx = Math.min(items.length, Math.ceil((scrollTop + containerHeight) / itemHeight) + 2);
  const visibleItems = items.slice(startIdx, endIdx);
  return (
    <div
      ref={containerRef}
      onScroll={(e) => setScrollTop((e.target as HTMLDivElement).scrollTop)}
      style={{ overflowY: 'auto', flex: 1 }}
    >
      <div style={{ height: totalHeight, position: 'relative' }}>
        {visibleItems.map((item, i) => (
          <div key={startIdx + i} style={{ position: 'absolute', top: (startIdx + i) * itemHeight, width: '100%', height: itemHeight }}>
            {renderItem(item, startIdx + i)}
          </div>
        ))}
      </div>
    </div>
  );
}
// Human-readable model name for display
function modelLabel(model?: string): string {
  if (!model) return '';
  const m = model.toLowerCase();
  if (m.includes('perplexity')) return 'Perplexity';
  if (m.includes('grok')) return 'Grok';
  if (m.includes('gemini')) return 'Gemini';
  if (m.includes('openai') || m.includes('gpt') || m.includes('o1') || m.includes('o3') || m.includes('o4')) return 'OpenAI';
  if (m.includes('claude') || m.includes('sonnet') || m.includes('opus') || m.includes('haiku')) return 'Claude';
  if (m.includes('deepseek')) return 'DeepSeek';
  if (m === 'agent_collab') return '';
  return model.charAt(0).toUpperCase() + model.slice(1);
}
// Extracts a readable summary from the raw backend JSON envelope.
// Covers every display_type the backend uses — never falls back to raw JSON.
function extractSummary(parsed: any): string {
  if (!parsed || typeof parsed !== 'object') return '';
  const s = parsed.structured || {};
  // Priority 1: non-empty top-level analysis field
  if (typeof parsed.analysis === 'string' && parsed.analysis.trim()) {
    return parsed.analysis.trim().slice(0, 500);
  }
  // Priority 2: briefing display_type — market_pulse.summary + verdict prefix
  if (s.market_pulse?.summary) {
    const verdict = typeof s.market_pulse.verdict === 'string' ? `${s.market_pulse.verdict} — ` : '';
    return `${verdict}${s.market_pulse.summary}`.slice(0, 500);
  }
  // Priority 3: common summary/context fields across most display_types
  for (const key of ['summary', 'market_context', 'market_overview', 'description', 'message', 'overview']) {
    const v = s[key];
    if (typeof v === 'string' && v.trim()) return v.trim().slice(0, 500);
  }
  // Priority 4: nested macro regime or source coverage descriptions
  if (s.macro_regime?.summary) return String(s.macro_regime.summary).slice(0, 500);
  if (s.market_regime?.summary) return String(s.market_regime.summary).slice(0, 500);
  return '';
}
// Extracts top ticker symbols from any structured response shape.
// Tries all known array fields — no client-side price math.
function extractTickers(parsed: any): string[] {
  if (!parsed || typeof parsed !== 'object') return [];
  const s = parsed.structured || {};
  const candidates = [
    s.picks, s.top_trades, s.trending_tickers, s.stocks, s.opportunities,
    s.top_conviction_plays, s.top_setups, s.recommendations,
    s.equities?.large_caps, s.equities?.mid_caps, s.equities?.small_micro_caps,
  ];
  for (const src of candidates) {
    if (Array.isArray(src) && src.length > 0) {
      const tickers = src.slice(0, 6)
        .map((p: any) => p.ticker || p.symbol || p.asset || '')
        .filter(Boolean);
      if (tickers.length > 0) return tickers;
    }
  }
  return [];
}
export function HistoryPanel({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const [history, setHistory] = useState<HistoryData>({});
  const [loading, setLoading] = useState(false);
  const [view, setView] = useState<View>({ level: 'categories' });
  const [backtestMap, setBacktestMap] = useState<Record<string, BacktestData>>({});
  const [backtestLoading, setBacktestLoading] = useState(false);
  const fetchHistory = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history`, { headers: authHeaders() });
      if (res.ok) {
        const data = await res.json();
        // Detect new format { items: [...], recent: [...], total_count: N }
        if (data && typeof data === 'object' && (Array.isArray(data.items) || Array.isArray(data.recent))) {
          setHistory(normalizeNewHistoryApiResponse(data) as HistoryData);
        } else {
          setHistory(normalizeHistoryBuckets(data) as HistoryData);
        }
      }
    } catch (e) {
      console.error('[HISTORY] fetch error:', e);
    } finally {
      setLoading(false);
    }
  }, []);
  const fetchBacktest = useCallback(async () => {
    setBacktestLoading(true);
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history/backtest-summary`, { headers: authHeaders() });
      if (res.ok) {
        const data = await res.json();
        setBacktestMap(data.backtest || {});
      }
    } catch (e) {
      console.error('[HISTORY] backtest fetch error:', e);
    } finally {
      setBacktestLoading(false);
    }
  }, []);
  const deleteHistoryEntry = useCallback(async (category: string, intent: string, entryId: string) => {
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history/${encodeURIComponent(category)}/${encodeURIComponent(intent)}/${encodeURIComponent(entryId)}`, {
        method: 'DELETE',
        headers: authHeaders(),
      });
      if (!res.ok) throw new Error(`Delete failed: ${res.status}`);
      await fetchHistory();
    } catch (e) {
      console.error('[HISTORY] delete entry error:', e);
    }
  }, [fetchHistory]);

  const deleteHistoryIntent = useCallback(async (category: string, intent: string) => {
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history/${encodeURIComponent(category)}/${encodeURIComponent(intent)}`, {
        method: 'DELETE',
        headers: authHeaders(),
      });
      if (!res.ok) throw new Error(`Delete intent failed: ${res.status}`);
      await fetchHistory();
    } catch (e) {
      console.error('[HISTORY] delete intent error:', e);
    }
  }, [fetchHistory]);
  useEffect(() => {
    if (isOpen) {
      setView({ level: 'categories' });
      fetchHistory();
      fetchBacktest();
    }
  }, [isOpen, fetchHistory, fetchBacktest]);
  if (!isOpen) return null;
  function getEntriesForIntent(categoryId: string, intent: string): HistoryEntry[] {
    const key = `${categoryId}::${intent}`;
    return history[key]?.entries || [];
  }
  function countForCategory(cat: typeof CATEGORIES[0]): number {
    return cat.intents.reduce((sum, i) => sum + getEntriesForIntent(cat.id, i.intent).length, 0);
  }
  function goBack() {
    if (view.level === 'detail') {
      setView({ level: 'entries', categoryId: view.categoryId, intent: view.intent, label: view.label });
    } else if (view.level === 'entries') {
      setView({ level: 'intents', categoryId: view.categoryId });
    } else if (view.level === 'intents') {
      setView({ level: 'categories' });
    }
  }
  function renderBreadcrumb() {
    if (view.level === 'categories') return null;
    const cat = CATEGORIES.find(c => c.id === (view as any).categoryId);
    const catTitle = cat?.title || categoryTitle((view as any).categoryId || '');
    const parts: string[] = [];
    if ((view as any).categoryId) parts.push(catTitle);
    if (view.level === 'entries' || view.level === 'detail') parts.push(view.label);
    if (view.level === 'detail') parts.push(view.entryLabel);
    return (
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '8px 16px', borderBottom: `1px solid ${C.border}`, background: C.bg }}>
        <button onClick={goBack} style={{ background: 'transparent', border: 'none', color: C.blue, cursor: 'pointer', fontSize: 11, fontFamily: font, padding: 0 }}>
          &larr; Back
        </button>
        <span style={{ color: C.dim, fontSize: 10, fontFamily: font, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {parts.join(' / ')}
        </span>
      </div>
    );
  }
  // Computes a section-level gain/loss % by averaging backend-provided cumulative_pct
  // values from backtestMap across all entries in the category. Returns null if no data.
  function sectionPerf(cat: typeof CATEGORIES[0]): number | null {
    const vals: number[] = [];
    for (const intent of cat.intents) {
      const entries = getEntriesForIntent(cat.id, intent.intent);
      for (const entry of entries) {
        const bt = backtestMap[entry.id];
        if (bt != null && typeof bt.cumulative_pct === 'number' && isFinite(bt.cumulative_pct)) {
          vals.push(bt.cumulative_pct);
        } else {
          // Also use inline ticker data if backtest map doesn't have this entry
          const priced = entry.tickers?.filter(t => typeof t.pct_change === 'number' && t.pct_change != null) || [];
          if (priced.length > 0) {
            const avg = priced.reduce((s, t) => s + t.pct_change!, 0) / priced.length;
            vals.push(avg);
          }
        }
      }
    }
    if (vals.length === 0) return null;
    return vals.reduce((s, v) => s + v, 0) / vals.length;
  }

  function renderCategories() {
    const knownCatIds = new Set(CATEGORIES.map(c => c.id));
    const dynamicCats: { id: string; title: string; count: number }[] = [];
    for (const key of Object.keys(history)) {
      const catId = key.split('::')[0];
      if (!knownCatIds.has(catId)) {
        const existing = dynamicCats.find(d => d.id === catId);
        const entries = history[key]?.entries?.length || 0;
        if (existing) { existing.count += entries; } else { dynamicCats.push({ id: catId, title: categoryTitle(catId), count: entries }); }
      }
    }
    return (
      <div style={{ padding: '8px 12px', display: 'flex', flexDirection: 'column', gap: 4 }}>
        {CATEGORIES.map(cat => {
          const count = countForCategory(cat);
          const perf = sectionPerf(cat);
          return (
            <button
              key={cat.id}
              onClick={() => setView({ level: 'intents', categoryId: cat.id })}
              style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                padding: '10px 12px', background: C.card, border: `1px solid ${C.border}`,
                borderRadius: 8, cursor: 'pointer', transition: 'all 0.15s', width: '100%',
                textAlign: 'left',
              }}
              className="panel-btn"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontSize: 7, color: C.dim }}>&#9654;</span>
                <span style={{ color: C.bright, fontSize: 12, fontWeight: 600, fontFamily: sansFont }}>{cat.title}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                {count > 0 && (
                  <span style={{ color: C.blue, fontSize: 10, fontWeight: 600, fontFamily: font, padding: '2px 8px', background: `${C.blue}12`, borderRadius: 10 }}>
                    {count}
                  </span>
                )}
                {count > 0 && (
                  perf != null
                    ? <span style={{ color: perf >= 0 ? C.green : C.red, fontSize: 10, fontWeight: 700, fontFamily: font, minWidth: 40, textAlign: 'right' }}>
                        {perf >= 0 ? '+' : ''}{perf.toFixed(1)}%
                      </span>
                    : <span style={{ color: C.dim, fontSize: 10, fontFamily: font, opacity: 0.5 }}>—</span>
                )}
              </div>
            </button>
          );
        })}
        {dynamicCats.map(cat => (
          <button
            key={cat.id}
            onClick={() => setView({ level: 'intents', categoryId: cat.id })}
            style={{
              display: 'flex', alignItems: 'center', justifyContent: 'space-between',
              padding: '10px 12px', background: C.card, border: `1px solid ${C.border}`,
              borderRadius: 8, cursor: 'pointer', transition: 'all 0.15s', width: '100%',
              textAlign: 'left',
            }}
            className="panel-btn"
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: 7, color: C.dim }}>&#9654;</span>
              <span style={{ color: C.bright, fontSize: 12, fontWeight: 600, fontFamily: sansFont }}>{cat.title}</span>
            </div>
            {cat.count > 0 && (
              <span style={{ color: C.blue, fontSize: 10, fontWeight: 600, fontFamily: font, padding: '2px 8px', background: `${C.blue}12`, borderRadius: 10 }}>
                {cat.count}
              </span>
            )}
          </button>
        ))}
      </div>
    );
  }
  function renderIntents() {
    if (view.level !== 'intents') return null;
    const cat = CATEGORIES.find(c => c.id === view.categoryId);
    const intents = cat ? cat.intents : Object.keys(history)
      .filter(key => key.startsWith(`${view.categoryId}::`))
      .map(key => {
        const intent = key.split('::')[1];
        return { label: categoryTitle(intent), intent };
      });
    return (
      <div style={{ padding: '8px 12px', display: 'flex', flexDirection: 'column', gap: 3 }}>
        {intents.map(i => {
          const entries = getEntriesForIntent(view.categoryId, i.intent);
          return (
            <div key={i.intent} style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
              <button
                onClick={() => {
                  if (entries.length > 0) {
                    setView({ level: 'entries', categoryId: view.categoryId, intent: i.intent, label: i.label });
                  }
                }}
                style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '8px 12px', background: entries.length > 0 ? C.card : `${C.card}80`,
                  border: `1px solid ${C.border}`, borderRadius: 6, cursor: entries.length > 0 ? 'pointer' : 'default',
                  transition: 'all 0.15s', width: '100%', textAlign: 'left',
                  opacity: entries.length > 0 ? 1 : 0.4,
                }}
                className={entries.length > 0 ? 'panel-btn' : ''}
              >
                <span style={{ color: entries.length > 0 ? C.bright : C.dim, fontSize: 11, fontFamily: sansFont }}>{i.label}</span>
                {entries.length > 0 && (
                  <span style={{ color: C.dim, fontSize: 9, fontFamily: font }}>{entries.length}</span>
                )}
              </button>
              {entries.length > 0 && (
                <button
                  onClick={(e) => { e.stopPropagation(); deleteHistoryIntent(view.categoryId, i.intent); }}
                  title="Delete all entries"
                  style={{ background: 'transparent', border: `1px solid ${C.border}`, borderRadius: 6, color: C.dim, cursor: 'pointer', padding: '6px 8px', fontSize: 10, fontFamily: font }}
                  className="panel-btn"
                >
                  Del
                </button>
              )}
            </div>
          );
        })}
      </div>
    );
  }
  function renderEntries() {
    if (view.level !== 'entries') return null;
    const entries = getEntriesForIntent(view.categoryId, view.intent);
    if (entries.length === 0) {
      return <div style={{ padding: 16, color: C.dim, fontSize: 11, fontFamily: font, textAlign: 'center' }}>No history yet</div>;
    }
    const renderItem = (entry: HistoryEntry) => {
      const d = new Date(entry.timestamp * 1000);
      const dateStr = d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
      const timeStr = d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
      const model = modelLabel(entry.model_used);
      const bt = backtestMap[entry.id];
      let labelParts = `${dateStr} ${timeStr}`;
      if (model) labelParts += ` ${model}`;

      // Compute avg % from inline tickers if backtest map doesn't have it
      let pctBadge: number | null = null;
      if (bt) {
        pctBadge = bt.cumulative_pct;
      } else {
        const priced = entry.tickers?.filter(t => t.pct_change != null) || [];
        if (priced.length > 0) {
          pctBadge = priced.reduce((s, t) => s + t.pct_change!, 0) / priced.length;
        }
      }

      // Compute a short preview text from the stored content without exposing raw JSON
      let previewText = '';
      try {
        const p = JSON.parse(entry.content);
        const s = extractSummary(p);
        previewText = s.slice(0, 90) + (s.length > 90 ? '…' : '');
      } catch { /* plain text */ }

      return (
        <div
          key={entry.id}
          onClick={() => setView({ level: 'detail', categoryId: view.categoryId, intent: view.intent, label: view.label, entry, entryLabel: labelParts })}
          style={{
            padding: '8px 12px', margin: '0 12px 3px', background: C.card,
            border: `1px solid ${C.border}`, borderRadius: 6, cursor: 'pointer',
            transition: 'all 0.15s', width: 'calc(100% - 24px)',
            boxSizing: 'border-box',
          }}
          className="panel-btn"
          role="button"
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <span style={{ color: C.bright, fontSize: 11, fontFamily: sansFont, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>
              {labelParts}
            </span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 8, flexShrink: 0 }}>
              {pctBadge != null && (
                <span style={{ color: pctBadge >= 0 ? C.green : C.red, fontSize: 11, fontWeight: 700, fontFamily: font }}>
                  {pctBadge >= 0 ? '+' : ''}{pctBadge.toFixed(1)}%
                </span>
              )}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  deleteHistoryEntry(view.categoryId, view.intent, entry.id);
                }}
                title="Delete entry"
                style={{ background: 'transparent', border: `1px solid ${C.border}`, borderRadius: 4, color: C.dim, fontSize: 9, padding: '2px 6px', cursor: 'pointer' }}
                className="panel-btn"
              >
                Del
              </button>
            </div>
          </div>
          {previewText && (
            <div style={{ color: C.dim, fontSize: 10, fontFamily: sansFont, marginTop: 3, lineHeight: 1.4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {previewText}
            </div>
          )}
        </div>
      );
    };
    if (entries.length > 20) {
      return (
        <VirtualList
          items={entries}
          renderItem={(item) => renderItem(item)}
          itemHeight={39}
        />
      );
    }
    return <div style={{ padding: '8px 0' }}>{entries.map(renderItem)}</div>;
  }
  function renderDetail() {
    if (view.level !== 'detail') return null;
    const { entry } = view;
    const bt = backtestMap[entry.id];

    // Parse stored content — never expose raw JSON as the default display
    let parsed: any = null;
    try { parsed = JSON.parse(entry.content); } catch { /* plain text content */ }

    const summary = parsed ? extractSummary(parsed) : entry.content;
    const topTickers = parsed ? extractTickers(parsed) : [];
    const intentLabel = view.label ||
      CATEGORIES.flatMap(c => c.intents).find(i => i.intent === view.intent)?.label || view.intent;

    // Performance — prefer backtest map, fall back to inline ticker data from API response
    const inlineTickers = entry.tickers?.filter(
      t => t.rec_price != null && t.current_price != null && t.pct_change != null
    ) || [];
    const hasBt = bt && bt.details.length > 0;
    const hasInline = inlineTickers.length > 0;
    const rows = hasBt
      ? bt.details
      : inlineTickers.map(t => ({
          ticker: t.ticker, rec_price: t.rec_price!,
          current_price: t.current_price!, pct_change: t.pct_change!,
        }));
    const totalTickers = hasBt ? bt.ticker_count : rows.length;
    const cumulativePct = hasBt
      ? bt.cumulative_pct
      : rows.length > 0 ? rows.reduce((s, r) => s + r.pct_change, 0) / rows.length : null;

    const conversation = (entry as any).conversation as { role: string; content: string }[] | undefined;

    return (
      <div style={{ padding: 16, flex: 1, overflowY: 'auto' }}>

        {/* ── Header card: title / timestamp / model / perf / tickers ── */}
        <div style={{
          marginBottom: 12, padding: 12, background: '#0d0f14',
          border: `1px solid ${C.border}`, borderRadius: 8,
        }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 8 }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ color: C.bright, fontSize: 13, fontWeight: 700, fontFamily: sansFont, marginBottom: 3 }}>
                {intentLabel}
              </div>
              <div style={{ color: C.dim, fontSize: 10, fontFamily: font }}>
                {new Date(entry.timestamp * 1000).toLocaleString('en-US', {
                  month: 'long', day: 'numeric', year: 'numeric',
                  hour: 'numeric', minute: '2-digit',
                })}
                {entry.model_used && modelLabel(entry.model_used) && (
                  <span style={{ marginLeft: 8, color: C.purple }}>{modelLabel(entry.model_used)}</span>
                )}
              </div>
            </div>
            {cumulativePct != null && (
              <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: 12 }}>
                <div style={{ color: C.dim, fontSize: 9, fontFamily: font, textTransform: 'uppercase', letterSpacing: 1, marginBottom: 2 }}>Since Issued</div>
                <div style={{ color: cumulativePct >= 0 ? C.green : C.red, fontSize: 18, fontWeight: 800, fontFamily: font, lineHeight: 1 }}>
                  {cumulativePct >= 0 ? '+' : ''}{cumulativePct.toFixed(1)}%
                </div>
                {totalTickers > 0 && (
                  <div style={{ color: C.dim, fontSize: 9, fontFamily: font, marginTop: 2 }}>{totalTickers} tickers</div>
                )}
              </div>
            )}
          </div>
          {entry.query && (
            <div style={{ color: C.dim, fontSize: 10, fontFamily: sansFont, fontStyle: 'italic', marginBottom: topTickers.length > 0 ? 8 : 0 }}>
              "{entry.query}"
            </div>
          )}
          {topTickers.length > 0 && (
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
              {topTickers.map(t => (
                <span key={t} style={{
                  padding: '2px 7px', background: `${C.blue}15`,
                  border: `1px solid ${C.blue}30`, borderRadius: 4,
                  color: C.blue, fontSize: 10, fontWeight: 700, fontFamily: font,
                }}>
                  {t}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* ── Summary text — the main readable content ── */}
        {summary && (
          <div style={{
            marginBottom: 12, padding: '12px 14px', background: C.card,
            border: `1px solid ${C.border}`, borderRadius: 8,
            color: C.text, fontSize: 12, fontFamily: sansFont, lineHeight: 1.75,
            whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          }}>
            {summary}
          </div>
        )}

        {/* ── Price tracking table ── */}
        {rows.length > 0 && (
          <div style={{ marginBottom: 12, padding: 10, background: '#0d0f14', border: `1px solid ${C.border}`, borderRadius: 6 }}>
            <div style={{ color: C.dim, fontSize: 9, fontFamily: font, marginBottom: 6, textTransform: 'uppercase', letterSpacing: 1 }}>
              Price Tracking
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 10, fontFamily: font }}>
              <thead>
                <tr style={{ color: C.dim }}>
                  <td style={{ padding: '3px 6px' }}>Ticker</td>
                  <td style={{ padding: '3px 6px', textAlign: 'right' }}>Rec</td>
                  <td style={{ padding: '3px 6px', textAlign: 'right' }}>Now</td>
                  <td style={{ padding: '3px 6px', textAlign: 'right' }}>%</td>
                </tr>
              </thead>
              <tbody>
                {rows.map(d => (
                  <tr key={d.ticker}>
                    <td style={{ padding: '3px 6px', color: C.bright, fontWeight: 600 }}>{d.ticker}</td>
                    <td style={{ padding: '3px 6px', textAlign: 'right', color: C.dim }}>${d.rec_price.toFixed(2)}</td>
                    <td style={{ padding: '3px 6px', textAlign: 'right', color: C.text }}>${d.current_price.toFixed(2)}</td>
                    <td style={{ padding: '3px 6px', textAlign: 'right', fontWeight: 700, color: d.pct_change >= 0 ? C.green : C.red }}>
                      {d.pct_change >= 0 ? '+' : ''}{d.pct_change.toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {cumulativePct != null && (
              <div style={{ marginTop: 6, paddingTop: 6, borderTop: `1px solid ${C.border}`, display: 'flex', justifyContent: 'space-between', fontSize: 10, fontFamily: font }}>
                <span style={{ color: C.dim }}>Avg ({totalTickers} tickers)</span>
                <span style={{ fontWeight: 700, color: cumulativePct >= 0 ? C.green : C.red }}>
                  {cumulativePct >= 0 ? '+' : ''}{cumulativePct.toFixed(1)}%
                </span>
              </div>
            )}
          </div>
        )}

        {/* ── Conversation thread (Terminal Chat / freeform entries) ── */}
        {conversation && conversation.length > 0 && (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {conversation.map((msg, i) => {
              let msgContent = msg.content;
              if (msg.role === 'assistant') {
                try {
                  const p = JSON.parse(msg.content);
                  // Use extractSummary — never fall back to raw JSON
                  msgContent = extractSummary(p) || p.analysis || msg.content;
                } catch { /* already plain text */ }
              }
              return (
                <div key={i} style={{
                  padding: 12, borderRadius: 8,
                  background: msg.role === 'user' ? '#1a1d25' : C.card,
                  border: `1px solid ${msg.role === 'user' ? '#2a2d35' : C.border}`,
                }}>
                  <div style={{ color: msg.role === 'user' ? C.blue : C.purple, fontSize: 9, fontFamily: font, marginBottom: 4, textTransform: 'uppercase' }}>
                    {msg.role === 'user' ? 'You' : 'Agent'}
                  </div>
                  <div style={{ color: C.text, fontSize: 12, fontFamily: sansFont, lineHeight: 1.7, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                    {msgContent}
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* ── Fallback: show plain text content only if nothing else rendered ── */}
        {!summary && rows.length === 0 && !conversation?.length && !parsed && (
          <div style={{ color: C.text, fontSize: 12, fontFamily: sansFont, lineHeight: 1.7, whiteSpace: 'pre-wrap', wordBreak: 'break-word', padding: 14, background: C.card, border: `1px solid ${C.border}`, borderRadius: 8 }}>
            {entry.content}
          </div>
        )}

        {/* ── Raw Data — hidden by default, expandable for debugging ── */}
        {parsed && (
          <details style={{ marginTop: 12 }}>
            <summary style={{ color: C.dim, fontSize: 9, fontFamily: font, cursor: 'pointer', userSelect: 'none', textTransform: 'uppercase', letterSpacing: 1, padding: '4px 0' }}>
              Raw Data
            </summary>
            <pre style={{
              marginTop: 8, padding: 10, background: '#0d0f14',
              border: `1px solid ${C.border}`, borderRadius: 6,
              color: C.dim, fontSize: 9, fontFamily: font,
              overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
            }}>
              {JSON.stringify(parsed, null, 2)}
            </pre>
          </details>
        )}
      </div>
    );
  }
  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/70 backdrop-blur-md" />
      <div
        className="relative bg-[#060709] border border-white/[0.08] rounded-2xl w-full max-w-[600px] max-h-[85vh] flex flex-col shadow-2xl"
        onClick={(e) => e.stopPropagation()}
        style={{ minHeight: 400 }}
      >
        <div style={{
          display: 'flex', alignItems: 'center', justifyContent: 'space-between',
          padding: '14px 20px', borderBottom: `1px solid ${C.border}`, flexShrink: 0,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div style={{
              width: 36, height: 36, background: 'linear-gradient(135deg, #a78bfa 0%, #7c3aed 100%)',
              borderRadius: 10, display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 16,
            }}>
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
                <polyline points="14,2 14,8 20,8"/>
                <line x1="16" y1="13" x2="8" y2="13"/>
                <line x1="16" y1="17" x2="8" y2="17"/>
                <polyline points="10,9 9,9 8,9"/>
              </svg>
            </div>
            <div>
              <h1 style={{ color: C.bright, fontSize: 15, fontWeight: 700, fontFamily: sansFont, margin: 0 }}>History</h1>
              <p style={{ color: C.dim, fontSize: 10, fontFamily: sansFont, margin: 0 }}>Past prompt responses</p>
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: 'transparent', border: `1px solid ${C.border}`, borderRadius: 8,
              color: C.dim, cursor: 'pointer', width: 32, height: 32,
              display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 14,
              fontFamily: font, transition: 'all 0.15s',
            }}
            className="panel-btn"
          >
            x
          </button>
        </div>
        {renderBreadcrumb()}
        <div style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}>
          {loading ? (
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 40 }}>
              <span style={{ color: C.dim, fontSize: 11, fontFamily: font }}>Loading history...</span>
            </div>
          ) : (
            <>
              {view.level === 'categories' && renderCategories()}
              {view.level === 'intents' && renderIntents()}
              {view.level === 'entries' && renderEntries()}
              {view.level === 'detail' && renderDetail()}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
