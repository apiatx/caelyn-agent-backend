import { useState, useEffect, useCallback, useRef } from 'react';

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
  overview: 'Overview', trades: 'Trades & Ideas', fundamental: 'Fundamental',
  sectors: 'Sectors', ta_screener: 'TA Screener',
  earnings_agent: 'Earnings Agent', prediction_markets: 'Prediction Markets',
  news_intelligence: 'NotifAI', terminal: 'Terminal',
};

function categoryTitle(id: string): string {
  if (CATEGORY_TITLE_MAP[id]) return CATEGORY_TITLE_MAP[id];
  return id.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

// Must match TradingAgent.tsx promptGroups exactly
const CATEGORIES: { id: string; title: string; intents: { label: string; intent: string }[] }[] = [
  { id: 'overview', title: 'Overview', intents: [
    { label: 'Daily Briefing', intent: 'daily_briefing' },
    { label: 'Macro Overview', intent: 'macro_outlook' },
    { label: 'Headlines', intent: 'news_leaders' },
    { label: 'Upcoming Catalysts', intent: 'catalyst_scan' },
    { label: 'Trending Now', intent: 'cross_asset_trending' },
    { label: 'Social Momentum', intent: 'social_momentum_scan' },
    { label: 'Sector Rotation', intent: 'sector_rotation' },
  ]},
  { id: 'trades', title: 'Trades & Ideas', intents: [
    { label: 'Best Trades', intent: 'best_trades' },
    { label: 'Best Investments', intent: 'long_term_conviction' },
    { label: 'Asymmetric R:R', intent: 'microcap_asymmetry' },
    { label: 'Small Cap Spec', intent: 'microcap_spec' },
    { label: 'Short Squeeze', intent: 'short_squeeze_scan' },
  ]},
  { id: 'fundamental', title: 'Fundamental', intents: [
    { label: 'Fundamental Leaders', intent: 'fundamental_leaders' },
    { label: 'Rapidly Improving', intent: 'fundamental_acceleration' },
    { label: 'Earnings Watch', intent: 'earnings_watch' },
    { label: 'Insider Buying', intent: 'insider_buying' },
    { label: 'Revenue Reaccelerating', intent: 'revenue_reaccelerating' },
    { label: 'Margin Expansion', intent: 'margin_expansion' },
    { label: 'Undervalued Growth', intent: 'undervalued_growth' },
    { label: 'Institutional Accumulation', intent: 'institutional_accumulation' },
    { label: 'Free Cash Flow Leaders', intent: 'free_cash_flow_leaders' },
  ]},
  { id: 'sectors', title: 'Sectors', intents: [
    { label: 'Crypto', intent: 'crypto_focus' },
    { label: 'Commodities', intent: 'commodities_focus' },
    { label: 'Energy', intent: 'sector_energy' },
    { label: 'Materials', intent: 'sector_materials' },
    { label: 'Aerospace/Defense', intent: 'sector_defense' },
    { label: 'Tech', intent: 'sector_tech' },
    { label: 'AI/Compute', intent: 'sector_ai' },
    { label: 'Quantum', intent: 'sector_quantum' },
    { label: 'Fintech', intent: 'sector_financials' },
    { label: 'Biotech', intent: 'sector_healthcare' },
    { label: 'Real Estate', intent: 'sector_real_estate' },
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
  { id: 'earnings_agent', title: 'Earnings Agent', intents: [
    { label: 'Earnings Agent', intent: 'earnings_agent' },
  ]},
  { id: 'prediction_markets', title: 'Prediction Markets', intents: [
    { label: 'Prediction Markets', intent: 'prediction_markets' },
  ]},
  { id: 'news_intelligence', title: 'NotifAI', intents: [
    { label: 'NotifAI', intent: 'news_intelligence' },
  ]},
  { id: 'terminal', title: 'Terminal', intents: [
    { label: 'Terminal Query', intent: 'freeform_query' },
  ]},
];

interface HistoryEntry {
  id: string;
  timestamp: number;
  content: string;
  display_type: string | null;
  model_used?: string;
  query?: string;
}

interface BacktestItem {
  ticker: string;
  recommended_price: number;
  recommended_date: string;
}

interface BacktestResult {
  ticker: string;
  recommended_price: number;
  recommended_date: string;
  current_price: number;
  pct_change: number;
  direction: string;
}

interface BacktestResponse {
  results: BacktestResult[];
  summary: string;
  model_used: string;
  as_of: string;
}

function parsePrice(val: any): number | null {
  if (val == null) return null;
  const s = String(val).replace(/[$,]/g, '').trim();
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function extractBacktestItems(entry: HistoryEntry): BacktestItem[] {
  let parsed: any = null;
  try { parsed = JSON.parse(entry.content); } catch { return []; }

  const recDate = new Date(entry.timestamp * 1000).toISOString().split('T')[0];
  const items: BacktestItem[] = [];
  const structured = parsed?.structured;

  // trades: structured.top_trades[].ticker + .entry
  if (structured?.top_trades && Array.isArray(structured.top_trades)) {
    for (const t of structured.top_trades) {
      const price = parsePrice(t.entry || t.price || t.entry_price);
      if (t.ticker && price) items.push({ ticker: t.ticker, recommended_price: price, recommended_date: recDate });
    }
  }

  // investments: structured.picks[].ticker + .price
  if (structured?.picks && Array.isArray(structured.picks)) {
    for (const p of structured.picks) {
      const price = parsePrice(p.price || p.entry || p.entry_price);
      if (p.ticker && price) items.push({ ticker: p.ticker, recommended_price: price, recommended_date: recDate });
    }
  }

  // analysis: structured.ticker + structured.price
  if (structured?.ticker && !items.length) {
    const price = parsePrice(structured.price || structured.entry || structured.current_price);
    if (price) items.push({ ticker: structured.ticker, recommended_price: price, recommended_date: recDate });
  }

  // Also check top-level arrays (some responses nest differently)
  if (!items.length && parsed?.top_trades && Array.isArray(parsed.top_trades)) {
    for (const t of parsed.top_trades) {
      const price = parsePrice(t.entry || t.price || t.entry_price);
      if (t.ticker && price) items.push({ ticker: t.ticker, recommended_price: price, recommended_date: recDate });
    }
  }
  if (!items.length && parsed?.picks && Array.isArray(parsed.picks)) {
    for (const p of parsed.picks) {
      const price = parsePrice(p.price || p.entry || p.entry_price);
      if (p.ticker && price) items.push({ ticker: p.ticker, recommended_price: price, recommended_date: recDate });
    }
  }

  return items;
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

function modelDisplayName(m: string): string {
  if (m === 'agent_collab') return 'Agent Collab';
  if (m === 'gpt-4o') return 'GPT-4o';
  return m.charAt(0).toUpperCase() + m.slice(1);
}

function buildEntryLabels(entries: HistoryEntry[]): Map<string, string> {
  const labels = new Map<string, string>();
  const dateCounts = new Map<string, number>();
  for (const e of entries) {
    const dateStr = formatDate(e.timestamp);
    const count = (dateCounts.get(dateStr) || 0) + 1;
    dateCounts.set(dateStr, count);
    const modelSuffix = e.model_used ? ` \u2014 ${modelDisplayName(e.model_used)}` : '';
    labels.set(e.id, count > 1 ? `${dateStr} \u2014 ${count}${modelSuffix}` : `${dateStr}${modelSuffix}`);
  }
  // Fix: re-number so first occurrence of a date with duplicates also gets a number
  const dateFirstSeen = new Map<string, string[]>();
  for (const e of entries) {
    const dateStr = formatDate(e.timestamp);
    if (!dateFirstSeen.has(dateStr)) dateFirstSeen.set(dateStr, []);
    dateFirstSeen.get(dateStr)!.push(e.id);
  }
  for (const [dateStr, ids] of dateFirstSeen) {
    if (ids.length > 1) {
      ids.forEach((id, i) => {
        const entry = entries.find(e => e.id === id);
        const modelSuffix = entry?.model_used ? ` \u2014 ${modelDisplayName(entry.model_used)}` : '';
        labels.set(id, `${dateStr} \u2014 ${i + 1}${modelSuffix}`);
      });
    }
  }
  return labels;
}

// Virtual scrolling for lists > 20 items
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

export function HistoryPanel({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const [history, setHistory] = useState<HistoryData>({});
  const [loading, setLoading] = useState(false);
  const [view, setView] = useState<View>({ level: 'categories' });
  const [backtestLoading, setBacktestLoading] = useState(false);
  const [backtestResult, setBacktestResult] = useState<BacktestResponse | null>(null);
  const [backtestError, setBacktestError] = useState<string | null>(null);
  const [backtestEntryId, setBacktestEntryId] = useState<string | null>(null);

  const fetchHistory = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history`, { headers: authHeaders() });
      if (res.ok) {
        const data = await res.json();
        setHistory(data);
      }
    } catch (e) {
      console.error('[HISTORY] fetch error:', e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (isOpen) {
      setView({ level: 'categories' });
      fetchHistory();
    }
  }, [isOpen, fetchHistory]);

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

  function renderCategories() {
    // Collect any dynamic categories from API not already in CATEGORIES
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
              {count > 0 && (
                <span style={{ color: C.blue, fontSize: 10, fontWeight: 600, fontFamily: font, padding: '2px 8px', background: `${C.blue}12`, borderRadius: 10 }}>
                  {count}
                </span>
              )}
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
    // For dynamic categories not in CATEGORIES, build intents from history data
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
            <button
              key={i.intent}
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

    const labels = buildEntryLabels(entries);

    const renderItem = (entry: HistoryEntry) => {
      const label = labels.get(entry.id) || formatDate(entry.timestamp);
      return (
        <button
          key={entry.id}
          onClick={() => setView({ level: 'detail', categoryId: view.categoryId, intent: view.intent, label: view.label, entry, entryLabel: label })}
          style={{
            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
            padding: '8px 12px', margin: '0 12px 3px', background: C.card,
            border: `1px solid ${C.border}`, borderRadius: 6, cursor: 'pointer',
            transition: 'all 0.15s', width: 'calc(100% - 24px)', textAlign: 'left',
            boxSizing: 'border-box',
          }}
          className="panel-btn"
        >
          <span style={{ color: C.bright, fontSize: 11, fontFamily: sansFont, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{label}</span>
          <span style={{ color: C.dim, fontSize: 9, fontFamily: font, flexShrink: 0, marginLeft: 8 }}>
            {new Date(entry.timestamp * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
          </span>
        </button>
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

  async function runBacktest(entry: HistoryEntry) {
    const items = extractBacktestItems(entry);
    if (items.length === 0) { setBacktestError('No tickers with prices found in this entry'); return; }
    setBacktestLoading(true);
    setBacktestError(null);
    setBacktestResult(null);
    setBacktestEntryId(entry.id);
    try {
      const modelLabel = entry.model_used ? modelDisplayName(entry.model_used) : 'unknown';
      const res = await fetch(`${AGENT_BACKEND_URL}/api/backtest`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ items, model_used: modelLabel }),
      });
      if (!res.ok) throw new Error(`Backtest failed (${res.status})`);
      const data: BacktestResponse = await res.json();
      setBacktestResult(data);
    } catch (e: any) {
      setBacktestError(e.message || 'Backtest request failed');
    } finally {
      setBacktestLoading(false);
    }
  }

  function renderBacktestResults() {
    if (!backtestResult && !backtestLoading && !backtestError) return null;
    const isCurrentEntry = view.level === 'detail' && backtestEntryId === view.entry.id;
    if (!isCurrentEntry) return null;

    if (backtestLoading) {
      return (
        <div style={{ padding: '12px 14px', background: `${C.bg}cc`, border: `1px solid ${C.border}`, borderRadius: 8, marginTop: 10 }}>
          <span style={{ color: C.dim, fontSize: 10, fontFamily: font }}>Running backtest...</span>
        </div>
      );
    }

    if (backtestError) {
      return (
        <div style={{ padding: '10px 14px', background: `${C.red}08`, border: `1px solid ${C.red}30`, borderRadius: 8, marginTop: 10 }}>
          <span style={{ color: C.red, fontSize: 10, fontFamily: font }}>{backtestError}</span>
        </div>
      );
    }

    if (!backtestResult) return null;
    const { results, summary, as_of } = backtestResult;

    return (
      <div style={{ marginTop: 10, border: `1px solid ${C.border}`, borderRadius: 8, overflow: 'hidden', background: C.card }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '8px 12px', borderBottom: `1px solid ${C.border}`, background: C.bg }}>
          <span style={{ color: C.bright, fontSize: 10, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', letterSpacing: '0.04em' }}>Backtest Results</span>
          <button onClick={() => { setBacktestResult(null); setBacktestEntryId(null); }} style={{ background: 'transparent', border: 'none', color: C.dim, cursor: 'pointer', fontSize: 10, fontFamily: font, padding: '2px 4px' }}>dismiss</button>
        </div>
        {/* Table header */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', padding: '6px 12px', borderBottom: `1px solid ${C.border}`, background: `${C.bg}80` }}>
          <span style={{ color: C.dim, fontSize: 8, fontWeight: 700, fontFamily: font, textTransform: 'uppercase' }}>Ticker</span>
          <span style={{ color: C.dim, fontSize: 8, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', textAlign: 'right' }}>Rec Price</span>
          <span style={{ color: C.dim, fontSize: 8, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', textAlign: 'right' }}>Now</span>
          <span style={{ color: C.dim, fontSize: 8, fontWeight: 700, fontFamily: font, textTransform: 'uppercase', textAlign: 'right' }}>% Change</span>
        </div>
        {/* Table rows */}
        {results.map((r, i) => {
          const color = r.direction === 'gain' ? C.green : r.direction === 'loss' ? C.red : C.dim;
          return (
            <div key={i} style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr 1fr', padding: '5px 12px', borderBottom: i < results.length - 1 ? `1px solid ${C.border}` : 'none' }}>
              <span style={{ color: C.bright, fontSize: 10, fontWeight: 600, fontFamily: font }}>{r.ticker}</span>
              <span style={{ color: C.dim, fontSize: 10, fontFamily: font, textAlign: 'right' }}>${r.recommended_price.toFixed(2)}</span>
              <span style={{ color: C.text, fontSize: 10, fontFamily: font, textAlign: 'right' }}>${r.current_price.toFixed(2)}</span>
              <span style={{ color, fontSize: 10, fontWeight: 700, fontFamily: font, textAlign: 'right' }}>
                {r.pct_change >= 0 ? '+' : ''}{r.pct_change.toFixed(1)}%
              </span>
            </div>
          );
        })}
        {/* Summary */}
        {summary && (
          <div style={{ padding: '8px 12px', borderTop: `1px solid ${C.border}`, background: `${C.bg}60` }}>
            <span style={{ color: C.text, fontSize: 10, fontFamily: sansFont, lineHeight: 1.5 }}>{summary}</span>
          </div>
        )}
        {/* As-of timestamp */}
        {as_of && (
          <div style={{ padding: '4px 12px 6px', textAlign: 'right' }}>
            <span style={{ color: C.dim, fontSize: 8, fontFamily: font }}>Prices as of {new Date(as_of).toLocaleString()}</span>
          </div>
        )}
      </div>
    );
  }

  function renderDetail() {
    if (view.level !== 'detail') return null;
    const { entry } = view;

    // Try to parse as JSON for structured display
    let parsed: any = null;
    try { parsed = JSON.parse(entry.content); } catch { /* plain text */ }

    const displayContent = parsed?.analysis || parsed?.structured?.message || parsed?.message || entry.content;
    const backtestItems = extractBacktestItems(entry);
    const canBacktest = backtestItems.length > 0;

    return (
      <div style={{ padding: 16, flex: 1, overflowY: 'auto' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
          <span style={{ color: C.dim, fontSize: 9, fontFamily: font }}>
            {new Date(entry.timestamp * 1000).toLocaleString()}
          </span>
          {entry.model_used && (
            <span style={{
              fontSize: 8, fontWeight: 700, fontFamily: font, textTransform: 'uppercase',
              color: entry.model_used === 'agent_collab' ? '#a78bfa' : C.blue,
              background: entry.model_used === 'agent_collab' ? 'rgba(139,92,246,0.12)' : `${C.blue}12`,
              border: `1px solid ${entry.model_used === 'agent_collab' ? 'rgba(139,92,246,0.3)' : C.blue + '30'}`,
              borderRadius: 6, padding: '2px 6px',
            }}>
              {entry.model_used === 'agent_collab' ? 'Agent Collab' : entry.model_used}
            </span>
          )}
          {canBacktest && (
            <button
              onClick={() => runBacktest(entry)}
              disabled={backtestLoading && backtestEntryId === entry.id}
              style={{
                marginLeft: 'auto', padding: '3px 10px', borderRadius: 6, fontSize: 9, fontWeight: 700,
                fontFamily: font, background: `${C.gold}15`, color: C.gold,
                border: `1px solid ${C.gold}40`, cursor: backtestLoading ? 'wait' : 'pointer',
                transition: 'all 0.15s', textTransform: 'uppercase', letterSpacing: '0.04em',
              }}
            >
              {backtestLoading && backtestEntryId === entry.id ? 'Testing...' : 'Backtest'}
            </button>
          )}
        </div>
        {entry.query && (
          <div style={{
            color: C.bright, fontSize: 11, fontFamily: font, padding: '8px 12px',
            background: 'rgba(255,255,255,0.03)', border: `1px solid ${C.border}`,
            borderRadius: 6, marginBottom: 10,
          }}>
            <span style={{ color: C.dim, fontSize: 8, textTransform: 'uppercase', fontWeight: 700, letterSpacing: '0.05em' }}>Query: </span>
            {entry.query}
          </div>
        )}
        <div style={{
          color: C.text, fontSize: 12, fontFamily: sansFont, lineHeight: 1.7,
          whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          padding: 14, background: C.card, border: `1px solid ${C.border}`,
          borderRadius: 8, maxHeight: 'calc(100vh - 340px)', overflowY: 'auto',
        }}>
          {displayContent}
        </div>
        {renderBacktestResults()}
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
        {/* Header */}
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

        {/* Breadcrumb */}
        {renderBreadcrumb()}

        {/* Content */}
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
