import { useState, useEffect, useCallback, useRef } from 'react';

const AGENT_BACKEND_URL = 'https://fast-api-server-trading-agent-aidanpilon.replit.app';
const AGENT_API_KEY = 'hippo_ak_7f3x9k2m4p8q1w5t';

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
];

interface HistoryEntry {
  id: string;
  timestamp: number;
  content: string;
  display_type: string | null;
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
        labels.set(id, `${dateStr} \u2014 ${i + 1}`);
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

  const fetchHistory = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/history`);
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
    const parts: string[] = [];
    if (cat) parts.push(cat.title);
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
      </div>
    );
  }

  function renderIntents() {
    if (view.level !== 'intents') return null;
    const cat = CATEGORIES.find(c => c.id === view.categoryId);
    if (!cat) return null;

    return (
      <div style={{ padding: '8px 12px', display: 'flex', flexDirection: 'column', gap: 3 }}>
        {cat.intents.map(i => {
          const entries = getEntriesForIntent(cat.id, i.intent);
          return (
            <button
              key={i.intent}
              onClick={() => {
                if (entries.length > 0) {
                  setView({ level: 'entries', categoryId: cat.id, intent: i.intent, label: i.label });
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
          <span style={{ color: C.bright, fontSize: 11, fontFamily: sansFont }}>{label}</span>
          <span style={{ color: C.dim, fontSize: 9, fontFamily: font }}>
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

  function renderDetail() {
    if (view.level !== 'detail') return null;
    const { entry } = view;

    // Try to parse as JSON for structured display
    let parsed: any = null;
    try { parsed = JSON.parse(entry.content); } catch { /* plain text */ }

    const displayContent = parsed?.analysis || parsed?.structured?.message || parsed?.message || entry.content;

    return (
      <div style={{ padding: 16, flex: 1, overflowY: 'auto' }}>
        <div style={{ color: C.dim, fontSize: 9, fontFamily: font, marginBottom: 12 }}>
          {new Date(entry.timestamp * 1000).toLocaleString()}
        </div>
        <div style={{
          color: C.text, fontSize: 12, fontFamily: sansFont, lineHeight: 1.7,
          whiteSpace: 'pre-wrap', wordBreak: 'break-word',
          padding: 14, background: C.card, border: `1px solid ${C.border}`,
          borderRadius: 8, maxHeight: 'calc(100vh - 260px)', overflowY: 'auto',
        }}>
          {displayContent}
        </div>
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
