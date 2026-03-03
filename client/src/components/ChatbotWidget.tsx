import { useState, useRef, useEffect } from 'react';
import { useLocation } from 'wouter';
import { useChatbot } from '@/contexts/ChatbotContext';
import cryptoHippoLogo from '@assets/image_1771549651056.png';
import caelynLogo from '@assets/image_1771541162366.png';

const C = {
  bg: '#0b0c10', card: '#111318', border: '#1a1d25', text: '#c9cdd6', bright: '#e8eaef',
  dim: '#6b7280', green: '#22c55e', red: '#ef4444', blue: '#3b82f6', gold: '#f59e0b',
  purple: '#a78bfa',
};
const font = "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace";
const sansFont = "'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";

function convColor(c?: string) { return c === 'High' ? C.green : c === 'Medium' ? C.gold : C.red; }
function changeColor(s?: string) { return (parseFloat(s || '0') >= 0) ? C.green : C.red; }
function trendColor(s?: string) { if (!s) return C.dim; if (s.includes('↑') || s.toLowerCase().includes('bullish') || s.toLowerCase().includes('above')) return C.green; if (s.includes('↓') || s.toLowerCase().includes('bearish') || s.toLowerCase().includes('below')) return C.red; return C.text; }

function formatAnalysis(text: string) {
  if (!text) return '';
  return text.replace(/^---+$/gm, '').replace(/^# (.*?)$/gm, `<div style="color:${C.bright};font-weight:800;font-size:15px;margin:12px 0 6px;font-family:${sansFont}">$1</div>`).replace(/^## (.*?)$/gm, `<div style="color:${C.bright};font-weight:700;font-size:14px;margin:10px 0 5px;font-family:${sansFont}">$1</div>`).replace(/^### (.*?)$/gm, `<div style="color:${C.blue};font-weight:700;font-size:12px;margin:8px 0 4px;font-family:${sansFont}">$1</div>`).replace(/\*\*(.*?)\*\*/g, `<span style="color:${C.bright};font-weight:700">$1</span>`).replace(/\n/g, '<br/>');
}

function Badge({ children, color }: { children: React.ReactNode, color: string }) {
  return <span style={{ display:'inline-block', padding:'2px 7px', borderRadius:4, fontSize:9, fontWeight:700, fontFamily:font, color, background:`${color}12`, border:`1px solid ${color}25`, letterSpacing:'0.04em', textTransform:'uppercase' }}>{children}</span>;
}

function StatRow({ label, value, color }: { label: string, value?: string, color?: string }) {
  if (!value) return null;
  return <div style={{ display:'flex', justifyContent:'space-between', padding:'4px 0', borderBottom:`1px solid ${C.border}` }}>
    <span style={{ color:C.dim, fontSize:10, fontFamily:font }}>{label}</span>
    <span style={{ color: color || trendColor(value), fontSize:11, fontWeight:600, fontFamily:font }}>{value}</span>
  </div>;
}

function IndicatorPill({ label, value, signal }: { label: string, value?: string|number, signal?: string }) {
  return <div style={{ background:C.bg, borderRadius:6, padding:'7px 9px', border:`1px solid ${C.border}` }}>
    <div style={{ color:C.dim, fontSize:8, fontFamily:font, textTransform:'uppercase', letterSpacing:'0.06em', marginBottom:2 }}>{label}</div>
    <div style={{ color:C.bright, fontSize:13, fontWeight:700, fontFamily:font }}>{value ?? 'N/A'}</div>
    {signal && <div style={{ color:trendColor(signal), fontSize:9, fontFamily:font, marginTop:1 }}>{signal}</div>}
  </div>;
}

function CardWrap({ children, borderColor }: { children: React.ReactNode, borderColor?: string }) {
  return <div style={{ background:C.card, border:`1px solid ${C.border}`, borderLeft:`3px solid ${borderColor || C.border}`, borderRadius:8, overflow:'hidden' }}>{children}</div>;
}

function MiniRenderer({ structured, analysis }: { structured: any, analysis: string }) {
  const s = structured || {};
  const dt = s.display_type;

  if (dt === 'trades' || dt === 'trending' || dt === 'investments') {
    const picks = s.picks || [];
    return <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
      {s.market_context && <div style={{ padding:'8px 10px', background:`${C.blue}08`, border:`1px solid ${C.blue}15`, borderRadius:6, color:C.text, fontSize:10, fontFamily:sansFont, lineHeight:1.5 }}>{s.market_context}</div>}
      {picks.slice(0, 5).map((p: any, i: number) => (
        <CardWrap key={i} borderColor={convColor(p.conviction)}>
          <div style={{ padding:'8px 10px' }}>
            <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:3 }}>
              <span style={{ width:18, height:18, borderRadius:'50%', background:`${C.blue}15`, display:'inline-flex', alignItems:'center', justifyContent:'center', color:C.blue, fontSize:8, fontWeight:800, fontFamily:font, flexShrink:0 }}>{i+1}</span>
              <span style={{ color:C.blue, fontWeight:800, fontSize:13, fontFamily:font }}>{p.ticker}</span>
              <span style={{ color:C.dim, fontSize:10 }}>{p.company}</span>
              <span style={{ color:changeColor(p.change), fontWeight:600, fontSize:11, fontFamily:font }}>{p.price} {p.change}</span>
            </div>
            <div style={{ color:C.text, fontSize:10, lineHeight:1.4, fontFamily:sansFont, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{p.thesis || p.investment_thesis}</div>
          </div>
          <div style={{ padding:'3px 10px', background:`${convColor(p.conviction)}08`, borderTop:`1px solid ${C.border}`, color:convColor(p.conviction), fontSize:8, fontWeight:700, fontFamily:font, textTransform:'uppercase' }}>{p.conviction} CONVICTION</div>
        </CardWrap>
      ))}
    </div>;
  }

  if (dt === 'briefing') {
    const pulse = s.market_pulse || {};
    const numbers = s.key_numbers || {};
    return <div>
      <div style={{ padding:'10px 12px', background:C.card, border:`1px solid ${C.border}`, borderRadius:8, marginBottom:6 }}>
        <div style={{ color:C.green, fontSize:14, fontWeight:800, fontFamily:sansFont, marginBottom:4 }}>{pulse.verdict}</div>
        {pulse.summary && <div style={{ color:C.text, fontSize:10, lineHeight:1.5, fontFamily:sansFont }}>{pulse.summary}</div>}
      </div>
      {Object.keys(numbers).length > 0 && <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(100px, 1fr))', gap:4 }}>
        {Object.entries(numbers).slice(0, 8).map(([key, val]: [string, any]) => (
          <div key={key} style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:'6px 8px' }}>
            <div style={{ color:C.dim, fontSize:7, fontFamily:font, textTransform:'uppercase', marginBottom:2 }}>{key.replace(/_/g, ' ')}</div>
            <div style={{ color:C.bright, fontSize:12, fontWeight:700, fontFamily:font }}>{val?.price || val?.value || 'N/A'}</div>
          </div>
        ))}
      </div>}
    </div>;
  }

  if (dt === 'crypto') {
    const momentum = s.top_momentum || [];
    const btcEth = s.btc_eth_summary || {};
    return <div>
      {s.market_overview && <div style={{ padding:'8px 10px', background:`${C.purple}08`, border:`1px solid ${C.purple}20`, borderRadius:6, marginBottom:6, color:C.text, fontSize:10, fontFamily:sansFont, lineHeight:1.5 }}>{s.market_overview}</div>}
      {(btcEth.btc || btcEth.eth) && <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:6, marginBottom:6 }}>
        {['btc', 'eth'].map(key => {
          const d = btcEth[key];
          if (!d) return null;
          return <div key={key} style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:8 }}>
            <div style={{ display:'flex', justifyContent:'space-between', marginBottom:3 }}>
              <span style={{ color:key === 'btc' ? '#f7931a' : '#627eea', fontWeight:800, fontSize:12, fontFamily:font }}>{key.toUpperCase()}</span>
              <span style={{ color:C.bright, fontSize:13, fontWeight:700, fontFamily:font }}>{d.price}</span>
            </div>
            <div style={{ display:'flex', gap:6, fontSize:10, fontFamily:font }}>
              <span style={{ color:changeColor(d.change_24h), fontWeight:600 }}>{d.change_24h}</span>
              {d.dominance && <span style={{ color:C.dim }}>Dom: <span style={{ color:C.bright }}>{d.dominance}</span></span>}
            </div>
          </div>;
        })}
      </div>}
      {momentum.slice(0, 3).map((c: any, i: number) => (
        <CardWrap key={i} borderColor={convColor(c.conviction)}>
          <div style={{ padding:'8px 10px', display:'flex', alignItems:'center', gap:6 }}>
            <span style={{ color:C.purple, fontWeight:800, fontSize:12, fontFamily:font }}>{c.symbol}</span>
            <span style={{ color:C.bright, fontSize:12, fontWeight:700, fontFamily:font }}>{c.price}</span>
            <span style={{ color:changeColor(c.change_24h), fontWeight:600, fontSize:10, fontFamily:font }}>{c.change_24h}</span>
            <Badge color={convColor(c.conviction)}>{c.conviction}</Badge>
          </div>
        </CardWrap>
      ))}
    </div>;
  }

  if (dt === 'sector_rotation') {
    const sectors = s.sectors || [];
    return <div>
      {s.summary && <div style={{ padding:'8px 10px', background:`${C.blue}08`, border:`1px solid ${C.blue}15`, borderRadius:6, marginBottom:6, color:C.text, fontSize:10, fontFamily:sansFont, lineHeight:1.5 }}>{s.summary}</div>}
      <div style={{ display:'grid', gridTemplateColumns:'repeat(auto-fill, minmax(110px, 1fr))', gap:4 }}>
        {sectors.slice(0, 11).map((sec: any, i: number) => {
          const changeStr = sec.change_today != null ? (typeof sec.change_today === 'number' ? `${sec.change_today >= 0 ? '+' : ''}${sec.change_today}%` : sec.change_today) : '—';
          const isPos = parseFloat(changeStr) >= 0;
          return <div key={i} style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:8 }}>
            <div style={{ color:C.blue, fontWeight:700, fontSize:10, fontFamily:font }}>{sec.etf}</div>
            <div style={{ color: isPos ? C.green : C.red, fontSize:14, fontWeight:700, fontFamily:font }}>{changeStr}</div>
            <div style={{ color:C.dim, fontSize:8, fontFamily:font }}>{sec.sector}</div>
            {sec.trend && <div style={{ color: sec.trend.includes('Stage 2') ? C.green : sec.trend.includes('Stage 4') ? C.red : C.gold, fontSize:7, fontFamily:font, marginTop:2 }}>{sec.trend}</div>}
          </div>;
        })}
      </div>
    </div>;
  }

  if (dt === 'analysis') {
    return <div>
      <div style={{ display:'flex', alignItems:'center', gap:8, padding:'8px 10px', background:C.card, border:`1px solid ${C.border}`, borderRadius:6, marginBottom:6 }}>
        <span style={{ color:C.blue, fontWeight:800, fontSize:16, fontFamily:font }}>{s.ticker}</span>
        <span style={{ color:C.dim, fontSize:10 }}>{s.company}</span>
        <span style={{ color:changeColor(s.change), fontWeight:700, fontSize:12, fontFamily:font }}>{s.price} {s.change}</span>
      </div>
      {s.verdict && <div style={{ padding:'6px 10px', background:`${C.green}08`, borderRadius:6, marginBottom:6, color:C.bright, fontSize:11, fontWeight:600, fontFamily:sansFont }}>{s.verdict}</div>}
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:4 }}>
        {s.ta && <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:8 }}>
          <div style={{ color:C.blue, fontSize:9, fontWeight:700, fontFamily:font, textTransform:'uppercase', marginBottom:4 }}>Technical</div>
          {Object.entries(s.ta).slice(0, 4).map(([k,v]) => <StatRow key={k} label={k.replace(/_/g,' ')} value={v as string} />)}
        </div>}
        {s.fundamentals && <div style={{ background:C.card, border:`1px solid ${C.border}`, borderRadius:6, padding:8 }}>
          <div style={{ color:C.green, fontSize:9, fontWeight:700, fontFamily:font, textTransform:'uppercase', marginBottom:4 }}>Fundamentals</div>
          {Object.entries(s.fundamentals).slice(0, 4).map(([k,v]) => <StatRow key={k} label={k.replace(/_/g,' ')} value={v as string} />)}
        </div>}
      </div>
    </div>;
  }

  if (dt === 'fundamentals' || dt === 'technicals') {
    const picks = s.picks || [];
    return <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
      {picks.slice(0, 5).map((p: any, i: number) => (
        <CardWrap key={i} borderColor={convColor(p.conviction)}>
          <div style={{ padding:'8px 10px' }}>
            <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:3 }}>
              <span style={{ width:18, height:18, borderRadius:'50%', background:`${C.blue}15`, display:'inline-flex', alignItems:'center', justifyContent:'center', color:C.blue, fontSize:8, fontWeight:800, fontFamily:font, flexShrink:0 }}>{i+1}</span>
              <span style={{ color:C.blue, fontWeight:800, fontSize:13, fontFamily:font }}>{p.ticker}</span>
              <span style={{ color:C.dim, fontSize:10 }}>{p.company}</span>
              <span style={{ color:changeColor(p.change), fontWeight:600, fontSize:11, fontFamily:font }}>{p.price} {p.change}</span>
            </div>
            {p.headline && <div style={{ color:C.gold, fontSize:10, fontWeight:600, fontFamily:sansFont }}>{p.headline}</div>}
            {p.setup_name && <div style={{ color:C.gold, fontSize:10, fontWeight:600, fontFamily:sansFont }}>{p.setup_name}</div>}
          </div>
          <div style={{ padding:'3px 10px', background:`${convColor(p.conviction)}08`, borderTop:`1px solid ${C.border}`, color:convColor(p.conviction), fontSize:8, fontWeight:700, fontFamily:font, textTransform:'uppercase' }}>{p.conviction} CONVICTION</div>
        </CardWrap>
      ))}
    </div>;
  }

  if (dt === 'earnings_catalyst') {
    const upcoming = s.upcoming || [];
    return <div style={{ display:'flex', flexDirection:'column', gap:6 }}>
      {upcoming.slice(0, 5).map((e: any, i: number) => (
        <CardWrap key={i}>
          <div style={{ padding:'8px 10px', display:'flex', alignItems:'center', gap:6 }}>
            <span style={{ color:C.blue, fontWeight:800, fontSize:12, fontFamily:font }}>{e.ticker}</span>
            <span style={{ color:C.dim, fontSize:10 }}>{e.company}</span>
            <Badge color={C.gold}>{e.earnings_date} ({e.days_away}d)</Badge>
          </div>
        </CardWrap>
      ))}
    </div>;
  }

  if (dt === 'screener') {
    const topPicks = s.top_picks || [];
    return <div>
      {s.summary && <div style={{ padding:'8px 10px', background:`${C.purple}08`, borderRadius:6, marginBottom:6, color:C.text, fontSize:10, fontFamily:sansFont, lineHeight:1.5 }}>{s.summary}</div>}
      {topPicks.slice(0, 4).map((pick: any, i: number) => (
        <div key={i} style={{ padding:'8px 10px', background:C.card, border:`1px solid ${C.purple}25`, borderRadius:6, borderLeft:`3px solid ${C.purple}`, marginBottom:4 }}>
          <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:3 }}>
            <span style={{ color:C.gold, fontWeight:800, fontSize:12, fontFamily:font }}>#{i+1}</span>
            <span style={{ color:C.blue, fontWeight:800, fontSize:12, fontFamily:font }}>{pick.ticker}</span>
            <span style={{ color:C.bright, fontSize:12, fontWeight:700, fontFamily:font }}>{pick.price}</span>
            <span style={{ color:changeColor(pick.change), fontSize:10, fontWeight:600, fontFamily:font }}>{pick.change}</span>
          </div>
          <div style={{ color:C.text, fontSize:10, lineHeight:1.4, fontFamily:sansFont, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>{pick.analysis || pick.thesis}</div>
        </div>
      ))}
    </div>;
  }

  if (dt === 'portfolio') {
    const holdings = s.holdings || [];
    return <div>
      {holdings.slice(0, 5).map((h: any, i: number) => (
        <CardWrap key={i} borderColor={h.rating?.includes('Buy') ? C.green : h.rating?.includes('Sell') ? C.red : C.gold}>
          <div style={{ padding:'8px 10px', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
              <span style={{ color:C.blue, fontWeight:800, fontSize:12, fontFamily:font }}>{h.ticker}</span>
              <span style={{ color:changeColor(h.change), fontSize:10, fontWeight:600, fontFamily:font }}>{h.price} {h.change}</span>
            </div>
            <Badge color={h.rating?.includes('Buy') ? C.green : h.rating?.includes('Sell') ? C.red : C.gold}>{h.rating}</Badge>
          </div>
        </CardWrap>
      ))}
    </div>;
  }

  if (dt === 'commodities') {
    const commodities = s.commodities || [];
    return <div>
      {s.market_overview && <div style={{ padding:'8px 10px', background:`${C.gold}08`, borderRadius:6, marginBottom:6, color:C.text, fontSize:10, fontFamily:sansFont }}>{s.market_overview}</div>}
      {commodities.slice(0, 4).map((c: any, i: number) => (
        <CardWrap key={i} borderColor={convColor(c.conviction)}>
          <div style={{ padding:'8px 10px', display:'flex', justifyContent:'space-between', alignItems:'center' }}>
            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
              <span style={{ color:C.gold, fontWeight:800, fontSize:12, fontFamily:font }}>{c.name}</span>
              <span style={{ color:C.bright, fontSize:12, fontWeight:700, fontFamily:font }}>{c.price}</span>
              <span style={{ color:changeColor(c.change_today), fontWeight:600, fontSize:10, fontFamily:font }}>{c.change_today}</span>
            </div>
            <Badge color={convColor(c.conviction)}>{c.conviction}</Badge>
          </div>
        </CardWrap>
      ))}
    </div>;
  }

  return <div style={{ color:C.text, fontSize:11, lineHeight:1.6, fontFamily:sansFont }} dangerouslySetInnerHTML={{ __html: formatAnalysis(analysis) }} />;
}

const QUICK_PROMPTS = [
  { l: 'Trending', p: 'What stocks are trending across ALL platforms right now? Cross-reference StockTwits, Yahoo Finance, StockAnalysis, Finviz, and Polygon.' },
  { l: 'Best Trades', p: 'Show me the best trade setups right now with MACD crossovers, RSI in 50-65, price above rising SMAs, volume 2x+ average.' },
  { l: 'Macro', p: 'Full macro dashboard. Fed funds rate, CPI, yield curve, VIX, Fear & Greed, DXY, oil, gold. Risk-on or risk-off?' },
  { l: 'Sectors', p: 'Full sector rotation analysis. Show every major sector ETF performance, RSI, trend direction, and relative strength vs SPY.' },
  { l: 'Crypto', p: 'Full crypto market scan. Global market state, funding rates, hot categories, top momentum coins with social buzz.' },
  { l: 'Briefing', p: 'Give me the full daily intelligence briefing in 60 seconds. Market pulse, key numbers, top signals, and highest conviction moves.' },
];

export default function ChatbotWidget() {
  const [location] = useLocation();
  const [mode, setMode] = useState<'collapsed' | 'small' | 'expanded'>('collapsed');
  const [input, setInput] = useState('');
  const [isScrolling, setIsScrolling] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const scrollTimerRef = useRef<any>(null);
  const { messages, isLoading, loadingStage, sendMessage, clearChat, hasUnread, setHasUnread } = useChatbot();

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isLoading]);

  useEffect(() => {
    if (mode !== 'collapsed') setHasUnread(false);
  }, [mode, setHasUnread]);

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolling(true);
      if (scrollTimerRef.current) clearTimeout(scrollTimerRef.current);
      scrollTimerRef.current = setTimeout(() => setIsScrolling(false), 1500);
    };
    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  if (location === '/app/hippo-ai') return null;

  const handleSend = () => {
    if (!input.trim() || isLoading) return;
    sendMessage(input.trim());
    setInput('');
  };

  const isMobile = typeof window !== 'undefined' && window.innerWidth < 768;

  if (mode === 'collapsed') {
    return (
      <>
        <style>{`
          @keyframes chatbot-entrance {
            0% { transform: scale(1); opacity: 0.4; }
            20% { transform: scale(1.25); opacity: 1; }
            35% { transform: scale(0.95); opacity: 1; }
            50% { transform: scale(1.15); opacity: 1; }
            65% { transform: scale(1); opacity: 1; }
            100% { transform: scale(1); opacity: 1; }
          }
          .chatbot-btn-entrance {
            animation: chatbot-entrance 1.5s ease-out 0.3s 1 both;
          }
          .chatbot-btn-entrance:hover {
            transform: scale(1.6) !important;
          }
        `}</style>
        <button className="chatbot-btn-entrance" onClick={() => { setMode('small'); setHasUnread(false); }} style={{
          position: 'fixed', bottom: isMobile ? 16 : 24, right: isMobile ? 16 : 24, zIndex: 9999,
          width: isMobile ? 80 : 100, height: isMobile ? 80 : 100,
          background: 'none', border: 'none', padding: 0,
          cursor: 'pointer',
          filter: 'drop-shadow(0 4px 12px rgba(0,0,0,0.5)) drop-shadow(0 0 20px rgba(139,92,246,0.25))',
        }}>
          <img src={cryptoHippoLogo} alt="Chat" style={{ width: '100%', height: '100%', objectFit: 'contain' }} />
          {hasUnread && <span style={{ position:'absolute', top:2, right:2, width:12, height:12, borderRadius:'50%', background:C.green, border:`2px solid ${C.bg}` }} />}
        </button>
      </>
    );
  }

  const isExpanded = mode === 'expanded';
  const panelW = isMobile ? '100vw' : (isExpanded ? 700 : 400);
  const panelH = isMobile ? (isExpanded ? '100vh' : '60vh') : (isExpanded ? '80vh' : 500);
  const showChips = messages.length === 0;

  return (
    <div style={{
      position: 'fixed',
      bottom: isMobile ? 0 : 24,
      right: isMobile ? 0 : 24,
      zIndex: 9999,
      width: panelW,
      height: panelH,
      display: 'flex', flexDirection: 'column',
      background: 'rgba(11,12,16,0.92)',
      backdropFilter: 'blur(12px)',
      WebkitBackdropFilter: 'blur(12px)',
      border: `1px solid ${C.border}`,
      borderRadius: isMobile ? (isExpanded ? 0 : '16px 16px 0 0') : 12,
      boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
      transition: 'all 0.2s ease-out',
      overflow: 'hidden',
    }}>
      <div style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 14px',
        background: C.card,
        borderBottom: `1px solid ${C.border}`,
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <span style={{ color: C.bright, fontSize: 13, fontWeight: 700, fontFamily: sansFont }}>Ask Caelyn</span>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {isExpanded && <button onClick={clearChat} style={{ padding:'4px 8px', background:'transparent', border:`1px solid ${C.border}`, borderRadius:4, color:C.dim, fontSize:10, cursor:'pointer', fontFamily:font }} onMouseEnter={e => e.currentTarget.style.color = C.bright} onMouseLeave={e => e.currentTarget.style.color = C.dim}>Clear</button>}
          <button onClick={() => setMode(isExpanded ? 'small' : 'expanded')} style={{ padding:'4px 8px', background:'transparent', border:`1px solid ${C.border}`, borderRadius:4, color:C.dim, fontSize:12, cursor:'pointer', fontFamily:font }} onMouseEnter={e => e.currentTarget.style.color = C.bright} onMouseLeave={e => e.currentTarget.style.color = C.dim}>{isExpanded ? '↙' : '↗'}</button>
          <button onClick={() => setMode('collapsed')} style={{ padding:'4px 8px', background:'transparent', border:`1px solid ${C.border}`, borderRadius:4, color:C.dim, fontSize:12, cursor:'pointer', fontFamily:font }} onMouseEnter={e => e.currentTarget.style.color = C.red} onMouseLeave={e => e.currentTarget.style.color = C.dim}>✕</button>
        </div>
      </div>

      <div style={{ flex: 1, overflowY: 'auto', padding: 12, display: 'flex', flexDirection: 'column', gap: 10 }}>
        {messages.length === 0 && !isLoading && (
          <div style={{ textAlign: 'center', padding: '40px 20px', color: C.dim }}>
            <img src={caelynLogo} alt="" style={{ width: 240, height: 240, margin: '0 auto 16px', opacity: 1, filter: 'brightness(1.3) drop-shadow(0 0 18px rgba(120, 200, 255, 0.45))' }} />
            <div style={{ fontSize: 13, fontFamily: sansFont, marginBottom: 4 }}>Ask me anything about markets</div>
            <div style={{ fontSize: 10, fontFamily: font }}>Stocks, crypto, macro, sectors...</div>
          </div>
        )}
        {messages.map((msg, i) => (
          <div key={i} style={{ display: 'flex', justifyContent: msg.role === 'user' ? 'flex-end' : 'flex-start' }}>
            <div style={{
              maxWidth: '90%',
              padding: msg.role === 'user' ? '8px 12px' : '10px 12px',
              borderRadius: 10,
              background: msg.role === 'user' ? `${C.blue}20` : C.card,
              border: `1px solid ${msg.role === 'user' ? C.blue + '30' : C.border}`,
              color: msg.role === 'user' ? C.bright : C.text,
              fontSize: 11,
              lineHeight: 1.5,
              fontFamily: sansFont,
            }}>
              {msg.role === 'user' ? msg.content : (
                msg.structured ? <MiniRenderer structured={msg.structured} analysis={msg.content} /> : <div dangerouslySetInnerHTML={{ __html: formatAnalysis(msg.content) }} />
              )}
            </div>
          </div>
        ))}
        {isLoading && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '10px 12px', background: C.card, borderRadius: 10, border: `1px solid ${C.border}` }}>
              <div style={{ display: 'flex', gap: 4 }}>
                {[0,1,2].map(i => <span key={i} style={{ width:6, height:6, borderRadius:'50%', background:C.purple, animation:`chatbot-dot 1.2s ease-in-out ${i*0.2}s infinite` }} />)}
              </div>
              <span style={{ color: C.dim, fontSize: 10, fontFamily: font }}>{loadingStage}</span>
            </div>
            <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <img src={caelynLogo} alt="" style={{ width: 200, height: 200, opacity: 0.35, filter: 'brightness(1.3) drop-shadow(0 0 14px rgba(120, 200, 255, 0.3))', animation: 'chatbot-dot 2s ease-in-out infinite' }} />
            </div>
          </>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div style={{ flexShrink: 0, borderTop: `1px solid ${C.border}`, padding: 10, background: C.card }}>
        {showChips && (
          <div className="chatbot-chips-bar" style={{ display: 'flex', gap: 4, marginBottom: 8, overflowX: 'auto', paddingBottom: 0, WebkitOverflowScrolling: 'touch', scrollbarWidth: 'none', msOverflowStyle: 'none' } as React.CSSProperties}>
            {QUICK_PROMPTS.map(qp => (
              <button key={qp.l} onClick={() => { sendMessage(qp.p); }} disabled={isLoading} style={{
                padding: isMobile ? '6px 12px' : '4px 10px', background: `${C.purple}08`, border: `1px solid ${C.purple}18`,
                borderRadius: 12, color: C.dim, fontSize: isMobile ? 11 : 9, fontWeight: 600, fontFamily: font,
                cursor: isLoading ? 'not-allowed' : 'pointer', whiteSpace: 'nowrap', transition: 'all 0.15s', flexShrink: 0,
              }} onMouseEnter={e => { e.currentTarget.style.borderColor = C.purple; e.currentTarget.style.color = C.bright; }} onMouseLeave={e => { e.currentTarget.style.borderColor = `${C.purple}18`; e.currentTarget.style.color = C.dim; }}>{qp.l}</button>
            ))}
          </div>
        )}
        <div style={{ display: 'flex', gap: 6 }}>
          <input
            type="text" value={input} onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSend()}
            placeholder="Ask about any stock, crypto, macro..."
            style={{
              flex: 1, padding: '8px 12px', border: `1px solid ${C.border}`, borderRadius: 8,
              background: C.bg, color: C.bright, fontSize: isMobile ? 16 : 12, fontFamily: sansFont, outline: 'none',
            }}
          />
          <button onClick={handleSend} disabled={isLoading || !input.trim()} style={{
            padding: '8px 16px',
            background: isLoading || !input.trim() ? C.card : `linear-gradient(135deg, ${C.blue}, #2563eb)`,
            color: isLoading || !input.trim() ? C.dim : 'white',
            border: 'none', borderRadius: 8, cursor: isLoading || !input.trim() ? 'not-allowed' : 'pointer',
            fontWeight: 700, fontSize: 12, fontFamily: sansFont,
          }}>Send</button>
        </div>
      </div>

      <style>{`
        @keyframes chatbot-dot { 0%, 80%, 100% { transform: scale(0.6); opacity:0.4; } 40% { transform: scale(1); opacity:1; } }
      `}</style>
    </div>
  );
}
