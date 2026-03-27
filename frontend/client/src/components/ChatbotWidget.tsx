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

const CRYPTO_TICKERS = new Set(['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'AVAX', 'DOT', 'MATIC', 'LINK', 'DOGE', 'SHIB', 'UNI', 'AAVE', 'LTC', 'NEAR', 'FTM', 'ATOM', 'APT', 'SUI', 'ARB', 'OP', 'INJ', 'TIA', 'SEI', 'JUP', 'WIF', 'PEPE', 'BONK', 'RENDER', 'FET', 'TAO', 'ONDO', 'PENDLE', 'RUNE', 'STX', 'MKR', 'CRV', 'SNX', 'COMP', 'IMX', 'GALA', 'AXS', 'SAND', 'MANA', 'FIL', 'ICP', 'HBAR', 'VET', 'ALGO', 'EGLD', 'MINA', 'KAVA', 'ROSE', 'ZEC', 'EOS', 'XLM', 'TRX', 'TON', 'WLD', 'PYTH', 'JTO', 'STRK', 'BLUR', 'ENA', 'W', 'ETHFI', 'DYM', 'ALT', 'PIXEL', 'PORTAL', 'PAXG']);

function getTVSymbol(ticker: string): string {
  const t = ticker.toUpperCase();
  if (CRYPTO_TICKERS.has(t)) return `BINANCE:${t}USDT`;
  return t;
}

function ChatboxChart({ ticker }: { ticker: string }) {
  const sym = getTVSymbol(ticker);
  const [ivl, setIvl] = useState('D');
  const intervals = [{ l: '1H', v: '60' }, { l: '4H', v: '240' }, { l: '1D', v: 'D' }, { l: '1W', v: 'W' }, { l: '1M', v: 'M' }];
  return <div style={{ margin: '8px 0' }}>
    <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 }}>
      <span style={{ color: C.blue, fontWeight: 700, fontSize: 11, fontFamily: font, marginRight: 4 }}>{ticker}</span>
      {intervals.map(iv => <button key={iv.v} onClick={(e) => { e.stopPropagation(); setIvl(iv.v); }} style={{ padding: '1px 6px', fontSize: 8, fontWeight: 600, fontFamily: font, background: ivl === iv.v ? C.blue + '20' : 'transparent', color: ivl === iv.v ? C.blue : C.dim, border: `1px solid ${ivl === iv.v ? C.blue + '40' : C.border}`, borderRadius: 3, cursor: 'pointer' }}>{iv.l}</button>)}
    </div>
    <div style={{ borderRadius: 6, overflow: 'hidden', border: `1px solid ${C.border}` }}>
      <iframe src={`https://s.tradingview.com/widgetembed/?symbol=${encodeURIComponent(sym)}&interval=${ivl}&theme=dark&style=1&locale=en&hide_top_toolbar=1&hide_side_toolbar=1&allow_symbol_change=0&save_image=0&width=100%25&height=180`} style={{ width: '100%', height: 180, border: 'none', display: 'block' }} title={`${sym} chart`} />
    </div>
  </div>;
}

function formatChatMarkdown(text: string) {
  if (!text) return '';
  return text
    .replace(/^> (.*?)$/gm, `<div style="border-left:3px solid ${C.blue};padding:4px 10px;margin:6px 0;background:${C.blue}08;color:${C.text};font-size:11px;border-radius:0 4px 4px 0">$1</div>`)
    .replace(/^---+$/gm, `<hr style="border:none;border-top:1px solid ${C.border};margin:8px 0"/>`)
    .replace(/^### (.*?)$/gm, `<div style="color:${C.blue};font-weight:700;font-size:12px;margin:8px 0 4px;font-family:${sansFont}">$1</div>`)
    .replace(/^## (.*?)$/gm, `<div style="color:${C.bright};font-weight:700;font-size:13px;margin:10px 0 5px;font-family:${sansFont}">$1</div>`)
    .replace(/^# (.*?)$/gm, `<div style="color:${C.bright};font-weight:800;font-size:14px;margin:12px 0 6px;font-family:${sansFont}">$1</div>`)
    .replace(/\*\*(.*?)\*\*/g, `<span style="color:${C.bright};font-weight:700">$1</span>`)
    .replace(/\*(.*?)\*/g, `<em style="color:${C.text}">$1</em>`)
    .replace(/^- (.*?)$/gm, `<div style="padding-left:12px;margin:2px 0"><span style="color:${C.dim};margin-right:6px">•</span>$1</div>`)
    .replace(/\n\n/g, '<div style="height:8px"></div>')
    .replace(/\n/g, '<br/>');
}

function ChatboxMessage({ content, structured }: { content: string, structured?: any }) {
  const isChatbox = structured?.display_type === 'chatbox';
  const tickers: string[] = isChatbox ? (structured?.tickers || []) : [];

  if (isChatbox || !structured) {
    const displayText = isChatbox ? (structured?.message || content) : content;
    return <div>
      <div style={{ color: C.text, fontSize: 11, lineHeight: 1.6, fontFamily: sansFont }} dangerouslySetInnerHTML={{ __html: formatChatMarkdown(displayText) }} />
      {tickers.length > 0 && (
        <div style={{ marginTop: 8 }}>
          {tickers.slice(0, 3).map(t => <ChatboxChart key={t} ticker={t} />)}
        </div>
      )}
    </div>;
  }

  // Fallback: legacy structured response (shouldn't happen with chatbox_mode, but safe)
  return <div style={{ color: C.text, fontSize: 11, lineHeight: 1.6, fontFamily: sansFont }} dangerouslySetInnerHTML={{ __html: formatChatMarkdown(content) }} />;
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
                <ChatboxMessage content={msg.content} structured={msg.structured} />
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
