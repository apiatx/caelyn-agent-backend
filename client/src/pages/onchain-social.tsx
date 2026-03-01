import { useState, useCallback, useEffect, useRef } from 'react';
import { openSecureLink } from '@/utils/security';
import socialImage from "@assets/image_1771574082445.png";

// ─── Grok Agent Constants ─────────────────────────────────────────
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";

const SUGGESTED_PROMPTS = [
  "Top stock tickers trending on X right now",
  "Which tickers have the most bullish sentiment today?",
  "What are top finance accounts saying about the market?",
  "Most discussed crypto tokens on X this week",
  "What bearish warnings are traders posting on X?",
  "Highest engagement trading posts in the last 24 hours",
  "What is X saying about Fed rate decisions?",
  "Retail sentiment vs institutional sentiment on X",
];

// ─── Sentiment color helper ───────────────────────────────────────
function renderGrokResponse(text: string) {
  // Split into lines, apply color coding for sentiment words
  return text.split('\n').map((line, i) => {
    // Apply inline coloring for sentiment keywords
    const parts: Array<{ text: string; color?: string }> = [];
    let remaining = line;

    const patterns: Array<{ regex: RegExp; color: string }> = [
      { regex: /\b(bullish|buy|strong buy|long|upgrade|breakout|moon|pump|rally|green|accumulate)\b/gi, color: '#22c55e' },
      { regex: /\b(bearish|sell|short|downgrade|breakdown|dump|crash|red|distribute|warning|risk|avoid)\b/gi, color: '#ef4444' },
      { regex: /\b(neutral|hold|mixed|sideways|consolidat\w*|uncertain|wait)\b/gi, color: '#6b7280' },
      { regex: /(@\w+)/g, color: '#38bdf8' },
      { regex: /(\$[A-Z]{1,6})/g, color: '#a78bfa' },
      { regex: /(Sentiment Score:?\s*\d+\/10|Confidence:?\s*\d+\/10|\d+\/10)/gi, color: '#f59e0b' },
    ];

    // Simple approach: just highlight keywords inline
    let html = remaining;
    for (const { regex, color } of patterns) {
      html = html.replace(regex, (match) => `<span style="color:${color};font-weight:600">${match}</span>`);
    }

    // Bold markdown-style headers (lines starting with ## or **)
    if (/^#{1,4}\s/.test(html)) {
      html = html.replace(/^#{1,4}\s*(.*)/, '<span style="color:#e2e8f0;font-weight:700;font-size:0.85rem">$1</span>');
    }
    html = html.replace(/\*\*(.+?)\*\*/g, '<span style="color:#e2e8f0;font-weight:700">$1</span>');

    return (
      <div key={i} dangerouslySetInnerHTML={{ __html: html || '&nbsp;' }} />
    );
  });
}

interface GrokMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
}

function GrokSocialAgent() {
  const [messages, setMessages] = useState<GrokMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg: GrokMessage = { role: 'user', content: text.trim(), timestamp: Date.now() };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const res = await fetch(`${AGENT_BACKEND_URL}/api/social/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': AGENT_API_KEY,
        },
        body: JSON.stringify({ query: text.trim() }),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`${res.status}: ${errText.slice(0, 200)}`);
      }

      const data = await res.json();
      const responseText = data.response || data.error || 'No response received';

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: responseText,
        timestamp: Date.now(),
      }]);
    } catch (err) {
      setMessages(prev => [...prev, {
        role: 'assistant',
        content: `Error: ${err instanceof Error ? err.message : 'Failed to reach Grok. Please try again.'}`,
        timestamp: Date.now(),
      }]);
    } finally {
      setLoading(false);
    }
  }, [loading]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  return (
    <section style={{ maxWidth: 880, margin: '0 auto', padding: '0 3rem 2rem', position: 'relative', zIndex: 1 }}>
      <div style={{
        background: '#0a0b1e',
        border: '1px solid #1e2148',
        borderRadius: 12,
        padding: '1.5rem',
        marginBottom: '1.5rem',
      }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
          <div style={{
            width: 32, height: 32,
            background: 'linear-gradient(135deg, #6366f1 0%, #3b82f6 50%, #06b6d4 100%)',
            borderRadius: 8,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: '0.9rem', fontWeight: 700, color: '#fff',
          }}>𝕏</div>
          <div>
            <h3 style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.82rem', fontWeight: 600, color: '#e2e8f0',
              letterSpacing: '-0.01em', margin: 0,
            }}>Grok Social Agent</h3>
            <p style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.62rem', color: '#475569',
              margin: 0, letterSpacing: '0.02em',
            }}>Real-time X/Twitter sentiment via xAI Grok</p>
          </div>
        </div>

        {/* Input bar */}
        <form onSubmit={handleSubmit} style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }}>
          <input
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask Grok about X/Twitter sentiment..."
            disabled={loading}
            style={{
              flex: 1,
              background: '#111228',
              border: '1px solid #1e2148',
              borderRadius: 8,
              padding: '0.65rem 0.9rem',
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.78rem',
              color: '#e2e8f0',
              outline: 'none',
              transition: 'border-color 0.2s',
              opacity: loading ? 0.5 : 1,
            }}
            onFocus={e => { e.currentTarget.style.borderColor = '#3b82f6'; }}
            onBlur={e => { e.currentTarget.style.borderColor = '#1e2148'; }}
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            style={{
              background: 'linear-gradient(135deg, #6366f1 0%, #3b82f6 100%)',
              border: 'none',
              borderRadius: 8,
              padding: '0.65rem 1rem',
              color: '#fff',
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.72rem',
              fontWeight: 600,
              cursor: loading || !input.trim() ? 'not-allowed' : 'pointer',
              opacity: loading || !input.trim() ? 0.35 : 1,
              transition: 'opacity 0.2s',
              letterSpacing: '0.04em',
              whiteSpace: 'nowrap',
            }}
          >{loading ? '...' : 'SEND'}</button>
        </form>

        {/* Pre-prompt chips */}
        <div style={{
          display: 'flex', flexWrap: 'wrap', gap: '0.4rem',
          marginBottom: messages.length > 0 || loading ? '1rem' : 0,
        }}>
          {SUGGESTED_PROMPTS.map(prompt => (
            <button
              key={prompt}
              onClick={() => sendMessage(prompt)}
              disabled={loading}
              style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '0.65rem',
                color: '#64748b',
                background: 'rgba(99,102,241,0.06)',
                border: '1px solid rgba(99,102,241,0.2)',
                borderRadius: 100,
                padding: '0.35rem 0.75rem',
                cursor: loading ? 'not-allowed' : 'pointer',
                transition: 'all 0.2s',
                opacity: loading ? 0.4 : 1,
                whiteSpace: 'nowrap',
              }}
              onMouseOver={e => {
                if (!loading) {
                  (e.currentTarget as HTMLButtonElement).style.background = 'rgba(99,102,241,0.15)';
                  (e.currentTarget as HTMLButtonElement).style.color = '#94a3b8';
                  (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(99,102,241,0.4)';
                }
              }}
              onMouseOut={e => {
                (e.currentTarget as HTMLButtonElement).style.background = 'rgba(99,102,241,0.06)';
                (e.currentTarget as HTMLButtonElement).style.color = '#64748b';
                (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(99,102,241,0.2)';
              }}
            >{prompt}</button>
          ))}
        </div>

        {/* Messages / Response area */}
        {(messages.length > 0 || loading) && (
          <div style={{
            maxHeight: 520,
            overflowY: 'auto',
            display: 'flex',
            flexDirection: 'column',
            gap: '0.6rem',
          }}>
            {messages.map((msg, i) => (
              <div key={i} style={{
                background: msg.role === 'user' ? 'rgba(99,102,241,0.08)' : '#0d0e22',
                border: `1px solid ${msg.role === 'user' ? 'rgba(99,102,241,0.2)' : '#1e2148'}`,
                borderRadius: 8,
                padding: '0.75rem 1rem',
              }}>
                <div style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.6rem',
                  fontWeight: 700,
                  textTransform: 'uppercase' as const,
                  letterSpacing: '0.1em',
                  color: msg.role === 'user' ? '#818cf8' : '#38bdf8',
                  marginBottom: '0.4rem',
                }}>{msg.role === 'user' ? 'YOU' : 'GROK'}</div>
                <div style={{
                  fontFamily: msg.role === 'assistant' ? "'JetBrains Mono', monospace" : "'Outfit', sans-serif",
                  fontSize: msg.role === 'assistant' ? '0.74rem' : '0.8rem',
                  lineHeight: 1.65,
                  color: msg.role === 'user' ? '#c7d2fe' : '#94a3b8',
                }}>
                  {msg.role === 'assistant' ? renderGrokResponse(msg.content) : msg.content}
                </div>
              </div>
            ))}
            {loading && (
              <div style={{
                background: '#0d0e22',
                border: '1px solid #1e2148',
                borderRadius: 8,
                padding: '0.75rem 1rem',
              }}>
                <div style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.74rem',
                  color: '#475569',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem',
                }}>
                  <span style={{
                    display: 'inline-block',
                    width: 12, height: 12,
                    border: '2px solid #3b82f6',
                    borderTopColor: 'transparent',
                    borderRadius: '50%',
                    animation: 'spin 0.8s linear infinite',
                  }} />
                  Scanning X...
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}

        {/* Clear button */}
        {messages.length > 0 && (
          <button
            onClick={() => setMessages([])}
            style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.6rem',
              color: '#334155',
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              marginTop: '0.5rem',
              padding: 0,
              transition: 'color 0.2s',
            }}
            onMouseOver={e => { (e.currentTarget as HTMLButtonElement).style.color = '#64748b'; }}
            onMouseOut={e => { (e.currentTarget as HTMLButtonElement).style.color = '#334155'; }}
          >Clear conversation</button>
        )}
      </div>

      {/* CSS animation for spinner */}
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </section>
  );
}

interface SafeLinkProps {
  href: string;
  children: React.ReactNode;
  className?: string;
  style?: React.CSSProperties;
}

const SafeLink: React.FC<SafeLinkProps> = ({ href, children, className = "", style }) => {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <button onClick={() => openInNewTab(href)} className={className} style={style}>
      {children}
    </button>
  );
};

export default function OnchainSocialPage() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white relative" style={{ background: '#050510', fontFamily: "'Outfit', sans-serif", lineHeight: 1.65 }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        .social-page .ice { color: #38bdf8; }
        .social-page .gradient-text {
          background: linear-gradient(135deg, #6366f1 0%, #3b82f6 40%, #06b6d4 100%);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
        }
        .social-page .profile-cell:hover { background: #161838 !important; }
        .social-page .section-card:hover { background: #161838 !important; }
      `}</style>

      <div className="social-page relative" style={{ zIndex: 1 }}>
        <div style={{
          position: 'fixed', top: '-40%', left: '-20%', width: '140%', height: '140%',
          background: 'radial-gradient(ellipse 800px 600px at 20% 15%, rgba(99,102,241,0.06) 0%, transparent 70%), radial-gradient(ellipse 600px 500px at 80% 70%, rgba(6,182,212,0.04) 0%, transparent 70%), radial-gradient(ellipse 900px 400px at 50% 50%, rgba(59,130,246,0.03) 0%, transparent 60%)',
          pointerEvents: 'none', zIndex: 0
        }} />

        {/* HERO */}
        <div style={{ padding: '1.5rem 3rem 1rem', maxWidth: 900, margin: '0 auto', textAlign: 'center', position: 'relative', zIndex: 1 }}>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.4rem', marginBottom: '0.5rem' }}>
            <img src={socialImage} alt="Caelyn.ai" style={{ width: 180, height: 'auto', objectFit: 'contain' }} />
            <h1 style={{ fontSize: 'clamp(1.4rem, 3vw, 2rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.03em', margin: 0 }}>
              <span className="gradient-text">Social</span>
            </h1>
            <p style={{ fontSize: '0.9rem', color: '#64748b', margin: 0, lineHeight: 1.5 }}>
              Social intelligence and community analytics
            </p>
          </div>
        </div>

        {/* DIVIDER */}
        <div style={{ width: 60, height: 2, background: 'linear-gradient(135deg, #6366f1, #3b82f6, #06b6d4)', margin: '0 auto 2rem', borderRadius: 2 }} />

        {/* ═══ Grok Social Agent ═══ */}
        <GrokSocialAgent />

        {/* StocksX Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>StocksX</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>Stocks <span className="ice">X Accounts</span></h2>

          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '0.5rem' }}>
              {[
                { name: 'RebellioMarket', handle: '@RebellioMarket' },
                { name: 'StocksToTrade', handle: '@StocksToTrade' },
                { name: 'Timothy Sykes', handle: '@timothysykes' },
                { name: 'Parangiras', handle: '@Parangiras' },
                { name: 'Real Sheep Wolf', handle: '@realsheepwolf' },
                { name: 'Eric Jackson', handle: '@ericjackson' },
                { name: 'The Long Invest', handle: '@TheLongInvest' },
                { name: 'Davy', handle: '@davyy888' },
                { name: 'PMDiChristina', handle: '@PMDiChristina' },
                { name: 'Joel Goes Digital', handle: '@JoelGoesDigital' },
                { name: 'Scot1andT', handle: '@Scot1andT' },
                { name: 'MACD Master', handle: '@MACDMaster328' },
                { name: 'Spartan Trading', handle: '@SpartanTrading' },
                { name: 'Planert41', handle: '@planert41' },
                { name: 'Maximus Holla', handle: '@Maximus_Holla' },
                { name: 'Canton Meow', handle: '@cantonmeow' },
                { name: 'Donald J Dean', handle: '@donaldjdean' },
                { name: 'AC Investor Blog', handle: '@ACInvestorBlog' },
                { name: 'Cestrian Inc', handle: '@CestrianInc' },
                { name: 'Invest In Assets', handle: '@InvestInAssets' },
                { name: 'Invest Insights', handle: '@investinsights4' },
                { name: 'Bits and Bips', handle: '@bitsandbips' },
                { name: 'BKnight221', handle: '@BKnight221' },
                { name: 'NFT Lunatic', handle: '@NFTLunatic' },
                { name: 'AllISeeIs_W', handle: '@alliseeis_W' },
                { name: 'HyesGregory', handle: '@HyesGregory' },
                { name: 'StockOptionCole', handle: '@StockOptionCole' },
                { name: 'newzage', handle: '@newzage' },
                { name: 'The__Solstice', handle: '@The__Solstice' },
                { name: 'thenewmoney_tnm', handle: '@thenewmoney_tnm' },
                { name: 'aleabitoreddit', handle: '@aleabitoreddit' }
              ].map((account) => (
                <SafeLink
                  key={account.handle}
                  href={`https://x.com/${account.handle.replace('@', '')}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account.name}</span>
                </SafeLink>
              ))}
            </div>
          </div>
        </section>

        {/* CryptoX Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>CryptoX</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>Crypto <span className="ice">X Accounts</span></h2>

          {/* Research and Fundamentals */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Research and Fundamentals</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'TechDev_52', 'ofvoice25355', 'CoinGurruu', 'stacy_muur', 
                'martypartymusic', 'Defi0xJeff', 'altcoinvector', 'DeFi_Paanda', 
                'cryptorinweb3', 'jkrdoc', 'Agent_rsch', 'OverkillTrading', 
                'dontbuytops', 'MetaverseRanger', 'aixCB_Vc', 'aixbt_agent',
                'nansen_ai', 'rogue_says', 'Globalflows', 'crypto_linn'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Traders */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Traders</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'TheEuroSniper', 'EricCryptoman', 'Whale_AI_net', 'CryptoThannos', 
                'HolderScan', 'Ethimedes', 'MisterSpread', 'CBATrades', 'DigimonCBA',
                'MWhalekiller', 'smileycapital', 'thedefivillain', 'doomsdart', 
                'bitcodyy', 'CryptoDarkSide4', 'DefiSabali', '0xTindorr', 
                'Chroma_Trading', 'follis_', 'AltcoinSniperTA', 'Bitcoinhabebe', 
                'sonder_crypto', 'istudycharts', 'Crypto_Tigers1', 'CryptoLimbo_',
                'cryptoknight890', 'CryptoEmree_', 'spetsnaz_3', 'newzage', 'The__Solstice',
                'jaydee_757', 'EasyInvests', 'sarper_onder', 'XForceGlobal', 'alecTrading', 'redhairshanks86', 'eliz883'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
              <SafeLink
                href="https://x.com/sonder_crypto/status/1968059158491767121"
                style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                className="profile-cell"
              >
                <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>sonder_crypto</span>
              </SafeLink>
              <SafeLink
                href="https://x.com/alecTrading/status/1971938635097559333"
                style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                className="profile-cell"
              >
                <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>alecTrading</span>
              </SafeLink>
            </div>
          </div>

          {/* Thoughts & Opinions */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Thoughts & Opinions</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'CryptoZer0_'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Macro */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Macro</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                '_The_Prophet__'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Market Today */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Market Today</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'aicryptopattern'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#38bdf8', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Chains */}
          <div style={{ background: '#111228', border: '1px solid #1e2148', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '1.25rem' }}>Chains</h3>

            {/* Base and Solana Ecosystems - Side by Side */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '1rem', marginBottom: '1rem' }}>
              {/* Base Ecosystem */}
              <div style={{ background: '#0c0d1e', border: '1px solid #1e2148', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem', textAlign: 'center' }}>Base Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'BaseDailyTK', name: 'Base Daily TK', desc: '@BaseDailyTK - Daily BASE network updates and insights' },
                    { handle: 'MemesOnBase', name: 'Memes On Base', desc: '@MemesOnBase - BASE network meme culture and community' },
                    { handle: 'MemesOnBase_', name: 'Memes On Base', desc: '@MemesOnBase_ - BASE network meme culture and trends' },
                    { handle: 'Shake51_', name: 'Shake51', desc: '@Shake51_ - BASE network trading insights' },
                    { handle: '1CrypticPoet', name: 'CrypticPoet', desc: '@1CrypticPoet - BASE network alpha and trading signals' },
                    { handle: 'jamatto14', name: 'Jamatto14', desc: '@jamatto14 - BASE network insights and updates' },
                    { handle: 'MrGreen_18', name: 'MrGreen_18', desc: '@MrGreen_18 - BASE network trading signals and alpha' },
                    { handle: 'chironchain', name: 'chironchain', desc: '@chironchain - BASE network insights' },
                    { handle: 'goodvimonly', name: 'goodvimonly', desc: '@goodvimonly - BASE network analysis' },
                    { handle: '0x_tesseract', name: '0x_tesseract', desc: '@0x_tesseract - BASE network trading' },
                    { handle: 'Prometheus_The1', name: 'Prometheus_The1', desc: '@Prometheus_The1 - BASE network insights' },
                    { handle: 'lil_louieT', name: 'lil_louieT', desc: '@lil_louieT - BASE network trading' },
                  ].map((account) => (
                    <SafeLink
                      key={account.handle}
                      href={`https://x.com/${account.handle}`}
                      style={{ padding: '0.6rem 0.75rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: '#38bdf8', fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>

              {/* Solana Ecosystem */}
              <div style={{ background: '#0c0d1e', border: '1px solid #1e2148', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem', textAlign: 'center' }}>Solana Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'Dior100x', name: 'Dior100x', desc: '@Dior100x - Solana trading insights', color: '#38bdf8' },
                    { handle: '_Shadow36', name: '_Shadow36', desc: '@_Shadow36 - Solana market analysis', color: '#38bdf8' },
                    { handle: 'WolverCrypto', name: 'WolverCrypto', desc: '@WolverCrypto - Crypto trading insights', color: '#38bdf8' },
                    { handle: 'watchingmarkets', name: 'watchingmarkets', desc: '@watchingmarkets - Market watching insights', color: '#38bdf8' },
                    { handle: 'Crypto_Alch', name: 'Crypto_Alch', desc: '@Crypto_Alch - Crypto alchemy insights', color: '#38bdf8' },
                    { handle: 'bruhbearr', name: 'bruhbearr', desc: '@bruhbearr - Solana trading insights', color: '#38bdf8' },
                    { handle: 'AltcoinMarksman', name: 'AltcoinMarksman', desc: '@AltcoinMarksman - Solana market analysis', color: '#38bdf8' },
                  ].map((account) => (
                    <SafeLink
                      key={account.handle}
                      href={`https://x.com/${account.handle}`}
                      style={{ padding: '0.6rem 0.75rem', background: `${account.color}14`, border: `1px solid ${account.color}33`, borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: account.color, fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: account.color, fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>
            </div>

            {/* Bittensor and BNB Ecosystems - Side by Side */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '1rem' }}>
              {/* Bittensor Ecosystem */}
              <div style={{ background: '#0c0d1e', border: '1px solid #1e2148', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem', textAlign: 'center' }}>Bittensor Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'tao_agent', name: 'TAO Agent', desc: '@tao_agent - Bittensor Signal Intelligence', color: '#38bdf8' },
                    { handle: 'Bitcast_network', name: 'Bitcast Network', desc: '@Bitcast_network - TAO Network Analytics', color: '#38bdf8' },
                    { handle: 'TaoStacker', name: 'TaoStacker', desc: '@TaoStacker - TAO Staking Insights', color: '#38bdf8' },
                    { handle: 'TaoIsTheKey', name: 'TaoIsTheKey', desc: '@TaoIsTheKey - TAO Market Analysis', color: '#38bdf8' },
                    { handle: 'varimotrades', name: 'VARiMOtrading', desc: '@varimotrades - TAO Trading Signals', color: '#38bdf8' },
                    { handle: '_g_x_g', name: 'GXG', desc: '@_g_x_g - Bittensor Intelligence', color: '#38bdf8' },
                    { handle: 'TalkingTensor', name: 'Talking Tensor', desc: '@TalkingTensor - Bittensor Insights', color: '#38bdf8' },
                    { handle: 'Shogun__base', name: 'Shogun Base', desc: '@Shogun__base - Base Network Trading', color: '#38bdf8' },
                    { handle: 'Victor_crypto_2', name: 'Victor Crypto', desc: '@Victor_crypto_2 - Crypto Market Analysis', color: '#38bdf8' },
                    { handle: 'btcrenaissance', name: 'BTC Renaissance', desc: '@btcrenaissance - Bittensor Insights', color: '#38bdf8' },
                  ].map((account) => (
                    <SafeLink
                      key={account.handle}
                      href={`https://x.com/${account.handle}`}
                      style={{ padding: '0.6rem 0.75rem', background: `${account.color}14`, border: `1px solid ${account.color}33`, borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: account.color, fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: account.color, fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>

              {/* BNB Ecosystem */}
              <div style={{ background: '#0c0d1e', border: '1px solid #1e2148', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem', textAlign: 'center' }}>BNB Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'cryptoknight890', name: 'CryptoKnight890', desc: '@cryptoknight890 - BNB ecosystem insights' },
                    { handle: 'BastilleBtc', name: 'BastilleBtc', desc: '@BastilleBtc - BNB trading and insights' },
                    { handle: 'JuliusElum', name: 'JuliusElum', desc: '@JuliusElum - BNB ecosystem analysis' },
                  ].map((account) => (
                    <SafeLink
                      key={account.handle}
                      href={`https://x.com/${account.handle}`}
                      style={{ padding: '0.6rem 0.75rem', background: 'rgba(56,189,248,0.08)', border: '1px solid rgba(56,189,248,0.2)', borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: '#38bdf8', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: '#38bdf8', fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>
            </div>
          </div>

        </section>

        {/* Platforms Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Platforms</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>Social <span className="ice">Media</span></h2>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1px', background: '#1e2148', border: '1px solid #1e2148', borderRadius: 12, overflow: 'hidden' }}>
            <SafeLink
              href='https://substack.com/'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem' }}>📰</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8' }}>Substack</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Newsletter Publishing Platform</div>
            </SafeLink>

            <SafeLink
              href='https://x.com/home'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem', fontWeight: 700 }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8' }}>X</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Media & News Feed</div>
            </SafeLink>

            <SafeLink
              href='https://farcaster.xyz/'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem' }}>🌐</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8' }}>Farcaster</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Decentralized Social Network</div>
            </SafeLink>
          </div>
        </section>

        {/* Analytics Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.75rem' }}>Analytics</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>AI-Powered <span className="ice">Intelligence</span></h2>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1px', background: '#1e2148', border: '1px solid #1e2148', borderRadius: 12, overflow: 'hidden' }}>
            <SafeLink
              href='https://yaps.kaito.ai/'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.5rem' }}>Kaito</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>AI-Powered Social Intelligence</div>
            </SafeLink>

            <SafeLink
              href='https://app.kolytics.pro/leaderboard'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.5rem' }}>Kolytics</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Signal Analytics</div>
            </SafeLink>

            <SafeLink
              href='https://www.alphabot.app/pulse'
              style={{ background: '#111228', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#38bdf8', marginBottom: '0.5rem' }}>Alphabot</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Sentiment Bot</div>
            </SafeLink>
          </div>
        </section>

        {/* FOOTER */}
        <footer style={{ borderTop: '1px solid #1e2148', padding: '3rem', textAlign: 'center', color: '#38bdf8', fontSize: '0.85rem', maxWidth: 880, margin: '2rem auto 0' }}>
          <p style={{ fontSize: '0.75rem', color: '#475569' }}>
            Social intelligence and community analytics
          </p>
        </footer>
      </div>
    </div>
  );
}
