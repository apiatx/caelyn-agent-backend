import { useState, useCallback, useEffect, useRef } from 'react';
import { openSecureLink } from '@/utils/security';
import socialImage from "@assets/image_1771574082445.png";

// ─── Grok Agent Constants ─────────────────────────────────────────
const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
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
  if (typeof text !== 'string') text = JSON.stringify(text, null, 2);
  // Split into lines, apply color coding for sentiment words
  return text.split('\n').map((line, i) => {
    // Apply inline coloring for sentiment keywords
    const parts: Array<{ text: string; color?: string }> = [];
    let remaining = line;

    const patterns: Array<{ regex: RegExp; color: string }> = [
      { regex: /\b(bullish|buy|strong buy|long|upgrade|breakout|moon|pump|rally|green|accumulate)\b/gi, color: '#22c55e' },
      { regex: /\b(bearish|sell|short|downgrade|breakdown|dump|crash|red|distribute|warning|risk|avoid)\b/gi, color: '#ef4444' },
      { regex: /\b(neutral|hold|mixed|sideways|consolidat\w*|uncertain|wait)\b/gi, color: '#6b7280' },
      { regex: /(@\w+)/g, color: '#5cc8f0' },
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

// ─── Structured briefing card renderer ────────────────────────────
interface BriefingSection {
  heading: string;
  bullets: string[];
}

interface BriefingResponse {
  display_type: 'briefing';
  title: string;
  summary: string;
  sections: BriefingSection[];
  sentiment_score?: number;
  confidence?: number;
  metadata?: { tokens_analyzed?: number; sources?: string[] };
}

function isBriefingResponse(obj: any): obj is BriefingResponse {
  return obj && typeof obj === 'object' && obj.display_type === 'briefing';
}

function renderBriefingCard(data: BriefingResponse) {
  const sentimentColor = (data.sentiment_score ?? 5) >= 7
    ? '#22c55e'
    : (data.sentiment_score ?? 5) >= 4
      ? '#f59e0b'
      : '#ef4444';
  const confidenceColor = (data.confidence ?? 5) >= 7
    ? '#22c55e'
    : (data.confidence ?? 5) >= 4
      ? '#f59e0b'
      : '#64748b';

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(15,23,42,0.95), rgba(10,15,30,0.98))',
      border: '1px solid rgba(92,200,240,0.15)',
      borderRadius: 10,
      padding: '1rem 1.2rem',
      fontFamily: "'JetBrains Mono', monospace",
    }}>
      {/* Title */}
      <div style={{
        fontSize: '0.85rem',
        fontWeight: 700,
        color: '#e2e8f0',
        marginBottom: '0.5rem',
        borderBottom: '1px solid rgba(255,255,255,0.06)',
        paddingBottom: '0.5rem',
      }}>{data.title}</div>

      {/* Summary */}
      <div style={{
        fontSize: '0.74rem',
        color: '#94a3b8',
        lineHeight: 1.7,
        marginBottom: '0.75rem',
      }}>{data.summary}</div>

      {/* Sections */}
      {data.sections?.map((section, si) => (
        <div key={si} style={{ marginBottom: '0.6rem' }}>
          <div style={{
            fontSize: '0.76rem',
            fontWeight: 700,
            color: '#5cc8f0',
            marginBottom: '0.3rem',
          }}>{section.heading}</div>
          <ul style={{
            margin: 0,
            paddingLeft: '1.2rem',
            listStyleType: 'disc',
          }}>
            {section.bullets?.map((bullet, bi) => (
              <li key={bi} style={{
                fontSize: '0.72rem',
                color: '#94a3b8',
                lineHeight: 1.65,
                marginBottom: '0.15rem',
              }}>{bullet}</li>
            ))}
          </ul>
        </div>
      ))}

      {/* Sentiment & Confidence */}
      {(data.sentiment_score != null || data.confidence != null) && (
        <div style={{
          display: 'flex',
          gap: '1rem',
          marginTop: '0.6rem',
          paddingTop: '0.5rem',
          borderTop: '1px solid rgba(255,255,255,0.06)',
          fontSize: '0.7rem',
        }}>
          {data.sentiment_score != null && (
            <span style={{ color: sentimentColor, fontWeight: 600 }}>
              Sentiment: {data.sentiment_score}/10
            </span>
          )}
          {data.confidence != null && (
            <span style={{ color: confidenceColor, fontWeight: 600 }}>
              Confidence: {data.confidence}/10
            </span>
          )}
        </div>
      )}

      {/* Metadata */}
      {data.metadata?.sources && data.metadata.sources.length > 0 && (
        <div style={{
          marginTop: '0.4rem',
          fontSize: '0.65rem',
          color: '#475569',
        }}>
          Sources: {data.metadata.sources.join(', ')}
        </div>
      )}
    </div>
  );
}

interface GrokMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
  structured?: any;
}

// ─── Structured social response renderer ─────────────────────────
const font = "'JetBrains Mono', monospace";
const sansFont = "'Outfit', sans-serif";

function ConvictionBadge({ value }: { value: string }) {
  const color = /high/i.test(value) ? '#22c55e' : /medium/i.test(value) ? '#f59e0b' : '#64748b';
  return (
    <span style={{
      padding: '1px 7px', borderRadius: 100, fontSize: '0.6rem', fontWeight: 700,
      fontFamily: font, color, border: `1px solid ${color}40`,
      background: `${color}12`, textTransform: 'uppercase' as const, letterSpacing: '0.06em',
    }}>{value}</span>
  );
}

function renderConsensusResponse(structured: any) {
  const C = {
    blue: '#38bdf8', gold: '#f59e0b', green: '#22c55e', red: '#ef4444',
    purple: '#a78bfa', dim: '#475569', text: '#94a3b8', bright: '#e2e8f0',
    card: 'rgba(10,12,28,0.85)', border: 'rgba(255,255,255,0.07)',
  };

  const tickers: any[] = structured.consensus_tickers || [];
  const momentumLeaders: any[] = structured.momentum_leaders || [];
  const earlyVsCrowded = structured.early_vs_crowded || {};
  const earlyStage: any[] = earlyVsCrowded.early_stage || [];
  const crowded: any[] = earlyVsCrowded.crowded || [];
  const finalOpinion = structured.final_opinion || {};

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>

      {/* Header */}
      <div style={{ borderBottom: `1px solid ${C.border}`, paddingBottom: '0.6rem', marginBottom: '0.2rem' }}>
        <div style={{ color: C.bright, fontWeight: 700, fontSize: '0.85rem', fontFamily: font, marginBottom: 4 }}>
          {structured.title || 'Consensus Tickers Among Top X Traders'}
        </div>
        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          {structured.analysis_window && (
            <span style={{ color: C.dim, fontSize: '0.62rem', fontFamily: font }}>
              Window: <span style={{ color: C.blue }}>{structured.analysis_window}</span>
            </span>
          )}
          {structured.accounts_analyzed && (
            <span style={{ color: C.dim, fontSize: '0.62rem', fontFamily: font }}>
              Accounts: <span style={{ color: C.blue }}>{Array.isArray(structured.accounts_analyzed) ? structured.accounts_analyzed.length : structured.accounts_analyzed}</span>
            </span>
          )}
        </div>
      </div>

      {/* Consensus summary */}
      {structured.consensus_summary && (
        <div style={{ color: C.text, fontSize: '0.74rem', fontFamily: sansFont, lineHeight: 1.65,
          background: `${C.blue}08`, border: `1px solid ${C.blue}18`, borderRadius: 8, padding: '0.65rem 0.9rem' }}>
          {structured.consensus_summary}
        </div>
      )}

      {/* Consensus tickers */}
      {tickers.length > 0 && (
        <div>
          <div style={{ color: C.dim, fontSize: '0.6rem', fontWeight: 700, fontFamily: font,
            textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '0.5rem' }}>
            Consensus Tickers ({tickers.length})
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
            {tickers.map((t: any, i: number) => (
              <div key={i} style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: 8, padding: '0.75rem 0.9rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap', marginBottom: t.thesis || t.why_bullish ? 6 : 0 }}>
                  {t.rank != null && <span style={{ color: C.gold, fontWeight: 800, fontSize: '0.8rem', fontFamily: font }}>#{t.rank}</span>}
                  <span style={{ color: C.blue, fontWeight: 800, fontSize: '0.88rem', fontFamily: font }}>{t.ticker}</span>
                  {t.conviction && <ConvictionBadge value={t.conviction} />}
                  {t.consensus_strength && (
                    <span style={{ color: C.purple, fontSize: '0.62rem', fontFamily: font }}>{t.consensus_strength}</span>
                  )}
                  {t.trader_count != null && (
                    <span style={{ color: C.dim, fontSize: '0.62rem', fontFamily: font }}>{t.trader_count} traders</span>
                  )}
                  {t.signal_weight != null && (
                    <span style={{ color: C.gold, fontSize: '0.62rem', fontFamily: font }}>score {t.signal_weight}</span>
                  )}
                  {t.momentum && (
                    <span style={{ color: /increas/i.test(t.momentum) ? C.green : C.dim, fontSize: '0.62rem', fontFamily: font }}>↑ {t.momentum}</span>
                  )}
                </div>
                {t.thesis && <div style={{ color: C.text, fontSize: '0.72rem', fontFamily: sansFont, lineHeight: 1.6, marginBottom: 4 }}>{t.thesis}</div>}
                {t.why_bullish && <div style={{ color: C.green, fontSize: '0.68rem', fontFamily: sansFont, lineHeight: 1.5, marginBottom: 4 }}>Bullish: {t.why_bullish}</div>}
                {t.risks && <div style={{ color: C.red, fontSize: '0.68rem', fontFamily: sansFont, lineHeight: 1.5, marginBottom: 4 }}>Risks: {t.risks}</div>}
                {Array.isArray(t.representative_reasons) && t.representative_reasons.length > 0 && (
                  <div style={{ marginTop: 4 }}>
                    {t.representative_reasons.map((r: string, j: number) => (
                      <div key={j} style={{ color: C.dim, fontSize: '0.65rem', fontFamily: sansFont, lineHeight: 1.5 }}>• {r}</div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Momentum leaders */}
      {momentumLeaders.length > 0 && (
        <div>
          <div style={{ color: C.dim, fontSize: '0.6rem', fontWeight: 700, fontFamily: font,
            textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '0.4rem' }}>
            Momentum Leaders
          </div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
            {momentumLeaders.map((m: any, i: number) => (
              <div key={i} style={{ padding: '0.35rem 0.75rem', background: `${C.green}10`,
                border: `1px solid ${C.green}28`, borderRadius: 8 }}>
                <span style={{ color: C.green, fontWeight: 700, fontFamily: font, fontSize: '0.72rem' }}>{m.ticker}</span>
                {m.note && <span style={{ color: C.dim, fontSize: '0.62rem', fontFamily: sansFont, marginLeft: 6 }}>{m.note}</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Early vs Crowded */}
      {(earlyStage.length > 0 || crowded.length > 0) && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
          <div>
            <div style={{ color: C.dim, fontSize: '0.6rem', fontWeight: 700, fontFamily: font,
              textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '0.4rem' }}>
              Early Stage
            </div>
            {earlyStage.length > 0 ? earlyStage.map((e: any, i: number) => (
              <div key={i} style={{ padding: '0.35rem 0.65rem', background: `${C.purple}10`,
                border: `1px solid ${C.purple}28`, borderRadius: 6, marginBottom: 4 }}>
                <span style={{ color: C.purple, fontWeight: 700, fontFamily: font, fontSize: '0.72rem' }}>{e.ticker}</span>
                {e.note && <div style={{ color: C.dim, fontSize: '0.62rem', fontFamily: sansFont }}>{e.note}</div>}
              </div>
            )) : <div style={{ color: C.dim, fontSize: '0.65rem', fontFamily: sansFont }}>—</div>}
          </div>
          <div>
            <div style={{ color: C.dim, fontSize: '0.6rem', fontWeight: 700, fontFamily: font,
              textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '0.4rem' }}>
              Crowded
            </div>
            {crowded.length > 0 ? crowded.map((c: any, i: number) => (
              <div key={i} style={{ padding: '0.35rem 0.65rem', background: `${C.gold}08`,
                border: `1px solid ${C.gold}28`, borderRadius: 6, marginBottom: 4 }}>
                <span style={{ color: C.gold, fontWeight: 700, fontFamily: font, fontSize: '0.72rem' }}>{c.ticker}</span>
                {c.note && <div style={{ color: C.dim, fontSize: '0.62rem', fontFamily: sansFont }}>{c.note}</div>}
              </div>
            )) : <div style={{ color: C.dim, fontSize: '0.65rem', fontFamily: sansFont }}>—</div>}
          </div>
        </div>
      )}

      {/* Final opinion */}
      {(finalOpinion.reasoning || (finalOpinion.strongest_buys && finalOpinion.strongest_buys.length > 0)) && (
        <div style={{ background: `${C.gold}08`, border: `1px solid ${C.gold}22`, borderRadius: 8, padding: '0.75rem 0.9rem' }}>
          <div style={{ color: C.gold, fontSize: '0.6rem', fontWeight: 700, fontFamily: font,
            textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8 }}>
            Final Opinion
          </div>
          {finalOpinion.strongest_buys && finalOpinion.strongest_buys.length > 0 && (
            <div style={{ marginBottom: 6 }}>
              <span style={{ color: C.dim, fontSize: '0.65rem', fontFamily: font }}>Strongest Buys: </span>
              {finalOpinion.strongest_buys.map((t: string, i: number) => (
                <span key={i} style={{ color: C.green, fontWeight: 700, fontFamily: font, fontSize: '0.72rem', marginRight: 6 }}>{t}</span>
              ))}
            </div>
          )}
          {finalOpinion.watch_closely && finalOpinion.watch_closely.length > 0 && (
            <div style={{ marginBottom: 6 }}>
              <span style={{ color: C.dim, fontSize: '0.65rem', fontFamily: font }}>Watch Closely: </span>
              {finalOpinion.watch_closely.map((t: string, i: number) => (
                <span key={i} style={{ color: C.blue, fontWeight: 700, fontFamily: font, fontSize: '0.72rem', marginRight: 6 }}>{t}</span>
              ))}
            </div>
          )}
          {finalOpinion.reasoning && (
            <div style={{ color: C.text, fontSize: '0.72rem', fontFamily: sansFont, lineHeight: 1.65 }}>{finalOpinion.reasoning}</div>
          )}
        </div>
      )}
    </div>
  );
}

function GrokSocialAgent() {
  const [messages, setMessages] = useState<GrokMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesContainerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const container = messagesContainerRef.current;
    if (container) {
      container.scrollTop = container.scrollHeight;
    }
  }, [messages]);

  const sendMessage = useCallback(async (text: string, presetIntent?: string) => {
    const effectiveText = text.trim() || (presetIntent ? presetIntent.replace(/_/g, ' ') : '');
    if (!effectiveText && !presetIntent) return;
    if (loading) return;
    const userMsg: GrokMessage = { role: 'user', content: text.trim() || presetIntent || '', timestamp: Date.now() };
    setMessages(prev => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const payload: Record<string, any> = { query: text.trim() };
      if (presetIntent) payload.preset_intent = presetIntent;

      const res = await fetch(`${AGENT_BACKEND_URL}/api/social/query`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`${res.status}: ${errText.slice(0, 200)}`);
      }

      const data = await res.json();
      const responseText = data.response || data.analysis || data.error || 'No response received';

      setMessages(prev => [...prev, {
        role: 'assistant',
        content: responseText,
        timestamp: Date.now(),
        structured: data.structured || null,
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
        border: '1px solid rgba(255,255,255,0.06)',
        borderRadius: 12,
        padding: '1.5rem',
        marginBottom: '1.5rem',
      }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem' }}>
          <div style={{
            width: 32, height: 32,
            background: 'linear-gradient(135deg, #2090d0 0%, #3b82f6 50%, #80d8f8 100%)',
            borderRadius: 8,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
            fontSize: '0.9rem', fontWeight: 700, color: '#fff',
          }}>𝕏</div>
          <div>
            <h3 style={{
              fontFamily: "'JetBrains Mono', monospace",
              fontSize: '0.82rem', fontWeight: 600, color: '#e2e8f0',
              letterSpacing: '-0.01em', margin: 0,
            }}>Ask Caelyn</h3>
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
              background: 'rgba(10,12,18,0.85)',
              border: '1px solid rgba(255,255,255,0.06)',
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
            onBlur={e => { e.currentTarget.style.borderColor = 'rgba(255,255,255,0.06)'; }}
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            style={{
              background: 'linear-gradient(135deg, #2090d0 0%, #3b82f6 100%)',
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

        {/* ── Social preset buttons ── */}
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginBottom: '0.6rem' }}>
          {[
            { label: 'Consensus tickers among top X traders', preset: 'x_trader_consensus' },
            { label: 'Concensus tickers among select X traders', preset: 'x_select_trader_consensus' },
          ].map(({ label, preset }) => (
            <button
              key={preset}
              onClick={() => sendMessage(label, preset)}
              disabled={loading}
              style={{
                fontFamily: font,
                fontSize: '0.68rem',
                fontWeight: 700,
                color: '#38bdf8',
                background: 'rgba(56,189,248,0.08)',
                border: '1px solid rgba(56,189,248,0.3)',
                borderRadius: 8,
                padding: '0.45rem 1rem',
                cursor: loading ? 'not-allowed' : 'pointer',
                opacity: loading ? 0.4 : 1,
                transition: 'all 0.2s',
                letterSpacing: '0.02em',
                display: 'flex',
                alignItems: 'center',
                gap: '0.4rem',
              }}
              onMouseOver={e => {
                if (!loading) {
                  (e.currentTarget as HTMLButtonElement).style.background = 'rgba(56,189,248,0.15)';
                  (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(56,189,248,0.5)';
                }
              }}
              onMouseOut={e => {
                (e.currentTarget as HTMLButtonElement).style.background = 'rgba(56,189,248,0.08)';
                (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(56,189,248,0.3)';
              }}
            >
              <span style={{ fontSize: '0.7rem' }}>𝕏</span>
              {label}
            </button>
          ))}
        </div>

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
                background: 'rgba(32,144,208,0.06)',
                border: '1px solid rgba(32,144,208,0.2)',
                borderRadius: 100,
                padding: '0.35rem 0.75rem',
                cursor: loading ? 'not-allowed' : 'pointer',
                transition: 'all 0.2s',
                opacity: loading ? 0.4 : 1,
                whiteSpace: 'nowrap',
              }}
              onMouseOver={e => {
                if (!loading) {
                  (e.currentTarget as HTMLButtonElement).style.background = 'rgba(32,144,208,0.15)';
                  (e.currentTarget as HTMLButtonElement).style.color = '#94a3b8';
                  (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(32,144,208,0.4)';
                }
              }}
              onMouseOut={e => {
                (e.currentTarget as HTMLButtonElement).style.background = 'rgba(32,144,208,0.06)';
                (e.currentTarget as HTMLButtonElement).style.color = '#64748b';
                (e.currentTarget as HTMLButtonElement).style.borderColor = 'rgba(32,144,208,0.2)';
              }}
            >{prompt}</button>
          ))}
        </div>

        {/* Messages / Response area */}
        {(messages.length > 0 || loading) && (
          <div ref={messagesContainerRef} style={{
            maxHeight: 520,
            overflowY: 'auto',
            display: 'flex',
            flexDirection: 'column',
            gap: '0.6rem',
          }}>
            {messages.map((msg, i) => (
              <div key={i} style={{
                background: msg.role === 'user' ? 'rgba(32,144,208,0.08)' : '#0d0e22',
                border: `1px solid ${msg.role === 'user' ? 'rgba(32,144,208,0.2)' : 'rgba(255,255,255,0.06)'}`,
                borderRadius: 8,
                padding: '0.75rem 1rem',
              }}>
                <div style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: '0.6rem',
                  fontWeight: 700,
                  textTransform: 'uppercase' as const,
                  letterSpacing: '0.1em',
                  color: msg.role === 'user' ? '#80d8f8' : '#5cc8f0',
                  marginBottom: '0.4rem',
                }}>{msg.role === 'user' ? 'YOU' : 'GROK'}</div>
                <div style={{
                  fontFamily: msg.role === 'assistant' ? "'JetBrains Mono', monospace" : "'Outfit', sans-serif",
                  fontSize: msg.role === 'assistant' ? '0.74rem' : '0.8rem',
                  lineHeight: 1.65,
                  color: msg.role === 'user' ? '#c7d2fe' : '#94a3b8',
                }}>
                  {msg.role === 'assistant'
                    ? (msg.structured?.scan_type === 'x_trader_consensus' || msg.structured?.display_type === 'social')
                      ? renderConsensusResponse(msg.structured)
                      : isBriefingResponse(msg.content) || isBriefingResponse(msg.structured)
                        ? renderBriefingCard((isBriefingResponse(msg.content) ? msg.content : msg.structured) as BriefingResponse)
                        : renderGrokResponse(msg.content)
                    : msg.content}
                </div>
              </div>
            ))}
            {loading && (
              <div style={{
                background: '#0d0e22',
                border: '1px solid rgba(255,255,255,0.06)',
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
    <div className="min-h-screen text-white relative" style={{ background: '#050608', fontFamily: "'Outfit', sans-serif", lineHeight: 1.65 }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        .social-page .ice { color: #5cc8f0; }
        .social-page .gradient-text {
          background: linear-gradient(135deg, #e0f0ff 0%, #5cc8f0 40%, #2090d0 100%);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
        }
        .social-page .profile-cell:hover { background: rgba(255,255,255,0.03) !important; }
        .social-page .section-card:hover { background: rgba(255,255,255,0.03) !important; }
      `}</style>

      <div className="social-page relative" style={{ zIndex: 1 }}>
        <div style={{
          position: 'fixed', top: '-40%', left: '-20%', width: '140%', height: '140%',
          background: 'radial-gradient(ellipse 800px 600px at 20% 15%, rgba(40,160,220,0.04) 0%, transparent 70%), radial-gradient(ellipse 600px 500px at 80% 70%, rgba(60,180,240,0.03) 0%, transparent 70%), radial-gradient(ellipse 900px 400px at 50% 50%, rgba(50,170,230,0.02) 0%, transparent 60%)',
          pointerEvents: 'none', zIndex: 0
        }} />

        {/* HERO */}
        <div style={{ padding: '1.5rem 3rem 0', maxWidth: 900, margin: '0 auto', textAlign: 'center', position: 'relative', zIndex: 1 }}>
          <div style={{ position: 'relative', display: 'inline-flex', flexDirection: 'column', alignItems: 'center', gap: '0.4rem', paddingBottom: '0.5rem', marginBottom: '0.5rem' }}>
            {/* Blurred white blob — no border-radius corners, no hard edges, pure seamless fade */}
            <div style={{
              position: 'absolute',
              inset: -80,
              background: 'white',
              filter: 'blur(50px)',
              borderRadius: '50%',
              zIndex: 0,
              pointerEvents: 'none',
            }} />
            <img src={socialImage} alt="Caelyn.ai" style={{ width: 320, height: 'auto', objectFit: 'contain', position: 'relative', zIndex: 1 }} />
            <h1 style={{ fontSize: 'clamp(1.4rem, 3vw, 2rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.03em', margin: 0, marginTop: '-70px', position: 'relative', zIndex: 1 }}>
              <span className="gradient-text">Social</span>
            </h1>
            <p style={{ fontSize: '0.9rem', color: '#64748b', margin: 0, lineHeight: 1.5, position: 'relative', zIndex: 1 }}>
              Social intelligence and community analytics
            </p>
          </div>
        </div>

        {/* ═══ Grok Social Agent ═══ */}
        <div style={{ marginTop: '2rem' }}>
          <GrokSocialAgent />
        </div>

        {/* StocksX + CryptoX Side by Side */}
        <section style={{ maxWidth: 1400, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <div style={{ display: 'flex', gap: '1.5rem', alignItems: 'stretch' }}>

          {/* ── Left: Stocks X ── */}
          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.5rem' }}>StocksX</h3>
          <h2 style={{ fontSize: '1.4rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1rem', color: '#e2e8f0' }}>Stocks <span className="ice">X Accounts</span></h2>

          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.25rem', flex: 1, overflowY: 'auto', maxHeight: '65vh' }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '0.5rem' }}>
              {[
                { name: 'StockSavvyShay', handle: '@StockSavvyShay' },
                { name: 'HyperTechInvest', handle: '@HyperTechInvest' },
                { name: 'crux_capital_', handle: '@crux_capital_' },
                { name: 'SJCapitalInvest', handle: '@SJCapitalInvest' },
                { name: 'BlackPantherCap', handle: '@BlackPantherCap' },
                { name: 'Kaizen_Investor', handle: '@Kaizen_Investor' },
                { name: 'Venu_7_', handle: '@Venu_7_' },
                { name: 'CKCapitalxx', handle: '@CKCapitalxx' },
                { name: 'TheTape_TNM', handle: '@TheTape_TNM' },
                { name: 'equitydd', handle: '@equitydd' },
                { name: 'Speculator_io', handle: '@Speculator_io' },
                { name: 'DrJebaim', handle: '@DrJebaim' },
                { name: 'StonkValue', handle: '@StonkValue' },
                { name: 'stamatoudism', handle: '@stamatoudism' },
                { name: 'yianisz', handle: '@yianisz' },
                { name: 'sunxliao', handle: '@sunxliao' },
                { name: 'futurist_lens', handle: '@futurist_lens' },
                { name: 'Thomas_james_1', handle: '@Thomas_james_1' },
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
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account.name}</span>
                </SafeLink>
              ))}
            </div>
          </div>
          </div>
          {/* ── Right: Crypto X ── */}
          <div style={{ flex: 1, minWidth: 0, display: 'flex', flexDirection: 'column' }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.5rem' }}>CryptoX</h3>
          <h2 style={{ fontSize: '1.4rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1rem', color: '#e2e8f0' }}>Crypto <span className="ice">X Accounts</span></h2>
          <div style={{ flex: 1, overflowY: 'auto', maxHeight: '65vh', display: 'flex', flexDirection: 'column', gap: '1rem' }}>

          {/* Research and Fundamentals */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Research and Fundamentals</h3>
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
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Traders */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Traders</h3>
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
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
              <SafeLink
                href="https://x.com/sonder_crypto/status/1968059158491767121"
                style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                className="profile-cell"
              >
                <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>sonder_crypto</span>
              </SafeLink>
              <SafeLink
                href="https://x.com/alecTrading/status/1971938635097559333"
                style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                className="profile-cell"
              >
                <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>alecTrading</span>
              </SafeLink>
            </div>
          </div>

          {/* Thoughts & Opinions */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Thoughts & Opinions</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'CryptoZer0_'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Macro */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Macro</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                '_The_Prophet__'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Market Today */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Market Today</h3>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.5rem' }}>
              {[
                'aicryptopattern'
              ].map((account) => (
                <SafeLink
                  key={account}
                  href={`https://x.com/${account}`}
                  style={{ padding: '0.6rem 0.9rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                  className="profile-cell"
                >
                  <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                  <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#5cc8f0', fontWeight: 500 }}>{account}</span>
                </SafeLink>
              ))}
            </div>
          </div>

          {/* Chains */}
          <div style={{ background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, padding: '1.5rem', marginBottom: '1.5rem' }}>
            <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '1.25rem' }}>Chains</h3>

            {/* Base and Solana Ecosystems - Side by Side */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))', gap: '1rem', marginBottom: '1rem' }}>
              {/* Base Ecosystem */}
              <div style={{ background: 'rgba(8,10,16,0.9)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem', textAlign: 'center' }}>Base Ecosystem</h4>
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
                      style={{ padding: '0.6rem 0.75rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: '#5cc8f0', fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>

              {/* Solana Ecosystem */}
              <div style={{ background: 'rgba(8,10,16,0.9)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem', textAlign: 'center' }}>Solana Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'Dior100x', name: 'Dior100x', desc: '@Dior100x - Solana trading insights', color: '#5cc8f0' },
                    { handle: '_Shadow36', name: '_Shadow36', desc: '@_Shadow36 - Solana market analysis', color: '#5cc8f0' },
                    { handle: 'WolverCrypto', name: 'WolverCrypto', desc: '@WolverCrypto - Crypto trading insights', color: '#5cc8f0' },
                    { handle: 'watchingmarkets', name: 'watchingmarkets', desc: '@watchingmarkets - Market watching insights', color: '#5cc8f0' },
                    { handle: 'Crypto_Alch', name: 'Crypto_Alch', desc: '@Crypto_Alch - Crypto alchemy insights', color: '#5cc8f0' },
                    { handle: 'bruhbearr', name: 'bruhbearr', desc: '@bruhbearr - Solana trading insights', color: '#5cc8f0' },
                    { handle: 'AltcoinMarksman', name: 'AltcoinMarksman', desc: '@AltcoinMarksman - Solana market analysis', color: '#5cc8f0' },
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
              <div style={{ background: 'rgba(8,10,16,0.9)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem', textAlign: 'center' }}>Bittensor Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'tao_agent', name: 'TAO Agent', desc: '@tao_agent - Bittensor Signal Intelligence', color: '#5cc8f0' },
                    { handle: 'Bitcast_network', name: 'Bitcast Network', desc: '@Bitcast_network - TAO Network Analytics', color: '#5cc8f0' },
                    { handle: 'TaoStacker', name: 'TaoStacker', desc: '@TaoStacker - TAO Staking Insights', color: '#5cc8f0' },
                    { handle: 'TaoIsTheKey', name: 'TaoIsTheKey', desc: '@TaoIsTheKey - TAO Market Analysis', color: '#5cc8f0' },
                    { handle: 'varimotrades', name: 'VARiMOtrading', desc: '@varimotrades - TAO Trading Signals', color: '#5cc8f0' },
                    { handle: '_g_x_g', name: 'GXG', desc: '@_g_x_g - Bittensor Intelligence', color: '#5cc8f0' },
                    { handle: 'TalkingTensor', name: 'Talking Tensor', desc: '@TalkingTensor - Bittensor Insights', color: '#5cc8f0' },
                    { handle: 'Shogun__base', name: 'Shogun Base', desc: '@Shogun__base - Base Network Trading', color: '#5cc8f0' },
                    { handle: 'Victor_crypto_2', name: 'Victor Crypto', desc: '@Victor_crypto_2 - Crypto Market Analysis', color: '#5cc8f0' },
                    { handle: 'btcrenaissance', name: 'BTC Renaissance', desc: '@btcrenaissance - Bittensor Insights', color: '#5cc8f0' },
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
              <div style={{ background: 'rgba(8,10,16,0.9)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 10, padding: '1.25rem' }}>
                <h4 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem', textAlign: 'center' }}>BNB Ecosystem</h4>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: '0.5rem' }}>
                  {[
                    { handle: 'cryptoknight890', name: 'CryptoKnight890', desc: '@cryptoknight890 - BNB ecosystem insights' },
                    { handle: 'BastilleBtc', name: 'BastilleBtc', desc: '@BastilleBtc - BNB trading and insights' },
                    { handle: 'JuliusElum', name: 'JuliusElum', desc: '@JuliusElum - BNB ecosystem analysis' },
                  ].map((account) => (
                    <SafeLink
                      key={account.handle}
                      href={`https://x.com/${account.handle}`}
                      style={{ padding: '0.6rem 0.75rem', background: 'rgba(92,200,240,0.08)', border: '1px solid rgba(92,200,240,0.2)', borderRadius: 6, transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
                      className="profile-cell"
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginBottom: '0.25rem' }}>
                        <span style={{ color: '#5cc8f0', fontWeight: 700, fontSize: '0.85rem' }}>𝕏</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', color: '#5cc8f0', fontWeight: 500 }}>{account.name}</span>
                      </div>
                      <div style={{ fontSize: '0.68rem', color: '#475569' }}>{account.desc}</div>
                    </SafeLink>
                  ))}
                </div>
              </div>
            </div>
          </div>

          </div>
          </div>
          </div>
        </section>

        {/* Platforms Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Platforms</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>Social <span className="ice">Media</span></h2>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1px', background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, overflow: 'hidden' }}>
            <SafeLink
              href='https://substack.com/'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem' }}>📰</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0' }}>Substack</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Newsletter Publishing Platform</div>
            </SafeLink>

            <SafeLink
              href='https://x.com/home'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem', fontWeight: 700 }}>𝕏</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0' }}>X</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Media & News Feed</div>
            </SafeLink>

            <SafeLink
              href='https://farcaster.xyz/'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem' }}>
                <span style={{ fontSize: '1.2rem' }}>🌐</span>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0' }}>Farcaster</span>
              </div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Decentralized Social Network</div>
            </SafeLink>
          </div>
        </section>

        {/* Analytics Section */}
        <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>
          <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>Analytics</h3>
          <h2 style={{ fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1.5rem', color: '#e2e8f0' }}>AI-Powered <span className="ice">Intelligence</span></h2>

          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '1px', background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, overflow: 'hidden' }}>
            <SafeLink
              href='https://yaps.kaito.ai/'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.5rem' }}>Kaito</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>AI-Powered Social Intelligence</div>
            </SafeLink>

            <SafeLink
              href='https://app.kolytics.pro/leaderboard'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.5rem' }}>Kolytics</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Signal Analytics</div>
            </SafeLink>

            <SafeLink
              href='https://www.alphabot.app/pulse'
              style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', cursor: 'pointer', textAlign: 'left' }}
              className="section-card"
            >
              <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.5rem' }}>Alphabot</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 500, color: '#e2e8f0' }}>Social Sentiment Bot</div>
            </SafeLink>
          </div>
        </section>

        {/* FOOTER */}
        <footer style={{ borderTop: '1px solid rgba(255,255,255,0.06)', padding: '3rem', textAlign: 'center', color: '#5cc8f0', fontSize: '0.85rem', maxWidth: 880, margin: '2rem auto 0' }}>
          <p style={{ fontSize: '0.75rem', color: '#475569' }}>
            Social intelligence and community analytics
          </p>
        </footer>
      </div>
    </div>
  );
}
