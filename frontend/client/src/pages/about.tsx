import caelynLogo from "@assets/image_1771576238262.png";
import caelynFairy1 from "@assets/image_1771572217667.png";
import caelynFairy2 from "@assets/ChatGPT_Image_Feb_20,_2026,_12_58_48_AM_1771570952831.png";
import caelynFairy3 from "@assets/image_1771565690741.png";

export default function AboutPage() {
  return (
    <div className="min-h-screen text-white relative" style={{ background: '#050608', fontFamily: "'Outfit', sans-serif", lineHeight: 1.65 }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        .about-page .ice { color: #5cc8f0; }
        .about-page .gradient-text {
          background: linear-gradient(135deg, #e0f0ff 0%, #5cc8f0 40%, #2090d0 100%);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
        }
        .about-page .data-cell:hover { background: rgba(255,255,255,0.03) !important; }
        .about-page .pipeline-wrap::before {
          content: '';
          position: absolute;
          left: 15px;
          top: 24px;
          bottom: 24px;
          width: 1px;
          background: linear-gradient(to bottom, #2090d0, #5cc8f0, #80d8f8);
          opacity: 0.3;
        }
      `}</style>

      <div className="about-page relative" style={{ zIndex: 1 }}>
        <div style={{
          position: 'fixed', top: '-40%', left: '-20%', width: '140%', height: '140%',
          background: 'radial-gradient(ellipse 800px 600px at 20% 15%, rgba(40,160,220,0.04) 0%, transparent 70%), radial-gradient(ellipse 600px 500px at 80% 70%, rgba(60,180,240,0.03) 0%, transparent 70%), radial-gradient(ellipse 900px 400px at 50% 50%, rgba(50,170,230,0.02) 0%, transparent 60%)',
          pointerEvents: 'none', zIndex: 0
        }} />

        {/* HERO */}
        <div style={{ padding: '4rem 3rem 2rem', maxWidth: 900, margin: '0 auto', textAlign: 'center', position: 'relative', zIndex: 1 }}>
          <div style={{
            display: 'inline-block', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.72rem', fontWeight: 500,
            letterSpacing: '0.12em', textTransform: 'uppercase', color: '#5cc8f0',
            border: '1px solid rgba(92,200,240,0.2)', borderRadius: 100, padding: '0.4rem 1.2rem',
            marginBottom: '2.5rem', background: 'rgba(92,200,240,0.04)'
          }}>
            Cross-Asset Trading Intelligence
          </div>
          <h1 style={{ fontSize: 'clamp(2.4rem, 5vw, 3.6rem)', fontWeight: 700, lineHeight: 1.15, letterSpacing: '-0.03em', marginBottom: '1.5rem' }}>
            One agent. Every market.<br />
            <span className="gradient-text">Institutional-grade signal.</span>
          </h1>
          <p style={{ fontSize: '1.15rem', color: '#64748b', maxWidth: 680, margin: '0 auto', lineHeight: 1.7 }}>
            CaelynAI is an AI trading terminal that aggregates 19 real-time data sources, scores opportunities across stocks, crypto, and commodities using a deterministic framework, and delivers actionable trade plans — not market commentary.
          </p>
        </div>

        {/* CAELYN.AI LOGO IMAGE */}
        <div style={{ display: 'flex', justifyContent: 'center', padding: '1rem 0' }}>
          <div style={{ maxWidth: 550, width: '90%' }}>
            <img src={caelynLogo} alt="CaelynAI" style={{ width: '100%', height: 'auto', display: 'block' }} />
          </div>
        </div>

        {/* DIVIDER */}
        <div style={{ width: 60, height: 2, background: 'linear-gradient(135deg, #2090d0, #3b82f6, #80d8f8)', margin: '0 auto 2rem', borderRadius: 2 }} />

        {/* WHAT IT IS */}
        <Section>
          <SectionLabel>What CaelynAI Is</SectionLabel>
          <h2 style={h2Style}>A <span className="ice">decision engine</span> for active traders</h2>
          <p style={{ ...pStyle, color: '#64748b', fontSize: '1.05rem' }}>
            CaelynAI replaces the 20-tab workflow. It pulls real-time price action, technical indicators, fundamental data, derivatives positioning, social sentiment, and macro signals — then a scoring engine ranks everything by conviction before an AI strategist synthesizes the output into specific trades with entries, stops, and targets.
          </p>
          <p style={pStyle}>
            Every recommendation passes a simple test: would a portfolio manager with $2M of their own capital actually size into this position? If the answer is no, it doesn't make the cut. If nothing qualifies, CaelynAI says so. Capital preservation is a valid output.
          </p>
        </Section>

        {/* COVERAGE */}
        <Section>
          <SectionLabel>Coverage</SectionLabel>
          <h2 style={h2Style}>Stocks. Crypto. Commodities. <span className="ice">Ranked together.</span></h2>
          <p style={pStyle}>
            Most tools silo asset classes. CaelynAI evaluates them on the same scorecard. If the best risk-adjusted setup today is a gold ETF, it surfaces a gold ETF — not a mediocre tech stock just because you opened the equity tab.
          </p>
          <DataGrid>
            <DataCell label="Equities" value={<>Large, mid, small, and <span className="ice">micro-cap</span> stocks. Screeners for oversold bounces, value-momentum, insider breakouts, high-growth small caps, dividend value, and short squeezes.</>} />
            <DataCell label="Crypto" value={<>Top 500 coins via CoinGecko and CMC. <span className="ice">HyperLiquid perps</span> with real-time funding rates, squeeze candidates, and open interest. altFINS technical analysis across 2,000+ tokens. Grok-powered X sentiment.</>} />
            <DataCell label="Commodities" value="Gold, oil, silver, copper, natural gas, uranium. Tracked through ETF proxies with the same TA and macro overlay applied to equities." />
            <DataCell label="Macro" value={<>Fed rate trajectory, CPI, PCE, GDP, unemployment, yield curve, VIX, DXY, and <span className="ice">Fear &amp; Greed</span> — woven into every single recommendation as the decision lens.</>} fullWidth />
          </DataGrid>
        </Section>

        {/* IMAGE BETWEEN SECTIONS */}
        <div style={{ display: 'flex', justifyContent: 'center', padding: '1rem 0' }}>
          <img src={caelynFairy2} alt="CaelynAI" style={{ width: 280, height: 280, borderRadius: 16, objectFit: 'cover' }} />
        </div>

        {/* ARCHITECTURE */}
        <Section>
          <SectionLabel>Architecture</SectionLabel>
          <h2 style={h2Style}>How the <span className="ice">pipeline</span> works</h2>
          <p style={pStyle}>
            Every scan follows the same disciplined flow. No shortcuts, no vibes-based analysis.
          </p>
          <div className="pipeline-wrap" style={{ display: 'flex', flexDirection: 'column', gap: 0, margin: '2.5rem 0', position: 'relative' }}>
            <PipelineStep text={<><strong>Routing</strong> — Preset intents resolve deterministically. Free-text queries go through an orchestrator that builds a data-gathering plan based on asset classes, modules, and filters.</>} />
            <PipelineStep text={<><strong>Data Gathering</strong> — Parallel API calls with per-source timeouts, circuit breakers on repeated failures, tiered fallbacks, and a caching layer. If a source is down, the pipeline degrades gracefully — never returns blank.</>} />
            <PipelineStep text={<><strong>Regime Detection</strong> — Macro regime (risk-on / risk-off / neutral) is computed from VIX, DXY, yield curve, Fear &amp; Greed. This regime gates which asset classes and sectors get elevated or penalized.</>} />
            <PipelineStep text={<><strong>Deterministic Scoring</strong> — Each candidate is scored 0–100 using weighted components: 30% Technical (Weinstein stage, trend, volume, pattern), 30% Catalyst (specificity, time-bound, repricing potential), 20% Sector Alignment, 10% Social Momentum, 10% Risk/Reward.</>} />
            <PipelineStep text={<><strong>AI Synthesis</strong> — Claude receives the scored, compressed data and produces the final output: trade plans with entry, stop, target, R:R, conviction, position sizing tier, and a thesis grounded in the data — not generated from memory.</>} />
          </div>
        </Section>

        {/* CRYPTO INTELLIGENCE */}
        <Section>
          <SectionLabel>Crypto Intelligence</SectionLabel>
          <h2 style={h2Style}>Built for <span className="ice">derivatives traders</span></h2>
          <p style={pStyle}>
            The crypto module treats BTC as the only true investment. Everything else is a trade based on hype cycles paired with real technical momentum or catalysts. The system is built around this philosophy.
          </p>
          <FeatureRow label="HyperLiquid Perps" desc={<><strong>Real-time funding rates, open interest, and volume</strong> across every perpetual future on HyperLiquid. Squeeze candidates (extreme negative funding + rising price), crowded longs (liquidation risk), and funding divergences — the highest-signal trades in crypto.</>} />
          <FeatureRow label="X Sentiment via Grok" desc={<><strong>Social velocity detection</strong> — not just who's mentioned, but whose mentions are accelerating fastest. A micro-cap going from 100 to 1,000 mentions is higher signal than a large-cap with steady 5,000. Grok scans X in real-time and returns structured sentiment with BTC context.</>} />
          <FeatureRow label="altFINS TA" desc={<><strong>Pre-computed technical analysis across 2,000+ crypto assets.</strong> RSI, MACD, SMA/EMA crossovers, chart patterns, candlestick patterns, support/resistance, and trend scores. CaelynAI never calculates indicators from raw candles — it uses institutional-grade pre-computed signals.</>} />
          <FeatureRow label="Narrative Rotation" desc={<><strong>Hot category tracking</strong> from CoinGecko and CMC. Which sectors (AI, gaming, RWA, meme coins, L2s, DePIN) are gaining market cap? Dual-trending across both platforms is the strongest momentum signal.</>} isLast />
        </Section>

        {/* IMAGE BETWEEN SECTIONS */}
        <div style={{ display: 'flex', justifyContent: 'center', padding: '1rem 0' }}>
          <img src={caelynFairy3} alt="CaelynAI" style={{ width: 280, height: 280, borderRadius: '50%', objectFit: 'cover' }} />
        </div>

        {/* VS ALTERNATIVES */}
        <Section>
          <SectionLabel>The Difference</SectionLabel>
          <h2 style={h2Style}>What this <span className="ice">replaces</span></h2>
          <table style={{ width: '100%', borderCollapse: 'separate', borderSpacing: 0, border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, overflow: 'hidden', margin: '2rem 0', fontSize: '0.88rem' }}>
            <thead>
              <tr>
                <th style={thStyle}></th>
                <th style={thStyle}>Typical AI Tool</th>
                <th style={thStyle}>CaelynAI</th>
              </tr>
            </thead>
            <tbody>
              <CompareRow label="Output" them={'"NVDA looks interesting. Consider doing more research."'} us={<>Entry $142.50 · Stop $138 · Target $158 · R:R 3.4:1 · HIGH conviction · Tier 2 size</>} />
              <CompareRow label="Data" them="ChatGPT training data (months old)" us={<>19 live APIs aggregated per request <Tag color="green">real-time</Tag></>} />
              <CompareRow label="Scope" them="Stocks only, or crypto only" us={<>Stocks + crypto + commodities ranked on one scorecard <Tag color="blue">cross-asset</Tag></>} />
              <CompareRow label="Crypto Derivatives" them="None" us={<>HyperLiquid funding, OI, squeezes, divergences <Tag color="green">live</Tag></>} />
              <CompareRow label="Social Signal" them="Summarizes Reddit threads" us="Grok scans X for social velocity + StockTwits + Reddit, cross-referenced with volume" />
              <CompareRow label="When nothing's good" them="Forces 10 picks anyway" us={<>"Nothing actionable. Cash is the position." <Tag color="blue">honest</Tag></>} isLast />
            </tbody>
          </table>
        </Section>

        {/* DATA SOURCES */}
        <Section>
          <SectionLabel>Data Sources</SectionLabel>
          <h2 style={h2Style}><span className="ice">19</span> providers. One unified view.</h2>
          <p style={pStyle}>
            Every scan pulls from multiple sources in parallel. If one is rate-limited or down, the others fill the gap. No single point of failure.
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))', gap: '0.75rem', margin: '1.5rem 0' }}>
            <SourceItem color="#5cc8f0" name="Finnhub" />
            <SourceItem color="#5cc8f0" name="Finviz" />
            <SourceItem color="#5cc8f0" name="StockAnalysis" />
            <SourceItem color="#5cc8f0" name="AlphaVantage" />
            <SourceItem color="#5cc8f0" name="Polygon" />
            <SourceItem color="#5cc8f0" name="TwelveData" />
            <SourceItem color="#5cc8f0" name="FMP" />
            <SourceItem color="#22c55e" name="CoinGecko" />
            <SourceItem color="#22c55e" name="CoinMarketCap" />
            <SourceItem color="#22c55e" name="HyperLiquid" />
            <SourceItem color="#22c55e" name="altFINS" />
            <SourceItem color="#2090d0" name="Grok / xAI" />
            <SourceItem color="#2090d0" name="StockTwits" />
            <SourceItem color="#2090d0" name="Reddit" />
            <SourceItem color="#f97316" name="FRED" />
            <SourceItem color="#f97316" name="Fear & Greed" />
            <SourceItem color="#f97316" name="SEC / EDGAR" />
            <SourceItem color="#80d8f8" name="Options Flow" />
            <SourceItem color="#80d8f8" name="Claude (Anthropic)" />
          </div>
        </Section>

        {/* IMAGE BETWEEN SECTIONS */}
        <div style={{ display: 'flex', justifyContent: 'center', padding: '1rem 0' }}>
          <img src={caelynFairy2} alt="CaelynAI" style={{ width: 280, height: 280, borderRadius: 16, objectFit: 'cover' }} />
        </div>

        {/* CAPABILITIES */}
        <Section>
          <SectionLabel>Capabilities</SectionLabel>
          <h2 style={h2Style}>What you can <span className="ice">ask</span></h2>
          <DataGrid>
            <DataCell label="Daily Briefing" value="Market pulse, top scanner signals, sector rotation, portfolio bias, and macro catalysts. One scan, full context." />
            <DataCell label="Trending Now" value="Cross-asset shortlist with social receipts, technical confirmations, ratings, confidence scores, and position sizing." />
            <DataCell label="Best Trades" value="Technical-first. Multiple indicator confirmation. Entry, stop, targets, timeframe, R:R, and TradingView-linked symbols." />
            <DataCell label="Crypto Scanner" value="BTC/ETH dominance, Fear & Greed, HyperLiquid perps, squeeze candidates, X sentiment velocity, narrative rotation, and momentum picks." />
            <DataCell label="6 Equity Screeners" value="Oversold+Growing, Value+Momentum, Insider+Breakout, High Growth Small Cap, Dividend Value, Short Squeeze. Deterministic filters, not AI guesses." />
            <DataCell label="Natural Language" value={<>Ask anything. "What's the best squeeze play right now?" "Compare NVDA and AMD." "Is this a good time to buy gold?" The agent routes to the right data automatically.</>} />
          </DataGrid>
        </Section>

        {/* PORTFOLIO */}
        <Section>
          <SectionLabel>Portfolio</SectionLabel>
          <h2 style={h2Style}>Track positions. Get <span className="ice">AI portfolio review.</span></h2>
          <p style={pStyle}>
            Add your holdings — stocks, ETFs, crypto, commodities. See real-time P&L, asset allocation, and concentration risk. Then hit "AI Portfolio Review" for a position-by-position verdict: BUY MORE, HOLD, TRIM, or SELL — each grounded in current technicals, fundamentals, and macro context.
          </p>
        </Section>

        {/* BOTTOM CAELYN.AI LOGO IMAGE */}
        <div style={{ display: 'flex', justifyContent: 'center', padding: '1rem 0' }}>
          <div style={{ maxWidth: 360, width: '80%', WebkitMaskImage: 'radial-gradient(ellipse 90% 80% at center, black 40%, transparent 100%)', maskImage: 'radial-gradient(ellipse 90% 80% at center, black 40%, transparent 100%)' }}>
            <img src={caelynLogo} alt="CaelynAI" style={{ width: '100%', height: 'auto', display: 'block' }} />
          </div>
        </div>

        {/* FOOTER */}
        <footer style={{ borderTop: '1px solid rgba(255,255,255,0.06)', padding: '3rem', textAlign: 'center', color: '#64748b', fontSize: '0.85rem' }}>
          <p><span style={{ background: 'linear-gradient(135deg, #2090d0, #3b82f6, #80d8f8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', fontWeight: 600 }}>CaelynAI</span> — Signal over noise.</p>
          <p style={{ marginTop: '0.5rem', fontSize: '0.75rem', color: '#475569' }}>
            Not financial advice. CaelynAI is a research tool. All trading involves risk.
          </p>
        </footer>
      </div>
    </div>
  );
}

const h2Style: React.CSSProperties = { fontSize: '1.6rem', fontWeight: 600, letterSpacing: '-0.02em', marginBottom: '1rem', color: '#e2e8f0' };
const pStyle: React.CSSProperties = { color: '#475569', marginBottom: '1.25rem' };
const thStyle: React.CSSProperties = { background: 'rgba(10,12,18,0.85)', padding: '1rem 1.25rem', textAlign: 'left', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#64748b', borderBottom: '1px solid rgba(255,255,255,0.06)' };

function Section({ children }: { children: React.ReactNode }) {
  return <section style={{ maxWidth: 880, margin: '0 auto', padding: '2rem 3rem', position: 'relative', zIndex: 1 }}>{children}</section>;
}

function SectionLabel({ children }: { children: string }) {
  return <h3 style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.78rem', fontWeight: 500, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#5cc8f0', marginBottom: '0.75rem' }}>{children}</h3>;
}

function DataGrid({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '1px', background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 12, overflow: 'hidden', margin: '2.5rem 0' }}>
      {children}
    </div>
  );
}

function DataCell({ label, value, fullWidth }: { label: string; value: React.ReactNode; fullWidth?: boolean }) {
  return (
    <div className="data-cell" style={{ background: 'rgba(10,12,18,0.85)', padding: '1.5rem', transition: 'background 0.2s', ...(fullWidth ? { gridColumn: '1 / -1', textAlign: 'center' } : {}) }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.7rem', fontWeight: 500, letterSpacing: '0.08em', textTransform: 'uppercase', color: '#64748b', marginBottom: '0.5rem' }}>{label}</div>
      <div style={{ fontSize: '1.1rem', fontWeight: 600, color: '#e2e8f0', lineHeight: 1.4 }}>{value}</div>
    </div>
  );
}

function PipelineStep({ text }: { text: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', alignItems: 'flex-start', gap: '1.25rem', padding: '0.9rem 0', position: 'relative' }}>
      <div style={{ width: 10, height: 10, borderRadius: '50%', background: '#5cc8f0', boxShadow: '0 0 8px rgba(92,200,240,0.3)', marginTop: 5, flexShrink: 0, marginLeft: 10 }} />
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.82rem', color: '#475569', lineHeight: 1.5 }}>{text}</div>
    </div>
  );
}

function FeatureRow({ label, desc, isLast }: { label: string; desc: React.ReactNode; isLast?: boolean }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: '200px 1fr', gap: '2rem', padding: '2rem 0', borderBottom: isLast ? 'none' : '1px solid rgba(255,255,255,0.06)', alignItems: 'start' }}>
      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', fontWeight: 500, letterSpacing: '0.06em', color: '#5cc8f0', paddingTop: '0.15rem' }}>{label}</div>
      <div style={{ color: '#475569', fontSize: '0.95rem' }}>{desc}</div>
    </div>
  );
}

function CompareRow({ label, them, us, isLast }: { label: string; them: string; us: React.ReactNode; isLast?: boolean }) {
  const tdStyle: React.CSSProperties = { padding: '0.85rem 1.25rem', borderBottom: isLast ? 'none' : '1px solid rgba(255,255,255,0.04)', color: '#475569' };
  return (
    <tr>
      <td style={tdStyle}>{label}</td>
      <td style={{ ...tdStyle, color: '#64748b' }}>{them}</td>
      <td style={{ ...tdStyle, color: '#5cc8f0', fontWeight: 500 }}>{us}</td>
    </tr>
  );
}

function Tag({ color, children }: { color: 'green' | 'red' | 'blue'; children: string }) {
  const colors = {
    green: { bg: 'rgba(34,197,94,0.12)', text: '#22c55e' },
    red: { bg: 'rgba(239,68,68,0.12)', text: '#ef4444' },
    blue: { bg: 'rgba(92,200,240,0.12)', text: '#5cc8f0' },
  };
  return (
    <span style={{ display: 'inline-block', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.65rem', fontWeight: 500, letterSpacing: '0.05em', padding: '0.2rem 0.6rem', borderRadius: 4, textTransform: 'uppercase', background: colors[color].bg, color: colors[color].text }}>
      {children}
    </span>
  );
}

function SourceItem({ color, name }: { color: string; name: string }) {
  return (
    <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '0.75rem', color: '#475569', padding: '0.6rem 0.9rem', background: 'rgba(10,12,18,0.85)', border: '1px solid rgba(255,255,255,0.06)', borderRadius: 6, display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
      <span style={{ width: 6, height: 6, borderRadius: '50%', background: color, flexShrink: 0 }} />
      {name}
    </div>
  );
}
