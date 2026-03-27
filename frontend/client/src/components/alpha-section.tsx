import { useEffect } from 'react';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { BarChart3, ExternalLink, TrendingUp, Link2, Star, Wallet, TrendingDown, Globe, Layers, Activity, Brain } from 'lucide-react';
import { openSecureLink } from '@/utils/security';
import onchainImage from "@assets/images_1756750962640.jpeg";
import diamondImage from "@assets/images (4)_1757213323269.jpeg";
import TopDailyGainersTop500 from './top-daily-gainers-top500';
import TopTrendingCMC from './top-trending-cmc';

// Enhanced Glass Card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-xl bg-gradient-to-br from-black/80 via-gray-900/60 to-black/90 border border-white/30 shadow-2xl shadow-purple-500/10 hover:shadow-purple-500/20 transition-all duration-500 ${className}`}>
    {children}
  </Card>
);

// Safe iframe component
const SafeIframe = ({ src, title, className = "" }: { src: string; title: string; className?: string }) => {
  return (
    <div className="w-full">
      <iframe
        src={src}
        title={title}
        className={`w-full h-[600px] rounded-lg border border-white/[0.06] ${className}`}
        frameBorder="0"
        loading="lazy"
        sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation"
        referrerPolicy="strict-origin-when-cross-origin"
        style={{
          background: '#000000',
          colorScheme: 'dark'
        }}
      />
    </div>
  );
};

interface SafeLinkProps {
  href: string;
  children: React.ReactNode;
  className?: string;
}

const SafeLink: React.FC<SafeLinkProps> = ({ href, children, className = "" }) => {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <button onClick={() => openInNewTab(href)} className={className}>
      {children}
    </button>
  );
};

export default function AlphaSection() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  useEffect(() => {
    const scriptId = 'altfins-market-data-script';
    if (!document.getElementById(scriptId)) {
      const script = document.createElement('script');
      script.id = scriptId;
      script.type = 'module';
      script.src = 'https://cdn.altfins.com/js/altfins-market-data-component.js';
      document.body.appendChild(script);
    }

    const style = document.createElement('style');
    style.id = 'altfins-market-clip-style';
    style.textContent = `
      .altfins-market-clip {
        position: relative;
        overflow: hidden;
      }
      .altfins-market-clip altfins-market-data-component {
        display: block;
        margin-bottom: -30px;
      }
    `;
    document.head.appendChild(style);
    return () => {
      const el = document.getElementById('altfins-market-clip-style');
      if (el) el.remove();
    };
  }, []);

  return (
    <div className="space-y-4">
      <div className="space-y-8">
        <GlassCard className="p-3 sm:p-4 lg:p-6">

          {/* Screening Section - Side by Side Layout */}
          <div className="flex flex-col lg:flex-row gap-6 lg:items-stretch">
            {/* 24h Gainers (CMC Top 500) - Left Side */}
            <div className="lg:w-1/2 flex">
              <div className="w-full">
                <TopDailyGainersTop500 />
              </div>
            </div>

            {/* Top 20 by Volume - Right Side */}
            <div className="lg:w-1/2 flex">
              <div className="w-full">
                <TopTrendingCMC />
              </div>
            </div>
          </div>

          {/* AltFins Market Data Widgets */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-8 mb-6">
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Bullish Pattern Breakouts</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='BULLISH_PATTERN_BREAKOUTS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "PATTERN_SUBTYPE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Bullish Emerging Patterns</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='BULLISH_EMERGING_PATTERNS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Bollinger Band Breakout Above</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='BOLLINGER_BAND_BREAKOUT_ABOVE' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Early Bullish Momentum Inflection</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='EARLY_BULLISH_MOMENTUM_INFLECTION' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "MACD_HISTOGRAM_H2"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Fresh Bullish EMA Crossover</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='FRESH_BULLISH_EMA_CROSSOVER' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Fresh Golden Crossover</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='FRESH_GOLDEN_CROSSOVER' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Fresh Bullish MACD Signal Line Crossover</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='FRESH_BULLISH_MACD_SIGNAL_LINE_CROSSOVER' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">New Local High</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='NEW_LOCAL_HIGH' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-amber-400 mb-3 tracking-wide uppercase">Very Overbought Coins</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='VERY_OVERBOUGHT_COINS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "MACD_BS_SIGNAL", "RSI14", "IR_RSI14"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-red-400 mb-3 tracking-wide uppercase">Very Oversold Coins</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='VERY_OVERSOLD_COINS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "MACD_BS_SIGNAL", "RSI14", "IR_RSI14"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4 lg:col-span-2">
              <h3 className="text-sm font-bold text-cyan-400 mb-3 tracking-wide uppercase">Oversold in Uptrend</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='OVERSOLD_IN_UPTREND' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "RSI14", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Pullback Day Uptrend</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='PULLBACK_DAY_UPTREND' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Pullback Week Uptrend</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='PULLBACK_WEEK_UPTREND' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1W", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-red-400 mb-3 tracking-wide uppercase">Strong Trend Downtrend</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='STRONG_DOWNTREND' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Strong Trend Uptrend</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='STRONG_UPTREND' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-blue-400 mb-3 tracking-wide uppercase">Trading in Range</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='TRADING_IN_RANGE' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-purple-400 mb-3 tracking-wide uppercase">Unusual Volume</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='UNUSUAL_VOLUME' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "VOLUME_CHANGE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Uptrend Bullish Momentum</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='UPTREND_BULLISH_MOMENTUM' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D", "MACD_BS_SIGNAL", "RSI14", "SHORT_TERM_SCORE", "MEDIUM_TERM_SCORE", "LONG_TERM_SCORE"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-emerald-400 mb-3 tracking-wide uppercase">Trend Momentum Bullish</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='UP_DOWN_TREND_AND_FRESH_MOMENTUM_INFLECTION_BULLISH' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-green-400 mb-3 tracking-wide uppercase">Top Gainers</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='TOP_GAINERS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/10 rounded-xl p-3 sm:p-4">
              <h3 className="text-sm font-bold text-red-400 mb-3 tracking-wide uppercase">Top Losers</h3>
              <div className="altfins-market-clip w-full" dangerouslySetInnerHTML={{
                __html: `<altfins-market-data-component theme='no-border compact dark' type='TOP_LOSERS' valueids='["COIN", "LAST_PRICE", "PRICE_CHANGE_1D"]' affiliateid='test_id'></altfins-market-data-component>`
              }} />
            </div>
          </div>

          {/* CoinGlass iframe */}
          <div className="mt-8 mb-8">
            <div className="mb-3 flex justify-end">
              <button
                onClick={() => openInNewTab('https://www.coinglass.com/')}
                className="text-blue-300 hover:text-blue-200 text-sm font-medium hover:underline transition-colors duration-300 flex items-center gap-1"
                data-testid="button-coinglass-external"
              >
                Open Full View →
              </button>
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
              <iframe
                src="https://www.coinglass.com/"
                className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                title="CoinGlass"
                frameBorder="0"
                scrolling="yes"
              />
            </div>
          </div>

          {/* Banterbubbles */}
          <div className="mb-6">
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
              <iframe
                src="https://banterbubbles.com/?utm_source=cbanter&utm_medium=cbanter&utm_campaign=cbanter&source=cbanter"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="Banterbubbles Market Intelligence"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                allow="fullscreen; clipboard-write; autoplay; camera; microphone; geolocation"
              />
            </div>
          </div>

          {/* All tool buttons - flex wrap for natural flow */}
          <div className="flex flex-wrap gap-2 mt-6 mb-6">
            <SafeLink href="https://app.nansen.ai/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-purple-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-purple-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-purple-400 font-bold text-xs">N</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Nansen</span>
              </div>
            </SafeLink>
            <SafeLink href="https://messari.io/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-orange-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-orange-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-orange-400 font-bold text-xs">M</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Messari</span>
              </div>
            </SafeLink>
            <SafeLink href="https://cookie.fun/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-yellow-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-yellow-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-yellow-400 font-bold text-xs">C</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Cookie.fun</span>
              </div>
            </SafeLink>
            <SafeLink href="https://coinmarketcap.com/?type=coins&tableRankBy=trending_all_24h" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-blue-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-blue-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-blue-400 font-bold text-xs">C</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">CMC</span>
              </div>
            </SafeLink>
            <SafeLink href="https://www.coingecko.com/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-green-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-green-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-green-400 font-bold text-xs">G</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">CoinGecko</span>
              </div>
            </SafeLink>
            <SafeLink href="https://opensea.io/stats/tokens" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-cyan-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-cyan-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-cyan-400 font-bold text-xs">O</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">OpenSea</span>
              </div>
            </SafeLink>
            <SafeLink href="https://dex.coinmarketcap.com/token/all/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-blue-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-blue-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-blue-400 font-bold text-[10px]">CD</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">CMC Dex</span>
              </div>
            </SafeLink>
            <SafeLink href="https://geckoterminal.com/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-emerald-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-emerald-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-emerald-400 font-bold text-[10px]">GT</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">GeckoTerminal</span>
              </div>
            </SafeLink>
            <SafeLink href="https://dexscreener.com/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-purple-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-purple-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-purple-400 font-bold text-[10px]">DS</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">DexScreener</span>
              </div>
            </SafeLink>
            <SafeLink href="https://dexcheck.ai/app" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-yellow-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-yellow-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-yellow-400 font-bold text-xs">D</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">DexCheck</span>
              </div>
            </SafeLink>
            <SafeLink href="https://dapp.velvet.capital/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-pink-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-pink-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-pink-400 font-bold text-xs">V</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Velvet Capital</span>
              </div>
            </SafeLink>
            <SafeLink href="https://www.binance.com/en/markets/alpha-all" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-yellow-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-yellow-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-yellow-400 font-bold text-xs">B</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Binance Alpha</span>
              </div>
            </SafeLink>
            <SafeLink href="https://web3.binance.com/en/markets/alpha?chain=bsc" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-orange-500/40 rounded-lg transition-all duration-200 group">
              <div className="flex items-center gap-2">
                <div className="w-7 h-7 bg-orange-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-orange-400 font-bold text-[10px]">W3</span></div>
                <span className="text-white/70 group-hover:text-white text-sm font-medium">Web3 Alpha</span>
              </div>
            </SafeLink>
          </div>

          {/* AI Agents */}
          <div className="mb-6">
            <h4 className="text-xs font-semibold text-crypto-silver/60 uppercase tracking-widest mb-3 px-1">AI Agents</h4>
            <div className="flex flex-wrap gap-2">
              <SafeLink href="https://agents.cookie.fun/" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-green-500/40 rounded-lg transition-all duration-200 group">
                <div className="flex items-center gap-2">
                  <div className="w-7 h-7 bg-green-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-green-400 font-bold text-xs">C</span></div>
                  <span className="text-white/70 group-hover:text-white text-sm font-medium">Cookie Agents</span>
                </div>
              </SafeLink>
              <SafeLink href="https://ayaoracle.xyz/#agents_data" className="p-2.5 bg-white/[0.03] hover:bg-white/[0.07] border border-white/[0.06] hover:border-indigo-500/40 rounded-lg transition-all duration-200 group">
                <div className="flex items-center gap-2">
                  <div className="w-7 h-7 bg-indigo-500/20 rounded-md flex items-center justify-center flex-shrink-0"><span className="text-indigo-400 font-bold text-xs">A</span></div>
                  <span className="text-white/70 group-hover:text-white text-sm font-medium">Aya AI</span>
                </div>
              </SafeLink>
            </div>
          </div>
        </GlassCard>
      </div>

    </div>
  );
}