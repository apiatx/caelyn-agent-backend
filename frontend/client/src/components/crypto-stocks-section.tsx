import React, { useEffect, useRef, memo } from 'react';
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ExternalLink, TrendingUp } from "lucide-react";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

const StockScreenerWidget = memo(function StockScreenerWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-screener.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      width: "100%",
      height: "100%",
      defaultColumn: "overview",
      defaultScreen: "most_capitalized",
      showToolbar: true,
      locale: "en",
      market: "us",
      colorTheme: "dark"
    });
    container.current.appendChild(script);
  }, []);
  return (
    <div className="tradingview-widget-container" ref={container} style={{ width: "100%", height: "100%" }}>
      <div className="tradingview-widget-container__widget" style={{ width: "100%", height: "100%" }}></div>
    </div>
  );
});

export default function CryptoStocksSection() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="space-y-4 lg:space-y-8">
      <div className="w-full h-[600px] sm:h-[700px] rounded-lg overflow-hidden border border-white/[0.06]">
        <StockScreenerWidget />
      </div>

      <iframe
        src="https://banterbubbles.com/?utm_source=cbanter&utm_medium=cbanter&utm_campaign=cbanter&source=cbanter#stocks"
        className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
        title="Banterbubbles Stock Analysis"
        frameBorder="0"
        loading="eager"
        sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
        allow="fullscreen; clipboard-write; autoplay; camera; microphone; geolocation"
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <iframe
          src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CAPITALCOM%3AUS500"
          className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
          title="TradingView (CAPITALCOM:US500) Chart"
          frameBorder="0"
          scrolling="no"
          allow="fullscreen"
        />
        <iframe
          src="https://trendspider.com/markets/"
          className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
          title="TrendSpider Markets"
          sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
        />
      </div>

      <GlassCard className="p-3 sm:p-4 lg:p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-5 h-5 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
            <TrendingUp className="w-3 h-3 text-white" />
          </div>
          <h3 className="text-lg font-semibold text-white">Primary Resources</h3>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-6 gap-3">
          <button
            onClick={() => openInNewTab('https://app.koyfin.com/home')}
            className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 hover:from-blue-500/20 hover:to-blue-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">Koyfin</div>
            <div className="text-xs text-crypto-silver">Professional market data and analytics</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.ainvest.com/screener/')}
            className="bg-gradient-to-br from-green-500/10 to-green-600/10 hover:from-green-500/20 hover:to-green-600/20 border border-green-500/20 hover:border-green-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-green-300 mb-1">AInvest</div>
            <div className="text-xs text-crypto-silver">AI-powered stock screening and analysis</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.thenew.money/')}
            className="bg-gradient-to-br from-emerald-500/10 to-emerald-600/10 hover:from-emerald-500/20 hover:to-emerald-600/20 border border-emerald-500/20 hover:border-emerald-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-emerald-300 mb-1">TheNew.Money</div>
            <div className="text-xs text-crypto-silver">Financial market intelligence and analysis</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://stockanalysis.com/trending/')}
            className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 hover:from-blue-500/20 hover:to-blue-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">StockAnalysis.com</div>
            <div className="text-xs text-crypto-silver">Trending stocks and market analysis</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://finance.yahoo.com/markets/stocks/most-active/')}
            className="bg-gradient-to-br from-yellow-500/10 to-yellow-600/10 hover:from-yellow-500/20 hover:to-yellow-600/20 border border-yellow-500/20 hover:border-yellow-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-yellow-300 mb-1">Yahoo Finance Most Active</div>
            <div className="text-xs text-crypto-silver">Most actively traded stocks</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://unusualwhales.com/stock-screener')}
            className="bg-gradient-to-br from-purple-500/10 to-purple-600/10 hover:from-purple-500/20 hover:to-purple-600/20 border border-purple-500/20 hover:border-purple-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-purple-300 mb-1">Unusual Whales</div>
            <div className="text-xs text-crypto-silver">Whale activity and stock screening</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://www.perplexity.ai/finance')}
            className="bg-gradient-to-br from-cyan-500/10 to-cyan-600/10 hover:from-cyan-500/20 hover:to-cyan-600/20 border border-cyan-500/20 hover:border-cyan-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-cyan-300 mb-1">Perplexity AI Finance</div>
            <div className="text-xs text-crypto-silver">AI-powered financial research and insights</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://stocktwits.com/')}
            className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 hover:from-blue-500/20 hover:to-blue-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">StockTwits</div>
            <div className="text-xs text-crypto-silver">Social trading platform</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://finviz.com/')}
            className="bg-gradient-to-br from-purple-500/10 to-purple-600/10 hover:from-purple-500/20 hover:to-purple-600/20 border border-purple-500/20 hover:border-purple-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-purple-300 mb-1">Finviz</div>
            <div className="text-xs text-crypto-silver">Stock screener and market visualization</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://tradytics.com/overall-market')}
            className="bg-gradient-to-br from-green-500/10 to-green-600/10 hover:from-green-500/20 hover:to-green-600/20 border border-green-500/20 hover:border-green-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-green-300 mb-1">Tradytics</div>
            <div className="text-xs text-crypto-silver">Overall market analysis and trends</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://app.intellectia.ai/stock-market')}
            className="bg-gradient-to-br from-purple-500/10 to-purple-600/10 hover:from-purple-500/20 hover:to-purple-600/20 border border-purple-500/20 hover:border-purple-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-purple-300 mb-1">Intellectia AI</div>
            <div className="text-xs text-crypto-silver">AI-powered stock market analytics</div>
          </button>
          
          <button
            onClick={() => openInNewTab('https://fintel.io/s/us')}
            className="bg-gradient-to-br from-indigo-500/10 to-blue-600/10 hover:from-indigo-500/20 hover:to-blue-600/20 border border-indigo-500/20 hover:border-indigo-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-indigo-300 mb-1">Fintel</div>
            <div className="text-xs text-crypto-silver">Institutional ownership data</div>
          </button>
        </div>
      </GlassCard>

      <GlassCard className="p-3 sm:p-4 lg:p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-5 h-5 bg-gradient-to-r from-gray-500 to-slate-500 rounded-full flex items-center justify-center">
            <ExternalLink className="w-3 h-3 text-white" />
          </div>
          <h3 className="text-lg font-semibold text-white">Secondary Resources</h3>
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
          <button
            onClick={() => openInNewTab('https://atypica.ai/study')}
            className="bg-gradient-to-br from-indigo-500/10 to-indigo-600/10 hover:from-indigo-500/20 hover:to-indigo-600/20 border border-indigo-500/20 hover:border-indigo-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-indigo-300 mb-1">Atypica AI Study</div>
            <div className="text-xs text-crypto-silver">AI-driven market research and analysis</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.tipranks.com/dashboard')}
            className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 hover:from-blue-500/20 hover:to-blue-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">TipRanks</div>
            <div className="text-xs text-crypto-silver">Analyst ratings and stock insights</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.marketwatch.com/')}
            className="bg-gradient-to-br from-green-500/10 to-green-600/10 hover:from-green-500/20 hover:to-green-600/20 border border-green-500/20 hover:border-green-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-green-300 mb-1">MarketWatch</div>
            <div className="text-xs text-crypto-silver">Financial news and market data</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.barchart.com/news/chart-of-the-day')}
            className="bg-gradient-to-br from-orange-500/10 to-orange-600/10 hover:from-orange-500/20 hover:to-orange-600/20 border border-orange-500/20 hover:border-orange-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-orange-300 mb-1">Chart of the day</div>
            <div className="text-xs text-crypto-silver">Daily featured charts and market insights</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.screener.in/explore/')}
            className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 hover:from-blue-500/20 hover:to-blue-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">Screener.in</div>
            <div className="text-xs text-crypto-silver">Indian stock screening and exploration</div>
          </button>

          <button
            onClick={() => openInNewTab('https://seekingalpha.com/etfs-and-funds/etf-tables/key_markets')}
            className="bg-gradient-to-br from-orange-500/10 to-amber-600/10 hover:from-orange-500/20 hover:to-amber-600/20 border border-orange-500/20 hover:border-orange-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-orange-300 mb-1">Seeking Alpha ETFs</div>
            <div className="text-xs text-crypto-silver">ETF tables and key market funds</div>
          </button>

          <button
            onClick={() => openInNewTab('https://app.askedgar.io/')}
            className="bg-gradient-to-br from-amber-500/10 to-yellow-600/10 hover:from-amber-500/20 hover:to-yellow-600/20 border border-amber-500/20 hover:border-amber-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-amber-300 mb-1">AskEdgar</div>
            <div className="text-xs text-crypto-silver">SEC filings and company documents</div>
          </button>

          <button
            onClick={() => openInNewTab('https://www.kavout.com/')}
            className="bg-gradient-to-br from-teal-500/10 to-teal-600/10 hover:from-teal-500/20 hover:to-teal-600/20 border border-teal-500/20 hover:border-teal-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
          >
            <div className="text-sm font-medium text-white group-hover:text-teal-300 mb-1">Kavout</div>
            <div className="text-xs text-crypto-silver">AI-powered investment research platform</div>
          </button>
        </div>
      </GlassCard>

    </div>
  );
}
