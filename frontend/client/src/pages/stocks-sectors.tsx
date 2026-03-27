import { useEffect, useRef, memo } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, ExternalLink, BarChart3 } from "lucide-react";


const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

const ETFHeatmapWidget = memo(function ETFHeatmapWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-etf-heatmap.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      dataSource: "AllUSEtf",
      blockSize: "Value.Traded|1W",
      blockColor: "change",
      grouping: "asset_class",
      locale: "en",
      symbolUrl: "",
      colorTheme: "dark",
      hasTopBar: false,
      isDataSetEnabled: false,
      isZoomEnabled: true,
      hasSymbolTooltip: true,
      isMonoSize: false,
      width: "100%",
      height: "100%"
    });
    container.current.appendChild(script);
  }, []);

  return (
    <div className="tradingview-widget-container" ref={container}>
      <div className="tradingview-widget-container__widget"></div>
    </div>
  );
});

export default function StocksSectorsPage() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-4 lg:space-y-8">
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 mb-6">
              <div className="flex items-center gap-2">
                <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-teal-500 to-cyan-500 rounded-full flex items-center justify-center">
                  <BarChart3 className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                </div>
                <h3 className="text-lg sm:text-xl font-semibold text-white">ETF Heatmap</h3>
                <Badge className="bg-teal-500/20 text-teal-400 border-teal-500/30 text-xs">ALL US ETFs</Badge>
              </div>
            </div>
            <div className="w-full h-[600px] sm:h-[700px] rounded-lg overflow-hidden border border-white/[0.06]">
              <ETFHeatmapWidget />
            </div>
          </GlassCard>

          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 mb-6">
              <div className="flex items-center gap-2">
                <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                  <BarChart3 className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                </div>
                <h3 className="text-lg sm:text-xl font-semibold text-white">Stage Analysis Screener</h3>
                <Badge className="bg-gradient-to-r from-purple-500/20 to-pink-500/20 text-white border-white/[0.06] text-xs">SECTORS & FUNDS</Badge>
              </div>
              <button onClick={() => openInNewTab('https://screener.nextbigtrade.com/#/markets')} className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm flex items-center gap-1">
                <ExternalLink className="w-3 h-3" />
                Open Full View
              </button>
            </div>

            <div className="w-full space-y-6">
              <iframe
                src="https://screener.nextbigtrade.com/#/markets"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="Next Big Trade Sectors Screener"
                loading="eager"
                referrerPolicy="no-referrer"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                allow="fullscreen; clipboard-write; autoplay; camera; microphone; geolocation"
                frameBorder="0"
              />
              
              <button
                onClick={() => openInNewTab('https://www.vaneck.com/us/en/investments/social-sentiment-etf-buzz/overview/')}
                className="w-full bg-gradient-to-br from-amber-500/10 to-yellow-600/10 hover:from-amber-500/20 hover:to-yellow-600/20 border border-amber-500/20 hover:border-amber-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
              >
                <div className="text-sm font-medium text-white group-hover:text-amber-300 mb-1">BUZZ Social Sentiment ETF</div>
                <div className="text-xs text-crypto-silver">VanEck social sentiment ETF overview and performance</div>
              </button>

              <button
                onClick={() => openInNewTab('https://www.ssga.com/us/en/institutional/resources/sector-tracker#currentTab=dayOne&fundTicker=xle')}
                className="w-full bg-gradient-to-br from-yellow-500/10 to-amber-600/10 hover:from-yellow-500/20 hover:to-amber-600/20 border border-yellow-500/20 hover:border-yellow-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
              >
                <div className="text-sm font-medium text-white group-hover:text-yellow-300 mb-1">SPDR Sector Tracker</div>
                <div className="text-xs text-crypto-silver">State Street sector performance and ETF analysis</div>
              </button>
              
              <button
                onClick={() => openInNewTab('https://www.slickcharts.com/')}
                className="w-full bg-gradient-to-br from-blue-500/10 to-cyan-600/10 hover:from-blue-500/20 hover:to-cyan-600/20 border border-blue-500/20 hover:border-blue-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
              >
                <div className="text-sm font-medium text-white group-hover:text-blue-300 mb-1">SlickCharts Indices</div>
                <div className="text-xs text-crypto-silver">Stock market indices and data</div>
              </button>
              
              <button
                onClick={() => openInNewTab('https://www.etf.com/')}
                className="w-full bg-gradient-to-br from-green-500/10 to-emerald-600/10 hover:from-green-500/20 hover:to-emerald-600/20 border border-green-500/20 hover:border-green-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
              >
                <div className="text-sm font-medium text-white group-hover:text-green-300 mb-1">ETF.com</div>
                <div className="text-xs text-crypto-silver">ETF research, news and analysis</div>
              </button>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
