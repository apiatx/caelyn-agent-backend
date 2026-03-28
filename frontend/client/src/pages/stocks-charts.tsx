import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, ExternalLink } from "lucide-react";
import stonksIcon from "@assets/download (2)_1757104529784.jpeg";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function StocksChartsPage() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-4 lg:space-y-8">
          <div className="text-center relative mb-8">
            <div className="absolute inset-0 bg-gradient-to-r from-blue-500/20 via-green-500/20 to-purple-500/20 blur-3xl -z-10"></div>
            <div className="flex justify-center items-center gap-4 mb-6">
              <div className="w-20 h-20 sm:w-24 sm:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300">
                <img src={stonksIcon} alt="Charts" className="w-full h-full object-cover" />
              </div>
            </div>
            <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold mb-3 bg-gradient-to-r from-blue-400 via-green-300 to-purple-400 bg-clip-text text-transparent">Charts</h2>
            <Badge className="bg-gradient-to-r from-blue-500/30 to-green-500/30 text-white border-blue-400/50 text-sm px-4 py-1 mb-4">MARKET CHARTS</Badge>
            <div className="w-32 h-1 bg-gradient-to-r from-blue-500 to-green-500 mx-auto rounded-full mb-4"></div>
            <p className="text-lg text-white/80">Real-time market charts and technical analysis</p>
          </div>

          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-blue-500 to-green-500 rounded-full flex items-center justify-center">
                      <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">TradingView</h3>
                    <Badge className="bg-gradient-to-r from-blue-500/20 to-green-500/20 text-white border-white/[0.06] text-xs">SPX</Badge>
                  </div>
                  <div className="flex items-center gap-4">
                    <button onClick={() => openInNewTab('https://www.tradingview.com/symbols/SPX/?exchange=SP&timeframe=ALL')} className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm">Open Symbol →</button>
                    <button onClick={() => openInNewTab('https://www.tradingview.com/chart/e5l95XgZ/?symbol=SP%3ASPX')} className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm">Open Chart →</button>
                  </div>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CAPITALCOM%3AUS500"
                    className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                    title="TradingView (CAPITALCOM:US500) Chart"
                    frameBorder="0"
                    scrolling="no"
                    allow="fullscreen"
                  />
                </div>
              </div>

              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2 mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                      <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">TrendSpider Markets</h3>
                    <Badge className="bg-gradient-to-r from-purple-500/20 to-pink-500/20 text-white border-white/[0.06] text-xs">MARKET ANALYSIS</Badge>
                  </div>
                  <button onClick={() => openInNewTab('https://trendspider.com/markets/')} className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm">Open Full View →</button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://trendspider.com/markets/"
                    className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                    title="TrendSpider Markets"
                    sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                  />
                </div>
              </div>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
