import { Suspense } from "react";
import { Loader2, ExternalLink } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { openSecureLink } from "@/utils/security";
import suiLogo from "@assets/images (2)_1757139042170.jpeg";

// Glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function SUIPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Content */}
      <div className="max-w-[95vw] mx-auto px-2 sm:px-3 mt-4 pb-8">
        {/* SUI Chart */}
        <div className="mb-6">
          <Suspense fallback={<div className="flex items-center justify-center py-8"><Loader2 className="w-6 h-6 animate-spin text-blue-500" /><span className="ml-2 text-white">Loading SUI Chart...</span></div>}>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-blue-400">SUI</h3>
                  <Badge variant="outline" className="bg-blue-500/20 text-blue-300 border-blue-500/30">
                    SUI
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/sui/')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ASUIUSDT')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ASUIUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="SUI Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
            </div>
          </Suspense>
        </div>

        {/* SUI Ecosystem Tools */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-blue-400 mb-3">SUI Explorer</h3>
            <p className="text-crypto-silver text-sm mb-3">Explore transactions and addresses on Sui Network</p>
            <button
              onClick={() => openSecureLink('https://suivision.xyz/')}
              className="w-full bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              SuiVision
            </button>
          </GlassCard>

          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-blue-400 mb-3">Cetus Protocol</h3>
            <p className="text-crypto-silver text-sm mb-3">Leading DEX on Sui Network</p>
            <button
              onClick={() => openSecureLink('https://www.cetus.zone/')}
              className="w-full bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              Cetus
            </button>
          </GlassCard>

          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-blue-400 mb-3">DexScreener SUI</h3>
            <p className="text-crypto-silver text-sm mb-3">Real-time SUI token analytics</p>
            <button
              onClick={() => openSecureLink('https://dexscreener.com/sui')}
              className="w-full bg-blue-600/20 hover:bg-blue-600/30 text-blue-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              DexScreener
            </button>
          </GlassCard>
        </div>

        {/* Bluefin Trading Button */}
        <GlassCard className="p-6 mb-6">
          <button
            onClick={() => openSecureLink('https://trade.bluefin.io/swap/USDC-SUI')}
            className="w-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 hover:from-blue-500/30 hover:to-cyan-500/30 border-2 border-blue-500/30 hover:border-blue-400/50 rounded-xl p-6 transition-all duration-300 text-left group shadow-lg hover:shadow-blue-500/20"
          >
            <div className="flex items-center gap-3 mb-2">
              <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <div className="text-xl font-bold text-white group-hover:text-blue-300">Bluefin Trading</div>
              <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
                DEX
              </Badge>
            </div>
            <div className="text-sm text-crypto-silver group-hover:text-white/45">USDC-SUI trading on Sui Network • Decentralized exchange</div>
          </button>
        </GlassCard>
      </div>
    </div>
  );
}