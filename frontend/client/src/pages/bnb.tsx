import { Suspense } from "react";
import { Loader2, ExternalLink } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { openSecureLink } from "@/utils/security";
import bnbLogo from "@assets/eb2349c3-b2f8-4a93-a286-8f86a62ea9d8_1757138768380.png";

// Glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

// Safe link component
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

export default function BNBPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Content */}
      <div className="max-w-[95vw] mx-auto px-2 sm:px-3 mt-4 pb-8">
        {/* BNB Chart */}
        <div className="mb-6">
          <Suspense fallback={<div className="flex items-center justify-center py-8"><Loader2 className="w-6 h-6 animate-spin text-yellow-500" /><span className="ml-2 text-white">Loading BNB Chart...</span></div>}>
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-yellow-400">BNB</h3>
                  <Badge variant="outline" className="bg-yellow-500/20 text-yellow-300 border-yellow-500/30">
                    BNB
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/bnb/')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ABNBUSDT')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ABNBUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="BNB Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
            </div>
          </Suspense>
        </div>

        {/* BNB Ecosystem Tools */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-yellow-400 mb-3">BNB Chain Explorer</h3>
            <p className="text-crypto-silver text-sm mb-3">Explore transactions and addresses on BNB Smart Chain</p>
            <button
              onClick={() => openSecureLink('https://bscscan.com/')}
              className="w-full bg-yellow-600/20 hover:bg-yellow-600/30 text-yellow-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              BscScan
            </button>
          </GlassCard>

          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-yellow-400 mb-3">PancakeSwap</h3>
            <p className="text-crypto-silver text-sm mb-3">Leading DEX on BNB Smart Chain</p>
            <button
              onClick={() => openSecureLink('https://pancakeswap.finance/')}
              className="w-full bg-yellow-600/20 hover:bg-yellow-600/30 text-yellow-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              PancakeSwap
            </button>
          </GlassCard>

          <GlassCard className="p-4">
            <h3 className="text-lg font-semibold text-yellow-400 mb-3">DexScreener BNB</h3>
            <p className="text-crypto-silver text-sm mb-3">Real-time BNB Chain token analytics</p>
            <button
              onClick={() => openSecureLink('https://dexscreener.com/bsc')}
              className="w-full bg-yellow-600/20 hover:bg-yellow-600/30 text-yellow-300 py-2 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
            >
              <ExternalLink className="w-4 h-4" />
              DexScreener
            </button>
          </GlassCard>
        </div>

        {/* Thena Finance Enhanced Button */}
        <div className="mb-8">
          <button
            onClick={() => openSecureLink('https://thena.fi/')}
            className="w-full p-8 text-center bg-gradient-to-b from-yellow-600/10 to-orange-700/10 hover:from-yellow-600/20 hover:to-orange-700/20 border border-yellow-600/20 hover:border-yellow-500/40 rounded-lg transition-all duration-300 group"
            data-testid="button-thena"
          >
            <div className="w-16 h-16 bg-gradient-to-br from-yellow-600 to-orange-700 rounded-full flex items-center justify-center mx-auto mb-4">
              <ExternalLink className="w-8 h-8 text-white" />
            </div>
            <h4 className="text-2xl font-bold text-white group-hover:text-yellow-300 mb-3">Thena Finance</h4>
            <p className="text-white/35 group-hover:text-white/45 text-base max-w-lg mx-auto">Swap, bridge, perps, earn</p>
          </button>
        </div>

        {/* Alpha Section - Copied from Screening page */}
        <div className="space-y-8 mt-12">
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center justify-center mb-6">
              <h4 className="text-2xl font-bold bg-gradient-to-r from-white to-yellow-100 bg-clip-text text-transparent">Alpha</h4>
            </div>
            
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <SafeLink
                href='https://www.binance.com/en/markets/alpha-all'
                className="p-5 bg-gradient-to-br from-yellow-500/15 to-yellow-600/15 hover:from-yellow-500/25 hover:to-yellow-600/25 border border-yellow-500/30 hover:border-yellow-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-yellow-500/20 transform"
              >
                <div className="flex flex-col items-center gap-2 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-br from-yellow-500 to-yellow-600 rounded-lg flex items-center justify-center shadow-md">
                    <span className="text-white font-bold text-xs">B</span>
                  </div>
                  <h4 className="text-yellow-300 font-bold text-lg">Binance Alpha</h4>
                </div>
                <p className="text-white/45 text-sm font-medium text-center">Alpha project listings and market data</p>
              </SafeLink>

              <SafeLink
                href='https://web3.binance.com/en/markets/alpha?chain=bsc'
                className="p-5 bg-gradient-to-br from-orange-500/15 to-orange-600/15 hover:from-orange-500/25 hover:to-orange-600/25 border border-orange-500/30 hover:border-orange-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-orange-500/20 transform"
              >
                <div className="flex flex-col items-center gap-2 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-br from-orange-500 to-orange-600 rounded-lg flex items-center justify-center shadow-md">
                    <span className="text-white font-bold text-xs">W3</span>
                  </div>
                  <h4 className="text-orange-300 font-bold text-lg">Binance Web3 Alpha</h4>
                </div>
                <p className="text-white/45 text-sm font-medium text-center">Web3 alpha projects on BSC</p>
              </SafeLink>
            </div>
            
          </GlassCard>
        </div>
      </div>
    </div>
  );
}