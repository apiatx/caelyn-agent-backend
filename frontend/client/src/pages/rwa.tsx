import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ExternalLink, TrendingUp } from "lucide-react";
import { getSecureLinkProps, openSecureLink } from "@/utils/security";
import tokenizedStocksLogo from "@assets/image_1765946345697.png";

// Safe iframe component
const SafeIframe = ({ src, title, className = "", ...props }: { 
  src: string; 
  title: string; 
  className?: string; 
  [key: string]: any; 
}) => (
  <iframe
    src={src}
    title={title}
    className={className}
    sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
    {...props}
  />
);

// Glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function RWAPage() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-6">
          {/* Tokenized Stocks Section */}
          <GlassCard className="p-6">
            <div className="text-center relative mb-8">
              <div className="absolute inset-0 bg-gradient-to-r from-purple-500/20 via-blue-500/20 to-indigo-500/20 blur-3xl -z-10"></div>
              <div className="flex justify-center items-center gap-4 mb-6">
                <div className="w-20 h-20 sm:w-24 sm:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300">
                  <img src={tokenizedStocksLogo} alt="Tokenized Stocks" className="w-full h-full object-cover" />
                </div>
                <div className="text-left">
                  <h3 className="text-2xl sm:text-3xl lg:text-4xl font-bold bg-gradient-to-r from-white via-purple-200 to-blue-200 bg-clip-text text-transparent">Tokenized Stocks</h3>
                  <Badge className="bg-gradient-to-r from-purple-500/30 to-blue-500/30 text-white border-purple-400/50 text-sm mt-2 px-3 py-1">
                    BLOCKCHAIN STOCKS
                  </Badge>
                </div>
              </div>
              <p className="text-lg text-white/80 font-medium tracking-wide">Real-world assets & tokenized equity trading</p>
              <div className="w-32 h-1 bg-gradient-to-r from-purple-500 to-blue-500 mx-auto mt-4 rounded-full"></div>
            </div>

            <div className="flex justify-between items-center mb-4">
              <a
                {...getSecureLinkProps('https://app.rwa.xyz/')}
                className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1 ml-auto"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <SafeIframe
                src="https://app.rwa.xyz/"
                title="RWA.xyz Platform"
                className="w-full h-[800px] border-0"
              />
            </div>
            
            <div className="mt-4 text-center">
              <p className="text-xs text-white/35">
                RWA.xyz platform • Comprehensive real world asset trading
              </p>
            </div>
          </GlassCard>

          {/* The World Onchain ASSETS */}
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-2">
                <div className="w-6 h-6 bg-gradient-to-r from-yellow-500 to-orange-500 rounded-full flex items-center justify-center">
                  <TrendingUp className="w-4 h-4 text-white" />
                </div>
                <h3 className="text-xl font-semibold text-white">The World Onchain</h3>
                <Badge className="bg-yellow-500/20 text-yellow-400 border-yellow-500/30 text-xs">
                  ASSETS
                </Badge>
              </div>
              <a
                href="https://jup.ag/pro?tab=stocks"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>

            {/* Jupiter Pro - Stocks Iframe */}
            <div className="mb-6">
              <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                <SafeIframe
                  src="https://jup.ag/pro?tab=stocks"
                  title="Jupiter Pro Stocks Trading"
                  className="w-full h-[600px] border-0"
                />
              </div>
              <div className="mt-2 text-center">
                <p className="text-xs text-white/35">
                  Tokenized stock trading on Solana via Jupiter Pro
                </p>
              </div>
            </div>

            {/* Allo Finance, Ondo Finance, and Ostium Buttons */}
            <div className="grid grid-cols-3 gap-4 mb-6">
              {/* Allo Finance - Orange Button */}
              <button
                onClick={() => openInNewTab('https://app.allo.xyz/trade/RWA')}
                className="bg-gradient-to-br from-orange-500 to-orange-600 hover:from-orange-600 hover:to-orange-700 border-2 border-orange-400 hover:border-orange-300 rounded-xl p-6 transition-all duration-300 text-left group shadow-lg hover:shadow-orange-500/20"
              >
                <div className="text-xl font-bold text-white group-hover:text-orange-50 mb-2">Allo Finance</div>
                <div className="text-sm text-white/90 group-hover:text-white">Real World Asset (RWA) tokenization and trading platform</div>
              </button>

              {/* Ondo Finance - White Button */}
              <button
                onClick={() => openInNewTab('https://app.ondo.finance/')}
                className="bg-white hover:bg-gray-100 border-2 border-gray-300 hover:border-gray-400 rounded-xl p-6 transition-all duration-300 text-left group shadow-lg hover:shadow-gray-300/20"
              >
                <div className="text-xl font-bold text-black group-hover:text-gray-800 mb-2">Ondo Finance</div>
                <div className="text-sm text-black/80 group-hover:text-black">Institutional yield & tokenized treasuries</div>
              </button>

              {/* Ostium - Orange Button */}
              <button
                onClick={() => openInNewTab('https://app.ostium.com/trade?from=SPX&to=USD')}
                className="bg-gradient-to-br from-orange-600 to-amber-600 hover:from-orange-700 hover:to-amber-700 border-2 border-orange-400 hover:border-orange-300 rounded-xl p-6 transition-all duration-300 text-left group shadow-lg hover:shadow-orange-500/20"
              >
                <div className="text-xl font-bold text-white group-hover:text-orange-50 mb-2">Ostium</div>
                <div className="text-sm text-white/90 group-hover:text-white">Real-world asset perpetuals trading</div>
              </button>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-yellow-500 to-orange-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">AU</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">Oro Gold</h3>
                    <p className="text-white/35 text-xs">Trade gold</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/orogoldapp"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://orogold.app/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-yellow-500/20 border border-yellow-500/30 rounded text-yellow-400 hover:bg-yellow-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-indigo-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">📈</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">xStocks</h3>
                    <p className="text-white/35 text-xs">Trade stocks</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/xStocksFi"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://xstocks.com/us"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">🏢</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">PreStocks</h3>
                    <p className="text-white/35 text-xs">Trade pre-IPO stocks</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/PreStocksFi"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://prestocks.com/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-purple-500/20 border border-purple-500/30 rounded text-purple-400 hover:bg-purple-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-orange-500 to-yellow-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">₿</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">Apollo BTC</h3>
                    <p className="text-white/35 text-xs">Trade native Bitcoin</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/ApolloBTCportal"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://apolloportal.io/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-orange-500/20 border border-orange-500/30 rounded text-orange-400 hover:bg-orange-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-green-500 to-emerald-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">🏠</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">Parcl</h3>
                    <p className="text-white/35 text-xs">Trade real estate</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/Parcl"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://www.parcl.co/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-green-500/20 border border-green-500/30 rounded text-green-400 hover:bg-green-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-pink-500 to-purple-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">✨</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">Magic Eden</h3>
                    <p className="text-white/35 text-xs">Trade digital slop</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/MagicEden"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://magiceden.io/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-pink-500/20 border border-pink-500/30 rounded text-pink-400 hover:bg-pink-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">🏦</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">RECC Finance</h3>
                    <p className="text-white/35 text-xs">Trade RWA</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/RECCFinance"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://recc.finance/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-cyan-500/20 border border-cyan-500/30 rounded text-cyan-400 hover:bg-cyan-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>

              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">📊</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">Etherfuse</h3>
                    <p className="text-white/35 text-xs">Trade bonds</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/etherfuse"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://www.etherfuse.com/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-indigo-500/20 border border-indigo-500/30 rounded text-indigo-400 hover:bg-indigo-500/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>


              <div className="p-4 bg-black/20 border border-white/[0.06] rounded-lg">
                <div className="flex items-center gap-3 mb-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-green-600 to-lime-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">🌾</span>
                  </div>
                  <div>
                    <h3 className="text-white font-medium text-sm">AgriDex</h3>
                    <p className="text-white/35 text-xs">Trade agriculture</p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <a
                    href="https://x.com/AgriDexPlatform"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-blue-500/20 border border-blue-500/30 rounded text-blue-400 hover:bg-blue-500/30 text-xs transition-colors"
                  >
                    <span className="font-bold">𝕏</span>
                    <span>Twitter</span>
                  </a>
                  <a
                    href="https://agridex.com/"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex-1 flex items-center justify-center gap-1 px-3 py-2 bg-green-600/20 border border-green-600/30 rounded text-green-400 hover:bg-green-600/30 text-xs transition-colors"
                  >
                    <ExternalLink className="w-3 h-3" />
                    <span>Website</span>
                  </a>
                </div>
              </div>
            </div>
          </GlassCard>

          {/* Plume Network RWA Platform */}
          <GlassCard className="p-6">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-6 h-6 bg-purple-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-xl font-semibold text-white">Plume Network</h4>
              <span className="text-xs px-2 py-0.5 rounded-full bg-purple-500/20 text-purple-400 border border-purple-500/30 font-medium">RWA PLATFORM</span>
              <button
                onClick={() => openInNewTab('https://portal.plume.org/')}
                className="text-purple-400 hover:text-purple-300 text-sm ml-auto"
              >
                Open Full View →
              </button>
            </div>
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <SafeIframe
                src="https://portal.plume.org/"
                title="Plume Network Portal"
                className="w-full h-[600px] border-0"
              />
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}