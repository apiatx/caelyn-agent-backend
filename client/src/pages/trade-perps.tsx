import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, Wallet, ExternalLink, Activity } from "lucide-react";
import { openSecureLink, getSecureIframeProps, getSecureLinkProps } from "@/utils/security";
import hyperliquidLogo from "@assets/hyperliquid-logo_1755977414943.png";

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

// Enhanced glass card component for Perps section
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-gradient-to-br from-black/60 via-black/40 to-transparent backdrop-blur-xl border border-white/20 shadow-2xl hover:shadow-purple-500/10 transition-all duration-300 ${className}`}>
    {children}
  </Card>
);

// Use secure link opening
const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function TradePerpsPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-12 p-6">
          {/* Perps Section */}
          <GlassCard className="p-8">
            <div className="space-y-6">
              
              {/* DeFiLlama Derivatives */}
              <div className="mb-6">
                <div className="flex justify-end mb-3">
                  <a
                    {...getSecureLinkProps('https://defillama.com/protocols/derivatives')}
                    className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
                  >
                    Open Full View <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                  <SafeIframe
                    src="https://defillama.com/protocols/derivatives"
                    title="DeFiLlama Derivatives Protocols"
                    className="w-full h-[800px] border-0"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    DeFi derivatives protocols overview • Market analytics and TVL metrics
                  </p>
                </div>
              </div>

              {/* Hyperliquid Trading */}
              <div className="mb-6">
                <div className="flex justify-end mb-3">
                  <a
                    {...getSecureLinkProps('https://app.hyperliquid.xyz/trade/HYPE')}
                    className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
                  >
                    Open Full View <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                  <iframe
                    {...getSecureIframeProps('https://app.hyperliquid.xyz/trade/HYPE', 'Hyperliquid HYPE Trading')}
                    className="w-full h-[800px] border-0"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    Live HYPE trading on Hyperliquid • Real-time orderbook and charts
                  </p>
                </div>
              </div>

              {/* Vooi.io Iframe */}
              <div className="mb-6">
                <div className="flex justify-end mb-3">
                  <a
                    {...getSecureLinkProps('https://pro.vooi.io/')}
                    className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
                  >
                    Open Full View <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                  <SafeIframe
                    src="https://pro.vooi.io/"
                    title="Vooi.io Pro Trading"
                    className="w-full h-[800px] border-0"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    Professional perpetual trading on Vooi.io • Advanced trading interface
                  </p>
                </div>
              </div>

              {/* Avantisfi Iframe */}
              <div className="mb-6">
                <div className="flex justify-end mb-3">
                  <a
                    {...getSecureLinkProps('https://www.avantisfi.com/trade?asset=BTC-USD')}
                    className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
                  >
                    Open Full View <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                  <SafeIframe
                    src="https://www.avantisfi.com/trade?asset=BTC-USD"
                    title="Avantisfi Trading Terminal"
                    className="w-full h-[800px] border-0"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    BTC-USD perpetual trading on Avantisfi • Advanced derivatives platform
                  </p>
                </div>
              </div>

              {/* Variational - Full Width Blue Button */}
              <div className="w-full mt-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://omni.variational.io/')}
                  className="group w-full bg-gradient-to-br from-blue-600/40 via-cyan-500/30 to-blue-500/40 border border-blue-400/50 hover:from-blue-500/50 hover:via-cyan-400/40 hover:to-blue-400/50 hover:border-blue-300/70 text-white justify-center p-8 h-auto rounded-lg transition-all duration-500 flex items-center shadow-2xl hover:shadow-blue-500/40 transform hover:scale-[1.02] backdrop-blur-sm"
                  data-testid="button-variational-featured"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-500" />
                      Variational
                    </div>
                    <div className="text-base text-blue-100/90 font-medium">Multi-chain perpetual trading platform</div>
                  </div>
                </Button>
              </div>

              {/* BloFin - Full Width Colored Button */}
              <div className="w-full mt-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://blofin.com/en/market/derivatives')}
                  className="group w-full bg-gradient-to-br from-blue-500/40 via-purple-500/30 to-indigo-500/40 border border-blue-400/50 hover:from-blue-400/50 hover:via-purple-400/40 hover:to-indigo-400/50 hover:border-blue-300/70 text-white justify-center p-8 h-auto rounded-lg transition-all duration-500 flex items-center shadow-2xl hover:shadow-blue-500/40 transform hover:scale-[1.02] backdrop-blur-sm"
                  data-testid="button-blofin"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-500" />
                      BloFin
                    </div>
                    <div className="text-base text-blue-100/90 font-medium">Advanced perpetual futures trading platform</div>
                  </div>
                </Button>
              </div>

              {/* SuperP - Button Link */}
              <div className="w-full mt-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://noliquidation.superp.xyz/en')}
                  className="group w-full bg-gradient-to-br from-green-600/40 via-emerald-500/30 to-green-500/40 border border-green-400/50 hover:from-green-500/50 hover:via-emerald-400/40 hover:to-green-400/50 hover:border-green-300/70 text-white justify-center p-8 h-auto rounded-lg transition-all duration-500 flex items-center shadow-2xl hover:shadow-green-500/40 transform hover:scale-[1.02] backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-500" />
                      SuperP
                    </div>
                    <div className="text-base text-green-100/90 font-medium">No liquidation perpetual futures</div>
                  </div>
                </Button>
              </div>

            </div>
          </GlassCard>

          {/* Memes Section */}
          <GlassCard className="p-8">
            <div className="space-y-6">
              {/* Memes Section Header */}
              <div className="text-center mb-6">
                <div className="flex items-center justify-center gap-3 mb-3">
                  <div className="w-12 h-12 bg-gradient-to-r from-purple-500 to-pink-500 rounded-xl flex items-center justify-center">
                    <Activity className="w-6 h-6 text-white" />
                  </div>
                  <h1 className="text-3xl font-bold text-white">Memes</h1>
                </div>
                <p className="text-crypto-silver">Meme token trading and perpetual futures</p>
              </div>

              {/* Uranus.ag - Button Link */}
              <div className="w-full mb-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://uranus.ag/trade/?token=BFgdzMkTPdKKJeTipv2njtDEwhKxkgFueJQfJGt1jups')}
                  className="group w-full bg-gradient-to-br from-purple-600/40 via-violet-500/30 to-purple-500/40 border border-purple-400/50 hover:from-purple-500/50 hover:via-violet-400/40 hover:to-purple-400/50 hover:border-purple-300/70 text-white justify-center p-8 h-auto rounded-lg transition-all duration-500 flex items-center shadow-2xl hover:shadow-purple-500/40 transform hover:scale-[1.02] backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-500" />
                      Uranus.ag
                    </div>
                    <div className="text-base text-purple-100/90 font-medium">Decentralized exchange for meme tokens</div>
                  </div>
                </Button>
              </div>

              {/* Quanto - Full Width Enhanced Colored Button */}
              <div className="w-full mt-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://quanto.trade/en/markets/BTC-USD-SWAP-LIN')}
                  className="group w-full bg-gradient-to-br from-orange-500/40 via-red-500/30 to-pink-500/40 border border-orange-400/50 hover:from-orange-400/50 hover:via-red-400/40 hover:to-pink-400/50 hover:border-orange-300/70 text-white justify-center p-8 h-auto rounded-lg transition-all duration-500 flex items-center shadow-2xl hover:shadow-orange-500/40 transform hover:scale-[1.02] backdrop-blur-sm"
                  data-testid="button-quanto"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-500" />
                      Quanto
                    </div>
                    <div className="text-base text-orange-100/90 font-medium">Next-generation perpetual futures trading</div>
                  </div>
                </Button>
              </div>
            </div>
          </GlassCard>

          {/* Analyze Section */}
          <GlassCard className="p-8">
            <div className="space-y-6">
              {/* Analyze Section Header */}
              <div className="text-center mb-6">
                <div className="flex items-center justify-center gap-3 mb-3">
                  <div className="w-12 h-12 bg-black rounded-xl flex items-center justify-center">
                    <img src={hyperliquidLogo} alt="HyperLiquid" className="w-8 h-8 rounded-lg" />
                  </div>
                  <h1 className="text-3xl font-bold text-white">Analyze</h1>
                </div>
                <p className="text-crypto-silver">Advanced market analytics and trading insights</p>
              </div>

              {/* CoinGlass Main Analytics */}
              <div className="mb-6">
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

              {/* HyperDash */}
              <div className="mt-6">
                <div className="flex justify-end mb-3">
                  <a
                    href="https://hyperdash.info/analytics"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
                  >
                    Open Full View <ExternalLink className="w-3 h-3" />
                  </a>
                </div>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
                  <iframe
                    src="https://hyperdash.info/analytics"
                    className="w-full h-[600px] border-0"
                    title="Hyperdash Analytics"
                    allow="clipboard-read; clipboard-write"
                    sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    Hyperliquid ecosystem analytics • Volume, TVL, and performance metrics
                  </p>
                </div>
              </div>

              {/* CoinGlass Hyperliquid Whale Tracker */}
              <div className="mt-6">
                <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                  <iframe
                    src="https://www.coinglass.com/hyperliquid"
                    className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                    title="CoinGlass Hyperliquid"
                    frameBorder="0"
                    scrolling="yes"
                  />
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    Hyperliquid whale tracker • CoinGlass analytics and insights
                  </p>
                </div>
              </div>

              {/* CoinGlass HL Analytics */}
              <div className="mt-6">
                <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                  <iframe
                    src="https://www.coinglass.com/hl"
                    className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                    title="CoinGlass HL Analytics"
                    frameBorder="0"
                    scrolling="yes"
                  />
                </div>
              </div>

              {/* Copy Trade / Top Wallets Section */}
              <div className="mt-6">
                <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                  <div className="w-6 h-6 bg-gradient-to-r from-purple-500 to-indigo-500 rounded-full flex items-center justify-center">
                    <span className="text-white text-xs font-bold">CT</span>
                  </div>
                  Copy Trade / Top Wallets
                  <span className="text-xs bg-purple-500/20 text-purple-400 px-2 py-1 rounded-full border border-purple-500/30">
                    VAULT STRATEGY
                  </span>
                </h3>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
                  {/* Hyperbot Network - Button Link */}
                  <button
                    onClick={() => window.open('https://hyperbot.network/discover', '_blank', 'noopener,noreferrer')}
                    className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-blue-500/30 transition-all duration-200 group mb-6 w-full"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                        <span className="text-white font-bold text-sm">HB</span>
                      </div>
                      <div className="text-left">
                        <h4 className="text-white font-medium text-sm">Hyperbot Network</h4>
                        <p className="text-white/35 text-xs">Discover top traders and copy trade on Hyperliquid</p>
                      </div>
                    </div>
                    <div className="w-4 h-4 text-white/35 group-hover:text-blue-400 transition-colors">→</div>
                  </button>
                  
                  <div className="grid grid-cols-1 gap-4">
                    <button
                      onClick={() => window.open('https://app.hyperliquid.xyz/vaults/0xe11b12a81ad743ae805078b0da61e9166475a829', '_blank')}
                      className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-purple-500/30 transition-all duration-200 group"
                    >
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-indigo-500 rounded-full flex items-center justify-center">
                          <span className="text-white font-bold text-sm">V</span>
                        </div>
                        <div className="text-left">
                          <h4 className="text-white font-medium text-sm">DegenAI Vault</h4>
                          <p className="text-white/35 text-xs">Copy trading vault strategy - 0xe11b12a81ad743ae805078b0da61e9166475a829</p>
                        </div>
                      </div>
                      <div className="w-4 h-4 text-white/35 group-hover:text-purple-400 transition-colors">→</div>
                    </button>

                    <button
                      onClick={() => window.open('https://degenai.dev/', '_blank')}
                      className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-pink-500/30 transition-all duration-200 group"
                    >
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 bg-gradient-to-r from-pink-500 to-purple-500 rounded-full flex items-center justify-center">
                          <span className="text-white font-bold text-sm">AI</span>
                        </div>
                        <div className="text-left">
                          <h4 className="text-white font-medium text-sm">DegenAI Perps Bot</h4>
                          <p className="text-white/35 text-xs">AI-powered perpetual trading bot - Advanced trading automation</p>
                        </div>
                      </div>
                      <div className="w-4 h-4 text-white/35 group-hover:text-pink-400 transition-colors">→</div>
                    </button>
                  </div>
                </div>
              </div>

              {/* CoinMarketMan HyperTracker */}
              <div className="mt-6">
                <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
                  <div className="w-6 h-6 bg-gradient-to-r from-orange-500 to-red-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-xs">C</span>
                  </div>
                  HyperTracker
                  <span className="text-xs bg-orange-500/20 text-orange-400 px-2 py-1 rounded-full border border-orange-500/30">
                    TRACKER
                  </span>
                </h3>
                
                <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
                  <div className="grid grid-cols-1 gap-3">
                    <button
                      onClick={() => window.open('https://app.coinmarketman.com/hypertracker', '_blank', 'noopener,noreferrer')}
                      className="bg-gradient-to-br from-orange-500/10 to-red-600/10 hover:from-orange-500/20 hover:to-red-600/20 border border-orange-500/20 hover:border-orange-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="text-sm font-medium text-white group-hover:text-orange-300 mb-1">Perp Trader Sentiment</div>
                          <div className="text-xs text-crypto-silver">Real-time trader sentiment analysis</div>
                        </div>
                        <ExternalLink className="w-4 h-4 text-orange-400 group-hover:text-orange-300" />
                      </div>
                    </button>
                  </div>
                  
                  {/* Portfolio Subsection */}
                  <div className="mt-6">
                    <h4 className="text-md font-semibold text-orange-400 mb-3 flex items-center gap-2">
                      <div className="w-4 h-4 bg-gradient-to-r from-orange-500 to-red-500 rounded-full flex items-center justify-center">
                        <span className="text-white font-bold text-xs">P</span>
                      </div>
                      Portfolio
                    </h4>
                    
                    <div className="grid grid-cols-1 gap-3">
                      <button
                        onClick={() => window.open('https://app.coinmarketman.com/hypertracker', '_blank', 'noopener,noreferrer')}
                        className="bg-gradient-to-br from-orange-500/10 to-red-600/10 hover:from-orange-500/20 hover:to-red-600/20 border border-orange-500/20 hover:border-orange-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
                      >
                        <div className="flex items-center justify-between">
                          <div>
                            <div className="text-sm font-medium text-white group-hover:text-orange-300 mb-1">Portfolio Analytics</div>
                            <div className="text-xs text-crypto-silver">Comprehensive portfolio tracking and metrics</div>
                          </div>
                          <ExternalLink className="w-4 h-4 text-orange-400 group-hover:text-orange-300" />
                        </div>
                      </button>
                      
                      <button
                        onClick={() => window.open('https://app.coinmarketman.com/dashboard/intelligence?tab=open_positions', '_blank', 'noopener,noreferrer')}
                        className="bg-gradient-to-br from-orange-500/10 to-red-600/10 hover:from-orange-500/20 hover:to-red-600/20 border border-orange-500/20 hover:border-orange-400/40 rounded-lg p-4 transition-all duration-300 text-left group"
                      >
                        <div className="flex items-center justify-between">
                          <div>
                            <div className="text-sm font-medium text-white group-hover:text-orange-300 mb-1">Wallet Intelligence</div>
                            <div className="text-xs text-crypto-silver">Open positions and perpetual futures intelligence</div>
                          </div>
                          <ExternalLink className="w-4 h-4 text-orange-400 group-hover:text-orange-300" />
                        </div>
                      </button>
                    </div>
                  </div>
                </div>
              </div>

            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}