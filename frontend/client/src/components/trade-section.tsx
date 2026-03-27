import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ArrowLeftRight, TrendingUp, ExternalLink, Wallet, ArrowUpDown } from "lucide-react";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";


// Safe components for external links and iframes
const SafeLink = ({ href, children, className = "", ...props }: { 
  href: string; 
  children: React.ReactNode; 
  className?: string; 
  [key: string]: any; 
}) => (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    className={className}
    onClick={(e) => {
      e.preventDefault();
      openSecureLink(href);
    }}
    {...props}
  >
    {children}
  </a>
);

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

// Enhanced glass card component for Trade section
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-gradient-to-br from-black/60 via-black/40 to-transparent backdrop-blur-xl border border-white/20 shadow-2xl hover:shadow-purple-500/10 transition-all duration-300 ${className}`}>
    {children}
  </Card>
);

// Use secure link opening
const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function TradeSection() {
  return (
    <div className="space-y-12 p-6">
      {/* Swidge Section */}
      <GlassCard className="p-8">
        {/* Swidge Subsection */}
        <div className="space-y-6">
          <div className="text-center mb-6">
            <div className="flex items-center justify-center gap-3 mb-2">
              <div className="w-8 h-8 bg-gradient-to-r from-blue-400 to-indigo-500 rounded-lg flex items-center justify-center">
                <ArrowLeftRight className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-indigo-300 bg-clip-text text-transparent">Swidge</h4>
            </div>
          </div>
          
          {/* Primary App - Relay Bridge (Full Width) */}
          <div className="w-full">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://www.relay.link/bridge')}
              className="group w-full bg-gradient-to-br from-blue-500/40 via-indigo-500/30 to-purple-500/40 border-blue-400/50 hover:from-blue-400/50 hover:via-indigo-400/40 hover:to-purple-400/50 hover:border-blue-300/70 text-white justify-center p-8 h-auto shadow-2xl hover:shadow-blue-500/40 transform hover:scale-[1.02] transition-all duration-500 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                  <ArrowLeftRight className="w-7 h-7 group-hover:rotate-180 transition-transform duration-500" />
                  Relay Bridge
                </div>
                <div className="text-base text-blue-100/90 font-medium">Primary cross-chain asset bridging platform</div>
              </div>
            </Button>
          </div>

          {/* Rubic Exchange and Ethereum Chart - Side by Side */}
          <div className="mt-8">
            <div className="flex flex-col lg:flex-row gap-4">
              {/* Rubic Exchange - 1/3 width */}
              <div className="w-full lg:w-1/3">
                <iframe
                  src="https://app.rubic.exchange/?fromChain=ETH&toChain=ETH"
                  className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                  title="Rubic Exchange"
                  frameBorder="0"
                  loading="lazy"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                  referrerPolicy="no-referrer-when-downgrade"
                  data-testid="iframe-rubic"
                />
              </div>

              {/* Ethereum TradingView Chart - 2/3 width */}
              <div className="w-full lg:w-2/3">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3AETHUSDT"
                  className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                  title="TradingView Ethereum Chart"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                />
              </div>
            </div>
          </div>

          {/* Matcha Button - Same style as Definitive Edge but green */}
          <div className="w-full mt-6">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://matcha.xyz/tokens/ethereum/0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')}
              className="group w-full bg-gradient-to-br from-green-500/40 via-emerald-500/30 to-teal-500/40 border-green-400/50 hover:from-green-400/50 hover:via-emerald-400/40 hover:to-teal-400/50 hover:border-green-300/70 text-white justify-center p-8 h-auto shadow-2xl hover:shadow-green-500/40 transform hover:scale-[1.02] transition-all duration-500 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                  <ArrowLeftRight className="w-7 h-7 group-hover:rotate-180 transition-transform duration-500" />
                  Matcha
                </div>
                <div className="text-base text-green-100/90 font-medium">DEX aggregator</div>
              </div>
            </Button>
          </div>

          {/* Other Apps */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://oku.trade/?isExactOut=false&inputChain=plasma&inToken=0x6100E367285b01F48D07953803A2d8dCA5D19873&outToken=0xB8CE59FC3717ada4C02eaDF9682A9e934F625ebb')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  Oku Trade
                </div>
                <div className="text-sm text-white/45">Plasma chain DEX</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://app.swing.xyz/swap?fromChain=base&fromToken=ETH&toChain=ethereum&toToken=SWING')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  Galaxy Swing
                </div>
                <div className="text-sm text-white/45">Cross-chain swap protocol</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://app.debridge.finance/?inputChain=1&outputChain=8453&inputCurrency=&outputCurrency=&dlnMode=simple')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  deBridge
                </div>
                <div className="text-sm text-white/45">Cross-chain liquidity and infrastructure</div>
              </div>
            </Button>
          </div>

          {/* DODO, XY Finance, and Stargate Finance */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://app.dodoex.io/swap/network/mainnet/1-ETH/56-%24BeAI')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  DODO
                </div>
                <div className="text-sm text-white/45">Decentralized exchange protocol</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://app.xy.finance/')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  XY Finance
                </div>
                <div className="text-sm text-white/45">Cross-chain swap aggregator</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://stargate.finance/bridge')}
              className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-base flex items-center justify-center">
                  <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  Stargate Finance
                </div>
                <div className="text-sm text-white/45">Omnichain bridge protocol</div>
              </div>
            </Button>
          </div>

          {/* Jumper Exchange Button - Big purple button like Matcha */}
          <div className="w-full mt-6">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://jumper.exchange/')}
              className="group w-full bg-gradient-to-br from-purple-500/40 via-violet-500/30 to-purple-500/40 border-purple-400/50 hover:from-purple-400/50 hover:via-violet-400/40 hover:to-purple-400/50 hover:border-purple-300/70 text-white justify-center p-8 h-auto shadow-2xl hover:shadow-purple-500/40 transform hover:scale-[1.02] transition-all duration-500 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                  <ArrowLeftRight className="w-7 h-7 group-hover:rotate-180 transition-transform duration-500" />
                  Jumper Exchange
                </div>
                <div className="text-base text-purple-100/90 font-medium">Multi-chain bridge aggregator</div>
              </div>
            </Button>
          </div>

{/* Mayan Finance iframe */}
          <div className="mt-8">
            <div className="flex justify-end mb-3">
              <button
                onClick={() => openInNewTab('https://swap.mayan.finance/')}
                className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
                data-testid="button-mayan-fullview"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </button>
            </div>
            <div className="w-full">
              <iframe
                src="https://swap.mayan.finance/"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="Mayan Finance Cross-chain Swap"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                referrerPolicy="no-referrer-when-downgrade"
                data-testid="iframe-mayan"
              />
            </div>
          </div>

        </div>
      </GlassCard>

      {/* Simple Swap Section */}
      <GlassCard className="p-8">
        {/* Simple Swap Subsection */}
        <div className="space-y-6">
          <div className="text-center mb-6">
            <div className="flex items-center justify-center gap-3 mb-2">
              <div className="w-8 h-8 bg-gradient-to-r from-pink-400 to-rose-500 rounded-lg flex items-center justify-center">
                <ArrowUpDown className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-2xl font-bold bg-gradient-to-r from-pink-400 to-rose-300 bg-clip-text text-transparent">Simple Swap</h4>
            </div>
          </div>
          
          {/* DefiLlama Swap iframe */}
          <div className="mb-8">
            <div className="flex justify-end mb-3">
              <button
                onClick={() => openInNewTab('https://swap.defillama.com/?chain=bsc&from=0x0000000000000000000000000000000000000000&tab=swap&to=')}
                className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
                data-testid="button-defillama-swap-fullview"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </button>
            </div>
            <div className="w-full">
              <iframe
                src="https://swap.defillama.com/?chain=bsc&from=0x0000000000000000000000000000000000000000&tab=swap&to="
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="DefiLlama Swap - BSC Cross-chain Swap"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                referrerPolicy="no-referrer-when-downgrade"
                data-testid="iframe-defillama-swap"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://app.uniswap.org')}
              className="group bg-gradient-to-br from-pink-500/30 via-pink-600/20 to-rose-600/30 border-pink-400/40 hover:from-pink-400/40 hover:via-pink-500/30 hover:to-rose-500/40 hover:border-pink-300/60 text-white justify-center p-6 h-auto shadow-2xl hover:shadow-pink-500/30 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-lg flex items-center justify-center">
                  <ArrowUpDown className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  Uniswap
                </div>
                <div className="text-sm text-pink-100/90">Leading DEX on Ethereum</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://pancakeswap.finance/swap')}
              className="group bg-gradient-to-br from-yellow-500/30 via-amber-500/20 to-orange-500/30 border-yellow-400/40 hover:from-yellow-400/40 hover:via-amber-400/30 hover:to-orange-400/40 hover:border-yellow-300/60 text-white justify-center p-6 h-auto shadow-2xl hover:shadow-yellow-500/30 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-lg flex items-center justify-center">
                  <ArrowUpDown className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  PancakeSwap
                </div>
                <div className="text-sm text-yellow-100/90">Popular BSC DEX</div>
              </div>
            </Button>

            <Button
              variant="outline"
              onClick={() => openInNewTab('https://jup.ag')}
              className="group bg-gradient-to-br from-teal-500/30 via-cyan-500/20 to-blue-500/30 border-teal-400/40 hover:from-teal-400/40 hover:via-cyan-400/30 hover:to-blue-400/40 hover:border-teal-300/60 text-white justify-center p-6 h-auto shadow-2xl hover:shadow-teal-500/30 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
            >
              <div className="text-center">
                <div className="font-bold text-lg flex items-center justify-center">
                  <ArrowUpDown className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                  Jupiter
                </div>
                <div className="text-sm text-teal-100/90">Solana swap aggregator</div>
              </div>
            </Button>
          </div>

        </div>
      </GlassCard>

      {/* Private Swap Section */}
      <GlassCard className="p-8">
        <div className="space-y-6">
          <div className="text-center mb-6">
            <div className="flex items-center justify-center gap-3 mb-2">
              <div className="w-8 h-8 bg-gradient-to-r from-purple-400 to-violet-500 rounded-lg flex items-center justify-center">
                <ArrowLeftRight className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-2xl font-bold bg-gradient-to-r from-purple-400 to-violet-300 bg-clip-text text-transparent">Private Swap</h4>
            </div>
          </div>
          
          {/* Zkaster iframe */}
          <div>
            <div className="flex justify-end mb-3">
              <button
                onClick={() => openInNewTab('https://app.zkaster.com/')}
                className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
                data-testid="button-zkaster-fullview"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </button>
            </div>
            <div className="w-full">
              <iframe
                src="https://app.zkaster.com/"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="Zkaster Private Swap"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
                data-testid="iframe-zkaster"
              />
            </div>
          </div>

        </div>
      </GlassCard>

    </div>
  );
}