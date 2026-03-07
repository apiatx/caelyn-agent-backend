import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { TrendingUp, ExternalLink, ArrowLeftRight } from "lucide-react";
import { openSecureLink } from "@/utils/security";

// Enhanced glass card component for Spot Terminals section
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-gradient-to-br from-black/60 via-black/40 to-transparent backdrop-blur-xl border border-white/20 shadow-2xl hover:shadow-purple-500/10 transition-all duration-300 ${className}`}>
    {children}
  </Card>
);

// Use secure link opening
const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function TradeSpotTerminalsPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-12 p-6">
          {/* Multi-Chain Trading Terminals Section */}
          <GlassCard className="p-8">
            {/* Trading Terminals Subsection */}
            <div className="space-y-6">
              <div className="flex items-center justify-between mb-6">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-purple-400 to-fuchsia-500 rounded-lg flex items-center justify-center">
                    <TrendingUp className="w-4 h-4 text-white" />
                  </div>
                  <h4 className="text-2xl font-bold bg-gradient-to-r from-purple-400 to-fuchsia-300 bg-clip-text text-transparent">Trading Terminals</h4>
                </div>
                <button
                  onClick={() => openInNewTab('https://app.tabtrader.com/trading?list=Spot&market=BINANCE&pair=BTCUSDT')}
                  className="text-blue-400 hover:text-blue-300 text-sm flex items-center gap-2 bg-black/20 border border-blue-500/30 px-4 py-2 rounded-lg hover:bg-blue-500/20 transition-all duration-300"
                  data-testid="button-tabtrader-external"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Full View
                </button>
              </div>

              {/* TabTrader Terminal */}
              <iframe
                src="https://app.tabtrader.com/trading?list=Spot&market=BINANCE&pair=BTCUSDT"
                className="w-full h-[600px] rounded-lg border border-white/[0.06] mb-6"
                title="TabTrader Terminal"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                referrerPolicy="no-referrer-when-downgrade"
                data-testid="iframe-tabtrader"
              />

              {/* Primary App - AltFins (Full Width) */}
              <div className="w-full">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://altfins.com/')}
                  className="group w-full bg-gradient-to-br from-cyan-500/40 via-teal-500/30 to-emerald-500/40 border-cyan-400/50 hover:from-cyan-400/50 hover:via-teal-400/40 hover:to-emerald-400/50 hover:border-cyan-300/70 text-white justify-center p-8 h-auto shadow-2xl hover:shadow-cyan-500/40 transform hover:scale-[1.02] transition-all duration-500 backdrop-blur-sm"
                  data-testid="button-altfins"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <TrendingUp className="w-7 h-7 group-hover:scale-110 transition-transform duration-500" />
                      AltFins
                    </div>
                    <div className="text-base text-cyan-100/90 font-medium">Advanced crypto analytics and trading tools</div>
                  </div>
                </Button>
              </div>

              {/* Other Apps */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://app.definitive.fi/0x9FDBdA0A5e284c32744D2f17Ee5c74B284993463/hyperevm')}
                  className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                      Definitive Edge
                    </div>
                    <div className="text-sm text-white/45">Trade any token, on any chain</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://universalx.app/home')}
                  className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                      UniversalX
                    </div>
                    <div className="text-sm text-white/45">Trade any token, on any chain</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://o1.exchange/')}
                  className="group bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                      O1 Exchange
                    </div>
                    <div className="text-sm text-white/45">Advanced trading on Base and Solana</div>
                  </div>
                </Button>

              </div>

              {/* Ave.ai - Simple Black Button */}
              <div className="w-full">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://ave.ai/')}
                  className="group w-full bg-black/20 border-white/[0.06] hover:bg-gray-500/20 hover:border-gray-500/30 text-white justify-center p-5 h-auto shadow-lg hover:shadow-gray-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                  data-testid="button-ave-ai"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <ArrowLeftRight className="w-5 h-5 mr-2 group-hover:rotate-180 transition-transform duration-300" />
                      Ave.ai
                    </div>
                    <div className="text-sm text-white/45">AI-powered multi-chain trading terminal</div>
                  </div>
                </Button>
              </div>

              {/* OurBit Button */}
              <div className="w-full">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://www.ourbit.com/')}
                  className="group w-full bg-gradient-to-br from-purple-500/40 via-fuchsia-500/30 to-pink-500/40 border-purple-400/50 hover:from-purple-400/50 hover:via-fuchsia-400/40 hover:to-pink-400/50 hover:border-purple-300/70 text-white justify-center p-8 h-auto shadow-2xl hover:shadow-purple-500/40 transform hover:scale-[1.02] transition-all duration-500 backdrop-blur-sm"
                  data-testid="button-ourbit"
                >
                  <div className="text-center">
                    <div className="font-bold text-2xl flex items-center justify-center gap-3 mb-2">
                      <ArrowLeftRight className="w-7 h-7 group-hover:rotate-180 transition-transform duration-500" />
                      OurBit
                    </div>
                    <div className="text-base text-purple-100/90 font-medium">Primary multi-chain trading terminal - CEX & DEX, Spot & Futures</div>
                  </div>
                </Button>
              </div>

            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
