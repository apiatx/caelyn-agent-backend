import { useState, Suspense } from "react";
import { useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, ExternalLink, Activity } from "lucide-react";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";
import { SectionLoadingState } from "@/components/loading-screen";

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

// Enhanced glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-gradient-to-br from-black/60 via-black/40 to-transparent backdrop-blur-xl border border-white/20 shadow-2xl hover:shadow-purple-500/10 transition-all duration-300 ${className}`}>
    {children}
  </Card>
);

// Use secure link opening
const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function TradeOptionsPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-12 p-6">
          {/* Options Section */}
          <GlassCard className="p-8">
            {/* Options Subsection */}
            <div className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://www.hegic.co/app#/arbitrum/trade/new')}
                  className="group bg-gradient-to-br from-green-600/30 via-emerald-600/20 to-teal-600/30 border-green-400/40 hover:from-green-500/40 hover:via-emerald-500/30 hover:to-teal-500/40 hover:border-green-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-green-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Hegic
                    </div>
                    <div className="text-sm text-green-200/90">Decentralized options protocol</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://deri.io/#/lite/trade/option/BTCUSD-50000-P')}
                  className="group bg-gradient-to-br from-teal-600/30 via-cyan-600/20 to-blue-600/30 border-teal-400/40 hover:from-teal-500/40 hover:via-cyan-500/30 hover:to-blue-500/40 hover:border-teal-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-teal-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Deri Protocol
                    </div>
                    <div className="text-sm text-teal-200/90">Bitcoin options trading</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://www.binance.com/en/futures/BTC_USDT')}
                  className="group bg-gradient-to-br from-lime-600/30 via-green-600/20 to-emerald-600/30 border-lime-400/40 hover:from-lime-500/40 hover:via-green-500/30 hover:to-emerald-500/40 hover:border-lime-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-lime-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Tradoor
                    </div>
                    <div className="text-sm text-lime-200/90">Crypto perps and options</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://www.stryke.xyz/en/dashboard')}
                  className="group bg-gradient-to-br from-orange-600/30 via-amber-600/20 to-yellow-600/30 border-orange-400/40 hover:from-orange-500/40 hover:via-amber-500/30 hover:to-yellow-500/40 hover:border-orange-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-orange-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Stryke
                    </div>
                    <div className="text-sm text-orange-200/90">Decentralized options trading</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://app.gammaswap.com/trade/mainnet/0xccaab6d7bee6d60bceeec0924f2ea188efa3d39f')}
                  className="group bg-gradient-to-br from-purple-600/30 via-violet-600/20 to-indigo-600/30 border-purple-400/40 hover:from-purple-500/40 hover:via-violet-500/30 hover:to-indigo-500/40 hover:border-purple-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-purple-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      GammaSwap
                    </div>
                    <div className="text-sm text-purple-200/90">Gamma trading platform</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://speedmarkets.xyz/speed-markets')}
                  className="group bg-gradient-to-br from-blue-600/30 via-indigo-600/20 to-purple-600/30 border-blue-400/40 hover:from-blue-500/40 hover:via-indigo-500/30 hover:to-purple-500/40 hover:border-blue-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-blue-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <Activity className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Speed Markets
                    </div>
                    <div className="text-sm text-blue-200/90">Fast-paced options trading</div>
                  </div>
                </Button>

                <Button
                  variant="outline"
                  onClick={() => openInNewTab('https://app.derive.xyz/trade/options?symbol=BTC')}
                  className="group bg-gradient-to-br from-orange-600/30 via-red-600/20 to-pink-600/30 border-orange-400/40 hover:from-orange-500/40 hover:via-red-500/30 hover:to-pink-500/40 hover:border-orange-300/60 text-white justify-center p-5 h-auto shadow-xl hover:shadow-orange-500/20 transform hover:scale-105 transition-all duration-300 backdrop-blur-sm"
                  data-testid="button-derive"
                >
                  <div className="text-center">
                    <div className="font-bold text-base flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 mr-2 group-hover:rotate-12 group-hover:scale-110 transition-transform duration-300" />
                      Derive
                    </div>
                    <div className="text-sm text-orange-200/90">BTC options trading platform</div>
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