import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Bitcoin, TrendingUp, Brain, ExternalLink } from "lucide-react";
import chartIcon from "@assets/images_1757104413238.png";

// Glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function CryptoStonksPage() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-6">
          {/* Page Header */}
          <div className="text-center relative mb-8">
            <div className="absolute inset-0 bg-gradient-to-r from-orange-500/20 via-amber-500/20 to-yellow-500/20 blur-3xl -z-10"></div>
            <div className="flex justify-center items-center gap-4 mb-6">
              <div className="w-20 h-20 sm:w-24 sm:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300">
                <img 
                  src={chartIcon} 
                  alt="Chart Icon" 
                  className="w-full h-full object-cover"
                />
              </div>
              <div className="text-left">
                <h1 className="text-2xl sm:text-3xl lg:text-4xl font-bold bg-gradient-to-r from-white via-orange-100 to-amber-100 bg-clip-text text-transparent">Crypto Treasuries</h1>
                <Badge className="bg-gradient-to-r from-orange-500/30 to-amber-500/30 text-white border-orange-400/50 text-sm mt-2 px-3 py-1">
                  CORPORATE HOLDINGS
                </Badge>
              </div>
            </div>
            <p className="text-lg text-white/80 font-medium tracking-wide">Track corporate Bitcoin treasuries and crypto adoption by public companies</p>
            <div className="w-32 h-1 bg-gradient-to-r from-orange-500 to-amber-500 mx-auto mt-4 rounded-full"></div>
          </div>
          {/* Crypto Treasuries */}
          <GlassCard className="p-3 sm:p-4 lg:p-6">

            <div className="space-y-6">
              {/* Treasuries and ETFs - Artemis */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-emerald-500 rounded-full flex items-center justify-center">
                      <TrendingUp className="w-2.5 h-2.5 text-white" />
                    </div>
                    <h4 className="text-lg font-semibold text-emerald-400">Treasuries and ETFs</h4>
                    <Badge className="bg-emerald-500/20 text-emerald-400 border-emerald-500/30 text-xs">
                      ARTEMIS
                    </Badge>
                  </div>
                  <button
                    onClick={() => openInNewTab('https://app.artemisanalytics.com/digital-asset-treasuries')}
                    className="text-emerald-400 hover:text-emerald-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://app.artemisanalytics.com/digital-asset-treasuries"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Artemis Digital Asset Treasuries"
                    sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                  />
                </div>
                <button
                  onClick={() => openInNewTab('https://blockworks.com/analytics/treasury-companies')}
                  className="w-full bg-gradient-to-r from-indigo-600/80 to-purple-600/80 hover:from-indigo-500/90 hover:to-purple-500/90 border border-indigo-400/30 hover:border-indigo-300/50 rounded-lg p-4 transition-all duration-300 group flex items-center justify-between"
                >
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-white/10 rounded-lg flex items-center justify-center">
                      <TrendingUp className="w-5 h-5 text-indigo-300" />
                    </div>
                    <div className="text-left">
                      <div className="text-base font-bold text-white group-hover:text-indigo-100">Blockworks Treasury Analytics</div>
                      <div className="text-xs text-white/60 group-hover:text-white/80">Corporate treasury companies & ETF holdings</div>
                    </div>
                  </div>
                  <ExternalLink className="w-5 h-5 text-white/50 group-hover:text-white/80 transition-colors" />
                </button>
              </div>

              {/* Bitcoin Treasuries */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-orange-500 rounded-full flex items-center justify-center">
                      <Bitcoin className="w-2.5 h-2.5 text-white" />
                    </div>
                    <h4 className="text-lg font-semibold text-orange-400">Bitcoin Treasuries</h4>
                    <Badge className="bg-orange-500/20 text-orange-400 border-orange-500/30 text-xs">
                      BTC
                    </Badge>
                  </div>
                  <button
                    onClick={() => openInNewTab('https://bitcointreasuries.net/')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://bitcointreasuries.net/"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Bitcoin Treasuries"
                    sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                  />
                </div>
              </div>

              {/* Ethereum Reserve */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <div className="w-4 h-4 bg-blue-500 rounded-full flex items-center justify-center">
                      <TrendingUp className="w-2.5 h-2.5 text-white" />
                    </div>
                    <h4 className="text-lg font-semibold text-blue-400">Ethereum Treasuries</h4>
                    <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
                      ETH
                    </Badge>
                  </div>
                  <button
                    onClick={() => openInNewTab('https://strategicethreserve.xyz/')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://strategicethreserve.xyz/"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Ethereum Treasuries"
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