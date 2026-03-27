import { useEffect, useRef } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Brain, ExternalLink } from "lucide-react";
import { openSecureLink } from '@/utils/security';

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-sm bg-gradient-to-br from-black/90 to-black/95 border border-white/20 ${className}`}>
    {children}
  </Card>
);

function CryptoTickerTape() {
  const containerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!containerRef.current) return;
    const iframe = document.createElement("iframe");
    iframe.style.width = "100%";
    iframe.style.height = "100%";
    iframe.style.border = "none";
    iframe.setAttribute("sandbox", "allow-scripts allow-same-origin allow-popups");
    containerRef.current.appendChild(iframe);
    const doc = iframe.contentDocument;
    if (doc) {
      doc.open();
      doc.write(`<!DOCTYPE html><html><head><style>body{margin:0;padding:0;overflow:hidden;background:transparent;}</style></head><body><script type="module" src="https://widgets.tradingview-widget.com/w/en/tv-ticker-tape.js"><\/script><tv-ticker-tape symbols='BITSTAMP:BTCUSD,BITSTAMP:ETHUSD,CRYPTOCAP:XRP,CRYPTOCAP:BNB,CRYPTOCAP:SOL,CRYPTO:TRXUSD,CRYPTOCAP:DOGE,CRYPTO:HYPEHUSD,CRYPTOCAP:LINK,CRYPTOCAP:XMR,CRYPTOCAP:XLM,CRYPTOCAP:ZEC,CRYPTOCAP:HBAR,CRYPTOCAP:LTC,CRYPTOCAP:SUI,COINBASE:TAOUSD' theme="dark" transparent></tv-ticker-tape></body></html>`);
      doc.close();
    }
    return () => {
      if (containerRef.current && iframe.parentNode === containerRef.current) {
        containerRef.current.removeChild(iframe);
      }
    };
  }, []);
  return <div ref={containerRef} className="w-full" style={{ height: 78 }} />;
}

export default function OnchainAnalyzePage() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="sticky top-0 z-50 border-b border-white/[0.06] bg-black/90 backdrop-blur-lg">
        <CryptoTickerTape />
      </div>

      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Main Analysis Tools */}
          <GlassCard className="p-6">
            
            {/* Artemis Analytics */}
            <div className="mb-8 relative">
              <div className="relative">
                <div className="w-full">
                  <iframe
                    src="https://app.artemisanalytics.com/"
                    title="Artemis Analytics Dashboard"
                    className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                    frameBorder="0"
                    loading="lazy"
                    sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation"
                    referrerPolicy="strict-origin-when-cross-origin"
                    style={{
                      background: '#000000',
                      colorScheme: 'dark'
                    }}
                  />
                </div>
              </div>
            </div>

            {/* Token Terminal - Full width row */}
            <div className="mb-6">
              <button
                onClick={() => openInNewTab('https://tokenterminal.com/explorer')}
                className="w-full bg-gradient-to-br from-purple-500/15 to-purple-600/15 hover:from-purple-500/25 hover:to-purple-600/25 border border-purple-500/30 hover:border-purple-400/50 rounded-xl p-5 transition-all duration-300 text-center group shadow-lg hover:shadow-purple-500/20 hover:scale-105 transform"
                data-testid="button-token-terminal"
              >
                <div className="text-base font-semibold text-white group-hover:text-purple-200 mb-2 flex items-center justify-center gap-2">
                  <div className="w-2 h-2 bg-purple-400 rounded-full group-hover:animate-pulse"></div>
                  Token Terminal
                </div>
                <div className="text-sm text-white/45 group-hover:text-gray-200">Protocol metrics explorer</div>
              </button>
            </div>

            {/* Developer Report, The Block, and Chainspect - 3 across */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              <button
                onClick={() => openInNewTab('https://www.developerreport.com/')}
                className="bg-gradient-to-br from-green-500/15 to-green-600/15 hover:from-green-500/25 hover:to-green-600/25 border border-green-500/30 hover:border-green-400/50 rounded-xl p-5 transition-all duration-300 text-center group shadow-lg hover:shadow-green-500/20 hover:scale-105 transform"
                data-testid="button-developer-report"
              >
                <div className="text-base font-semibold text-white group-hover:text-green-200 mb-2 flex flex-col items-center gap-2">
                  <div className="w-2 h-2 bg-green-400 rounded-full group-hover:animate-pulse"></div>
                  Developer Report
                </div>
                <div className="text-sm text-white/45 group-hover:text-gray-200 text-center">Developer activity by blockchain</div>
              </button>

              <button
                onClick={() => openInNewTab('https://www.theblock.co/data/decentralized-finance/dex-non-custodial')}
                className="bg-gradient-to-br from-orange-500/15 to-orange-600/15 hover:from-orange-500/25 hover:to-orange-600/25 border border-orange-500/30 hover:border-orange-400/50 rounded-xl p-5 transition-all duration-300 text-center group shadow-lg hover:shadow-orange-500/20 hover:scale-105 transform"
                data-testid="button-the-block"
              >
                <div className="text-base font-semibold text-white group-hover:text-orange-200 mb-2 flex flex-col items-center gap-2">
                  <div className="w-2 h-2 bg-orange-400 rounded-full group-hover:animate-pulse"></div>
                  The Block: DEX Metrics
                </div>
                <div className="text-sm text-white/45 group-hover:text-gray-200 text-center">Comprehensive DEX analytics and metrics</div>
              </button>

              <button
                onClick={() => openInNewTab('https://chainspect.app/dashboard')}
                className="bg-gradient-to-br from-cyan-500/15 to-cyan-600/15 hover:from-cyan-500/25 hover:to-cyan-600/25 border border-cyan-500/30 hover:border-cyan-400/50 rounded-xl p-5 transition-all duration-300 text-center group shadow-lg hover:shadow-cyan-500/20 hover:scale-105 transform"
                data-testid="button-chainspect"
              >
                <div className="text-base font-semibold text-white group-hover:text-cyan-200 mb-2 flex flex-col items-center gap-2">
                  <div className="w-2 h-2 bg-cyan-400 rounded-full group-hover:animate-pulse"></div>
                  Chainspect
                </div>
                <div className="text-sm text-white/45 group-hover:text-gray-200 text-center">Chain scalability and decentralization analytics</div>
              </button>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}