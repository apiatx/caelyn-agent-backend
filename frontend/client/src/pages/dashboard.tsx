import { useState, Suspense, useEffect, useRef } from "react";
import CryptoDashboardSection from "@/components/crypto-dashboard-section";
import PortfolioSection from "@/components/portfolio-section";
import AlphaSection from "@/components/alpha-section";
import BittensorDashboardSection from "@/components/bittensor-dashboard-section";
import BaseSection from "@/components/base-section";
import SolanaSection from "@/components/solana-section";
import DeFiSection from "@/components/defi-section";
import { SectionLoadingState } from "@/components/loading-screen";

type TabType = "dashboard" | "alpha" | "base" | "bittensor" | "solana" | "defi" | "portfolio";

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

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<TabType>("dashboard");

  // Handle URL fragments on page load and hash changes
  useEffect(() => {
    const handleHashChange = () => {
      const hash = window.location.hash.slice(1); // Remove the #
      if (hash && ["alpha", "base", "bittensor", "solana", "defi", "portfolio"].includes(hash)) {
        setActiveTab(hash as TabType);
        window.scrollTo({ top: 0, behavior: 'smooth' });
      } else {
        setActiveTab("dashboard");
      }
    };

    // Check initial hash
    handleHashChange();

    // Listen for hash changes
    window.addEventListener('hashchange', handleHashChange);
    
    return () => {
      window.removeEventListener('hashchange', handleHashChange);
    };
  }, []);

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="sticky top-0 z-50 border-b border-white/[0.06] bg-black/90 backdrop-blur-lg">
        <CryptoTickerTape />
      </div>

      {/* Content */}
      <div className="max-w-[95vw] mx-auto px-2 sm:px-3 mt-4 pb-8">
        <Suspense fallback={<SectionLoadingState title="Market Overview" />}>
          {activeTab === "dashboard" && <CryptoDashboardSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="Portfolio" />}>
          {activeTab === "portfolio" && <PortfolioSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="Alpha Analytics" />}>
          {activeTab === "alpha" && <AlphaSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="Base Network" />}>
          {activeTab === "base" && <BaseSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="Solana Network" />}>
          {activeTab === "solana" && <SolanaSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="Bittensor" />}>
          {activeTab === "bittensor" && <BittensorDashboardSection />}
        </Suspense>
        <Suspense fallback={<SectionLoadingState title="DeFi Platforms" />}>
          {activeTab === "defi" && <DeFiSection />}
        </Suspense>
      </div>
    </div>
  );
}