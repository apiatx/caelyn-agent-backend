import { useEffect, useRef } from "react";
import CryptoStocksSection from "@/components/crypto-stocks-section";

function MarketSummaryWidget() {
  const containerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!containerRef.current) return;
    const iframe = document.createElement("iframe");
    iframe.style.width = "100%";
    iframe.style.height = "100%";
    iframe.style.border = "none";
    iframe.setAttribute("sandbox", "allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox allow-top-navigation-by-user-activation");
    containerRef.current.appendChild(iframe);
    const doc = iframe.contentDocument;
    if (doc) {
      doc.open();
      doc.write(`<!DOCTYPE html><html><head><style>body{margin:0;padding:0;overflow:hidden;background:transparent;}</style></head><body><script type="module" src="https://widgets.tradingview-widget.com/w/en/tv-market-summary.js"><\/script><tv-market-summary time-frame="7D" direction="horizontal" theme="dark" transparent></tv-market-summary></body></html>`);
      doc.close();
    }
    return () => {
      if (containerRef.current && iframe.parentNode === containerRef.current) {
        containerRef.current.removeChild(iframe);
      }
    };
  }, []);
  return <div ref={containerRef} className="w-full h-[200px]" />;
}

export default function StocksScreeningPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="w-full rounded-lg overflow-hidden border border-white/[0.06] mb-4">
          <MarketSummaryWidget />
        </div>
        <CryptoStocksSection />
      </main>
    </div>
  );
}
