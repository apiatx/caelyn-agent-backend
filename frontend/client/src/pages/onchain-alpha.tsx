import { useEffect, useRef } from "react";
import AlphaSection from "@/components/alpha-section";

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

export default function OnchainAlphaPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="sticky top-0 z-50 border-b border-white/[0.06] bg-black/90 backdrop-blur-lg">
        <CryptoTickerTape />
      </div>

      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 pt-2 pb-8">
        <AlphaSection />
      </main>
    </div>
  );
}