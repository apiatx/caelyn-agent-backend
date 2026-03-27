import CryptoStocksSection from "@/components/crypto-stocks-section";
import TickerTapeWidget from "@/components/TickerTapeWidget";

export default function CryptoStocksPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="w-full h-[72px] overflow-hidden">
        <TickerTapeWidget />
      </div>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <CryptoStocksSection />
      </main>
    </div>
  );
}