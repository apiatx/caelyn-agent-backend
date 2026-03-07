import ArbitrumContent from "@/components/arbitrum-content";
import arbitrumLogo from "@assets/image_1771293557192.png";

export default function ArbitrumPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <ArbitrumContent />
      </main>
    </div>
  );
}
