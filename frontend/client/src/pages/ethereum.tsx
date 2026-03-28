import { Suspense } from "react";
import { EthereumSection } from "@/components/ethereum-section";
import ethereumLogo from "@assets/image_1771292182048.png";
import { SectionLoadingState } from "@/components/loading-screen";

export default function EthereumPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Ethereum Section */}
          <Suspense fallback={<SectionLoadingState title="Ethereum Network" />}>
            <EthereumSection />
          </Suspense>
        </div>
      </main>
    </div>
  );
}