import DeFiSection from "@/components/defi-section";

export default function DeFiPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <DeFiSection />
      </main>
    </div>
  );
}