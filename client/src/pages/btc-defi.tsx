import BTCDeFiContent from "@/components/btc-defi-content";

export default function BTCDeFiPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <BTCDeFiContent />
      </main>
    </div>
  );
}
