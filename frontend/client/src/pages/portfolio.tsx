import PortfolioSection from "@/components/portfolio-section";

export default function PortfolioPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <PortfolioSection />
      </main>
    </div>
  );
}
