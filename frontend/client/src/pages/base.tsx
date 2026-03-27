import BaseSection from "@/components/base-section";
import baseLogo from "@assets/base logo_1755977414942.webp";

export default function BasePage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <BaseSection />
      </main>
    </div>
  );
}