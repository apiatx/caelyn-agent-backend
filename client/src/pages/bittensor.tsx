import BittensorDashboardSection from "@/components/bittensor-dashboard-section";
import bittensorLogo from "@assets/bittensor_1755977414942.png";

export default function BittensorPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <BittensorDashboardSection />
      </main>
    </div>
  );
}