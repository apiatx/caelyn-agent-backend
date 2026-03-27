import { GlassCard } from "@/components/ui/glass-card";
import { ExternalLink } from "lucide-react";

export default function OnchainAirdropPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <GlassCard className="p-6">

          <div className="flex justify-end mb-3">
            <a
              href="https://www.alphadrops.net/alpha"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
              data-testid="button-alphadrops-fullview"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <iframe
              src="https://www.alphadrops.net/alpha"
              className="w-full h-[700px] border-0"
              title="AlphaDrops - Crypto Airdrops"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
              data-testid="iframe-alphadrops"
            />
          </div>
          
          <div className="mt-4 text-center">
            <p className="text-xs text-white/35">
              AlphaDrops • Discover and track cryptocurrency airdrops
            </p>
          </div>
        </GlassCard>
      </main>
    </div>
  );
}
