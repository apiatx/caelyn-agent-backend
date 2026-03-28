import { Card } from "@/components/ui/card";
import { Gamepad2, ExternalLink } from "lucide-react";
import { openSecureLink } from "@/utils/security";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-xl bg-gradient-to-br from-black/80 via-gray-900/60 to-black/90 border border-white/30 shadow-2xl shadow-purple-500/10 hover:shadow-purple-500/20 transition-all duration-500 ${className}`}>
    {children}
  </Card>
);

export default function P2EPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Play-to-Earn Gaming Hub */}
          <GlassCard className="p-6">
            <div className="flex items-center gap-3 mb-6">
              <div className="w-10 h-10 bg-gradient-to-r from-green-500 to-emerald-500 rounded-xl flex items-center justify-center">
                <Gamepad2 className="w-6 h-6 text-white" />
              </div>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden mb-6">
              <iframe
                src="https://playtoearn.com/trending-blockchaingames"
                className="w-full h-[700px] border-0"
                title="PlayToEarn Trending Blockchain Games"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                data-testid="iframe-playtoearn"
              />
            </div>

            {/* Enhanced Link Buttons */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <button
                onClick={() => openSecureLink('https://chainplay.gg/')}
                className="p-4 bg-gradient-to-br from-blue-500/15 to-cyan-500/15 hover:from-blue-500/25 hover:to-cyan-500/25 border border-blue-500/30 hover:border-blue-400/50 rounded-lg hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-blue-500/20 transform"
                data-testid="button-chainplay"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-blue-500 to-cyan-500 rounded-lg flex items-center justify-center shadow-md flex-shrink-0">
                      <Gamepad2 className="w-5 h-5 text-white" />
                    </div>
                    <div className="text-left">
                      <h4 className="text-blue-300 font-bold text-base">ChainPlay Gaming Hub</h4>
                      <p className="text-blue-400/70 text-xs">Discover and track blockchain games</p>
                    </div>
                  </div>
                  <ExternalLink className="w-4 h-4 text-blue-400 flex-shrink-0" />
                </div>
              </button>

              <button
                onClick={() => openSecureLink('https://dappradar.com/blog/category/games')}
                className="p-4 bg-gradient-to-br from-purple-500/15 to-pink-500/15 hover:from-purple-500/25 hover:to-pink-500/25 border border-purple-500/30 hover:border-purple-400/50 rounded-lg hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-purple-500/20 transform"
                data-testid="button-dappradar"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg flex items-center justify-center shadow-md flex-shrink-0">
                      <Gamepad2 className="w-5 h-5 text-white" />
                    </div>
                    <div className="text-left">
                      <h4 className="text-purple-300 font-bold text-base">P2E Gaming News & Insights</h4>
                      <p className="text-purple-400/70 text-xs">DappRadar Games Blog</p>
                    </div>
                  </div>
                  <ExternalLink className="w-4 h-4 text-purple-400 flex-shrink-0" />
                </div>
              </button>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
