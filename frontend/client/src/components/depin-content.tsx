import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Zap } from "lucide-react";
import { openSecureLink } from "@/utils/security";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function DePINContent() {
  return (
    <div className="space-y-8">
      {/* DePIN Section */}
      <GlassCard className="p-6">

        {/* Pinlink Marketplace Button */}
        <div className="mb-8">
          <button
            onClick={() => openInNewTab('https://pinlink.ai/marketplace')}
            className="w-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 hover:from-indigo-500/30 hover:to-purple-500/30 border-2 border-indigo-500/30 hover:border-indigo-400/50 rounded-xl p-6 transition-all duration-300 text-left group shadow-lg hover:shadow-indigo-500/20"
          >
            <div className="flex items-center gap-3 mb-2">
              <div className="w-8 h-8 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full flex items-center justify-center">
                <Zap className="w-4 h-4 text-white" />
              </div>
              <div className="text-xl font-bold text-white group-hover:text-indigo-300">Pinlink Marketplace</div>
              <Badge className="bg-indigo-500/20 text-indigo-400 border-indigo-500/30 text-xs">
                DEPIN MARKETPLACE
              </Badge>
            </div>
            <div className="text-sm text-crypto-silver group-hover:text-white/45">
              Decentralized Physical Infrastructure Networks marketplace • Discover and invest in DePIN projects
            </div>
          </button>
        </div>
      </GlassCard>
    </div>
  );
}
