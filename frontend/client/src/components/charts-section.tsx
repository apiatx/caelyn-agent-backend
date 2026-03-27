import { GlassCard } from "@/components/ui/glass-card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, ExternalLink, Brain } from "lucide-react";
import { Button } from "@/components/ui/button";
import { openDexScreenerLink } from "@/utils/mobile-links";

export default function ChartsSection() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="space-y-8">
      <div className="text-center">
        <h2 className="text-2xl lg:text-3xl font-bold text-white mb-2">Charts & Analytics</h2>
        <p className="text-crypto-silver">Multi-chain market analysis and trading data</p>
      </div>

      {/* DexScreener Multicharts */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-3">
            <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
              <TrendingUp className="w-4 h-4 text-white" />
            </div>
            <h2 className="text-xl font-semibold text-white">DexScreener Multicharts</h2>
            <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
              MULTICHARTS
            </Badge>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => openDexScreenerLink('multicharts')}
            className="text-crypto-silver hover:text-white"
          >
            <ExternalLink className="h-4 w-4 mr-2" />
            Open Full View
          </Button>
        </div>
        
        <div className="w-full">
          <iframe
            src="https://dexscreener.com/multicharts?theme=dark"
            className="w-full h-[600px] lg:h-[800px] rounded-lg border border-white/[0.06]"
            title="DexScreener Multicharts"
            sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
          />
        </div>
      </GlassCard>
    </div>
  );
}