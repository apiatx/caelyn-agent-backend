import { ExternalLink } from "lucide-react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";

export default function MarketResearchSection() {
  return (
    <div className="space-y-8">
      {/* Cointelegraph Latest News */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-3">
            <h2 className="text-xl font-semibold text-white">Crypto News</h2>
            <span className="px-2 py-1 bg-gradient-to-r from-blue-500 to-purple-600 text-white text-xs rounded-lg font-medium">
              COINTELEGRAPH
            </span>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => window.open('https://cointelegraph.com/category/latest-news', '_blank')}
            className="text-crypto-silver hover:text-white"
          >
            <ExternalLink className="h-4 w-4 mr-2" />
            Open Full View
          </Button>
        </div>
        
        <div className="space-y-4">
          <h3 className="text-xl font-semibold text-white mb-4">News Sources</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Button
              variant="outline"
              onClick={() => window.open('https://cointelegraph.com/category/latest-news', '_blank')}
              className="bg-black/20 border-white/[0.06] hover:bg-blue-500/20 hover:border-blue-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">Cointelegraph</div>
                <div className="text-sm text-crypto-silver">Latest crypto news & analysis</div>
              </div>
            </Button>
          </div>
        </div>
      </GlassCard>
    </div>
  );
}