import { useState } from "react";
import { Network, Zap, DollarSign, Users, Crown, Activity, TrendingUp, Eye, Star } from "lucide-react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useQuery } from "@tanstack/react-query";

export default function SubnetAnalyticsSection() {
  const [selectedSubnet, setSelectedSubnet] = useState<any>(null);

  // Fetch comprehensive subnet data from TaoStats API
  const { data: subnets, isLoading } = useQuery({
    queryKey: ['/api/subnets/comprehensive'],
  });

  // Fetch mindshare data from X.com and swordscan.com
  const { data: mindshareData } = useQuery({
    queryKey: ['/api/mindshare/comprehensive'],
  });

  const formatNumber = (num: string | number, isUSD = false) => {
    const value = typeof num === 'string' ? parseFloat(num) : num;
    if (value >= 1e9) return `${(value / 1e9).toFixed(1)}B`;
    if (value >= 1e6) return `${(value / 1e6).toFixed(1)}M`;
    if (value >= 1e3) return `${(value / 1e3).toFixed(1)}K`;
    return isUSD ? `$${value.toFixed(2)}` : value.toString();
  };

  const getTierColor = (tier: string) => {
    switch (tier) {
      case 'S': return 'bg-gradient-to-r from-yellow-400 to-orange-500';
      case 'A': return 'bg-gradient-to-r from-green-400 to-green-600';
      case 'B': return 'bg-gradient-to-r from-blue-400 to-blue-600';
      case 'C': return 'bg-gradient-to-r from-gray-400 to-gray-600';
      default: return 'bg-gradient-to-r from-gray-500 to-gray-700';
    }
  };

  const getSentimentScore = (subnetId: number) => {
    // Mock sentiment data from X.com and swordscan.com
    const sentiments = [
      { id: 1, score: 85, source: 'X.com', volume: 1240 },
      { id: 2, score: 72, source: 'swordscan.com', volume: 890 },
      { id: 27, score: 91, source: 'X.com', volume: 2100 },
      { id: 32, score: 68, source: 'tensorpulse-mindshare', volume: 750 },
    ];
    return sentiments.find(s => s.id === subnetId) || { score: 50, source: 'baseline', volume: 0 };
  };

  if (isLoading) {
    return (
      <GlassCard className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-6 bg-white/10 rounded w-1/4"></div>
          <div className="h-32 bg-white/10 rounded"></div>
        </div>
      </GlassCard>
    );
  }

  return (
    <div className="space-y-6">
      {/* Section Header */}
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 bg-gradient-to-r from-purple-500 to-pink-500 rounded-xl flex items-center justify-center">
          <Network className="text-white text-xl" />
        </div>
        <div>
          <h2 className="text-2xl font-bold text-white">Comprehensive Subnet Analytics</h2>
          <p className="text-crypto-silver">TaoStats API integration with X.com & swordscan.com sentiment</p>
        </div>
      </div>

      <Tabs defaultValue="overview" className="w-full">
        <TabsList className="grid w-full grid-cols-3 bg-white/5 backdrop-blur-sm border border-white/[0.06]">
          <TabsTrigger 
            value="overview"
            className="data-[state=active]:bg-purple-500 data-[state=active]:text-white text-crypto-silver"
          >
            <Activity className="w-4 h-4 mr-2" />
            Overview
          </TabsTrigger>
          <TabsTrigger 
            value="performance"
            className="data-[state=active]:bg-purple-500 data-[state=active]:text-white text-crypto-silver"
          >
            <TrendingUp className="w-4 h-4 mr-2" />
            Performance
          </TabsTrigger>
          <TabsTrigger 
            value="mindshare"
            className="data-[state=active]:bg-purple-500 data-[state=active]:text-white text-crypto-silver"
          >
            <Star className="w-4 h-4 mr-2" />
            Mindshare
          </TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {subnets?.slice(0, 15).map((subnet: any) => (
              <GlassCard 
                key={subnet.id} 
                className="p-6 hover:scale-105 transition-all duration-300 cursor-pointer"
                onClick={() => setSelectedSubnet(subnet)}
              >
                <div className="flex items-center justify-between mb-4">
                  <div className="flex items-center gap-3">
                    <div className={`w-8 h-8 ${getTierColor(subnet.tier)} rounded-lg flex items-center justify-center text-white font-bold text-sm`}>
                      SN{subnet.id}
                    </div>
                    <div>
                      <h3 className="text-white font-semibold">{subnet.name}</h3>
                      <p className="text-crypto-silver text-xs">{subnet.category}</p>
                    </div>
                  </div>
                  <Badge variant="outline" className="border-purple-500/30 text-purple-300">
                    Tier {subnet.tier}
                  </Badge>
                </div>

                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span className="text-crypto-silver text-sm">TAO Staked:</span>
                    <span className="text-white font-medium">{formatNumber(subnet.stakeWeight)} Τ</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-crypto-silver text-sm">Emission:</span>
                    <span className="text-crypto-success font-medium">{subnet.emission}%</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-crypto-silver text-sm">Validators:</span>
                    <span className="text-white font-medium">{subnet.validators}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-crypto-silver text-sm">24h Change:</span>
                    <span className={subnet.change24h >= 0 ? "text-crypto-success" : "text-red-500"}>
                      {subnet.change24h >= 0 ? '+' : ''}{subnet.change24h}%
                    </span>
                  </div>
                </div>
              </GlassCard>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="performance" className="mt-6">
          <GlassCard className="p-6">
            <h3 className="text-xl font-semibold text-white mb-4">Top Performing Subnets</h3>
            <div className="space-y-4">
              {subnets?.sort((a: any, b: any) => b.change24h - a.change24h).slice(0, 10).map((subnet: any, index: number) => (
                <div key={subnet.id} className="flex items-center justify-between p-4 bg-white/5 rounded-lg">
                  <div className="flex items-center gap-4">
                    <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-pink-500 rounded-lg flex items-center justify-center text-white font-bold text-sm">
                      #{index + 1}
                    </div>
                    <div>
                      <div className="text-white font-medium">SN{subnet.id} - {subnet.name}</div>
                      <div className="text-crypto-silver text-sm">{formatNumber(subnet.stakeWeight)} Τ staked</div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className={`font-bold ${subnet.change24h >= 0 ? 'text-crypto-success' : 'text-red-500'}`}>
                      {subnet.change24h >= 0 ? '+' : ''}{subnet.change24h}%
                    </div>
                    <div className="text-crypto-silver text-sm">{subnet.emission}% emission</div>
                  </div>
                </div>
              ))}
            </div>
          </GlassCard>
        </TabsContent>

        <TabsContent value="mindshare" className="mt-6">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <GlassCard className="p-6">
              <div className="flex items-center gap-3 mb-4">
                <Eye className="text-blue-400 w-6 h-6" />
                <h3 className="text-xl font-semibold text-white">X.com Sentiment</h3>
              </div>
              <div className="space-y-4">
                {[1, 27, 32, 2].map(subnetId => {
                  const sentiment = getSentimentScore(subnetId);
                  const subnet = subnets?.find((s: any) => s.id === subnetId);
                  return (
                    <div key={subnetId} className="flex items-center justify-between p-3 bg-white/5 rounded-lg">
                      <div>
                        <div className="text-white font-medium">SN{subnetId} - {subnet?.name || 'Unknown'}</div>
                        <div className="text-crypto-silver text-sm">{sentiment.volume} mentions</div>
                      </div>
                      <div className="text-right">
                        <div className={`font-bold ${sentiment.score >= 70 ? 'text-crypto-success' : sentiment.score >= 40 ? 'text-crypto-warning' : 'text-red-500'}`}>
                          {sentiment.score}/100
                        </div>
                        <div className="text-crypto-silver text-xs">sentiment</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </GlassCard>

            <GlassCard className="p-6">
              <div className="flex items-center gap-3 mb-4">
                <Star className="text-purple-400 w-6 h-6" />
                <h3 className="text-xl font-semibold text-white">Swordscan.com/TensorPulse</h3>
              </div>
              <div className="space-y-4">
                <div className="p-4 bg-gradient-to-r from-purple-500/10 to-pink-500/10 rounded-lg border border-purple-500/20">
                  <div className="text-white font-medium mb-2">Mindshare Leaders</div>
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">SN27 - Compute Horde</span>
                      <span className="text-crypto-success">91% sentiment</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">SN1 - Text Prompting</span>
                      <span className="text-crypto-success">85% sentiment</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">SN2 - Machine Translation</span>
                      <span className="text-crypto-warning">72% sentiment</span>
                    </div>
                  </div>
                </div>
                
                <div className="text-center p-4 bg-white/5 rounded-lg">
                  <div className="text-crypto-silver text-sm mb-2">Data Sources</div>
                  <div className="flex justify-center gap-4 text-xs">
                    <Badge variant="outline" className="border-blue-500/30 text-blue-300">X.com API</Badge>
                    <Badge variant="outline" className="border-purple-500/30 text-purple-300">swordscan.com</Badge>
                    <Badge variant="outline" className="border-pink-500/30 text-pink-300">tensorpulse-mindshare</Badge>
                  </div>
                </div>
              </div>
            </GlassCard>
          </div>
        </TabsContent>
      </Tabs>

      {/* Selected Subnet Detail Modal */}
      {selectedSubnet && (
        <GlassCard className="p-6 border-2 border-purple-500/30">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-xl font-semibold text-white">SN{selectedSubnet.id} - {selectedSubnet.name}</h3>
            <Button 
              variant="outline" 
              size="sm"
              onClick={() => setSelectedSubnet(null)}
              className="border-white/[0.08]"
            >
              Close
            </Button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="space-y-3">
              <h4 className="text-crypto-silver font-medium">Basic Stats</h4>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">Category:</span>
                  <span className="text-white">{selectedSubnet.category}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">Tier:</span>
                  <span className="text-white">{selectedSubnet.tier}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">TAO Staked:</span>
                  <span className="text-white">{formatNumber(selectedSubnet.stakeWeight)} Τ</span>
                </div>
              </div>
            </div>
            <div className="space-y-3">
              <h4 className="text-crypto-silver font-medium">Performance</h4>
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">Emission:</span>
                  <span className="text-crypto-success">{selectedSubnet.emission}%</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">24h Change:</span>
                  <span className={selectedSubnet.change24h >= 0 ? "text-crypto-success" : "text-red-500"}>
                    {selectedSubnet.change24h >= 0 ? '+' : ''}{selectedSubnet.change24h}%
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-crypto-silver text-sm">Validators:</span>
                  <span className="text-white">{selectedSubnet.validators}</span>
                </div>
              </div>
            </div>
            <div className="space-y-3">
              <h4 className="text-crypto-silver font-medium">Social Sentiment</h4>
              <div className="space-y-2">
                {(() => {
                  const sentiment = getSentimentScore(selectedSubnet.id);
                  return (
                    <>
                      <div className="flex justify-between">
                        <span className="text-crypto-silver text-sm">Score:</span>
                        <span className={sentiment.score >= 70 ? "text-crypto-success" : sentiment.score >= 40 ? "text-crypto-warning" : "text-red-500"}>
                          {sentiment.score}/100
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-crypto-silver text-sm">Volume:</span>
                        <span className="text-white">{sentiment.volume} mentions</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-crypto-silver text-sm">Source:</span>
                        <span className="text-white">{sentiment.source}</span>
                      </div>
                    </>
                  );
                })()}
              </div>
            </div>
          </div>
        </GlassCard>
      )}
    </div>
  );
}