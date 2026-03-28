import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { useQuery } from "@tanstack/react-query";
import { Globe, ArrowLeftRight, TrendingUp, ExternalLink, Star } from "lucide-react";
import { openSecureLink } from "@/utils/security";

interface DashboardData {
  portfolioValue: number;
  portfolioPnL: number;
  portfolioPnLPercent: number;
  baseTopMovers: Array<{
    token: string;
    name: string;
    price: number;
    change24h: number;
    volume24h: number;
  }>;
  taoSubnetMovers: Array<{
    subnet: string;
    name: string;
    emissions: number;
    change24h: number;
    stakeWeight: number;
  }>;
  largeWalletActivity: Array<{
    type: 'BASE' | 'TAO';
    fromToken: string;
    toToken: string;
    amount: number;
    amountUsd: number;
    wallet: string;
    timestamp: string;
  }>;
  socialPulse: Array<{
    type: 'BASE' | 'TAO';
    token: string;
    name: string;
    mentions: number;
    sentiment: 'positive' | 'negative' | 'neutral';
    trending: boolean;
  }>;
}

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

// Safe iframe component that doesn't throw errors
const SafeIframe = ({ src, title, className = "" }: { src: string; title: string; className?: string }) => {
  return (
    <div className="w-full">
      <iframe
        src={src}
        title={title}
        className={`w-full h-[600px] rounded-lg border border-white/[0.06] ${className}`}
        frameBorder="0"
        loading="lazy"
        sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation"
        referrerPolicy="strict-origin-when-cross-origin"
        style={{
          background: '#000000',
          colorScheme: 'dark'
        }}
      />
    </div>
  );
};

// Safe link component
const SafeLink = ({ href, children, className = "" }: { href: string; children: React.ReactNode; className?: string }) => {
  const handleClick = (e: React.MouseEvent) => {
    e.preventDefault();
    try {
      window.open(href, '_blank');
    } catch (error) {
      console.warn('Failed to open URL:', href, error);
    }
  };

  return (
    <a
      href={href}
      onClick={handleClick}
      className={className}
      target="_blank"
      rel="noopener noreferrer"
    >
      {children}
    </a>
  );
};

export default function BaseSection() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  const { data: dashboardData, isLoading: loadingDashboard, error } = useQuery<DashboardData>({
    queryKey: ['/api/dashboard'],
    refetchInterval: 300000, // Refresh every 5 minutes
    retry: 3, // Retry failed requests
    staleTime: 60000 // Consider data stale after 1 minute
  });

  const isLoading = loadingDashboard;

  // Enhanced error handling and data validation
  if (error) {
    console.error('Dashboard data error:', error);
  }

  // Ensure we have valid data structure with fallbacks
  const safeData = dashboardData || {
    portfolioValue: 0,
    portfolioPnL: 0,
    portfolioPnLPercent: 0,
    baseTopMovers: [],
    taoSubnetMovers: [],
    largeWalletActivity: [],
    socialPulse: []
  };

  // Ensure arrays are properly initialized
  const baseTopMovers = Array.isArray(safeData.baseTopMovers) ? safeData.baseTopMovers : [];
  const taoSubnetMovers = Array.isArray(safeData.taoSubnetMovers) ? safeData.taoSubnetMovers : [];
  const largeWalletActivity = Array.isArray(safeData.largeWalletActivity) ? safeData.largeWalletActivity : [];
  const socialPulse = Array.isArray(safeData.socialPulse) ? safeData.socialPulse : [];

  if (isLoading && !dashboardData) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[...Array(6)].map((_, i) => (
            <GlassCard key={i} className="p-6 animate-pulse">
              <div className="h-24 bg-white/10 rounded"></div>
            </GlassCard>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-8">
        {/* Show full Base content even when API fails */}
        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
                <span className="text-white text-xs font-bold">D</span>
              </div>
              <h3 className="text-xl font-semibold text-white">Trending</h3>
              <span className="px-2 py-1 bg-blue-500/20 text-blue-400 text-xs rounded-full font-medium">
                Live Data
              </span>
            </div>
            <SafeLink 
              href="https://dexscreener.com/base?theme=dark"
              className="flex items-center gap-1 text-blue-400 hover:text-blue-300 text-sm transition-colors"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </SafeLink>
          </div>
          <div className="p-6 bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/20 rounded-lg">
            <div className="flex items-center justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                  <TrendingUp className="w-4 h-4 text-white" />
                </div>
                <h3 className="text-xl font-semibold text-white">DexScreener Base</h3>
              </div>
              <button
                onClick={() => openInNewTab("https://dexscreener.com/base?theme=dark")}
                className="bg-gradient-to-r from-blue-500 to-cyan-500 hover:from-blue-600 hover:to-cyan-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
              >
                <ExternalLink className="w-4 h-4" />
                Open DexScreener
              </button>
            </div>
            <p className="text-crypto-silver mb-4">
              Live trending Base network tokens and pair analytics with real-time charts.
            </p>
            <div className="bg-black/20 rounded-lg p-4 border border-blue-500/20">
              <p className="text-sm text-crypto-silver">
                🚀 Trending tokens on Base network
                <br />
                📊 Real-time price and volume data
                <br />
                💰 DEX trading pairs and liquidity pools
              </p>
            </div>
          </div>
          
          {/* Birdeye Base */}
          <div className="border-t border-white/[0.06] pt-4 mt-4">
            <div className="p-6 bg-gradient-to-r from-orange-500/10 to-yellow-500/10 border border-orange-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-orange-500 to-yellow-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-sm">🐦</span>
                  </div>
                  <h3 className="text-xl font-semibold text-white">Birdeye</h3>
                </div>
                <button
                  onClick={() => openInNewTab("https://birdeye.so/base/find-gems")}
                  className="bg-gradient-to-r from-orange-500 to-yellow-500 hover:from-orange-600 hover:to-yellow-600 text-white font-semibold px-4 py-2 rounded-lg transition-all duration-200 flex items-center gap-2"
                >
                  <ExternalLink className="w-4 h-4" />
                  Open Birdeye
                </button>
              </div>
              <p className="text-crypto-silver mb-4">
                Professional Base network analytics with advanced token research and market insights.
              </p>
              <div className="bg-black/20 rounded-lg p-4 border border-orange-500/20">
                <p className="text-sm text-crypto-silver">
                  🚀 Base network token analytics
                  <br />
                  📊 Professional charts and trading data
                  <br />
                  💎 Market research and portfolio tools
                </p>
              </div>
            </div>
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-gradient-to-r from-green-500 to-emerald-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">FT</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Farterminal</h3>
            <span className="px-2 py-1 bg-green-500/20 text-green-400 text-xs rounded-full font-medium">
              TERMINAL
            </span>
            <SafeLink 
              href="https://www.terminal.co/?tab=base"
              className="ml-auto text-green-400 hover:text-green-300 text-xs"
            >
              Open in New Tab →
            </SafeLink>
          </div>
          <SafeIframe 
            src="https://www.terminal.co/?tab=base"
            title="Farterminal"
          />
        </GlassCard>

        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-gradient-to-r from-orange-500 to-red-500 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">BC</span>
            </div>
            <h3 className="text-xl font-semibold text-white">BlockCreeper Explorer</h3>
            <span className="px-2 py-1 bg-orange-500/20 text-orange-400 text-xs rounded-full font-medium">
              BLOCKCHAIN EXPLORER
            </span>
            <SafeLink 
              href="https://www.blockcreeper.com/"
              className="ml-auto text-orange-400 hover:text-orange-300 text-xs"
            >
              Open in New Tab →
            </SafeLink>
          </div>
          <SafeIframe 
            src="https://www.blockcreeper.com/"
            title="BlockCreeper Explorer"
          />
        </GlassCard>

        {/* Indexy Base Analytics */}
        <GlassCard className="p-8">
          <div className="text-center mb-8">
            <div className="flex items-center justify-center gap-3 mb-4">
              <div className="w-10 h-10 bg-gradient-to-r from-green-400 to-emerald-500 rounded-xl flex items-center justify-center shadow-lg">
                <TrendingUp className="w-6 h-6 text-white" />
              </div>
              <h4 className="text-3xl font-bold bg-gradient-to-r from-green-400 to-emerald-300 bg-clip-text text-transparent">Indexy</h4>
              <Badge className="bg-green-500/30 text-green-200 border-green-400/40 px-3 py-1 font-semibold">
                BASE ANALYTICS
              </Badge>
            </div>
            <p className="text-green-200/80 text-lg font-medium">Professional-grade crypto market indexing platform for Base ecosystem</p>
          </div>
          
          <SafeLink
            href='https://indexy.xyz/home'
            className="group w-full bg-gradient-to-br from-green-500/40 via-emerald-500/30 to-teal-500/40 border border-green-400/50 hover:from-green-400/50 hover:via-emerald-400/40 hover:to-teal-400/50 hover:border-green-300/70 text-white justify-center p-8 h-auto rounded-xl transition-all duration-500 flex items-center shadow-2xl hover:shadow-green-500/40 transform hover:scale-105 backdrop-blur-sm block"
          >
            <div className="text-center">
              <div className="flex items-center justify-center mb-4">
                <div>
                  <h4 className="text-3xl font-bold text-green-100 group-hover:text-white transition-colors duration-300 mb-2">Indexy</h4>
                  <Badge className="bg-green-400/30 text-green-100 border-green-300/40 px-4 py-2 font-bold text-sm">
                    PREMIUM PLATFORM
                  </Badge>
                </div>
              </div>
              <p className="text-green-100/90 text-lg font-medium mb-4 max-w-2xl mx-auto leading-relaxed">
                Professional-grade crypto market indexing platform with advanced social intelligence, 
                real-time sentiment analysis, and comprehensive Base ecosystem tracking
              </p>
              <div className="flex items-center justify-center gap-6 text-sm text-green-200/80">
                <div className="flex items-center gap-2">
                  <Star className="w-4 h-4" />
                  <span>Social Analytics</span>
                </div>
                <div className="flex items-center gap-2">
                  <TrendingUp className="w-4 h-4" />
                  <span>Market Indexes</span>
                </div>
                <div className="flex items-center gap-2">
                  <ExternalLink className="w-4 h-4" />
                  <span>Base Ecosystem</span>
                </div>
              </div>
            </div>
          </SafeLink>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-gradient-to-r from-blue-500 to-cyan-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">CS</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Checkr.social</h3>
            <span className="px-2 py-1 bg-blue-500/20 text-blue-400 text-xs rounded-full font-medium">
              SOCIAL ANALYTICS
            </span>
            <SafeLink 
              href="https://checkr.social/"
              className="ml-auto text-blue-400 hover:text-blue-300 text-xs"
            >
              Open in New Tab →
            </SafeLink>
          </div>
          <SafeIframe 
            src="https://checkr.social/"
            title="Checkr.social"
          />
        </GlassCard>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* DexScreener Base Network iframe */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">D</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Trending</h3>
            <span className="px-2 py-1 bg-blue-500/20 text-blue-400 text-xs rounded-full font-medium">
              Live Data
            </span>
          </div>
          <SafeLink 
            href="https://dexscreener.com/base?theme=dark"
            className="flex items-center gap-1 text-blue-400 hover:text-blue-300 text-sm transition-colors"
          >
            Open Full View <ExternalLink className="w-3 h-3" />
          </SafeLink>
        </div>
        <div className="space-y-4">
          {/* Alphanomics iframe */}
          <div className="w-full bg-black/20 border border-white/[0.06] rounded-lg p-4 shadow-lg mb-4">
            <SafeIframe 
              src="https://platform.alphanomics.io/"
              title="Alphanomics Analytics Platform"
              className="w-full h-[600px] rounded-lg"
            />
          </div>
          
          {/* Farterminal iframe moved to top */}
          <div className="w-full bg-black/20 border border-white/[0.06] rounded-lg p-4 shadow-lg mb-4">
            <SafeIframe 
              src="https://www.terminal.co/?tab=base"
              title="Farterminal"
              className="w-full h-[600px] rounded-lg"
            />
          </div>
          
          {/* Higher.bot iframe */}
          <div className="w-full bg-black/20 border border-white/[0.06] rounded-lg p-4 shadow-lg mb-4">
            <SafeIframe
              src="https://app.higher.bot/en/trenches?ref=Raretype"
              title="Higher.bot Trading Terminal"
              className="w-full h-[600px] rounded-lg"
            />
          </div>
          
          {/* DexScreener Button */}
          <button
            onClick={() => openInNewTab('https://dexscreener.com/base?theme=dark')}
            className="w-full p-4 bg-green-500/10 border border-green-500/20 rounded-lg hover:bg-green-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-green-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-green-400 font-semibold">DexScreener</h4>
            </div>
            <p className="text-white/35 text-sm">Live trending Base tokens and pair analytics with real-time charts</p>
          </button>
          
          {/* 30 Day Trending on OpenSea */}
          <button
            onClick={() => openInNewTab('https://opensea.io/stats/tokens?sortBy=thirtyDayPriceChange&chains=base')}
            className="w-full p-4 bg-cyan-500/10 border border-cyan-500/20 rounded-lg hover:bg-cyan-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-cyan-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-cyan-400 font-semibold">OpenSea</h4>
            </div>
            <p className="text-white/35 text-sm">Trending Base tokens by 30-day price changes</p>
          </button>
            
          {/* Trending AI Agents */}
          <button
            onClick={() => openInNewTab('https://www.coingecko.com/en/categories/artificial-intelligence')}
            className="w-full p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg hover:bg-purple-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-purple-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-purple-400 font-semibold">Trending AI Agents on CoinGecko</h4>
            </div>
            <p className="text-white/35 text-sm">View trending AI agent tokens and artificial intelligence category</p>
          </button>
          
          {/* BlockCreeper button moved here */}
          <button
            onClick={() => openInNewTab('https://www.blockcreeper.com/')}
            className="w-full p-4 bg-orange-500/10 border border-orange-500/20 rounded-lg hover:bg-orange-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-orange-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-orange-400 font-semibold">BlockCreeper Explorer</h4>
            </div>
            <p className="text-white/35 text-sm">Blockchain explorer and analytics platform</p>
          </button>
          
        </div>
      </GlassCard>











      {/* Ecosystems Section */}
      <div className="space-y-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="w-10 h-10 bg-gradient-to-r from-purple-500 to-indigo-500 rounded-xl flex items-center justify-center">
            <Globe className="text-white text-xl" />
          </div>
          <div>
            <h2 className="text-2xl font-bold text-white">Ecosystems</h2>
            <p className="text-crypto-silver">AI Agent Platforms & Trading Tools</p>
          </div>
        </div>

        {/* Virtuals Ecosystem */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="w-6 h-6 bg-gradient-to-r from-purple-500 to-indigo-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">V</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Virtuals</h3>
            <span className="px-2 py-1 bg-purple-500/20 text-purple-400 text-xs rounded-full font-medium">
              AI AGENTS
            </span>
          </div>
          
          <div className="space-y-4">
            <SafeLink
              href="https://app.virtuals.io/"
              className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-purple-500/30 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-indigo-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-sm">V</span>
                </div>
                <div>
                  <h4 className="text-white font-medium text-sm">Virtuals.io</h4>
                  <p className="text-white/35 text-xs">AI agent platform</p>
                </div>
              </div>
              <div className="w-4 h-4 text-white/35 group-hover:text-purple-400 transition-colors">→</div>
            </SafeLink>

            {/* Whale Intel Button */}
            <SafeLink
              href="https://whaleintel.ai/virtuals"
              className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-cyan-500/30 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-sm">W</span>
                </div>
                <div>
                  <h4 className="text-white font-medium text-sm">Whale Intel</h4>
                  <p className="text-white/35 text-xs">Virtuals whale analytics</p>
                </div>
              </div>
              <div className="w-4 h-4 text-white/35 group-hover:text-cyan-400 transition-colors">→</div>
            </SafeLink>

            {/* Loky AI Terminal Button */}
            <SafeLink
              href="https://lokyai.com/terminal/genesis"
              className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-indigo-500/30 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-indigo-500 to-purple-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-sm">L</span>
                </div>
                <div>
                  <h4 className="text-white font-medium text-sm">Loky AI</h4>
                  <p className="text-white/35 text-xs">AI trading terminal</p>
                </div>
              </div>
              <div className="w-4 h-4 text-white/35 group-hover:text-indigo-400 transition-colors">→</div>
            </SafeLink>
          </div>
        </GlassCard>

        {/* Creator.bid Ecosystem */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="w-6 h-6 bg-gradient-to-r from-emerald-500 to-teal-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">C</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Creator.bid</h3>
            <span className="px-2 py-1 bg-emerald-500/20 text-emerald-400 text-xs rounded-full font-medium">
              AGENT MARKETPLACE
            </span>
          </div>
          
          <SafeLink
            href="https://creator.bid/agents"
            className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-emerald-500/30 transition-all duration-200 group"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-gradient-to-r from-emerald-500 to-teal-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold text-sm">C</span>
              </div>
              <div>
                <h4 className="text-white font-medium text-sm">Creator.bid</h4>
                <p className="text-white/35 text-xs">Agent marketplace</p>
              </div>
            </div>
            <div className="w-4 h-4 text-white/35 group-hover:text-emerald-400 transition-colors">→</div>
          </SafeLink>
        </GlassCard>

        {/* Clanker Ecosystem */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="w-6 h-6 bg-gradient-to-r from-blue-500 to-cyan-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">CL</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Clanker</h3>
            <span className="px-2 py-1 bg-blue-500/20 text-blue-400 text-xs rounded-full font-medium">
              TOKEN CREATION
            </span>
          </div>
          
          <div className="space-y-4">
            <SafeLink
              href="https://www.clanker.world/"
              className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-blue-500/30 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-sm">CL</span>
                </div>
                <div>
                  <h4 className="text-white font-medium text-sm">Clanker</h4>
                  <p className="text-white/35 text-xs">Token creation platform</p>
                </div>
              </div>
              <div className="w-4 h-4 text-white/35 group-hover:text-blue-400 transition-colors">→</div>
            </SafeLink>

            <div className="border-t border-white/[0.06] pt-4">
              <SafeIframe 
                src="https://bankr.bot/terminal"
                title="Bankr.bot Trading Terminal"
              />
            </div>

            {/* Checkr iframe moved here under Clanker */}
            <div className="border-t border-white/[0.06] pt-4">
              <SafeIframe 
                src="https://checkr.social/"
                title="Checkr.social"
              />
            </div>
          </div>
        </GlassCard>

        {/* Zora Ecosystem */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-6">
            <div className="w-6 h-6 bg-gradient-to-r from-purple-500 to-pink-600 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">Z</span>
            </div>
            <h3 className="text-xl font-semibold text-white">Zora</h3>
            <span className="px-2 py-1 bg-purple-500/20 text-purple-400 text-xs rounded-full font-medium">
              NFT PLATFORM
            </span>
          </div>
          
          <div className="space-y-4">
            <SafeLink
              href="https://zora.co/explore/top-today"
              className="w-full flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-purple-500/30 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-sm">Z</span>
                </div>
                <div>
                  <h4 className="text-white font-medium text-sm">Zora</h4>
                  <p className="text-white/35 text-xs">Explore top trending NFTs today</p>
                </div>
              </div>
              <div className="w-4 h-4 text-white/35 group-hover:text-purple-400 transition-colors">→</div>
            </SafeLink>

            {/* Zoracle iframe for Zora */}
            <div className="border-t border-white/[0.06] pt-4">
              <SafeIframe 
                src="https://www.zoracle.xyz/"
                title="Zoracle"
              />
            </div>
          </div>
        </GlassCard>
      </div>

      {/* Indexy Base Analytics moved here */}
      <GlassCard className="p-8">
        <SafeLink
          href='https://indexy.xyz/home'
          className="group w-full bg-gradient-to-br from-green-500/40 via-emerald-500/30 to-teal-500/40 border border-green-400/50 hover:from-green-400/50 hover:via-emerald-400/40 hover:to-teal-400/50 hover:border-green-300/70 text-white justify-center p-8 h-auto rounded-xl transition-all duration-500 flex items-center shadow-2xl hover:shadow-green-500/40 transform hover:scale-105 backdrop-blur-sm block"
        >
          <div className="text-center">
            <div className="flex items-center justify-center mb-4">
              <div>
                <h4 className="text-3xl font-bold text-green-100 group-hover:text-white transition-colors duration-300 mb-2">Indexy</h4>
                <Badge className="bg-green-400/30 text-green-100 border-green-300/40 px-4 py-2 font-bold text-sm">
                  PREMIUM PLATFORM
                </Badge>
              </div>
            </div>
            <p className="text-green-100/90 text-lg font-medium mb-4 max-w-2xl mx-auto leading-relaxed">
              Track crypto markets with smart indices
            </p>
            <div className="flex items-center justify-center gap-6 text-sm text-green-200/80">
              <div className="flex items-center gap-2">
                <Star className="w-4 h-4" />
                <span>Social Analytics</span>
              </div>
              <div className="flex items-center gap-2">
                <TrendingUp className="w-4 h-4" />
                <span>Market Indexes</span>
              </div>
              <div className="flex items-center gap-2">
                <ExternalLink className="w-4 h-4" />
                <span>Base Ecosystem</span>
              </div>
            </div>
          </div>
        </SafeLink>
      </GlassCard>

      {/* Enhanced Ecosystem Section */}
      <div className="space-y-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="w-10 h-10 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-xl flex items-center justify-center">
            <Globe className="text-white text-xl" />
          </div>
          <div>
            <h2 className="text-2xl font-bold text-white">Base Ecosystem</h2>
            <p className="text-crypto-silver">Official Base network resources and applications</p>
          </div>
        </div>

        <GlassCard className="p-6">
          <div className="space-y-4">
            <SafeLink
              href="https://www.base.org/ecosystem"
              className="w-full flex items-center justify-between p-4 bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/20 rounded-lg hover:bg-blue-500/20 hover:border-blue-400/40 transition-all duration-200 group"
            >
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-xl flex items-center justify-center">
                  <Globe className="w-5 h-5 text-white" />
                </div>
                <div>
                  <h4 className="text-white font-semibold text-lg">Base Ecosystem Directory</h4>
                  <p className="text-blue-200 text-sm">Comprehensive ecosystem guide and official dApps</p>
                </div>
              </div>
              <ExternalLink className="w-5 h-5 text-blue-400 group-hover:text-blue-300 transition-colors" />
            </SafeLink>
            
            {/* Base App moved here */}
            <div className="p-6 bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/20 rounded-lg">
              <div className="flex items-center justify-between mb-4">
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                    <span className="text-white text-sm font-bold">CB</span>
                  </div>
                  <div>
                    <h4 className="text-lg font-semibold text-white">Coinbase Wallet</h4>
                    <p className="text-sm text-crypto-silver">Official Base network wallet app</p>
                  </div>
                </div>
                <Badge className="bg-gradient-to-r from-blue-500/20 to-cyan-500/20 text-blue-400 border-blue-500/30">
                  OFFICIAL
                </Badge>
              </div>
              <p className="text-crypto-silver mb-4 text-sm">
                Access your Base network assets, interact with DeFi protocols, and manage your crypto portfolio with Coinbase's official wallet app.
              </p>
              <button
                onClick={() => openInNewTab('https://wallet.coinbase.com/home')}
                className="w-full bg-gradient-to-r from-blue-500/20 to-cyan-500/20 border border-blue-500/30 hover:bg-blue-500/30 text-white py-3 px-4 rounded-lg transition-all duration-200 flex items-center justify-center gap-2"
              >
                <ExternalLink className="w-4 h-4" />
                Open Coinbase Wallet
              </button>
            </div>
          </div>
        </GlassCard>
      </div>
    </div>
  );
}