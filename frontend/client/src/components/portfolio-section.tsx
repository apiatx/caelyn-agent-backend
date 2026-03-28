import { useState, useEffect } from "react";
import { Wallet, ExternalLink, TrendingUp, Edit3, Save, Plus, Activity, ChevronDown, BarChart3, Brain } from "lucide-react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { usePortfolio } from "@/hooks/use-portfolio";
import { usePortfolioValueHistory } from "@/hooks/use-portfolio-value-history";
import { Skeleton } from "@/components/ui/skeleton";
import { HoldingDetailModal } from "@/components/holding-detail-modal";
import { DataIntegrityNotice } from "@/components/data-integrity-notice";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useDeBankPortfolio } from "@/hooks/use-debank-portfolio";
import { useStakingData } from "@/hooks/use-staking-data";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import type { Holding, Subnet } from "@shared/schema";
import { getSecureIframeProps, getSecureLinkProps } from "@/utils/security";
import ethereumLogo from "@assets/Ethereum_logo_2014.svg_1755977414942.png";
import solanaLogo from "@assets/solana_1755977414939.png";
import bittensorLogo from "@assets/bittensor_1755977414942.png";
import hyperliquidLogo from "@assets/hyperliquid-logo_1755977414943.png";
import smartWalletsImage from "@assets/download (1)_1757198511747.png";


export default function PortfolioSection() {
  const { data: portfolio, isLoading } = usePortfolio(1);
  const [selectedTimeframe, setSelectedTimeframe] = useState('24h');
  const [selectedWallet, setSelectedWallet] = useState('total'); // 'total', 'base', 'tao'
  const { data: portfolioHistory, isLoading: isHistoryLoading } = usePortfolioValueHistory(1, selectedTimeframe);
  const [selectedHolding, setSelectedHolding] = useState<Holding | null>(null);
  const [isEditingWallets, setIsEditingWallets] = useState(false);
  const [baseWalletAddress, setBaseWalletAddress] = useState('');
  const [taoWalletAddress, setTaoWalletAddress] = useState('');
  const [isBaseHoldingsOpen, setIsBaseHoldingsOpen] = useState(true);
  const [isTaoHoldingsOpen, setIsTaoHoldingsOpen] = useState(false);

  // Get DeBank portfolio data using the BASE wallet address
  const { data: debankData, isLoading: isDebankLoading } = useDeBankPortfolio(
    portfolio?.baseWalletAddress || baseWalletAddress
  );

  // Debug logging to see what data we're getting
  useEffect(() => {
    console.log('🔍 Portfolio State:', {
      portfolioWallet: portfolio?.baseWalletAddress,
      localWallet: baseWalletAddress,
      finalWallet: portfolio?.baseWalletAddress || baseWalletAddress,
      timestamp: new Date().toLocaleTimeString()
    });
  }, [portfolio, baseWalletAddress]);

  useEffect(() => {
    if (debankData) {
      console.log('🔍 DeBank Data Received:', {
        success: debankData.success,
        totalValue: debankData.data?.totalValue,
        tokensCount: debankData.data?.topTokens?.length,
        timestamp: new Date().toLocaleTimeString()
      });
    }
  }, [debankData]);

  // Get staking data for the wallet
  const { data: stakingData, isLoading: isStakingLoading } = useStakingData(
    portfolio?.baseWalletAddress || baseWalletAddress
  );

  if (isLoading) {
    return (
      <div className="space-y-8">
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {[...Array(4)].map((_, i) => (
            <Skeleton key={i} className="h-32 rounded-2xl" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* PORTFOLIO Section - Enhanced Header */}
      <div className="text-center relative mb-8">
        <div className="absolute inset-0 bg-gradient-to-r from-cyan-500/20 via-emerald-500/20 to-teal-500/20 blur-3xl -z-10"></div>
        <div className="flex justify-center items-center gap-4 mb-6">
          <div className="w-20 h-20 sm:w-24 sm:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300">
            <img 
              src={smartWalletsImage} 
              alt="Portfolio" 
              className="w-full h-full object-cover"
            />
          </div>
          <div className="text-left">
            <h2 className="text-2xl sm:text-3xl lg:text-4xl font-bold bg-gradient-to-r from-white via-cyan-100 to-emerald-100 bg-clip-text text-transparent">Portfolio</h2>
            <Badge className="bg-gradient-to-r from-cyan-500/30 to-emerald-500/30 text-white border-cyan-400/50 text-sm mt-2 px-3 py-1">
              MULTI-CHAIN
            </Badge>
          </div>
        </div>
        <p className="text-lg text-white/80 font-medium tracking-wide">Cross-Chain Portfolio Management & Analytics</p>
        <div className="w-32 h-1 bg-gradient-to-r from-cyan-500 to-emerald-500 mx-auto mt-4 rounded-full"></div>
      </div>

      {/* Portfolio Sections - Individual Glass Cards */}
      <div className="space-y-8">

        {/* HyperLiquid Section */}
        <div className="backdrop-blur-xl bg-gradient-to-br from-white/5 via-teal-500/5 to-white/5 border border-teal-400/20 rounded-xl px-6 py-5 shadow-lg shadow-teal-500/10">
          <div className="space-y-5">
            {/* HyperLiquid Portfolio Iframe */}
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="w-9 h-9 rounded-lg flex items-center justify-center bg-black/30">
                    <img src={hyperliquidLogo} alt="HyperLiquid" className="w-7 h-7 rounded-lg" />
                  </div>
                  <h4 className="text-xl font-bold bg-gradient-to-r from-teal-300 to-cyan-200 bg-clip-text text-transparent">HyperLiquid</h4>
                </div>
                <button
                  onClick={() => window.open('https://app.hyperliquid.xyz/portfolio', '_blank', 'noopener,noreferrer')}
                  className="text-teal-300 hover:text-teal-200 text-xs font-medium bg-teal-500/10 px-3 py-1.5 rounded-md border border-teal-400/20 hover:bg-teal-500/20 transition-all flex items-center gap-1.5"
                >
                  Open Full <ExternalLink className="w-3 h-3" />
                </button>
              </div>
              <div className="bg-black/20 border border-teal-400/10 rounded-lg overflow-hidden">
                <iframe
                  src="https://app.hyperliquid.xyz/portfolio"
                  className="w-full h-[450px] border-0"
                  title="HyperLiquid Portfolio"
                  frameBorder="0"
                  loading="lazy"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                  style={{
                    background: 'transparent',
                    colorScheme: 'dark'
                  }}
                />
              </div>
            </div>
            
            <Button
              variant="outline"
              onClick={() => window.open('https://app.coinmarketman.com/hypertracker', '_blank', 'noopener,noreferrer')}
              className="group w-full bg-gradient-to-br from-teal-600/15 via-cyan-600/10 to-blue-600/15 border-teal-400/30 hover:from-teal-500/25 hover:via-cyan-500/20 hover:to-blue-500/25 hover:border-teal-300/50 text-white justify-start p-4 h-auto shadow-md hover:shadow-teal-500/15 hover:scale-[1.02] transition-all duration-200"
            >
              <div className="w-10 h-10 bg-gradient-to-br from-teal-400 to-cyan-500 rounded-lg flex items-center justify-center mr-3 group-hover:scale-110 transition-transform">
                <TrendingUp className="w-5 h-5 text-white" />
              </div>
              <div className="text-left flex-1">
                <div className="font-bold text-base">CoinMarketMan</div>
                <div className="text-xs text-teal-200/80">HyperLiquid Perpetuals Analytics</div>
              </div>
              <ExternalLink className="h-4 w-4 text-teal-300" />
            </Button>
          </div>
        </div>

        {/* Multi-Chain & EVM Side by Side */}
        <div className="grid grid-cols-2 gap-4">
          {/* Multi-Chain Section */}
          <div className="backdrop-blur-xl bg-gradient-to-br from-white/5 via-cyan-500/5 to-white/5 border border-cyan-400/20 rounded-xl px-6 py-5 shadow-lg shadow-cyan-500/10">
            <div className="space-y-5">
              <div className="flex items-center gap-3">
                <div className="w-9 h-9 bg-gradient-to-r from-cyan-400 to-blue-500 rounded-lg flex items-center justify-center shadow-md">
                  <Activity className="w-5 h-5 text-white" />
                </div>
                <h4 className="text-xl font-bold bg-gradient-to-r from-cyan-300 to-blue-200 bg-clip-text text-transparent">Multi-Chain</h4>
              </div>
              
              <Button
                variant="outline"
                onClick={() => window.open('https://cerebro.xyz/dashboard', '_blank', 'noopener,noreferrer')}
                className="group w-full bg-gradient-to-br from-cyan-600/15 via-blue-600/10 to-indigo-600/15 border-cyan-400/30 hover:from-cyan-500/25 hover:via-blue-500/20 hover:to-indigo-500/25 hover:border-cyan-300/50 text-white justify-start p-4 h-auto shadow-md hover:shadow-cyan-500/15 hover:scale-[1.02] transition-all duration-200"
              >
                <div className="w-10 h-10 bg-gradient-to-br from-cyan-400 to-blue-500 rounded-lg flex items-center justify-center mr-3 group-hover:scale-110 transition-transform">
                  <Brain className="w-5 h-5 text-white" />
                </div>
                <div className="text-left flex-1">
                  <div className="font-bold text-base">Cerebro</div>
                  <div className="text-xs text-cyan-200/80">Multi-Chain Portfolio Analytics</div>
                </div>
                <ExternalLink className="h-4 w-4 text-cyan-300" />
              </Button>
            </div>
          </div>

          {/* EVM Section */}
          <div className="backdrop-blur-xl bg-gradient-to-br from-white/5 via-blue-500/5 to-white/5 border border-blue-400/20 rounded-xl px-6 py-5 shadow-lg shadow-blue-500/10">
            <div className="space-y-4">
              <div className="flex items-center gap-3">
                <div className="w-9 h-9 rounded-lg flex items-center justify-center bg-black/30">
                  <img src={ethereumLogo} alt="Ethereum" className="w-7 h-7 rounded-lg" />
                </div>
                <h4 className="text-xl font-bold bg-gradient-to-r from-blue-300 to-purple-200 bg-clip-text text-transparent">EVM</h4>
              </div>
              <Button
                variant="outline"
                onClick={() => window.open('https://debank.com/profile', '_blank', 'noopener,noreferrer')}
                className="group w-full bg-gradient-to-br from-blue-600/15 via-purple-600/10 to-blue-600/15 border-blue-400/30 hover:from-blue-500/25 hover:via-purple-500/20 hover:to-blue-500/25 hover:border-blue-300/50 text-white justify-start p-4 h-auto shadow-md hover:shadow-blue-500/15 hover:scale-[1.02] transition-all duration-200"
              >
                <div className="w-10 h-10 bg-gradient-to-br from-blue-400 to-purple-500 rounded-lg flex items-center justify-center mr-3 group-hover:scale-110 transition-transform">
                  <BarChart3 className="w-5 h-5 text-white" />
                </div>
                <div className="text-left flex-1">
                  <div className="font-bold text-base">DeBank</div>
                  <div className="text-xs text-blue-200/80">EVM Portfolio Tracking & Analytics</div>
                </div>
                <ExternalLink className="h-4 w-4 text-blue-300" />
              </Button>
            </div>
          </div>
        </div>

        {/* Solana Section */}
        <div className="backdrop-blur-xl bg-gradient-to-br from-white/5 via-purple-500/5 to-white/5 border border-purple-400/20 rounded-xl px-6 py-5 shadow-lg shadow-purple-500/10">
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-9 h-9 rounded-lg flex items-center justify-center bg-black/30">
                  <img src={solanaLogo} alt="Solana" className="w-7 h-7 rounded-lg" />
                </div>
                <h4 className="text-xl font-bold bg-gradient-to-r from-purple-300 to-violet-200 bg-clip-text text-transparent">Solana</h4>
              </div>
              <button
                onClick={() => window.open('https://jup.ag/portfolio', '_blank', 'noopener,noreferrer')}
                className="text-purple-300 hover:text-purple-200 text-xs font-medium bg-purple-500/10 px-3 py-1.5 rounded-md border border-purple-400/20 hover:bg-purple-500/20 transition-all flex items-center gap-1.5"
              >
                Open Full <ExternalLink className="w-3 h-3" />
              </button>
            </div>
            <div className="bg-black/20 border border-purple-400/10 rounded-lg overflow-hidden">
              <iframe
                src="https://jup.ag/portfolio"
                className="w-full h-[450px] border-0"
                title="Jupiter Portfolio"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>
        </div>

      </div>

      {/* Holding Detail Modal */}
      <HoldingDetailModal 
        holding={selectedHolding}
        subnets={[]}
        onClose={() => setSelectedHolding(null)}
      />
    </div>
  );
}