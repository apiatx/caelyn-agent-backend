import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Search, Wallet, ExternalLink, TrendingUp, DollarSign } from 'lucide-react';
import { GlassCard } from './glass-card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { openDexScreenerLink } from '@/utils/mobile-links';

interface TokenBalance {
  token: string;
  symbol: string;
  balance: string;
  decimals: number;
  value: number;
  logo?: string;
  contractAddress?: string;
  chain: string;
  price: number;
  change24h: number;
}

interface ChainBalance {
  chain: string;
  totalValue: number;
  tokens: TokenBalance[];
}

interface CMCPortfolio {
  totalValue: number;
  chains: ChainBalance[];
  summary: { chain: string; value: number; tokenCount: number }[];
}

export function MultiChainPortfolioTracker() {
  const [walletAddress, setWalletAddress] = useState('');
  const [searchAddress, setSearchAddress] = useState('');

  const { data: portfolio, isLoading, error, refetch } = useQuery<CMCPortfolio>({
    queryKey: ['/api/coinbase/portfolio', searchAddress],
    enabled: !!searchAddress && searchAddress.length === 42, // Valid Ethereum address length
    refetchInterval: 30000, // Refresh every 30 seconds
    staleTime: 25000
  });

  const handleSearch = () => {
    if (walletAddress.trim() && walletAddress.length === 42) {
      setSearchAddress(walletAddress.trim());
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const openDexScreener = (contractAddress: string, chain: string) => {
    const chainName = chain.toLowerCase();
    if (contractAddress !== 'native') {
      openDexScreenerLink(`${chainName}/${contractAddress}`);
    }
  };

  const getChainColor = (chain: string) => {
    switch (chain.toLowerCase()) {
      case 'ethereum': return 'blue';
      case 'base': return 'purple';
      default: return 'gray';
    }
  };

  const formatBalance = (balance: string) => {
    const num = parseFloat(balance);
    if (num === 0) return '0';
    if (num < 0.001) return num.toExponential(2);
    if (num < 1) return num.toFixed(6);
    if (num < 1000) return num.toFixed(4);
    return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
  };

  return (
    <GlassCard className="p-3 sm:p-4 lg:p-6">
      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-4 lg:mb-6">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 sm:w-8 sm:h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
            <Wallet className="w-4 h-4 sm:w-5 sm:h-5 text-white" />
          </div>
          <h3 className="text-lg sm:text-xl lg:text-2xl font-semibold text-white">Multi-Chain Portfolio Tracker</h3>
        </div>
        <Badge className="bg-gradient-to-r from-blue-500/20 to-purple-500/20 text-blue-300 border-blue-500/30 text-xs">
          ETHEREUM + BASE
        </Badge>
      </div>

      {/* Search Input */}
      <div className="flex flex-col sm:flex-row gap-3 mb-4 lg:mb-6">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-white/35 w-4 h-4" />
          <Input
            type="text"
            placeholder="Enter wallet address (0x...)"
            value={walletAddress}
            onChange={(e) => setWalletAddress(e.target.value)}
            onKeyPress={handleKeyPress}
            className="pl-10 bg-black/20 border-white/[0.08] text-white placeholder-gray-400 text-sm sm:text-base"
          />
        </div>
        <Button
          onClick={handleSearch}
          disabled={!walletAddress.trim() || walletAddress.length !== 42}
          className="bg-gradient-to-r from-blue-500 to-purple-500 hover:from-blue-600 hover:to-purple-600 w-full sm:w-auto px-6"
        >
          Track Portfolio
        </Button>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-400"></div>
          <span className="ml-3 text-white/45">Scanning blockchain networks...</span>
        </div>
      )}

      {/* Error State */}
      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 mb-4">
          <p className="text-red-400">Failed to fetch portfolio data. Please try again.</p>
        </div>
      )}

      {/* Portfolio Results */}
      {portfolio && (
        <div className="space-y-6">
          {/* Summary Cards */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 sm:gap-4">
            <div className="bg-gradient-to-r from-green-500/10 to-emerald-500/10 border border-green-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <DollarSign className="w-4 h-4 sm:w-5 sm:h-5 text-green-400" />
                <span className="text-green-400 font-semibold text-sm sm:text-base">Total Value</span>
              </div>
              <p className="text-xl sm:text-2xl font-bold text-white">
                ${portfolio.totalValue.toLocaleString(undefined, { maximumFractionDigits: 2 })}
              </p>
            </div>

            <div className="bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <TrendingUp className="w-4 h-4 sm:w-5 sm:h-5 text-blue-400" />
                <span className="text-blue-400 font-semibold text-sm sm:text-base">Total Chains</span>
              </div>
              <p className="text-xl sm:text-2xl font-bold text-white">{portfolio.chains.length}</p>
            </div>

            <div className="bg-gradient-to-r from-purple-500/10 to-pink-500/10 border border-purple-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <Wallet className="w-4 h-4 sm:w-5 sm:h-5 text-purple-400" />
                <span className="text-purple-400 font-semibold text-sm sm:text-base">Total Assets</span>
              </div>
              <p className="text-xl sm:text-2xl font-bold text-white">
                {portfolio.chains.reduce((sum, chain) => sum + chain.tokens.length, 0)}
              </p>
            </div>
          </div>

          {/* Chain Breakdown */}
          {portfolio.chains.map((chainData) => (
            <div key={chainData.chain} className="space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <Badge className={`bg-${getChainColor(chainData.chain)}-500/20 text-${getChainColor(chainData.chain)}-400 border-${getChainColor(chainData.chain)}-500/30`}>
                    {chainData.chain.toUpperCase()}
                  </Badge>
                  <span className="text-xl font-semibold text-white">
                    ${chainData.totalValue.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                  </span>
                  <span className="text-white/35">({chainData.tokens.length} assets)</span>
                </div>
              </div>

              {/* Token List */}
              <div className="grid gap-3">
                {chainData.tokens
                  ?.filter(token => token.value >= 1) // Only show tokens worth $1+
                  ?.sort((a, b) => b.value - a.value)
                  ?.map((token, index) => (
                  <div key={`${token.contractAddress}-${index}`} className="bg-black/20 border border-white/[0.06] rounded-lg p-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        {token.logo ? (
                          <img src={token.logo} alt={token.symbol} className="w-10 h-10 rounded-full" />
                        ) : (
                          <div className="w-10 h-10 bg-gradient-to-r from-gray-600 to-gray-700 rounded-full flex items-center justify-center">
                            <span className="text-white font-bold text-sm">
                              {token.symbol.substring(0, 2)}
                            </span>
                          </div>
                        )}
                        <div>
                          <p className="font-semibold text-white">{token.symbol}</p>
                          <p className="text-sm text-white/35">{formatBalance(token.balance)}</p>
                          <p className="text-xs text-gray-500">${token.price.toFixed(4)}</p>
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <div className="text-right">
                          <p className="font-semibold text-white">
                            ${token.value.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                          </p>
                          {token.change24h !== 0 && (
                            <p className={`text-sm ${token.change24h >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                              {token.change24h >= 0 ? '+' : ''}{token.change24h.toFixed(2)}%
                            </p>
                          )}
                          <p className="text-sm text-white/35">{token.chain}</p>
                        </div>
                        {token.contractAddress !== 'native' && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => openDexScreener(token.contractAddress!, token.chain)}
                            className="text-white/35 border-gray-600 hover:text-white hover:border-gray-400"
                          >
                            <ExternalLink className="w-4 h-4" />
                          </Button>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}

          {/* Refresh Info */}
          <div className="text-center text-sm text-white/35 border-t border-white/[0.06] pt-4">
            Portfolio data refreshes every 30 seconds • Powered by Coinbase + Blockchain APIs
          </div>
        </div>
      )}

      {/* Empty State */}
      {!portfolio && !isLoading && searchAddress && (
        <div className="text-center py-12">
          <Wallet className="w-16 h-16 text-white/35 mx-auto mb-4" />
          <p className="text-white/35 mb-2">No portfolio data found</p>
          <p className="text-sm text-gray-500">Make sure the wallet address is correct and has transaction history</p>
        </div>
      )}

      {/* Initial State */}
      {!searchAddress && !isLoading && (
        <div className="text-center py-12">
          <Search className="w-16 h-16 text-white/35 mx-auto mb-4" />
          <p className="text-white/35 mb-2">Enter a wallet address to track portfolio</p>
          <p className="text-sm text-gray-500">
            Track holdings across Ethereum and Base networks with live pricing data
          </p>
        </div>
      )}
    </GlassCard>
  );
}