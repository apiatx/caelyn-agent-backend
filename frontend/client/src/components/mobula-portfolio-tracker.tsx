import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { Wallet, TrendingUp, TrendingDown, Search, Loader2 } from 'lucide-react';
import { GlassCard } from './glass-card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface MobulaPortfolioBalance {
  asset: string;
  balance: number;
  balance_usd: number;
  price: number;
}

interface MobulaWalletResponse {
  balances: MobulaPortfolioBalance[];
  total_balance_usd: number;
}

function formatPrice(price: number): string {
  if (price >= 1) {
    return `$${price.toFixed(2)}`;
  } else if (price >= 0.01) {
    return `$${price.toFixed(4)}`;
  } else {
    return `$${price.toFixed(8)}`;
  }
}

function formatBalance(balance: number): string {
  if (balance >= 1e9) {
    return `${(balance / 1e9).toFixed(2)}B`;
  } else if (balance >= 1e6) {
    return `${(balance / 1e6).toFixed(2)}M`;
  } else if (balance >= 1e3) {
    return `${(balance / 1e3).toFixed(2)}K`;
  } else {
    return balance.toFixed(4);
  }
}

export function MobulaPortfolioTracker() {
  const [walletAddress, setWalletAddress] = useState('');
  const [searchedAddress, setSearchedAddress] = useState('');

  const { data: portfolio, isLoading, error, refetch } = useQuery<MobulaWalletResponse>({
    queryKey: ['/api/mobula/wallet', searchedAddress],
    enabled: !!searchedAddress,
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  const handleSearch = () => {
    if (walletAddress.trim()) {
      setSearchedAddress(walletAddress.trim());
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  return (
    <GlassCard className="p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="w-6 h-6 bg-green-500 rounded-full flex items-center justify-center">
          <Wallet className="w-4 h-4 text-white" />
        </div>
        <h3 className="text-xl font-semibold text-white">Mobula Portfolio Tracker</h3>
        <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
          MOBULA API
        </Badge>
      </div>

      {/* Wallet Address Input */}
      <div className="flex gap-2 mb-6">
        <Input
          type="text"
          placeholder="Enter wallet address (0x...)"
          value={walletAddress}
          onChange={(e) => setWalletAddress(e.target.value)}
          onKeyPress={handleKeyPress}
          className="flex-1 bg-gray-900/50 border-white/[0.06] text-white placeholder-gray-400"
        />
        <Button 
          onClick={handleSearch}
          disabled={!walletAddress.trim() || isLoading}
          className="bg-green-500 hover:bg-green-600 text-white"
        >
          {isLoading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <Search className="w-4 h-4" />
          )}
        </Button>
      </div>

      {/* Loading State */}
      {isLoading && searchedAddress && (
        <div className="text-center py-8">
          <Loader2 className="w-8 h-8 animate-spin text-green-400 mx-auto mb-4" />
          <p className="text-white/35">Fetching portfolio data...</p>
          <p className="text-gray-500 text-sm mt-1">Analyzing wallet: {searchedAddress.slice(0, 8)}...{searchedAddress.slice(-6)}</p>
        </div>
      )}

      {/* Error State */}
      {error && searchedAddress && (
        <div className="text-center py-8">
          <div className="w-16 h-16 bg-red-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
            <TrendingDown className="w-8 h-8 text-red-400" />
          </div>
          <p className="text-red-400 mb-2">Failed to load portfolio</p>
          <p className="text-white/35 text-sm">Please check the wallet address and try again</p>
          <Button 
            onClick={() => refetch()}
            variant="outline"
            className="mt-4 border-red-500/30 text-red-400 hover:bg-red-500/10"
          >
            Retry
          </Button>
        </div>
      )}

      {/* Portfolio Data */}
      {portfolio && !isLoading && (
        <div className="space-y-4">
          {/* Portfolio Summary */}
          <div className="p-4 bg-green-500/10 border border-green-500/20 rounded-lg">
            <div className="flex items-center justify-between">
              <div>
                <h4 className="text-green-400 font-semibold">Total Portfolio Value</h4>
                <p className="text-white/35 text-sm">
                  {searchedAddress.slice(0, 8)}...{searchedAddress.slice(-6)}
                </p>
              </div>
              <div className="text-right">
                <div className="text-2xl font-bold text-white">
                  ${portfolio.total_balance_usd.toFixed(2)}
                </div>
                <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
                  {portfolio.balances.length} ASSETS
                </Badge>
              </div>
            </div>
          </div>

          {/* Holdings List */}
          {portfolio.balances.length > 0 ? (
            <div className="space-y-2 max-h-[400px] overflow-y-auto">
              <h4 className="text-white font-semibold mb-2">Holdings Breakdown</h4>
              {portfolio.balances
                .filter(balance => balance.balance_usd >= 0.01) // Only show holdings worth more than $0.01
                .sort((a, b) => b.balance_usd - a.balance_usd) // Sort by USD value
                .map((balance, index) => (
                  <div
                    key={`${balance.asset}-${index}`}
                    className="flex items-center justify-between p-3 bg-gray-900/30 rounded-lg border border-white/[0.03]"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 bg-green-500/20 rounded-full flex items-center justify-center">
                        <span className="text-green-400 font-semibold text-xs">
                          {balance.asset.slice(0, 2).toUpperCase()}
                        </span>
                      </div>
                      <div>
                        <div className="text-white font-semibold">{balance.asset.toUpperCase()}</div>
                        <div className="text-white/35 text-sm">
                          {formatBalance(balance.balance)} tokens
                        </div>
                      </div>
                    </div>
                    
                    <div className="text-right">
                      <div className="text-white font-semibold">
                        ${balance.balance_usd.toFixed(2)}
                      </div>
                      <div className="text-white/35 text-sm">
                        {formatPrice(balance.price)} per token
                      </div>
                    </div>
                  </div>
                ))}
            </div>
          ) : (
            <div className="text-center py-8">
              <div className="w-16 h-16 bg-gray-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
                <Wallet className="w-8 h-8 text-white/35" />
              </div>
              <p className="text-white/35">No token balances found</p>
              <p className="text-gray-500 text-sm">This wallet appears to be empty or contains only very small amounts</p>
            </div>
          )}
        </div>
      )}

      {/* Initial State */}
      {!searchedAddress && !isLoading && (
        <div className="text-center py-8">
          <div className="w-16 h-16 bg-green-500/20 rounded-full flex items-center justify-center mx-auto mb-4">
            <Search className="w-8 h-8 text-green-400" />
          </div>
          <h4 className="text-white font-semibold mb-2">Track Any Wallet</h4>
          <p className="text-white/35 mb-4">Enter a wallet address to view real-time portfolio data</p>
          <div className="text-sm text-gray-500 space-y-1">
            <p>• Supports Ethereum and multi-chain addresses</p>
            <p>• Real-time token balances and USD values</p>
            <p>• Powered by Mobula API</p>
          </div>
        </div>
      )}
    </GlassCard>
  );
}