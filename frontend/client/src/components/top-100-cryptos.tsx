import { useQuery } from '@tanstack/react-query';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { GlassCard } from './glass-card';
import { Badge } from '@/components/ui/badge';

interface MobulaAsset {
  id: number;
  name: string;
  symbol: string;
  logo?: string;
  price?: number;
  price_change_24h?: number;
  market_cap?: number;
  volume_24h?: number;
  rank?: number;
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

function formatMarketCap(marketCap: number): string {
  if (marketCap >= 1e12) {
    return `$${(marketCap / 1e12).toFixed(2)}T`;
  } else if (marketCap >= 1e9) {
    return `$${(marketCap / 1e9).toFixed(2)}B`;
  } else if (marketCap >= 1e6) {
    return `$${(marketCap / 1e6).toFixed(2)}M`;
  } else {
    return `$${(marketCap / 1e3).toFixed(2)}K`;
  }
}

function getPriceChangeDisplay(change: number) {
  if (change > 0) {
    return {
      icon: <TrendingUp className="w-4 h-4" />,
      color: 'text-green-400',
      bgColor: 'bg-green-500/10',
      borderColor: 'border-green-500/20',
      prefix: '+'
    };
  } else if (change < 0) {
    return {
      icon: <TrendingDown className="w-4 h-4" />,
      color: 'text-red-400',
      bgColor: 'bg-red-500/10',
      borderColor: 'border-red-500/20',
      prefix: ''
    };
  } else {
    return {
      icon: <Minus className="w-4 h-4" />,
      color: 'text-white/35',
      bgColor: 'bg-gray-500/10',
      borderColor: 'border-gray-500/20',
      prefix: ''
    };
  }
}

export function Top100Cryptos() {
  const { data: cryptos, isLoading, error } = useQuery<MobulaAsset[]>({
    queryKey: ['/api/mobula/top100'],
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  if (isLoading) {
    return (
      <GlassCard className="p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
            <TrendingUp className="w-4 h-4 text-white" />
          </div>
          <h3 className="text-xl font-semibold text-white">Top 100 Cryptocurrencies</h3>
          <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30">
            MOBULA API
          </Badge>
        </div>
        <div className="space-y-3">
          {Array.from({ length: 10 }).map((_, i) => (
            <div key={i} className="animate-pulse">
              <div className="h-16 bg-gray-800/50 rounded-lg"></div>
            </div>
          ))}
        </div>
      </GlassCard>
    );
  }

  if (error) {
    return (
      <GlassCard className="p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-6 h-6 bg-red-500 rounded-full flex items-center justify-center">
            <TrendingUp className="w-4 h-4 text-white" />
          </div>
          <h3 className="text-xl font-semibold text-white">Top 100 Cryptocurrencies</h3>
          <Badge className="bg-red-500/20 text-red-400 border-red-500/30">
            ERROR
          </Badge>
        </div>
        <div className="text-center py-8">
          <p className="text-red-400">Failed to load cryptocurrency data</p>
          <p className="text-white/35 text-sm mt-2">Please check your API connection</p>
        </div>
      </GlassCard>
    );
  }

  if (!cryptos || cryptos.length === 0) {
    return (
      <GlassCard className="p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-6 h-6 bg-gray-500 rounded-full flex items-center justify-center">
            <TrendingUp className="w-4 h-4 text-white" />
          </div>
          <h3 className="text-xl font-semibold text-white">Top 100 Cryptocurrencies</h3>
          <Badge className="bg-gray-500/20 text-white/35 border-gray-500/30">
            NO DATA
          </Badge>
        </div>
        <div className="text-center py-8">
          <p className="text-white/35">No cryptocurrency data available</p>
        </div>
      </GlassCard>
    );
  }

  return (
    <GlassCard className="p-6">
      <div className="flex items-center gap-2 mb-4">
        <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
          <TrendingUp className="w-4 h-4 text-white" />
        </div>
        <h3 className="text-xl font-semibold text-white">Top 100 Cryptocurrencies</h3>
        <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30">
          LIVE
        </Badge>
        <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30">
          {cryptos.length} ASSETS
        </Badge>
      </div>
      
      <div className="max-h-[600px] overflow-y-auto space-y-2">
        {cryptos.map((crypto) => {
          const priceChange = getPriceChangeDisplay(crypto.price_change_24h || 0);
          
          return (
            <div
              key={crypto.id}
              className="flex items-center justify-between p-4 bg-gray-900/30 rounded-lg border border-white/[0.03] hover:border-white/[0.06] transition-colors"
            >
              <div className="flex items-center gap-3">
                <div className="flex items-center gap-2 min-w-[60px]">
                  <span className="text-xs text-white/35 font-mono">
                    #{crypto.rank || 'N/A'}
                  </span>
                </div>
                
                {crypto.logo && (
                  <img
                    src={crypto.logo}
                    alt={crypto.symbol}
                    className="w-8 h-8 rounded-full"
                    onError={(e) => {
                      e.currentTarget.style.display = 'none';
                    }}
                  />
                )}
                
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-white font-semibold">{crypto.symbol}</span>
                    <span className="text-white/35 text-sm">{crypto.name}</span>
                  </div>
                  {crypto.market_cap && (
                    <div className="text-xs text-gray-500">
                      {formatMarketCap(crypto.market_cap)} market cap
                    </div>
                  )}
                </div>
              </div>
              
              <div className="text-right">
                {crypto.price && (
                  <div className="text-white font-semibold">
                    {formatPrice(crypto.price)}
                  </div>
                )}
                
                {crypto.price_change_24h !== undefined && (
                  <div className={`flex items-center gap-1 justify-end ${priceChange.color}`}>
                    {priceChange.icon}
                    <span className="text-sm">
                      {priceChange.prefix}{Math.abs(crypto.price_change_24h).toFixed(2)}%
                    </span>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </GlassCard>
  );
}