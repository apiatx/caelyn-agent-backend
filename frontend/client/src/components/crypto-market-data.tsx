import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { TrendingUp, TrendingDown, DollarSign, BarChart3, Clock } from 'lucide-react';
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";

interface CryptoData {
  id: number;
  name: string;
  symbol: string;
  cmc_rank: number;
  quote: {
    USD: {
      price: number;
      volume_24h: number;
      percent_change_1h: number;
      percent_change_24h: number;
      percent_change_7d: number;
      percent_change_30d: number;
      market_cap: number;
    };
  };
}

interface CryptoMarketDataProps {
  symbol: string;
  className?: string;
}

const formatPrice = (price: number): string => {
  if (price >= 1) {
    return price.toLocaleString('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  } else {
    return price.toLocaleString('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 4,
      maximumFractionDigits: 6
    });
  }
};

const formatMarketCap = (marketCap: number): string => {
  if (marketCap >= 1e12) {
    return `$${(marketCap / 1e12).toFixed(2)}T`;
  } else if (marketCap >= 1e9) {
    return `$${(marketCap / 1e9).toFixed(2)}B`;
  } else if (marketCap >= 1e6) {
    return `$${(marketCap / 1e6).toFixed(2)}M`;
  } else {
    return `$${marketCap.toLocaleString()}`;
  }
};

const formatVolume = (volume: number): string => {
  if (volume >= 1e9) {
    return `$${(volume / 1e9).toFixed(2)}B`;
  } else if (volume >= 1e6) {
    return `$${(volume / 1e6).toFixed(2)}M`;
  } else {
    return `$${(volume / 1e3).toFixed(2)}K`;
  }
};

const getChangeColor = (change: number): string => {
  if (change > 0) return 'text-green-400';
  if (change < 0) return 'text-red-400';
  return 'text-white/35';
};

const getChangeIcon = (change: number) => {
  if (change > 0) return <TrendingUp className="w-3 h-3" />;
  if (change < 0) return <TrendingDown className="w-3 h-3" />;
  return null;
};

export default function CryptoMarketData({ symbol, className = "" }: CryptoMarketDataProps) {
  const { data: cryptoData, isLoading, error, refetch } = useQuery<CryptoData[]>({
    queryKey: ['/api/coinmarketcap/majors'],
    refetchInterval: 30000, // Refresh every 30 seconds
    staleTime: 25000, // Consider data stale after 25 seconds
  });

  // Find the specific crypto data for this symbol
  const crypto = cryptoData?.find(c => c.symbol === symbol.toUpperCase());

  if (isLoading) {
    return (
      <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] p-4 ${className}`}>
        <div className="animate-pulse flex space-x-4">
          <div className="rounded-full bg-gray-700 h-8 w-8"></div>
          <div className="flex-1 space-y-2">
            <div className="h-4 bg-gray-700 rounded w-3/4"></div>
            <div className="space-y-1">
              <div className="h-3 bg-gray-700 rounded w-1/2"></div>
              <div className="h-3 bg-gray-700 rounded w-1/3"></div>
            </div>
          </div>
        </div>
      </Card>
    );
  }

  if (error || !crypto) {
    return (
      <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] p-4 ${className}`}>
        <div className="text-center">
          <p className="text-red-400 text-sm mb-2">Failed to load {symbol} data</p>
          <button 
            onClick={() => refetch()}
            className="text-crypto-silver hover:text-white text-xs underline"
          >
            Retry
          </button>
        </div>
      </Card>
    );
  }

  const quote = crypto.quote.USD;

  return (
    <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] p-4 ${className}`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 bg-crypto-silver/20 rounded-full flex items-center justify-center">
            <span className="text-white font-bold text-xs">{crypto.symbol}</span>
          </div>
          <div>
            <h4 className="text-white font-semibold text-sm">{crypto.name}</h4>
            <p className="text-crypto-silver text-xs">Rank #{crypto.cmc_rank}</p>
          </div>
        </div>
        <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
          LIVE
        </Badge>
      </div>

      {/* Main Data Points - All in one row */}
      <div className="grid grid-cols-4 gap-3 text-xs">
        <div>
          <div className="flex items-center gap-1 mb-1">
            <DollarSign className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">Price</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatPrice(quote.price)}</p>
        </div>
        
        <div>
          <div className="flex items-center gap-1 mb-1">
            <Clock className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">24h Change</span>
          </div>
          <div className={`flex items-center gap-1 font-semibold text-sm ${getChangeColor(quote.percent_change_24h)}`}>
            {getChangeIcon(quote.percent_change_24h)}
            <span>{quote.percent_change_24h >= 0 ? '+' : ''}{quote.percent_change_24h.toFixed(2)}%</span>
          </div>
        </div>
        
        <div>
          <div className="flex items-center gap-1 mb-1">
            <BarChart3 className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">Market Cap</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatMarketCap(quote.market_cap)}</p>
        </div>
        
        <div>
          <div className="flex items-center gap-1 mb-1">
            <TrendingUp className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">24h Volume</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatVolume(quote.volume_24h)}</p>
        </div>
      </div>

      {/* Performance Indicators */}
      <div className="mt-3 pt-3 border-t border-white/[0.03]">
        <div className="grid grid-cols-3 gap-2 text-xs">
          <div className="text-center">
            <span className="text-crypto-silver block">1h</span>
            <span className={`font-medium ${getChangeColor(quote.percent_change_1h)}`}>
              {quote.percent_change_1h >= 0 ? '+' : ''}{quote.percent_change_1h.toFixed(2)}%
            </span>
          </div>
          <div className="text-center">
            <span className="text-crypto-silver block">7d</span>
            <span className={`font-medium ${getChangeColor(quote.percent_change_7d)}`}>
              {quote.percent_change_7d >= 0 ? '+' : ''}{quote.percent_change_7d.toFixed(2)}%
            </span>
          </div>
          <div className="text-center">
            <span className="text-crypto-silver block">30d</span>
            <span className={`font-medium ${getChangeColor(quote.percent_change_30d)}`}>
              {quote.percent_change_30d >= 0 ? '+' : ''}{quote.percent_change_30d.toFixed(2)}%
            </span>
          </div>
        </div>
      </div>

      {/* Last Updated */}
      <div className="mt-2 pt-2 border-t border-white/[0.03]">
        <p className="text-crypto-silver text-xs text-center">
          Data updates every 30s • Powered by CoinMarketCap
        </p>
      </div>
    </Card>
  );
}