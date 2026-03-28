import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { TrendingUp, TrendingDown, DollarSign, BarChart3, Clock } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

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

interface CryptoMarketDataBareProps {
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

export default function CryptoMarketDataBare({ symbol, className = "" }: CryptoMarketDataBareProps) {
  const { data: cryptoData, isLoading, error, refetch } = useQuery<CryptoData[]>({
    queryKey: ['/api/coinmarketcap/majors'],
    refetchInterval: 30000, // Refresh every 30 seconds
    staleTime: 25000, // Consider data stale after 25 seconds
  });

  // Find the specific crypto data for this symbol
  const crypto = cryptoData?.find(c => c.symbol === symbol.toUpperCase());

  if (isLoading) {
    return (
      <div className={`${className}`}>
        <div className="animate-pulse">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <div className="rounded-full bg-gray-700 h-6 w-6"></div>
              <div className="h-4 bg-gray-700 rounded w-20"></div>
            </div>
            <div className="h-4 bg-gray-700 rounded w-12"></div>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div className="h-8 bg-gray-700 rounded"></div>
            <div className="h-8 bg-gray-700 rounded"></div>
            <div className="h-8 bg-gray-700 rounded"></div>
            <div className="h-8 bg-gray-700 rounded"></div>
          </div>
        </div>
      </div>
    );
  }

  if (error || !crypto) {
    return (
      <div className={`${className}`}>
        <div className="text-center">
          <p className="text-red-400 text-sm mb-2">Failed to load {symbol} data</p>
          <button 
            onClick={() => refetch()}
            className="text-crypto-silver hover:text-white text-xs underline"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  const quote = crypto.quote.USD;

  return (
    <div className={`${className}`}>
      {/* Header */}
      <div className="flex items-center justify-end mb-3">
        <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
          LIVE
        </Badge>
      </div>

      {/* Main Data Points - All in one row */}
      <div className="grid grid-cols-3 gap-4 text-xs mb-4">
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 mb-1">
            <DollarSign className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">Price</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatPrice(quote.price)}</p>
        </div>
        
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 mb-1">
            <BarChart3 className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">Market Cap</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatMarketCap(quote.market_cap)}</p>
        </div>
        
        <div className="text-center">
          <div className="flex items-center justify-center gap-1 mb-1">
            <TrendingUp className="w-3 h-3 text-crypto-silver" />
            <span className="text-crypto-silver">24h Volume</span>
          </div>
          <p className="text-white font-semibold text-sm">{formatVolume(quote.volume_24h)}</p>
        </div>
      </div>

      {/* Performance Indicators */}
      <div className="grid grid-cols-4 gap-3 text-xs pt-3 border-t border-white/[0.03]">
        <div className="text-center">
          <span className="text-crypto-silver block mb-1">1h</span>
          <span className={`font-medium ${getChangeColor(quote.percent_change_1h)}`}>
            {quote.percent_change_1h >= 0 ? '+' : ''}{quote.percent_change_1h.toFixed(2)}%
          </span>
        </div>
        <div className="text-center">
          <span className="text-crypto-silver block mb-1">24h</span>
          <span className={`font-medium ${getChangeColor(quote.percent_change_24h)}`}>
            {quote.percent_change_24h >= 0 ? '+' : ''}{quote.percent_change_24h.toFixed(2)}%
          </span>
        </div>
        <div className="text-center">
          <span className="text-crypto-silver block mb-1">7d</span>
          <span className={`font-medium ${getChangeColor(quote.percent_change_7d)}`}>
            {quote.percent_change_7d >= 0 ? '+' : ''}{quote.percent_change_7d.toFixed(2)}%
          </span>
        </div>
        <div className="text-center">
          <span className="text-crypto-silver block mb-1">30d</span>
          <span className={`font-medium ${getChangeColor(quote.percent_change_30d)}`}>
            {quote.percent_change_30d >= 0 ? '+' : ''}{quote.percent_change_30d.toFixed(2)}%
          </span>
        </div>
      </div>

      {/* Data Source */}
      <div className="mt-3 pt-2 border-t border-white/[0.03]">
        <p className="text-crypto-silver text-xs text-center">
          Data updates every 30s • Powered by CoinMarketCap
        </p>
      </div>
    </div>
  );
}