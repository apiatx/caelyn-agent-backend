import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, ExternalLink, Flame } from 'lucide-react';
import { Badge } from "@/components/ui/badge";

interface CoinData {
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

const TopTrendingCMC = () => {
  const [trendingCoins, setTrendingCoins] = useState<CoinData[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchTrending = async () => {
      try {
        setIsLoading(true);
        const response = await fetch('/api/coinmarketcap/trending');
        
        if (!response.ok) {
          throw new Error('Failed to fetch trending coins');
        }
        
        const data = await response.json();
        setTrendingCoins(data.slice(0, 20));
        setError(null);
      } catch (err) {
        console.error('Error fetching trending coins:', err);
        setError('Failed to load trending coins');
      } finally {
        setIsLoading(false);
      }
    };

    fetchTrending();
    
    // Refresh every 5 minutes
    const interval = setInterval(fetchTrending, 5 * 60 * 1000);
    
    return () => clearInterval(interval);
  }, []);

  const formatPrice = (price: number) => {
    if (price < 0.01) {
      return `$${price.toFixed(6)}`;
    } else if (price < 1) {
      return `$${price.toFixed(4)}`;
    } else if (price < 100) {
      return `$${price.toFixed(2)}`;
    } else {
      return `$${price.toFixed(0)}`;
    }
  };

  const formatMarketCap = (marketCap: number) => {
    if (marketCap >= 1e9) {
      return `$${(marketCap / 1e9).toFixed(1)}B`;
    } else if (marketCap >= 1e6) {
      return `$${(marketCap / 1e6).toFixed(0)}M`;
    } else {
      return `$${(marketCap / 1e3).toFixed(0)}K`;
    }
  };

  const formatVolume = (volume: number) => {
    if (volume >= 1e9) {
      return `$${(volume / 1e9).toFixed(1)}B`;
    } else if (volume >= 1e6) {
      return `$${(volume / 1e6).toFixed(0)}M`;
    } else {
      return `$${(volume / 1e3).toFixed(0)}K`;
    }
  };

  const formatPercentChange = (change: number) => {
    const isPositive = change > 0;
    const prefix = isPositive ? '+' : '';
    return `${prefix}${change.toFixed(2)}%`;
  };

  const getChangeColor = (change: number) => {
    return change > 0 ? 'text-green-400' : 'text-red-400';
  };

  const openCoinPage = (coin: CoinData) => {
    // Convert coin name to CMC URL slug format
    const slug = coin.name
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, '') // Remove special characters
      .replace(/\s+/g, '-') // Replace spaces with hyphens
      .replace(/-+/g, '-') // Replace multiple hyphens with single
      .replace(/^-|-$/g, ''); // Remove leading/trailing hyphens
    
    window.open(`https://coinmarketcap.com/currencies/${slug}/`, '_blank', 'noopener,noreferrer');
  };

  if (isLoading) {
    return (
      <div className="h-full bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6 flex flex-col">
        <div className="flex flex-col items-center justify-center space-y-2 mb-4">
          <div className="flex items-center space-x-2">
            <Flame className="w-5 h-5 text-orange-400" />
            <h3 className="text-lg font-semibold text-white">Top 20 by Volume</h3>
          </div>
          <button
            onClick={() => window.open('https://coinmarketcap.com/', '_blank', 'noopener,noreferrer')}
            className="text-sm text-yellow-400 hover:text-yellow-300 transition-colors duration-200 flex items-center gap-1"
          >
            View on CMC
            <ExternalLink className="w-3 h-3" />
          </button>
        </div>
        <div className="flex items-center justify-center py-8 flex-1">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-orange-400"></div>
          <span className="ml-3 text-crypto-silver">Loading top coins by volume...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="h-full bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6 flex flex-col">
        <div className="flex flex-col items-center justify-center space-y-2 mb-4">
          <div className="flex items-center space-x-2">
            <Flame className="w-5 h-5 text-orange-400" />
            <h3 className="text-lg font-semibold text-white">Top 20 by Volume</h3>
          </div>
          <button
            onClick={() => window.open('https://coinmarketcap.com/', '_blank', 'noopener,noreferrer')}
            className="text-sm text-yellow-400 hover:text-yellow-300 transition-colors duration-200 flex items-center gap-1"
          >
            View on CMC
            <ExternalLink className="w-3 h-3" />
          </button>
        </div>
        <div className="text-center py-8 flex-1">
          <TrendingDown className="w-12 h-12 text-red-400 mx-auto mb-2" />
          <p className="text-red-400">{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6 flex flex-col" data-testid="top-trending-cmc">
      <div className="flex flex-col items-center justify-center space-y-2 mb-6 flex-shrink-0">
        <div className="flex items-center space-x-2">
          <Flame className="w-5 h-5 text-orange-400" />
          <h3 className="text-lg font-semibold text-white">Top 20 by Volume</h3>
        </div>
      </div>
      
      <div className="space-y-2 overflow-y-auto flex-1 pr-2">
        {trendingCoins.map((coin, index) => (
          <div
            key={coin.id}
            className="bg-gradient-to-r from-orange-500/5 to-yellow-600/5 border border-orange-500/10 rounded-lg p-2 hover:border-orange-400/30 hover:bg-orange-500/10 transition-all duration-200 cursor-pointer group"
            onClick={() => openCoinPage(coin)}
            data-testid={`trending-coin-card-${coin.symbol}`}
          >
            <div className="flex items-center justify-between gap-2">
              <span className="text-xs font-bold text-orange-400 min-w-[24px]">#{index + 1}</span>
              
              <div className="min-w-0 flex-1">
                <div className="flex flex-col">
                  <h4 className="font-semibold text-white text-xs truncate">{coin.name}</h4>
                  <div className="flex items-center gap-1">
                    <span className="text-[10px] text-crypto-silver">{coin.symbol}</span>
                    <span className="text-[10px] text-gray-500">#{coin.cmc_rank}</span>
                  </div>
                </div>
              </div>
              
              <div className="text-right min-w-[60px]">
                <div className="text-xs font-medium text-white">{formatPrice(coin.quote.USD.price)}</div>
              </div>
              
              <div className="text-center min-w-[60px]">
                <div className={`flex items-center justify-center space-x-1 ${getChangeColor(coin.quote.USD.percent_change_24h)}`}>
                  {coin.quote.USD.percent_change_24h > 0 ? (
                    <TrendingUp className="w-3 h-3" />
                  ) : (
                    <TrendingDown className="w-3 h-3" />
                  )}
                  <span className="font-medium text-xs">
                    {formatPercentChange(coin.quote.USD.percent_change_24h)}
                  </span>
                </div>
                <div className="text-crypto-silver text-[9px]">24h</div>
              </div>
              
              <div className="text-center min-w-[50px]">
                <div className="font-medium text-white text-xs">{formatMarketCap(coin.quote.USD.market_cap)}</div>
                <div className="text-crypto-silver text-[9px]">MCap</div>
              </div>
              
              <ExternalLink className="w-3 h-3 text-white/35 group-hover:text-orange-400 transition-colors flex-shrink-0" />
            </div>
          </div>
        ))}
      </div>
      
      <div className="mt-4 text-center flex-shrink-0">
        <p className="text-xs text-crypto-silver">
          Data updates every 5 minutes • Click any coin to view on CoinMarketCap
        </p>
      </div>
    </div>
  );
};

export default TopTrendingCMC;
