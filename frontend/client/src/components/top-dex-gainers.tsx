import { useState, useEffect } from 'react';
import { TrendingUp, TrendingDown, ExternalLink } from 'lucide-react';
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

const TopDexGainers = () => {
  const [gainers, setGainers] = useState<CoinData[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchDexGainers = async () => {
      try {
        setIsLoading(true);
        const response = await fetch('/api/coinmarketcap/dex-gainers');
        
        if (!response.ok) {
          throw new Error('Failed to fetch DEX token gainers');
        }
        
        const data = await response.json();
        // Sort by 24h percent change in descending order
        const sortedGainers = data.sort((a: CoinData, b: CoinData) => 
          b.quote.USD.percent_change_24h - a.quote.USD.percent_change_24h
        );
        setGainers(sortedGainers);
        setError(null);
      } catch (err) {
        console.error('Error fetching DEX token gainers:', err);
        setError('Failed to load DEX token gainers');
      } finally {
        setIsLoading(false);
      }
    };

    fetchDexGainers();
    
    // Refresh every 5 minutes
    const interval = setInterval(fetchDexGainers, 5 * 60 * 1000);
    
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
      return `$${(marketCap / 1e6).toFixed(1)}M`;
    } else {
      return `$${(marketCap / 1e3).toFixed(0)}K`;
    }
  };

  const formatPercentage = (percentage: number) => {
    const abs = Math.abs(percentage);
    if (abs >= 100) {
      return `${percentage > 0 ? '+' : ''}${percentage.toFixed(0)}%`;
    } else if (abs >= 10) {
      return `${percentage > 0 ? '+' : ''}${percentage.toFixed(1)}%`;
    } else {
      return `${percentage > 0 ? '+' : ''}${percentage.toFixed(2)}%`;
    }
  };

  const openCoinPage = (symbol: string) => {
    const url = `https://coinmarketcap.com/currencies/${symbol.toLowerCase()}/`;
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  if (error) {
    return (
      <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <div className="flex items-center space-x-2">
              <TrendingUp className="w-5 h-5 text-orange-400" />
              <h3 className="text-lg font-semibold text-white">Top 20 DEX Token Gainers</h3>
            </div>
            <p className="text-sm text-crypto-silver">CMC Dexscan Tokens</p>
          </div>
        </div>
        <div className="flex items-center justify-center py-8">
          <div className="text-center">
            <div className="text-red-400 mb-2">⚠️</div>
            <p className="text-crypto-silver">{error}</p>
          </div>
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <div className="flex items-center space-x-2">
              <TrendingUp className="w-5 h-5 text-orange-400" />
              <h3 className="text-lg font-semibold text-white">Top 20 DEX Token Gainers</h3>
            </div>
            <p className="text-sm text-crypto-silver">CMC Dexscan Tokens</p>
          </div>
        </div>
        <div className="flex items-center justify-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-orange-400"></div>
          <span className="ml-3 text-crypto-silver">Loading DEX gainers...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <div className="flex items-center space-x-2">
            <TrendingUp className="w-5 h-5 text-orange-400" />
            <h3 className="text-lg font-semibold text-white">Top 20 DEX Token Gainers</h3>
          </div>
          <p className="text-sm text-crypto-silver">CMC Dexscan Tokens</p>
        </div>
        <div className="text-right">
          <Badge className="bg-orange-500/20 text-orange-400 border-orange-500/30 text-xs">
            24H GAINERS
          </Badge>
          <p className="text-xs text-crypto-silver mt-1">Data updates every 5 minutes</p>
        </div>
      </div>
      
      <div className="space-y-2">
        {gainers.map((coin, index) => (
          <div
            key={coin.id}
            onClick={() => openCoinPage(coin.symbol)}
            className="flex items-center justify-between p-3 bg-gray-900/40 backdrop-blur-sm border border-gray-700/50 rounded-lg hover:bg-gray-800/60 hover:border-gray-600/60 transition-all duration-200 cursor-pointer group"
            data-testid={`dex-gainer-row-${coin.symbol.toLowerCase()}`}
          >
            <div className="flex items-center space-x-3 min-w-0 flex-1">
              <div className="flex-shrink-0 w-8 text-center">
                <span className="text-sm font-medium text-crypto-silver">#{index + 1}</span>
              </div>
              
              <div className="min-w-0 flex-1">
                <div className="flex items-center space-x-2">
                  <h4 className="text-sm font-semibold text-white truncate group-hover:text-orange-300 transition-colors">
                    {coin.name}
                  </h4>
                  <span className="text-xs text-crypto-silver bg-gray-800/60 px-2 py-1 rounded">
                    {coin.symbol}
                  </span>
                </div>
                <div className="flex items-center space-x-2 mt-1">
                  <span className="text-xs text-crypto-silver">
                    Rank #{coin.cmc_rank}
                  </span>
                  <span className="text-xs text-crypto-silver">
                    {formatMarketCap(coin.quote.USD.market_cap)}
                  </span>
                </div>
              </div>
            </div>
            
            <div className="flex items-center space-x-4 flex-shrink-0">
              <div className="text-right">
                <div className="text-sm font-medium text-white">
                  {formatPrice(coin.quote.USD.price)}
                </div>
                <div className="flex items-center space-x-1">
                  <div className="flex items-center">
                    {coin.quote.USD.percent_change_24h > 0 ? (
                      <TrendingUp className="w-3 h-3 text-green-400" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-400" />
                    )}
                    <span className={`text-xs font-medium ml-1 ${
                      coin.quote.USD.percent_change_24h > 0 ? 'text-green-400' : 'text-red-400'
                    }`}>
                      {formatPercentage(coin.quote.USD.percent_change_24h)}
                    </span>
                  </div>
                </div>
              </div>
              
              <ExternalLink className="w-4 h-4 text-crypto-silver group-hover:text-orange-400 transition-colors" />
            </div>
          </div>
        ))}
      </div>
      
      {gainers.length === 0 && (
        <div className="text-center py-8">
          <p className="text-crypto-silver">No DEX token gainers data available</p>
        </div>
      )}
    </div>
  );
};

export default TopDexGainers;