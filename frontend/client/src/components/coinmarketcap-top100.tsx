import { useQuery } from '@tanstack/react-query';
import { TrendingUp, TrendingDown, DollarSign, BarChart3, Globe, ArrowUpDown, ArrowUp, ArrowDown } from 'lucide-react';
import { GlassCard } from './glass-card';
import { Badge } from '@/components/ui/badge';
import { useState, useMemo } from 'react';
import { openCoinMarketCapLink } from '@/utils/mobile-links';

interface CoinMarketCapCrypto {
  id: number;
  name: string;
  symbol: string;
  slug: string;
  cmc_rank: number;
  quote: {
    USD: {
      price: number;
      volume_24h: number;
      percent_change_1h: number;
      percent_change_24h: number;
      percent_change_7d: number;
      percent_change_30d: number;
      percent_change_60d: number;
      percent_change_90d: number;
      market_cap: number;
    };
  };
}

type SortField = 'rank' | 'name' | 'price' | 'market_cap' | 'volume_24h' | 'percent_change_24h' | 'percent_change_7d' | 'percent_change_30d' | 'percent_change_60d' | 'percent_change_90d';
type SortDirection = 'asc' | 'desc';

export function CoinMarketCapTop100() {
  const [sortField, setSortField] = useState<SortField>('rank');
  const [sortDirection, setSortDirection] = useState<SortDirection>('asc');

  const { data: cryptos, isLoading, error, refetch } = useQuery<CoinMarketCapCrypto[]>({
    queryKey: ['/api/coinmarketcap/top100'],
    refetchInterval: 300000, // Refresh every 5 minutes
    staleTime: 240000 // 4 minutes
  });

  const sortedCryptos = useMemo(() => {
    if (!cryptos) return [];

    return [...cryptos].sort((a, b) => {
      let aValue: number | string;
      let bValue: number | string;

      switch (sortField) {
        case 'rank':
          aValue = a.cmc_rank;
          bValue = b.cmc_rank;
          break;
        case 'name':
          aValue = a.name;
          bValue = b.name;
          break;
        case 'price':
          aValue = a.quote.USD.price;
          bValue = b.quote.USD.price;
          break;
        case 'market_cap':
          aValue = a.quote.USD.market_cap;
          bValue = b.quote.USD.market_cap;
          break;
        case 'volume_24h':
          aValue = a.quote.USD.volume_24h;
          bValue = b.quote.USD.volume_24h;
          break;
        case 'percent_change_24h':
          aValue = a.quote.USD.percent_change_24h || 0;
          bValue = b.quote.USD.percent_change_24h || 0;
          break;
        case 'percent_change_7d':
          aValue = a.quote.USD.percent_change_7d || 0;
          bValue = b.quote.USD.percent_change_7d || 0;
          break;
        case 'percent_change_30d':
          aValue = a.quote.USD.percent_change_30d || 0;
          bValue = b.quote.USD.percent_change_30d || 0;
          break;
        case 'percent_change_60d':
          aValue = a.quote.USD.percent_change_60d || 0;
          bValue = b.quote.USD.percent_change_60d || 0;
          break;
        case 'percent_change_90d':
          aValue = a.quote.USD.percent_change_90d || 0;
          bValue = b.quote.USD.percent_change_90d || 0;
          break;
        default:
          aValue = a.cmc_rank;
          bValue = b.cmc_rank;
      }

      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortDirection === 'asc' 
          ? aValue.localeCompare(bValue)
          : bValue.localeCompare(aValue);
      }

      return sortDirection === 'asc' 
        ? (aValue as number) - (bValue as number)
        : (bValue as number) - (aValue as number);
    });
  }, [cryptos, sortField, sortDirection]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection(field === 'rank' ? 'asc' : 'desc');
    }
  };

  const getSortIcon = (field: SortField) => {
    if (sortField !== field) {
      return <ArrowUpDown className="w-3 h-3 text-crypto-silver/50" />;
    }
    return sortDirection === 'asc' 
      ? <ArrowUp className="w-3 h-3 text-blue-400" />
      : <ArrowDown className="w-3 h-3 text-blue-400" />;
  };

  const formatPrice = (price: number) => {
    if (price < 0.01) return `$${price.toFixed(6)}`;
    if (price < 1) return `$${price.toFixed(4)}`;
    if (price < 100) return `$${price.toFixed(2)}`;
    return `$${price.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  };

  const formatMarketCap = (marketCap: number) => {
    if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
    if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
    if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
    return `$${marketCap.toLocaleString()}`;
  };

  const formatVolume = (volume: number) => {
    if (volume >= 1e9) return `$${(volume / 1e9).toFixed(2)}B`;
    if (volume >= 1e6) return `$${(volume / 1e6).toFixed(2)}M`;
    if (volume >= 1e3) return `$${(volume / 1e3).toFixed(2)}K`;
    return `$${volume.toLocaleString()}`;
  };

  const formatPercentage = (percentage: number) => {
    return `${percentage >= 0 ? '+' : ''}${percentage.toFixed(2)}%`;
  };

  const getPercentageColor = (percentage: number) => {
    return percentage >= 0 ? 'text-green-400' : 'text-red-400';
  };

  const openCoinMarketCap = (slug: string) => {
    openCoinMarketCapLink(`currencies/${slug}/`);
  };

  return (
    <GlassCard className="p-3 sm:p-4 lg:p-6">
      <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-4 lg:mb-6">
        <div className="flex items-center gap-2">
          <div className="w-7 h-7 sm:w-8 sm:h-8 bg-gradient-to-r from-yellow-500 to-orange-500 rounded-full flex items-center justify-center">
            <BarChart3 className="w-4 h-4 sm:w-5 sm:h-5 text-white" />
          </div>
          <h3 className="text-lg sm:text-xl lg:text-2xl font-semibold text-white">Top 100 Cryptocurrencies</h3>
        </div>
        <div className="flex gap-2">
          <Badge className="bg-gradient-to-r from-yellow-500/20 to-orange-500/20 text-yellow-300 border-yellow-500/30 text-xs">
            COINMARKETCAP
          </Badge>
          <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
            5MIN REFRESH
          </Badge>
        </div>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-12">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-yellow-400"></div>
          <span className="ml-3 text-white/45">Loading market data from CoinMarketCap...</span>
        </div>
      )}

      {error && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4 mb-4">
          <p className="text-red-400">Failed to fetch cryptocurrency data. Please try again.</p>
        </div>
      )}

      {cryptos && (
        <div className="space-y-4">
          {/* Summary Stats */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 sm:gap-4 mb-4 lg:mb-6">
            <div className="bg-gradient-to-r from-green-500/10 to-emerald-500/10 border border-green-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <DollarSign className="w-4 h-4 sm:w-5 sm:h-5 text-green-400" />
                <span className="text-green-400 font-semibold text-sm sm:text-base">Total Market Cap</span>
              </div>
              <p className="text-lg sm:text-xl font-bold text-white">
                {formatMarketCap(cryptos.reduce((sum, crypto) => sum + crypto.quote.USD.market_cap, 0))}
              </p>
            </div>

            <div className="bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <BarChart3 className="w-4 h-4 sm:w-5 sm:h-5 text-blue-400" />
                <span className="text-blue-400 font-semibold text-sm sm:text-base">24h Volume</span>
              </div>
              <p className="text-lg sm:text-xl font-bold text-white">
                {formatVolume(cryptos.reduce((sum, crypto) => sum + crypto.quote.USD.volume_24h, 0))}
              </p>
            </div>

            <div className="bg-gradient-to-r from-yellow-500/10 to-orange-500/10 border border-yellow-500/20 rounded-lg p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-2">
                <Globe className="w-4 h-4 sm:w-5 sm:h-5 text-yellow-400" />
                <span className="text-yellow-400 font-semibold text-sm sm:text-base">Cryptocurrencies</span>
              </div>
              <p className="text-lg sm:text-xl font-bold text-white">{cryptos.length}</p>
            </div>
          </div>

          {/* Interactive Sorting Header Row */}
          <div className="bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-lg p-4 mb-4">
            <h3 className="text-white font-semibold mb-3 flex items-center gap-2">
              <TrendingUp className="w-5 h-5 text-blue-400" />
              Sort By
            </h3>
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-2">
              <button 
                onClick={() => handleSort('market_cap')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'market_cap' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                Market Cap {getSortIcon('market_cap')}
              </button>
              <button 
                onClick={() => handleSort('volume_24h')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'volume_24h' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                24h Volume {getSortIcon('volume_24h')}
              </button>
              <button 
                onClick={() => handleSort('percent_change_7d')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'percent_change_7d' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                7d % {getSortIcon('percent_change_7d')}
              </button>
              <button 
                onClick={() => handleSort('percent_change_30d')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'percent_change_30d' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                30d % {getSortIcon('percent_change_30d')}
              </button>
              <button 
                onClick={() => handleSort('percent_change_90d')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'percent_change_90d' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                90d % {getSortIcon('percent_change_90d')}
              </button>
              <button 
                onClick={() => handleSort('price')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'price' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                Price {getSortIcon('price')}
              </button>
              <button 
                onClick={() => handleSort('rank')}
                className={`flex items-center justify-center gap-1 px-3 py-2 rounded-lg border transition-all ${
                  sortField === 'rank' 
                    ? 'bg-blue-500/20 border-blue-500/50 text-blue-400' 
                    : 'bg-black/20 border-white/[0.06] text-white/35 hover:text-white hover:border-white/[0.10]'
                }`}
              >
                Rank {getSortIcon('rank')}
              </button>
            </div>
          </div>

          {/* Desktop Table View */}
          <div className="hidden lg:block">
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <div className="grid grid-cols-10 gap-4 p-4 bg-black/30 border-b border-white/[0.06]">
                <div className="text-white/35 font-semibold">#</div>
                <div className="text-white/35 font-semibold col-span-2">Name</div>
                <div className="text-white/35 font-semibold text-right">Price</div>
                <div className="text-white/35 font-semibold text-right">24h %</div>
                <div className="text-white/35 font-semibold text-right">7d %</div>
                <div className="text-white/35 font-semibold text-right">30d %</div>
                <div className="text-white/35 font-semibold text-right">YTD %</div>
                <div className="text-white/35 font-semibold text-right">Market Cap</div>
                <div className="text-white/35 font-semibold text-right">Volume</div>
              </div>
              <div className="max-h-96 overflow-y-auto">
                {sortedCryptos.slice(0, 50).map((crypto) => (
                  <div
                    key={crypto.id}
                    className="grid grid-cols-10 gap-4 p-4 border-b border-white/[0.03] hover:bg-black/20 transition-colors"
                  >
                    <div className="text-white/45 font-medium">{crypto.cmc_rank}</div>
                    <div className="col-span-2">
                      <div 
                        onClick={() => openCoinMarketCap(crypto.slug)}
                        className="flex items-center gap-2 cursor-pointer hover:text-blue-400 transition-colors"
                      >
                        <div className="w-8 h-8 bg-gradient-to-r from-gray-600 to-gray-700 rounded-full flex items-center justify-center">
                          <span className="text-white font-bold text-xs">{crypto.symbol.substring(0, 2)}</span>
                        </div>
                        <div>
                          <p className="font-semibold text-white">{crypto.name}</p>
                          <p className="text-sm text-white/35">{crypto.symbol}</p>
                        </div>
                      </div>
                    </div>
                    <div className="text-right font-semibold text-white">
                      {formatPrice(crypto.quote.USD.price)}
                    </div>
                    <div className={`text-right font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_24h)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_24h)}
                    </div>
                    <div className={`text-right font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_7d)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_7d)}
                    </div>
                    <div className={`text-right font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_30d || 0)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_30d || 0)}
                    </div>
                    <div className={`text-right font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_90d || 0)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_90d || 0)}
                    </div>
                    <div className="text-right font-semibold text-white">
                      {formatMarketCap(crypto.quote.USD.market_cap)}
                    </div>
                    <div className="text-right font-semibold text-white">
                      {formatVolume(crypto.quote.USD.volume_24h)}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>



          {/* Mobile Card View */}
          <div className="lg:hidden grid gap-2 sm:gap-3 max-h-80 sm:max-h-96 overflow-y-auto">
            {sortedCryptos.slice(0, 50).map((crypto) => (
              <div
                key={crypto.id}
                className="bg-black/20 border border-white/[0.06] rounded-lg p-4 hover:bg-black/30 transition-colors"
              >
                <div className="flex items-center justify-between mb-3">
                  <div 
                    onClick={() => openCoinMarketCap(crypto.slug)}
                    className="flex items-center gap-3 cursor-pointer hover:text-blue-400 transition-colors"
                  >
                    <span className="text-white/35 font-medium">#{crypto.cmc_rank}</span>
                    <div className="w-8 h-8 bg-gradient-to-r from-gray-600 to-gray-700 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">{crypto.symbol.substring(0, 2)}</span>
                    </div>
                    <div>
                      <p className="font-semibold text-white">{crypto.name}</p>
                      <p className="text-sm text-white/35">{crypto.symbol}</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="font-semibold text-white">{formatPrice(crypto.quote.USD.price)}</p>
                    <p className={`text-sm font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_24h)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_24h)}
                    </p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4 text-sm mb-3">
                  <div>
                    <span className="text-white/35">Market Cap:</span>
                    <p className="text-white font-semibold">{formatMarketCap(crypto.quote.USD.market_cap)}</p>
                  </div>
                  <div>
                    <span className="text-white/35">Volume:</span>
                    <p className="text-white font-semibold">{formatVolume(crypto.quote.USD.volume_24h)}</p>
                  </div>
                </div>
                <div className="grid grid-cols-4 gap-2 text-xs">
                  <div className="text-center">
                    <span className="text-white/35 block">7d</span>
                    <p className={`font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_7d)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_7d)}
                    </p>
                  </div>
                  <div className="text-center">
                    <span className="text-white/35 block">30d</span>
                    <p className={`font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_30d || 0)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_30d || 0)}
                    </p>
                  </div>
                  <div className="text-center">
                    <span className="text-white/35 block">90d</span>
                    <p className={`font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_60d || 0)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_60d || 0)}
                    </p>
                  </div>
                  <div className="text-center">
                    <span className="text-white/35 block">YTD</span>
                    <p className={`font-semibold ${getPercentageColor(crypto.quote.USD.percent_change_90d || 0)}`}>
                      {formatPercentage(crypto.quote.USD.percent_change_90d || 0)}
                    </p>
                  </div>
                </div>
              </div>
            ))}
          </div>

          {/* Footer */}
          <div className="text-center text-sm text-white/35 border-t border-white/[0.06] pt-4">
            Data updates every 5 minutes • Powered by CoinMarketCap API
          </div>
        </div>
      )}
    </GlassCard>
  );
}