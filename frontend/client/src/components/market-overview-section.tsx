import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import { 
  TrendingUp, 
  TrendingDown, 
  DollarSign, 
  BarChart3, 
  Globe, 
  Activity,
  Bitcoin,
  LineChart,
  PieChart,
  ExternalLink,
  Calendar
} from 'lucide-react';
import { openCoinMarketCapLink } from '@/utils/mobile-links';
import { openSecureLink } from '@/utils/security';
import cryptoHippoWithBitcoin from "@assets/download (3)_1757211707979.png";

// Function to open external links
const openInNewTab = (url: string) => {
  window.open(url, '_blank', 'noopener,noreferrer');
};

interface GlobalMetrics {
  active_cryptocurrencies: number;
  total_cryptocurrencies: number;
  active_market_pairs: number;
  active_exchanges: number;
  total_exchanges: number;
  eth_dominance: number;
  btc_dominance: number;
  quote: {
    USD: {
      total_market_cap: number;
      total_volume_24h: number;
      total_volume_24h_reported: number;
      altcoin_volume_24h: number;
      altcoin_market_cap: number;
      defi_volume_24h: number;
      defi_market_cap: number;
      defi_24h_percentage_change: number;
      stablecoin_volume_24h: number;
      stablecoin_market_cap: number;
      derivatives_volume_24h: number;
      total_market_cap_yesterday: number;
      total_volume_24h_yesterday: number;
      total_market_cap_yesterday_percentage_change: number;
      total_volume_24h_yesterday_percentage_change: number;
    };
  };
}

interface AltSeasonData {
  index_value: number;
  timestamp: string;
  is_alt_season: boolean;
  description: string;
  historical: {
    yesterday: number;
    last_week: number;
    last_month: number;
    last_month_description: string;
  };
  yearly: {
    high: number;
    high_date: string;
    high_description: string;
    low: number;
    low_date: string;
    low_description: string;
  };
}

interface ETFNetflow {
  total_netflow: number;
  btc_netflow: number;
  eth_netflow: number;
  btc_percent_change: number;
  eth_percent_change: number;
}

interface ETFMetrics {
  symbol: string;
  name: string;
  net_assets: number;
  daily_flows: number;
  ytd_flows: number;
  market_cap: number;
  price: number;
  price_change_24h: number;
  volume_24h: number;
  last_updated: string;
}

interface ETFData {
  btc_etfs: ETFMetrics[];
  eth_etfs: ETFMetrics[];
  total_btc_flows: number;
  total_eth_flows: number;
  last_updated: string;
  cache_expires: string;
}

interface FearGreedData {
  index_value: number;
  timestamp: string;
  classification: string;
  historical: {
    yesterday: number;
    yesterday_classification: string;
    last_week: number;
    last_week_classification: string;
    last_month: number;
    last_month_classification: string;
  };
  yearly: {
    high: number;
    high_date: string;
    high_classification: string;
    low: number;
    low_date: string;
    low_classification: string;
  };
}

interface MarketOverview {
  globalMetrics: GlobalMetrics;
  altSeasonIndex: AltSeasonData;
  etfNetflows: ETFNetflow;
  fearGreedIndex: FearGreedData;
}

export function MarketOverviewSection() {
  const { data: overview, isLoading, error } = useQuery<MarketOverview>({
    queryKey: ['/api/coinmarketcap/market-overview'],
    refetchInterval: 180000, // Refresh every 3 minutes for REAL-TIME data (ultra aggressive)
    staleTime: 120000, // 2 minutes
    retry: 3,
    refetchOnWindowFocus: true, // Force refresh when user returns to tab
    refetchOnReconnect: true // Force refresh on reconnection
  });

  // Debug logging (production ready)
  if (error) {
    console.error('Market Overview Query Error:', error);
  }

  // Real-time ETF flows data (cached twice daily to preserve API credits)
  const { data: etfData, isLoading: etfLoading } = useQuery<ETFData>({
    queryKey: ['/api/etf/flows'],
    refetchInterval: 7200000, // 2 hours (12 times daily)
    staleTime: 3600000, // 1 hour
    retry: 2
  });

  const formatCurrency = (value: number) => {
    if (value >= 1e12) {
      return `$${(value / 1e12).toFixed(2)}T`;
    } else if (value >= 1e9) {
      return `$${(value / 1e9).toFixed(2)}B`;
    } else if (value >= 1e6) {
      return `$${(value / 1e6).toFixed(2)}M`;
    } else if (value >= 1e3) {
      return `$${(value / 1e3).toFixed(2)}K`;
    }
    return `$${value.toFixed(2)}`;
  };

  const formatPercentage = (value: number) => {
    const formatted = value.toFixed(2);
    return value >= 0 ? `+${formatted}%` : `${formatted}%`;
  };

  const formatNetflow = (value: number) => {
    const absValue = Math.abs(value);
    if (absValue >= 1000) {
      return `${value >= 0 ? '+' : '-'}$${absValue.toLocaleString()}M`;
    }
    return `${value >= 0 ? '+' : ''}$${value.toFixed(1)}M`;
  };

  const getPercentageColor = (value: number) => {
    return value >= 0 ? 'text-green-400' : 'text-red-400';
  };

  const getAltSeasonColor = (value: number) => {
    if (value > 75) return 'text-green-400';
    if (value > 25) return 'text-yellow-400';
    return 'text-red-400';
  };

  const getAltSeasonBgColor = (value: number) => {
    if (value > 75) return 'from-green-500/10 to-emerald-500/10 border-green-500/20';
    if (value > 25) return 'from-yellow-500/10 to-orange-500/10 border-yellow-500/20';
    return 'from-red-500/10 to-rose-500/10 border-red-500/20';
  };

  const openAltSeasonChart = () => {
    openCoinMarketCapLink('charts/altcoin-season-index/');
  };

  const openETFPage = () => {
    openCoinMarketCapLink('etf/');
  };

  const openFearGreedChart = () => {
    openCoinMarketCapLink('charts/fear-and-greed-index/');
  };

  const openMarketCharts = () => {
    openCoinMarketCapLink('charts/');
  };

  const getFearGreedColor = (value: number) => {
    if (value >= 75) return 'text-green-400';
    if (value >= 55) return 'text-green-300';
    if (value >= 45) return 'text-white/45';
    if (value >= 25) return 'text-orange-400';
    return 'text-red-400';
  };

  const getFearGreedBadgeColor = (classification: string) => {
    switch (classification) {
      case 'Extreme Greed': return 'bg-green-500 text-white';
      case 'Greed': return 'bg-green-400 text-white';
      case 'Neutral': return 'bg-gray-500 text-white';
      case 'Fear': return 'bg-orange-500 text-white';
      case 'Extreme Fear': return 'bg-red-500 text-white';
      default: return 'bg-gray-500 text-white';
    }
  };

  // Create circular gauge path based on value
  const createGaugePath = (value: number) => {
    const angle = (value / 100) * 180; // Half circle (180 degrees)
    const radian = (angle - 90) * (Math.PI / 180);
    const x = 50 + 40 * Math.cos(radian);
    const y = 50 + 40 * Math.sin(radian);
    return `M 10 50 A 40 40 0 ${angle > 90 ? 1 : 0} 1 ${x} ${y}`;
  };

  if (isLoading) {
    return (
      <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
        <div className="flex items-center gap-2 mb-4">
          <Globe className="w-5 h-5 text-blue-400" />
          <h2 className="text-xl font-bold text-white">Market Overview</h2>
          <div className="ml-auto">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-400"></div>
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="bg-black/30 border border-white/[0.06] rounded-lg p-4 animate-pulse">
              <div className="h-4 bg-gray-600 rounded mb-2"></div>
              <div className="h-6 bg-gray-600 rounded"></div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (error || (!overview && !isLoading)) {
    return (
      <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
        <div className="flex items-center gap-2 mb-4">
          <Globe className="w-5 h-5 text-red-400" />
          <h2 className="text-xl font-bold text-white">Market Overview</h2>
        </div>
        <div className="text-center text-white/35">
          <Activity className="w-12 h-12 mx-auto mb-2 opacity-50" />
          <p>Unable to load market data</p>
          <p className="text-sm">Please check your connection and try again</p>
          {error && <p className="text-xs text-red-400 mt-2">Error: {error.message}</p>}
        </div>
      </div>
    );
  }

  // Show loading if still loading and no data
  if (isLoading && !overview) {
    return (
      <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
        <div className="flex items-center gap-2 mb-4">
          <Globe className="w-5 h-5 text-blue-400" />
          <h2 className="text-xl font-bold text-white">Market Overview</h2>
          <div className="ml-auto">
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-400"></div>
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <div key={i} className="bg-black/30 border border-white/[0.06] rounded-lg p-4 animate-pulse">
              <div className="h-4 bg-gray-600 rounded mb-2"></div>
              <div className="h-6 bg-gray-600 rounded"></div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  // Ensure we have data before rendering
  if (!overview) {
    return null;
  }

  const { globalMetrics, altSeasonIndex, etfNetflows, fearGreedIndex } = overview;
  const usd = globalMetrics.quote.USD;

  return (
    <div className="bg-black/20 border border-white/[0.06] rounded-lg p-4 sm:p-6">
      {/* Crypto Hippo Header */}
      <div className="text-center mb-4">
        <img 
          src={cryptoHippoWithBitcoin}
          alt="Crypto Hippo with Bitcoin"
          className="w-20 h-20 sm:w-24 sm:h-24 mx-auto object-contain"
          style={{filter: 'drop-shadow(0 0 8px rgba(255, 165, 0, 0.3))'}}
        />
      </div>
      
      <div className="flex items-center gap-2 mb-6">
        <Globe className="w-5 h-5 text-blue-400" />
        <h2 className="text-xl font-bold text-white">Crypto Market Overview</h2>
        <div className="ml-auto text-xs text-white/35">Live Data</div>
      </div>

      {/* Global Market Stats - Clickable Link */}
      <div 
        onClick={openMarketCharts}
        className="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6 mb-6 cursor-pointer group hover:opacity-80 transition-opacity duration-200"
        title="Click to view detailed charts on CoinMarketCap"
      >
        <div className="bg-gradient-to-r from-blue-500/10 to-purple-500/10 border border-blue-500/20 rounded-lg p-3 sm:p-4">
          <div className="flex items-center gap-2 mb-2">
            <DollarSign className="w-4 h-4 text-blue-400" />
            <span className="text-blue-400 font-semibold text-xs sm:text-sm">Total Market Cap</span>
          </div>
          <p className="text-lg sm:text-xl font-bold text-white">{formatCurrency(usd.total_market_cap)}</p>
          <p className={`text-xs ${getPercentageColor(usd.total_market_cap_yesterday_percentage_change)}`}>
            {formatPercentage(usd.total_market_cap_yesterday_percentage_change)} 24h
          </p>
        </div>

        <div className="bg-gradient-to-r from-green-500/10 to-emerald-500/10 border border-green-500/20 rounded-lg p-3 sm:p-4">
          <div className="flex items-center gap-2 mb-2">
            <BarChart3 className="w-4 h-4 text-green-400" />
            <span className="text-green-400 font-semibold text-xs sm:text-sm">24h Volume</span>
          </div>
          <p className="text-lg sm:text-xl font-bold text-white">{formatCurrency(usd.total_volume_24h)}</p>
          <p className={`text-xs ${getPercentageColor(usd.total_volume_24h_yesterday_percentage_change)}`}>
            {formatPercentage(usd.total_volume_24h_yesterday_percentage_change)} 24h
          </p>
        </div>

        <div className="bg-gradient-to-r from-orange-500/10 to-yellow-500/10 border border-orange-500/20 rounded-lg p-3 sm:p-4">
          <div className="flex items-center gap-2 mb-2">
            <Bitcoin className="w-4 h-4 text-orange-400" />
            <span className="text-orange-400 font-semibold text-xs sm:text-sm">BTC Dominance</span>
          </div>
          <p className="text-lg sm:text-xl font-bold text-white">{globalMetrics.btc_dominance.toFixed(1)}%</p>
        </div>

        <div className="bg-gradient-to-r from-purple-500/10 to-pink-500/10 border border-purple-500/20 rounded-lg p-3 sm:p-4">
          <div className="flex items-center gap-2 mb-2">
            <PieChart className="w-4 h-4 text-purple-400" />
            <span className="text-purple-400 font-semibold text-xs sm:text-sm">ETH Dominance</span>
          </div>
          <p className="text-lg sm:text-xl font-bold text-white">{globalMetrics.eth_dominance.toFixed(1)}%</p>
        </div>


      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Fear & Greed Index - Clickable Link */}
        <div 
          onClick={openFearGreedChart}
          className="cursor-pointer group hover:opacity-80 transition-opacity duration-200"
          title="Click to view Fear & Greed Chart on CoinMarketCap"
        >
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <Activity className="w-5 h-5 text-purple-400" />
            Fear and Greed Index
            <span className="ml-auto text-xs text-purple-400 group-hover:text-purple-300 transition-colors">
              View Chart →
            </span>
          </h3>
          
          <div className="space-y-3">
            {/* Fear & Greed Circular Gauge */}
            <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-6">
              <div className="flex flex-col items-center mb-6">
                {/* Circular Gauge */}
                <div className="relative mb-4">
                  <svg width="200" height="120" className="overflow-visible">
                    {/* Background Arc */}
                    <path
                      d="M 20 100 A 80 80 0 0 1 180 100"
                      stroke="url(#gaugeGradient)"
                      strokeWidth="8"
                      fill="none"
                      strokeLinecap="round"
                    />
                    
                    {/* Value Indicator Dot */}
                    <circle
                      cx={20 + (fearGreedIndex.index_value / 100) * 160}
                      cy={100 - Math.sin((fearGreedIndex.index_value / 100) * Math.PI) * 80}
                      r="6"
                      fill="white"
                      stroke="#374151"
                      strokeWidth="2"
                    />
                    
                    {/* Gradient Definition */}
                    <defs>
                      <linearGradient id="gaugeGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                        <stop offset="0%" stopColor="#EF4444" /> {/* Red - Fear */}
                        <stop offset="25%" stopColor="#F97316" /> {/* Orange */}
                        <stop offset="50%" stopColor="#EAB308" /> {/* Yellow */}
                        <stop offset="75%" stopColor="#84CC16" /> {/* Light Green */}
                        <stop offset="100%" stopColor="#22C55E" /> {/* Green - Greed */}
                      </linearGradient>
                    </defs>
                  </svg>
                </div>
                
                {/* Index Value and Classification */}
                <div className="text-center">
                  <div className="text-4xl font-bold text-white mb-1">{fearGreedIndex.index_value}</div>
                  <div className="text-white/35 text-lg">{fearGreedIndex.classification}</div>
                </div>
              </div>
            </div>

            {/* Historical Values and Yearly High/Low side by side */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {/* Historical Values */}
              <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
                <h4 className="text-white text-lg font-bold mb-3">Historical Values</h4>
                
                <div className="space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Yesterday</span>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFearGreedBadgeColor(fearGreedIndex.historical.yesterday_classification)}`}>
                      {fearGreedIndex.historical.yesterday_classification} - {fearGreedIndex.historical.yesterday}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Last Week</span>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFearGreedBadgeColor(fearGreedIndex.historical.last_week_classification)}`}>
                      {fearGreedIndex.historical.last_week_classification} - {fearGreedIndex.historical.last_week}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Last Month</span>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFearGreedBadgeColor(fearGreedIndex.historical.last_month_classification)}`}>
                      {fearGreedIndex.historical.last_month_classification} - {fearGreedIndex.historical.last_month}
                    </span>
                  </div>
                </div>
              </div>

              {/* Yearly High and Low */}
              <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
                <h4 className="text-white text-lg font-bold mb-3">Yearly High and Low</h4>
                
                <div className="space-y-3">
                  <div>
                    <div className="flex justify-between items-center">
                      <div>
                        <p className="text-white font-medium text-sm">Yearly High</p>
                        <p className="text-white/35 text-xs">({fearGreedIndex.yearly.high_date})</p>
                      </div>
                      <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFearGreedBadgeColor(fearGreedIndex.yearly.high_classification)}`}>
                        {fearGreedIndex.yearly.high_classification} - {fearGreedIndex.yearly.high}
                      </span>
                    </div>
                  </div>
                  
                  <div>
                    <div className="flex justify-between items-center">
                      <div>
                        <p className="text-white font-medium text-sm">Yearly Low</p>
                        <p className="text-white/35 text-xs">({fearGreedIndex.yearly.low_date})</p>
                      </div>
                      <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFearGreedBadgeColor(fearGreedIndex.yearly.low_classification)}`}>
                        {fearGreedIndex.yearly.low_classification} - {fearGreedIndex.yearly.low}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Real ETF Net Flows from CoinMarketCap - Clickable Link */}
        <div 
          onClick={openETFPage}
          className="cursor-pointer group hover:opacity-80 transition-opacity duration-200"
          title="Click to view ETF page on CoinMarketCap"
        >
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <BarChart3 className="w-5 h-5 text-green-400" />
            ETF Net Flows
            <span className="text-xs bg-green-500/20 text-green-400 px-2 py-1 rounded-full border border-green-500/30">
              CMC REAL-TIME
            </span>
            <span className="ml-auto text-xs text-green-400 group-hover:text-green-300 transition-colors">
              View All ETFs →
            </span>
          </h3>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg p-6">
            {etfLoading ? (
              <div className="text-center py-8">
                <div className="animate-spin w-6 h-6 border-2 border-green-400 border-t-transparent rounded-full mx-auto mb-2"></div>
                <p className="text-sm text-white/35">Loading real ETF data...</p>
              </div>
            ) : etfData ? (
              <>
                {/* Total ETF Net Flow */}
                <div className="text-center mb-6">
                  <p className="text-sm text-white/35 mb-1">Total ETF Net Flow (Daily)</p>
                  <p className={`text-2xl sm:text-3xl font-bold ${getPercentageColor(etfData.total_btc_flows + etfData.total_eth_flows)}`}>
                    {formatNetflow(etfData.total_btc_flows + etfData.total_eth_flows)}
                  </p>
                  <p className="text-xs text-white/35 mt-1">
                    Last updated: {new Date(etfData.last_updated).toLocaleString()}
                  </p>
                </div>
                
                {/* BTC and ETH Breakdown */}
                <div className="grid grid-cols-2 gap-4">
                  <div className="text-center p-4 bg-orange-500/5 border border-orange-500/20 rounded-lg">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <div className="w-6 h-6 bg-orange-500 rounded-full flex items-center justify-center">
                        <span className="text-white font-bold text-xs">₿</span>
                      </div>
                      <span className="text-white font-semibold">BTC ETFs</span>
                    </div>
                    <p className={`text-xl font-bold ${getPercentageColor(etfData.total_btc_flows)}`}>
                      {formatNetflow(etfData.total_btc_flows)}
                    </p>
                    <p className="text-xs text-white/35">
                      {etfData.btc_etfs.length} BTC ETFs tracked
                    </p>
                  </div>
                  
                  <div className="text-center p-4 bg-blue-500/5 border border-blue-500/20 rounded-lg">
                    <div className="flex items-center justify-center gap-2 mb-2">
                      <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
                        <span className="text-white font-bold text-xs">Ξ</span>
                      </div>
                      <span className="text-white font-semibold">ETH ETFs</span>
                    </div>
                    <p className={`text-xl font-bold ${getPercentageColor(etfData.total_eth_flows)}`}>
                      {formatNetflow(etfData.total_eth_flows)}
                    </p>
                    <p className="text-xs text-white/35">
                      {etfData.eth_etfs.length} ETH ETFs tracked
                    </p>
                  </div>
                </div>
                
                <div className="mt-4 text-center">
                  <p className="text-xs text-white/35">
                    Real-time data from CoinMarketCap API • Updates twice daily (8AM/8PM UTC)
                  </p>
                </div>
              </>
            ) : (
              <div className="text-center py-8">
                <p className="text-sm text-red-400">Unable to load ETF data</p>
                <p className="text-xs text-white/35 mt-1">Check API connection</p>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Alt Season Index - Clickable Link */}
      <div className="grid grid-cols-1 lg:grid-cols-1 gap-6 mt-6">
        <div 
          onClick={openAltSeasonChart}
          className="cursor-pointer group hover:opacity-80 transition-opacity duration-200"
          title="Click to view Alt Season Index on CoinMarketCap"
        >
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-blue-400" />
            Alt Season Index
            <span className="ml-auto text-xs text-blue-400 group-hover:text-blue-300 transition-colors">
              View Chart →
            </span>
          </h3>
          
          <div className="space-y-3">
            {/* CMC Altcoin Season Index */}
            <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
              
              <div className="flex items-baseline gap-1 mb-4">
                <span className="text-white text-3xl font-bold">{altSeasonIndex.index_value}</span>
                <span className="text-white/35 text-lg">/ 100</span>
              </div>
              
              {/* Progress Bar */}
              <div className="mb-3">
                <div className="flex justify-between text-xs text-white/35 mb-1">
                  <span>Bitcoin Season</span>
                  <span>Altcoin Season</span>
                </div>
                <div className="relative h-4 bg-gradient-to-r from-orange-500 via-orange-300 to-blue-500 rounded-full">
                  <div 
                    className="absolute top-1/2 transform -translate-y-1/2 w-3 h-3 bg-white rounded-full border border-gray-700 shadow-lg"
                    style={{ left: `calc(${altSeasonIndex.index_value}% - 6px)` }}
                  />
                </div>
              </div>
            </div>

            {/* Historical Values and Yearly High/Low side by side */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {/* Historical Values */}
              <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
                <h4 className="text-white text-lg font-bold mb-3">Historical Values</h4>
                
                <div className="space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Yesterday</span>
                    <span className="bg-gray-600 text-white px-2 py-1 rounded-full text-xs font-medium">
                      {altSeasonIndex.historical.yesterday}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Last Week</span>
                    <span className="bg-gray-600 text-white px-2 py-1 rounded-full text-xs font-medium">
                      {altSeasonIndex.historical.last_week}
                    </span>
                  </div>
                  <div className="flex justify-between items-center">
                    <span className="text-white/35 text-sm">Last Month</span>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                      altSeasonIndex.historical.last_month > 75 ? 'bg-blue-500 text-white' : 
                      altSeasonIndex.historical.last_month > 25 ? 'bg-gray-600 text-white' : 'bg-orange-500 text-white'
                    }`}>
                      {altSeasonIndex.historical.last_month_description} - {altSeasonIndex.historical.last_month}
                    </span>
                  </div>
                </div>
              </div>

              {/* Yearly High and Low */}
              <div className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4">
                <h4 className="text-white text-lg font-bold mb-3">Yearly High and Low</h4>
                
                <div className="space-y-3">
                  <div>
                    <div className="flex justify-between items-center">
                      <div>
                        <p className="text-white font-medium text-sm">Yearly High</p>
                        <p className="text-white/35 text-xs">({altSeasonIndex.yearly.high_date})</p>
                      </div>
                      <span className="bg-blue-500 text-white px-2 py-1 rounded-full text-xs font-medium">
                        {altSeasonIndex.yearly.high_description} - {altSeasonIndex.yearly.high}
                      </span>
                    </div>
                  </div>
                  
                  <div>
                    <div className="flex justify-between items-center">
                      <div>
                        <p className="text-white font-medium text-sm">Yearly Low</p>
                        <p className="text-white/35 text-xs">({altSeasonIndex.yearly.low_date})</p>
                      </div>
                      <span className="bg-orange-500 text-white px-2 py-1 rounded-full text-xs font-medium">
                        {altSeasonIndex.yearly.low_description} - {altSeasonIndex.yearly.low}
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* News Section */}
      <div className="grid grid-cols-1 lg:grid-cols-1 gap-6 mt-6">
        <div>
          <h3 className="text-lg font-semibold text-white mb-4 flex items-center gap-2">
            <Globe className="w-5 h-5 text-green-400" />
            News
          </h3>
          
          {/* DefiLlama Feed iframe */}
          <div className="mb-6">
            <div className="flex justify-end mb-3">
              <button
                onClick={() => openInNewTab('https://feed.defillama.com/')}
                className="text-xs text-green-400 hover:text-green-300 transition-colors flex items-center gap-1"
                data-testid="button-defillama-feed-fullview"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </button>
            </div>
            <div className="w-full">
              <iframe
                src="https://feed.defillama.com/"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                title="DefiLlama News Feed"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                referrerPolicy="no-referrer-when-downgrade"
                data-testid="iframe-defillama-feed"
              />
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            <div 
              onClick={() => openInNewTab('https://www.binance.com/en/square')}
              className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4 cursor-pointer group hover:opacity-80 transition-opacity duration-200"
              title="Click to view Binance Square"
            >
              <div className="flex items-center gap-2 mb-2">
                <div className="w-6 h-6 bg-yellow-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-xs">B</span>
                </div>
                <h4 className="text-white font-medium">Binance Square</h4>
              </div>
              <p className="text-white/35 text-sm">Crypto news and social updates</p>
            </div>

            <div 
              onClick={() => openCoinMarketCapLink('leaderboard/')}
              className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4 cursor-pointer group hover:opacity-80 transition-opacity duration-200"
              title="Click to view CMC Leaderboard"
            >
              <div className="flex items-center gap-2 mb-2">
                <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-xs">C</span>
                </div>
                <h4 className="text-white font-medium">CMC Leaderboard</h4>
              </div>
              <p className="text-white/35 text-sm">Market Rankings</p>
            </div>

            <div 
              onClick={() => openInNewTab('https://www.coingecko.com/en/chains')}
              className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4 cursor-pointer group hover:opacity-80 transition-opacity duration-200"
              title="Click to view CoinGecko Chains"
            >
              <div className="flex items-center gap-2 mb-2">
                <div className="w-6 h-6 bg-green-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-xs">G</span>
                </div>
                <h4 className="text-white font-medium">CoinGecko Chains</h4>
              </div>
              <p className="text-white/35 text-sm">Blockchain Analytics</p>
            </div>

            <div 
              onClick={() => openInNewTab('https://intel.arkm.com/insights')}
              className="bg-gray-800/50 border border-gray-700/50 rounded-lg p-4 cursor-pointer group hover:opacity-80 transition-opacity duration-200"
              title="Click to view Arkham Intel Insights"
              data-testid="button-arkham-intel"
            >
              <div className="flex items-center gap-2 mb-2">
                <div className="w-6 h-6 bg-purple-500 rounded-full flex items-center justify-center">
                  <span className="text-white font-bold text-xs">A</span>
                </div>
                <h4 className="text-white font-medium">Arkham Intel</h4>
              </div>
              <p className="text-white/35 text-sm">On-chain intelligence & whale tracking</p>
            </div>
          </div>
          
          {/* CryptoPanic News iframe */}
          <div className="mt-6">
            <iframe
              src="https://cryptopanic.com/news/bitcoin/"
              className="w-full h-[500px] rounded-lg border border-white/[0.06]"
              title="CryptoPanic Bitcoin News"
              frameBorder="0"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
              referrerPolicy="no-referrer-when-downgrade"
            />
          </div>

          {/* Crypto Events Calendar Button */}
          <div className="mt-6">
            <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
              <button
                onClick={() => openSecureLink('https://coinmarketcal.com/en/')}
                className="bg-gradient-to-br from-crypto-warning/10 to-yellow-600/10 hover:from-crypto-warning/20 hover:to-yellow-600/20 border border-crypto-warning/20 hover:border-crypto-warning/40 rounded-lg p-4 transition-all duration-300 text-left group w-full max-w-md"
              >
                <div className="flex items-center gap-3">
                  <div className="w-8 h-8 bg-crypto-warning/20 rounded-full flex items-center justify-center">
                    <Calendar className="w-4 h-4 text-crypto-warning" />
                  </div>
                  <div>
                    <div className="text-sm font-medium text-white group-hover:text-crypto-warning mb-1">Crypto Events Calendar</div>
                    <div className="text-xs text-crypto-silver">Track upcoming crypto events and announcements</div>
                  </div>
                  <ExternalLink className="w-4 h-4 text-crypto-warning ml-auto" />
                </div>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* DXY (U.S. Dollar Index) Chart */}
      <div className="bg-black/20 border border-white/[0.06] rounded-lg p-3 sm:p-4 lg:p-6 mt-6">
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-6">
          <div className="flex items-center gap-2">
            <div className="w-5 h-5 sm:w-6 sm:h-6 bg-green-500 rounded-full flex items-center justify-center">
              <span className="text-white font-bold text-xs">$</span>
            </div>
            <h3 className="text-lg sm:text-xl font-semibold text-white">U.S. Dollar Index (DXY)</h3>
            <div className="bg-green-500/20 text-green-400 border border-green-500/30 text-xs px-2 py-1 rounded-full">
              FOREX
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => openInNewTab('https://www.tradingview.com/chart/e5l95XgZ/?symbol=TVC%3ADXY')}
              className="text-green-400 hover:text-green-300 text-xs sm:text-sm flex items-center gap-1"
            >
              Open in New Tab <ExternalLink className="w-3 h-3" />
            </button>
          </div>
        </div>

        <div className="w-full">
          <iframe
            src="https://www.tradingview.com/chart/e5l95XgZ/?symbol=TVC%3ADXY"
            className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
            title="U.S. Dollar Index (DXY) Chart"
            frameBorder="0"
            sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
            style={{
              background: 'transparent',
              colorScheme: 'dark'
            }}
          />
        </div>
      </div>

      {/* M2 Global Liquidity Index Chart */}
      <div className="bg-black/20 border border-white/[0.06] rounded-lg p-3 sm:p-4 lg:p-6 mt-6">
        <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-6">
          <div className="flex items-center gap-2">
            <div className="w-5 h-5 sm:w-6 sm:h-6 bg-indigo-500 rounded-full flex items-center justify-center">
              <span className="text-white font-bold text-xs">M2</span>
            </div>
            <h3 className="text-lg sm:text-xl font-semibold text-white">M2 Global Liquidity Index</h3>
            <div className="bg-indigo-500/20 text-indigo-400 border border-indigo-500/30 text-xs px-2 py-1 rounded-full">
              LIQUIDITY
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => openInNewTab('https://www.tradingview.com/script/34U4rcdC/')}
              className="text-indigo-400 hover:text-indigo-300 text-xs sm:text-sm flex items-center gap-1"
            >
              M2 / BTC <ExternalLink className="w-3 h-3" />
            </button>
            <span className="text-crypto-silver text-xs">|</span>
            <button
              onClick={() => openInNewTab('https://www.tradingview.com/chart/e5l95XgZ/')}
              className="text-indigo-400 hover:text-indigo-300 text-xs sm:text-sm"
            >
              Open in New Tab →
            </button>
          </div>
        </div>

        <div className="w-full">
          <iframe
            src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_m2global&symbol=FRED%3AM2SL&interval=1M&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=FRED%3AM2SL"
            className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
            title="M2 Global Liquidity Index Chart"
            frameBorder="0"
            scrolling="no"
          />
        </div>
      </div>

    </div>
  );
}