import React from 'react';
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, BarChart3, DollarSign, Activity, Eye, Globe, Wallet, ExternalLink, Calendar } from "lucide-react";
import { Button } from "@/components/ui/button";
import cryptoHippoWithBitcoin from "@assets/download (3)_1757211833568.png";

import { MarketOverviewSection } from './market-overview-section';
import { openSecureLink } from '@/utils/security';

// Glass card component for crypto dashboard
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function CryptoDashboardSection() {
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="space-y-4 lg:space-y-8">
      {/* Market Overview Links */}
      <div className="space-y-4">
        {/* Market Overview Links - Above MacroEdge */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <button
            onClick={() => openInNewTab('https://coinmarketcap.com/charts/')}
            className="p-4 bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/20 rounded-lg hover:border-blue-400/50 transition-all duration-200 text-left group"
          >
            <div className="flex items-center gap-3">
              <BarChart3 className="w-6 h-6 text-blue-400" />
              <div>
                <div className="font-medium text-blue-200 group-hover:text-blue-100 transition-colors">
                  Crypto Market Overview
                </div>
                <div className="text-xs text-white/35 mt-1">
                  CoinMarketCap Charts
                </div>
              </div>
            </div>
          </button>

          <button
            onClick={() => openInNewTab('https://coinmarketcap.com/charts/fear-and-greed-index/')}
            className="p-4 bg-gradient-to-r from-purple-500/10 to-pink-500/10 border border-purple-500/20 rounded-lg hover:border-purple-400/50 transition-all duration-200 text-left group"
          >
            <div className="flex items-center gap-3">
              <Activity className="w-6 h-6 text-purple-400" />
              <div>
                <div className="font-medium text-purple-200 group-hover:text-purple-100 transition-colors">
                  Fear & Greed Index
                </div>
                <div className="text-xs text-white/35 mt-1">
                  Market Sentiment
                </div>
              </div>
            </div>
          </button>

          <button
            onClick={() => openInNewTab('https://coinmarketcap.com/etf/')}
            className="p-4 bg-gradient-to-r from-green-500/10 to-emerald-500/10 border border-green-500/20 rounded-lg hover:border-green-400/50 transition-all duration-200 text-left group"
          >
            <div className="flex items-center gap-3">
              <DollarSign className="w-6 h-6 text-green-400" />
              <div>
                <div className="font-medium text-green-200 group-hover:text-green-100 transition-colors">
                  ETF Net Flows
                </div>
                <div className="text-xs text-white/35 mt-1">
                  Institutional Flow
                </div>
              </div>
            </div>
          </button>

          <button
            onClick={() => openInNewTab('https://coinmarketcap.com/charts/altcoin-season-index/')}
            className="p-4 bg-gradient-to-r from-orange-500/10 to-yellow-500/10 border border-orange-500/20 rounded-lg hover:border-orange-400/50 transition-all duration-200 text-left group"
          >
            <div className="flex items-center gap-3">
              <TrendingUp className="w-6 h-6 text-orange-400" />
              <div>
                <div className="font-medium text-orange-200 group-hover:text-orange-100 transition-colors">
                  Alt Season Index
                </div>
                <div className="text-xs text-white/35 mt-1">
                  Altcoin Performance
                </div>
              </div>
            </div>
          </button>
        </div>
        
      </div>







      {/* News Section */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-6">
          <div className="flex items-center space-x-3">
            <h2 className="text-xl font-semibold text-white">News</h2>
            <span className="px-2 py-1 bg-gradient-to-r from-blue-500 to-purple-600 text-white text-xs rounded-lg font-medium">
              COINTELEGRAPH
            </span>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => openInNewTab('https://cointelegraph.com/category/latest-news')}
            className="text-crypto-silver hover:text-white"
          >
            <ExternalLink className="h-4 w-4 mr-2" />
            Open Full View
          </Button>
        </div>
        
        {/* DL News Main iframe */}
        <div className="mb-6">
          <div className="flex justify-end mb-3">
            <button
              onClick={() => openInNewTab('https://www.dlnews.com/')}
              className="text-xs text-green-400 hover:text-green-300 transition-colors flex items-center gap-1"
              data-testid="button-dlnews-fullview"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </button>
          </div>
          <div className="w-full">
            <iframe
              src="https://www.dlnews.com/"
              className="w-full h-[600px] rounded-lg border border-white/[0.06]"
              title="DL News"
              frameBorder="0"
              loading="lazy"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
              referrerPolicy="no-referrer-when-downgrade"
              data-testid="iframe-dlnews"
            />
          </div>
        </div>

        {/* NotifAI News iframe */}
        <div className="mb-6">
          <div className="flex justify-end mb-3">
            <button
              onClick={() => openInNewTab('https://www.notifai.news/')}
              className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
              data-testid="button-notifai-fullview"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </button>
          </div>
          <div className="w-full">
            <iframe
              src="https://www.notifai.news/"
              className="w-full h-[600px] rounded-lg border border-white/[0.06]"
              title="NotifAI News"
              frameBorder="0"
              loading="lazy"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
              referrerPolicy="no-referrer-when-downgrade"
              data-testid="iframe-notifai"
            />
          </div>
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://cointelegraph.com/category/latest-news')}
              className="bg-black/20 border-white/[0.06] hover:bg-blue-500/20 hover:border-blue-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">Cointelegraph</div>
                <div className="text-sm text-crypto-silver">Latest crypto news & analysis</div>
              </div>
            </Button>
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://www.coindesk.com/')}
              className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">CoinDesk</div>
                <div className="text-sm text-crypto-silver">Digital asset news & insights</div>
              </div>
            </Button>
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://www.bankless.com/')}
              className="bg-black/20 border-white/[0.06] hover:bg-purple-500/20 hover:border-purple-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">Bankless</div>
                <div className="text-sm text-crypto-silver">DeFi & Web3 content</div>
              </div>
            </Button>
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://coingape.com/category/news/')}
              className="bg-black/20 border-white/[0.06] hover:bg-green-500/20 hover:border-green-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">CoinGape</div>
                <div className="text-sm text-crypto-silver">Crypto news & market updates</div>
              </div>
            </Button>
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://u.today/')}
              className="bg-black/20 border-white/[0.06] hover:bg-red-500/20 hover:border-red-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">U.Today</div>
                <div className="text-sm text-crypto-silver">Cryptocurrency & blockchain news</div>
              </div>
            </Button>
            <Button
              variant="outline"
              onClick={() => openInNewTab('https://thedefiant.io/')}
              className="bg-black/20 border-white/[0.06] hover:bg-indigo-500/20 hover:border-indigo-500/30 text-white justify-start p-4 h-auto"
            >
              <ExternalLink className="w-4 h-4 mr-2" />
              <div className="text-left">
                <div className="font-semibold">The Defiant</div>
                <div className="text-sm text-crypto-silver">DeFi & open finance news</div>
              </div>
            </Button>
          </div>

          {/* Crypto Events Calendar Button - moved to bottom of News section */}
          <div className="mt-6 pt-6 border-t border-white/[0.06]">
            <button
              onClick={() => openSecureLink('https://coinmarketcal.com/en/')}
              className="bg-gradient-to-br from-crypto-warning/10 to-yellow-600/10 hover:from-crypto-warning/20 hover:to-yellow-600/20 border border-crypto-warning/20 hover:border-crypto-warning/40 rounded-lg p-4 transition-all duration-300 group w-full"
              data-testid="button-crypto-calendar"
            >
              <div className="flex items-center justify-center gap-3">
                <div className="w-8 h-8 bg-crypto-warning/20 rounded-full flex items-center justify-center">
                  <Calendar className="w-4 h-4 text-crypto-warning" />
                </div>
                <div className="text-center">
                  <div className="text-lg font-medium text-white group-hover:text-crypto-warning mb-1">Crypto Events Calendar</div>
                  <div className="text-xs text-crypto-silver">Track upcoming crypto events and announcements</div>
                </div>
                <ExternalLink className="w-4 h-4 text-crypto-warning" />
              </div>
            </button>
          </div>
        </div>
      </GlassCard>

      {/* Bitcoin Charts Section */}
      <GlassCard className="p-3 sm:p-4 lg:p-6">
        <div className="space-y-3 sm:space-y-4">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-5 h-5 sm:w-6 sm:h-6 bg-orange-500 rounded-full flex items-center justify-center">
              <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
            </div>
            <h3 className="text-lg sm:text-xl font-semibold text-white">Bitcoin</h3>
            <Badge className="bg-orange-500/20 text-orange-400 border-orange-500/30 text-xs">
              BITCOIN ANALYSIS
            </Badge>
          </div>
          {/* Row 1: NewHedge + BitBo Charts */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4">
            <button
              onClick={() => openInNewTab('https://newhedge.io/bitcoin')}
              className="p-3 sm:p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg hover:bg-purple-500/20 transition-colors"
            >
              <h5 className="text-purple-400 font-semibold mb-1 text-sm sm:text-base">NewHedge Bitcoin</h5>
              <p className="text-white/35 text-xs sm:text-sm">Bitcoin Analytics</p>
            </button>

            <button
              onClick={() => openInNewTab('https://charts.bitbo.io/index/')}
              className="p-3 sm:p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg hover:bg-purple-500/20 transition-colors"
            >
              <h5 className="text-purple-400 font-semibold mb-1 text-sm sm:text-base">BitBo Charts</h5>
              <p className="text-white/35 text-xs sm:text-sm">Crypto Indices</p>
            </button>
          </div>

          {/* Row 2: CryptoQuant + BitcoinMagazine */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4">
            <button
              onClick={() => openInNewTab('https://cryptoquant.com/community/dashboard/67c05819d6c9383057d3df58')}
              className="p-3 sm:p-4 bg-blue-500/10 border border-blue-500/20 rounded-lg hover:bg-blue-500/20 transition-colors"
            >
              <h5 className="text-blue-400 font-semibold mb-1 text-sm sm:text-base">CryptoQuant</h5>
              <p className="text-white/35 text-xs sm:text-sm">On-Chain Metrics</p>
            </button>

            <button
              onClick={() => openInNewTab('https://www.bitcoinmagazinepro.com/charts/pi-cycle-top-indicator/')}
              className="p-3 sm:p-4 bg-orange-500/10 border border-orange-500/20 rounded-lg hover:bg-orange-500/20 transition-colors"
            >
              <h5 className="text-orange-400 font-semibold mb-1 text-sm sm:text-base">BitcoinMagazine Indicators</h5>
              <p className="text-white/35 text-xs sm:text-sm">Bitcoin Analysis</p>
            </button>
          </div>

          {/* Row 3: Bitcoin Monthly Returns Heatmap - Full Width */}
          <div className="grid grid-cols-1 gap-3 sm:gap-4">
            <button
              onClick={() => openInNewTab('https://newhedge.io/bitcoin/monthly-returns-heatmap')}
              className="p-3 sm:p-4 bg-orange-500/10 border border-orange-500/20 rounded-lg hover:bg-orange-500/20 transition-colors"
            >
              <h5 className="text-orange-400 font-semibold mb-1 text-sm sm:text-base">Bitcoin Monthly Returns Heatmap</h5>
              <p className="text-white/35 text-xs sm:text-sm">Historical performance analysis</p>
            </button>
          </div>

        </div>
      </GlassCard>

      {/* Altcoin Charts Section */}
      <GlassCard className="p-3 sm:p-4 lg:p-6">
        <div className="space-y-3 sm:space-y-4">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-5 h-5 sm:w-6 sm:h-6 bg-purple-500 rounded-full flex items-center justify-center">
              <ExternalLink className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
            </div>
            <h3 className="text-lg sm:text-xl font-semibold text-white">Altcoins</h3>
            <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30 text-xs">
              MARKET INTELLIGENCE
            </Badge>
          </div>

          {/* Row 1: Open Interest - Full Width */}
          <div className="grid grid-cols-1 gap-3 sm:gap-4">
            <button
              onClick={() => openInNewTab('https://coinalyze.net/futures-data/global-charts/')}
              className="p-3 sm:p-4 bg-red-500/10 border border-red-500/20 rounded-lg hover:bg-red-500/20 transition-colors"
            >
              <h5 className="text-red-400 font-semibold mb-1 text-sm sm:text-base">Open Interest</h5>
              <p className="text-white/35 text-xs sm:text-sm">Futures Data</p>
            </button>
          </div>

          {/* Row 2: SoSo Value + Messari */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 sm:gap-4">
            <button
              onClick={() => openInNewTab('https://sosovalue.com/assets/cryptoindex')}
              className="p-3 sm:p-4 bg-cyan-500/10 border border-cyan-500/20 rounded-lg hover:bg-cyan-500/20 transition-colors"
            >
              <h5 className="text-cyan-400 font-semibold mb-1 text-sm sm:text-base">SoSo Value</h5>
              <p className="text-white/35 text-xs sm:text-sm">Crypto Index</p>
            </button>

            <button
              onClick={() => openInNewTab('https://messari.io/')}
              className="p-3 sm:p-4 bg-indigo-500/10 border border-indigo-500/20 rounded-lg hover:bg-indigo-500/20 transition-colors"
            >
              <h5 className="text-indigo-400 font-semibold mb-1 text-sm sm:text-base">Messari</h5>
              <p className="text-white/35 text-xs sm:text-sm">Crypto Research</p>
            </button>
          </div>
        </div>
      </GlassCard>

      {/* Coinglass Bull Market Peak Signals */}
      <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
        <div className="flex justify-end p-3">
          <button
            onClick={() => openInNewTab('https://www.coinglass.com/bull-market-peak-signals')}
            className="text-xs text-green-400 hover:text-green-300 transition-colors flex items-center gap-1"
          >
            Open Full View <ExternalLink className="w-3 h-3" />
          </button>
        </div>
        <iframe
          src="https://www.coinglass.com/bull-market-peak-signals"
          className="w-full h-[400px] sm:h-[500px] lg:h-[600px]"
          title="Coinglass Bull Market Peak Signals"
          sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
          referrerPolicy="no-referrer-when-downgrade"
        />
      </div>
    </div>
  );
}