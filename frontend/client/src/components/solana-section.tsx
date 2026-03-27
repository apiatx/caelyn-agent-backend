import React from 'react';
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { BarChart3, Activity, Zap, TrendingDown, TrendingUp, ExternalLink, Star } from "lucide-react";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";
// Glass card component for Solana section
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function SolanaSection() {
  return (
    <div className="space-y-8">
      {/* Solana Price Chart */}
      <div className="space-y-3">
        <div className="flex items-center justify-end gap-4">
          <button
            onClick={() => openSecureLink('https://coinmarketcap.com/currencies/solana/')}
            className="flex items-center gap-1 text-purple-400 hover:text-purple-300 text-xs transition-colors"
          >
            <span>CoinMarketCap</span>
            <ExternalLink className="w-3 h-3" />
          </button>
          <button
            onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ASOLUSDT')}
            className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm"
          >
            Open Full View →
          </button>
        </div>
        <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
          <iframe
            src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ASOLUSDT"
            className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
            title="Solana Advanced Chart"
            frameBorder="0"
            scrolling="no"
          />
        </div>
      </div>

      {/* DexScreener Solana */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 bg-purple-500 rounded-full flex items-center justify-center">
              <BarChart3 className="w-4 h-4 text-white" />
            </div>
            <h3 className="text-xl font-semibold text-white">Trending</h3>
            <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30">
              LIVE CHARTS
            </Badge>
          </div>
          <button
            onClick={() => openSecureLink('https://dexscreener.com/solana')}
            className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm"
          >
            Open Full View →
          </button>
        </div>
        <div className="w-full space-y-4">
          {/* Moby Screener Button */}
          <button
            onClick={() => openSecureLink("https://www.mobyscreener.com/")}
            className="w-full p-4 bg-blue-500/10 border border-blue-500/20 rounded-lg hover:bg-blue-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-blue-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-blue-400 font-semibold">Moby Screener</h4>
            </div>
            <p className="text-white/35 text-sm">Advanced Solana token analytics and screening tools</p>
          </button>
          
          <button
            onClick={() => openSecureLink("https://dexscreener.com/solana?theme=dark")}
            className="w-full p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg hover:bg-purple-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-purple-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-purple-400 font-semibold">DexScreener</h4>
            </div>
            <p className="text-white/35 text-sm">Live trending Solana tokens and pair analytics with real-time charts</p>
          </button>
          
          {/* Birdeye Solana */}
          <button
            onClick={() => openSecureLink("https://birdeye.so/solana")}
            className="w-full p-4 bg-orange-500/10 border border-orange-500/20 rounded-lg hover:bg-orange-500/20 transition-colors text-left"
          >
            <div className="flex items-center gap-2 mb-2">
              <div className="w-8 h-8 bg-orange-500 rounded-full flex items-center justify-center">
                <ExternalLink className="w-4 h-4 text-white" />
              </div>
              <h4 className="text-orange-400 font-semibold">Birdeye</h4>
            </div>
            <p className="text-white/35 text-sm">Advanced Solana token analytics with comprehensive market data</p>
          </button>
          
          {/* 30 Day Trending on OpenSea */}
          <button
              onClick={() => openSecureLink('https://opensea.io/stats/tokens?sortBy=thirtyDayPriceChange&chains=solana')}
              className="w-full p-4 bg-cyan-500/10 border border-cyan-500/20 rounded-lg hover:bg-cyan-500/20 transition-colors text-left"
            >
              <div className="flex items-center gap-2 mb-2">
                <div className="w-8 h-8 bg-cyan-500 rounded-full flex items-center justify-center">
                  <ExternalLink className="w-4 h-4 text-white" />
                </div>
                <h4 className="text-cyan-400 font-semibold">OpenSea</h4>
              </div>
              <p className="text-white/35 text-sm">Trending Solana tokens by 30-day price changes</p>
            </button>
        </div>
      </GlassCard>





      {/* Trade Section */}
      <div className="space-y-6">
        <div className="flex items-center gap-3 mb-6">
          <div className="w-10 h-10 bg-gradient-to-r from-green-500 to-emerald-500 rounded-xl flex items-center justify-center">
            <Zap className="text-white text-xl" />
          </div>
          <div>
            <h2 className="text-2xl font-bold text-white">Trade</h2>
            <p className="text-crypto-silver">DEX Aggregators & Trading Platforms</p>
          </div>
        </div>

        {/* Jupiter */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-green-500 rounded-full flex items-center justify-center">
              <Zap className="w-4 h-4 text-white" />
            </div>
            <h3 className="text-xl font-semibold text-white">Jupiter</h3>
            <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
              DEX TRADING
            </Badge>
          </div>
          <div className="w-full">
            <iframe
              src="https://jup.ag/?utm_source=phantom&utm_medium=list"
              className="w-full h-[600px] rounded-lg border border-white/[0.06]"
              title="Jupiter DEX Aggregator"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
            />
          </div>
        </GlassCard>

        {/* Raydium */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-blue-500 rounded-full flex items-center justify-center">
              <Activity className="w-4 h-4 text-white" />
            </div>
            <h3 className="text-xl font-semibold text-white">Raydium</h3>
            <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30">
              AMM & DEX
            </Badge>
          </div>
          <div className="w-full">
            <iframe
              src="https://raydium.io/swap/?inputMint=sol&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
              className="w-full h-[600px] rounded-lg border border-white/[0.06]"
              title="Raydium"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
            />
          </div>
        </GlassCard>

        {/* Uranus */}
        <GlassCard className="p-6">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-6 h-6 bg-purple-500 rounded-full flex items-center justify-center">
              <Activity className="w-4 h-4 text-white" />
            </div>
            <h3 className="text-xl font-semibold text-white">Uranus</h3>
            <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30">
              DEX TRADING
            </Badge>
          </div>
          <div className="w-full">
            <iframe
              src="https://uranus.ag/trade/?token=BFgdzMkTPdKKJeTipv2njtDEwhKxkgFueJQfJGt1jups"
              className="w-full h-[600px] rounded-lg border border-white/[0.06]"
              title="Uranus DEX"
              sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
            />
          </div>
        </GlassCard>
      </div>

      {/* Trenches - Moved from Trench section and renamed */}
      <GlassCard className="p-6">
        <div className="flex items-center gap-2 mb-6">
          <div className="w-6 h-6 bg-gradient-to-r from-red-500 to-orange-500 rounded-full flex items-center justify-center">
            <TrendingDown className="w-4 h-4 text-white" />
          </div>
          <h3 className="text-xl font-semibold text-white">Trenches</h3>
          <Badge className="bg-gradient-to-r from-red-500/20 to-orange-500/20 text-red-400 border-red-500/30">
            TRADING LEADERS
          </Badge>
        </div>
        
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          <a
            href="https://web3.okx.com/leaderboard"
            onClick={(e) => {e.preventDefault(); openSecureLink('https://web3.okx.com/leaderboard');}}
            className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-red-500/30 transition-all duration-200 group cursor-pointer"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-gradient-to-r from-red-500 to-orange-500 rounded-full flex items-center justify-center">
                <TrendingDown className="w-4 h-4 text-white" />
              </div>
              <div>
                <h3 className="text-white font-medium text-sm">OKX Leaderboard</h3>
                <p className="text-white/35 text-xs">Trading rankings</p>
              </div>
            </div>
            <ExternalLink className="w-4 h-4 text-white/35 group-hover:text-red-400 transition-colors" />
          </a>

          <a
            href="https://bonk.fun"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-orange-500/30 transition-all duration-200 group"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-gradient-to-r from-orange-500 to-yellow-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold text-sm">B</span>
              </div>
              <div>
                <h3 className="text-white font-medium text-sm">BONK.fun</h3>
                <p className="text-white/35 text-xs">Token platform</p>
              </div>
            </div>
            <ExternalLink className="w-4 h-4 text-white/35 group-hover:text-orange-400 transition-colors" />
          </a>

          <a
            href="https://pump.fun"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-green-500/30 transition-all duration-200 group"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-gradient-to-r from-green-500 to-emerald-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold text-sm">P</span>
              </div>
              <div>
                <h3 className="text-white font-medium text-sm">Pump.fun</h3>
                <p className="text-white/35 text-xs">Launch platform</p>
              </div>
            </div>
            <ExternalLink className="w-4 h-4 text-white/35 group-hover:text-green-400 transition-colors" />
          </a>

          <a
            href="https://believe.app"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center justify-between p-4 bg-black/20 border border-white/[0.06] rounded-lg hover:bg-black/30 hover:border-blue-500/30 transition-all duration-200 group"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold text-sm">B</span>
              </div>
              <div>
                <h3 className="text-white font-medium text-sm">Believe.app</h3>
                <p className="text-white/35 text-xs">Trading app</p>
              </div>
            </div>
            <ExternalLink className="w-4 h-4 text-white/35 group-hover:text-blue-400 transition-colors" />
          </a>
        </div>
      </GlassCard>


      {/* Ecosystem */}
      <GlassCard className="p-6">
        <div className="flex items-center gap-2 mb-4">
          <div className="w-6 h-6 bg-gradient-to-r from-purple-500 to-indigo-500 rounded-full flex items-center justify-center">
            <span className="text-white text-xs font-bold">E</span>
          </div>
          <h3 className="text-xl font-semibold text-white">Ecosystem</h3>
          <span className="px-2 py-1 bg-purple-500/20 text-purple-400 text-xs rounded-full font-medium">
            ECOSYSTEM GUIDE
          </span>
        </div>
        <div className="p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg">
          <button
            onClick={() => openSecureLink('https://academy.swissborg.com/en/learn/solana-ecosystem')}
            className="w-full text-left hover:bg-purple-500/20 transition-colors rounded-lg p-3"
          >
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-purple-500 rounded-full flex items-center justify-center">
                <span className="text-white font-bold text-xs">S</span>
              </div>
              <div>
                <p className="text-purple-400 font-medium">Solana Ecosystem</p>
                <p className="text-sm text-white/35">SwissBorg Academy ecosystem guide</p>
              </div>
            </div>
          </button>
        </div>
      </GlassCard>
    </div>
  );
}