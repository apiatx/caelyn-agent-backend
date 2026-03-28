import React, { useState } from 'react';
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, Activity, BarChart3, Brain, Wallet, Zap, DollarSign, Layers, ChartLine, Settings } from "lucide-react";
import hippoImage from "@assets/Gls1Y3XG_400x400_1755979622876.jpg";
import newHeaderBackground from "@assets/photo-1504333638930-c8787321eee0_1757208194192.avif";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { useScrollFade } from "@/hooks/useScrollFade";



export default function BTCPage() {
  const headerOpacity = useScrollFade(30, 120);
  
  const openInNewTab = (url: string) => {
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Header */}
      <header 
        className="glass-card-dark border-b border-white/[0.06] sticky top-0 z-50 transition-opacity duration-300 relative overflow-hidden" 
        style={{ opacity: headerOpacity, pointerEvents: headerOpacity < 0.1 ? 'none' : 'auto' }}
      >
        {/* Background Image */}
        <div 
          className="absolute inset-0 opacity-75"
          style={{
            backgroundImage: `url(${newHeaderBackground})`,
            backgroundSize: 'cover',
            backgroundPosition: 'center',
            backgroundRepeat: 'no-repeat'
          }}
        />
        {/* Content Layer */}
        <div className="relative z-10 max-w-[95vw] mx-auto px-2 sm:px-3">
          <div className="flex justify-between items-center py-3 lg:py-4">
            <div className="flex items-center space-x-3 sm:space-x-4">
              <div className="w-16 h-16 sm:w-20 sm:h-20 lg:w-24 lg:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-lg">
                <img 
                  src={hippoImage}
                  alt="CryptoHippo"
                  className="w-full h-full object-cover"
                />
              </div>
              <h1 className="text-2xl sm:text-3xl lg:text-4xl xl:text-5xl font-bold text-white drop-shadow-lg">
                CryptoHippo
              </h1>
            </div>
            <div className="flex items-center space-x-2 sm:space-x-4">
              <GlassCard className="px-2 py-1 sm:px-3 sm:py-2 hidden sm:block">
                <span className="text-xs sm:text-sm text-crypto-silver">Portfolio Value</span>
                <div className="text-sm sm:text-lg font-semibold text-crypto-success">$127,845.32</div>
              </GlassCard>
              <GlassCard className="px-2 py-2 sm:hidden">
                <div className="text-sm font-semibold text-crypto-success">$127.8K</div>
              </GlassCard>
              <Button variant="ghost" size="sm" className="p-1 sm:p-2">
                <Settings className="h-3 w-3 sm:h-4 sm:w-4" />
              </Button>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="max-w-[95vw] mx-auto px-2 sm:px-3 mt-4">
        <GlassCard className="p-1 sm:p-2">
          {/* Desktop Navigation */}
          <div className="hidden lg:flex space-x-2">
            <button
              onClick={() => window.location.href = "/"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <Activity className="w-4 h-4 mr-2 inline" />Market Overview
            </button>
            <button
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 bg-gradient-to-r from-white/[0.06] to-white/[0.03] border border-white/[0.08] text-white"
            >
              <TrendingUp className="w-4 h-4 mr-2 inline" />BTC
            </button>
            <button
              onClick={() => window.location.href = "/alts"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <BarChart3 className="w-4 h-4 mr-2 inline" />Alts
            </button>
            <button
              onClick={() => window.location.href = "/#alpha"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <TrendingUp className="w-4 h-4 mr-2 inline" />Alpha
            </button>
            <button
              onClick={() => window.location.href = "/#base"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <ChartLine className="w-4 h-4 mr-2 inline" />Base
            </button>
            <button
              onClick={() => window.location.href = "/#bittensor"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <Brain className="w-4 h-4 mr-2 inline" />Bittensor
            </button>
            <button
              onClick={() => window.location.href = "/hype"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <TrendingUp className="w-4 h-4 mr-2 inline" />Hype
            </button>
            <button
              onClick={() => window.location.href = "/#abstract"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <Layers className="w-4 h-4 mr-2 inline" />Abstract
            </button>
            <button
              onClick={() => window.location.href = "/#solana"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <Zap className="w-4 h-4 mr-2 inline" />Solana
            </button>
            <button
              onClick={() => window.location.href = "/#defi"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <DollarSign className="w-4 h-4 mr-2 inline" />DeFi
            </button>
            <button
              onClick={() => window.location.href = "/#portfolio"}
              className="flex-1 py-3 px-6 rounded-xl font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
            >
              <Wallet className="w-4 h-4 mr-2 inline" />Portfolio
            </button>
          </div>

          {/* Mobile Navigation - Horizontal Scroll */}
          <div className="lg:hidden overflow-x-auto">
            <div className="flex space-x-1 min-w-max pb-2">
              <button
                onClick={() => window.location.href = "/"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <Activity className="w-4 h-4 mr-1 inline" />Market Overview
              </button>
              <button
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 bg-gradient-to-r from-white/[0.06] to-white/[0.03] border border-white/[0.08] text-white"
              >
                <TrendingUp className="w-4 h-4 mr-1 inline" />BTC
              </button>
              <button
                onClick={() => window.location.href = "/alts"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <BarChart3 className="w-4 h-4 mr-1 inline" />Alts
              </button>
              <button
                onClick={() => window.location.href = "/#alpha"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <TrendingUp className="w-4 h-4 mr-1 inline" />Alpha
              </button>
              <button
                onClick={() => window.location.href = "/#base"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <ChartLine className="w-4 h-4 mr-1 inline" />Base
              </button>
              <button
                onClick={() => window.location.href = "/#bittensor"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <Brain className="w-4 h-4 mr-1 inline" />Bittensor
              </button>
              <button
                onClick={() => window.location.href = "/hype"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <TrendingUp className="w-4 h-4 mr-1 inline" />Hype
              </button>
              <button
                onClick={() => window.location.href = "/#abstract"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <Layers className="w-4 h-4 mr-1 inline" />Abstract
              </button>
              <button
                onClick={() => window.location.href = "/#solana"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <Zap className="w-4 h-4 mr-1 inline" />Solana
              </button>
              <button
                onClick={() => window.location.href = "/#defi"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <DollarSign className="w-4 h-4 mr-1 inline" />DeFi
              </button>
              <button
                onClick={() => window.location.href = "/#portfolio"}
                className="whitespace-nowrap py-2 px-3 rounded-lg text-sm font-medium transition-all duration-300 hover:bg-white/5 text-crypto-silver"
              >
                <Wallet className="w-4 h-4 mr-1 inline" />Portfolio
              </button>
            </div>
          </div>
        </GlassCard>
      </nav>

      {/* Content */}
      <div className="max-w-[95vw] mx-auto px-2 sm:px-3 mt-4 pb-8">

        <div className="space-y-4 lg:space-y-8">
          <div className="text-center px-3 sm:px-0">
            <h2 className="text-xl sm:text-2xl lg:text-3xl font-bold text-white mb-2">Bitcoin Analytics</h2>
            <p className="text-sm sm:text-base text-crypto-silver">Bitcoin price action and dominance analysis</p>
          </div>

          {/* Bitcoin Charts - Side by Side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
            {/* Bitcoin Price Chart */}
            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 sm:w-6 sm:h-6 bg-orange-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                  </div>
                  <h3 className="text-lg sm:text-xl font-semibold text-white">Bitcoin Price Chart</h3>
                  <Badge className="bg-orange-500/20 text-orange-400 border-orange-500/30 text-xs">
                    BITCOIN
                  </Badge>
                </div>
                <button
                  onClick={() => openInNewTab('https://www.tradingview.com/chart/?symbol=BTCUSD')}
                  className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm sm:ml-auto"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="w-full">
                <iframe
                  src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_btc&symbol=BINANCE%3ABTCUSDT&interval=1D&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=BINANCE%3ABTCUSDT"
                  className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                  title="Bitcoin Chart"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                />
              </div>
            </GlassCard>

            {/* BTC Dominance Chart */}
            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 sm:w-6 sm:h-6 bg-yellow-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                  </div>
                  <h3 className="text-lg sm:text-xl font-semibold text-white">BTC Dominance</h3>
                  <Badge className="bg-yellow-500/20 text-yellow-400 border-yellow-500/30 text-xs">
                    BTC.D
                  </Badge>
                </div>
                <button
                  onClick={() => openInNewTab('https://www.tradingview.com/chart/e5l95XgZ/?symbol=CRYPTOCAP%3ABTC.D')}
                  className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm sm:ml-auto"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="w-full">
                <iframe
                  src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_btcd&symbol=CRYPTOCAP%3ABTC.D&interval=1D&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=CRYPTOCAP%3ABTC.D"
                  className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                  title="BTC Dominance Chart"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                />
              </div>
            </GlassCard>
          </div>

          {/* Ethereum Charts - Side by Side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
            {/* Ethereum Price Chart */}
            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 sm:w-6 sm:h-6 bg-blue-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                  </div>
                  <h3 className="text-lg sm:text-xl font-semibold text-white">Ethereum Price Chart</h3>
                  <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
                    ETHEREUM
                  </Badge>
                </div>
                <button
                  onClick={() => openInNewTab('https://www.tradingview.com/chart/?symbol=ETHUSD')}
                  className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm sm:ml-auto"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="w-full">
                <iframe
                  src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_eth&symbol=BINANCE%3AETHUSDT&interval=1D&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=BINANCE%3AETHUSDT"
                  className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                  title="Ethereum Chart"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                />
              </div>
            </GlassCard>

            {/* ETH Dominance Chart */}
            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 sm:w-6 sm:h-6 bg-purple-500 rounded-full flex items-center justify-center">
                    <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                  </div>
                  <h3 className="text-lg sm:text-xl font-semibold text-white">ETH Dominance</h3>
                  <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30 text-xs">
                    ETH.D
                  </Badge>
                </div>
                <button
                  onClick={() => openInNewTab('https://www.tradingview.com/chart/?symbol=CRYPTOCAP%3AETH.D')}
                  className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm sm:ml-auto"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="w-full">
                <iframe
                  src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_ethd&symbol=CRYPTOCAP%3AETH.D&interval=1D&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=CRYPTOCAP%3AETH.D"
                  className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                  title="ETH Dominance Chart"
                  sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
                />
              </div>
            </GlassCard>
          </div>

          {/* OTHERS Dominance Chart */}
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
              <div className="flex items-center gap-2">
                <div className="w-5 h-5 sm:w-6 sm:h-6 bg-green-500 rounded-full flex items-center justify-center">
                  <TrendingUp className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                </div>
                <h3 className="text-lg sm:text-xl font-semibold text-white">OTHERS Dominance</h3>
                <Badge className="bg-green-500/20 text-green-400 border-green-500/30 text-xs">
                  OTHERS.D
                </Badge>
              </div>
              <button
                onClick={() => openInNewTab('https://www.tradingview.com/chart/?symbol=CRYPTOCAP%3AOTHERS.D')}
                className="text-green-400 hover:text-green-300 text-xs sm:text-sm sm:ml-auto"
              >
                Open in New Tab →
              </button>
            </div>
            <div className="w-full">
              <iframe
                src="https://s.tradingview.com/widgetembed/?frameElementId=tradingview_othersd&symbol=CRYPTOCAP%3AOTHERS.D&interval=1D&hidesidetoolbar=0&hidetoptoolbar=0&symboledit=1&saveimage=1&toolbarbg=0a0a0a&studies=[]&hideideas=1&theme=dark&style=1&timezone=Etc%2FUTC&studies_overrides={}&overrides={}&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22,%22timeframes_toolbar%22,%22show_interval_dialog_on_key_press%22]&disabled_features=[]&locale=en&utm_source=cryptohippo.com&utm_medium=widget&utm_campaign=chart&utm_term=CRYPTOCAP%3AOTHERS.D"
                className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                title="OTHERS Dominance Chart"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
              />
            </div>
          </GlassCard>
        </div>
      </div>
    </div>
  );
}