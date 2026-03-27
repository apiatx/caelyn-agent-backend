import React, { Suspense, useEffect, useRef } from 'react';
import { Badge } from "@/components/ui/badge";
import { ExternalLink } from "lucide-react";
import { SectionLoadingState } from "@/components/loading-screen";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";
import TopDailyGainers from "@/components/top-daily-gainers";
import CryptoMarketDataBare from "@/components/crypto-market-data-bare";
import solanaLogo from "@assets/solana_1755977414939.png";
import hyperliquidLogo from "@assets/hyperliquid-logo_1755977414943.png";
import bittensorLogo from "@assets/bittensor_1755977414942.png";
import bnbLogo from "@assets/eb2349c3-b2f8-4a93-a286-8f86a62ea9d8_1757138768380.png";
import suiLogo from "@assets/images (2)_1757139042170.jpeg";
import dogecoinLogo from "@assets/dogecoin-doge-cryptocurrency-golden-currency-600nw-2293972845_1757205415591.webp";

function CryptoTickerTape() {
  const containerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!containerRef.current) return;
    const iframe = document.createElement("iframe");
    iframe.style.width = "100%";
    iframe.style.height = "100%";
    iframe.style.border = "none";
    iframe.setAttribute("sandbox", "allow-scripts allow-same-origin allow-popups");
    containerRef.current.appendChild(iframe);
    const doc = iframe.contentDocument;
    if (doc) {
      doc.open();
      doc.write(`<!DOCTYPE html><html><head><style>body{margin:0;padding:0;overflow:hidden;background:transparent;}</style></head><body><script type="module" src="https://widgets.tradingview-widget.com/w/en/tv-ticker-tape.js"><\/script><tv-ticker-tape symbols='BITSTAMP:BTCUSD,BITSTAMP:ETHUSD,CRYPTOCAP:XRP,CRYPTOCAP:BNB,CRYPTOCAP:SOL,CRYPTO:TRXUSD,CRYPTOCAP:DOGE,CRYPTO:HYPEHUSD,CRYPTOCAP:LINK,CRYPTOCAP:XMR,CRYPTOCAP:XLM,CRYPTOCAP:ZEC,CRYPTOCAP:HBAR,CRYPTOCAP:LTC,CRYPTOCAP:SUI,COINBASE:TAOUSD' theme="dark" transparent></tv-ticker-tape></body></html>`);
      doc.close();
    }
    return () => {
      if (containerRef.current && iframe.parentNode === containerRef.current) {
        containerRef.current.removeChild(iframe);
      }
    };
  }, []);
  return <div ref={containerRef} className="w-full" style={{ height: 78 }} />;
}

export default function AltcoinsPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="sticky top-0 z-50 border-b border-white/[0.06] bg-black/90 backdrop-blur-lg">
        <CryptoTickerTape />
      </div>

      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-6 lg:space-y-8">
          <div className="text-center px-3 sm:px-0">
            <div className="flex items-center justify-center gap-3 mb-2">
              {/* Left side - 3 images */}
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-orange-400/50 shadow-lg">
                  <img src={dogecoinLogo} alt="Dogecoin" className="w-full h-full object-cover" />
                </div>
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-purple-400/50 shadow-lg">
                  <img src={solanaLogo} alt="Solana" className="w-full h-full object-cover" />
                </div>
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-emerald-400/50 shadow-lg">
                  <img src={hyperliquidLogo} alt="Hyperliquid" className="w-full h-full object-cover" />
                </div>
              </div>
              
              {/* Center text */}
              <h2 className="text-xl sm:text-2xl lg:text-3xl font-bold text-white mx-4">Altcoins</h2>
              
              {/* Right side - 3 images */}
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-red-400/50 shadow-lg">
                  <img src={bittensorLogo} alt="Bittensor" className="w-full h-full object-cover" />
                </div>
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-yellow-400/50 shadow-lg">
                  <img src={bnbLogo} alt="BNB" className="w-full h-full object-cover" />
                </div>
                <div className="w-8 h-8 rounded-full overflow-hidden border-2 border-blue-400/50 shadow-lg">
                  <img src={suiLogo} alt="SUI" className="w-full h-full object-cover" />
                </div>
              </div>
            </div>
            <p className="text-sm sm:text-base text-crypto-silver">Alternative cryptocurrencies and emerging market opportunities</p>
          </div>

          {/* Daily Gainers */}
          <div className="space-y-4">
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-4 lg:p-6 transform transition-all duration-500 hover:scale-[1.01] hover:shadow-2xl hover:shadow-crypto-warning/20">
              <div className="flex items-center justify-center gap-2 mb-4">
                <h3 className="text-lg font-semibold text-white text-center">📈 24h Gainers (CMC Top 100)</h3>
                <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
                  LIVE
                </Badge>
              </div>
              <Suspense fallback={<SectionLoadingState />}>
                <TopDailyGainers />
              </Suspense>
            </div>
          </div>

          {/* XRP Chart */}
          <Suspense fallback={<SectionLoadingState title="XRP Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-white/35">XRP</h3>
                  <Badge variant="outline" className="bg-gray-500/20 text-white/45 border-gray-500/30">
                    XRP
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/xrp/')}
                    className="text-white/35 hover:text-white/45 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BITSTAMP%3AXRPUSD')}
                    className="text-white/35 hover:text-white/45 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BITSTAMP%3AXRPUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="XRP Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* XRP Real-time Data */}
              <CryptoMarketDataBare symbol="XRP" />
            </div>
          </Suspense>

          {/* BNB Chart */}
          <Suspense fallback={<SectionLoadingState title="BNB Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-yellow-400">BNB</h3>
                  <Badge variant="outline" className="bg-yellow-500/20 text-yellow-300 border-yellow-500/30">
                    BNB
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/bnb/')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ABNBUSDT')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ABNBUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="BNB Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* BNB Real-time Data */}
              <CryptoMarketDataBare symbol="BNB" />
            </div>
          </Suspense>

          {/* Solana Chart */}
          <Suspense fallback={<SectionLoadingState title="Solana Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-purple-400">Solana</h3>
                  <Badge variant="outline" className="bg-purple-500/20 text-purple-300 border-purple-500/30">
                    SOL
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/solana/')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ASOLUSDT')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
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
              
              {/* Solana Real-time Data */}
              <CryptoMarketDataBare symbol="SOL" />
            </div>
          </Suspense>

          {/* DOGE Chart */}
          <Suspense fallback={<SectionLoadingState title="DOGE Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-orange-400">DOGE</h3>
                  <Badge variant="outline" className="bg-orange-500/20 text-orange-300 border-orange-500/30">
                    DOGE
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/dogecoin/')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ADOGEUSDT')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ADOGEUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="DOGE Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* DOGE Real-time Data */}
              <CryptoMarketDataBare symbol="DOGE" />
            </div>
          </Suspense>

          {/* CHAINLINK Chart */}
          <Suspense fallback={<SectionLoadingState title="CHAINLINK Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-blue-400">Chainlink</h3>
                  <Badge variant="outline" className="bg-blue-500/20 text-blue-300 border-blue-500/30">
                    LINK
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/chainlink/')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ALINKUSDT')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ALINKUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="CHAINLINK Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Chainlink Real-time Data */}
              <CryptoMarketDataBare symbol="LINK" />
            </div>
          </Suspense>

          {/* Hyperliquid Chart */}
          <Suspense fallback={<SectionLoadingState title="Hyperliquid Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-cyan-400">Hyperliquid</h3>
                  <Badge variant="outline" className="bg-cyan-500/20 text-cyan-300 border-cyan-500/30">
                    HYPE
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/hyperliquid/')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=PYTH%3AHYPEUSD')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=PYTH%3AHYPEUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="Hyperliquid Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Hyperliquid Real-time Data */}
              <CryptoMarketDataBare symbol="HYPE" />
            </div>
          </Suspense>

          {/* SUI Chart */}
          <Suspense fallback={<SectionLoadingState title="SUI Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-green-400">SUI</h3>
                  <Badge variant="outline" className="bg-green-500/20 text-green-300 border-green-500/30">
                    SUI
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/sui/')}
                    className="text-green-400 hover:text-green-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3ASUIUSDT')}
                    className="text-green-400 hover:text-green-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ASUIUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="SUI Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* SUI Real-time Data */}
              <CryptoMarketDataBare symbol="SUI" />
            </div>
          </Suspense>

          {/* XLM Chart */}
          <Suspense fallback={<SectionLoadingState title="XLM Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-blue-400">Stellar</h3>
                  <Badge variant="outline" className="bg-blue-500/20 text-blue-300 border-blue-500/30">
                    XLM
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/stellar/')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=COINBASE%3AXLMUSD')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=COINBASE%3AXLMUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="XLM Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Stellar Real-time Data */}
              <CryptoMarketDataBare symbol="XLM" />
            </div>
          </Suspense>

          {/* HBAR Chart */}
          <Suspense fallback={<SectionLoadingState title="HBAR Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-green-400">Hedera</h3>
                  <Badge variant="outline" className="bg-green-500/20 text-green-300 border-green-500/30">
                    HBAR
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/hedera-hashgraph/')}
                    className="text-green-400 hover:text-green-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=BINANCE%3AHBARUSDT')}
                    className="text-green-400 hover:text-green-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3AHBARUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="Hedera HBAR Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Hedera Real-time Data */}
              <CryptoMarketDataBare symbol="HBAR" />
            </div>
          </Suspense>

          {/* LTC Chart */}
          <Suspense fallback={<SectionLoadingState title="LTC Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-white/35">Litecoin</h3>
                  <Badge variant="outline" className="bg-gray-500/20 text-white/45 border-gray-500/30">
                    LTC
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/litecoin/')}
                    className="text-white/35 hover:text-white/45 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=COINBASE%3ALTCUSD')}
                    className="text-white/35 hover:text-white/45 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=COINBASE%3ALTCUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="LTC Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Litecoin Real-time Data */}
              <CryptoMarketDataBare symbol="LTC" />
            </div>
          </Suspense>

          {/* XMR Chart */}
          <Suspense fallback={<SectionLoadingState title="XMR Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-orange-400">Monero</h3>
                  <Badge variant="outline" className="bg-orange-500/20 text-orange-300 border-orange-500/30">
                    XMR
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/monero/')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=KRAKEN%3AXMRUSD')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=KRAKEN%3AXMRUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="XMR Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Monero Real-time Data */}
              <CryptoMarketDataBare symbol="XMR" />
            </div>
          </Suspense>

          {/* ENA Chart */}
          <Suspense fallback={<SectionLoadingState title="ENA Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-purple-400">Ethena</h3>
                  <Badge variant="outline" className="bg-purple-500/20 text-purple-300 border-purple-500/30">
                    ENA
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/ethena/')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=BINANCE%3AENAUSDT')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3AENAUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="ENA Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Ethena Real-time Data */}
              <CryptoMarketDataBare symbol="ENA" />
            </div>
          </Suspense>

          {/* Bittensor Chart */}
          <Suspense fallback={<SectionLoadingState title="Bittensor Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-orange-400">Bittensor</h3>
                  <Badge variant="outline" className="bg-orange-500/20 text-orange-300 border-orange-500/30">
                    TAO
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/bittensor/')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BITGET%3ATAOUSDT')}
                    className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BITGET%3ATAOUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="Bittensor Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Bittensor Real-time Data */}
              <CryptoMarketDataBare symbol="TAO" />
            </div>
          </Suspense>

          {/* Ondo Finance Chart */}
          <Suspense fallback={<SectionLoadingState title="Ondo Finance Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-purple-400">Ondo Finance</h3>
                  <Badge variant="outline" className="bg-purple-500/20 text-purple-300 border-purple-500/30">
                    ONDO
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/ondo-finance/')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=BYBIT%3AONDOUSDT')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BYBIT%3AONDOUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="Ondo Finance Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Ondo Finance Real-time Data */}
              <CryptoMarketDataBare symbol="ONDO" />
            </div>
          </Suspense>

          {/* PENGU Chart */}
          <Suspense fallback={<SectionLoadingState title="PENGU Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-cyan-400">PENGU</h3>
                  <Badge variant="outline" className="bg-cyan-500/20 text-cyan-300 border-cyan-500/30">
                    PENGU
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/pudgy-penguins/')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BINANCE%3APENGUUSDT')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3APENGUUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="PENGU Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* PENGU Real-time Data */}
              <CryptoMarketDataBare symbol="PENGU" />
            </div>
          </Suspense>

          {/* SEI Chart */}
          <Suspense fallback={<SectionLoadingState title="SEI Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-red-400">Sei</h3>
                  <Badge variant="outline" className="bg-red-500/20 text-red-300 border-red-500/30">
                    SEI
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/sei/')}
                    className="text-red-400 hover:text-red-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=BINANCE%3ASEIUSDT')}
                    className="text-red-400 hover:text-red-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ASEIUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="SEI Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* SEI Real-time Data */}
              <CryptoMarketDataBare symbol="SEI" />
            </div>
          </Suspense>

          {/* BONK Chart */}
          <Suspense fallback={<SectionLoadingState title="BONK Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-yellow-400">BONK</h3>
                  <Badge variant="outline" className="bg-yellow-500/20 text-yellow-300 border-yellow-500/30">
                    BONK
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/bonk/')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=MEXC%3ABONKUSDT')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=MEXC%3ABONKUSDT"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="BONK Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* BONK Real-time Data */}
              <CryptoMarketDataBare symbol="BONK" />
            </div>
          </Suspense>

          {/* AERO Chart */}
          <Suspense fallback={<SectionLoadingState title="AERO Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-blue-400">Aerodrome Finance</h3>
                  <Badge variant="outline" className="bg-blue-500/20 text-blue-300 border-blue-500/30">
                    AERO
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/aerodrome-finance/')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=COINBASE%3AAEROUSD')}
                    className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=COINBASE%3AAEROUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="AERO Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* Aerodrome Finance Real-time Data */}
              <CryptoMarketDataBare symbol="AERO" />
            </div>
          </Suspense>

          {/* VIRTUAL Chart */}
          <Suspense fallback={<SectionLoadingState title="VIRTUAL Chart" />}>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-2">
                  <h3 className="text-lg font-semibold text-cyan-400">VIRTUAL</h3>
                  <Badge variant="outline" className="bg-cyan-500/20 text-cyan-300 border-cyan-500/30">
                    VIRTUAL
                  </Badge>
                </div>
                <div className="flex items-center space-x-3">
                  <button
                    onClick={() => openSecureLink('https://coinmarketcap.com/currencies/virtuals-protocol/')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm flex items-center gap-1"
                  >
                    <ExternalLink className="w-3 h-3" />
                    CoinMarketCap
                  </button>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=CRYPTO%3AVIRTUALUSD')}
                    className="text-cyan-400 hover:text-cyan-300 text-xs sm:text-sm"
                  >
                    Open Full View →
                  </button>
                </div>
              </div>
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTO%3AVIRTUALUSD"
                  className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                  title="VIRTUAL Advanced Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
              
              {/* VIRTUAL Real-time Data */}
              <CryptoMarketDataBare symbol="VIRTUAL" />
            </div>
          </Suspense>

          {/* Velo Chart */}
          <Suspense fallback={<SectionLoadingState title="Velo Chart" />}>
            <div className="space-y-3">
              <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <h3 className="text-lg font-semibold text-white">SUPERCHARTS</h3>
                  </div>
                </div>
                
                {/* CoinGlass SuperCharts */}
                <div className="space-y-3">
                  <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
                    <iframe
                      src="https://www.coinglass.com/tv/Binance_BTCUSDT"
                      className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                      title="CoinGlass SuperCharts"
                      frameBorder="0"
                      scrolling="no"
                      loading="lazy"
                      sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
                    />
                  </div>
                </div>
                
                {/* Velo Charts Button */}
                <div className="pt-4 border-t border-white/[0.06]">
                  <div className="flex flex-col items-center justify-center text-center space-y-3">
                    <div className="w-12 h-12 bg-gradient-to-r from-green-500 to-blue-500 rounded-full flex items-center justify-center text-sm font-bold">
                      V
                    </div>
                    <div>
                      <h5 className="text-md font-medium text-white mb-1">Velo Charts</h5>
                      <p className="text-crypto-silver text-sm max-w-sm">
                        Access comprehensive altcoin trading charts and analytics.
                      </p>
                    </div>
                    <button
                      onClick={() => window.open('https://velo.xyz/chart', '_blank', 'noopener,noreferrer')}
                      className="bg-gradient-to-r from-green-500 to-blue-500 hover:from-green-600 hover:to-blue-600 text-white px-6 py-2.5 rounded-lg font-medium transition-all duration-200 hover:scale-105 flex items-center gap-2 text-sm"
                    >
                      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                      </svg>
                      View Velo Charts
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </Suspense>



        </div>
      </main>
    </div>
  );
}