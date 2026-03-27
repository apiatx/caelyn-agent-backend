import React, { Suspense, useEffect, useRef } from 'react';
import { Badge } from "@/components/ui/badge";
import { ExternalLink } from "lucide-react";
import bitcoinLogo from "@assets/Bitcoin.svg_1755979187828.webp";
import ethereumLogo from "@assets/Ethereum_logo_2014.svg_1755977414942.png";
import { SectionLoadingState } from "@/components/loading-screen";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";
import CryptoMarketData from "@/components/crypto-market-data";
import CryptoMarketDataBare from "@/components/crypto-market-data-bare";
import TopDailyGainers from "@/components/top-daily-gainers";

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

export default function TopChartsPage() {

  useEffect(() => {
    const scriptId = 'altfins-screener-script';
    if (!document.getElementById(scriptId)) {
      const script = document.createElement('script');
      script.id = scriptId;
      script.type = 'module';
      script.src = 'https://cdn.altfins.com/js/altfins-screener-data-component.js';
      document.body.appendChild(script);
    }
  }, []);

  useEffect(() => {
    const style = document.createElement('style');
    style.textContent = `
      .altfins-clip-wrapper {
        position: relative;
        overflow: hidden;
      }
      .altfins-clip-wrapper altfins-screener-data-component {
        display: block;
        margin-bottom: -30px;
      }
    `;
    document.head.appendChild(style);
    return () => { document.head.removeChild(style); };
  }, []);

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
              <div className="w-8 h-8 sm:w-10 sm:h-10 bg-black rounded-xl flex items-center justify-center">
                <img 
                  src={bitcoinLogo}
                  alt="Bitcoin"
                  className="w-6 h-6 sm:w-8 sm:h-8 object-contain"
                  style={{filter: 'drop-shadow(0 0 4px rgba(255, 165, 0, 0.3))'}}
                />
              </div>
              <h2 className="text-xl sm:text-2xl lg:text-3xl font-bold text-white">Major Cryptocurrencies</h2>
              <div className="w-8 h-8 sm:w-10 sm:h-10 bg-black rounded-xl flex items-center justify-center">
                <img 
                  src={ethereumLogo}
                  alt="Ethereum"
                  className="w-6 h-6 sm:w-8 sm:h-8 object-contain"
                  style={{filter: 'drop-shadow(0 0 4px rgba(70, 130, 180, 0.3))'}}
                />
              </div>
            </div>
            <p className="text-sm sm:text-base text-crypto-silver">Bitcoin and Ethereum price action and market dominance analysis</p>
          </div>

          {/* AltFins Screener Widget */}
          <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6">
            <div 
              className="altfins-clip-wrapper w-full"
              dangerouslySetInnerHTML={{
                __html: `<altfins-screener-data-component symbols='["BTC", "ETH"]' theme='no-border compact dark' valueids='["COIN", "LAST_PRICE", "MACD_BS_SIGNAL", "SMA20_SMA50_BS_SIGNAL", "X_EMA50_CROSS_EMA200", "SHORT_TERM_TREND_CHANGE", "MEDIUM_TERM_TREND_CHANGE", "LONG_TERM_TREND_CHANGE", "PERCENTAGE_ABOVE_FROM_52_WEEK_LOW", "PERCENTAGE_DOWN_FROM_52_WEEK_HIGH", "SMA50_TREND", "SMA200_TREND", "VOLUME_CHANGE", "PRICE_CHANGE_1D", "PRICE_CHANGE_1W", "PRICE_CHANGE_1M", "PRICE_CHANGE_3M", "PRICE_CHANGE_6M", "PRICE_CHANGE_1Y"]' affiliateid='test_id'></altfins-screener-data-component>`
              }}
            />
          </div>

          {/* Crypto Total Market Cap Section */}
          <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6">
            <div className="text-center mb-6">
              <h2 className="text-xl sm:text-2xl font-bold text-white">Crypto Total Market Cap</h2>
            </div>
            {/* Top Row - Market Cap Charts Side by Side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6 mb-6">
              {/* Crypto Total Market Cap Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-yellow-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">$</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">BTC + ALTS</h3>
                    <Badge className="bg-yellow-500/20 text-yellow-400 border-yellow-500/30 text-xs">
                      BTC + ALTS
                    </Badge>
                  </div>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=CRYPTOCAP%3ATOTAL')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm sm:ml-auto"
                  >
                    Open in New Tab →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTOCAP%3ATOTAL"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Crypto Total Market Cap"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>

              {/* Total Market Cap Excluding BTC Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-teal-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">$</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">ALTS</h3>
                    <Badge className="bg-teal-500/20 text-teal-400 border-teal-500/30 text-xs">
                      ALTS
                    </Badge>
                  </div>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/e5l95XgZ/?symbol=CRYPTOCAP%3ATOTAL2')}
                    className="text-teal-400 hover:text-teal-300 text-xs sm:text-sm sm:ml-auto"
                  >
                    Open in New Tab →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTOCAP%3ATOTAL2"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Total Market Cap Excluding BTC"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>
            </div>

            {/* Bottom Row - OTHERS Dominance Chart Full Width */}
            <div>
              <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                <div className="flex items-center gap-2">
                  <div className="w-5 h-5 sm:w-6 sm:h-6 bg-green-500 rounded-full flex items-center justify-center">
                    <span className="text-white font-bold text-xs">%</span>
                  </div>
                  <h3 className="text-lg sm:text-xl font-semibold text-white">OTHERS Dominance</h3>
                  <Badge className="bg-green-500/20 text-green-400 border-green-500/30 text-xs">
                    OTHERS.D
                  </Badge>
                </div>
                <button
                  onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=CRYPTOCAP%3AOTHERS.D')}
                  className="text-green-400 hover:text-green-300 text-xs sm:text-sm sm:ml-auto"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="w-full">
                <iframe
                  src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTOCAP%3AOTHERS.D"
                  className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                  title="OTHERS Dominance Chart"
                  frameBorder="0"
                  scrolling="no"
                />
              </div>
            </div>
          </div>

          {/* Bitcoin Section */}
          <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-6">
            <div className="text-center">
              <div className="flex items-center justify-center gap-3 mb-4">
                <div className="w-8 h-8 sm:w-10 sm:h-10 bg-black rounded-xl flex items-center justify-center">
                  <img 
                    src={bitcoinLogo}
                    alt="Bitcoin"
                    className="w-6 h-6 sm:w-8 sm:h-8 object-contain"
                    style={{filter: 'drop-shadow(0 0 4px rgba(255, 165, 0, 0.3))'}}
                  />
                </div>
                <h2 className="text-xl sm:text-2xl font-bold text-white">Bitcoin</h2>
              </div>
            </div>
            {/* Bitcoin Charts - Side by Side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
              {/* Bitcoin Price Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-orange-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">₿</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">BTC Price Chart</h3>
                  </div>
                  <div className="flex items-center gap-2 sm:ml-auto">
                    <button
                      onClick={() => openSecureLink('https://coinmarketcap.com/currencies/bitcoin/')}
                      className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm flex items-center gap-1"
                    >
                      CMC <ExternalLink className="w-3 h-3" />
                    </button>
                    <span className="text-crypto-silver text-xs">|</span>
                    <button
                      onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=BTCUSD')}
                      className="text-orange-400 hover:text-orange-300 text-xs sm:text-sm"
                    >
                      Open in New Tab →
                    </button>
                  </div>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3ABTCUSDT"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Bitcoin Chart"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>

              {/* BTC Dominance Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-yellow-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">%</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">BTC Dominance</h3>
                    <Badge className="bg-yellow-500/20 text-yellow-400 border-yellow-500/30 text-xs">
                      BTC.D
                    </Badge>
                  </div>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=CRYPTOCAP%3ABTC.D')}
                    className="text-yellow-400 hover:text-yellow-300 text-xs sm:text-sm sm:ml-auto"
                  >
                    Open in New Tab →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTOCAP%3ABTC.D"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="BTC Dominance Chart"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>
            </div>
            
            {/* Bitcoin Real-time Data */}
            <CryptoMarketDataBare symbol="BTC" />
          </div>

          {/* Ethereum Section */}
          <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-6">
            <div className="text-center">
              <div className="flex items-center justify-center gap-3 mb-4">
                <div className="w-8 h-8 sm:w-10 sm:h-10 bg-black rounded-xl flex items-center justify-center">
                  <img 
                    src={ethereumLogo}
                    alt="Ethereum"
                    className="w-6 h-6 sm:w-8 sm:h-8 object-contain"
                    style={{filter: 'drop-shadow(0 0 4px rgba(70, 130, 180, 0.3))'}}
                  />
                </div>
                <h2 className="text-xl sm:text-2xl font-bold text-white">Ethereum</h2>
              </div>
            </div>
            {/* Ethereum Charts - Side by Side */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-6">
              {/* Ethereum Price Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-blue-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">Ξ</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">ETH Price Chart</h3>
                    <Badge className="bg-blue-500/20 text-blue-400 border-blue-500/30 text-xs">
                      ETHEREUM
                    </Badge>
                  </div>
                  <div className="flex items-center gap-2 sm:ml-auto">
                    <button
                      onClick={() => openSecureLink('https://coinmarketcap.com/currencies/ethereum/')}
                      className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm flex items-center gap-1"
                    >
                      CMC <ExternalLink className="w-3 h-3" />
                    </button>
                    <span className="text-crypto-silver text-xs">|</span>
                    <button
                      onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=ETHUSD')}
                      className="text-blue-400 hover:text-blue-300 text-xs sm:text-sm"
                    >
                      Open in New Tab →
                    </button>
                  </div>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=BINANCE%3AETHUSDT"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="Ethereum Chart"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>

              {/* ETH Dominance Chart */}
              <div>
                <div className="flex flex-col sm:flex-row items-start sm:items-center gap-2 mb-3 sm:mb-4">
                  <div className="flex items-center gap-2">
                    <div className="w-5 h-5 sm:w-6 sm:h-6 bg-purple-500 rounded-full flex items-center justify-center">
                      <span className="text-white font-bold text-xs">%</span>
                    </div>
                    <h3 className="text-lg sm:text-xl font-semibold text-white">ETH Dominance</h3>
                    <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30 text-xs">
                      ETH.D
                    </Badge>
                  </div>
                  <button
                    onClick={() => openSecureLink('https://www.tradingview.com/chart/?symbol=CRYPTOCAP%3AETH.D')}
                    className="text-purple-400 hover:text-purple-300 text-xs sm:text-sm sm:ml-auto"
                  >
                    Open in New Tab →
                  </button>
                </div>
                <div className="w-full">
                  <iframe
                    src="https://s.tradingview.com/embed-widget/advanced-chart/?locale=en&width=100%25&height=610&interval=1D&range=3M&style=1&toolbar_bg=0a0a0a&enable_publishing=true&withdateranges=true&hide_side_toolbar=false&allow_symbol_change=true&calendar=false&studies=%5B%5D&theme=dark&timezone=Etc%2FUTC&hide_top_toolbar=false&disabled_features=[%22volume_force_overlay%22,%22create_volume_indicator_by_default%22]&enabled_features=[%22use_localstorage_for_settings%22,%22study_templates%22,%22header_indicators%22,%22header_compare%22,%22header_undo_redo%22,%22header_screenshot%22,%22header_chart_type%22,%22header_settings%22,%22header_resolutions%22,%22header_fullscreen_button%22,%22left_toolbar%22,%22drawing_templates%22]&symbol=CRYPTOCAP%3AETH.D"
                    className="w-full h-[400px] sm:h-[500px] lg:h-[600px] rounded-lg border border-white/[0.06]"
                    title="ETH Dominance Chart"
                    frameBorder="0"
                    scrolling="no"
                  />
                </div>
              </div>
            </div>
            
            {/* Ethereum Real-time Data */}
            <CryptoMarketDataBare symbol="ETH" />
          </div>

          {/* Monthly Returns */}
          <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-3 sm:p-4 lg:p-6 space-y-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-2">
                <h3 className="text-lg font-semibold text-emerald-400">Monthly Returns</h3>
                <Badge variant="outline" className="bg-emerald-500/20 text-emerald-300 border-emerald-500/30">
                  RETURNS
                </Badge>
              </div>
              <div className="flex items-center space-x-3">
                <button
                  onClick={() => openSecureLink('https://www.coinglass.com/today')}
                  className="text-emerald-400 hover:text-emerald-300 text-xs sm:text-sm flex items-center gap-1"
                >
                  <ExternalLink className="w-3 h-3" />
                  Open Full View →
                </button>
              </div>
            </div>
            <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl overflow-hidden">
              <iframe
                src="https://www.coinglass.com/today"
                className="w-full h-[600px] sm:h-[800px] lg:h-[900px] rounded-lg border border-white/[0.06]"
                title="CoinGlass Monthly Returns"
                frameBorder="0"
                scrolling="yes"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox allow-storage-access-by-user-activation"
              />
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}