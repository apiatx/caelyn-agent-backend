import { useEffect, useRef, memo } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Activity, Newspaper, Calendar } from "lucide-react";
import TickerTapeWidget from "@/components/TickerTapeWidget";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

const MarketOverviewWidget = memo(function MarketOverviewWidget({ config }: { config: object }) {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-market-overview.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify(config);
    container.current.appendChild(script);
  }, []);
  return (
    <div className="tradingview-widget-container" ref={container} style={{ width: "100%", height: "100%" }}>
      <div className="tradingview-widget-container__widget" style={{ width: "100%", height: "100%" }}></div>
    </div>
  );
});

const indicesConfig = {
  "title": "Indices",
  "tabs": [
    {"title": "US & Canada", "title_raw": "US & Canada", "symbols": [{"s": "FOREXCOM:SPXUSD", "d": "S&P 500"}, {"s": "FOREXCOM:NSXUSD", "d": "US 100"}, {"s": "BMFBOVESPA:ISP1!", "d": "S&P 500"}, {"s": "INDEX:DXY", "d": "U.S. Dollar Currency Index"}, {"s": "FOREXCOM:DJI", "d": "Dow 30"}]},
    {"title": "Europe", "title_raw": "Europe", "symbols": [{"s": "INDEX:SX5E", "d": "Euro Stoxx 50"}, {"s": "FOREXCOM:UKXGBP", "d": "UK 100"}, {"s": "INDEX:DEU40", "d": "DAX Index"}, {"s": "INDEX:CAC40", "d": "CAC 40 Index"}, {"s": "INDEX:SMI", "d": "SWISS MARKET INDEX SMI\u00ae PRICE"}]},
    {"title": "Asia/Pacific", "title_raw": "Asia/Pacific", "symbols": [{"s": "INDEX:NKY", "d": "Nikkei 225"}, {"s": "INDEX:HSI", "d": "Hang Seng"}, {"s": "BSE:SENSEX", "d": "Sensex"}, {"s": "BSE:BSE500", "d": "S&P BSE 500 INDEX"}, {"s": "INDEX:KSIC", "d": "Kospi Composite"}]}
  ],
  "width": "100%", "height": "100%", "showChart": true, "showFloatingTooltip": false, "locale": "en",
  "plotLineColorGrowing": "#2962FF", "plotLineColorFalling": "#2962FF",
  "belowLineFillColorGrowing": "rgba(41, 98, 255, 0.12)", "belowLineFillColorFalling": "rgba(41, 98, 255, 0.12)",
  "belowLineFillColorGrowingBottom": "rgba(41, 98, 255, 0)", "belowLineFillColorFallingBottom": "rgba(41, 98, 255, 0)",
  "gridLineColor": "rgba(240, 243, 250, 0)", "scaleFontColor": "rgba(120, 123, 134, 1)",
  "showSymbolLogo": true, "symbolActiveColor": "rgba(41, 98, 255, 0.12)", "colorTheme": "dark"
};

const stocksConfig = {
  "title": "Stocks",
  "tabs": [
    {"title": "Financial", "symbols": [{"s": "NYSE:JPM", "d": "JPMorgan Chase"}, {"s": "NYSE:WFC", "d": "Wells Fargo Co New"}, {"s": "NYSE:BAC", "d": "Bank Amer Corp"}, {"s": "NYSE:HSBC", "d": "Hsbc Hldgs Plc"}, {"s": "NYSE:C", "d": "Citigroup Inc"}, {"s": "NYSE:MA", "d": "Mastercard Incorporated"}]},
    {"title": "Technology", "symbols": [{"s": "NASDAQ:AAPL", "d": "Apple"}, {"s": "NASDAQ:GOOGL", "d": "Alphabet"}, {"s": "NASDAQ:MSFT", "d": "Microsoft"}, {"s": "NASDAQ:FB", "d": "Meta Platforms"}, {"s": "NYSE:ORCL", "d": "Oracle Corp"}, {"s": "NASDAQ:INTC", "d": "Intel Corp"}]},
    {"title": "Services", "symbols": [{"s": "NASDAQ:AMZN", "d": "Amazon"}, {"s": "NYSE:BABA", "d": "Alibaba Group Hldg Ltd"}, {"s": "NYSE:T", "d": "At&t Inc"}, {"s": "NYSE:WMT", "d": "Walmart"}, {"s": "NYSE:V", "d": "Visa"}]}
  ],
  "width": "100%", "height": "100%", "showChart": true, "showFloatingTooltip": false, "locale": "en",
  "plotLineColorGrowing": "#2962FF", "plotLineColorFalling": "#2962FF",
  "belowLineFillColorGrowing": "rgba(41, 98, 255, 0.12)", "belowLineFillColorFalling": "rgba(41, 98, 255, 0.12)",
  "belowLineFillColorGrowingBottom": "rgba(41, 98, 255, 0)", "belowLineFillColorFallingBottom": "rgba(41, 98, 255, 0)",
  "gridLineColor": "rgba(240, 243, 250, 0)", "scaleFontColor": "rgba(120, 123, 134, 1)",
  "showSymbolLogo": true, "symbolActiveColor": "rgba(41, 98, 255, 0.12)", "colorTheme": "dark"
};

const currenciesConfig = {
  "title": "Currencies",
  "tabs": [
    {"title": "Major", "title_raw": "Major", "symbols": [{"s": "FX_IDC:EURUSD", "d": "EUR to USD"}, {"s": "FX_IDC:USDJPY", "d": "USD to JPY"}, {"s": "FX_IDC:GBPUSD", "d": "GBP to USD"}, {"s": "FX_IDC:AUDUSD", "d": "AUD to USD"}, {"s": "FX_IDC:USDCAD", "d": "USD to CAD"}, {"s": "FX_IDC:USDCHF", "d": "USD to CHF"}], "quick_link": {"title": "More majors", "href": "/markets/currencies/rates-major/"}},
    {"title": "Minor", "title_raw": "Minor", "symbols": [{"s": "FX_IDC:EURGBP", "d": "EUR to GBP"}, {"s": "FX_IDC:EURJPY", "d": "EUR to JPY"}, {"s": "FX_IDC:GBPJPY", "d": "GBP to JPY"}, {"s": "FX_IDC:CADJPY", "d": "CAD to JPY"}, {"s": "FX_IDC:GBPCAD", "d": "GBP to CAD"}, {"s": "FX_IDC:EURCAD", "d": "EUR to CAD"}], "quick_link": {"title": "More minors", "href": "/markets/currencies/rates-minor/"}},
    {"title": "Exotic", "title_raw": "Exotic", "symbols": [{"s": "FX_IDC:USDSEK", "d": "USD to SEK"}, {"s": "FX_IDC:USDMXN", "d": "USD to MXN"}, {"s": "FX_IDC:USDZAR", "d": "USD to ZAR"}, {"s": "FX_IDC:EURTRY", "d": "EUR to TRY"}, {"s": "FX_IDC:EURNOK", "d": "EUR to NOK"}, {"s": "FX_IDC:GBPPLN", "d": "GBP to PLN"}], "quick_link": {"title": "More exotics", "href": "/markets/currencies/rates-exotic/"}}
  ],
  "title_link": "/markets/currencies/rates-major/",
  "width": "100%", "height": "100%", "showChart": true, "showFloatingTooltip": false, "locale": "en",
  "plotLineColorGrowing": "#2962FF", "plotLineColorFalling": "#2962FF",
  "belowLineFillColorGrowing": "rgba(41, 98, 255, 0.12)", "belowLineFillColorFalling": "rgba(41, 98, 255, 0.12)",
  "belowLineFillColorGrowingBottom": "rgba(41, 98, 255, 0)", "belowLineFillColorFallingBottom": "rgba(41, 98, 255, 0)",
  "gridLineColor": "rgba(240, 243, 250, 0)", "scaleFontColor": "rgba(120, 123, 134, 1)",
  "showSymbolLogo": true, "symbolActiveColor": "rgba(41, 98, 255, 0.12)", "colorTheme": "dark"
};

const cryptoConfig = {
  "title": "Cryptocurrencies", "title_raw": "Cryptocurrencies",
  "tabs": [
    {"title": "Overview", "title_raw": "Overview", "symbols": [{"s": "CRYPTOCAP:TOTAL"}, {"s": "BITSTAMP:BTCUSD"}, {"s": "BITSTAMP:ETHUSD"}, {"s": "COINBASE:SOLUSD"}, {"s": "BINANCE:AVAXUSD"}, {"s": "COINBASE:UNIUSD"}], "quick_link": {"title": "More cryptocurrencies", "href": "/markets/cryptocurrencies/prices-all/"}},
    {"title": "Bitcoin", "title_raw": "Bitcoin", "symbols": [{"s": "BITSTAMP:BTCUSD"}, {"s": "COINBASE:BTCEUR"}, {"s": "COINBASE:BTCGBP"}, {"s": "BITFLYER:BTCJPY"}, {"s": "BMFBOVESPA:BIT1!"}], "quick_link": {"title": "More Bitcoin pairs", "href": "/symbols/BTCUSD/markets/"}},
    {"title": "Ethereum", "title_raw": "Ethereum", "symbols": [{"s": "BITSTAMP:ETHUSD"}, {"s": "KRAKEN:ETHEUR"}, {"s": "COINBASE:ETHGBP"}, {"s": "BITFLYER:ETHJPY"}, {"s": "BINANCE:ETHBTC"}, {"s": "BINANCE:ETHUSDT"}], "quick_link": {"title": "More Ethereum pairs", "href": "/symbols/ETHUSD/markets/"}},
    {"title": "Solana", "title_raw": "Solana", "symbols": [{"s": "COINBASE:SOLUSD"}, {"s": "BINANCE:SOLEUR"}, {"s": "COINBASE:SOLGBP"}, {"s": "BINANCE:SOLBTC"}, {"s": "COINBASE:SOLETH"}, {"s": "BINANCE:SOLUSDT"}], "quick_link": {"title": "More Solana pairs", "href": "/symbols/SOLUSD/markets/"}},
    {"title": "Uniswap", "title_raw": "Uniswap", "symbols": [{"s": "COINBASE:UNIUSD"}, {"s": "KRAKEN:UNIEUR"}, {"s": "COINBASE:UNIGBP"}, {"s": "BINANCE:UNIBTC"}, {"s": "KRAKEN:UNIETH"}, {"s": "BINANCE:UNIUSDT"}], "quick_link": {"title": "More Uniswap pairs", "href": "/symbols/UNIUSD/markets/"}}
  ],
  "title_link": "/markets/cryptocurrencies/prices-all/",
  "width": "100%", "height": "100%", "showChart": true, "showFloatingTooltip": false, "locale": "en",
  "plotLineColorGrowing": "#2962FF", "plotLineColorFalling": "#2962FF",
  "belowLineFillColorGrowing": "rgba(41, 98, 255, 0.12)", "belowLineFillColorFalling": "rgba(41, 98, 255, 0.12)",
  "belowLineFillColorGrowingBottom": "rgba(41, 98, 255, 0)", "belowLineFillColorFallingBottom": "rgba(41, 98, 255, 0)",
  "gridLineColor": "rgba(240, 243, 250, 0)", "scaleFontColor": "rgba(120, 123, 134, 1)",
  "showSymbolLogo": true, "symbolActiveColor": "rgba(41, 98, 255, 0.12)", "colorTheme": "dark"
};

const ForexHeatmapWidget = memo(function ForexHeatmapWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-forex-heat-map.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      colorTheme: "dark",
      isTransparent: true,
      locale: "en",
      currencies: ["EUR", "USD", "JPY", "GBP", "CHF", "AUD", "CAD", "NZD", "CNY"],
      width: "100%",
      height: "100%"
    });
    container.current.appendChild(script);
  }, []);
  return (
    <div className="tradingview-widget-container" ref={container} style={{ width: "100%", height: "100%" }}>
      <div className="tradingview-widget-container__widget" style={{ width: "100%", height: "100%" }}></div>
    </div>
  );
});

function EconomicMapWidget() {
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
      doc.write(`<!DOCTYPE html><html><head><style>body{margin:0;padding:0;overflow:hidden;background:transparent;}</style></head><body><script type="module" src="https://widgets.tradingview-widget.com/w/en/tv-economic-map.js"><\/script><tv-economic-map metric="intr" theme="dark" transparent style="width:100%;height:700px;display:block;"></tv-economic-map></body></html>`);
      doc.close();
    }
    return () => {
      if (containerRef.current && iframe.parentNode === containerRef.current) {
        containerRef.current.removeChild(iframe);
      }
    };
  }, []);
  return <div ref={containerRef} className="w-full h-full" />;
}

const TopStoriesWidget = memo(function TopStoriesWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-timeline.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      displayMode: "regular",
      feedMode: "market",
      colorTheme: "dark",
      isTransparent: false,
      locale: "en",
      market: "stock",
      width: "100%",
      height: 550
    });
    container.current.appendChild(script);
  }, []);

  return (
    <div className="tradingview-widget-container" ref={container}>
      <div className="tradingview-widget-container__widget"></div>
    </div>
  );
});

const EconomicCalendarWidget = memo(function EconomicCalendarWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-events.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      colorTheme: "dark",
      isTransparent: false,
      locale: "en",
      countryFilter: "ar,au,br,ca,cn,fr,de,in,id,it,jp,kr,mx,ru,sa,za,tr,gb,us,eu",
      importanceFilter: "-1,0,1",
      width: "100%",
      height: 550
    });
    container.current.appendChild(script);
  }, []);

  return (
    <div className="tradingview-widget-container" ref={container}>
      <div className="tradingview-widget-container__widget"></div>
    </div>
  );
});

export default function StocksDashboardPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <div className="w-full h-[72px] overflow-hidden">
        <TickerTapeWidget />
      </div>

      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-4 lg:space-y-8">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 lg:gap-8">
            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex items-center gap-2 mb-6">
                <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-blue-500 to-cyan-500 rounded-full flex items-center justify-center">
                  <Newspaper className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                </div>
                <h3 className="text-lg sm:text-xl font-semibold text-white">Top Stories</h3>
              </div>
              <div className="w-full min-h-[560px] rounded-lg overflow-hidden border border-white/[0.06]">
                <TopStoriesWidget />
              </div>
            </GlassCard>

            <GlassCard className="p-3 sm:p-4 lg:p-6">
              <div className="flex items-center gap-2 mb-6">
                <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-orange-500 to-amber-500 rounded-full flex items-center justify-center">
                  <Calendar className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
                </div>
                <h3 className="text-lg sm:text-xl font-semibold text-white">Economic Calendar</h3>
              </div>
              <div className="w-full min-h-[560px] rounded-lg overflow-hidden border border-white/[0.06]">
                <EconomicCalendarWidget />
              </div>
            </GlassCard>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4">
            <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
              <MarketOverviewWidget config={indicesConfig} />
            </div>
            <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
              <MarketOverviewWidget config={stocksConfig} />
            </div>
            <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
              <MarketOverviewWidget config={currenciesConfig} />
            </div>
            <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
              <MarketOverviewWidget config={cryptoConfig} />
            </div>
          </div>

          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center gap-2 mb-6">
              <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-yellow-500 to-orange-500 rounded-full flex items-center justify-center">
                <Activity className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
              </div>
              <h3 className="text-lg sm:text-xl font-semibold text-white">Forex Heatmap</h3>
            </div>
            <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
              <ForexHeatmapWidget />
            </div>
          </GlassCard>

          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center gap-2 mb-6">
              <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-green-500 to-emerald-500 rounded-full flex items-center justify-center">
                <Activity className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
              </div>
              <h3 className="text-lg sm:text-xl font-semibold text-white">Economic Map</h3>
            </div>
            <div className="w-full h-[700px] rounded-lg overflow-hidden border border-white/[0.06]">
              <EconomicMapWidget />
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
