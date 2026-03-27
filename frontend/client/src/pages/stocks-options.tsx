import { useEffect, useRef, useMemo } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { BarChart3 } from "lucide-react";

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

function OptionsChainWidget() {
  const containerRef = useRef<HTMLDivElement>(null);

  const config = useMemo(() => ({
    "showSymbolLogo": true,
    "symbolsGroups": [
      {
        "name": "Top Stocks",
        "symbols": [
          { "name": "NASDAQ:AAPL" },
          { "name": "NASDAQ:MSFT" },
          { "name": "NASDAQ:AMZN" },
          { "name": "NASDAQ:GOOGL" },
          { "name": "NASDAQ:META" },
          { "name": "NASDAQ:NVDA" },
          { "name": "NASDAQ:TSLA" },
          { "name": "NYSE:JPM" },
          { "name": "CBOE:SPX" },
          { "name": "AMEX:SPY" }
        ]
      }
    ],
    "colorTheme": "dark",
    "isTransparent": true,
    "locale": "en",
    "width": "100%",
    "height": "100%"
  }), []);

  useEffect(() => {
    if (!containerRef.current) return;
    containerRef.current.innerHTML = "";

    const widgetDiv = document.createElement("div");
    widgetDiv.className = "tradingview-widget-container__widget";
    containerRef.current.appendChild(widgetDiv);

    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-market-quotes.js";
    script.async = true;
    script.type = "text/javascript";
    script.textContent = JSON.stringify(config);
    containerRef.current.appendChild(script);

    return () => {
      if (containerRef.current) {
        containerRef.current.innerHTML = "";
      }
    };
  }, [config]);

  return <div ref={containerRef} className="tradingview-widget-container w-full h-[500px] sm:h-[600px] lg:h-[700px]" />;
}

function OptionsOverviewWidget() {
  const containerRef = useRef<HTMLDivElement>(null);

  const config = useMemo(() => ({
    "symbols": [
      ["CBOE:VIX|1D"],
      ["AMEX:SPY|1D"],
      ["NASDAQ:QQQ|1D"],
      ["AMEX:IWM|1D"]
    ],
    "chartOnly": false,
    "width": "100%",
    "height": "100%",
    "locale": "en",
    "colorTheme": "dark",
    "autosize": true,
    "showVolume": false,
    "showMA": false,
    "hideDateRanges": false,
    "hideMarketStatus": false,
    "hideSymbolLogo": false,
    "scalePosition": "right",
    "scaleMode": "Normal",
    "fontFamily": "-apple-system, BlinkMacSystemFont, Trebuchet MS, Roboto, Ubuntu, sans-serif",
    "fontSize": "10",
    "noTimeScale": false,
    "valuesTracking": "1",
    "changeMode": "price-and-percent",
    "chartType": "area",
    "lineWidth": 2,
    "lineType": 0,
    "dateRanges": [
      "1d|1",
      "1m|30",
      "3m|60",
      "12m|1D",
      "60m|1W",
      "all|1M"
    ],
    "isTransparent": true
  }), []);

  useEffect(() => {
    if (!containerRef.current) return;
    containerRef.current.innerHTML = "";

    const widgetDiv = document.createElement("div");
    widgetDiv.className = "tradingview-widget-container__widget";
    containerRef.current.appendChild(widgetDiv);

    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-symbol-overview.js";
    script.async = true;
    script.type = "text/javascript";
    script.textContent = JSON.stringify(config);
    containerRef.current.appendChild(script);

    return () => {
      if (containerRef.current) {
        containerRef.current.innerHTML = "";
      }
    };
  }, [config]);

  return <div ref={containerRef} className="tradingview-widget-container w-full h-[400px] sm:h-[450px] lg:h-[500px]" />;
}

export default function StocksOptionsPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-4 lg:space-y-8">
          {/* VIX & Key Options Indices */}
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-purple-500 to-pink-500 rounded-full flex items-center justify-center">
                <BarChart3 className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
              </div>
              <h3 className="text-lg sm:text-xl font-semibold text-white">Options Overview</h3>
              <Badge className="bg-purple-500/20 text-purple-400 border-purple-500/30 text-xs">VIX & INDICES</Badge>
            </div>
            <OptionsOverviewWidget />
          </GlassCard>

          {/* Options Market Quotes */}
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-cyan-500 to-blue-500 rounded-full flex items-center justify-center">
                <BarChart3 className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
              </div>
              <h3 className="text-lg sm:text-xl font-semibold text-white">Options Market Quotes</h3>
              <Badge className="bg-cyan-500/20 text-cyan-400 border-cyan-500/30 text-xs">TOP TICKERS</Badge>
            </div>
            <OptionsChainWidget />
          </GlassCard>

          {/* Unusual Options Activity */}
          <GlassCard className="p-3 sm:p-4 lg:p-6">
            <div className="flex items-center gap-2 mb-4">
              <div className="w-5 h-5 sm:w-6 sm:h-6 bg-gradient-to-r from-amber-500 to-orange-500 rounded-full flex items-center justify-center">
                <BarChart3 className="w-3 h-3 sm:w-4 sm:h-4 text-white" />
              </div>
              <h3 className="text-lg sm:text-xl font-semibold text-white">Unusual Options Activity</h3>
              <Badge className="bg-amber-500/20 text-amber-400 border-amber-500/30 text-xs">FLOW</Badge>
            </div>
            <div className="w-full">
              <iframe
                src="https://www.barchart.com/options/unusual-activity/stocks?embed=true"
                className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                title="Unusual Options Activity"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                allow="fullscreen"
              />
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}
