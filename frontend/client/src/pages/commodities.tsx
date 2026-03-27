import { useEffect, useRef, memo } from "react";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { TrendingUp, Coins, Diamond, Droplets, Flame, Zap, Gem, Mountain, Hammer, Wheat, Box } from "lucide-react";
import goldBarsImage from "@assets/istockphoto-1455233823-612x612_1757104224615.jpg";

const CommoditiesQuotesWidget = memo(function CommoditiesQuotesWidget() {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current) return;
    const script = document.createElement("script");
    script.src = "https://s3.tradingview.com/external-embedding/embed-widget-market-quotes.js";
    script.type = "text/javascript";
    script.async = true;
    script.innerHTML = JSON.stringify({
      "title": "Commodities",
      "width": "100%",
      "height": "100%",
      "locale": "en",
      "showSymbolLogo": true,
      "symbolsGroups": [
        {"name": "Energy", "symbols": [{"name": "PYTH:WTI3!", "displayName": "WTI Crude Oil"}, {"name": "BMFBOVESPA:ETH1!", "displayName": "Ethanol"}]},
        {"name": "Metals", "symbols": [{"name": "CMCMARKETS:GOLD", "displayName": "Gold"}, {"name": "CMCMARKETS:SILVER", "displayName": "Silver"}, {"name": "CMCMARKETS:PLATINUM", "displayName": "Platinum"}, {"name": "CMCMARKETS:COPPER", "displayName": "Copper"}]},
        {"name": "Agricultural", "symbols": [{"name": "BMFBOVESPA:ICF1!", "displayName": "Coffee"}, {"name": "CMCMARKETS:COTTON", "displayName": "Cotton"}, {"name": "BMFBOVESPA:SJC1!", "displayName": "Soybean"}, {"name": "BMFBOVESPA:CCM1!", "displayName": "Corn"}]},
        {"name": "Currencies", "symbols": [{"name": "BMFBOVESPA:EUR1!", "displayName": "Euro"}, {"name": "BMFBOVESPA:GBP1!", "displayName": "British Pound"}, {"name": "BMFBOVESPA:JPY1!", "displayName": "Japanese Yen"}, {"name": "BMFBOVESPA:CHF1!", "displayName": "Swiss Franc"}, {"name": "BMFBOVESPA:AUD1!", "displayName": "Australian Dollar"}, {"name": "BMFBOVESPA:CAD1!", "displayName": "Canadian Dollar"}]},
        {"name": "Indices", "symbols": [{"name": "BMFBOVESPA:ISP1!", "displayName": "S&P 500"}, {"name": "BMFBOVESPA:BRI1!", "displayName": "Brazil 50"}, {"name": "BMFBOVESPA:INK1!", "displayName": "Nikkei 225"}, {"name": "EUREX:FDAX1!", "displayName": "DAX"}, {"name": "BMFBOVESPA:WIN1!", "displayName": "Bovespa Index-Mini Futures"}]}
      ],
      "colorTheme": "dark"
    });
    container.current.appendChild(script);
  }, []);
  return (
    <div className="tradingview-widget-container" ref={container} style={{ width: "100%", height: "100%" }}>
      <div className="tradingview-widget-container__widget" style={{ width: "100%", height: "100%" }}></div>
    </div>
  );
});

// Glass card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function CommoditiesPage() {
  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Header */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center relative">
          <div className="absolute inset-0 bg-gradient-to-r from-amber-500/20 via-yellow-500/20 to-orange-500/20 blur-3xl -z-10"></div>
          <div className="flex justify-center items-center gap-4 mb-6">
            <div className="w-20 h-20 sm:w-24 sm:h-24 rounded-full overflow-hidden border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300">
              <img src={goldBarsImage} alt="Commodities" className="w-full h-full object-cover" />
            </div>
            <div className="text-left">
              <h2 className="text-2xl sm:text-3xl lg:text-4xl font-bold bg-gradient-to-r from-white via-amber-200 to-yellow-200 bg-clip-text text-transparent">Commodities</h2>
              <Badge className="bg-gradient-to-r from-amber-500/30 to-yellow-500/30 text-white border-amber-400/50 text-sm mt-2 px-3 py-1">
                PRECIOUS METALS & RESOURCES
              </Badge>
            </div>
          </div>
          <p className="text-lg text-white/80 font-medium tracking-wide">Track precious metals and commodity market movements</p>
          <div className="w-32 h-1 bg-gradient-to-r from-amber-500 to-yellow-500 mx-auto mt-4 rounded-full"></div>
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-6">

          <div className="w-full h-[500px] rounded-lg overflow-hidden border border-white/[0.06]">
            <CommoditiesQuotesWidget />
          </div>

          <div className="w-full rounded-lg overflow-hidden border border-white/[0.06]">
              <iframe
                src="https://www.juniorminingnetwork.com/commodity-charts.html"
                className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                title="Junior Mining Network Commodity Charts"
                frameBorder="0"
                loading="eager"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                allow="fullscreen; clipboard-write; autoplay; camera; microphone; geolocation"
              />
          </div>

          <div className="w-full rounded-lg overflow-hidden border border-white/[0.06]">
              <iframe
                src="https://sprottetfs.com/"
                className="w-full h-[500px] sm:h-[600px] lg:h-[700px] rounded-lg border border-white/[0.06]"
                title="Sprott ETFs"
                loading="eager"
                referrerPolicy="no-referrer"
                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                allow="fullscreen; clipboard-write; autoplay; camera; microphone; geolocation"
                frameBorder="0"
              />
          </div>

          {/* Commodity Quick Charts Grid */}
          <GlassCard className="p-3 sm:p-4">
            <div className="flex items-center gap-2 mb-3">
              <div className="w-5 h-5 bg-gradient-to-r from-amber-500 to-yellow-500 rounded-full flex items-center justify-center">
                <TrendingUp className="w-3 h-3 text-white" />
              </div>
              <h3 className="text-sm font-semibold text-white">Quick Charts</h3>
              <Badge className="bg-amber-500/20 text-amber-400 border-amber-500/30 text-[10px]">
                TRADINGVIEW
              </Badge>
            </div>
            <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 gap-2">
              {[
                { name: 'Gold', ticker: 'XAUUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=OANDA%3AXAUUSD', color: 'yellow-500', icon: <Coins className="w-3.5 h-3.5" /> },
                { name: 'Silver', ticker: 'XAGUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=TVC%3ASILVER', color: 'gray-400', icon: <Diamond className="w-3.5 h-3.5" /> },
                { name: 'Copper', ticker: 'HGUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=CAPITALCOM%3ACOPPER', color: 'orange-600', icon: <TrendingUp className="w-3.5 h-3.5" /> },
                { name: 'Oil', ticker: 'USOIL', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=TVC%3AUSOIL', color: 'slate-500', icon: <Droplets className="w-3.5 h-3.5" /> },
                { name: 'Nat Gas', ticker: 'XNGUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=FXOPEN%3AXNGUSD', color: 'blue-500', icon: <Flame className="w-3.5 h-3.5" /> },
                { name: 'Uranium', ticker: 'UX1!', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=COMEX%3AUX1%21', color: 'green-500', icon: <Zap className="w-3.5 h-3.5" /> },
                { name: 'Platinum', ticker: 'XPTUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=CAPITALCOM%3APLATINUM', color: 'slate-400', icon: <Gem className="w-3.5 h-3.5" /> },
                { name: 'Coal', ticker: 'NCF1!', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=ICEEUR%3ANCF1%21', color: 'gray-600', icon: <Mountain className="w-3.5 h-3.5" /> },
                { name: 'Iron', ticker: 'TIO1!', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=COMEX%3ATIO1%21', color: 'red-600', icon: <Hammer className="w-3.5 h-3.5" /> },
                { name: 'Soybeans', ticker: 'SOYBNUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=OANDA%3ASOYBNUSD', color: 'amber-600', icon: <Wheat className="w-3.5 h-3.5" /> },
                { name: 'Aluminum', ticker: 'ALUM', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=PEPPERSTONE%3AALUMINIUM', color: 'cyan-500', icon: <Box className="w-3.5 h-3.5" /> },
                { name: 'Wheat', ticker: 'WHEATUSD', url: 'https://www.tradingview.com/chart/e5l95XgZ/?symbol=OANDA%3AWHEATUSD', color: 'yellow-700', icon: <Wheat className="w-3.5 h-3.5" /> },
              ].map((item) => (
                <button
                  key={item.ticker}
                  onClick={() => window.open(item.url, '_blank', 'noopener,noreferrer')}
                  className="flex flex-col items-center gap-1 p-2.5 rounded-lg border border-white/10 bg-white/5 hover:bg-white/10 hover:border-white/20 transition-all duration-200 group cursor-pointer"
                >
                  <div className="text-white/70 group-hover:text-white transition-colors">
                    {item.icon}
                  </div>
                  <span className="text-[11px] font-medium text-white/90 group-hover:text-white">{item.name}</span>
                  <span className="text-[9px] text-white/40 group-hover:text-white/60 font-mono">{item.ticker}</span>
                </button>
              ))}
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}