import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Wallet } from "lucide-react";
import { openSecureLink } from "@/utils/security";

// Glass card component for On Ramp section
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

// Use secure link opening
const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function TradeOnRampPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* On Ramp Glass Card */}
          <GlassCard className="p-6">

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <Button
                variant="outline"
                onClick={() => openInNewTab('https://www.coinbase.com/home')}
                className="bg-black/20 border-white/[0.06] hover:bg-blue-500/20 hover:border-blue-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
                data-testid="button-coinbase"
              >
                <div className="p-3 rounded-lg bg-gradient-to-r from-blue-500/20 to-blue-600/20">
                  <Wallet className="h-6 w-6 text-blue-400" />
                </div>
                <div className="text-center">
                  <div className="font-semibold text-lg">Coinbase</div>
                  <div className="text-sm text-crypto-silver">Leading crypto exchange</div>
                </div>
              </Button>

              <Button
                variant="outline"
                onClick={() => openInNewTab('https://www.kraken.com/')}
                className="bg-black/20 border-white/[0.06] hover:bg-purple-500/20 hover:border-purple-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
                data-testid="button-kraken"
              >
                <div className="p-3 rounded-lg bg-gradient-to-r from-purple-500/20 to-purple-600/20">
                  <Wallet className="h-6 w-6 text-purple-400" />
                </div>
                <div className="text-center">
                  <div className="font-semibold text-lg">Kraken</div>
                  <div className="text-sm text-crypto-silver">Professional trading platform</div>
                </div>
              </Button>

              <Button
                variant="outline"
                onClick={() => openInNewTab('https://www.moonpay.com/buy')}
                className="bg-black/20 border-white/[0.06] hover:bg-green-500/20 hover:border-green-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
                data-testid="button-moonpay"
              >
                <div className="p-3 rounded-lg bg-gradient-to-r from-green-500/20 to-green-600/20">
                  <Wallet className="h-6 w-6 text-green-400" />
                </div>
                <div className="text-center">
                  <div className="font-semibold text-lg">MoonPay</div>
                  <div className="text-sm text-crypto-silver">Crypto payment gateway</div>
                </div>
              </Button>

              <Button
                variant="outline"
                onClick={() => openInNewTab('https://strike.me/en/')}
                className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
                data-testid="button-strike"
              >
                <div className="p-3 rounded-lg bg-gradient-to-r from-orange-500/20 to-yellow-500/20">
                  <Wallet className="h-6 w-6 text-orange-400" />
                </div>
                <div className="text-center">
                  <div className="font-semibold text-lg">Strike</div>
                  <div className="text-sm text-crypto-silver">Bitcoin payment app</div>
                </div>
              </Button>
            </div>

            {/* ZKP2P Big Button - Full width */}
            <div className="mt-6">
              <Button
                variant="outline"
                onClick={() => openInNewTab('https://www.zkp2p.xyz/swap?tab=buy')}
                className="w-full p-6 text-center bg-gradient-to-b from-cyan-600/10 to-teal-700/10 hover:from-cyan-600/20 hover:to-teal-700/20 border border-cyan-600/20 hover:border-cyan-500/40 rounded-lg transition-all duration-300 group"
                data-testid="button-zkp2p"
              >
                <div className="w-12 h-12 bg-gradient-to-br from-cyan-600 to-teal-700 rounded-full flex items-center justify-center mx-auto mb-3">
                  <Wallet className="w-6 h-6 text-white" />
                </div>
                <h4 className="text-lg font-bold text-white group-hover:text-cyan-300 mb-2">ZKP2P</h4>
                <p className="text-white/35 group-hover:text-white/45 text-sm max-w-md mx-auto">Zero-knowledge peer-to-peer fiat onramp</p>
              </Button>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}