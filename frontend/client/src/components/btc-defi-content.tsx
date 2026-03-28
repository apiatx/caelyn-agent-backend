import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DollarSign, ExternalLink, Layers, Shield } from "lucide-react";
import { openSecureLink, getSecureIframeProps } from "@/utils/security";

const SafeLink = ({ href, children, className = "", ...props }: { 
  href: string; 
  children: React.ReactNode; 
  className?: string; 
  [key: string]: any; 
}) => (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    className={className}
    onClick={(e) => {
      e.preventDefault();
      openSecureLink(href);
    }}
    {...props}
  >
    {children}
  </a>
);

const SafeIframe = ({ src, title, className = "", ...props }: { 
  src: string; 
  title: string; 
  className?: string; 
  [key: string]: any; 
}) => (
  <iframe
    src={src}
    title={title}
    className={className}
    sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
    {...props}
  />
);

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

const openInNewTab = (url: string) => {
  openSecureLink(url);
};

export default function BTCDeFiContent() {
  return (
    <div className="space-y-8">
      {/* Bitcoin DeFi Glass Card */}
      <GlassCard className="p-6">
        
        {/* BitFi BTC Staking - Full Width Iframe */}
        <div className="mb-6">
          <div className="flex justify-end mb-3">
            <SafeLink
              href="https://app.bitfi.one/bfBTC/stake"
              className="text-xs text-orange-400 hover:text-orange-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </SafeLink>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <SafeIframe
              src="https://app.bitfi.one/bfBTC/stake"
              title="BitFi BTC Staking"
              className="w-full h-[600px] border-0"
            />
          </div>
          
          <div className="mt-4 text-center">
            <p className="text-xs text-white/35">
              BitFi bfBTC staking • Bitcoin DeFi yield farming protocol
            </p>
          </div>
        </div>

        {/* Merlin Chain Stake BTC - Full Width Iframe */}
        <div className="mb-6">
          <div className="flex justify-end mb-3">
            <SafeLink
              href="https://merlinchain.io/stakebtc"
              className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </SafeLink>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <SafeIframe
              src="https://merlinchain.io/stakebtc"
              title="Merlin Chain Stake BTC"
              className="w-full h-[600px] border-0"
            />
          </div>
          
          <div className="mt-4 text-center">
            <p className="text-xs text-white/35">
              Merlin Chain • Bitcoin Layer 2 staking protocol
            </p>
          </div>
        </div>

        {/* Goat Network Yield - Full Width Iframe */}
        <div className="mb-6">
          <div className="flex justify-end mb-3">
            <SafeLink
              href="https://yield.goat.network/"
              className="text-xs text-green-400 hover:text-green-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </SafeLink>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <SafeIframe
              src="https://yield.goat.network/"
              title="Goat Network Yield Protocol"
              className="w-full h-[600px] border-0"
            />
          </div>
          
          <div className="mt-4 text-center">
            <p className="text-xs text-white/35">
              Goat Network Yield • DeFi yield optimization protocol
            </p>
          </div>
        </div>
        
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <Button
            variant="outline"
            onClick={() => openInNewTab('https://www.stacks.co/explore/ecosystem?category=All+Teams#apps')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-orange-500/20 to-yellow-500/20">
              <Layers className="h-6 w-6 text-orange-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">Stacks</div>
              <div className="text-sm text-crypto-silver">Bitcoin DeFi Layer</div>
            </div>
          </Button>

          <Button
            variant="outline"
            onClick={() => openInNewTab('https://app.satlayer.xyz/vaults/restake')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-orange-500/20 to-red-500/20">
              <Shield className="h-6 w-6 text-orange-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">SatLayer</div>
              <div className="text-sm text-crypto-silver">Bitcoin restaking vaults</div>
            </div>
          </Button>

          <Button
            variant="outline"
            onClick={() => openInNewTab('https://www.btcfi.one/dashboard')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
            data-testid="button-btcfi-dashboard"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-orange-500/20 to-amber-500/20">
              <DollarSign className="h-6 w-6 text-orange-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">BTCfi Dashboard</div>
              <div className="text-sm text-crypto-silver">Bitcoin DeFi Dashboard</div>
            </div>
          </Button>

          <Button
            variant="outline"
            onClick={() => openInNewTab('https://app.solv.finance/solvbtc?network=ethereum')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
            data-testid="button-solv-finance"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-yellow-500/20 to-orange-500/20">
              <Layers className="h-6 w-6 text-yellow-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">Solv Finance</div>
              <div className="text-sm text-crypto-silver">SolvBTC Ethereum</div>
            </div>
          </Button>

          <Button
            variant="outline"
            onClick={() => openInNewTab('https://usda.avalonfinance.xyz/markets/')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
            data-testid="button-avalon"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-orange-500/20 to-red-500/20">
              <DollarSign className="h-6 w-6 text-orange-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">Avalon Finance</div>
              <div className="text-sm text-crypto-silver">Bitcoin DeFi USDA Markets</div>
            </div>
          </Button>

          <Button
            variant="outline"
            onClick={() => openInNewTab('https://www.lombard.finance/app/')}
            className="bg-black/20 border-white/[0.06] hover:bg-orange-500/20 hover:border-orange-500/30 text-white justify-start p-6 h-auto flex-col space-y-2"
            data-testid="button-lombard"
          >
            <div className="p-3 rounded-lg bg-gradient-to-r from-green-500/20 to-emerald-500/20">
              <Shield className="h-6 w-6 text-green-400" />
            </div>
            <div className="text-center">
              <div className="font-semibold text-lg">Lombard Finance</div>
              <div className="text-sm text-crypto-silver">Bitcoin DeFi Protocol</div>
            </div>
          </Button>
        </div>
      </GlassCard>
    </div>
  );
}
