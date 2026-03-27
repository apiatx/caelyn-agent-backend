import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Wallet, Shield, Bot, ExternalLink, Zap, Lock, Users, ArrowUpRight } from "lucide-react";
import { openSecureLink } from '@/utils/security';

// Safe Glass Card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-sm bg-gradient-to-br from-black/90 to-black/95 border border-white/20 ${className}`}>
    {children}
  </Card>
);

interface SafeLinkProps {
  href: string;
  children: React.ReactNode;
  className?: string;
}

const SafeLink: React.FC<SafeLinkProps> = ({ href, children, className = "" }) => {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <button onClick={() => openInNewTab(href)} className={className}>
      {children}
    </button>
  );
};

export default function OnchainSmartWalletsPage() {
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Smart Wallets */}
          <GlassCard className="p-6">
            <div className="mb-8">
              <iframe
                src="https://app.chainlyze.ai/smart-wallet"
                title="Chainlyze Smart Wallet Tracker"
                className="w-full h-[600px] rounded-lg border border-white/[0.06]"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation"
                referrerPolicy="strict-origin-when-cross-origin"
                style={{
                  background: '#000000',
                  colorScheme: 'dark'
                }}
              />
            </div>
            
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
              <SafeLink
                href='https://hyperdash.info/trader/0x15b325660a1c4a9582a7d834c31119c0cb9e3a42'
                className="p-4 bg-purple-500/10 border border-purple-500/20 rounded-lg hover:bg-purple-500/20 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  <h4 className="text-purple-400 font-semibold">HyperLiquid Whale</h4>
                </div>
                <p className="text-white/35 text-sm">Hyperdash Trader Analytics</p>
              </SafeLink>

              <SafeLink
                href='https://debank.com/profile/0x3f135ba020d0ed288d8dd85cd3d600451b121013'
                className="p-4 bg-blue-500/10 border border-blue-500/20 rounded-lg hover:bg-blue-500/20 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  <h4 className="text-blue-400 font-semibold">WhaleAI - ETH/BASE</h4>
                </div>
                <p className="text-white/35 text-sm">DeBank Portfolio Analysis</p>
              </SafeLink>

              <SafeLink
                href='https://debank.com/profile/0xb1058c959987e3513600eb5b4fd82aeee2a0e4f9'
                className="p-4 bg-orange-500/10 border border-orange-500/20 rounded-lg hover:bg-orange-500/20 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  <h4 className="text-orange-400 font-semibold">Debt Relief Bot</h4>
                </div>
                <p className="text-white/35 text-sm">DeBank wallet tracker</p>
              </SafeLink>

              <SafeLink
                href='https://app.hyperliquid.xyz/vaults/0xe11b12a81ad743ae805078b0da61e9166475a829'
                className="p-4 bg-red-500/10 border border-red-500/20 rounded-lg hover:bg-red-500/20 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  <h4 className="text-red-400 font-semibold">DegenAI HL Vault</h4>
                </div>
                <p className="text-white/35 text-sm">Copy trading vault strategy</p>
              </SafeLink>

              <SafeLink
                href='https://degenai.dev/'
                className="p-4 bg-pink-500/10 border border-pink-500/20 rounded-lg hover:bg-pink-500/20 transition-colors"
              >
                <div className="flex items-center gap-2 mb-2">
                  <h4 className="text-pink-400 font-semibold">DegenAI Perps Bot</h4>
                </div>
                <p className="text-white/35 text-sm">AI-powered perpetual trading bot</p>
              </SafeLink>
            </div>

            {/* Indexy - Full Width Button */}
            <div className="mt-6">
              <SafeLink
                href='https://indexy.xyz/home'
                className="w-full p-6 bg-gradient-to-r from-green-500/15 to-green-600/15 hover:from-green-500/25 hover:to-green-600/25 border border-green-500/30 hover:border-green-400/50 rounded-xl transition-all duration-300 shadow-lg hover:shadow-green-500/20 transform hover:scale-[1.02] flex items-center justify-center gap-3"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 bg-gradient-to-br from-green-500 to-green-600 rounded-lg flex items-center justify-center shadow-md">
                    <span className="text-white font-bold text-lg">I</span>
                  </div>
                  <div className="text-center">
                    <h4 className="text-green-300 font-bold text-xl">Indexy</h4>
                    <p className="text-white/45 text-sm">Crypto market indexing platform</p>
                  </div>
                  <ExternalLink className="w-5 h-5 text-green-400" />
                </div>
              </SafeLink>
            </div>
          </GlassCard>
        </div>
      </main>
    </div>
  );
}