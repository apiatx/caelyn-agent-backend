import React from 'react';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import { BarChart3, ExternalLink } from 'lucide-react';
import { openSecureLink } from '@/utils/security';

// Enhanced Glass Card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-xl bg-gradient-to-br from-black/80 via-gray-900/60 to-black/90 border border-white/30 shadow-2xl shadow-purple-500/10 hover:shadow-purple-500/20 transition-all duration-500 ${className}`}>
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

export default function OnchainDiscoverPage() {

  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Discover Web3 Content */}
          <div className="space-y-8 mt-12">
            <GlassCard className="p-6">
              
              {/* DappRadar - First row */}
              <div className="mb-6">
                <button
                  onClick={() => openInNewTab('https://dappradar.com/')}
                  className="w-full p-6 text-center bg-gradient-to-b from-blue-800/10 to-blue-900/10 hover:from-blue-800/20 hover:to-blue-900/20 border border-blue-800/20 hover:border-blue-700/40 rounded-lg transition-all duration-300 group"
                  data-testid="button-dappradar"
                >
                  <div className="w-12 h-12 bg-blue-800 rounded-full flex items-center justify-center mx-auto mb-3">
                    <BarChart3 className="w-6 h-6 text-white" />
                  </div>
                  <h4 className="text-lg font-bold text-white group-hover:text-blue-300 mb-2">DappRadar</h4>
                  <p className="text-white/35 group-hover:text-white/45 text-sm max-w-md mx-auto">Discover DApps & analytics across all blockchains</p>
                </button>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                <SafeLink
                  href='https://www.ethereum-ecosystem.com/'
                  className="p-5 bg-gradient-to-br from-gray-600/15 to-gray-700/15 hover:from-gray-600/25 hover:to-gray-700/25 border border-gray-500/30 hover:border-gray-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-gray-500/20 transform"
                >
                  <div className="flex flex-col items-center gap-2 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-gray-600 to-gray-700 rounded-xl flex items-center justify-center shadow-md">
                      <span className="text-white font-bold">E</span>
                    </div>
                    <h4 className="text-gray-200 font-bold text-lg">Ethereum Ecosystem</h4>
                  </div>
                  <p className="text-white/45 text-sm font-medium text-center">Comprehensive ecosystem guide</p>
                </SafeLink>

                <SafeLink
                  href='https://academy.swissborg.com/en/learn/solana-ecosystem'
                  className="p-5 bg-gradient-to-br from-indigo-500/15 to-indigo-600/15 hover:from-indigo-500/25 hover:to-indigo-600/25 border border-indigo-500/30 hover:border-indigo-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-indigo-500/20 transform"
                >
                  <div className="flex flex-col items-center gap-2 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-indigo-500 to-indigo-600 rounded-xl flex items-center justify-center shadow-md">
                      <span className="text-white font-bold">S</span>
                    </div>
                    <h4 className="text-indigo-300 font-bold text-lg">Solana Ecosystem</h4>
                  </div>
                  <p className="text-white/45 text-sm font-medium text-center">SwissBorg Academy ecosystem guide</p>
                </SafeLink>

                <SafeLink
                  href='https://www.base.org/ecosystem'
                  className="p-5 bg-gradient-to-br from-cyan-500/15 to-cyan-600/15 hover:from-cyan-500/25 hover:to-cyan-600/25 border border-cyan-500/30 hover:border-cyan-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-cyan-500/20 transform"
                >
                  <div className="flex flex-col items-center gap-2 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-cyan-500 to-cyan-600 rounded-xl flex items-center justify-center shadow-md">
                      <span className="text-white font-bold">B</span>
                    </div>
                    <h4 className="text-cyan-300 font-bold text-lg">Base Ecosystem</h4>
                  </div>
                  <p className="text-white/45 text-sm font-medium text-center">Official Base ecosystem directory</p>
                </SafeLink>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <SafeLink
                  href='https://gravity.xyz/'
                  className="p-5 bg-gradient-to-br from-emerald-500/15 to-emerald-600/15 hover:from-emerald-500/25 hover:to-emerald-600/25 border border-emerald-500/30 hover:border-emerald-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-emerald-500/20 transform"
                >
                  <div className="flex flex-col items-center gap-2 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-emerald-500 to-emerald-600 rounded-xl flex items-center justify-center shadow-md">
                      <span className="text-white font-bold">G</span>
                    </div>
                    <h4 className="text-emerald-300 font-bold text-lg">Gravity Ecosystem</h4>
                  </div>
                  <p className="text-white/45 text-sm font-medium text-center">Explore the Gravity network</p>
                </SafeLink>

                <SafeLink
                  href='https://zapper.xyz/'
                  className="p-5 bg-gradient-to-br from-purple-500/15 to-purple-600/15 hover:from-purple-500/25 hover:to-purple-600/25 border border-purple-500/30 hover:border-purple-400/50 rounded-xl hover:scale-105 transition-all duration-300 shadow-lg hover:shadow-purple-500/20 transform"
                >
                  <div className="flex flex-col items-center gap-2 mb-3">
                    <div className="w-10 h-10 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl flex items-center justify-center shadow-md">
                      <span className="text-white font-bold">Z</span>
                    </div>
                    <h4 className="text-purple-300 font-bold text-lg">Zapper</h4>
                  </div>
                  <p className="text-white/45 text-sm font-medium text-center">DeFi portfolio management</p>
                </SafeLink>
              </div>

              {/* Alchemy Button - Full width underneath Gravity and Zapper */}
              <div className="mt-6">
                <button
                  onClick={() => openInNewTab('https://www.alchemy.com/dapps/')}
                  className="w-full p-6 text-center bg-gradient-to-b from-blue-600/10 to-indigo-700/10 hover:from-blue-600/20 hover:to-indigo-700/20 border border-blue-600/20 hover:border-blue-500/40 rounded-lg transition-all duration-300 group"
                  data-testid="button-alchemy"
                >
                  <div className="w-12 h-12 bg-gradient-to-br from-blue-600 to-indigo-700 rounded-full flex items-center justify-center mx-auto mb-3">
                    <span className="text-white font-bold text-xl">A</span>
                  </div>
                  <h4 className="text-lg font-bold text-white group-hover:text-blue-300 mb-2">Alchemy</h4>
                  <p className="text-white/35 group-hover:text-white/45 text-sm max-w-md mx-auto">Discover top DApps and Web3 applications</p>
                </button>
              </div>
            </GlassCard>
          </div>
        </div>
      </main>
    </div>
  );
}