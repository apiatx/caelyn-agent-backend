import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Brain, ExternalLink } from "lucide-react";
import { getSecureIframeProps, getSecureLinkProps } from "@/utils/security";
import { openSecureLink } from '@/utils/security';

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-sm bg-gradient-to-br from-white/10 to-white/5 border border-white/20 ${className}`}>
    {children}
  </Card>
);

export default function BittensorDashboardSection() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-6">
        {/* TaoBot */}
        <div>
          <div className="flex justify-end mb-3">
            <a
              href="https://www.tao.bot/explore"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <iframe
              src="https://www.tao.bot/explore"
              className="w-full h-[600px] border-0"
              title="TaoBot"
              frameBorder="0"
              loading="lazy"
              style={{
                background: 'transparent',
                colorScheme: 'dark'
              }}
            />
          </div>
        </div>

        {/* TaoStats, Tao.app, and Bittensor.ai Buttons */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {/* TaoStats Button - Black with White Text */}
          <button
            onClick={() => openSecureLink('https://taostats.io/')}
            className="group relative overflow-hidden bg-black border border-white/20 rounded-lg p-6 hover:border-orange-400/50 transition-all duration-300 hover:scale-[1.02]"
            data-testid="button-taostats"
          >
            <div className="flex flex-col items-center justify-center space-y-3">
              <div className="w-16 h-16 bg-orange-500/10 border border-orange-500/30 rounded-xl flex items-center justify-center group-hover:bg-orange-500/20 transition-all">
                <span className="text-3xl font-bold text-orange-400">TS</span>
              </div>
              <h3 className="text-xl font-bold text-white">TaoStats</h3>
              <p className="text-sm text-white/35">Bittensor network analytics</p>
            </div>
            <div className="absolute top-3 right-3">
              <ExternalLink className="w-4 h-4 text-gray-500 group-hover:text-orange-400 transition-colors" />
            </div>
          </button>

          {/* Tao.app Explorer Button - White with Black Text */}
          <button
            onClick={() => openSecureLink('https://www.tao.app/explorer')}
            className="group relative overflow-hidden bg-white border border-gray-300 rounded-lg p-6 hover:border-cyan-500 transition-all duration-300 hover:scale-[1.02]"
            data-testid="button-taoapp"
          >
            <div className="flex flex-col items-center justify-center space-y-3">
              <div className="w-16 h-16 bg-cyan-500/10 border border-cyan-500/30 rounded-xl flex items-center justify-center group-hover:bg-cyan-500/20 transition-all">
                <span className="text-3xl font-bold text-cyan-600">TA</span>
              </div>
              <h3 className="text-xl font-bold text-black">Tao.app</h3>
              <p className="text-sm text-gray-600">Explorer & analytics</p>
            </div>
            <div className="absolute top-3 right-3">
              <ExternalLink className="w-4 h-4 text-white/35 group-hover:text-cyan-500 transition-colors" />
            </div>
          </button>

          {/* Bittensor.ai Button - Purple with White Text */}
          <button
            onClick={() => openSecureLink('https://www.bittensor.ai/')}
            className="group relative overflow-hidden bg-purple-600 border border-purple-400/50 rounded-lg p-6 hover:border-purple-300 transition-all duration-300 hover:scale-[1.02]"
            data-testid="button-bittensor"
          >
            <div className="flex flex-col items-center justify-center space-y-3">
              <div className="w-16 h-16 bg-white/10 border border-white/30 rounded-xl flex items-center justify-center group-hover:bg-white/20 transition-all">
                <span className="text-3xl font-bold text-white">B</span>
              </div>
              <h3 className="text-xl font-bold text-white">Bittensor.ai</h3>
              <p className="text-sm text-purple-100">Official Bittensor hub</p>
            </div>
            <div className="absolute top-3 right-3">
              <ExternalLink className="w-4 h-4 text-purple-200 group-hover:text-white transition-colors" />
            </div>
          </button>
        </div>

        {/* TaoTensorLaw and TaoMarketCap - Side by Side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* TaoTensorLaw */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taotensorlaw.com/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taotensorlaw.com/"
                className="w-full h-[600px] border-0"
                title="TaoTensorLaw"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>

          {/* Top dTAO Wallets (TaoMarketCap) */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taomarketcap.com/blockchain/accounts"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-orange-400 hover:text-orange-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taomarketcap.com/blockchain/accounts"
                className="w-full h-[600px] border-0"
                title="Top dTAO Wallets"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>
        </div>

        {/* TaoYield */}
        <div>
          <div className="flex justify-end mb-3">
            <a
              href="https://www.taoyield.com/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-emerald-400 hover:text-emerald-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <iframe
              src="https://www.taoyield.com/"
              className="w-full h-[600px] border-0"
              title="TaoYield"
              frameBorder="0"
              loading="lazy"
              sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
              style={{
                background: 'transparent',
                colorScheme: 'dark'
              }}
            />
          </div>
        </div>

        {/* TaoRevenue and Backprop Finance - Side by Side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* TaoRevenue */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taorevenue.com/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taorevenue.com/"
                className="w-full h-[600px] border-0"
                title="TaoRevenue"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>

          {/* Backprop Finance */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://backprop.finance/screener/bubbles"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-green-400 hover:text-green-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://backprop.finance/screener/bubbles"
                className="w-full h-[600px] border-0"
                title="Backprop Finance Screener"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>
        </div>

        {/* TaoTrack Simulator and TaoCagr - Side by Side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* TaoTrack Simulator */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taotrack.com/simulator"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-cyan-400 hover:text-cyan-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taotrack.com/simulator"
                className="w-full h-[600px] border-0"
                title="TaoTrack Simulator"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>

          {/* TaoCagr */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taocagr.com/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-amber-400 hover:text-amber-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taocagr.com/"
                className="w-full h-[600px] border-0"
                title="TaoCagr"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>
        </div>

        {/* TaoGalaxy */}
        <div>
          <div className="flex justify-end mb-3">
            <a
              href="https://taogalaxy.com/app"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-pink-400 hover:text-pink-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <iframe
              src="https://taogalaxy.com/app"
              className="w-full h-[600px] border-0"
              title="TaoGalaxy"
              frameBorder="0"
              loading="lazy"
              sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
              style={{
                background: 'transparent',
                colorScheme: 'dark'
              }}
            />
          </div>
        </div>

        {/* SubnetAlpha */}
        <div>
          <div className="flex justify-end mb-3">
            <a
              href="https://subnetalpha.ai/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-indigo-400 hover:text-indigo-300 transition-colors flex items-center gap-1"
            >
              Open Full View <ExternalLink className="w-3 h-3" />
            </a>
          </div>
          
          <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
            <iframe
              src="https://subnetalpha.ai/"
              className="w-full h-[600px] border-0"
              title="SubnetAlpha"
              frameBorder="0"
              loading="lazy"
              sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
              style={{
                background: 'transparent',
                colorScheme: 'dark'
              }}
            />
          </div>
        </div>

        {/* TaoBridge, TaoFi, and VoidAI - Three Across */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* TaoBridge */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://taobridge.xyz/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-purple-400 hover:text-purple-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://taobridge.xyz/"
                className="w-full h-[600px] border-0"
                title="TaoBridge"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>

          {/* TaoFi Swap Integration */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://www.taofi.com/swap"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-teal-400 hover:text-teal-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://www.taofi.com/swap"
                className="w-full h-[600px] border-0"
                title="TaoFi Swap"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>

          {/* VoidAI Bridge */}
          <div>
            <div className="flex justify-end mb-3">
              <a
                href="https://bridge.voidai.com/bridge-chains"
                target="_blank"
                rel="noopener noreferrer"
                className="text-xs text-violet-400 hover:text-violet-300 transition-colors flex items-center gap-1"
              >
                Open Full View <ExternalLink className="w-3 h-3" />
              </a>
            </div>
            
            <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
              <iframe
                src="https://bridge.voidai.com/bridge-chains"
                className="w-full h-[600px] border-0"
                title="VoidAI Bridge"
                frameBorder="0"
                loading="lazy"
                sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox"
                style={{
                  background: 'transparent',
                  colorScheme: 'dark'
                }}
              />
            </div>
          </div>
        </div>

        {/* TaoHub Portfolio */}
        <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
          <iframe
            src="https://www.taohub.info/"
            className="w-full h-[600px] border-0"
            title="TaoHub Portfolio"
            frameBorder="0"
            loading="lazy"
            sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
            style={{
              background: 'transparent',
              colorScheme: 'dark'
            }}
          />
        </div>
      </div>
    </div>
  );
}
