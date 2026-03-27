import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Search } from "lucide-react";
import { openSecureLink } from '@/utils/security';

// Safe Glass Card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-sm bg-gradient-to-br from-black/90 to-black/95 border border-white/20 ${className}`}>
    {children}
  </Card>
);

// Safe iframe component
const SafeIframe = ({ src, title, className = "" }: { src: string; title: string; className?: string }) => {
  return (
    <div className="w-full">
      <iframe
        src={src}
        title={title}
        className={`w-full h-[500px] rounded-lg border border-white/[0.06] ${className}`}
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
  );
};

interface SafeLinkProps {
  href: string;
  children: React.ReactNode;
  className?: string;
}

const SafeLink = ({ href, children, className = "" }: SafeLinkProps) => {
  const handleClick = () => {
    openSecureLink(href);
  };

  return (
    <button
      onClick={handleClick}
      className={className}
      data-testid={`safe-link-${href.replace(/[^a-zA-Z0-9]/g, '-')}`}
    >
      {children}
    </button>
  );
};

export default function OnchainInspectPage() {
  
  const openInNewTab = (url: string) => {
    openSecureLink(url);
  };

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Inspect Section - Enhanced Header */}
          <div className="text-center relative mb-6">
            <div className="absolute inset-0 bg-gradient-to-r from-yellow-500/20 via-orange-500/20 to-red-500/20 blur-3xl -z-10"></div>
            <div className="flex justify-center items-center gap-4 mb-6">
              <div className="w-28 h-28 rounded-full flex items-center justify-center border-2 border-yellow-400 shadow-2xl hover:scale-110 transition-transform duration-300 bg-gradient-to-r from-yellow-400 to-orange-500">
                <Search className="w-16 h-16 text-white" />
              </div>
              <h2 className="text-2xl lg:text-3xl font-bold bg-gradient-to-r from-white via-yellow-200 to-orange-200 bg-clip-text text-transparent">Inspect</h2>
            </div>
            <p className="text-lg text-white/80 font-medium tracking-wide">Token analysis and inspection tools</p>
            <div className="w-32 h-1 bg-gradient-to-r from-yellow-500 to-orange-500 mx-auto mt-4 rounded-full"></div>
          </div>

          {/* Token Sniffer Section */}
          <div className="space-y-12">
            <GlassCard className="p-8">
              <div className="text-center mb-6">
                <div className="flex items-center justify-center gap-3 mb-2">
                  <div className="w-10 h-10 bg-gradient-to-r from-yellow-400 to-orange-500 rounded-xl flex items-center justify-center shadow-lg">
                    <span className="text-white font-bold text-lg">T</span>
                  </div>
                  <h3 className="text-2xl font-bold bg-gradient-to-r from-yellow-400 to-orange-300 bg-clip-text text-transparent">Token Sniffer</h3>
                  <Badge className="bg-yellow-500/30 text-yellow-200 border-yellow-400/40 px-3 py-1 font-semibold">
                    AI CHAT
                  </Badge>
                </div>
                <button
                  onClick={() => openInNewTab('https://wach.ai/chat')}
                  className="text-yellow-300 hover:text-yellow-200 text-sm font-medium bg-yellow-500/20 px-4 py-2 rounded-lg border border-yellow-400/30 hover:bg-yellow-500/30 transition-all duration-300 mx-auto"
                  data-testid="button-token-sniffer-external"
                >
                  Open in New Tab →
                </button>
              </div>
              <div className="mb-6">
                <SafeIframe
                  src="https://wach.ai/chat"
                  title="WachAI Chat Interface"
                  className="w-full h-[500px] rounded-lg border border-white/[0.06]"
                />
              </div>

              {/* Bubblemaps */}
              <div className="mb-6">
                <SafeLink
                  href='https://bubblemaps.io/'
                  className="group w-full p-6 bg-gradient-to-br from-purple-600/30 via-fuchsia-600/20 to-pink-600/30 border-purple-400/40 hover:from-purple-500/40 hover:via-fuchsia-500/30 hover:to-pink-500/40 hover:border-purple-300/60 rounded-xl transition-all duration-300 block shadow-xl hover:shadow-purple-500/20 transform hover:scale-105 backdrop-blur-sm"
                >
                  <div className="text-center">
                    <div className="flex items-center justify-center gap-3 mb-3">
                      <div className="w-8 h-8 bg-gradient-to-r from-purple-400 to-pink-500 rounded-lg flex items-center justify-center shadow-lg group-hover:rotate-12 transition-transform duration-300">
                        <span className="text-white font-bold">B</span>
                      </div>
                      <h4 className="text-xl font-bold text-purple-200 group-hover:text-white transition-colors duration-300">Bubblemaps</h4>
                    </div>
                    <p className="text-purple-200/80 text-base font-medium">Token Analytics & Visualization Platform</p>
                  </div>
                </SafeLink>
              </div>
            </GlassCard>
          </div>
        </div>
      </main>
    </div>
  );
}