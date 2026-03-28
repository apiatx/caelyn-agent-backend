import { Card } from "@/components/ui/card";
import { ExternalLink } from "lucide-react";
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

const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`bg-black/40 backdrop-blur-lg border-white/[0.06] ${className}`}>
    {children}
  </Card>
);

export default function ArbitrumContent() {
  return (
    <div className="space-y-8">
      {/* Arbitrum Glass Card */}
      <GlassCard className="p-6">
        <div className="flex justify-end mb-3">
          <SafeLink
            href="https://portal.arbitrum.io/"
            className="text-xs text-blue-400 hover:text-blue-300 transition-colors flex items-center gap-1"
          >
            Open Full View <ExternalLink className="w-3 h-3" />
          </SafeLink>
        </div>
        
        <div className="bg-black/20 border border-white/[0.06] rounded-lg overflow-hidden">
          <iframe
            {...getSecureIframeProps('https://portal.arbitrum.io/', 'Arbitrum Ecosystem Portal')}
            className="w-full h-[700px] border-0"
          />
        </div>
        
        <div className="mt-4 text-center">
          <p className="text-xs text-white/35">
            Arbitrum Ecosystem Portal • Explore the Arbitrum ecosystem
          </p>
        </div>
      </GlassCard>
    </div>
  );
}
