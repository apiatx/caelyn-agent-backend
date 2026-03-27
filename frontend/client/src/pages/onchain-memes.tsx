import { Card } from "@/components/ui/card";

// Enhanced Glass Card component
const GlassCard = ({ children, className = "" }: { children: React.ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-xl bg-gradient-to-br from-black/80 via-gray-900/60 to-black/90 border border-white/30 shadow-2xl shadow-purple-500/10 hover:shadow-purple-500/20 transition-all duration-500 ${className}`}>
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
        className={`w-full h-[600px] rounded-lg border border-white/[0.06] ${className}`}
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

export default function OnchainMemesPage() {

  return (
    <div className="min-h-screen text-white" style={{ background: '#050608' }}>
      {/* Main Content */}
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-8">
          {/* Memecoins */}
          <div className="space-y-8">
            <GlassCard className="p-6">

              {/* Capitoday */}
              <div className="mb-8">
                <SafeIframe
                  src="https://capitoday.com/"
                  title="Capitoday Memecoins Platform"
                  className="h-[600px]"
                />
              </div>

              {/* HolderScan */}
              <div className="mb-8">
                <SafeIframe
                  src="https://holderscan.com/"
                  title="HolderScan Memecoins Analytics"
                  className="h-[600px]"
                />
              </div>
            </GlassCard>
          </div>
        </div>
      </main>
    </div>
  );
}