import React from 'react';
import { Loader2 } from 'lucide-react';

export function LoadingScreen() {
  return (
    <div className="min-h-screen text-white flex items-center justify-center" style={{background: 'linear-gradient(135deg, hsl(0, 0%, 0%) 0%, hsl(0, 0%, 10%) 50%, hsl(0, 0%, 0%) 100%)'}}>
      <div className="text-center">
        <div className="relative">
          <Loader2 className="h-12 w-12 animate-spin text-crypto-silver mx-auto mb-4" />
          <div className="absolute inset-0 rounded-full bg-gradient-to-r from-crypto-silver/20 to-white/10 animate-pulse" />
        </div>
        <h2 className="text-xl font-semibold text-white mb-2">Loading CryptoHippo</h2>
        <p className="text-crypto-silver text-sm">Preparing your crypto analytics...</p>
      </div>
    </div>
  );
}

export function SectionLoadingState({ title }: { title?: string }) {
  return (
    <div className="bg-black/40 backdrop-blur-lg border border-white/[0.06] rounded-xl p-6">
      <div className="animate-pulse">
        <div className="flex items-center gap-3 mb-6">
          <div className="w-6 h-6 bg-crypto-silver/20 rounded-full"></div>
          <div className="h-4 bg-crypto-silver/20 rounded w-32"></div>
        </div>
        <div className="space-y-3">
          <div className="h-4 bg-crypto-silver/10 rounded w-full"></div>
          <div className="h-4 bg-crypto-silver/10 rounded w-4/5"></div>
          <div className="h-4 bg-crypto-silver/10 rounded w-3/5"></div>
        </div>
      </div>
      {title && (
        <div className="text-center mt-4">
          <p className="text-xs text-crypto-silver">Loading {title}...</p>
        </div>
      )}
    </div>
  );
}