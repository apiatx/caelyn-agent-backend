import { AlertTriangle, ExternalLink } from "lucide-react";

export function DataIntegrityNotice() {
  return (
    <div className="bg-orange-50 dark:bg-orange-950/20 border border-orange-200 dark:border-orange-800 rounded-lg p-4 mb-6">
      <div className="flex items-start gap-3">
        <AlertTriangle className="h-5 w-5 text-orange-600 dark:text-orange-400 mt-0.5" />
        <div className="flex-1">
          <h3 className="font-semibold text-orange-900 dark:text-orange-100">
            API Authentication Required
          </h3>
          <p className="text-sm text-orange-700 dark:text-orange-300 mt-1">
            Your wallet addresses are connected but API keys require activation. The platform maintains data integrity by displaying empty portfolios rather than simulated data until authentic blockchain data is available.
          </p>
          <div className="mt-3 flex flex-col sm:flex-row gap-2">
            <span className="text-sm text-orange-600 dark:text-orange-400">
              Connected: 0x1677...8D97E (BASE) • 5EyoW...3FFAA (TAO)
            </span>
          </div>
          <div className="mt-2 space-y-1">
            <p className="text-xs text-orange-600 dark:text-orange-400">
              ✓ Whale monitoring active (live BASE transactions)
            </p>
            <p className="text-xs text-orange-600 dark:text-orange-400">
              ✓ Market research and trading signals functional
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}