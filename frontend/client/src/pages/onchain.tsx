import { useEffect } from 'react';
import { useLocation } from 'wouter';

export default function OnchainPage() {
  const [, setLocation] = useLocation();

  useEffect(() => {
    // Redirect to analytics subpage as default
    setLocation('/app/onchain/analytics');
  }, [setLocation]);

  return (
    <div className="min-h-screen bg-gradient-to-br from-black via-gray-900 to-black text-white flex items-center justify-center">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-400 mx-auto mb-4"></div>
        <p className="text-crypto-silver">Redirecting to Onchain Alpha...</p>
      </div>
    </div>
  );
}