import { AlertCircle } from "lucide-react";

export default function NotFound() {
  return (
    <div className="min-h-screen w-full flex items-center justify-center" style={{ background: '#050608' }}>
      <div className="glass-card p-8 max-w-md mx-4 text-center">
        <AlertCircle className="h-10 w-10 mx-auto mb-4" style={{ color: 'hsl(200, 90%, 58%)' }} />
        <h1 className="text-xl font-bold text-white mb-2">404 — Page Not Found</h1>
        <p className="text-sm text-white/40">
          The page you're looking for doesn't exist or has been moved.
        </p>
      </div>
    </div>
  );
}
