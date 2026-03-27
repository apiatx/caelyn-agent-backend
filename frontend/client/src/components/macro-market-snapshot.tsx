import { useQuery } from '@tanstack/react-query';
import { TrendingUp, TrendingDown, Minus, RefreshCw } from 'lucide-react';

interface BenchmarkETF {
  ticker: string;
  price: number;
  change_pct: number;
  week_52_high: number;
  pct_from_52w_high: number;
}

interface VIXData {
  current: number;
  change: number;
  change_pct: number;
}

interface MacroDashboard {
  benchmark_etfs: BenchmarkETF[];
  vix: VIXData;
  last_updated: string;
}

const ETF_NAMES: Record<string, string> = {
  SPY: 'S&P 500',
  QQQ: 'Nasdaq 100',
  TLT: '20+ Yr Treasury',
  GLD: 'Gold',
  USO: 'Crude Oil',
  HYG: 'High Yield Corp',
};

export function MacroMarketSnapshot() {
  const { data, isLoading, error, dataUpdatedAt } = useQuery<MacroDashboard>({
    queryKey: ['/api/macro/dashboard'],
    refetchInterval: 120000,
    staleTime: 60000,
    retry: 3,
    refetchOnWindowFocus: true,
  });

  const formatDate = (iso: string) => {
    const d = new Date(iso);
    return d.toLocaleDateString('en-US', {
      month: 'long',
      day: 'numeric',
      year: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    });
  };

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-6 bg-white/5 rounded w-48" />
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
            {Array.from({ length: 7 }).map((_, i) => (
              <div key={i} className="h-24 bg-white/5 rounded-xl" />
            ))}
          </div>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="p-6 text-center text-white/40">
        Failed to load market data
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-6 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-white tracking-tight">
            Market Snapshot
          </h2>
          <p className="text-xs text-white/40 mt-0.5">
            {data.last_updated ? formatDate(data.last_updated) : '—'}
          </p>
        </div>
        <div className="flex items-center gap-1.5 text-[10px] text-white/30">
          <RefreshCw className="w-3 h-3" />
          Live
        </div>
      </div>

      {/* ETF Grid + VIX */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {data.benchmark_etfs?.map((etf) => (
          <TickerCard key={etf.ticker} etf={etf} />
        ))}
        {data.vix && <VIXCard vix={data.vix} />}
      </div>
    </div>
  );
}

function TickerCard({ etf }: { etf: BenchmarkETF }) {
  const isPositive = etf.change_pct >= 0;
  const icon = isPositive ? (
    <TrendingUp className="w-3.5 h-3.5" />
  ) : (
    <TrendingDown className="w-3.5 h-3.5" />
  );

  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-3.5 hover:border-white/[0.12] transition-colors">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-bold text-white/90 tracking-wide">
          {etf.ticker}
        </span>
        <span className={`flex items-center gap-1 text-xs font-medium ${
          isPositive ? 'text-emerald-400' : 'text-red-400'
        }`}>
          {icon}
          {isPositive ? '+' : ''}{etf.change_pct?.toFixed(2)}%
        </span>
      </div>
      <div className="text-base font-semibold text-white mb-1">
        ${etf.price?.toFixed(2)}
      </div>
      <div className="flex items-center justify-between">
        <span className="text-[10px] text-white/30">
          {ETF_NAMES[etf.ticker] || etf.ticker}
        </span>
        <span className={`text-[10px] ${
          etf.pct_from_52w_high >= -5 ? 'text-white/40' : 'text-amber-400/70'
        }`}>
          {etf.pct_from_52w_high?.toFixed(1)}% from 52WH
        </span>
      </div>
    </div>
  );
}

function VIXCard({ vix }: { vix: VIXData }) {
  const level = vix.current >= 30 ? 'high' : vix.current >= 20 ? 'elevated' : 'low';
  const levelColor = level === 'high'
    ? 'text-red-400 border-red-500/20 bg-red-500/5'
    : level === 'elevated'
      ? 'text-amber-400 border-amber-500/20 bg-amber-500/5'
      : 'text-emerald-400 border-emerald-500/20 bg-emerald-500/5';

  const isDown = vix.change_pct < 0;

  return (
    <div className={`border rounded-xl p-3.5 transition-colors ${levelColor}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-bold tracking-wide">VIX</span>
        <span className={`flex items-center gap-1 text-xs font-medium ${
          isDown ? 'text-emerald-400' : 'text-red-400'
        }`}>
          {isDown ? <TrendingDown className="w-3.5 h-3.5" /> : <TrendingUp className="w-3.5 h-3.5" />}
          {vix.change_pct >= 0 ? '+' : ''}{vix.change_pct?.toFixed(2)}%
        </span>
      </div>
      <div className="text-base font-semibold mb-1">
        {vix.current?.toFixed(2)}
      </div>
      <div className="flex items-center justify-between">
        <span className="text-[10px] opacity-60">Volatility Index</span>
        <span className="text-[10px] opacity-60 capitalize">{level}</span>
      </div>
    </div>
  );
}
