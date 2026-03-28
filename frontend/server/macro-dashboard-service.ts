import { fmpService } from './fmp-service';

const BENCHMARK_ETFS = ['SPY', 'QQQ', 'TLT', 'GLD', 'USO', 'HYG'];

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

interface MacroDashboardResponse {
  benchmark_etfs: BenchmarkETF[];
  vix: VIXData;
  last_updated: string;
}

// Cache with 2-minute TTL
let cachedDashboard: { data: MacroDashboardResponse; timestamp: number } | null = null;
const CACHE_TTL = 2 * 60 * 1000;

export const macroDashboardService = {
  async getDashboard(): Promise<MacroDashboardResponse> {
    if (cachedDashboard && Date.now() - cachedDashboard.timestamp < CACHE_TTL) {
      return cachedDashboard.data;
    }

    const [etfQuotes, vixQuotes] = await Promise.all([
      fmpService.getQuotes(BENCHMARK_ETFS),
      fmpService.getStockDetails(['VIX'], { VIX: 'index' }),
    ]);

    const benchmark_etfs: BenchmarkETF[] = etfQuotes.map((q) => ({
      ticker: q.symbol,
      price: q.price,
      change_pct: q.changesPercentage,
      week_52_high: q.yearHigh,
      pct_from_52w_high: q.yearHigh > 0
        ? parseFloat((((q.price - q.yearHigh) / q.yearHigh) * 100).toFixed(1))
        : 0,
    }));

    const vixQuote = vixQuotes[0];
    const vix: VIXData = {
      current: vixQuote?.price || 0,
      change: vixQuote?.change || 0,
      change_pct: vixQuote?.changesPercentage || 0,
    };

    const result: MacroDashboardResponse = {
      benchmark_etfs,
      vix,
      last_updated: new Date().toISOString(),
    };

    cachedDashboard = { data: result, timestamp: Date.now() };
    return result;
  },
};
