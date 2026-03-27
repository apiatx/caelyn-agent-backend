import { useQuery } from "@tanstack/react-query";
import type { PortfolioValueHistory } from "@shared/schema";

export function usePortfolioValueHistory(portfolioId: number, timeframe: string = '24h') {
  // Calculate limit based on timeframe
  const getLimitForTimeframe = (timeframe: string) => {
    switch (timeframe) {
      case '24h': return 1440; // 24 hours * 60 minutes
      case '7d': return 10080; // 7 days * 1440 minutes
      case '30d': return 43200; // 30 days * 1440 minutes
      case '90d': return 129600; // 90 days * 1440 minutes
      case 'ytd': return 365 * 1440; // 365 days * 1440 minutes
      case 'all': return 1000000; // Large number for all data
      default: return 1440;
    }
  };

  const limit = getLimitForTimeframe(timeframe);

  return useQuery<PortfolioValueHistory[]>({
    queryKey: ['/api/portfolio', portfolioId, 'value-history', timeframe, limit],
    queryFn: async () => {
      const response = await fetch(`/api/portfolio/${portfolioId}/value-history?limit=${limit}`);
      if (!response.ok) {
        throw new Error('Failed to fetch portfolio value history');
      }
      return response.json();
    },
    refetchInterval: 30000, // Refetch every 30 seconds for real-time updates
  });
}