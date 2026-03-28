import { useQuery } from '@tanstack/react-query';

interface TrendingTicker {
  symbol: string;
  sentiment: number;
  mentions: number;
  lastUpdated: string;
}

export const useSocialPulse = () => {
  return useQuery<TrendingTicker[]>({
    queryKey: ['/api/social-pulse'],
    refetchInterval: 60 * 60 * 1000, // Refetch every hour
    staleTime: 30 * 60 * 1000, // Consider data stale after 30 minutes
  });
};