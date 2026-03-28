import { useQuery } from '@tanstack/react-query';

export interface StakedPosition {
  protocol: string;
  protocolUrl: string;
  totalValue: number;
  positions: {
    token: string;
    symbol: string;
    amount: number;
    price: number;
    value: number;
    unlockTime?: string;
  }[];
}

export interface StakingData {
  totalStakedValue: number;
  protocols: StakedPosition[];
}

export function useStakingData(walletAddress: string | null) {
  return useQuery({
    queryKey: ['/api/staking', walletAddress],
    queryFn: async (): Promise<StakingData> => {
      if (!walletAddress) {
        return { totalStakedValue: 0, protocols: [] };
      }
      
      const response = await fetch(`/api/staking/${walletAddress}`);
      if (!response.ok) {
        throw new Error('Failed to fetch staking data');
      }
      return response.json();
    },
    enabled: !!walletAddress,
    refetchInterval: 30000, // Refresh every 30 seconds for real-time updates
    staleTime: 15000, // Consider data stale after 15 seconds
  });
}