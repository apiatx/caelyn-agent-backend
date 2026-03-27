import { useQuery } from '@tanstack/react-query';

export interface DeBankToken {
  symbol: string;
  name: string;
  amount: number;
  price: number;
  value: number;
  chain: string;
  logo?: string;
}

export interface DeBankChain {
  name: string;
  value: number;
  logo: string;
}

export interface DeBankPortfolioData {
  totalValue: number;
  baseValue: number;
  taoValue: number;
  chains: DeBankChain[];
  topTokens: DeBankToken[];
}

export interface DeBankPortfolioResponse {
  success: boolean;
  data: DeBankPortfolioData;
  rawData?: any;
  message?: string;
  error?: string;
}

export function useDeBankPortfolio(walletAddress: string | null) {
  return useQuery<DeBankPortfolioResponse>({
    queryKey: ['/api/debank/portfolio', walletAddress],
    enabled: !!walletAddress && walletAddress.length > 0,
    refetchInterval: 5000, // Refresh every 5 seconds for real-time updates
    staleTime: 0, // Always consider data stale
    retry: 3,
  });
}

export function useDeBankTokens(walletAddress: string | null, chainId?: string) {
  return useQuery({
    queryKey: ['/api/debank/tokens', walletAddress, chainId],
    enabled: !!walletAddress && walletAddress.length > 0,
    refetchInterval: 30000,
    retry: 3,
  });
}

export function useDeBankProtocols(walletAddress: string | null) {
  return useQuery({
    queryKey: ['/api/debank/protocols', walletAddress],
    enabled: !!walletAddress && walletAddress.length > 0,
    refetchInterval: 60000, // Refresh every minute for protocols
    retry: 3,
  });
}