import { useQuery } from "@tanstack/react-query";

export interface TopMover {
  token: string;
  symbol: string;
  price: number;
  change24h: number;
  volume24h: number;
  marketCap: number;
  network: 'BASE' | 'ETH';
  contractAddress?: string;
}

export interface WhaleTransaction {
  id: string;
  token: string;
  amount: string;
  amountUsd: string;
  fromAddress: string;
  toAddress: string;
  txHash: string;
  timestamp: string;
  network: 'BASE' | 'TAO';
  action: 'BUY' | 'SELL' | 'TRANSFER';
}

export interface SocialMention {
  token: string;
  mentions: number;
  sentiment: 'positive' | 'negative' | 'neutral';
  volume24h: number;
  trendingScore: number;
  sources: string[];
}

export interface MarketAnalysis {
  topMovers: TopMover[];
  socialSentiment: SocialMention[];
  whaleActivity: WhaleTransaction[];
  aiAnalysis: {
    sentiment: {
      overall: 'bullish' | 'bearish' | 'neutral';
      confidence: number;
      analysis: string;
      signals: string[];
    };
    signals: {
      token: string;
      action: 'buy' | 'sell' | 'hold';
      confidence: number;
      reasoning: string;
      priceTarget?: string;
      timeframe: string;
    }[];
    trends: string;
  };
  lastUpdated: string;
}

export interface PortfolioOptimization {
  recommendations: string[];
  riskLevel: 'low' | 'medium' | 'high';
  suggestedAllocations: { token: string; percentage: number }[];
  reasoning: string;
}

// Hook for top 24h movers - updated for 5-minute refresh as requested
export function useTopMovers() {
  return useQuery({
    queryKey: ['/api/real-time/top-movers'],
    refetchInterval: 300000, // Refresh every 5 minutes (300 seconds)
    staleTime: 300000,
  });
}

// Hook for real-time whale activity
export function useWhaleActivity() {
  return useQuery({
    queryKey: ['/api/real-time/whale-activity'],
    refetchInterval: 15000, // Refresh every 15 seconds
    staleTime: 15000,
  });
}

// Hook for social sentiment data
export function useSocialSentiment() {
  return useQuery({
    queryKey: ['/api/real-time/social-sentiment'],
    refetchInterval: 30000, // Refresh every 30 seconds
    staleTime: 30000,
  });
}

// Hook for comprehensive market analysis with AI - aligned with top movers refresh
export function useMarketAnalysis() {
  return useQuery({
    queryKey: ['/api/real-time/market-analysis'],
    refetchInterval: 300000, // Refresh every 5 minutes to match top movers
    staleTime: 300000,
  });
}

// Hook for AI-powered portfolio optimization
export function usePortfolioOptimization(portfolioId: number) {
  return useQuery({
    queryKey: ['/api/real-time/portfolio-optimization', portfolioId],
    refetchInterval: 300000, // Refresh every 5 minutes
    staleTime: 300000,
    enabled: !!portfolioId,
  });
}