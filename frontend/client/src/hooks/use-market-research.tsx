import { useQuery } from "@tanstack/react-query";
import type { MarketInsight, TradeSignal } from "@shared/schema";

export function useMarketResearch() {
  const { data: insights, isLoading: insightsLoading } = useQuery<MarketInsight[]>({
    queryKey: ["/api/market-insights"],
  });

  const { data: signals, isLoading: signalsLoading } = useQuery<TradeSignal[]>({
    queryKey: ["/api/trade-signals"],
  });

  return {
    insights,
    signals,
    isLoading: insightsLoading || signalsLoading,
  };
}
