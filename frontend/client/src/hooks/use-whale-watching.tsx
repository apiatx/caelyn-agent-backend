import { useQuery } from "@tanstack/react-query";
import type { PremiumAccess, WhaleTransaction } from "@shared/schema";

interface WhaleWatchingData {
  hasAccess: boolean;
  access?: PremiumAccess;
}

export function useWhaleWatching(userId: number) {
  const { data: accessData } = useQuery<WhaleWatchingData>({
    queryKey: ["/api/premium-access", userId, "whale_watching"],
    enabled: !!userId,
  });

  const { data: premiumTransactions } = useQuery<WhaleTransaction[]>({
    queryKey: ["/api/whale-transactions"],
    enabled: accessData?.hasAccess || false,
  });

  // Free whale transactions - always available
  const { data: freeTransactions } = useQuery<WhaleTransaction[]>({
    queryKey: ["/api/whale-transactions"],
    refetchInterval: 5000, // Refresh every 5 seconds
  });

  return {
    data: accessData?.hasAccess,
    premiumTransactions,
    freeTransactions,
  };
}
