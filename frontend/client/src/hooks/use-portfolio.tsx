import { useQuery } from "@tanstack/react-query";
import type { Portfolio, Holding } from "@shared/schema";

interface PortfolioWithHoldings extends Portfolio {
  holdings?: Holding[];
}

export function usePortfolio(userId: number) {
  return useQuery<PortfolioWithHoldings>({
    queryKey: ["/api/portfolio", userId],
    enabled: !!userId,
  });
}
