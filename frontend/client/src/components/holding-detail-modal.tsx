import { useState } from "react";
import { X, ExternalLink, TrendingUp, TrendingDown } from "lucide-react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import type { Holding, Subnet } from "@shared/schema";

interface HoldingDetailModalProps {
  holding: Holding | null;
  subnets?: Subnet[];
  onClose: () => void;
}

export function HoldingDetailModal({ holding, subnets, onClose }: HoldingDetailModalProps) {
  if (!holding) return null;

  const isProfit = parseFloat(holding.pnl) >= 0;
  const totalValue = parseFloat(holding.amount) * parseFloat(holding.currentPrice);

  return (
    <Dialog open={!!holding} onOpenChange={onClose}>
      <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto glass-card-dark border-white/[0.08]">
        <DialogHeader>
          <DialogTitle className="text-2xl font-bold text-white flex items-center">
            <div className={`w-12 h-12 rounded-full mr-4 flex items-center justify-center text-lg font-bold ${
              holding.network === 'BASE' 
                ? 'bg-blue-500' 
                : 'bg-gradient-to-r from-purple-500 to-pink-500'
            }`}>
              {holding.symbol}
            </div>
            {holding.symbol} Holding Details
          </DialogTitle>
        </DialogHeader>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-6">
          {/* Position Overview */}
          <GlassCard className="p-6">
            <h3 className="text-lg font-semibold text-white mb-4">Position Overview</h3>
            <div className="space-y-4">
              <div className="flex justify-between items-center">
                <span className="text-crypto-silver">Amount</span>
                <span className="text-white font-medium">{holding.amount} {holding.symbol}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-crypto-silver">Entry Price</span>
                <span className="text-white font-medium">${holding.entryPrice}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-crypto-silver">Current Price</span>
                <span className="text-white font-medium">${holding.currentPrice}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-crypto-silver">Total Value</span>
                <span className="text-white font-semibold">${totalValue.toFixed(2)}</span>
              </div>
              <div className="border-t border-white/[0.06] pt-4">
                <div className="flex justify-between items-center">
                  <span className="text-crypto-silver">Unrealized PnL</span>
                  <div className="text-right">
                    <div className={`font-semibold ${isProfit ? 'text-crypto-success' : 'text-crypto-danger'}`}>
                      {isProfit ? '+' : ''}${holding.pnl}
                    </div>
                    <div className={`text-sm ${isProfit ? 'text-crypto-success' : 'text-crypto-danger'}`}>
                      {isProfit ? '+' : ''}{holding.pnlPercentage}%
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </GlassCard>

          {/* Performance Metrics */}
          <GlassCard className="p-6">
            <h3 className="text-lg font-semibold text-white mb-4">Performance Metrics</h3>
            <div className="space-y-4">
              <div className="flex items-center justify-between p-3 backdrop-blur-sm bg-white/5 rounded-xl">
                <div className="flex items-center">
                  {isProfit ? (
                    <TrendingUp className="text-crypto-success mr-3 h-5 w-5" />
                  ) : (
                    <TrendingDown className="text-crypto-danger mr-3 h-5 w-5" />
                  )}
                  <div>
                    <div className="text-white font-medium">24h Change</div>
                    <div className="text-crypto-silver text-sm">Price movement</div>
                  </div>
                </div>
                <div className={`text-lg font-semibold ${isProfit ? 'text-crypto-success' : 'text-crypto-danger'}`}>
                  {isProfit ? '+' : ''}3.24%
                </div>
              </div>
              
              <div className="flex items-center justify-between p-3 backdrop-blur-sm bg-white/5 rounded-xl">
                <div className="flex items-center">
                  <div className="w-5 h-5 bg-crypto-warning rounded-full mr-3"></div>
                  <div>
                    <div className="text-white font-medium">Volatility</div>
                    <div className="text-crypto-silver text-sm">7-day average</div>
                  </div>
                </div>
                <div className="text-lg font-semibold text-crypto-warning">
                  12.4%
                </div>
              </div>
            </div>
          </GlassCard>
        </div>

        {/* TAO Subnet Details (if TAO holding) */}
        {holding.network === 'TAO' && subnets && (
          <GlassCard className="p-6 mt-6">
            <h3 className="text-lg font-semibold text-white mb-4">Subnet Allocation</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              {subnets.map((subnet) => (
                <div key={subnet.id} className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center">
                      <div className={`w-8 h-8 rounded-full mr-3 flex items-center justify-center text-xs font-bold ${
                        subnet.netuid === 1 ? 'bg-gradient-to-r from-blue-500 to-purple-500' :
                        subnet.netuid === 18 ? 'bg-gradient-to-r from-green-500 to-blue-500' :
                        'bg-gradient-to-r from-purple-500 to-pink-500'
                      }`}>
                        {subnet.netuid}
                      </div>
                      <div>
                        <h4 className="text-white font-medium text-sm">{subnet.name}</h4>
                        <p className="text-crypto-silver text-xs">{subnet.description}</p>
                      </div>
                    </div>
                  </div>
                  <div className="space-y-2 text-xs">
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">Stake Weight</span>
                      <span className="text-white">{subnet.stakeWeight}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">Emissions</span>
                      <span className="text-crypto-success">+{subnet.emissions} TAO</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-crypto-silver">24h PnL</span>
                      <span className="text-crypto-success">+{subnet.pnl24h}%</span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </GlassCard>
        )}

        {/* Action Buttons */}
        <div className="flex justify-between items-center mt-6 pt-6 border-t border-white/[0.06]">
          <Button variant="ghost" className="text-crypto-silver hover:text-white">
            <ExternalLink className="h-4 w-4 mr-2" />
            View on Explorer
          </Button>
          <Button onClick={onClose} className="bg-crypto-silver text-crypto-black hover:bg-white">
            Close
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}