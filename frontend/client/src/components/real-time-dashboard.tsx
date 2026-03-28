import { useState } from "react";
import { TrendingUp, TrendingDown, Activity, Brain, Target, AlertTriangle } from "lucide-react";
import { GlassCard } from "@/components/ui/glass-card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Progress } from "@/components/ui/progress";
import { useTopMovers, useWhaleActivity, useSocialSentiment, useMarketAnalysis, usePortfolioOptimization } from "@/hooks/use-real-time-data";

export default function RealTimeDashboard({ portfolioId }: { portfolioId?: number }) {
  const { data: topMovers, isLoading: loadingMovers } = useTopMovers();
  const { data: whaleActivity, isLoading: loadingWhales } = useWhaleActivity();
  const { data: socialSentiment, isLoading: loadingSocial } = useSocialSentiment();
  const { data: marketAnalysis, isLoading: loadingAnalysis } = useMarketAnalysis();
  const { data: portfolioOptimization, isLoading: loadingOptimization } = usePortfolioOptimization(portfolioId || 1);

  return (
    <div className="space-y-6">
      {/* Real-Time Status Header */}
      <GlassCard className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <div className="w-3 h-3 bg-green-500 rounded-full animate-pulse"></div>
            <h2 className="text-xl font-semibold text-white">Real-Time Market Intelligence</h2>
          </div>
          <div className="flex items-center space-x-4 text-sm text-crypto-silver">
            <span>Last Update: {new Date().toLocaleTimeString()}</span>
            <Badge variant="outline" className="text-green-500 border-green-500">LIVE</Badge>
          </div>
        </div>
      </GlassCard>

      <Tabs defaultValue="movers" className="w-full">
        <TabsList className="grid w-full grid-cols-5">
          <TabsTrigger value="movers">Top Movers</TabsTrigger>
          <TabsTrigger value="whales">Whale Activity</TabsTrigger>
          <TabsTrigger value="sentiment">Social Sentiment</TabsTrigger>
          <TabsTrigger value="ai-analysis">AI Analysis</TabsTrigger>
          <TabsTrigger value="optimization">Portfolio AI</TabsTrigger>
        </TabsList>

        {/* Top 24h Movers */}
        <TabsContent value="movers">
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <TrendingUp className="text-crypto-success mr-3 h-5 w-5" />
                Top 24h Movers (Real-Time)
              </h3>
              <div className="text-sm text-crypto-silver">
                {loadingMovers ? 'Loading...' : `${topMovers?.length || 0} assets tracked`}
              </div>
            </div>
            
            <div className="space-y-3">
              {loadingMovers ? (
                <div className="text-center py-8">
                  <Activity className="w-8 h-8 text-crypto-silver mx-auto mb-2 animate-spin" />
                  <p className="text-crypto-silver">Loading real-time market data...</p>
                </div>
              ) : (
                topMovers?.map((mover, index) => (
                  <div key={index} className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-3">
                        <div className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold ${
                          mover.network === 'BASE' ? 'bg-blue-500' : 'bg-gray-500'
                        }`}>
                          {mover.network}
                        </div>
                        <div>
                          <h4 className="font-medium text-white">{mover.token}</h4>
                          <p className="text-sm text-crypto-silver">{mover.symbol}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-white font-medium">${mover.price.toFixed(6)}</div>
                        <div className={`flex items-center text-sm ${
                          mover.change24h >= 0 ? 'text-crypto-success' : 'text-red-500'
                        }`}>
                          {mover.change24h >= 0 ? <TrendingUp className="w-4 h-4 mr-1" /> : <TrendingDown className="w-4 h-4 mr-1" />}
                          {mover.change24h.toFixed(2)}%
                        </div>
                      </div>
                    </div>
                    <div className="mt-2 grid grid-cols-2 gap-4 text-xs text-crypto-silver">
                      <div>Volume: ${mover.volume24h.toLocaleString()}</div>
                      <div>Market Cap: ${mover.marketCap.toLocaleString()}</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </GlassCard>
        </TabsContent>

        {/* Real-Time Whale Activity */}
        <TabsContent value="whales">
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <Activity className="text-blue-500 mr-3 h-5 w-5" />
                Live Whale Transactions
              </h3>
              <div className="text-sm text-crypto-silver">
                {loadingWhales ? 'Loading...' : `${whaleActivity?.length || 0} recent transactions`}
              </div>
            </div>
            
            <div className="space-y-3">
              {loadingWhales ? (
                <div className="text-center py-8">
                  <Activity className="w-8 h-8 text-crypto-silver mx-auto mb-2 animate-spin" />
                  <p className="text-crypto-silver">Loading whale activity...</p>
                </div>
              ) : (
                whaleActivity?.map((tx, index) => (
                  <div key={index} className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center space-x-3">
                        <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                          tx.action === 'BUY' ? 'bg-green-500' : tx.action === 'SELL' ? 'bg-red-500' : 'bg-blue-500'
                        }`}>
                          {tx.action === 'BUY' ? '💰' : tx.action === 'SELL' ? '📉' : '🔄'}
                        </div>
                        <div>
                          <h4 className="font-medium text-white">{tx.token} {tx.action}</h4>
                          <p className="text-sm text-crypto-silver">{new Date(tx.timestamp).toLocaleString()}</p>
                        </div>
                      </div>
                      <div className="text-right">
                        <div className="text-crypto-success font-medium">${parseFloat(tx.amountUsd).toLocaleString()}</div>
                        <div className="text-crypto-silver text-sm">{parseFloat(tx.amount).toLocaleString()} {tx.token}</div>
                      </div>
                    </div>
                    <div className="text-xs text-crypto-silver truncate">
                      TX: {tx.txHash}
                    </div>
                  </div>
                ))
              )}
            </div>
          </GlassCard>
        </TabsContent>

        {/* Social Sentiment Analysis */}
        <TabsContent value="sentiment">
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <Target className="text-purple-500 mr-3 h-5 w-5" />
                Real-Time Social Sentiment
              </h3>
              <div className="text-sm text-crypto-silver">
                {loadingSocial ? 'Loading...' : `${socialSentiment?.length || 0} tokens monitored`}
              </div>
            </div>
            
            <div className="space-y-3">
              {loadingSocial ? (
                <div className="text-center py-8">
                  <Activity className="w-8 h-8 text-crypto-silver mx-auto mb-2 animate-spin" />
                  <p className="text-crypto-silver">Analyzing social sentiment...</p>
                </div>
              ) : (
                socialSentiment?.map((mention, index) => (
                  <div key={index} className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center space-x-3">
                        <Badge 
                          variant="outline" 
                          className={`${
                            mention.sentiment === 'positive' 
                              ? 'text-green-500 border-green-500' 
                              : mention.sentiment === 'negative' 
                              ? 'text-red-500 border-red-500' 
                              : 'text-yellow-500 border-yellow-500'
                          }`}
                        >
                          {mention.sentiment.toUpperCase()}
                        </Badge>
                        <h4 className="font-medium text-white">{mention.token}</h4>
                      </div>
                      <div className="text-right">
                        <div className="text-white font-medium">{mention.mentions} mentions</div>
                        <div className="text-crypto-silver text-sm">Trending: {mention.trendingScore.toFixed(0)}/100</div>
                      </div>
                    </div>
                    <Progress value={mention.trendingScore} className="mb-2" />
                    <div className="text-xs text-crypto-silver">
                      Sources: {mention.sources.join(', ')} • Volume: ${mention.volume24h.toLocaleString()}
                    </div>
                  </div>
                ))
              )}
            </div>
          </GlassCard>
        </TabsContent>

        {/* AI Market Analysis */}
        <TabsContent value="ai-analysis">
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <Brain className="text-pink-500 mr-3 h-5 w-5" />
                AI Market Analysis
              </h3>
              <Badge variant="outline" className="text-pink-500 border-pink-500">
                AI POWERED
              </Badge>
            </div>
            
            {loadingAnalysis ? (
              <div className="text-center py-8">
                <Brain className="w-8 h-8 text-crypto-silver mx-auto mb-2 animate-spin" />
                <p className="text-crypto-silver">AI analyzing market data...</p>
              </div>
            ) : marketAnalysis?.aiAnalysis ? (
              <div className="space-y-6">
                {/* Overall Market Sentiment */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">Market Sentiment Analysis</h4>
                  <div className="flex items-center justify-between mb-3">
                    <Badge 
                      variant="outline" 
                      className={`${
                        marketAnalysis.aiAnalysis.sentiment.overall === 'bullish' 
                          ? 'text-green-500 border-green-500' 
                          : marketAnalysis.aiAnalysis.sentiment.overall === 'bearish' 
                          ? 'text-red-500 border-red-500' 
                          : 'text-yellow-500 border-yellow-500'
                      }`}
                    >
                      {marketAnalysis.aiAnalysis.sentiment.overall.toUpperCase()}
                    </Badge>
                    <span className="text-crypto-silver">
                      Confidence: {Math.round(marketAnalysis.aiAnalysis.sentiment.confidence * 100)}%
                    </span>
                  </div>
                  <p className="text-crypto-silver text-sm mb-3">{marketAnalysis.aiAnalysis.sentiment.analysis}</p>
                  <div className="space-y-1">
                    {marketAnalysis.aiAnalysis.sentiment.signals.map((signal, i) => (
                      <div key={i} className="text-xs text-white bg-black/20 rounded px-2 py-1">
                        • {signal}
                      </div>
                    ))}
                  </div>
                </div>

                {/* AI Trading Signals */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">AI Trading Signals</h4>
                  <div className="space-y-3">
                    {marketAnalysis.aiAnalysis.signals.map((signal, i) => (
                      <div key={i} className="bg-black/20 rounded-lg p-3">
                        <div className="flex items-center justify-between mb-2">
                          <div className="flex items-center space-x-2">
                            <Badge 
                              variant="outline" 
                              className={`${
                                signal.action === 'buy' 
                                  ? 'text-green-500 border-green-500' 
                                  : signal.action === 'sell' 
                                  ? 'text-red-500 border-red-500' 
                                  : 'text-yellow-500 border-yellow-500'
                              }`}
                            >
                              {signal.action.toUpperCase()}
                            </Badge>
                            <span className="text-white font-medium">{signal.token}</span>
                          </div>
                          <span className="text-crypto-silver text-sm">
                            {Math.round(signal.confidence * 100)}% confidence
                          </span>
                        </div>
                        <p className="text-crypto-silver text-sm mb-1">{signal.reasoning}</p>
                        <div className="text-xs text-crypto-silver">
                          Timeframe: {signal.timeframe}
                          {signal.priceTarget && ` • Target: ${signal.priceTarget}`}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Market Trends */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">Market Trend Analysis</h4>
                  <p className="text-crypto-silver text-sm whitespace-pre-wrap">{marketAnalysis.aiAnalysis.trends}</p>
                </div>
              </div>
            ) : (
              <div className="text-center py-8">
                <AlertTriangle className="w-8 h-8 text-yellow-500 mx-auto mb-2" />
                <p className="text-crypto-silver">AI analysis unavailable</p>
              </div>
            )}
          </GlassCard>
        </TabsContent>

        {/* Portfolio Optimization */}
        <TabsContent value="optimization">
          <GlassCard className="p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-lg font-semibold text-white flex items-center">
                <Target className="text-blue-500 mr-3 h-5 w-5" />
                AI Portfolio Optimization
              </h3>
              <Badge variant="outline" className="text-blue-500 border-blue-500">
                PERSONALIZED
              </Badge>
            </div>
            
            {loadingOptimization ? (
              <div className="text-center py-8">
                <Brain className="w-8 h-8 text-crypto-silver mx-auto mb-2 animate-spin" />
                <p className="text-crypto-silver">Optimizing your portfolio...</p>
              </div>
            ) : portfolioOptimization ? (
              <div className="space-y-6">
                {/* Risk Assessment */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">Risk Assessment</h4>
                  <Badge 
                    variant="outline" 
                    className={`${
                      portfolioOptimization.riskLevel === 'low' 
                        ? 'text-green-500 border-green-500' 
                        : portfolioOptimization.riskLevel === 'high' 
                        ? 'text-red-500 border-red-500' 
                        : 'text-yellow-500 border-yellow-500'
                    }`}
                  >
                    {portfolioOptimization.riskLevel.toUpperCase()} RISK
                  </Badge>
                  <p className="text-crypto-silver text-sm mt-2">{portfolioOptimization.reasoning}</p>
                </div>

                {/* Recommendations */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">AI Recommendations</h4>
                  <div className="space-y-2">
                    {portfolioOptimization.recommendations.map((rec, i) => (
                      <div key={i} className="text-sm text-crypto-silver bg-black/20 rounded px-3 py-2">
                        • {rec}
                      </div>
                    ))}
                  </div>
                </div>

                {/* Suggested Allocations */}
                <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] p-4">
                  <h4 className="font-medium text-white mb-3">Suggested Allocations</h4>
                  <div className="space-y-3">
                    {portfolioOptimization.suggestedAllocations.map((allocation, i) => (
                      <div key={i} className="flex items-center justify-between">
                        <span className="text-white">{allocation.token}</span>
                        <div className="flex items-center space-x-2">
                          <Progress value={allocation.percentage} className="w-20" />
                          <span className="text-crypto-silver text-sm">{allocation.percentage}%</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="text-center py-8">
                <AlertTriangle className="w-8 h-8 text-yellow-500 mx-auto mb-2" />
                <p className="text-crypto-silver">Portfolio optimization unavailable</p>
              </div>
            )}
          </GlassCard>
        </TabsContent>
      </Tabs>
    </div>
  );
}