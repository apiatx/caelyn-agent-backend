import { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Loader2, ExternalLink, Wallet, TrendingUp, DollarSign, Globe, CheckCircle } from 'lucide-react';
import { useDeBankPortfolio } from '@/hooks/use-debank-portfolio';

interface DeBankPortfolioSectionProps {
  initialWalletAddress?: string;
}

export function DeBankPortfolioSection({ initialWalletAddress = '' }: DeBankPortfolioSectionProps) {
  const [walletAddress, setWalletAddress] = useState(initialWalletAddress);
  const [currentWallet, setCurrentWallet] = useState(initialWalletAddress);

  const { data: portfolioData, isLoading, error, refetch } = useDeBankPortfolio(currentWallet);

  const handleWalletSubmit = () => {
    if (walletAddress.trim()) {
      setCurrentWallet(walletAddress.trim());
    }
  };

  const formatValue = (value: number) => {
    if (value >= 1000000) {
      return `$${(value / 1000000).toFixed(2)}M`;
    }
    if (value >= 1000) {
      return `$${(value / 1000).toFixed(2)}K`;
    }
    return `$${value.toFixed(2)}`;
  };

  const openDeBankProfile = () => {
    if (currentWallet) {
      window.open(`https://debank.com/profile/${currentWallet}`, '_blank');
    }
  };

  return (
    <div className="space-y-6">
      {/* Wallet Input Section */}
      <Card className="glass-card">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Wallet className="h-5 w-5" />
            Multi-Chain Portfolio Tracker
          </CardTitle>
          <CardDescription>
            Enter a wallet address to view authentic portfolio data across BASE and Ethereum networks
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div className="flex gap-3">
              <Input
                placeholder="Enter wallet address (0x...)"
                value={walletAddress}
                onChange={(e) => setWalletAddress(e.target.value)}
                className="flex-1"
              />
              <Button 
                onClick={handleWalletSubmit}
                disabled={!walletAddress.trim() || isLoading}
              >
                {isLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  'Track Wallet'
                )}
              </Button>
              {currentWallet && (
                <Button variant="outline" onClick={openDeBankProfile}>
                  <ExternalLink className="h-4 w-4" />
                </Button>
              )}
            </div>
            <div className="text-xs text-white/35 space-y-1">
              <div>Your wallet is pre-loaded and ready to track!</div>
              <div className="text-green-400">
                ✓ Currently tracking: {initialWalletAddress ? initialWalletAddress.slice(0, 10) + '...' : 'No wallet set'}
              </div>
              <div>Alternative: 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 (Vitalik)</div>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Loading State */}
      {isLoading && currentWallet && (
        <Card className="glass-card">
          <CardContent className="pt-6">
            <div className="flex items-center justify-center space-x-2">
              <Loader2 className="h-6 w-6 animate-spin" />
              <span>Fetching authentic blockchain data...</span>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Error State */}
      {error && (
        <Card className="glass-card border-red-500/20 bg-red-500/5">
          <CardContent className="pt-6">
            <div className="text-center text-red-400">
              <p className="font-medium">Failed to fetch portfolio</p>
              <p className="text-sm mt-1">
                Unable to retrieve authentic portfolio data from blockchain APIs
              </p>
              <Button variant="outline" onClick={() => refetch()} className="mt-3">
                Try Again
              </Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Portfolio Data */}
      {portfolioData?.success && portfolioData.data && (
        <>
          {/* Portfolio Overview */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Card className="glass-card">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <DollarSign className="h-4 w-4" />
                  Total Portfolio Value
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-green-400">
                  {formatValue(portfolioData.data.totalValue)}
                </div>
              </CardContent>
            </Card>

            <Card className="glass-card">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <TrendingUp className="h-4 w-4 text-blue-400" />
                  BASE Network
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-blue-400">
                  {formatValue(portfolioData.data.baseValue)}
                </div>
              </CardContent>
            </Card>

            <Card className="glass-card">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <TrendingUp className="h-4 w-4 text-purple-400" />
                  TAO Network
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold text-purple-400">
                  {formatValue(portfolioData.data.taoValue)}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Implementation Status */}
          <Card className="glass-card border-blue-500/20 bg-blue-500/5">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-blue-400">
                <CheckCircle className="h-5 w-5" />
                Free API Implementation
              </CardTitle>
              <CardDescription className="text-blue-300">
                Cost-effective alternative to DeBank's $200 API
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-green-400">
                    <CheckCircle className="h-4 w-4" />
                    <span>Etherscan & Basescan APIs</span>
                  </div>
                  <div className="flex items-center gap-2 text-green-400">
                    <CheckCircle className="h-4 w-4" />
                    <span>Authentic blockchain data</span>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-green-400">
                    <CheckCircle className="h-4 w-4" />
                    <span>Real-time ETH pricing</span>
                  </div>
                  <div className="flex items-center gap-2 text-green-400">
                    <CheckCircle className="h-4 w-4" />
                    <span>DeBank-style interface</span>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Chain Distribution */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Globe className="h-5 w-5" />
                Chain Distribution
              </CardTitle>
              <CardDescription>Assets across different blockchain networks</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {portfolioData.data.chains.map((chain, index) => (
                  <div key={index} className="text-center space-y-2">
                    {chain.logo && (
                      <img 
                        src={chain.logo} 
                        alt={chain.name}
                        className="w-8 h-8 mx-auto rounded-full"
                      />
                    )}
                    <div>
                      <div className="font-medium">{chain.name}</div>
                      <div className="text-sm text-green-400">
                        {formatValue(chain.value)}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* Top Holdings */}
          <Card className="glass-card">
            <CardHeader>
              <CardTitle>Top Holdings</CardTitle>
              <CardDescription>
                Largest token positions in the portfolio
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {portfolioData.data.topTokens.map((token, index) => (
                  <div key={index} className="flex items-center justify-between p-3 rounded-lg bg-white/5 hover:bg-white/10 transition-colors">
                    <div className="flex items-center gap-3">
                      {token.logo && (
                        <img 
                          src={token.logo} 
                          alt={token.symbol}
                          className="w-8 h-8 rounded-full"
                        />
                      )}
                      <div>
                        <div className="font-medium">{token.symbol}</div>
                        <div className="text-sm text-white/35">{token.name}</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="font-medium text-green-400">
                        {formatValue(token.value)}
                      </div>
                      <div className="text-sm text-white/35">
                        {token.amount.toFixed(4)} @ ${token.price.toFixed(4)}
                      </div>
                    </div>
                    <Badge variant="outline" className="ml-2">
                      {token.chain.toUpperCase()}
                    </Badge>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          {/* External Links */}
          <Card className="glass-card">
            <CardContent className="pt-6">
              <div className="text-center space-y-3">
                <p className="text-sm text-white/35">
                  View complete portfolio analysis on external platforms
                </p>
                <div className="flex gap-3 justify-center">
                  <Button onClick={openDeBankProfile} variant="outline" className="gap-2">
                    <ExternalLink className="h-4 w-4" />
                    DeBank.com
                  </Button>
                  <Button 
                    onClick={() => window.open(`https://etherscan.io/address/${currentWallet}`, '_blank')}
                    variant="outline" 
                    className="gap-2"
                  >
                    <ExternalLink className="h-4 w-4" />
                    Etherscan
                  </Button>
                  <Button 
                    onClick={() => window.open(`https://basescan.org/address/${currentWallet}`, '_blank')}
                    variant="outline" 
                    className="gap-2"
                  >
                    <ExternalLink className="h-4 w-4" />
                    Basescan
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </>
      )}

      {/* No Data State */}
      {!currentWallet && (
        <Card className="glass-card">
          <CardContent className="pt-6">
            <div className="text-center text-white/35">
              <Wallet className="h-12 w-12 mx-auto mb-4 opacity-50" />
              <p>Enter a wallet address to view portfolio data from DeBank</p>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}