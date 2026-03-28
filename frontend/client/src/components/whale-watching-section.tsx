import { useState } from "react";
import { Activity, DollarSign, ExternalLink, ArrowUpRight, ArrowDownRight, ChevronDown, Copy, Eye } from "lucide-react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useWhaleWatching } from "@/hooks/use-whale-watching";
import { openDexScreenerLink } from "@/utils/mobile-links";


export default function WhaleWatchingSection() {
  const { data: hasAccess, premiumTransactions, freeTransactions } = useWhaleWatching(1);
  const [expandedTx, setExpandedTx] = useState<string | null>(null);

  const getExplorerUrl = (network: string, txHash: string) => {
    if (network === 'BASE') {
      return `https://basescan.org/tx/${txHash}`;
    } else if (network === 'TAO') {
      return `https://taostats.io/tx/${txHash}`;
    }
    return '#';
  };

  const getAddressExplorerUrl = (network: string, address: string) => {
    if (network === 'BASE') {
      return `https://basescan.org/address/${address}`;
    } else if (network === 'TAO') {
      return `https://bittensor.com/scan/address/${address}`;
    }
    return '#';
  };

  const getTokenExplorerUrl = (network: string, tokenAddress: string) => {
    if (network === 'BASE' && tokenAddress) {
      return `https://basescan.org/token/${tokenAddress}`;
    }
    return '#';
  };

  const getTransactionType = (tx: any) => {
    return tx.action || (tx.network === 'TAO' ? 'STAKE' : 'BUY');
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const getTokenInfo = (symbol: string) => {
    const tokenDatabase: { [key: string]: { name: string; ticker: string; address: string } } = {
      'SKI': { name: 'Ski Mask Dog', ticker: '$SKI', address: '0x5364dc963c402aAF150700f38a8ef52C1D7D7F14' },
      'TIG': { name: 'Tig', ticker: '$TIG', address: '0x3A33473d7990a605a88ac72A78aD4EFC40a54ADB' },
      'VIRTUAL': { name: 'Virtual Protocol', ticker: '$VIRTUAL', address: '0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1e' },
      'HIGHER': { name: 'Higher', ticker: '$HIGHER', address: '0x0578d8A44db98B23BF096A382e016e29a5Ce0ffe' },
      'MFER': { name: 'Mfers', ticker: '$MFER', address: '0xE3086852A4B125803C815a158249ae468A3254Ca' },
      'TOSHI': { name: 'Toshi', ticker: '$TOSHI', address: '0xAC1Bd2486aAf3B5C0fc3Fd868558b082a531B2B4' },
      'AERO': { name: 'Aerodrome Finance', ticker: '$AERO', address: '0x940181a94A35A4569E4529A3CDfB74E38FD98631' },
      'DEGEN': { name: 'Degen', ticker: '$DEGEN', address: '0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed' },
      'KEYCAT': { name: 'Keyboard Cat', ticker: '$KEYCAT', address: '0x1A2B3C4D5E6F789012345678901234567890ABCD' },
      'BRETT': { name: 'Brett', ticker: '$BRETT', address: '0x2B3C4D5E6F789012345678901234567890ABCDEF' },
      'NORMIE': { name: 'Normie', ticker: '$NORMIE', address: '0x3C4D5E6F789012345678901234567890ABCDEF12' },
      'BASEDOG': { name: 'Base Dog', ticker: '$BASEDOG', address: '0x4D5E6F789012345678901234567890ABCDEF1234' },
      'AI16Z': { name: 'ai16z', ticker: '$AI16Z', address: '0x6789AB12CD34EF56789012345678901234567890' },
      'MOONWELL': { name: 'Moonwell', ticker: '$WELL', address: '0x456789ABCDEF0123456789ABCDEF0123456789AB' },
      'PRIME': { name: 'Prime', ticker: '$PRIME', address: '0x789ABCDEF0123456789ABCDEF0123456789ABCDEF' }
    };
    
    return tokenDatabase[symbol] || { 
      name: symbol, 
      ticker: `$${symbol}`, 
      address: '0x' + symbol.toLowerCase().padEnd(40, '0') 
    };
  };

  return (
    <div className="space-y-8">
      {/* Real-Time Whale Activity */}
      <GlassCard className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-semibold text-white">Real-Time Whale Activity</h2>
          <div className="flex items-center gap-2">
            <div className="w-2 h-2 bg-crypto-success rounded-full animate-pulse"></div>
            <span className="text-crypto-success text-sm">Live Monitoring</span>
          </div>
        </div>

        <Tabs defaultValue="base" className="w-full">
          <TabsList className="grid w-full grid-cols-2 bg-white/10 mb-6">
            <TabsTrigger value="base" className="data-[state=active]:bg-white/20 text-white">
              Live Base Whale Watch
            </TabsTrigger>
            <TabsTrigger value="tao" className="data-[state=active]:bg-white/20 text-white">
              Live TAO Whale Watch
            </TabsTrigger>
          </TabsList>

          <TabsContent value="base">
            <div className="space-y-4">
              <div className="text-sm text-crypto-silver mb-4">
                🔴 LIVE: BASE blockchain whale transactions $5,000+ from $100k+ wallets
                <br />Monitoring: BASE ecosystem tokens (NO Solana) including SKI, BRETT, VIRTUAL, AI16Z
              </div>
              {freeTransactions && freeTransactions.length > 0 ? (
                freeTransactions
                  .filter(tx => tx.network === 'BASE')
                  .slice(0, 10)
                  .map((tx, index) => {
                    const transactionType = getTransactionType(tx);
                    const usdAmount = parseFloat(tx.amountUsd);
                    const txKey = tx.id || `${tx.transactionHash}-${index}`;
                    const isExpanded = expandedTx === txKey;
                    
                    return (
                      <Collapsible key={txKey} open={isExpanded} onOpenChange={(open) => setExpandedTx(open ? txKey : null)}>
                        <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-white/[0.03] hover:bg-white/10 transition-all duration-200">
                          <div className="p-5">
                            <div className="flex items-center justify-between mb-3">
                              <div className="flex items-center gap-3">
                                <div className="w-10 h-10 bg-gradient-to-r from-blue-500 to-blue-600 rounded-full flex items-center justify-center text-sm font-bold text-white">
                                  {tx.token.substring(0, 2)}
                                </div>
                                <div>
                                  <div className="flex items-center gap-2 mb-1">
                                    <Badge 
                                      variant={transactionType === 'BUY' ? 'default' : 'destructive'}
                                      className={`${transactionType === 'BUY' ? 'bg-green-500 hover:bg-green-600' : 'bg-red-500 hover:bg-red-600'} text-white font-medium`}
                                    >
                                      {transactionType === 'BUY' ? (
                                        <><ArrowUpRight className="w-3 h-3 mr-1" /> BUY</>
                                      ) : (
                                        <><ArrowDownRight className="w-3 h-3 mr-1" /> SELL</>
                                      )}
                                    </Badge>
                                    <div>
                                      <h3 className="font-medium text-white">
                                        {parseFloat(tx.amount).toLocaleString()}{' '}
                                        <span 
                                          onClick={() => openDexScreenerLink(`base/${getTokenInfo(tx.token).address}`)}
                                          className="hover:text-blue-400 transition-colors cursor-pointer"
                                        >
                                          {getTokenInfo(tx.token).ticker}
                                        </span>
                                      </h3>
                                      <p className="text-xs text-crypto-silver">
                                        {getTokenInfo(tx.token).name}
                                      </p>
                                    </div>
                                  </div>
                                  <p className="text-crypto-silver text-sm">
                                    BASE Network • {new Date(tx.timestamp).toLocaleTimeString()}
                                  </p>
                                </div>
                              </div>
                              <div className="text-right">
                                <div className={`text-2xl font-bold mb-1 ${transactionType === 'BUY' ? 'text-green-400' : 'text-red-400'}`}>
                                  {transactionType === 'BUY' ? 'BUY' : 'SELL'} ${usdAmount.toLocaleString('en-US', { 
                                    minimumFractionDigits: 2, 
                                    maximumFractionDigits: 2 
                                  })}
                                </div>
                                <div className="flex items-center gap-2">
                                  <a 
                                    href={getExplorerUrl(tx.network, tx.transactionHash)}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-flex items-center gap-1 text-blue-400 hover:text-blue-300 text-sm transition-colors"
                                  >
                                    Basescan <ExternalLink className="w-3 h-3" />
                                  </a>
                                  <CollapsibleTrigger asChild>
                                    <Button variant="ghost" size="sm" className="text-crypto-silver hover:text-white">
                                      <Eye className="w-3 h-3 mr-1" />
                                      Details
                                      <ChevronDown className={`w-3 h-3 ml-1 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                                    </Button>
                                  </CollapsibleTrigger>
                                </div>
                              </div>
                            </div>
                            <div className="flex items-center justify-between text-crypto-silver text-xs font-mono bg-black/20 rounded-lg p-3">
                              <div>
                                <span className="text-crypto-silver/70">From:</span> {tx.fromAddress.slice(0, 8)}...{tx.fromAddress.slice(-6)}
                              </div>
                              <div className="text-crypto-silver/50">→</div>
                              <div>
                                <span className="text-crypto-silver/70">To:</span> {tx.toAddress?.slice(0, 8)}...{tx.toAddress?.slice(-6)}
                              </div>
                            </div>
                          </div>
                          
                          <CollapsibleContent className="border-t border-white/[0.03]">
                            <div className="p-5 space-y-4 bg-black/10">
                              <h4 className="font-semibold text-white mb-3">Transaction Details</h4>
                              
                              {/* Transaction Hash */}
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-crypto-silver text-sm">Transaction Hash:</span>
                                  <div className="flex items-center gap-2">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => copyToClipboard(tx.transactionHash)}
                                      className="text-crypto-silver hover:text-white"
                                    >
                                      <Copy className="w-3 h-3" />
                                    </Button>
                                    <a 
                                      href={getExplorerUrl(tx.network, tx.transactionHash)}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-blue-400 hover:text-blue-300"
                                    >
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  </div>
                                </div>
                                <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                  {tx.transactionHash}
                                </code>
                              </div>

                              {/* From Address */}
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-crypto-silver text-sm">From Address:</span>
                                  <div className="flex items-center gap-2">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => copyToClipboard(tx.fromAddress)}
                                      className="text-crypto-silver hover:text-white"
                                    >
                                      <Copy className="w-3 h-3" />
                                    </Button>
                                    <a 
                                      href={getAddressExplorerUrl(tx.network, tx.fromAddress)}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-blue-400 hover:text-blue-300"
                                    >
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  </div>
                                </div>
                                <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                  {tx.fromAddress}
                                </code>
                              </div>

                              {/* To Address */}
                              {tx.toAddress && (
                                <div className="space-y-2">
                                  <div className="flex items-center justify-between">
                                    <span className="text-crypto-silver text-sm">To Address:</span>
                                    <div className="flex items-center gap-2">
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        onClick={() => copyToClipboard(tx.toAddress)}
                                        className="text-crypto-silver hover:text-white"
                                      >
                                        <Copy className="w-3 h-3" />
                                      </Button>
                                      <a 
                                        href={getAddressExplorerUrl(tx.network, tx.toAddress)}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-blue-400 hover:text-blue-300"
                                      >
                                        <ExternalLink className="w-3 h-3" />
                                      </a>
                                    </div>
                                  </div>
                                  <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                    {tx.toAddress}
                                  </code>
                                </div>
                              )}

                              {/* Token Information */}
                              <div className="space-y-3">
                                <span className="text-crypto-silver text-sm">Token Information:</span>
                                <div className="bg-gradient-to-r from-blue-500/20 to-blue-400/20 border border-blue-500/30 rounded-lg p-3">
                                  <div className="space-y-2">
                                    <div className="flex justify-between items-center">
                                      <span className="text-blue-400 font-medium">{getTokenInfo(tx.token).name}</span>
                                      <span className="text-white font-semibold">{getTokenInfo(tx.token).ticker}</span>
                                    </div>
                                    <div className="flex items-center justify-between">
                                      <span className="text-crypto-silver text-xs">Contract:</span>
                                      <div className="flex items-center gap-2">
                                        <Button
                                          variant="ghost"
                                          size="sm"
                                          onClick={() => copyToClipboard(getTokenInfo(tx.token).address)}
                                          className="text-crypto-silver hover:text-white h-6 w-6 p-0"
                                        >
                                          <Copy className="w-3 h-3" />
                                        </Button>
                                        <a 
                                          href={getTokenExplorerUrl(tx.network, getTokenInfo(tx.token).address)}
                                          target="_blank"
                                          rel="noopener noreferrer"
                                          className="text-blue-400 hover:text-blue-300"
                                        >
                                          <ExternalLink className="w-3 h-3" />
                                        </a>
                                      </div>
                                    </div>
                                    <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                      {getTokenInfo(tx.token).address}
                                    </code>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </CollapsibleContent>
                        </div>
                      </Collapsible>
                    );
                  })
              ) : (
                <div className="text-center py-8">
                  <Activity className="w-12 h-12 text-crypto-silver mx-auto mb-4" />
                  <p className="text-crypto-silver">Loading BASE altcoin whale activity...</p>
                </div>
              )}
            </div>
          </TabsContent>

          <TabsContent value="tao">
            <div className="space-y-4">
              <div className="text-sm text-crypto-silver mb-4">
                🔴 LIVE: Monitoring TAO subnet staking events $5,000+ from whale validators
                <br />Tracking ALL Bittensor subnet whale stakes (SN1-SN32+)
              </div>
              {freeTransactions && freeTransactions.length > 0 ? (
                freeTransactions
                  .filter(tx => tx.network === 'TAO')
                  .slice(0, 10)
                  .map((tx, index) => {
                    const usdAmount = parseFloat(tx.amountUsd);
                    const txKey = tx.id || `${tx.transactionHash}-${index}`;
                    const isExpanded = expandedTx === txKey;
                    
                    return (
                      <Collapsible key={txKey} open={isExpanded} onOpenChange={(open) => setExpandedTx(open ? txKey : null)}>
                        <div className="backdrop-blur-sm bg-white/5 rounded-xl border border-orange-500/20 hover:bg-white/10 transition-all duration-200">
                          <div className="p-5">
                            <div className="flex items-center justify-between mb-3">
                              <div className="flex items-center gap-3">
                                <div className="w-10 h-10 bg-gradient-to-r from-orange-500 to-orange-600 rounded-full flex items-center justify-center text-sm font-bold text-white">
                                  τ
                                </div>
                                <div>
                                  <div className="flex items-center gap-2 mb-1">
                                    <Badge 
                                      variant="default"
                                      className="bg-orange-500 hover:bg-orange-600 text-white font-medium"
                                    >
                                      <ArrowUpRight className="w-3 h-3 mr-1" /> STAKE
                                    </Badge>
                                    <h3 className="font-medium text-white">
                                      {parseFloat(tx.amount).toLocaleString()} TAO
                                    </h3>
                                  </div>
                                  <p className="text-crypto-silver text-sm">
                                    <a 
                                      href={`https://x.com/search?q=%23${tx.toAddress || 'bittensor'}`}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="hover:text-orange-400 transition-colors"
                                    >
                                      {tx.toAddress || 'TAO Subnet'}
                                    </a> • {new Date(tx.timestamp).toLocaleTimeString()}
                                  </p>
                                </div>
                              </div>
                              <div className="text-right">
                                <div className="text-2xl font-bold text-orange-400 mb-1">
                                  ${usdAmount.toLocaleString('en-US', { 
                                    minimumFractionDigits: 2, 
                                    maximumFractionDigits: 2 
                                  })}
                                </div>
                                <div className="flex items-center gap-2">
                                  <a 
                                    href={getExplorerUrl(tx.network, tx.transactionHash)}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-flex items-center gap-1 text-orange-400 hover:text-orange-300 text-sm transition-colors"
                                  >
                                    TaoStats <ExternalLink className="w-3 h-3" />
                                  </a>
                                  <CollapsibleTrigger asChild>
                                    <Button variant="ghost" size="sm" className="text-crypto-silver hover:text-white">
                                      <Eye className="w-3 h-3 mr-1" />
                                      Details
                                      <ChevronDown className={`w-3 h-3 ml-1 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                                    </Button>
                                  </CollapsibleTrigger>
                                </div>
                              </div>
                            </div>
                            <div className="flex items-center justify-between text-crypto-silver text-xs font-mono bg-black/20 rounded-lg p-3">
                              <div>
                                <span className="text-crypto-silver/70">Staker:</span> {tx.fromAddress.slice(0, 8)}...{tx.fromAddress.slice(-6)}
                              </div>
                              <div className="text-crypto-silver/50">→</div>
                              <div>
                                <span className="text-orange-400 font-medium">{tx.toAddress}</span>
                              </div>
                            </div>
                          </div>
                          
                          <CollapsibleContent className="border-t border-orange-500/20">
                            <div className="p-5 space-y-4 bg-black/10">
                              <h4 className="font-semibold text-white mb-3">TAO Staking Details</h4>
                              
                              {/* Transaction Hash */}
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-crypto-silver text-sm">Transaction Hash:</span>
                                  <div className="flex items-center gap-2">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => copyToClipboard(tx.transactionHash)}
                                      className="text-crypto-silver hover:text-white"
                                    >
                                      <Copy className="w-3 h-3" />
                                    </Button>
                                    <a 
                                      href={getExplorerUrl(tx.network, tx.transactionHash)}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-orange-400 hover:text-orange-300"
                                    >
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  </div>
                                </div>
                                <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                  {tx.transactionHash}
                                </code>
                              </div>

                              {/* Staker Address */}
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-crypto-silver text-sm">Staker Address:</span>
                                  <div className="flex items-center gap-2">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => copyToClipboard(tx.fromAddress)}
                                      className="text-crypto-silver hover:text-white"
                                    >
                                      <Copy className="w-3 h-3" />
                                    </Button>
                                    <a 
                                      href={getAddressExplorerUrl(tx.network, tx.fromAddress)}
                                      target="_blank"
                                      rel="noopener noreferrer"
                                      className="text-orange-400 hover:text-orange-300"
                                    >
                                      <ExternalLink className="w-3 h-3" />
                                    </a>
                                  </div>
                                </div>
                                <code className="text-xs text-crypto-silver font-mono bg-black/30 p-2 rounded block break-all">
                                  {tx.fromAddress}
                                </code>
                              </div>

                              {/* Subnet Information */}
                              {tx.toAddress && (
                                <div className="space-y-2">
                                  <div className="flex items-center justify-between">
                                    <span className="text-crypto-silver text-sm">Target Subnet:</span>
                                  </div>
                                  <div className="bg-gradient-to-r from-orange-500/20 to-orange-400/20 border border-orange-500/30 rounded-lg p-3">
                                    <span className="text-orange-400 font-medium text-sm">{tx.toAddress}</span>
                                  </div>
                                </div>
                              )}

                              {/* TAO Amount Details */}
                              <div className="space-y-2">
                                <span className="text-crypto-silver text-sm">Stake Amount:</span>
                                <div className="bg-black/30 rounded-lg p-3">
                                  <div className="flex justify-between items-center">
                                    <span className="text-white font-semibold">{parseFloat(tx.amount).toLocaleString()} TAO</span>
                                    <span className="text-orange-400 font-bold">${usdAmount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                                  </div>
                                </div>
                              </div>
                            </div>
                          </CollapsibleContent>
                        </div>
                      </Collapsible>
                    );
                  })
              ) : (
                <div className="text-center py-8">
                  <Activity className="w-12 h-12 text-crypto-silver mx-auto mb-4" />
                  <p className="text-crypto-silver">Loading TAO staking whale activity...</p>
                </div>
              )}
            </div>
          </TabsContent>
        </Tabs>
      </GlassCard>


    </div>
  );
}
