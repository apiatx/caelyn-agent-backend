import fetch from 'node-fetch';

interface TokenBalance {
  token: string;
  symbol: string;
  balance: string;
  decimals: number;
  value?: number;
  logo?: string;
  contractAddress?: string;
  chain: string;
}

interface ChainConfig {
  name: string;
  apiUrl: string;
  apiKey: string;
  nativeSymbol: string;
  chainId: string;
}

export class MultiChainService {
  private chains: ChainConfig[] = [
    {
      name: 'Ethereum',
      apiUrl: 'https://api.etherscan.io/api',
      apiKey: process.env.ETHERSCAN_API_KEY!,
      nativeSymbol: 'ETH',
      chainId: '1'
    },
    {
      name: 'Base',
      apiUrl: 'https://api.basescan.org/api',
      apiKey: process.env.BASESCAN_API_KEY!,
      nativeSymbol: 'ETH',
      chainId: '8453'
    }
  ];

  private mobulaApiKey = process.env.MOBULA_API_KEY || '3371ca28-c28d-4b0f-908e-9529c27dfa4d';

  // Get native token balance (ETH) for a wallet on a specific chain
  async getNativeBalance(walletAddress: string, chain: ChainConfig): Promise<TokenBalance | null> {
    try {
      const response = await fetch(
        `${chain.apiUrl}?module=account&action=balance&address=${walletAddress}&tag=latest&apikey=${chain.apiKey}`
      );
      
      const data = await response.json() as any;
      
      if (data.status === '1') {
        const balanceInEth = parseFloat(data.result) / Math.pow(10, 18);
        
        if (balanceInEth > 0.001) { // Only include if balance > 0.001 ETH
          return {
            token: chain.nativeSymbol,
            symbol: chain.nativeSymbol,
            balance: balanceInEth.toFixed(6),
            decimals: 18,
            contractAddress: 'native',
            chain: chain.name
          };
        }
      }
      
      return null;
    } catch (error) {
      console.error(`❌ Error fetching ${chain.name} native balance:`, error);
      return null;
    }
  }

  // Get ERC-20 token balances for a wallet on a specific chain
  async getTokenBalances(walletAddress: string, chain: ChainConfig): Promise<TokenBalance[]> {
    try {
      const response = await fetch(
        `${chain.apiUrl}?module=account&action=tokentx&address=${walletAddress}&page=1&offset=100&sort=desc&apikey=${chain.apiKey}`
      );
      
      const data = await response.json() as any;
      
      if (data.status === '1' && data.result) {
        // Get unique tokens from recent transactions
        const uniqueTokens = new Map<string, any>();
        
        data.result.forEach((tx: any) => {
          if (!uniqueTokens.has(tx.contractAddress)) {
            uniqueTokens.set(tx.contractAddress, {
              contractAddress: tx.contractAddress,
              symbol: tx.tokenSymbol,
              name: tx.tokenName,
              decimals: parseInt(tx.tokenDecimal)
            });
          }
        });

        // Get current balances for each token
        const balances: TokenBalance[] = [];
        
        for (const [contractAddress, tokenInfo] of uniqueTokens) {
          try {
            const balanceResponse = await fetch(
              `${chain.apiUrl}?module=account&action=tokenbalance&contractaddress=${contractAddress}&address=${walletAddress}&tag=latest&apikey=${chain.apiKey}`
            );
            
            const balanceData = await balanceResponse.json() as any;
            
            if (balanceData.status === '1' && balanceData.result !== '0') {
              const balance = parseFloat(balanceData.result) / Math.pow(10, tokenInfo.decimals);
              
              if (balance > 0) {
                balances.push({
                  token: tokenInfo.name,
                  symbol: tokenInfo.symbol,
                  balance: balance.toFixed(6),
                  decimals: tokenInfo.decimals,
                  contractAddress: contractAddress,
                  chain: chain.name
                });
              }
            }
            
            // Rate limiting
            await new Promise(resolve => setTimeout(resolve, 200));
          } catch (error) {
            console.error(`Error fetching balance for ${tokenInfo.symbol}:`, error);
          }
        }
        
        return balances;
      }
      
      return [];
    } catch (error) {
      console.error(`❌ Error fetching ${chain.name} token balances:`, error);
      return [];
    }
  }

  // Get price data from Mobula for tokens
  async getTokenPrices(tokens: TokenBalance[]): Promise<TokenBalance[]> {
    const enrichedTokens: TokenBalance[] = [];
    
    for (const token of tokens) {
      try {
        // Try to get price from Mobula
        let price = 0;
        
        if (token.symbol === 'ETH') {
          // Get ETH price
          const response = await fetch(
            `https://api.mobula.io/api/1/market/data?asset=ethereum&apikey=${this.mobulaApiKey}`
          );
          const data = await response.json() as any;
          price = data?.data?.price || 0;
        } else {
          // Try to get token price by symbol
          const response = await fetch(
            `https://api.mobula.io/api/1/market/data?asset=${token.symbol.toLowerCase()}&apikey=${this.mobulaApiKey}`
          );
          const data = await response.json() as any;
          price = data?.data?.price || 0;
        }
        
        const value = parseFloat(token.balance) * price;
        
        enrichedTokens.push({
          ...token,
          value: value,
          logo: `https://logo.moralis.io/0x1_0x${token.contractAddress}_${token.symbol.toLowerCase()}`
        });
        
        console.log(`💰 [MULTI-CHAIN] ${token.symbol} on ${token.chain}: ${token.balance} = $${value.toFixed(2)}`);
        
        // Rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (error) {
        console.error(`Error getting price for ${token.symbol}:`, error);
        enrichedTokens.push({
          ...token,
          value: 0
        });
      }
    }
    
    return enrichedTokens;
  }

  // Get complete multi-chain portfolio for a wallet address
  async getMultiChainPortfolio(walletAddress: string): Promise<{
    totalValue: number;
    chains: { [key: string]: TokenBalance[] };
    summary: { chain: string; value: number; tokenCount: number }[];
  }> {
    console.log(`🔍 [MULTI-CHAIN] Fetching portfolio for wallet: ${walletAddress}`);
    
    const allTokens: TokenBalance[] = [];
    const chainBalances: { [key: string]: TokenBalance[] } = {};
    
    for (const chain of this.chains) {
      console.log(`🔗 [MULTI-CHAIN] Scanning ${chain.name} network...`);
      
      const tokens: TokenBalance[] = [];
      
      // Get native token balance
      const nativeBalance = await this.getNativeBalance(walletAddress, chain);
      if (nativeBalance) {
        tokens.push(nativeBalance);
      }
      
      // Get ERC-20 token balances
      const tokenBalances = await this.getTokenBalances(walletAddress, chain);
      tokens.push(...tokenBalances);
      
      chainBalances[chain.name] = tokens;
      allTokens.push(...tokens);
      
      console.log(`✅ [MULTI-CHAIN] Found ${tokens.length} tokens on ${chain.name}`);
    }
    
    // Enrich with price data
    console.log(`💲 [MULTI-CHAIN] Fetching prices for ${allTokens.length} tokens...`);
    const enrichedTokens = await this.getTokenPrices(allTokens);
    
    // Reorganize by chain with prices
    const finalChainBalances: { [key: string]: TokenBalance[] } = {};
    enrichedTokens.forEach(token => {
      if (!finalChainBalances[token.chain]) {
        finalChainBalances[token.chain] = [];
      }
      finalChainBalances[token.chain].push(token);
    });
    
    // Calculate totals
    const totalValue = enrichedTokens.reduce((sum, token) => sum + (token.value || 0), 0);
    
    const summary = Object.keys(finalChainBalances).map(chain => ({
      chain,
      value: finalChainBalances[chain].reduce((sum, token) => sum + (token.value || 0), 0),
      tokenCount: finalChainBalances[chain].length
    }));
    
    console.log(`📊 [MULTI-CHAIN] Portfolio total: $${totalValue.toFixed(2)}`);
    
    return {
      totalValue,
      chains: finalChainBalances,
      summary
    };
  }
}