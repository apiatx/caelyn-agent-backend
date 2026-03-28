import fetch from 'node-fetch';

interface CMCTokenInfo {
  id: number;
  name: string;
  symbol: string;
  slug: string;
  logo: string;
  description: string;
  platform?: {
    id: number;
    name: string;
    symbol: string;
    slug: string;
    token_address: string;
  };
  quote: {
    USD: {
      price: number;
      volume_24h: number;
      percent_change_1h: number;
      percent_change_24h: number;
      percent_change_7d: number;
      market_cap: number;
      last_updated: string;
    };
  };
}

interface TokenBalance {
  token: string;
  symbol: string;
  balance: string;
  decimals: number;
  value: number;
  logo?: string;
  contractAddress?: string;
  chain: string;
  price: number;
  change24h: number;
}

interface ChainBalance {
  chain: string;
  totalValue: number;
  tokens: TokenBalance[];
}

interface CMCPortfolioResponse {
  totalValue: number;
  chains: ChainBalance[];
  summary: { chain: string; value: number; tokenCount: number }[];
}

export class CMCPortfolioService {
  private cmcApiKey = process.env.COINMARKETCAP_API_KEY || '7d9a361e-596d-4914-87e2-f1124da24897';
  private etherscanApiKey = process.env.ETHERSCAN_API_KEY!;
  private basescanApiKey = process.env.BASESCAN_API_KEY!;

  // Get ETH price from CoinMarketCap
  async getETHPrice(): Promise<number> {
    try {
      const response = await fetch('https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?id=1027', {
        headers: {
          'X-CMC_PRO_API_KEY': this.cmcApiKey,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        console.log(`⚠️ [CMC Portfolio] ETH price API error: ${response.status}`);
        return 3800; // Fallback price
      }

      const data = await response.json() as any;
      const ethPrice = data.data['1027'].quote.USD.price;
      console.log(`💎 [CMC Portfolio] ETH price from CoinMarketCap: $${ethPrice.toFixed(2)}`);
      return ethPrice;
    } catch (error) {
      console.error(`❌ [CMC Portfolio] Error fetching ETH price:`, error);
      return 3800; // Fallback price
    }
  }

  // Get token prices for major tokens by symbol (limited by Basic plan)
  async getCMCTokenPricesBySymbol(symbols: string[]): Promise<Map<string, { price: number; change24h: number; logo: string }>> {
    try {
      if (symbols.length === 0) {
        return new Map();
      }

      // CMC Basic plan supports symbol lookup for major tokens
      const symbolString = symbols.join(',');
      const url = `https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=${symbolString}`;
      
      console.log(`🔍 [CMC Portfolio] Fetching prices for symbols: ${symbolString}`);

      const response = await fetch(url, {
        headers: {
          'X-CMC_PRO_API_KEY': this.cmcApiKey,
          'Accept': 'application/json'
        }
      });

      if (!response.ok) {
        console.log(`⚠️ [CMC Portfolio] Symbol price API error: ${response.status}`);
        return new Map();
      }

      const data = await response.json() as any;
      const priceMap = new Map<string, { price: number; change24h: number; logo: string }>();
      
      for (const [symbol, tokenData] of Object.entries(data.data)) {
        const token = Array.isArray(tokenData) ? tokenData[0] : tokenData;
        priceMap.set(symbol.toLowerCase(), {
          price: token.quote.USD.price,
          change24h: token.quote.USD.percent_change_24h,
          logo: `https://s2.coinmarketcap.com/static/img/coins/64x64/${token.id}.png`
        });
      }

      console.log(`✅ [CMC Portfolio] Successfully fetched prices for ${priceMap.size} symbols`);
      return priceMap;
    } catch (error) {
      console.error(`❌ [CMC Portfolio] Error fetching symbol prices:`, error);
      return new Map();
    }
  }

  // Get native ETH balance
  async getNativeBalance(walletAddress: string, chainName: string, apiUrl: string, apiKey: string): Promise<TokenBalance | null> {
    try {
      const response = await fetch(
        `${apiUrl}?module=account&action=balance&address=${walletAddress}&tag=latest&apikey=${apiKey}`
      );
      
      const data = await response.json() as any;
      
      if (data.status === '1') {
        const balanceInEth = parseFloat(data.result) / Math.pow(10, 18);
        
        if (balanceInEth > 0.001) {
          return {
            token: 'Ethereum',
            symbol: 'ETH',
            balance: balanceInEth.toFixed(6),
            decimals: 18,
            value: balanceInEth * 3800, // Will be updated with real price
            contractAddress: 'native',
            chain: chainName,
            price: 3800,
            change24h: 0
          };
        }
      }
      
      return null;
    } catch (error) {
      console.error(`❌ [CMC Portfolio] Error fetching ${chainName} native balance:`, error);
      return null;
    }
  }

  // Get ERC-20 token balances
  async getTokenBalances(walletAddress: string, chainName: string, apiUrl: string, apiKey: string): Promise<TokenBalance[]> {
    try {
      const response = await fetch(
        `${apiUrl}?module=account&action=tokentx&address=${walletAddress}&page=1&offset=100&sort=desc&apikey=${apiKey}`
      );
      
      const data = await response.json() as any;
      const tokens: TokenBalance[] = [];
      const seenTokens = new Set<string>();
      
      if (data.status === '1' && Array.isArray(data.result)) {
        // Get unique tokens from transaction history
        for (const tx of data.result) {
          if (!seenTokens.has(tx.contractAddress)) {
            seenTokens.add(tx.contractAddress);
            
            // Get current balance for this token
            try {
              const balanceResponse = await fetch(
                `${apiUrl}?module=account&action=tokenbalance&contractaddress=${tx.contractAddress}&address=${walletAddress}&tag=latest&apikey=${apiKey}`
              );
              
              const balanceData = await balanceResponse.json() as any;
              
              if (balanceData.status === '1') {
                const balance = parseFloat(balanceData.result) / Math.pow(10, parseInt(tx.tokenDecimal) || 18);
                
                if (balance > 0) {
                  tokens.push({
                    token: tx.tokenName || 'Unknown Token',
                    symbol: tx.tokenSymbol || 'UNKNOWN',
                    balance: balance.toFixed(6),
                    decimals: parseInt(tx.tokenDecimal) || 18,
                    value: 0, // Will be updated with CMC price
                    contractAddress: tx.contractAddress,
                    chain: chainName,
                    price: 0,
                    change24h: 0
                  });
                }
              }
            } catch (error) {
              console.error(`❌ [CMC Portfolio] Error fetching balance for ${tx.contractAddress}:`, error);
            }
          }
        }
      }
      
      return tokens;
    } catch (error) {
      console.error(`❌ [CMC Portfolio] Error fetching ${chainName} token balances:`, error);
      return [];
    }
  }

  // Main method to get complete portfolio
  async getPortfolio(walletAddress: string): Promise<CMCPortfolioResponse> {
    try {
      console.log(`🔍 [CMC Portfolio] Fetching portfolio for wallet: ${walletAddress}`);
      
      const chains: ChainBalance[] = [];
      
      // Get ETH price from CoinMarketCap
      const ethPrice = await this.getETHPrice();
      
      // Ethereum
      console.log(`🔗 [CMC Portfolio] Scanning Ethereum network...`);
      const ethNative = await this.getNativeBalance(walletAddress, 'Ethereum', 'https://api.etherscan.io/api', this.etherscanApiKey);
      const ethTokens = await this.getTokenBalances(walletAddress, 'Ethereum', 'https://api.etherscan.io/api', this.etherscanApiKey);
      
      const ethAllTokens = [...(ethNative ? [ethNative] : []), ...ethTokens];
      
      // Base
      console.log(`🔗 [CMC Portfolio] Scanning Base network...`);
      const baseNative = await this.getNativeBalance(walletAddress, 'Base', 'https://api.basescan.org/api', this.basescanApiKey);
      const baseTokens = await this.getTokenBalances(walletAddress, 'Base', 'https://api.basescan.org/api', this.basescanApiKey);
      
      const baseAllTokens = [...(baseNative ? [baseNative] : []), ...baseTokens];
      
      // Get symbols for major tokens (CMC Basic plan limitation workaround)
      const allTokens = [...ethAllTokens, ...baseAllTokens];
      const majorTokenSymbols = allTokens
        .map(t => t.symbol)
        .filter(symbol => ['ETH', 'USDC', 'USDT', 'DAI', 'WETH', 'BTC', 'LTC', 'ADA', 'DOT', 'LINK', 'UNI', 'AAVE', 'MKR', 'SNX', 'COMP', 'YFI', 'SUSHI', '1INCH', 'CRV', 'BAL'].includes(symbol))
        .filter((symbol, index, arr) => arr.indexOf(symbol) === index); // Remove duplicates
      
      // Fetch CMC data for major tokens only
      const cmcPriceData = await this.getCMCTokenPricesBySymbol(majorTokenSymbols);
      
      // Update token values with CMC data
      const updateTokensWithCMCData = (tokens: TokenBalance[], chainName: string) => {
        return tokens.map(token => {
          if (token.contractAddress === 'native') {
            token.price = ethPrice;
            token.value = parseFloat(token.balance) * token.price;
            token.logo = 'https://s2.coinmarketcap.com/static/img/coins/64x64/1027.png'; // ETH logo
            token.change24h = 0; // We could fetch this but keeping it simple for now
          } else {
            const cmcData = cmcPriceData.get(token.symbol.toLowerCase());
            
            if (cmcData) {
              token.price = cmcData.price;
              token.change24h = cmcData.change24h;
              token.logo = cmcData.logo;
              token.value = parseFloat(token.balance) * token.price;
              console.log(`💰 [CMC Portfolio] ${token.symbol} on ${chainName}: ${token.balance} × $${token.price.toFixed(4)} = $${token.value.toFixed(2)}`);
            } else {
              // For tokens not found in CMC, keep minimal data
              token.price = 0;
              token.value = 0;
              token.change24h = 0;
              console.log(`⚠️ [CMC Portfolio] ${token.symbol} not found in CoinMarketCap Basic plan`);
            }
          }
          
          return token;
        }).filter(token => token.value >= 0.01); // Only show tokens with value >= $0.01
      };
      
      // Process chains
      if (ethAllTokens.length > 0) {
        const updatedEthTokens = updateTokensWithCMCData(ethAllTokens, 'Ethereum');
        if (updatedEthTokens.length > 0) {
          chains.push({
            chain: 'Ethereum',
            totalValue: updatedEthTokens.reduce((sum, token) => sum + token.value, 0),
            tokens: updatedEthTokens
          });
        }
      }
      
      if (baseAllTokens.length > 0) {
        const updatedBaseTokens = updateTokensWithCMCData(baseAllTokens, 'Base');
        if (updatedBaseTokens.length > 0) {
          chains.push({
            chain: 'Base',
            totalValue: updatedBaseTokens.reduce((sum, token) => sum + token.value, 0),
            tokens: updatedBaseTokens
          });
        }
      }
      
      const totalValue = chains.reduce((sum, chain) => sum + chain.totalValue, 0);
      const totalTokens = chains.reduce((sum, chain) => sum + chain.tokens.length, 0);
      
      console.log(`📊 [CMC Portfolio] Portfolio total: $${totalValue.toFixed(2)} across ${chains.length} chains (${totalTokens} tokens)`);
      
      return {
        totalValue,
        chains,
        summary: chains.map(chain => ({
          chain: chain.chain,
          value: chain.totalValue,
          tokenCount: chain.tokens.length
        }))
      };
      
    } catch (error) {
      console.error(`❌ [CMC Portfolio] Error fetching portfolio:`, error);
      return {
        totalValue: 0,
        chains: [],
        summary: []
      };
    }
  }
}

export const cmcPortfolioService = new CMCPortfolioService();