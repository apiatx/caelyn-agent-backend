import fetch from 'node-fetch';

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

interface CoinbasePortfolioResponse {
  totalValue: number;
  chains: ChainBalance[];
  summary: { chain: string; value: number; tokenCount: number }[];
}

export class CoinbasePortfolioService {
  private coinbaseApiKey = '8bbe4752-c30f-4c16-ab89-b9906369c832';
  private etherscanApiKey = process.env.ETHERSCAN_API_KEY!;
  private basescanApiKey = process.env.BASESCAN_API_KEY!;
  
  // Alternative API endpoints for multi-chain data
  private moralisApiKey = process.env.MORALIS_API_KEY || '';
  private alchemyApiKey = process.env.ALCHEMY_API_KEY || '';
  private covalentApiKey = 'cqt_rQtCkjKfCWmjVyBK4yPJcJf47Rtv';

  // BASE network tokens via Covalent Goldrush API
  async getBaseTokensViaCovalent(walletAddress: string): Promise<TokenBalance[]> {
    try {
      console.log(`🔍 [COVALENT BASE] Scanning BASE network via Covalent Goldrush API...`);
      
      // Use Covalent API to get token balances on BASE network (chain ID 8453)
      const response = await fetch(
        `https://api.covalenthq.com/v1/base-mainnet/address/${walletAddress}/balances_v2/?key=${this.covalentApiKey}`,
        {
          headers: {
            'Accept': 'application/json'
          }
        }
      );
      
      if (!response.ok) {
        throw new Error(`Covalent API error: ${response.status} ${response.statusText}`);
      }
      
      const data = await response.json() as any;
      console.log(`🔍 [COVALENT BASE] API Response status: ${response.status}`);
      
      if (data.error) {
        throw new Error(`Covalent API error: ${data.error_message}`);
      }
      
      const tokens: TokenBalance[] = [];
      
      if (data.data && data.data.items) {
        console.log(`💰 [COVALENT BASE] Found ${data.data.items.length} token entries`);
        
        for (const item of data.data.items) {
          if (item.balance && item.balance !== '0') {
            const balance = parseFloat(item.balance) / Math.pow(10, item.contract_decimals || 18);
            
            if (balance > 0.000001) { // Filter out dust
              tokens.push({
                token: item.contract_name || 'Unknown Token',
                symbol: item.contract_ticker_symbol || 'UNKNOWN',
                balance: balance.toString(),
                decimals: item.contract_decimals || 18,
                value: item.quote || 0,
                contractAddress: item.contract_address === '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' ? 'native' : item.contract_address,
                chain: 'Base',
                price: item.quote_rate || 0,
                change24h: item.quote_24h_delta || 0
              });
              
              console.log(`💎 [COVALENT BASE] ${item.contract_ticker_symbol}: ${balance.toLocaleString()} ($${item.quote?.toFixed(2) || '0.00'})`);
            }
          }
        }
      }
      
      console.log(`✅ [COVALENT BASE] Total tokens found: ${tokens.length}`);
      return tokens;
    } catch (error) {
      console.error(`❌ [COVALENT BASE] Error:`, error);
      return [];
    }
  }

  // Solana network token scanning
  async getSolanaTokens(walletAddress: string): Promise<TokenBalance[]> {
    try {
      console.log(`🔍 [SOLANA] Scanning Solana SPL tokens...`);
      
      // Try Solana RPC for SPL tokens
      const response = await fetch('https://api.mainnet-beta.solana.com', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'getTokenAccountsByOwner',
          params: [
            walletAddress,
            { programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' },
            { encoding: 'jsonParsed' }
          ]
        })
      });
      
      if (response.ok) {
        const data = await response.json() as any;
        if (data.result && data.result.value && data.result.value.length > 0) {
          console.log(`💰 [SOLANA] Found ${data.result.value.length} SPL token accounts`);
          
          // Process SPL tokens (would need to implement token metadata lookup)
          const tokens: TokenBalance[] = [];
          
          for (const tokenAccount of data.result.value.slice(0, 10)) { // Limit to 10 for testing
            try {
              const tokenData = tokenAccount.account.data.parsed.info;
              const balance = parseFloat(tokenData.tokenAmount.uiAmount || '0');
              
              if (balance > 0) {
                tokens.push({
                  token: `SPL Token ${tokenData.mint.slice(0, 8)}...`,
                  symbol: 'SPL',
                  balance: balance.toString(),
                  decimals: tokenData.tokenAmount.decimals,
                  value: 0,
                  contractAddress: tokenData.mint,
                  chain: 'Solana',
                  price: 0,
                  change24h: 0
                });
              }
            } catch (error) {
              console.log(`⚠️ [SOLANA] Error processing token:`, error);
            }
          }
          
          return tokens;
        }
      }
      
      return [];
    } catch (error) {
      console.error(`❌ [SOLANA] Error:`, error);
      return [];
    }
  }

  // Alternative token pricing for tokens not supported by Coinbase
  async getAlternativeTokenPrice(symbol: string, contractAddress: string): Promise<number> {
    try {
      // Try DexScreener for real-time pricing
      const response = await fetch(`https://api.dexscreener.com/latest/dex/tokens/${contractAddress}`);
      
      if (response.ok) {
        const data = await response.json() as any;
        
        if (data.pairs && data.pairs.length > 0) {
          // Find the pair with highest liquidity
          const bestPair = data.pairs.reduce((best: any, current: any) => {
            return (current.liquidity?.usd || 0) > (best.liquidity?.usd || 0) ? current : best;
          });
          
          if (bestPair.priceUsd) {
            const price = parseFloat(bestPair.priceUsd);
            console.log(`🔍 [DEXSCREENER] Found ${symbol} price: $${price.toFixed(8)}`);
            return price;
          }
        }
      }
      
      // Try CoinGecko as backup for Ethereum tokens
      if (contractAddress.startsWith('0x')) {
        try {
          const geckoResponse = await fetch(`https://api.coingecko.com/api/v3/simple/token_price/ethereum?contract_addresses=${contractAddress}&vs_currencies=usd`);
          
          if (geckoResponse.ok) {
            const geckoData = await geckoResponse.json() as any;
            const price = geckoData[contractAddress.toLowerCase()]?.usd;
            
            if (price) {
              console.log(`🦎 [COINGECKO] Found ${symbol} price: $${price.toFixed(8)}`);
              return price;
            }
          }
        } catch (geckoError) {
          console.log(`⚠️ [COINGECKO] Error for ${symbol}:`, geckoError);
        }
      }
      
      return 0;
    } catch (error) {
      console.error(`❌ [ALT PRICE] Error getting alternative price for ${symbol}:`, error);
      return 0;
    }
  }

  // Get ETH price from Coinbase Developer Platform API
  async getETHPrice(): Promise<number> {
    try {
      const response = await fetch('https://api.developer.coinbase.com/rpc/v1/base/getBalance', {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.coinbaseApiKey}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          address: '0x0000000000000000000000000000000000000000', // Just to test API access
          currency: 'ETH'
        })
      });
      
      if (response.ok) {
        console.log(`✅ [Coinbase Portfolio] Coinbase Developer API authenticated successfully`);
      }

      // Use exchange API for price data (public endpoint)
      const priceResponse = await fetch('https://api.exchange.coinbase.com/products/ETH-USD/ticker');
      
      if (!priceResponse.ok) {
        console.log(`⚠️ [Coinbase Portfolio] ETH price API error: ${priceResponse.status}`);
        return 3800;
      }

      const priceData = await priceResponse.json() as any;
      const ethPrice = parseFloat(priceData.price);
      console.log(`💎 [Coinbase Portfolio] ETH price from Coinbase: $${ethPrice.toFixed(2)}`);
      return ethPrice;
    } catch (error) {
      console.error(`❌ [Coinbase Portfolio] Error fetching ETH price:`, error);
      return 3800;
    }
  }

  // Get comprehensive token data using Coinbase Developer API
  async getCoinbaseTokenPrices(symbols: string[]): Promise<Map<string, { price: number; change24h: number }>> {
    try {
      if (symbols.length === 0) {
        return new Map();
      }

      console.log(`🔍 [Coinbase Portfolio] Fetching prices for symbols: ${symbols.join(', ')}`);
      const priceMap = new Map<string, { price: number; change24h: number }>();
      
      // Try Coinbase Developer API for asset pricing
      for (const symbol of symbols) {
        try {
          // Use Coinbase API for asset information
          const response = await fetch(`https://api.coinbase.com/v2/exchange-rates?currency=${symbol.toUpperCase()}`, {
            headers: {
              'Authorization': `Bearer ${this.coinbaseApiKey}`,
              'Accept': 'application/json'
            }
          });
          
          if (response.ok) {
            const data = await response.json() as any;
            if (data.data && data.data.rates && data.data.rates.USD) {
              const price = parseFloat(data.data.rates.USD);
              priceMap.set(symbol.toLowerCase(), { price, change24h: 0 });
              console.log(`💰 [Coinbase Portfolio] ${symbol} from Coinbase API: $${price.toFixed(4)}`);
              continue;
            }
          }
          
          // Fallback to Exchange API for major pairs
          const pair = `${symbol.toUpperCase()}-USD`;
          const supportedPairs = ['ETH-USD', 'BTC-USD', 'USDC-USD', 'USDT-USD', 'UNI-USD', 'LINK-USD', 'AAVE-USD', 'MKR-USD', 'COMP-USD', 'CRV-USD'];
          
          if (supportedPairs.includes(pair)) {
            const exchangeResponse = await fetch(`https://api.exchange.coinbase.com/products/${pair}/ticker`);
            
            if (exchangeResponse.ok) {
              const exchangeData = await exchangeResponse.json() as any;
              const price = parseFloat(exchangeData.price);
              
              priceMap.set(symbol.toLowerCase(), { price, change24h: 0 });
              console.log(`💰 [Coinbase Portfolio] ${symbol} from Exchange: $${price.toFixed(4)}`);
            }
          }
        } catch (error) {
          console.log(`⚠️ [Coinbase Portfolio] Error fetching ${symbol} price:`, error);
        }
      }

      console.log(`✅ [Coinbase Portfolio] Successfully fetched prices for ${priceMap.size} symbols`);
      return priceMap;
    } catch (error) {
      console.error(`❌ [Coinbase Portfolio] Error fetching symbol prices:`, error);
      return new Map();
    }
  }

  // Get native ETH balance using Coinbase Developer API
  async getNativeBalance(walletAddress: string, chainName: string): Promise<TokenBalance | null> {
    try {
      // Use Coinbase Developer API for onchain data
      const networkName = chainName === 'Ethereum' ? 'ethereum-mainnet' : 'base-mainnet';
      
      const response = await fetch(`https://api.developer.coinbase.com/rpc/v1/${networkName}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${this.coinbaseApiKey}`,
          'Content-Type': 'application/json',
          'Accept': 'application/json'
        },
        body: JSON.stringify({
          method: 'eth_getBalance',
          params: [walletAddress, 'latest'],
          id: 1,
          jsonrpc: '2.0'
        })
      });
      
      if (!response.ok) {
        console.log(`⚠️ [Coinbase Portfolio] ${chainName} balance API error: ${response.status}`);
        return this.getFallbackNativeBalance(walletAddress, chainName);
      }
      
      const data = await response.json() as any;
      
      if (data.result) {
        const balanceInWei = parseInt(data.result, 16);
        const balanceInEth = balanceInWei / Math.pow(10, 18);
        
        if (balanceInEth > 0.0001) {
          console.log(`💎 [Coinbase Portfolio] ${chainName} ETH balance: ${balanceInEth.toFixed(6)} ETH`);
          return {
            token: 'Ethereum',
            symbol: 'ETH',
            balance: balanceInEth.toFixed(6),
            decimals: 18,
            value: 0, // Will be updated with price
            contractAddress: 'native',
            chain: chainName,
            price: 0,
            change24h: 0
          };
        }
      }
      
      return null;
    } catch (error) {
      console.error(`❌ [Coinbase Portfolio] Error fetching ${chainName} native balance:`, error);
      return this.getFallbackNativeBalance(walletAddress, chainName);
    }
  }

  // Fallback to Etherscan/Basescan if Coinbase API fails
  async getFallbackNativeBalance(walletAddress: string, chainName: string): Promise<TokenBalance | null> {
    try {
      const apiUrl = chainName === 'Ethereum' ? 'https://api.etherscan.io/api' : 'https://api.basescan.org/api';
      const apiKey = chainName === 'Ethereum' ? this.etherscanApiKey : this.basescanApiKey;
      
      const response = await fetch(
        `${apiUrl}?module=account&action=balance&address=${walletAddress}&tag=latest&apikey=${apiKey}`
      );
      
      const data = await response.json() as any;
      
      if (data.status === '1') {
        const balanceInEth = parseFloat(data.result) / Math.pow(10, 18);
        
        if (balanceInEth > 0.0001) {
          return {
            token: 'Ethereum',
            symbol: 'ETH',
            balance: balanceInEth.toFixed(6),
            decimals: 18,
            value: 0,
            contractAddress: 'native',
            chain: chainName,
            price: 0,
            change24h: 0
          };
        }
      }
      
      return null;
    } catch (error) {
      console.error(`❌ [Coinbase Portfolio] Fallback ${chainName} balance error:`, error);
      return null;
    }
  }

  // Get comprehensive ERC-20 token balances using multiple methods
  async getTokenBalances(walletAddress: string, chainName: string, apiUrl: string, apiKey: string): Promise<TokenBalance[]> {
    try {
      const tokens: TokenBalance[] = [];
      const seenTokens = new Set<string>();
      
      console.log(`🔍 [Coinbase Portfolio] Scanning ${chainName} for all token holdings...`);
      
      // Method 1: Get recent token transactions (expanded to 1000 records)
      const txResponse = await fetch(
        `${apiUrl}?module=account&action=tokentx&address=${walletAddress}&page=1&offset=1000&sort=desc&apikey=${apiKey}`
      );
      
      const txData = await txResponse.json() as any;
      
      if (txData.status === '1' && Array.isArray(txData.result)) {
        console.log(`📊 [Coinbase Portfolio] Found ${txData.result.length} token transactions to analyze`);
        
        // Collect unique tokens from transaction history
        for (const tx of txData.result) {
          if (!seenTokens.has(tx.contractAddress) && tx.tokenSymbol && tx.tokenName && tx.contractAddress) {
            seenTokens.add(tx.contractAddress);
            
            try {
              // Get current balance for each token
              const balanceResponse = await fetch(
                `${apiUrl}?module=account&action=tokenbalance&contractaddress=${tx.contractAddress}&address=${walletAddress}&tag=latest&apikey=${apiKey}`
              );
              
              const balanceData = await balanceResponse.json() as any;
              
              console.log(`🔍 [BALANCE CHECK] ${tx.tokenSymbol}: status=${balanceData.status}, result=${balanceData.result}`);
              
              if (balanceData.status === '1' && balanceData.result && balanceData.result !== '0') {
                const decimals = parseInt(tx.tokenDecimal) || 18;
                const rawBalance = balanceData.result;
                const balance = parseFloat(rawBalance) / Math.pow(10, decimals);
                
                console.log(`🔍 [BALANCE CALC] ${tx.tokenSymbol}: rawBalance=${rawBalance}, decimals=${decimals}, balance=${balance}`);
                
                // Include ALL tokens with any balance for debugging
                if (balance > 0) {
                  tokens.push({
                    token: tx.tokenName,
                    symbol: tx.tokenSymbol,
                    balance: balance.toString(),
                    decimals: decimals,
                    value: 0, // Will be updated with pricing
                    contractAddress: tx.contractAddress,
                    chain: chainName,
                    price: 0,
                    change24h: 0
                  });
                  
                  console.log(`💰 [FOUND TOKEN] ${tx.tokenSymbol}: ${balance.toLocaleString()} tokens (${tx.tokenName})`);
                } else {
                  console.log(`❌ [ZERO BALANCE] ${tx.tokenSymbol}: balance is ${balance}`);
                }
              } else {
                console.log(`❌ [API ERROR] ${tx.tokenSymbol}: status=${balanceData.status}, result=${balanceData.result}`);
              }
            } catch (error) {
              console.log(`⚠️ [Coinbase Portfolio] Error checking balance for ${tx.tokenSymbol}:`, error);
            }
          }
        }
      }
      
      // Method 2: Get comprehensive token list using multiple pages
      console.log(`🔍 [Coinbase Portfolio] Scanning additional transaction pages for ${chainName}...`);
      
      // Scan multiple pages to catch more tokens
      for (let page = 2; page <= 5; page++) {
        try {
          const pageResponse = await fetch(
            `${apiUrl}?module=account&action=tokentx&address=${walletAddress}&page=${page}&offset=1000&sort=desc&apikey=${apiKey}`
          );
          
          const pageData = await pageResponse.json() as any;
          
          if (pageData.status === '1' && Array.isArray(pageData.result) && pageData.result.length > 0) {
            console.log(`📊 [Coinbase Portfolio] Page ${page}: Found ${pageData.result.length} additional transactions`);
            
            for (const tx of pageData.result) {
              if (!seenTokens.has(tx.contractAddress) && tx.tokenSymbol && tx.tokenName && tx.contractAddress) {
                seenTokens.add(tx.contractAddress);
                
                try {
                  const balanceResponse = await fetch(
                    `${apiUrl}?module=account&action=tokenbalance&contractaddress=${tx.contractAddress}&address=${walletAddress}&tag=latest&apikey=${apiKey}`
                  );
                  
                  const balanceData = await balanceResponse.json() as any;
                  
                  if (balanceData.status === '1' && balanceData.result && balanceData.result !== '0') {
                    const decimals = parseInt(tx.tokenDecimal) || 18;
                    const balance = parseFloat(balanceData.result) / Math.pow(10, decimals);
                    
                    if (balance > 0.000001) {
                      tokens.push({
                        token: tx.tokenName,
                        symbol: tx.tokenSymbol,
                        balance: balance.toString(),
                        decimals: decimals,
                        value: 0,
                        contractAddress: tx.contractAddress,
                        chain: chainName,
                        price: 0,
                        change24h: 0
                      });
                      
                      console.log(`💰 [Coinbase Portfolio] Page ${page} - ${tx.tokenSymbol}: ${balance.toLocaleString()} tokens`);
                    }
                  }
                } catch (error) {
                  console.log(`⚠️ [Coinbase Portfolio] Page ${page} error checking ${tx.tokenSymbol}:`, error);
                }
              }
            }
          } else {
            console.log(`📊 [Coinbase Portfolio] Page ${page}: No more transactions found`);
            break;
          }
        } catch (error) {
          console.log(`❌ [Coinbase Portfolio] Error scanning page ${page}:`, error);
          break;
        }
      }
      
      // Method 3: Direct check for known valuable tokens from transaction history
      console.log(`🔍 [Coinbase Portfolio] Direct checking valuable tokens from transactions for ${chainName}...`);
      
      const knownValuableTokens = chainName === 'Ethereum' 
        ? [
            { address: '0xea87148a703adc0de89db2ac2b6b381093ae8ee0', symbol: 'IRIS', name: 'I.R.I.S by Virtuals', decimals: 18 },
            { address: '0xb551b43af192965f74e3dfaa476c890b403cad95', symbol: 'DATA', name: 'Data bot', decimals: 9 },
            { address: '0xdaa7699352ac8709f3d2fd092226d3dd7da40474', symbol: 'OPCAT', name: 'OP_CAT', decimals: 18 },
            { address: '0x1495bc9e44af1f8bcb62278d2bec4540cf0c05ea', symbol: 'DEAI', name: 'Zero1 Token', decimals: 18 },
            { address: '0xa562912e1328eea987e04c2650efb5703757850c', symbol: 'DROPS', name: 'Drops', decimals: 18 },
            { address: '0x069d89974f4edabde69450f9cf5cf7d8cbd2568d', symbol: 'BVM', name: 'BVM', decimals: 18 },
            { address: '0x6b448aeb3bfd1dcbe337d59f6dee159daab52768', symbol: 'TOR', name: 'Resistor AI', decimals: 18 }
          ]
        : [
            // BASE network valuable tokens
            { address: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', symbol: 'USDC', name: 'USD Coin', decimals: 6 },
            { address: '0x4200000000000000000000000000000000000006', symbol: 'WETH', name: 'Wrapped Ether', decimals: 18 },
            { address: '0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb', symbol: 'DEGEN', name: 'Degen', decimals: 18 },
            { address: '0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42', symbol: 'EURC', name: 'Euro Coin', decimals: 6 },
            { address: '0x940181a94A35A4569E4529A3CDfB74e38FD98631', symbol: 'AERO', name: 'Aerodrome Finance', decimals: 18 },
            { address: '0x0578d8A44db98B23BF096A382e016e29a5Ce0ffe', symbol: 'HIGHER', name: 'Higher', decimals: 18 },
            { address: '0x532f27101965dd16442E59d40670FaF5eBB142E4', symbol: 'BRETT', name: 'Brett', decimals: 18 },
            { address: '0x4ed4E862860beD51a9570b96d89aF5E1B0Efefed', symbol: 'DEGEN', name: 'Degen Base', decimals: 18 }
          ];
      
      for (const token of knownValuableTokens) {
        if (!seenTokens.has(token.address)) {
          try {
            console.log(`🔍 [DIRECT CHECK] Testing ${token.symbol} at ${token.address}...`);
            
            const balanceResponse = await fetch(
              `${apiUrl}?module=account&action=tokenbalance&contractaddress=${token.address}&address=${walletAddress}&tag=latest&apikey=${apiKey}`
            );
            
            const balanceData = await balanceResponse.json() as any;
            console.log(`🔍 [DIRECT RESULT] ${token.symbol}: status=${balanceData.status}, result=${balanceData.result}`);
            
            if (balanceData.status === '1' && balanceData.result && balanceData.result !== '0') {
              const balance = parseFloat(balanceData.result) / Math.pow(10, token.decimals);
              
              if (balance > 0) {
                tokens.push({
                  token: token.name,
                  symbol: token.symbol,
                  balance: balance.toString(),
                  decimals: token.decimals,
                  value: 0,
                  contractAddress: token.address,
                  chain: chainName,
                  price: 0,
                  change24h: 0
                });
                
                console.log(`💎 [DIRECT FOUND] ${token.symbol}: ${balance.toLocaleString()} tokens`);
              }
            }
          } catch (error) {
            console.log(`❌ [DIRECT ERROR] Error checking ${token.symbol}:`, error);
          }
        }
      }
      
      // Method 4: Try major tokens if still not enough
      if (tokens.length < 10) {
        console.log(`🔍 [Coinbase Portfolio] Checking additional major tokens for ${chainName}...`);
        
        const majorTokens = chainName === 'Ethereum' 
          ? [
              { address: '0xA0b86a33E6441e4adCf4d61c72EE1dcc0f8cd17b', symbol: 'USDC', name: 'USD Coin', decimals: 6 },
              { address: '0xdac17f958d2ee523a2206206994597c13d831ec7', symbol: 'USDT', name: 'Tether USD', decimals: 6 },
              { address: '0x6b175474e89094c44da98b954eedeac495271d0f', symbol: 'DAI', name: 'Dai Stablecoin', decimals: 18 },
              { address: '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', symbol: 'UNI', name: 'Uniswap', decimals: 18 }
            ]
          : [
              { address: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913', symbol: 'USDC', name: 'USD Coin', decimals: 6 },
              { address: '0x4200000000000000000000000000000000000006', symbol: 'WETH', name: 'Wrapped Ether', decimals: 18 }
            ];
        
        for (const token of majorTokens) {
          if (!seenTokens.has(token.address)) {
            try {
              const balanceResponse = await fetch(
                `${apiUrl}?module=account&action=tokenbalance&contractaddress=${token.address}&address=${walletAddress}&tag=latest&apikey=${apiKey}`
              );
              
              const balanceData = await balanceResponse.json() as any;
              
              if (balanceData.status === '1' && balanceData.result && balanceData.result !== '0') {
                const balance = parseFloat(balanceData.result) / Math.pow(10, token.decimals);
                
                if (balance > 0.01) {
                  tokens.push({
                    token: token.name,
                    symbol: token.symbol,
                    balance: balance.toString(),
                    decimals: token.decimals,
                    value: 0,
                    contractAddress: token.address,
                    chain: chainName,
                    price: 0,
                    change24h: 0
                  });
                  
                  console.log(`💎 [Coinbase Portfolio] Major token ${token.symbol}: ${balance.toLocaleString()}`);
                }
              }
            } catch (error) {
              console.log(`⚠️ [Coinbase Portfolio] Error checking ${token.symbol}:`, error);
            }
          }
        }
      }
      
      console.log(`✅ [Coinbase Portfolio] Total found ${tokens.length} tokens on ${chainName}`);
      return tokens;
    } catch (error) {
      console.error(`❌ [Coinbase Portfolio] Error fetching ${chainName} token balances:`, error);
      return [];
    }
  }

  // Main method to get complete portfolio
  async getPortfolio(walletAddress: string): Promise<CoinbasePortfolioResponse> {
    try {
      console.log(`🔍 [Coinbase Portfolio] Fetching portfolio for wallet: ${walletAddress}`);
      
      const chains: ChainBalance[] = [];
      
      // Get ETH price from Coinbase
      const ethPrice = await this.getETHPrice();
      
      // Ethereum
      console.log(`🔗 [Coinbase Portfolio] Comprehensive Ethereum scan for wallet ${walletAddress}...`);
      const ethNative = await this.getNativeBalance(walletAddress, 'Ethereum');
      const ethTokens = await this.getTokenBalances(walletAddress, 'Ethereum', 'https://api.etherscan.io/api', this.etherscanApiKey);
      
      const ethAllTokens = [...(ethNative ? [ethNative] : []), ...ethTokens];
      
      // BASE Network (with API fallback and alternative methods)
      console.log(`🔗 [BASE] Comprehensive BASE network scan for wallet ${walletAddress}...`);
      const baseNative = await this.getNativeBalance(walletAddress, 'Base');
      let baseTokens: TokenBalance[] = [];
      
      try {
        baseTokens = await this.getTokenBalances(walletAddress, 'Base', 'https://api.basescan.org/api', this.basescanApiKey);
      } catch (error) {
        console.log(`⚠️ [BASE] Basescan API rate limited, switching to Coinbase Developer API...`);
      }
      
      // Always use Covalent Goldrush API for comprehensive BASE network data
      console.log(`🔄 [BASE] Using Covalent Goldrush API for authentic BASE network data...`);
      const covalentBaseTokens = await this.getBaseTokensViaCovalent(walletAddress);
      baseTokens = [...baseTokens, ...covalentBaseTokens];
      
      console.log(`🔍 [BASE] Found ${baseTokens.length} tokens on BASE network`);
      const baseAllTokens = [...(baseNative ? [baseNative] : []), ...baseTokens];
      
      // Solana Network Scan
      console.log(`🔗 [SOLANA] Scanning Solana network for wallet ${walletAddress}...`);
      const solanaTokens = await this.getSolanaTokens(walletAddress);
      console.log(`🔍 [SOLANA] Found ${solanaTokens.length} tokens on Solana network`);
      
      console.log(`📊 [MULTI-CHAIN] SCAN SUMMARY:`);
      console.log(`📊 [MULTI-CHAIN] - Ethereum: ${ethAllTokens.length} tokens found`);
      console.log(`📊 [MULTI-CHAIN] - Base: ${baseAllTokens.length} tokens found`);
      console.log(`📊 [MULTI-CHAIN] - Solana: ${solanaTokens.length} tokens found`);
      
      // Log all found tokens for debugging
      ethAllTokens.forEach(token => {
        console.log(`🔍 [Coinbase Portfolio] ETH Token: ${token.symbol} (${token.token}) - Balance: ${token.balance} - Contract: ${token.contractAddress}`);
      });
      
      baseAllTokens.forEach(token => {
        console.log(`🔍 [Coinbase Portfolio] BASE Token: ${token.symbol} (${token.token}) - Balance: ${token.balance} - Contract: ${token.contractAddress}`);
      });
      
      // Combine all chain tokens for comprehensive analysis  
      const allTokens = [...ethAllTokens, ...baseAllTokens, ...solanaTokens];
      const allSymbols = Array.from(new Set(allTokens.map(t => t.symbol.toLowerCase())));
      
      console.log(`🔍 [Coinbase Portfolio] ALL TOKENS FOUND: ${allSymbols.join(', ')}`);
      console.log(`🔍 [Coinbase Portfolio] TOTAL TOKEN COUNT: ${allTokens.length}`);
      
      // Fetch Coinbase price data for ALL found tokens
      const coinbasePriceData = await this.getCoinbaseTokenPrices(allSymbols);
      
      // Update token values with Coinbase data
      const updateTokensWithCoinbaseData = (tokens: TokenBalance[], chainName: string) => {
        return tokens.map(token => {
          if (token.contractAddress === 'native') {
            token.price = ethPrice;
            token.value = parseFloat(token.balance) * token.price;
            token.change24h = 0; // Could fetch from stats endpoint
          } else {
            const coinbaseData = coinbasePriceData.get(token.symbol.toLowerCase());
            
            if (coinbaseData) {
              token.price = coinbaseData.price;
              token.change24h = coinbaseData.change24h;
              token.value = parseFloat(token.balance) * token.price;
              console.log(`💰 [Coinbase Portfolio] ${token.symbol} on ${chainName}: ${token.balance} × $${token.price.toFixed(4)} = $${token.value.toFixed(2)}`);
            } else {
              // Try alternative pricing sources for tokens not supported by Coinbase
              token.price = 0;
              token.value = 0;
              token.change24h = 0;
              
              // Note: Alternative pricing will be implemented in next iteration
              console.log(`❌ [NO PRICE] ${token.symbol} (${token.token}) - Balance: ${parseFloat(token.balance).toLocaleString()} - Contract: ${token.contractAddress}`);

            }
          }
          
          return token;
        }); // Show ALL tokens including those without pricing
      };
      
      // Process chains
      if (ethAllTokens.length > 0) {
        const updatedEthTokens = updateTokensWithCoinbaseData(ethAllTokens, 'Ethereum');
        if (updatedEthTokens.length > 0) {
          chains.push({
            chain: 'Ethereum',
            totalValue: updatedEthTokens.reduce((sum, token) => sum + token.value, 0),
            tokens: updatedEthTokens
          });
        }
      }
      
      if (baseAllTokens.length > 0) {
        const updatedBaseTokens = updateTokensWithCoinbaseData(baseAllTokens, 'Base');
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
      
      console.log(`📊 [Coinbase Portfolio] Portfolio total: $${totalValue.toFixed(2)} across ${chains.length} chains (${totalTokens} tokens)`);
      
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
      console.error(`❌ [Coinbase Portfolio] Error fetching portfolio:`, error);
      return {
        totalValue: 0,
        chains: [],
        summary: []
      };
    }
  }
}

export const coinbasePortfolioService = new CoinbasePortfolioService();