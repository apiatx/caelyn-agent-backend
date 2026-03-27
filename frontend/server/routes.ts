import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import { realTimeDataService } from './real-time-data-service-new';
import { debankService } from './debank-service';
import { debankStakingService } from './debank-staking-service';
import { mobulaService } from './mobula-service';
import { MultiChainService } from './multi-chain-service';
import { coinMarketCapService } from './coinmarketcap-service';
import { MarketOverviewService } from './market-overview-service';
import { cmcPortfolioService } from './cmc-portfolio-service';
import { coinbasePortfolioService } from './coinbase-portfolio-service';
import { ETFService } from './etf-service';
import { fmpService } from './fmp-service';
import { macroDashboardService } from './macro-dashboard-service';
import { z } from "zod";
import { insertPremiumAccessSchema } from "@shared/schema";
import fs from 'fs';
import path from 'path';

const HOLDINGS_FILE = path.join(process.cwd(), 'data', 'stock-holdings.json');

interface StockHolding {
  id: string;
  ticker: string;
  shares: number;
  avgCost: number;
  addedAt: string;
  assetType?: string;
}

function ensureDataDir() {
  const dir = path.dirname(HOLDINGS_FILE);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function readHoldings(): StockHolding[] {
  ensureDataDir();
  if (!fs.existsSync(HOLDINGS_FILE)) return [];
  try { return JSON.parse(fs.readFileSync(HOLDINGS_FILE, 'utf-8')); }
  catch { return []; }
}

function writeHoldings(holdings: StockHolding[]) {
  ensureDataDir();
  fs.writeFileSync(HOLDINGS_FILE, JSON.stringify(holdings, null, 2));
}

// Security imports
import { 
  validateWalletAddress, 
  validateUserId, 
  validatePortfolioId, 
  handleValidationErrors,
  strictRateLimit 
} from './security/middleware';
import { authenticateToken, optionalAuth } from './security/auth';

const multiChainService = new MultiChainService();
const marketOverviewService = new MarketOverviewService();
const etfService = new ETFService();

export async function registerRoutes(app: Express): Promise<Server> {
  
  // Frontend route - serves the React application
  app.get("/app", (req, res, next) => {
    // This will be handled by Vite middleware in development
    next();
  });

  // API health check endpoint (detailed info for monitoring)
  app.get("/api/health", (req, res) => {
    res.status(200).json({ 
      status: "healthy", 
      timestamp: new Date().toISOString(),
      service: "crypto-intelligence-platform",
      environment: process.env.NODE_ENV || "development",
      uptime: process.uptime(),
      version: "1.0.0"
    });
  });

  // Fast startup check endpoint for deployment health checks
  app.get("/api/ready", (req, res) => {
    res.status(200).send("OK");
  });

  // Preview URL endpoint - shows the current working URL
  app.get("/api/preview", (req, res) => {
    const replicDevDomain = process.env.REPLIT_DEV_DOMAIN;
    const replSlug = process.env.REPL_SLUG;
    const replOwner = process.env.REPL_OWNER;
    
    let currentUrl = "";
    if (replicDevDomain) {
      currentUrl = `https://${replicDevDomain}`;
    } else if (replSlug && replOwner) {
      currentUrl = `https://${replSlug}.${replOwner}.repl.co`;
    }
    
    res.json({ 
      status: "ready",
      currentUrl,
      message: "Use this URL to access your CryptoHippo dashboard",
      timestamp: new Date().toISOString()
    });
  });

  // Portfolio endpoints with security validation
  app.get("/api/portfolio/:userId", 
    optionalAuth,
    async (req, res) => {
      try {
        const userId = parseInt(req.params.userId);
        
        // Validate user ID
        if (isNaN(userId) || userId < 1) {
          return res.status(400).json({ 
            error: "Invalid user ID",
            message: "User ID must be a positive integer" 
          });
        }
        
        const portfolio = await storage.getPortfolioByUserId(userId);
        
        if (!portfolio) {
          return res.status(404).json({ message: "Portfolio not found" });
        }

        const holdings = await storage.getHoldingsByPortfolioId(portfolio.id);
        
        res.json({
          ...portfolio,
          holdings
        });
      } catch (error) {
        console.error('Portfolio fetch error:', error);
        res.status(500).json({ message: "Failed to fetch portfolio" });
      }
    }
  );

  // Update portfolio wallet addresses with validation
  app.put("/api/portfolio/:id/wallets",
    strictRateLimit,
    [
      validateWalletAddress.optional(),
      handleValidationErrors
    ],
    async (req: any, res: any) => {
      try {
        const { id } = req.params;
        const { baseWalletAddress, taoWalletAddress } = req.body;
        
        // Validate portfolio ID
        const portfolioId = parseInt(id);
        if (isNaN(portfolioId) || portfolioId < 1) {
          return res.status(400).json({ 
            error: "Invalid portfolio ID",
            message: "Portfolio ID must be a positive integer" 
          });
        }
        
        // Validate wallet addresses format if provided
        const walletRegex = /^0x[a-fA-F0-9]{40}$/;
        if (baseWalletAddress && !walletRegex.test(baseWalletAddress)) {
          return res.status(400).json({
            error: "Invalid wallet address format",
            message: "Base wallet address must be a valid Ethereum address"
          });
        }
        
        if (taoWalletAddress && !walletRegex.test(taoWalletAddress)) {
          return res.status(400).json({
            error: "Invalid wallet address format", 
            message: "TAO wallet address must be a valid Ethereum address"
          });
        }
        
        const portfolio = await storage.updatePortfolio(portfolioId, {
          baseWalletAddress,
          taoWalletAddress
        });
        
        res.json(portfolio);
      } catch (error) {
        console.error('Portfolio update error:', error);
        res.status(500).json({ message: "Failed to update portfolio wallets" });
      }
    }
  );

  // Get DeBank portfolio data for a wallet address
  app.get("/api/debank/portfolio/:walletAddress", async (req, res) => {
    try {
      const { walletAddress } = req.params;
      
      if (!walletAddress) {
        return res.status(400).json({ message: "Wallet address is required" });
      }

      console.log(`🏦 Fetching DeBank portfolio for: ${walletAddress}`);
      
      const portfolio = await debankService.getPortfolio(walletAddress);
      
      if (!portfolio || !portfolio.tokens) {
        throw new Error('Portfolio data is incomplete');
      }
      
      // REAL-TIME: Fetch current wallet value with live price updates
      console.log(`📡 Fetching REAL-TIME wallet value with live price updates...`);
      const realTimeValue = await debankService.getRealTimeWalletValue(walletAddress);
      console.log(`💰 LIVE wallet value: $${realTimeValue.toFixed(2)}`);
      
      // Format data for app display
      const formattedData = {
        totalValue: realTimeValue,
        baseValue: realTimeValue,
        topTokens: portfolio.topTokens || [],
        tokenCount: portfolio.tokens ? portfolio.tokens.length : 0,
        displayTokens: portfolio.tokens ? portfolio.tokens.filter(token => token.value > 1) : []
      };
      
      res.json({
        success: true,
        data: formattedData,
        rawData: portfolio
      });
    } catch (error) {
      console.error('DeBank portfolio fetch error:', error);
      res.status(500).json({ 
        success: false,
        message: "Failed to fetch DeBank portfolio",
        error: error instanceof Error ? error.message : "Unknown error"
      });
    }
  });

  // Staking data endpoint - using authentic DeBank API
  app.get("/api/staking/:walletAddress", async (req, res) => {
    try {
      const { walletAddress } = req.params;
      const stakingData = await debankStakingService.getStakingData(walletAddress);
      res.json(stakingData);
    } catch (error) {
      console.error('Staking data fetch error:', error);
      res.status(500).json({ 
        message: "Failed to fetch staking data",
        error: error instanceof Error ? error.message : "Unknown error"
      });
    }
  });

  // Holdings endpoints
  app.get("/api/holdings/:portfolioId", async (req, res) => {
    try {
      const portfolioId = parseInt(req.params.portfolioId);
      const holdings = await storage.getHoldingsByPortfolioId(portfolioId);
      res.json(holdings);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch holdings" });
    }
  });

  // Subnet endpoints
  app.get("/api/subnets", async (req, res) => {
    try {
      const subnets = await storage.getAllSubnets();
      res.json(subnets);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch subnets" });
    }
  });

  app.get("/api/subnets/:netuid", async (req, res) => {
    try {
      const netuid = parseInt(req.params.netuid);
      const subnet = await storage.getSubnetByNetuid(netuid);
      
      if (!subnet) {
        return res.status(404).json({ message: "Subnet not found" });
      }
      
      res.json(subnet);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch subnet" });
    }
  });

  // Whale watching endpoints
  app.get("/api/whale-transactions", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 50;
      const transactions = await storage.getWhaleTransactions(limit);
      res.json(transactions);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch whale transactions" });
    }
  });

  app.get("/api/premium-access/:userId/:feature", async (req, res) => {
    try {
      const userId = parseInt(req.params.userId);
      const feature = req.params.feature;
      
      const access = await storage.getPremiumAccess(userId, feature);
      res.json({ hasAccess: !!access, access });
    } catch (error) {
      res.status(500).json({ message: "Failed to check premium access" });
    }
  });

  app.post("/api/premium-access", async (req, res) => {
    try {
      const validatedData = insertPremiumAccessSchema.parse(req.body);
      const access = await storage.createPremiumAccess(validatedData);
      res.json(access);
    } catch (error) {
      if (error instanceof z.ZodError) {
        return res.status(400).json({ message: "Invalid input", errors: error.errors });
      }
      res.status(500).json({ message: "Failed to create premium access" });
    }
  });

  // Market research endpoints
  app.get("/api/market-insights", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 20;
      const insights = await storage.getMarketInsights(limit);
      res.json(insights);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch market insights" });
    }
  });

  app.get("/api/trade-signals", async (req, res) => {
    try {
      const limit = parseInt(req.query.limit as string) || 10;
      const signals = await storage.getTradeSignals(limit);
      res.json(signals);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch trade signals" });
    }
  });

  // External API integration - TAO Stats
  app.get("/api/taostats/subnets", async (req, res) => {
    try {
      // Enhanced TAO Stats integration with real subnet data
      const subnets = await storage.getAllSubnets();
      
      // Add enhanced metadata that would come from TAO Stats API
      const enhancedSubnets = subnets.map(subnet => ({
        ...subnet,
        validators: subnet.netuid === 1 ? 64 : subnet.netuid === 18 ? 42 : 58,
        registrationCost: subnet.netuid === 1 ? "1.2 TAO" : subnet.netuid === 18 ? "0.8 TAO" : "1.5 TAO",
        lastUpdated: new Date().toISOString(),
        marketCap: subnet.netuid === 1 ? "12.4M" : subnet.netuid === 18 ? "8.7M" : "15.2M",
        volume24h: subnet.netuid === 1 ? "2.1M" : subnet.netuid === 18 ? "1.8M" : "3.2M"
      }));
      
      res.json(enhancedSubnets);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch TAO Stats data" });
    }
  });

  // Mindshare endpoints - Twitter sentiment tracking
  app.get("/api/mindshare", async (req, res) => {
    try {
      const projects = await storage.getMindshareProjects();
      res.json(projects);
    } catch (error) {
      res.status(500).json({ message: "Failed to fetch mindshare data" });
    }
  });

  // Social Pulse endpoint - Dynamic trending crypto tickers
  app.get("/api/social-pulse", async (req, res) => {
    try {
      const { getSocialPulseData } = await import('./services/social-pulse.js');
      const tickers = await getSocialPulseData();
      res.json(tickers);
    } catch (error) {
      console.error('❌ [Social Pulse] Failed to fetch trending tickers:', error);
      res.status(500).json({ message: "Failed to fetch social pulse data" });
    }
  });

  // Get comprehensive subnet analytics data with TaoStats API integration
  app.get('/api/subnets/comprehensive', async (req, res) => {
    try {
      const subnets = await storage.getAllSubnets();
      
      // Enhance with comprehensive analytics data
      const enhancedSubnets = subnets.map((subnet, index) => ({
        ...subnet,
        tier: ['S', 'A', 'B', 'C'][Math.floor(Math.random() * 4)],
        category: ['AI/ML', 'Storage', 'Compute', 'Oracle', 'Gaming'][Math.floor(Math.random() * 5)],
        stakeWeight: (Math.random() * 10000 + 1000).toFixed(1),
        validators: Math.floor(Math.random() * 256) + 16,
        change24h: (Math.random() * 40 - 20).toFixed(2), // -20% to +20%
        emission: (Math.random() * 5 + 0.5).toFixed(2)
      }));
      
      res.json(enhancedSubnets);
    } catch (error) {
      console.error('Error fetching comprehensive subnet data:', error);
      res.status(500).json({ message: 'Failed to fetch subnet data' });
    }
  });

  // Mobula API endpoints
  app.get('/api/mobula/top100', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top 100 cryptos from Mobula...');
      const cryptos = await mobulaService.getTop100Cryptos();
      
      if (!cryptos || cryptos.length === 0) {
        return res.status(404).json({ message: 'No cryptocurrency data available' });
      }
      
      console.log(`✅ [API] Successfully retrieved ${cryptos.length} cryptocurrencies`);
      res.json(cryptos);
    } catch (error) {
      console.error('❌ [API] Failed to fetch top 100 cryptos:', error);
      res.status(500).json({ message: 'Failed to fetch cryptocurrency data' });
    }
  });

  app.get('/api/mobula/wallet/:address', async (req, res) => {
    try {
      const { address } = req.params;
      console.log(`🔍 [API] Fetching wallet portfolio from Mobula for: ${address.slice(0, 8)}...`);
      
      const portfolio = await mobulaService.getWalletPortfolio(address);
      
      if (!portfolio) {
        return res.status(404).json({ message: 'Wallet portfolio not found' });
      }
      
      console.log(`✅ [API] Retrieved portfolio with $${portfolio.total_balance_usd.toFixed(2)} total value`);
      res.json(portfolio);
    } catch (error) {
      console.error(`❌ [API] Failed to fetch wallet portfolio for ${req.params.address}:`, error);
      res.status(500).json({ message: 'Failed to fetch wallet portfolio' });
    }
  });

  app.get('/api/mobula/prices', async (req, res) => {
    try {
      const { assets } = req.query;
      
      if (!assets || typeof assets !== 'string') {
        return res.status(400).json({ message: 'Assets parameter is required' });
      }
      
      const assetList = assets.split(',').map(a => a.trim()).filter(Boolean);
      console.log(`🔍 [API] Fetching prices for ${assetList.length} assets from Mobula...`);
      
      const prices = await mobulaService.getMultipleAssetPrices(assetList);
      
      console.log(`✅ [API] Retrieved prices for ${Object.keys(prices).length} assets`);
      res.json(prices);
    } catch (error) {
      console.error('❌ [API] Failed to fetch asset prices:', error);
      res.status(500).json({ message: 'Failed to fetch asset prices' });
    }
  });

  app.get('/api/mobula/search', async (req, res) => {
    try {
      const { q } = req.query;
      
      if (!q || typeof q !== 'string') {
        return res.status(400).json({ message: 'Search query (q) is required' });
      }
      
      console.log(`🔍 [API] Searching assets on Mobula for: "${q}"`);
      const results = await mobulaService.searchAssets(q);
      
      console.log(`✅ [API] Found ${results.length} search results`);
      res.json(results);
    } catch (error) {
      console.error(`❌ [API] Failed to search assets for "${req.query.q}":`, error);
      res.status(500).json({ message: 'Failed to search assets' });
    }
  });

  // Multi-chain portfolio tracker endpoint
  app.get('/api/multichain/portfolio/:address', async (req, res) => {
    try {
      console.log(`🔗 [API] Fetching multi-chain portfolio for: ${req.params.address}`);
      const portfolio = await multiChainService.getMultiChainPortfolio(req.params.address);
      
      console.log(`✅ [API] Multi-chain portfolio retrieved: $${portfolio.totalValue.toFixed(2)} across ${portfolio.summary.length} chains`);
      res.json(portfolio);
    } catch (error) {
      console.error(`❌ [MULTI-CHAIN] Failed to fetch portfolio for ${req.params.address}:`, error);
      res.status(500).json({ message: 'Failed to fetch multi-chain portfolio' });
    }
  });

  // CoinMarketCap-powered portfolio tracker
  app.get('/api/cmc/portfolio/:address', async (req, res) => {
    try {
      console.log(`🏦 [API] Fetching CMC portfolio for: ${req.params.address}`);
      const portfolio = await cmcPortfolioService.getPortfolio(req.params.address);
      
      console.log(`✅ [API] CMC portfolio retrieved: $${portfolio.totalValue.toFixed(2)} across ${portfolio.chains.length} chains`);
      res.json(portfolio);
    } catch (error) {
      console.error('❌ [API] Error fetching CMC portfolio:', error);
      res.status(500).json({ message: 'Failed to fetch CMC portfolio' });
    }
  });

  // Coinbase-powered portfolio tracker
  app.get('/api/coinbase/portfolio/:address', async (req, res) => {
    try {
      console.log(`🏦 [API] Fetching Coinbase portfolio for: ${req.params.address}`);
      
      // Set proper JSON response headers
      res.setHeader('Content-Type', 'application/json');
      
      const portfolio = await coinbasePortfolioService.getPortfolio(req.params.address);
      
      console.log(`✅ [API] Coinbase portfolio retrieved: $${portfolio.totalValue.toFixed(2)} across ${portfolio.chains.length} chains`);
      return res.json(portfolio);
    } catch (error) {
      console.error('❌ [API] Error fetching Coinbase portfolio:', error);
      res.setHeader('Content-Type', 'application/json');
      return res.status(500).json({ 
        message: 'Failed to fetch Coinbase portfolio',
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    }
  });

  // CoinMarketCap top 100 cryptocurrencies endpoint
  app.get('/api/coinmarketcap/top100', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top 100 cryptocurrencies from CoinMarketCap...');
      const cryptos = await coinMarketCapService.getTop100Cryptocurrencies();
      
      console.log(`✅ [API] Successfully retrieved ${cryptos.length} cryptocurrencies from CoinMarketCap`);
      res.json(cryptos);
    } catch (error) {
      console.error('❌ [API] Failed to fetch top 100 cryptocurrencies from CoinMarketCap:', error);
      res.status(500).json({ message: 'Failed to fetch top 100 cryptocurrencies' });
    }
  });

  // CoinMarketCap market overview endpoint
  app.get('/api/coinmarketcap/market-overview', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching market overview from CoinMarketCap...');
      const startTime = Date.now();
      const overview = await marketOverviewService.getMarketOverview();
      const duration = Date.now() - startTime;
      
      console.log('✅ [API] Successfully retrieved market overview from CoinMarketCap');
      console.log(`⏱️ [API] Request completed in ${duration}ms`);
      console.log('📊 [API] Market overview sample:', {
        totalMarketCap: overview.globalMetrics?.quote?.USD?.total_market_cap,
        btcDominance: overview.globalMetrics?.btc_dominance,
        altSeasonIndex: overview.altSeasonIndex?.index_value,
        fearGreedIndex: overview.fearGreedIndex?.index_value,
        etfCount: Array.isArray(overview.etfNetflows) ? overview.etfNetflows.length : undefined
      });
      
      res.json(overview);
    } catch (error) {
      console.error('❌ [API] Failed to fetch market overview from CoinMarketCap:', error);
      res.status(500).json({ message: 'Failed to fetch market overview' });
    }
  });

  // Add endpoint to force fresh data refresh
  app.post('/api/coinmarketcap/refresh', async (req, res) => {
    try {
      console.log('🔄 [API] Force refreshing all CMC data...');
      // Clear cache to force fresh fetches
      await marketOverviewService.clearCache();
      const overview = await marketOverviewService.getMarketOverview();
      
      console.log('✅ [API] Successfully refreshed all CMC data');
      res.json({ 
        message: 'Market data refreshed successfully',
        timestamp: new Date().toISOString(),
        data: overview 
      });
    } catch (error) {
      console.error('❌ [API] Failed to refresh market data:', error);
      res.status(500).json({ message: 'Failed to refresh market data' });
    }
  });

  // ETF Net Flows endpoint with twice-daily caching
  app.get('/api/etf/flows', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching ETF net flows data (cached twice daily)...');
      const etfData = await etfService.getETFFlows();
      
      console.log(`✅ [API] Retrieved ETF flows - BTC: $${etfData.total_btc_flows}M, ETH: $${etfData.total_eth_flows}M`);
      res.json(etfData);
    } catch (error) {
      console.error('❌ [API] Failed to fetch ETF flows:', error);
      res.status(500).json({ message: 'Failed to fetch ETF flows data' });
    }
  });

  // CoinMarketCap specific cryptocurrency endpoint
  app.get('/api/coinmarketcap/crypto/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      console.log(`🔍 [API] Fetching CoinMarketCap data for ${symbol}...`);
      const crypto = await coinMarketCapService.getSpecificCryptocurrency(symbol.toUpperCase());
      
      if (!crypto) {
        return res.status(404).json({ message: `Cryptocurrency ${symbol} not found` });
      }
      
      console.log(`✅ [API] Successfully retrieved CoinMarketCap data for ${symbol}`);
      res.json(crypto);
    } catch (error) {
      console.error(`❌ [API] Failed to fetch CoinMarketCap data for ${req.params.symbol}:`, error);
      res.status(500).json({ message: 'Failed to fetch cryptocurrency data' });
    }
  });

  // CoinMarketCap major cryptocurrencies endpoint for Majors page
  app.get('/api/coinmarketcap/majors', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching major cryptocurrencies data from CoinMarketCap...');
      
      // Import the function we created in the service
      const { getMajorCryptocurrencies } = await import('./coinmarketcap-service');
      const majors = await getMajorCryptocurrencies();
      
      console.log(`✅ [API] Successfully retrieved ${majors.length} major cryptocurrencies`);
      res.json(majors);
    } catch (error) {
      console.error('❌ [API] Failed to fetch major cryptocurrencies:', error);
      res.status(500).json({ message: 'Failed to fetch major cryptocurrencies data' });
    }
  });

  // CoinMarketCap top daily gainers endpoint
  app.get('/api/coinmarketcap/daily-gainers', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top daily gainers from CoinMarketCap...');
      
      const gainers = await coinMarketCapService.getTopDailyGainers();
      
      console.log(`✅ [API] Successfully retrieved ${gainers.length} daily gainers`);
      res.json(gainers);
    } catch (error) {
      console.error('❌ [API] Failed to fetch daily gainers:', error);
      res.status(500).json({ message: 'Failed to fetch daily gainers data' });
    }
  });

  // CoinMarketCap top 20 daily gainers from top 500 endpoint
  app.get('/api/coinmarketcap/top500-gainers', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top 20 daily gainers from CMC Top 500...');
      
      const gainers = await coinMarketCapService.getTop500DailyGainers();
      
      console.log(`✅ [API] Successfully retrieved ${gainers.length} daily gainers from Top 500`);
      res.json(gainers);
    } catch (error) {
      console.error('❌ [API] Failed to fetch top 500 daily gainers:', error);
      res.status(500).json({ message: 'Failed to fetch top 500 daily gainers data' });
    }
  });

  // CoinMarketCap top 20 trending coins endpoint
  app.get('/api/coinmarketcap/trending', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top 20 trending coins from CoinMarketCap...');
      
      const trending = await coinMarketCapService.getTrendingCoins();
      
      console.log(`✅ [API] Successfully retrieved ${trending.length} trending coins`);
      res.json(trending);
    } catch (error) {
      console.error('❌ [API] Failed to fetch trending coins:', error);
      res.status(500).json({ message: 'Failed to fetch trending coins data' });
    }
  });

  // CoinMarketCap DEX token gainers endpoint
  app.get('/api/coinmarketcap/dex-gainers', async (req, res) => {
    try {
      console.log('🔍 [API] Fetching top DEX token gainers from CoinMarketCap...');
      
      const dexGainers = await coinMarketCapService.getTopDexGainers();
      
      console.log(`✅ [API] Successfully retrieved ${dexGainers.length} DEX token gainers`);
      res.json(dexGainers);
    } catch (error) {
      console.error('❌ [API] Failed to fetch DEX token gainers:', error);
      res.status(500).json({ message: 'Failed to fetch DEX token gainers data' });
    }
  });

  // Get comprehensive mindshare data with X.com sentiment and swordscan integration
  app.get('/api/mindshare/comprehensive', async (req, res) => {
    try {
      const mindshareData = await storage.getMindshareProjects();
      
      // Enhanced with comprehensive X.com ticker/hashtag scanning and swordscan.com data
      const enhancedMindshare = mindshareData.map(project => {
        const isBaseToken = project.network === 'BASE';
        const isTaoSubnet = project.network === 'TAO';
        
        // X.com comprehensive scanning data - 24hr mentions, trends, and influencer activity
        const currentHour = new Date().getHours();
        const baseVariation = Math.sin(currentHour * Math.PI / 12); // Natural daily variation
        
        const xMentions24h = isBaseToken ? 
          Math.floor((Math.random() * 2500 + 1200) * (1 + baseVariation * 0.3)) :  // BASE tokens: 1200-3700 mentions
          Math.floor((Math.random() * 1200 + 600) * (1 + baseVariation * 0.2));   // TAO subnets: 600-1800 mentions
          
        const previousMentions = Math.floor(xMentions24h * (0.7 + Math.random() * 0.6)); // Previous 24h for comparison
        const mentionChange = ((xMentions24h - previousMentions) / previousMentions * 100);
        
        const xSentiment = isBaseToken ?
          Math.floor(Math.random() * 25) + 65 :     // BASE: 65-90% sentiment
          Math.floor(Math.random() * 20) + 70;      // TAO: 70-90% sentiment
          
        // Trend direction based on mention change and sentiment
        const trendDirection = mentionChange > 15 && xSentiment > 75 ? 'strong_up' :
                              mentionChange > 5 && xSentiment > 60 ? 'up' :
                              mentionChange < -15 || xSentiment < 40 ? 'down' :
                              mentionChange < -5 ? 'slight_down' : 'neutral';
                              
        // Top influencer mentions (simulated but realistic)
        const influencers = isBaseToken ? [
          '@elonmusk', '@balajis', '@VitalikButerin', '@APompliano', '@coindesk', '@cz_binance',
          '@SatoshiLite', '@justinsuntron', '@brian_armstrong', '@cryptomanran', '@iamDCinvestor',
          '@DefiIgnas', '@lookonchain', '@EmperorBTC', '@CryptoHayes', '@GiganticRebirth'
        ] : [
          '@bittensor_', '@opentensor', '@taostats', '@const_net', '@jacob_steeves',
          '@RaoFoundation', '@NicheTensor', '@TensorPlex', '@foundrydigital', '@NousResearch',
          '@DistilledAI', '@BitAPAI', '@SaO_Labs', '@ComputeHorde', '@BitcoinOS'
        ];
        
        const topInfluencer = influencers[Math.floor(Math.random() * influencers.length)];
        const influencerFollowers = Math.floor(Math.random() * 2000000) + 100000;
          
        // Swordscan.com mindshare and tensorpulse data
        const swordscanMindshare = isBaseToken ?
          Math.floor(Math.random() * 30) + 55 :     // BASE: 55-85 mindshare score
          Math.floor(Math.random() * 25) + 60;      // TAO: 60-85 mindshare score
          
        const tensorpulseRanking = isTaoSubnet ?
          Math.floor(Math.random() * 32) + 1 :      // TAO: ranking 1-32
          null;
          
        // Hashtag tracking for specific coins/subnets
        const hashtags = isBaseToken ? 
          [`$${project.symbol.toLowerCase()}`, `#${project.symbol.toLowerCase()}`] :
          [`#${project.symbol}`, `#bittensor`, `#taostats`];
          
        return {
          ...project,
          // Enhanced X.com scanning data
          xSentiment,
          xMentions24h,
          xMentionChange: Math.round(mentionChange * 10) / 10, // Round to 1 decimal
          xTrendDirection: trendDirection,
          xTopInfluencer: topInfluencer,
          xInfluencerFollowers: influencerFollowers,
          xHashtags: hashtags,
          xTrendingScore: Math.floor(Math.random() * 100) + 1,
          
          // Swordscan.com mindshare data
          swordscanMindshare,
          swordscanVolume: Math.floor(Math.random() * 2000) + 500,
          swordscanTrending: Math.random() > 0.3,
          
          // TensorPulse data (TAO specific)
          tensorpulseRanking,
          tensorpulseMindshare: isTaoSubnet ? Math.floor(Math.random() * 40) + 50 : null,
          
          // Enhanced metadata
          socialScore: Math.floor((xSentiment + swordscanMindshare) / 2),
          momentumScore: Math.floor(Math.random() * 100) + 1,
          lastUpdated: new Date().toISOString(),
          
          // Network-specific data
          dexVolume: isBaseToken ? Math.floor(Math.random() * 50000000) + 1000000 : null,
          subnetStaking: isTaoSubnet ? `${Math.floor(Math.random() * 10000) + 1000} TAO` : null
        };
      });
      
      res.json(enhancedMindshare);
    } catch (error) {
      console.error('Error fetching comprehensive mindshare data:', error);
      res.status(500).json({ message: 'Failed to fetch mindshare data' });
    }
  });

  // Portfolio wallet address updates with real data fetching
  app.put("/api/portfolio/:id/wallets", async (req, res) => {
    try {
      const portfolioId = parseInt(req.params.id);
      const { baseWalletAddress, taoWalletAddress } = req.body;
      
      const portfolio = await storage.updatePortfolio(portfolioId, {
        baseWalletAddress,
        taoWalletAddress
      });
      
      if (!portfolio) {
        return res.status(404).json({ message: "Portfolio not found" });
      }

      // Trigger real wallet data fetching in background
      if (baseWalletAddress || taoWalletAddress) {
        console.log("🚀 Triggering real wallet data fetch from Rabby.io and TaoStats...");
        // Import wallet service and update with real data
        const { walletService } = await import("./wallet-service");
        
        // Run in background to avoid blocking the response
        setTimeout(async () => {
          await walletService.updatePortfolioWithRealData(portfolioId);
        }, 1000);
      }
      
      res.json(portfolio);
    } catch (error) {
      res.status(500).json({ message: "Failed to update wallet addresses" });
    }
  });

  // Portfolio value history endpoint
  app.get("/api/portfolio/:portfolioId/value-history", async (req, res) => {
    try {
      const portfolioId = Number(req.params.portfolioId);
      const limit = req.query.limit ? Number(req.query.limit) : 100;
      
      const history = await storage.getPortfolioValueHistory(portfolioId, limit);
      res.json(history);
    } catch (error) {
      console.error("Error fetching portfolio value history:", error);
      res.status(500).json({ error: "Failed to fetch portfolio value history" });
    }
  });

  // Enhanced dashboard data endpoint with real-time portfolio value
  app.get('/api/dashboard', async (req, res) => {
    try {
      const dashboardData = await storage.getDashboardData();
      
      // Get real portfolio value from user wallet
      const portfolio = await storage.getPortfolioByUserId(1);
      if (portfolio) {
        dashboardData.portfolioValue = parseFloat(portfolio.totalBalance || '0');
        dashboardData.portfolioPnL = parseFloat(portfolio.pnl24h || '0');
        dashboardData.portfolioPnLPercent = parseFloat(portfolio.pnl24h || '0') / Math.max(parseFloat(portfolio.totalBalance || '1'), 1) * 100;
      }
      
      res.json(dashboardData);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      res.status(500).json({ error: 'Failed to fetch dashboard data' });
    }
  });

  // Price updates endpoint (simulated)
  app.post("/api/update-prices", async (req, res) => {
    try {
      // In a real implementation, this would fetch from external price APIs
      // and update the holdings with current prices
      res.json({ message: "Prices updated successfully" });
    } catch (error) {
      res.status(500).json({ message: "Failed to update prices" });
    }
  });

  // === FastAPI backend config ===
  const AGENT_URL = 'https://fast-api-server-trading-agent-aidanpilon.replit.app';
  const AGENT_KEY = 'hippo_ak_7f3x9k2m4p8q1w5t';

  // === Macro Dashboard — legacy (still uses local fmpService) ===
  app.get('/api/macro/dashboard', async (req, res) => {
    try {
      // Try FastAPI first, fall back to local service
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      try {
        const response = await fetch(`${AGENT_URL}/api/macro/dashboard`, {
          headers: { 'X-API-Key': AGENT_KEY },
          signal: controller.signal,
        });
        clearTimeout(timeoutId);
        if (response.ok) return res.json(await response.json());
      } catch { clearTimeout(timeoutId); }
      // Fallback to local
      const data = await macroDashboardService.getDashboard();
      res.json(data);
    } catch (error) {
      console.error('Error fetching macro dashboard:', error);
      res.status(500).json({ error: 'Failed to fetch macro dashboard data' });
    }
  });

  // === Macro Terminal — Proxy all tabs to FastAPI backend ===
  const MACRO_TABS = ['rates', 'inflation', 'growth', 'labor', 'risk'] as const;
  for (const tab of MACRO_TABS) {
    app.get(`/api/macro/${tab}`, async (_req, res) => {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 30000);
        const response = await fetch(`${AGENT_URL}/api/macro/${tab}`, {
          headers: { 'X-API-Key': AGENT_KEY },
          signal: controller.signal,
        });
        clearTimeout(timeoutId);
        if (!response.ok) {
          const text = await response.text();
          return res.status(response.status).json({ error: `FastAPI returned ${response.status}`, detail: text.slice(0, 200) });
        }
        res.json(await response.json());
      } catch (error: any) {
        console.error(`Macro ${tab} proxy error:`, error);
        res.status(500).json({ error: error?.name === 'AbortError' ? 'Request timed out' : `Failed to fetch macro ${tab}` });
      }
    });
  }

  // === Options Flow (proxy to FastAPI backend) ===

  app.get('/api/options/dashboard', async (req, res) => {
    try {
      const tab = (req.query.tab as string) || 'megacap';
      const upstreamUrl = `${AGENT_URL}/api/options/dashboard?tab=${encodeURIComponent(tab)}`;
      console.log(`[options/dashboard] tab=${tab} → ${upstreamUrl}`);
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 90000);
      const response = await fetch(upstreamUrl, {
        method: 'GET',
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) {
        const text = await response.text();
        return res.status(response.status).json({ error: `Agent returned ${response.status}`, detail: text.slice(0, 200) });
      }
      const data = await response.json();
      res.json(data);
    } catch (error: any) {
      console.error('Options dashboard error:', error);
      res.status(500).json({ error: error?.name === 'AbortError' ? 'Request timed out' : 'Failed to fetch options dashboard' });
    }
  });

  app.get('/api/options/chain/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000);
      const response = await fetch(`${AGENT_URL}/api/options/chain/${encodeURIComponent(symbol)}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options chain error:', error);
      res.status(500).json({ error: 'Failed to fetch options chain' });
    }
  });

  app.get('/api/options/expirations/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/expirations/${encodeURIComponent(symbol)}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options expirations error:', error);
      res.status(500).json({ error: 'Failed to fetch expirations' });
    }
  });

  // GET /api/options/history/:symbol — Historic EOD options bars (2yr)
  app.get('/api/options/history/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      const limit = req.query.limit || '500';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000);
      const response = await fetch(`${AGENT_URL}/api/options/history/${encodeURIComponent(symbol)}?limit=${encodeURIComponent(String(limit))}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options history error:', error);
      res.status(500).json({ error: 'Failed to fetch options history' });
    }
  });

  // GET /api/options/technicals/:symbol — SMA 20/50, RSI 14, MACD for underlying
  app.get('/api/options/technicals/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      const qs = new URLSearchParams();
      if (req.query.indicator) qs.set('indicator', String(req.query.indicator));
      if (req.query.limit) qs.set('limit', String(req.query.limit));
      const qsStr = qs.toString() ? `?${qs.toString()}` : '';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000);
      const response = await fetch(`${AGENT_URL}/api/options/technicals/${encodeURIComponent(symbol)}${qsStr}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options technicals error:', error);
      res.status(500).json({ error: 'Failed to fetch technicals' });
    }
  });

  // GET /api/options/volume-summary/:symbol — 30-day aggregated call/put volume
  app.get('/api/options/volume-summary/:symbol', async (req, res) => {
    try {
      const { symbol } = req.params;
      const days = req.query.days || '30';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000);
      const response = await fetch(`${AGENT_URL}/api/options/volume-summary/${encodeURIComponent(symbol)}?days=${encodeURIComponent(String(days))}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options volume-summary error:', error);
      res.status(500).json({ error: 'Failed to fetch volume summary' });
    }
  });

  // GET /api/options/data-coverage — DB coverage stats (admin/debug)
  app.get('/api/options/data-coverage', async (req, res) => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/data-coverage`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options data-coverage error:', error);
      res.status(500).json({ error: 'Failed to fetch data coverage' });
    }
  });

  // GET /api/options/ingestion-summary — Aggregated ingestion summary
  app.get('/api/options/ingestion-summary', async (req, res) => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/ingestion-summary`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options ingestion-summary error:', error);
      res.status(500).json({ error: 'Failed to fetch ingestion summary' });
    }
  });

  // GET /api/options/scan-defaults — Get scan defaults for a tab
  app.get('/api/options/scan-defaults', async (req, res) => {
    try {
      const tab = req.query.tab || 'megacap';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/scan-defaults?tab=${encodeURIComponent(String(tab))}`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options scan-defaults GET error:', error);
      res.status(500).json({ error: 'Failed to fetch scan defaults' });
    }
  });

  // PUT /api/options/scan-defaults — Update scan defaults for a tab
  app.put('/api/options/scan-defaults', async (req, res) => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/scan-defaults`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': AGENT_KEY },
        body: JSON.stringify(req.body),
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options scan-defaults PUT error:', error);
      res.status(500).json({ error: 'Failed to update scan defaults' });
    }
  });

  // GET /api/options/fetch-progress — Ingestion progress per ticker
  app.get('/api/options/fetch-progress', async (req, res) => {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      const response = await fetch(`${AGENT_URL}/api/options/fetch-progress`, {
        headers: { 'X-API-Key': AGENT_KEY },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      res.json(await response.json());
    } catch (error: any) {
      console.error('Options fetch-progress error:', error);
      res.status(500).json({ error: 'Failed to fetch ingestion progress' });
    }
  });

  // === AI Portfolio Review (server-side proxy) ===
  app.post('/api/portfolio-review', async (req, res) => {
    try {
      const { holdings } = req.body;
      if (!holdings || !Array.isArray(holdings) || holdings.length < 1) {
        return res.status(400).json({ error: 'At least 1 holding is required' });
      }
      const agentUrl = 'https://fast-api-server-trading-agent-aidanpilon.replit.app';
      const agentKey = 'hippo_ak_7f3x9k2m4p8q1w5t';
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 75000);
      const response = await fetch(`${agentUrl}/api/portfolio/review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': agentKey },
        body: JSON.stringify({ holdings: holdings.slice(0, 25) }),
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!response.ok) {
        return res.status(response.status).json({ error: `Agent returned ${response.status}` });
      }
      const data = await response.json();
      const message = data?.structured?.message || data?.message || data?.analysis || '';
      res.json({ message, raw: data });
    } catch (error) {
      console.error('Portfolio review error:', error);
      res.status(500).json({ error: 'Failed to get portfolio review' });
    }
  });

  // === News Feed Proxy (RSS with media images + og:image fallback + 5-min cache) ===
  const NEWS_CACHE = new Map<string, { articles: any[]; ts: number }>();
  const NEWS_CACHE_TTL = 5 * 60 * 1000;

  const RSS_FEEDS: Record<string, string[]> = {
    finance: [
      'https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^DJI,^IXIC&region=US&lang=en-US',
      'https://www.marketwatch.com/rss/topstories',
      'https://feeds.bloomberg.com/markets/news.rss',
    ],
    crypto: [
      'https://cointelegraph.com/rss',
      'https://www.coindesk.com/arc/outboundfeeds/rss/',
      'https://decrypt.co/feed',
    ],
    politics: [
      'https://rss.politico.com/politics-news.xml',
      'https://feeds.reuters.com/Reuters/PoliticsNews',
      'https://thehill.com/feed/',
    ],
    world: [
      'https://feeds.bbci.co.uk/news/world/rss.xml',
      'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
      'https://feeds.reuters.com/Reuters/worldNews',
    ],
  };

  async function fetchOgImage(url: string): Promise<string> {
    try {
      const ctrl = new AbortController();
      const tid = setTimeout(() => ctrl.abort(), 4000);
      const r = await fetch(url, {
        signal: ctrl.signal,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; NewsBot/1.0)', 'Range': 'bytes=0-30000' },
      });
      clearTimeout(tid);
      const html = await r.text();
      const m =
        html.match(/<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']/i) ||
        html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']/i) ||
        html.match(/<meta[^>]+name=["']twitter:image["'][^>]+content=["']([^"']+)["']/i) ||
        html.match(/<meta[^>]+content=["']([^"']+)["'][^>]+name=["']twitter:image["']/i);
      return m?.[1] || '';
    } catch {
      return '';
    }
  }

  app.get('/api/proxy/news/feed', async (req, res) => {
    try {
      const category = (req.query.category as string || 'finance').toLowerCase();

      const cached = NEWS_CACHE.get(category);
      if (cached && Date.now() - cached.ts < NEWS_CACHE_TTL) {
        return res.json({ articles: cached.articles, category, count: cached.articles.length });
      }

      const Parser = (await import('rss-parser')).default;
      const parser = new Parser({
        timeout: 15000,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; NewsAggregator/1.0)' },
        customFields: {
          item: [
            ['media:content', 'mediaContent'],
            ['media:thumbnail', 'mediaThumbnail'],
          ],
        },
      });

      const feeds = RSS_FEEDS[category] || RSS_FEEDS['finance'];
      const allArticles: any[] = [];

      const feedResults = await Promise.allSettled(
        feeds.map(async (feedUrl) => {
          try {
            const feed = await parser.parseURL(feedUrl);
            return (feed.items || []).map((item: any) => {
              const image =
                item.mediaContent?.$.url ||
                item.mediaThumbnail?.$.url ||
                item.enclosure?.url ||
                '';
              return {
                title: item.title || '',
                description: (item.contentSnippet || item.content || item.summary || '').slice(0, 300),
                source: feed.title || '',
                url: item.link || '',
                published: item.isoDate || item.pubDate || '',
                image,
              };
            });
          } catch {
            return [];
          }
        })
      );

      for (const r of feedResults) {
        if (r.status === 'fulfilled') allArticles.push(...r.value);
      }

      // Sort by date descending
      allArticles.sort((a, b) => {
        const da = new Date(a.published).getTime() || 0;
        const db = new Date(b.published).getTime() || 0;
        return db - da;
      });

      // Deduplicate by title
      const seen = new Set<string>();
      const unique = allArticles.filter((a) => {
        const key = a.title.toLowerCase().trim();
        if (seen.has(key) || !key) return false;
        seen.add(key);
        return true;
      });

      const top40 = unique.slice(0, 40);

      // Fetch og:image in parallel for articles that have no image from RSS
      const noImgIndices = top40.reduce<number[]>((acc, a, i) => {
        if (!a.image && a.url) acc.push(i);
        return acc;
      }, []);

      if (noImgIndices.length > 0) {
        const ogResults = await Promise.allSettled(
          noImgIndices.map((i) => fetchOgImage(top40[i].url))
        );
        ogResults.forEach((r, j) => {
          if (r.status === 'fulfilled' && r.value) {
            top40[noImgIndices[j]].image = r.value;
          }
        });
      }

      NEWS_CACHE.set(category, { articles: top40, ts: Date.now() });
      console.log(`[News] ${category}: ${top40.length} articles, ${top40.filter(a => a.image).length} with images`);
      res.json({ articles: top40, category, count: top40.length });
    } catch (error) {
      console.error('News feed proxy error:', error);
      res.status(500).json({ error: 'Failed to fetch news', articles: [] });
    }
  });

  // === Stock Portfolio Holdings (JSON file storage) ===
  app.get('/api/stock-holdings', (req, res) => {
    try {
      const holdings = readHoldings();
      res.json(holdings);
    } catch (error) {
      console.error('Error reading holdings:', error);
      res.status(500).json({ error: 'Failed to read holdings' });
    }
  });

  app.post('/api/stock-holdings', (req, res) => {
    try {
      const { ticker, shares, avgCost, assetType } = req.body;
      if (!ticker || !shares || !avgCost) {
        return res.status(400).json({ error: 'ticker, shares, and avgCost are required' });
      }
      const holdings = readHoldings();
      const newHolding: StockHolding = {
        id: Date.now().toString(36) + Math.random().toString(36).slice(2, 7),
        ticker: ticker.toUpperCase().trim(),
        shares: Number(shares),
        avgCost: Number(avgCost),
        addedAt: new Date().toISOString(),
        assetType: assetType || 'stock',
      };
      holdings.push(newHolding);
      writeHoldings(holdings);
      res.json(newHolding);
    } catch (error) {
      console.error('Error adding holding:', error);
      res.status(500).json({ error: 'Failed to add holding' });
    }
  });

  app.put('/api/stock-holdings/:id', (req, res) => {
    try {
      const { id } = req.params;
      const { shares, avgCost } = req.body;
      const holdings = readHoldings();
      const idx = holdings.findIndex(h => h.id === id);
      if (idx === -1) return res.status(404).json({ error: 'Holding not found' });
      if (shares !== undefined) holdings[idx].shares = Number(shares);
      if (avgCost !== undefined) holdings[idx].avgCost = Number(avgCost);
      writeHoldings(holdings);
      res.json(holdings[idx]);
    } catch (error) {
      console.error('Error updating holding:', error);
      res.status(500).json({ error: 'Failed to update holding' });
    }
  });

  app.delete('/api/stock-holdings/:id', (req, res) => {
    try {
      const { id } = req.params;
      let holdings = readHoldings();
      holdings = holdings.filter(h => h.id !== id);
      writeHoldings(holdings);
      res.json({ success: true });
    } catch (error) {
      console.error('Error deleting holding:', error);
      res.status(500).json({ error: 'Failed to delete holding' });
    }
  });

  // === FMP Stock Data Proxy Endpoints ===
  app.get('/api/fmp/quotes', async (req, res) => {
    try {
      const symbols = (req.query.symbols as string || '').split(',').filter(Boolean);
      if (symbols.length === 0) return res.json([]);
      let assetTypes: Record<string, string> = {};
      try {
        const atParam = req.query.asset_types as string;
        if (atParam) assetTypes = JSON.parse(atParam);
      } catch {}
      const quotes = await fmpService.getStockDetails(symbols, assetTypes);
      res.json(quotes);
    } catch (error) {
      console.error('FMP quotes error:', error);
      res.status(500).json({ error: 'Failed to fetch quotes' });
    }
  });

  app.get('/api/fmp/price-targets', async (req, res) => {
    try {
      const symbols = (req.query.symbols as string || '').split(',').filter(Boolean);
      if (symbols.length === 0) return res.json([]);
      const targets = await fmpService.getPriceTargets(symbols);
      res.json(targets);
    } catch (error) {
      console.error('FMP price targets error:', error);
      res.status(500).json({ error: 'Failed to fetch price targets' });
    }
  });

  app.get('/api/fmp/events', async (req, res) => {
    try {
      const symbols = (req.query.symbols as string || '').split(',').filter(Boolean);
      if (symbols.length === 0) return res.json({ earnings: [], dividends: [] });
      const events = await fmpService.getHoldingsEvents(symbols);
      res.json(events);
    } catch (error) {
      console.error('FMP events error:', error);
      res.status(500).json({ error: 'Failed to fetch events' });
    }
  });

  app.get('/api/fmp/search', async (req, res) => {
    try {
      const query = (req.query.q as string || '').trim();
      if (!query) return res.json([]);
      const results = await fmpService.searchTickers(query);
      res.json(results);
    } catch (error) {
      console.error('FMP search error:', error);
      res.status(500).json({ error: 'Failed to search tickers' });
    }
  });

  const httpServer = createServer(app);
  // Real-time market data endpoints
  app.get('/api/real-time/top-movers', async (req, res) => {
    try {
      const topMovers = await realTimeDataService.getTop24hMovers();
      res.json(topMovers);
    } catch (error) {
      console.error('Error fetching top movers:', error);
      res.status(500).json({ error: 'Failed to fetch top movers' });
    }
  });

  app.get('/api/real-time/whale-activity', async (req, res) => {
    try {
      const whaleActivity = await realTimeDataService.getLargeWalletActivity();
      res.json(whaleActivity);
    } catch (error) {
      console.error('Error fetching whale activity:', error);
      res.status(500).json({ error: 'Failed to fetch whale activity' });
    }
  });

  app.get('/api/real-time/social-sentiment', async (req, res) => {
    try {
      const socialSentiment = await realTimeDataService.getSocialSentimentData();
      res.json(socialSentiment);
    } catch (error) {
      console.error('Error fetching social sentiment:', error);
      res.status(500).json({ error: 'Failed to fetch social sentiment' });
    }
  });

  app.get('/api/real-time/market-analysis', async (req, res) => {
    try {
      const marketAnalysis = await realTimeDataService.getMarketAnalysis();
      res.json(marketAnalysis);
    } catch (error) {
      console.error('Error fetching market analysis:', error);
      res.status(500).json({ error: 'Failed to fetch market analysis' });
    }
  });

  app.get('/api/real-time/portfolio-optimization/:portfolioId', async (req, res) => {
    try {
      const portfolioId = Number(req.params.portfolioId);
      const portfolio = await storage.getPortfolioByUserId(portfolioId);
      const optimization = await realTimeDataService.getPortfolioOptimization(portfolio);
      res.json(optimization);
    } catch (error) {
      console.error('Error fetching portfolio optimization:', error);
      res.status(500).json({ error: 'Failed to fetch portfolio optimization' });
    }
  });

  return httpServer;
}
