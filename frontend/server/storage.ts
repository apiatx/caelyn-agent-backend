import { 
  users, portfolios, holdings, subnets, whaleTransactions, premiumAccess, marketInsights, tradeSignals, mindshareProjects, portfolioValueHistory,
  type User, type InsertUser, type Portfolio, type InsertPortfolio, type Holding, type InsertHolding,
  type Subnet, type InsertSubnet, type WhaleTransaction, type InsertWhaleTransaction,
  type PremiumAccess, type InsertPremiumAccess, type MarketInsight, type InsertMarketInsight,
  type TradeSignal, type InsertTradeSignal, type MindshareProject, type InsertMindshareProject,
  type PortfolioValueHistory, type InsertPortfolioValueHistory
} from "@shared/schema";

export interface IStorage {
  // User methods
  getUser(id: number): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;

  // Portfolio methods
  getPortfolioByUserId(userId: number): Promise<Portfolio | undefined>;
  getPortfolioById(portfolioId: number): Promise<Portfolio | undefined>;
  getAllPortfolios(): Promise<Portfolio[]>;
  createPortfolio(portfolio: InsertPortfolio): Promise<Portfolio>;
  updatePortfolio(id: number, portfolio: Partial<InsertPortfolio>): Promise<Portfolio | undefined>;

  // Holdings methods
  getHoldingsByPortfolioId(portfolioId: number): Promise<Holding[]>;
  createHolding(holding: InsertHolding): Promise<Holding>;
  updateHolding(id: number, holding: Partial<InsertHolding>): Promise<Holding | undefined>;

  // Subnet methods
  getAllSubnets(): Promise<Subnet[]>;
  getSubnetByNetuid(netuid: number): Promise<Subnet | undefined>;
  createSubnet(subnet: InsertSubnet): Promise<Subnet>;
  updateSubnet(id: number, subnet: Partial<InsertSubnet>): Promise<Subnet | undefined>;

  // Whale transaction methods
  getWhaleTransactions(limit?: number): Promise<WhaleTransaction[]>;
  createWhaleTransaction(transaction: InsertWhaleTransaction): Promise<WhaleTransaction>;

  // Premium access methods
  getPremiumAccess(userId: number, feature: string): Promise<PremiumAccess | undefined>;
  createPremiumAccess(access: InsertPremiumAccess): Promise<PremiumAccess>;

  // Market research methods
  getMarketInsights(limit?: number): Promise<MarketInsight[]>;
  createMarketInsight(insight: InsertMarketInsight): Promise<MarketInsight>;

  getTradeSignals(limit?: number): Promise<TradeSignal[]>;
  createTradeSignal(signal: InsertTradeSignal): Promise<TradeSignal>;

  // Mindshare methods
  getMindshareProjects(): Promise<MindshareProject[]>;
  createMindshareProject(project: InsertMindshareProject): Promise<MindshareProject>;

  // Portfolio value history methods
  getPortfolioValueHistory(portfolioId: number, limit?: number): Promise<PortfolioValueHistory[]>;
  createPortfolioValueHistory(history: InsertPortfolioValueHistory): Promise<PortfolioValueHistory>;

  // Dashboard methods
  getDashboardData(): Promise<any>;
}

export class MemStorage implements IStorage {
  private users: Map<number, User>;
  private portfolios: Map<number, Portfolio>;
  private holdings: Map<number, Holding>;
  private subnets: Map<number, Subnet>;
  private whaleTransactions: Map<number, WhaleTransaction>;
  private premiumAccess: Map<number, PremiumAccess>;
  private marketInsights: Map<number, MarketInsight>;
  private tradeSignals: Map<number, TradeSignal>;
  private mindshareProjects: Map<number, MindshareProject>;
  private portfolioValueHistory: Map<number, PortfolioValueHistory>;
  
  private currentUserId: number;
  private currentPortfolioId: number;
  private currentHoldingId: number;
  private currentSubnetId: number;
  private currentWhaleTransactionId: number;
  private currentPremiumAccessId: number;
  private currentMarketInsightId: number;
  private currentTradeSignalId: number;
  private currentMindshareProjectId: number;
  private currentPortfolioValueHistoryId: number;

  constructor() {
    this.users = new Map();
    this.portfolios = new Map();
    this.holdings = new Map();
    this.subnets = new Map();
    this.whaleTransactions = new Map();
    this.premiumAccess = new Map();
    this.marketInsights = new Map();
    this.tradeSignals = new Map();
    this.mindshareProjects = new Map();
    this.portfolioValueHistory = new Map();
    
    this.currentUserId = 1;
    this.currentPortfolioId = 1;
    this.currentHoldingId = 1;
    this.currentSubnetId = 1;
    this.currentWhaleTransactionId = 1;
    this.currentPremiumAccessId = 1;
    this.currentMarketInsightId = 1;
    this.currentTradeSignalId = 1;
    this.currentMindshareProjectId = 1;
    this.currentPortfolioValueHistoryId = 1;

    this.seedData();
  }

  private seedData() {
    // Create demo user and portfolio
    const user: User = { id: 1, username: "demo", password: "demo" };
    this.users.set(1, user);

    const portfolio: Portfolio = {
      id: 1,
      userId: 1,
      baseWalletAddress: null,
      taoWalletAddress: null,
      totalBalance: "0.00",
      baseHoldings: "0.00",
      taoHoldings: "0.00",
      pnl24h: "0.00",
      pnl7d: "0.00",
      pnl30d: "0.00",
      pnlYtd: "0.00",
      pnlAll: "0.00",
      updatedAt: new Date(),
    };
    this.portfolios.set(1, portfolio);

    // No seed holdings - only real data
    this.currentHoldingId = 1;

    // Seed subnets
    const subnetsData: Subnet[] = [
      {
        id: 1,
        netuid: 1,
        name: "Prompting",
        description: "Text generation & prompting",
        stakeWeight: "2847.3",
        emissions: "127.8",
        pnl24h: "2.34",
        updatedAt: new Date(),
      },
      {
        id: 2,
        netuid: 18,
        name: "Cortex.t",
        description: "Decentralized inference",
        stakeWeight: "1923.7",
        emissions: "89.4",
        pnl24h: "5.67",
        updatedAt: new Date(),
      },
      {
        id: 3,
        netuid: 27,
        name: "Compute",
        description: "Distributed computing",
        stakeWeight: "3421.8",
        emissions: "156.2",
        pnl24h: "3.12",
        updatedAt: new Date(),
      }
    ];

    subnetsData.forEach(subnet => this.subnets.set(subnet.id, subnet));
    this.currentSubnetId = 4;

    // Seed market insights
    const insights: MarketInsight[] = [
      {
        id: 1,
        title: "Coinbase Announces BASE Layer 2 Expansion",
        content: "Major institutional adoption expected as Coinbase scales BASE infrastructure for enterprise clients...",
        sentiment: "bullish",
        confidence: 87,
        source: "Coinbase Blog",
        impact: "high",
        publishedAt: new Date(Date.now() - 3600000), // 1 hour ago
      },
      {
        id: 2,
        title: "Bittensor Subnet 27 Reaches New ATH",
        content: "Compute subnet shows unprecedented growth with 245% increase in validator participation...",
        sentiment: "bullish",
        confidence: 94,
        source: "Taostats Analytics",
        impact: "high",
        publishedAt: new Date(Date.now() - 10800000), // 3 hours ago
      }
    ];

    insights.forEach(insight => this.marketInsights.set(insight.id, insight));
    this.currentMarketInsightId = 3;

    // Seed trade signals
    const signals: TradeSignal[] = [
      {
        id: 1,
        type: "buy",
        asset: "ETH/BASE",
        description: "ETH/BASE liquidity spike detected",
        confidence: 85,
        createdAt: new Date(Date.now() - 120000), // 2 minutes ago
      },
      {
        id: 2,
        type: "hold",
        asset: "TAO",
        description: "TAO subnet emissions increasing",
        confidence: 78,
        createdAt: new Date(Date.now() - 900000), // 15 minutes ago
      }
    ];

    signals.forEach(signal => this.tradeSignals.set(signal.id, signal));
    this.currentTradeSignalId = 3;

    // Seed whale transactions
    const whaleTransactions: WhaleTransaction[] = [
      {
        id: 1,
        network: "BASE",
        transactionHash: "0x742d35cc6bf9f2e7c2e4a123456789abcdef0123456789abcdef0123456789ab",
        fromAddress: "0x742d35cc6bf9f2e7c2e4a123456789abcdef012345",
        toAddress: "0x987654321fedcba9876543210fedcba9876543210",
        tokenAddress: "0x4200000000000000000000000000000000000006",
        amount: "20.5",
        amountUsd: "47823.45",
        token: "ETH",
        action: "BUY",
        timestamp: new Date(Date.now() - 120000), // 2 minutes ago
      },
      {
        id: 2,
        network: "TAO",
        transactionHash: "0x89abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        fromAddress: "5GrwvaEF5zXb26FZ96yf4vEL8uZAQ8kGF2EZhCz1t4MNqp8H",
        toAddress: "SN15: Blockchain Insights",
        tokenAddress: null, // TAO doesn't have contract addresses
        amount: "161.2",
        amountUsd: "89156.78",
        token: "TAO",
        action: "STAKE",
        timestamp: new Date(Date.now() - 480000), // 8 minutes ago
      },
      {
        id: 3,
        network: "BASE",
        transactionHash: "0x456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345",
        fromAddress: "0x123456789abcdef0123456789abcdef0123456789a",
        toAddress: "0xabcdef0123456789abcdef0123456789abcdef0123",
        tokenAddress: "0x4200000000000000000000000000000000000006",
        amount: "8.7",
        amountUsd: "20234.67",
        token: "ETH",
        action: "SELL",
        timestamp: new Date(Date.now() - 900000), // 15 minutes ago
      }
    ];

    whaleTransactions.forEach(tx => this.whaleTransactions.set(tx.id, tx));
    this.currentWhaleTransactionId = 4;

    // Seed comprehensive mindshare projects with BASE altcoins/memecoins and TAO subnets
    const mindshareProjects: MindshareProject[] = [
      // BASE Network Altcoins & Memecoins (comprehensive tracking)
      {
        id: 1,
        name: "Ski Mask Dog",
        symbol: "SKI",
        network: "BASE",
        marketCap: "125000000",
        volume24h: "18500000",
        mentions24h: 2847,
        sentiment: "84.2",
        trendingScore: 92,
        lastUpdated: new Date(),
      },
      {
        id: 2,
        name: "Keycat",
        symbol: "KEYCAT",
        network: "BASE",
        marketCap: "87000000",
        volume24h: "12400000",
        mentions24h: 1923,
        sentiment: "79.6",
        trendingScore: 88,
        lastUpdated: new Date(),
      },
      {
        id: 3,
        name: "Tig",
        symbol: "TIG",
        network: "BASE",
        marketCap: "95000000",
        volume24h: "15200000",
        mentions24h: 1654,
        sentiment: "82.1",
        trendingScore: 85,
        lastUpdated: new Date(),
      },
      {
        id: 4,
        name: "Giza",
        symbol: "GIZA",
        network: "BASE",
        marketCap: "67000000",
        volume24h: "9800000",
        mentions24h: 1287,
        sentiment: "76.4",
        trendingScore: 81,
        lastUpdated: new Date(),
      },
      {
        id: 5,
        name: "Virtual Protocol",
        symbol: "VIRTUAL",
        network: "BASE",
        marketCap: "540000000",
        volume24h: "45000000",
        mentions24h: 3521,
        sentiment: "89.7",
        trendingScore: 95,
        lastUpdated: new Date(),
      },
      {
        id: 6,
        name: "Higher",
        symbol: "HIGHER",
        network: "BASE",
        marketCap: "78000000",
        volume24h: "11200000",
        mentions24h: 1445,
        sentiment: "83.5",
        trendingScore: 87,
        lastUpdated: new Date(),
      },
      {
        id: 7,
        name: "Mfer",
        symbol: "MFER",
        network: "BASE",
        marketCap: "134000000",
        volume24h: "22100000",
        mentions24h: 2198,
        sentiment: "81.9",
        trendingScore: 89,
        lastUpdated: new Date(),
      },
      {
        id: 8,
        name: "Toshi",
        symbol: "TOSHI",
        network: "BASE",
        marketCap: "156000000",
        volume24h: "28700000",
        mentions24h: 2654,
        sentiment: "85.3",
        trendingScore: 91,
        lastUpdated: new Date(),
      },
      {
        id: 9,
        name: "Aerodrome Finance",
        symbol: "AERO",
        network: "BASE",
        marketCap: "890000000",
        volume24h: "125000000",
        mentions24h: 1876,
        sentiment: "88.4",
        trendingScore: 94,
        lastUpdated: new Date(),
      },
      {
        id: 10,
        name: "Degen",
        symbol: "DEGEN",
        network: "BASE",
        marketCap: "234000000",
        volume24h: "35600000",
        mentions24h: 3987,
        sentiment: "86.7",
        trendingScore: 93,
        lastUpdated: new Date(),
      },
      // TAO Subnet-specific mindshare tracking
      {
        id: 11,
        name: "SN1 - Prompting",
        symbol: "SN1",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 1834,
        sentiment: "91.2",
        trendingScore: 89,
        lastUpdated: new Date(),
      },
      {
        id: 12,
        name: "SN5 - Open Kaito",
        symbol: "SN5",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 892,
        sentiment: "78.9",
        trendingScore: 76,
        lastUpdated: new Date(),
      },
      {
        id: 13,
        name: "SN18 - Cortex.t",
        symbol: "SN18",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 1247,
        sentiment: "87.4",
        trendingScore: 84,
        lastUpdated: new Date(),
      },
      {
        id: 14,
        name: "SN27 - Compute Horde", 
        symbol: "SN27",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 1654,
        sentiment: "85.6",
        trendingScore: 88,
        lastUpdated: new Date(),
      },
      {
        id: 15,
        name: "SN32 - It's AI",
        symbol: "SN32",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 743,
        sentiment: "82.1",
        trendingScore: 79,
        lastUpdated: new Date(),
      },
      {
        id: 16,
        name: "SN51 - Bittensor Subnet",
        symbol: "SN51",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 567,
        sentiment: "79.8",
        trendingScore: 75,
        lastUpdated: new Date(),
      },
      {
        id: 17,
        name: "SN64 - Chutes",
        symbol: "SN64",
        network: "TAO",
        marketCap: null,
        volume24h: null,
        mentions24h: 445,
        sentiment: "76.3",
        trendingScore: 72,
        lastUpdated: new Date(),
      },
      
      // COMPREHENSIVE BASE ECOSYSTEM EXPANSION - Additional BASE tokens
      { id: 18, name: "Brett", symbol: "BRETT", network: "BASE", marketCap: "234000000", volume24h: "28900000", mentions24h: 3124, sentiment: "86.5", trendingScore: 93, lastUpdated: new Date() },
      { id: 19, name: "Normie", symbol: "NORMIE", network: "BASE", marketCap: "56000000", volume24h: "8200000", mentions24h: 1523, sentiment: "78.3", trendingScore: 84, lastUpdated: new Date() },
      { id: 20, name: "Based Dog", symbol: "BASEDOG", network: "BASE", marketCap: "23000000", volume24h: "3400000", mentions24h: 891, sentiment: "72.1", trendingScore: 76, lastUpdated: new Date() },
      { id: 21, name: "Based", symbol: "BASED", network: "BASE", marketCap: "45000000", volume24h: "6700000", mentions24h: 1234, sentiment: "80.4", trendingScore: 82, lastUpdated: new Date() },
      { id: 22, name: "Based AI", symbol: "BASEDAI", network: "BASE", marketCap: "78000000", volume24h: "11500000", mentions24h: 1876, sentiment: "83.7", trendingScore: 87, lastUpdated: new Date() },
      { id: 23, name: "Onchain", symbol: "ONCHAIN", network: "BASE", marketCap: "34000000", volume24h: "5100000", mentions24h: 743, sentiment: "75.8", trendingScore: 79, lastUpdated: new Date() },
      { id: 24, name: "Benji", symbol: "BENJI", network: "BASE", marketCap: "12000000", volume24h: "1800000", mentions24h: 456, sentiment: "69.2", trendingScore: 71, lastUpdated: new Date() },
      { id: 25, name: "Pump", symbol: "PUMP", network: "BASE", marketCap: "67000000", volume24h: "9800000", mentions24h: 1345, sentiment: "77.9", trendingScore: 81, lastUpdated: new Date() },
      { id: 26, name: "Moonwell", symbol: "MOONWELL", network: "BASE", marketCap: "89000000", volume24h: "13200000", mentions24h: 1678, sentiment: "82.3", trendingScore: 85, lastUpdated: new Date() },
      { id: 27, name: "Seamless", symbol: "SEAMLESS", network: "BASE", marketCap: "56000000", volume24h: "8300000", mentions24h: 1123, sentiment: "79.6", trendingScore: 83, lastUpdated: new Date() },
      { id: 28, name: "Blue", symbol: "BLUE", network: "BASE", marketCap: "23000000", volume24h: "3400000", mentions24h: 789, sentiment: "74.2", trendingScore: 77, lastUpdated: new Date() },
      { id: 29, name: "Bald", symbol: "BALD", network: "BASE", marketCap: "34000000", volume24h: "5100000", mentions24h: 923, sentiment: "76.8", trendingScore: 79, lastUpdated: new Date() },
      { id: 30, name: "Tab", symbol: "TAB", network: "BASE", marketCap: "12000000", volume24h: "1800000", mentions24h: 456, sentiment: "71.5", trendingScore: 73, lastUpdated: new Date() },
      { id: 31, name: "Ducky", symbol: "DUCKY", network: "BASE", marketCap: "8900000", volume24h: "1300000", mentions24h: 334, sentiment: "68.9", trendingScore: 70, lastUpdated: new Date() },
      { id: 32, name: "Jesus Coin", symbol: "JESUS", network: "BASE", marketCap: "45000000", volume24h: "6700000", mentions24h: 1234, sentiment: "80.1", trendingScore: 82, lastUpdated: new Date() },
      { id: 33, name: "USA Coin", symbol: "USA", network: "BASE", marketCap: "23000000", volume24h: "3400000", mentions24h: 667, sentiment: "75.3", trendingScore: 78, lastUpdated: new Date() },
      { id: 34, name: "Mochi", symbol: "MOCHI", network: "BASE", marketCap: "12000000", volume24h: "1800000", mentions24h: 445, sentiment: "72.7", trendingScore: 74, lastUpdated: new Date() },
      { id: 35, name: "Wojak", symbol: "WOJAK", network: "BASE", marketCap: "34000000", volume24h: "5100000", mentions24h: 891, sentiment: "77.2", trendingScore: 80, lastUpdated: new Date() },
      // Additional authentic BASE network tokens
      { id: 36, name: "AlienBase", symbol: "ALB", network: "BASE", marketCap: "45000000", volume24h: "6700000", mentions24h: 1234, sentiment: "78.4", trendingScore: 81, lastUpdated: new Date() },
      { id: 37, name: "BaseSwap", symbol: "BSX", network: "BASE", marketCap: "67000000", volume24h: "9800000", mentions24h: 1456, sentiment: "80.7", trendingScore: 84, lastUpdated: new Date() },
      { id: 38, name: "Compound", symbol: "COMP", network: "BASE", marketCap: "123000000", volume24h: "18200000", mentions24h: 1876, sentiment: "85.2", trendingScore: 87, lastUpdated: new Date() },
      { id: 39, name: "Curve", symbol: "CRV", network: "BASE", marketCap: "89000000", volume24h: "13200000", mentions24h: 1543, sentiment: "82.9", trendingScore: 85, lastUpdated: new Date() },
      { id: 40, name: "Uniswap", symbol: "UNI", network: "BASE", marketCap: "234000000", volume24h: "34500000", mentions24h: 2345, sentiment: "87.1", trendingScore: 90, lastUpdated: new Date() },
      { id: 41, name: "Balancer", symbol: "BAL", network: "BASE", marketCap: "56000000", volume24h: "8300000", mentions24h: 1123, sentiment: "79.8", trendingScore: 82, lastUpdated: new Date() },
      { id: 42, name: "Synapse", symbol: "SYN", network: "BASE", marketCap: "78000000", volume24h: "11500000", mentions24h: 1345, sentiment: "83.6", trendingScore: 86, lastUpdated: new Date() },
      { id: 43, name: "Wrapped Bitcoin", symbol: "WBTC", network: "BASE", marketCap: "456000000", volume24h: "67800000", mentions24h: 2876, sentiment: "88.3", trendingScore: 92, lastUpdated: new Date() },
      { id: 44, name: "Chainlink", symbol: "LINK", network: "BASE", marketCap: "189000000", volume24h: "27600000", mentions24h: 2234, sentiment: "86.7", trendingScore: 89, lastUpdated: new Date() },
      { id: 45, name: "Optimism", symbol: "OP", network: "BASE", marketCap: "345000000", volume24h: "51200000", mentions24h: 2987, sentiment: "84.5", trendingScore: 88, lastUpdated: new Date() },
      
      // COMPREHENSIVE TAO SUBNET EXPANSION - All remaining subnets
      { id: 50, name: "SN2 - Machine Translation", symbol: "SN2", network: "TAO", marketCap: null, volume24h: null, mentions24h: 756, sentiment: "87.4", trendingScore: 82, lastUpdated: new Date() },
      { id: 51, name: "SN3 - Data Scraping", symbol: "SN3", network: "TAO", marketCap: null, volume24h: null, mentions24h: 623, sentiment: "84.1", trendingScore: 78, lastUpdated: new Date() },
      { id: 52, name: "SN4 - Multi Modality", symbol: "SN4", network: "TAO", marketCap: null, volume24h: null, mentions24h: 945, sentiment: "88.7", trendingScore: 85, lastUpdated: new Date() },
      { id: 53, name: "SN6 - Compute", symbol: "SN6", network: "TAO", marketCap: null, volume24h: null, mentions24h: 834, sentiment: "86.2", trendingScore: 81, lastUpdated: new Date() },
      { id: 54, name: "SN7 - Storage", symbol: "SN7", network: "TAO", marketCap: null, volume24h: null, mentions24h: 712, sentiment: "85.8", trendingScore: 80, lastUpdated: new Date() },
      { id: 55, name: "SN8 - Time Series Prediction", symbol: "SN8", network: "TAO", marketCap: null, volume24h: null, mentions24h: 589, sentiment: "83.4", trendingScore: 77, lastUpdated: new Date() },
      { id: 56, name: "SN9 - Pre-training", symbol: "SN9", network: "TAO", marketCap: null, volume24h: null, mentions24h: 667, sentiment: "84.7", trendingScore: 79, lastUpdated: new Date() },
      { id: 57, name: "SN10 - Map Reduce", symbol: "SN10", network: "TAO", marketCap: null, volume24h: null, mentions24h: 534, sentiment: "82.1", trendingScore: 75, lastUpdated: new Date() },
      { id: 58, name: "SN11 - Text Prompting", symbol: "SN11", network: "TAO", marketCap: null, volume24h: null, mentions24h: 1456, sentiment: "90.5", trendingScore: 88, lastUpdated: new Date() },
      { id: 59, name: "SN12 - Compute", symbol: "SN12", network: "TAO", marketCap: null, volume24h: null, mentions24h: 789, sentiment: "86.9", trendingScore: 83, lastUpdated: new Date() },
      { id: 60, name: "SN13 - Data Universe", symbol: "SN13", network: "TAO", marketCap: null, volume24h: null, mentions24h: 623, sentiment: "84.3", trendingScore: 78, lastUpdated: new Date() },
      { id: 61, name: "SN14 - LLM Defender", symbol: "SN14", network: "TAO", marketCap: null, volume24h: null, mentions24h: 891, sentiment: "87.6", trendingScore: 84, lastUpdated: new Date() },
      { id: 62, name: "SN15 - Blockchain Insights", symbol: "SN15", network: "TAO", marketCap: null, volume24h: null, mentions24h: 712, sentiment: "85.2", trendingScore: 81, lastUpdated: new Date() },
      { id: 63, name: "SN16 - Audio", symbol: "SN16", network: "TAO", marketCap: null, volume24h: null, mentions24h: 567, sentiment: "83.8", trendingScore: 77, lastUpdated: new Date() },
      { id: 64, name: "SN17 - Three Gen", symbol: "SN17", network: "TAO", marketCap: null, volume24h: null, mentions24h: 445, sentiment: "81.4", trendingScore: 74, lastUpdated: new Date() },
      { id: 65, name: "SN19 - Vision", symbol: "SN19", network: "TAO", marketCap: null, volume24h: null, mentions24h: 834, sentiment: "86.1", trendingScore: 82, lastUpdated: new Date() },
      { id: 66, name: "SN20 - Bitagent", symbol: "SN20", network: "TAO", marketCap: null, volume24h: null, mentions24h: 623, sentiment: "84.7", trendingScore: 79, lastUpdated: new Date() },
      { id: 67, name: "SN21 - FileTao", symbol: "SN21", network: "TAO", marketCap: null, volume24h: null, mentions24h: 567, sentiment: "83.2", trendingScore: 76, lastUpdated: new Date() },
      { id: 68, name: "SN22 - Mining", symbol: "SN22", network: "TAO", marketCap: null, volume24h: null, mentions24h: 789, sentiment: "85.9", trendingScore: 81, lastUpdated: new Date() },
      { id: 69, name: "SN23 - NicheImage", symbol: "SN23", network: "TAO", marketCap: null, volume24h: null, mentions24h: 445, sentiment: "82.6", trendingScore: 75, lastUpdated: new Date() },
      { id: 70, name: "SN24 - Omega Labs", symbol: "SN24", network: "TAO", marketCap: null, volume24h: null, mentions24h: 678, sentiment: "84.8", trendingScore: 80, lastUpdated: new Date() },
      { id: 71, name: "SN25 - Tensor", symbol: "SN25", network: "TAO", marketCap: null, volume24h: null, mentions24h: 891, sentiment: "87.3", trendingScore: 83, lastUpdated: new Date() },
      { id: 72, name: "SN26 - Sturdy", symbol: "SN26", network: "TAO", marketCap: null, volume24h: null, mentions24h: 534, sentiment: "83.7", trendingScore: 77, lastUpdated: new Date() },
      { id: 73, name: "SN28 - Foundry S&P", symbol: "SN28", network: "TAO", marketCap: null, volume24h: null, mentions24h: 712, sentiment: "86.4", trendingScore: 82, lastUpdated: new Date() },
      { id: 74, name: "SN29 - Fractal", symbol: "SN29", network: "TAO", marketCap: null, volume24h: null, mentions24h: 623, sentiment: "84.9", trendingScore: 79, lastUpdated: new Date() },
      { id: 75, name: "SN30 - Eden", symbol: "SN30", network: "TAO", marketCap: null, volume24h: null, mentions24h: 567, sentiment: "83.5", trendingScore: 76, lastUpdated: new Date() },
      { id: 76, name: "SN31 - Wombo", symbol: "SN31", network: "TAO", marketCap: null, volume24h: null, mentions24h: 445, sentiment: "82.1", trendingScore: 74, lastUpdated: new Date() },
      { id: 77, name: "SN106 - VOID AI", symbol: "SN106", network: "TAO", marketCap: null, volume24h: null, mentions24h: 1123, sentiment: "88.9", trendingScore: 86, lastUpdated: new Date() }
    ];

    mindshareProjects.forEach(project => this.mindshareProjects.set(project.id, project));
    this.currentMindshareProjectId = 78;
  }

  // User methods
  async getUser(id: number): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(user => user.username === username);
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const id = this.currentUserId++;
    const user: User = { ...insertUser, id };
    this.users.set(id, user);
    return user;
  }

  // Portfolio methods
  async getPortfolioByUserId(userId: number): Promise<Portfolio | undefined> {
    return Array.from(this.portfolios.values()).find(portfolio => portfolio.userId === userId);
  }

  async getPortfolioById(portfolioId: number): Promise<Portfolio | undefined> {
    return this.portfolios.get(portfolioId);
  }

  async getAllPortfolios(): Promise<Portfolio[]> {
    return Array.from(this.portfolios.values());
  }

  async createPortfolio(insertPortfolio: InsertPortfolio): Promise<Portfolio> {
    const id = this.currentPortfolioId++;
    const portfolio: Portfolio = { 
      ...insertPortfolio, 
      id, 
      updatedAt: new Date(),
      baseWalletAddress: insertPortfolio.baseWalletAddress ?? null,
      taoWalletAddress: insertPortfolio.taoWalletAddress ?? null,
      pnl7d: insertPortfolio.pnl7d ?? "0.00",
      pnl30d: insertPortfolio.pnl30d ?? "0.00",
      pnlYtd: insertPortfolio.pnlYtd ?? "0.00",
      pnlAll: insertPortfolio.pnlAll ?? "0.00"
    };
    this.portfolios.set(id, portfolio);
    return portfolio;
  }

  async updatePortfolio(id: number, updates: Partial<InsertPortfolio>): Promise<Portfolio | undefined> {
    const portfolio = this.portfolios.get(id);
    if (!portfolio) return undefined;
    
    const updated: Portfolio = { 
      ...portfolio, 
      ...updates, 
      updatedAt: new Date(),
      pnl7d: updates.pnl7d ?? portfolio.pnl7d,
      pnl30d: updates.pnl30d ?? portfolio.pnl30d,
      pnlYtd: updates.pnlYtd ?? portfolio.pnlYtd,
      pnlAll: updates.pnlAll ?? portfolio.pnlAll
    };
    this.portfolios.set(id, updated);
    return updated;
  }

  // Holdings methods
  async getHoldingsByPortfolioId(portfolioId: number): Promise<Holding[]> {
    return Array.from(this.holdings.values()).filter(holding => holding.portfolioId === portfolioId);
  }

  async createHolding(insertHolding: InsertHolding): Promise<Holding> {
    const id = this.currentHoldingId++;
    const holding: Holding = { ...insertHolding, id };
    this.holdings.set(id, holding);
    return holding;
  }

  async updateHolding(id: number, updates: Partial<InsertHolding>): Promise<Holding | undefined> {
    const holding = this.holdings.get(id);
    if (!holding) return undefined;
    
    const updated: Holding = { ...holding, ...updates };
    this.holdings.set(id, updated);
    return updated;
  }

  // Subnet methods
  async getAllSubnets(): Promise<Subnet[]> {
    return Array.from(this.subnets.values());
  }

  async getSubnetByNetuid(netuid: number): Promise<Subnet | undefined> {
    return Array.from(this.subnets.values()).find(subnet => subnet.netuid === netuid);
  }

  async createSubnet(insertSubnet: InsertSubnet): Promise<Subnet> {
    const id = this.currentSubnetId++;
    const subnet: Subnet = { ...insertSubnet, id, updatedAt: new Date(), description: insertSubnet.description || null };
    this.subnets.set(id, subnet);
    return subnet;
  }

  async updateSubnet(id: number, updates: Partial<InsertSubnet>): Promise<Subnet | undefined> {
    const subnet = this.subnets.get(id);
    if (!subnet) return undefined;
    
    const updated: Subnet = { ...subnet, ...updates, updatedAt: new Date() };
    this.subnets.set(id, updated);
    return updated;
  }

  // Whale transaction methods
  async getWhaleTransactions(limit: number = 50): Promise<WhaleTransaction[]> {
    return Array.from(this.whaleTransactions.values())
      .sort((a, b) => (b.timestamp?.getTime() || 0) - (a.timestamp?.getTime() || 0))
      .slice(0, limit);
  }

  async createWhaleTransaction(insertTransaction: InsertWhaleTransaction): Promise<WhaleTransaction> {
    const id = this.currentWhaleTransactionId++;
    const transaction: WhaleTransaction = { ...insertTransaction, id, timestamp: new Date(), toAddress: insertTransaction.toAddress || null };
    this.whaleTransactions.set(id, transaction);
    return transaction;
  }

  // Premium access methods
  async getPremiumAccess(userId: number, feature: string): Promise<PremiumAccess | undefined> {
    return Array.from(this.premiumAccess.values())
      .find(access => access.userId === userId && access.feature === feature && access.expiresAt > new Date());
  }

  async createPremiumAccess(insertAccess: InsertPremiumAccess): Promise<PremiumAccess> {
    const id = this.currentPremiumAccessId++;
    const access: PremiumAccess = { ...insertAccess, id, createdAt: new Date() };
    this.premiumAccess.set(id, access);
    return access;
  }

  // Market research methods
  async getMarketInsights(limit: number = 20): Promise<MarketInsight[]> {
    return Array.from(this.marketInsights.values())
      .sort((a, b) => (b.publishedAt?.getTime() || 0) - (a.publishedAt?.getTime() || 0))
      .slice(0, limit);
  }

  async createMarketInsight(insertInsight: InsertMarketInsight): Promise<MarketInsight> {
    const id = this.currentMarketInsightId++;
    const insight: MarketInsight = { ...insertInsight, id, publishedAt: new Date() };
    this.marketInsights.set(id, insight);
    return insight;
  }

  async getTradeSignals(limit: number = 10): Promise<TradeSignal[]> {
    return Array.from(this.tradeSignals.values())
      .sort((a, b) => (b.createdAt?.getTime() || 0) - (a.createdAt?.getTime() || 0))
      .slice(0, limit);
  }

  async createTradeSignal(insertSignal: InsertTradeSignal): Promise<TradeSignal> {
    const id = this.currentTradeSignalId++;
    const signal: TradeSignal = { ...insertSignal, id, createdAt: new Date() };
    this.tradeSignals.set(id, signal);
    return signal;
  }

  // Mindshare methods
  async getMindshareProjects(): Promise<MindshareProject[]> {
    return Array.from(this.mindshareProjects.values())
      .sort((a, b) => b.trendingScore - a.trendingScore);
  }

  async createMindshareProject(insertProject: InsertMindshareProject): Promise<MindshareProject> {
    const id = this.currentMindshareProjectId++;
    const project: MindshareProject = { 
      ...insertProject, 
      id, 
      lastUpdated: new Date(),
      marketCap: insertProject.marketCap ?? null,
      volume24h: insertProject.volume24h ?? null
    };
    this.mindshareProjects.set(id, project);
    return project;
  }

  // Portfolio value history methods
  async getPortfolioValueHistory(portfolioId: number, limit?: number): Promise<PortfolioValueHistory[]> {
    const history = Array.from(this.portfolioValueHistory.values())
      .filter(h => h.portfolioId === portfolioId)
      .sort((a, b) => new Date(b.timestamp!).getTime() - new Date(a.timestamp!).getTime());
    
    return limit ? history.slice(0, limit) : history;
  }

  async createPortfolioValueHistory(history: InsertPortfolioValueHistory, customTimestamp?: Date): Promise<PortfolioValueHistory> {
    const portfolioHistory: PortfolioValueHistory = {
      id: this.currentPortfolioValueHistoryId++,
      ...history,
      timestamp: customTimestamp || new Date(),
    };
    this.portfolioValueHistory.set(portfolioHistory.id, portfolioHistory);
    return portfolioHistory;
  }

  // Dashboard data method
  async getDashboardData(): Promise<any> {
    const portfolio = this.portfolios.get(1); // Get primary portfolio
    const recentTransactions = Array.from(this.whaleTransactions.values())
      .filter(tx => tx.network === 'BASE' || tx.network === 'TAO')
      .sort((a, b) => new Date(b.timestamp!).getTime() - new Date(a.timestamp!).getTime())
      .slice(0, 5);

    return {
      portfolioValue: portfolio?.totalBalance || 0,
      portfolioPnL: portfolio?.pnl24h || 0,
      portfolioPnLPercent: 0, // Calculate based on current vs previous value
      baseTopMovers: [
        { token: 'SKI', name: 'Ski Mask Dog', price: 0.156, change24h: 15.2, volume24h: 2500000 },
        { token: 'VIRTUAL', name: 'Virtual Protocol', price: 12.45, change24h: 12.8, volume24h: 1800000 },
        { token: 'AI16Z', name: 'ai16z', price: 0.789, change24h: 9.4, volume24h: 1200000 },
        { token: 'BRETT', name: 'Brett', price: 0.089, change24h: 8.7, volume24h: 950000 },
        { token: 'HIGHER', name: 'Higher', price: 0.67, change24h: 7.1, volume24h: 800000 }
      ],
      taoSubnetMovers: [
        { subnet: 'SN27', name: 'Compute Horde', emissions: 125.4, change24h: 18.5, stakeWeight: 0.085 },
        { subnet: 'SN1', name: 'Prompting', emissions: 89.2, change24h: 14.2, stakeWeight: 0.092 },
        { subnet: 'SN20', name: 'Bitagent', emissions: 67.8, change24h: 11.7, stakeWeight: 0.075 },
        { subnet: 'SN5', name: 'Image Generation', emissions: 78.3, change24h: 9.8, stakeWeight: 0.068 },
        { subnet: 'SN15', name: 'Blockchain Insights', emissions: 45.6, change24h: 8.4, stakeWeight: 0.061 }
      ],
      largeWalletActivity: recentTransactions.map(tx => ({
        type: tx.network,
        fromToken: tx.network === 'BASE' ? 'ETH' : 'TAO',
        toToken: tx.token,
        amount: parseFloat(tx.amount),
        amountUsd: tx.amountUsd,
        wallet: tx.fromAddress,
        timestamp: tx.timestamp
      })),
      socialPulse: [
        { type: 'BASE', token: '$SKI', name: 'Ski Mask Dog', mentions: 2847, sentiment: 'positive', trending: true },
        { type: 'TAO', token: 'SN27', name: 'Compute Horde', mentions: 1923, sentiment: 'positive', trending: true },
        { type: 'BASE', token: '$VIRTUAL', name: 'Virtual Protocol', mentions: 1567, sentiment: 'positive', trending: false },
        { type: 'BASE', token: '$AI16Z', name: 'ai16z', mentions: 1234, sentiment: 'neutral', trending: false },
        { type: 'TAO', token: 'SN1', name: 'Prompting', mentions: 987, sentiment: 'positive', trending: false }
      ]
    };
  }
}

export const storage = new MemStorage();
