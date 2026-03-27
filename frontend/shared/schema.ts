import { pgTable, text, serial, integer, boolean, decimal, timestamp } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const users = pgTable("users", {
  id: serial("id").primaryKey(),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const portfolios = pgTable("portfolios", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  baseWalletAddress: text("base_wallet_address"),
  taoWalletAddress: text("tao_wallet_address"),
  totalBalance: decimal("total_balance", { precision: 18, scale: 8 }).notNull(),
  baseHoldings: decimal("base_holdings", { precision: 18, scale: 8 }).notNull(),
  taoHoldings: decimal("tao_holdings", { precision: 18, scale: 8 }).notNull(),
  pnl24h: decimal("pnl_24h", { precision: 18, scale: 8 }).notNull(),
  pnl7d: decimal("pnl_7d", { precision: 18, scale: 8 }).default("0.00"),
  pnl30d: decimal("pnl_30d", { precision: 18, scale: 8 }).default("0.00"),
  pnlYtd: decimal("pnl_ytd", { precision: 18, scale: 8 }).default("0.00"),
  pnlAll: decimal("pnl_all", { precision: 18, scale: 8 }).default("0.00"),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const portfolioValueHistory = pgTable("portfolio_value_history", {
  id: serial("id").primaryKey(),
  portfolioId: integer("portfolio_id").notNull(),
  totalValue: decimal("total_value", { precision: 18, scale: 8 }).notNull(),
  timestamp: timestamp("timestamp").defaultNow(),
});

export const holdings = pgTable("holdings", {
  id: serial("id").primaryKey(),
  portfolioId: integer("portfolio_id").notNull(),
  symbol: text("symbol").notNull(),
  network: text("network").notNull(), // 'BASE' or 'TAO'
  amount: decimal("amount", { precision: 18, scale: 8 }).notNull(),
  entryPrice: decimal("entry_price", { precision: 18, scale: 8 }).notNull(),
  currentPrice: decimal("current_price", { precision: 18, scale: 8 }).notNull(),
  pnl: decimal("pnl", { precision: 18, scale: 8 }).notNull(),
  pnlPercentage: decimal("pnl_percentage", { precision: 18, scale: 8 }).notNull(),
});

export const subnets = pgTable("subnets", {
  id: serial("id").primaryKey(),
  netuid: integer("netuid").notNull().unique(),
  name: text("name").notNull(),
  description: text("description"),
  stakeWeight: decimal("stake_weight", { precision: 18, scale: 8 }).notNull(),
  emissions: decimal("emissions", { precision: 18, scale: 8 }).notNull(),
  pnl24h: decimal("pnl_24h", { precision: 18, scale: 8 }).notNull(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export const whaleTransactions = pgTable("whale_transactions", {
  id: serial("id").primaryKey(),
  network: text("network").notNull(),
  transactionHash: text("transaction_hash").notNull(),
  fromAddress: text("from_address").notNull(),
  toAddress: text("to_address"),
  tokenAddress: text("token_address"), // Contract address for the token
  amount: decimal("amount", { precision: 18, scale: 8 }).notNull(),
  amountUsd: decimal("amount_usd", { precision: 18, scale: 8 }).notNull(),
  token: text("token").notNull(),
  action: text("action"), // BUY, SELL, STAKE
  timestamp: timestamp("timestamp").defaultNow(),
});

export const premiumAccess = pgTable("premium_access", {
  id: serial("id").primaryKey(),
  userId: integer("user_id").notNull(),
  feature: text("feature").notNull(), // 'whale_watching'
  expiresAt: timestamp("expires_at").notNull(),
  paymentAmount: decimal("payment_amount", { precision: 18, scale: 8 }).notNull(),
  paymentToken: text("payment_token").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export const marketInsights = pgTable("market_insights", {
  id: serial("id").primaryKey(),
  title: text("title").notNull(),
  content: text("content").notNull(),
  sentiment: text("sentiment").notNull(), // 'bullish', 'bearish', 'neutral'
  confidence: integer("confidence").notNull(), // 0-100
  source: text("source").notNull(),
  impact: text("impact").notNull(), // 'high', 'medium', 'low'
  publishedAt: timestamp("published_at").defaultNow(),
});

export const tradeSignals = pgTable("trade_signals", {
  id: serial("id").primaryKey(),
  type: text("type").notNull(), // 'buy', 'sell', 'hold'
  asset: text("asset").notNull(),
  description: text("description").notNull(),
  confidence: integer("confidence").notNull(),
  createdAt: timestamp("created_at").defaultNow(),
});

export const mindshareProjects = pgTable("mindshare_projects", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  symbol: text("symbol").notNull(),
  network: text("network").notNull(), // 'BASE' or 'TAO'
  marketCap: decimal("market_cap", { precision: 18, scale: 8 }),
  volume24h: decimal("volume_24h", { precision: 18, scale: 8 }),
  mentions24h: integer("mentions_24h").notNull(),
  sentiment: decimal("sentiment", { precision: 5, scale: 2 }).notNull(), // -100 to 100
  trendingScore: integer("trending_score").notNull(), // 1-100
  lastUpdated: timestamp("last_updated").defaultNow(),
});

// Insert schemas
export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export const insertPortfolioSchema = createInsertSchema(portfolios).omit({
  id: true,
  updatedAt: true,
});

export const insertHoldingSchema = createInsertSchema(holdings).omit({
  id: true,
});

export const insertSubnetSchema = createInsertSchema(subnets).omit({
  id: true,
  updatedAt: true,
});

export const insertWhaleTransactionSchema = createInsertSchema(whaleTransactions).omit({
  id: true,
  timestamp: true,
});

export const insertPremiumAccessSchema = createInsertSchema(premiumAccess).omit({
  id: true,
  createdAt: true,
});

export const insertMarketInsightSchema = createInsertSchema(marketInsights).omit({
  id: true,
  publishedAt: true,
});

export const insertTradeSignalSchema = createInsertSchema(tradeSignals).omit({
  id: true,
  createdAt: true,
});

export const insertMindshareProjectSchema = createInsertSchema(mindshareProjects).omit({
  id: true,
  lastUpdated: true,
});

export const insertPortfolioValueHistorySchema = createInsertSchema(portfolioValueHistory).omit({
  id: true,
  timestamp: true,
});

// Type exports
export type MindshareProject = typeof mindshareProjects.$inferSelect;
export type InsertMindshareProject = z.infer<typeof insertMindshareProjectSchema>;

export type PortfolioValueHistory = typeof portfolioValueHistory.$inferSelect;
export type InsertPortfolioValueHistory = z.infer<typeof insertPortfolioValueHistorySchema>;

// Types
export type User = typeof users.$inferSelect;
export type InsertUser = z.infer<typeof insertUserSchema>;

export type Portfolio = typeof portfolios.$inferSelect;
export type InsertPortfolio = z.infer<typeof insertPortfolioSchema>;

export type Holding = typeof holdings.$inferSelect;
export type InsertHolding = z.infer<typeof insertHoldingSchema>;

export type Subnet = typeof subnets.$inferSelect;
export type InsertSubnet = z.infer<typeof insertSubnetSchema>;

export type WhaleTransaction = typeof whaleTransactions.$inferSelect;
export type InsertWhaleTransaction = z.infer<typeof insertWhaleTransactionSchema>;

export type PremiumAccess = typeof premiumAccess.$inferSelect;
export type InsertPremiumAccess = z.infer<typeof insertPremiumAccessSchema>;

export type MarketInsight = typeof marketInsights.$inferSelect;
export type InsertMarketInsight = z.infer<typeof insertMarketInsightSchema>;

export type TradeSignal = typeof tradeSignals.$inferSelect;
export type InsertTradeSignal = z.infer<typeof insertTradeSignalSchema>;
