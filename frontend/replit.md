# CryptoHippo - Cryptocurrency Portfolio Management Platform

## Overview
CryptoHippo is a web application for comprehensive cryptocurrency portfolio management, focusing on BASE network and Bittensor (TAO) assets. It offers real-time portfolio tracking, whale transaction monitoring, and in-depth market research insights. The platform aims to provide a streamlined, secure, and data-rich environment for crypto investors, combining real-time market data with advanced analytics and access to essential Web3 tools. Its business vision is to become a leading hub for sophisticated crypto analysis and portfolio management across multiple blockchain ecosystems.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture
The application employs a full-stack monorepo architecture, ensuring clear separation and efficient development.

### Frontend Architecture
- **Framework**: React 18 with TypeScript
- **Styling**: Tailwind CSS with a custom glassmorphism design system utilizing shadcn/ui and Radix UI.
- **State Management**: TanStack Query for efficient server state management.
- **Routing**: Wouter for lightweight client-side routing.
- **Build Tool**: Vite.
- **UI/UX Decisions**: Emphasizes a dark theme, glass card UI, consistent branding, streamlined navigation with tabbed interfaces and icon-based cues.

### Backend Architecture
- **Runtime**: Node.js with Express.js.
- **Language**: TypeScript with ES modules.
- **API Design**: RESTful API endpoints under the `/api` prefix.
- **Database**: PostgreSQL with Drizzle ORM for type-safe operations.
- **Session Management**: Prepared for express-session with PostgreSQL store for authentication.

### Data Storage Solutions
- **Primary Database**: PostgreSQL hosted on Neon (serverless PostgreSQL).
- **ORM**: Drizzle ORM, following a schema-first approach.
- **Migrations**: Managed via drizzle-kit.
- **Connection**: Uses @neondatabase/serverless.

### Key Components
- **Database Schema**: Comprehensive schema including Users, Portfolios, Holdings, Subnets, Whale Transactions, Premium Access, Market Insights, and Trade Signals tables.
- **Frontend Components**: Dashboard, dedicated sections for Portfolio, Whale Watching, Market Research, and specialized network analytics (BASE, Bittensor, Solana, Abstract). Features custom glass card UI and dynamic chart displays.
- **API Endpoints**: Dedicated endpoints for portfolio management, holdings tracking, subnet information, and premium features.

### System Design Choices
- **Data Flow**: RESTful API communication with JSON. Type-safe database operations via Drizzle ORM. Polling-based real-time updates and client-side caching with React Query. Centralized error handling.
- **Security**: Enterprise-level security including HTTPS enforcement, XSS/SQL injection prevention, comprehensive CSP headers, JWT authentication readiness, input sanitization, rate limiting, CORS protection, and Helmet security headers. Secure iframe sandboxing and external link handling.
- **Performance**: Optimized frontend builds with code splitting, efficient Express.js backend, and optimized Drizzle ORM queries with caching.
- **Deployment**: Custom domain support with CORS configuration for cryptohippo.locker domain. Production-ready API endpoints with comprehensive error handling and TypeScript type safety.

## External Dependencies

- **Database**: Neon (PostgreSQL serverless).
- **UI Libraries**: Radix UI, shadcn/ui, Tailwind CSS.
- **Data Fetching/Management**: TanStack Query.
- **API Integrations**:
    - **Market Data**: CoinMarketCap API, GeckoTerminal API.
    - **Portfolio Tracking**: DeBank API, TaoStats API, TaoHub, Hyperfolio, DexScreener.
    - **Blockchain Explorers**: Etherscan v2, Basescan API, BlockCreeper.com.
    - **Analytics Platforms**: Artemis Analytics, Swordscan, Moby Screener, Bubblemaps, CoinMarketMan, StockAnalysis.com, Screener.in, AInvest.com, Unusual Whales, MacroEdge, Investing.com, StockTwits.
    - **Trading/DEX**: Jupiter Aggregator, Aerodrome Finance, Uniswap, UniversalX, Hyperliquid, Polymarket.
    - **Bittensor Specific**: TaoStats, Backprop.finance, TaoMarketCap.com.
    - **Treasury Data**: bitcointreasuries.net, strategicethreserve.xyz, taotreasuries.app, thenew.money.
    - **News**: Cointelegraph.
    - **Social Intelligence**: X.com.
    - **AI/DeFAI**: Aya AI, ChatGPT Crypto Trading & Investing GPT, AIxVC, Senpi AI, Arma Protocol, ZYF AI Dashboard.
    - **NFT Analytics**: OpenSea.
    - **Miscellaneous**: Terminal.co, Checkr.social, Virtuals.io, Creator.bid, Bankr.bot, Velvet Capital, Indexy.xyz, OKX Leaderboard, NewHedge.
- **Build Tools**: Vite, esbuild, tsx.