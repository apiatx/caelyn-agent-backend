# CryptoHippo Trading Dashboard - Design Guidelines

## Design Approach

**Reference-Based Approach**: Drawing from Dexscreener's navigation patterns, TradingView's chart interfaces, and Binance's trading dashboard aesthetics, creating a premium crypto trading experience with distinctive hippo branding and glassmorphism treatments.

**Core Principles**:
- Information density with breathing room
- Instant data accessibility
- Professional trading interface standards
- Playful hippo mascot integration without compromising seriousness

---

## Typography System

**Font Family**: 
- Primary: Inter (via Google Fonts) - clean, professional, excellent at small sizes for data display
- Accent: Space Grotesk - for headers and branding elements

**Hierarchy**:
- Hero/Display: text-4xl to text-6xl, font-bold (Space Grotesk)
- Section Headers: text-2xl to text-3xl, font-semibold (Space Grotesk)
- Card Titles: text-lg, font-medium (Inter)
- Data Labels: text-sm, font-medium, uppercase tracking-wide (Inter)
- Numerical Data: text-base to text-2xl, font-mono for prices/percentages
- Body Text: text-sm to text-base (Inter)
- Small Print/Metadata: text-xs, opacity-70 (Inter)

---

## Layout System

**Spacing Primitives**: Use Tailwind units of 2, 4, 6, and 8 as primary spacing (p-2, gap-4, m-6, space-y-8)

**Grid Structure**:
```
Desktop: Fixed sidebar (w-64) + main content area
Tablet: Collapsible sidebar + main content
Mobile: Bottom navigation + full-width content
```

**Container Strategy**:
- Dashboard pages: Full-width with px-4 to px-8 padding
- Content cards: Consistent p-4 to p-6 internal padding
- Charts/Trading views: Edge-to-edge within cards

**Vertical Rhythm**: Consistent section spacing of space-y-6 to space-y-8 within main content areas

---

## Navigation Architecture

**Left Sidebar (Dexscreener-style)**:
- Fixed width: w-64 on desktop
- Parent-child hierarchy with indented children (pl-8 for children)
- Parent items: text-sm, font-medium with chevron indicators
- Child items: text-sm, opacity-80
- Active states: glassmorphism highlight (bg-white/10)
- Hippo mascot logo at top (h-12)
- Collapsible sections with smooth transitions

**Navigation Groups**:
1. Overview (Dashboard, Portfolio)
2. Markets (Trending, Watchlist, All Coins)
3. Trading (Spot, Futures, P2P)
4. Predictions (Active Markets, Create Market)
5. Ecosystem (DeFi, NFTs, Staking)
6. Account (Settings, API Keys)

---

## Component Library

### Cards & Containers
**Base Card**: Glassmorphism treatment with bg-white/5, border border-white/10, backdrop-blur-xl, rounded-2xl
**Elevated Card**: bg-white/10 for important content sections
**Nested Cards**: bg-white/5 within main containers for sub-sections

### Data Display Components
**Price Tiles**: Large mono font numbers with color-coded percentage changes, micro sparkline charts
**Market Table**: Striped rows (odd:bg-white/5), sticky headers, sortable columns, inline mini charts
**Statistics Cards**: 2x2 or 3-column grid (grid-cols-2 lg:grid-cols-3), displaying key metrics with trend indicators
**Live Feed**: Scrollable list with subtle animation for new entries, timestamp on each item

### Trading Interface Components
**Chart Container**: Full-width glassmorphism card with integrated timeframe selector tabs at top
**Order Book**: Two-column layout (bids/asks), mono font, gradient intensity showing depth
**Trade Form**: Stacked inputs with glassmorphism, clear buy/sell button states, summary preview
**Position Cards**: Horizontal layout showing entry/current/PnL with color-coded profit indicators

### Prediction Market Components
**Market Cards**: Image thumbnail + title + current odds + volume, grid layout (grid-cols-1 md:grid-cols-2 lg:grid-cols-3)
**Outcome Options**: Pill-shaped buttons showing percentage and stake amounts
**Activity Timeline**: Vertical timeline with user avatars and bet amounts

### Branding Elements
**Hippo Mascot Integration**:
- Sidebar logo: Minimalist hippo silhouette
- Empty states: Playful hippo illustration with friendly messaging
- Loading states: Animated hippo icon
- Achievement badges: Hippo-themed reward graphics

---

## Page-Specific Layouts

### Dashboard (Homepage)
**Hero Section**: 
- Large hero image featuring abstract crypto visualization with hippo mascot silhouette overlay
- Glowing gradient effects (purple/blue/cyan spectrum)
- H-96 height with centered content
- CTA buttons with backdrop-blur-lg bg-white/10 backgrounds
- Portfolio value display overlaid on image

**Content Grid**: 
- 3-column layout (lg:grid-cols-3) below hero
- Left column (col-span-2): Live price charts, trending coins table
- Right column: Quick trade panel, recent transactions

### Market Analytics
**Full-width Chart Area**: 
- TradingView-style advanced chart taking 60% vertical space
- Glassmorphism control panel overlay in bottom-left
- Market depth visualization below main chart

**Market Overview Grid**:
- 4-column stats grid (grid-cols-2 lg:grid-cols-4)
- Market cap, 24h volume, dominance, fear/greed index
- Top gainers/losers tables in 2-column layout

### Trading Interface
**Split Layout**:
- Left (60%): Price chart with order book visualization overlay
- Right (40%): Trade execution panel with order forms
- Bottom strip: Open orders table, trade history tabs

### Prediction Markets
**Market Grid**: 3-column card grid with featured market banner at top spanning full width
**Market Detail**: Hero image showing market question, odds visualization chart, participation statistics, outcome selection interface

### Ecosystem Pages
**Category Tabs**: Horizontal tab navigation (DeFi, NFTs, Staking, Governance)
**Project Cards**: Grid layout with protocol logos, TVL/APY data, quick action buttons
**Comparison Tables**: Side-by-side protocol comparison with metrics

---

## Images

**Hero Images**:
1. **Dashboard Hero** (h-96): Abstract 3D cryptocurrency visualization with floating coins, blockchain network visualization, subtle hippo mascot silhouette integrated into background, gradient overlay (purple to blue), sharp and modern
2. **Prediction Markets Hero** (h-64): Dynamic betting visualization showing odds fluctuations, crowd participation imagery, energetic composition
3. **Ecosystem Landing** (h-80): Interconnected DeFi protocol visualization, network graph aesthetic, nodes and connections representing blockchain ecosystem

**Supporting Images**:
- Protocol logos throughout (h-10 to h-12, rounded-full or rounded-lg)
- Coin icons in tables and cards (h-8, rounded-full)
- Hippo mascot variations for empty states (h-32 to h-48, illustration style)
- Chart thumbnails in market cards (aspect-video, sparkline style)

**Image Treatment**: All images within glassmorphism cards should have subtle border-white/5 borders, overlay gradients where text is placed

---

## Responsive Behavior

**Breakpoint Strategy**:
- Mobile (<768px): Stack all columns, bottom nav, collapsible sections
- Tablet (768px-1024px): 2-column grids, slide-out sidebar
- Desktop (>1024px): Full multi-column layouts, persistent sidebar

**Mobile Optimizations**:
- Horizontal scrolling for data tables
- Bottom sheet modals for trading forms
- Swipeable chart timeframes
- Condensed navigation with hamburger menu

---

## Interactive Patterns

**Micro-interactions**:
- Card hover: subtle scale-105 transform with transition-transform duration-200
- Price updates: flash animation on change (bg-white/20 fade)
- Loading states: Skeleton screens with glassmorphism treatment, shimmer effect

**State Indicators**:
- Real-time updates: Pulsing dot indicator
- Profit/Loss: Green upward/red downward arrows with percentages
- Order status: Color-coded pills (pending, filled, cancelled)

---

## Accessibility Considerations

**Contrast**: Ensure text maintains WCAG AA standards against glassmorphism backgrounds (use text-white with appropriate opacity levels)
**Focus States**: Visible ring-2 ring-white/50 on all interactive elements
**Touch Targets**: Minimum h-12 for all buttons and interactive elements
**Screen Readers**: Proper ARIA labels for charts, tables, and dynamic content