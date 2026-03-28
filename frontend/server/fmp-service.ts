const FMP_API_KEY = process.env.FMP_API_KEY || '';
const FMP_BASE = 'https://financialmodelingprep.com/stable';

interface StockQuote {
  symbol: string;
  name: string;
  price: number;
  changesPercentage: number;
  change: number;
  dayLow: number;
  dayHigh: number;
  yearHigh: number;
  yearLow: number;
  marketCap: number;
  volume: number;
  avgVolume: number;
  open: number;
  previousClose: number;
  eps: number;
  pe: number;
  earningsAnnouncement: string;
  sector: string;
  industry: string;
  companyName: string;
}

interface PriceTarget {
  symbol: string;
  targetHigh: number;
  targetLow: number;
  targetConsensus: number;
  targetMedian: number;
}

interface EarningsEvent {
  date: string;
  symbol: string;
  eps: number | null;
  epsEstimated: number | null;
  time: string;
  revenue: number | null;
  revenueEstimated: number | null;
}

interface DividendEvent {
  date: string;
  label: string;
  symbol: string;
  dividend: number;
  adjDividend: number;
  recordDate: string;
  paymentDate: string;
  declarationDate: string;
}

const cache = new Map<string, { data: any; timestamp: number }>();
const CACHE_TTL = 60_000;
const CRYPTO_CACHE_TTL = 300_000;

function getCached<T>(key: string): T | null {
  const cached = cache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) return cached.data as T;
  return null;
}

function setCache(key: string, data: any) {
  cache.set(key, { data, timestamp: Date.now() });
}

const INDEX_YAHOO_MAP: Record<string, { yahoo: string; name: string }> = {
  'VIX': { yahoo: '^VIX', name: 'CBOE Volatility Index' },
  'SPX': { yahoo: '^GSPC', name: 'S&P 500' },
  'DJI': { yahoo: '^DJI', name: 'Dow Jones Industrial Average' },
  'IXIC': { yahoo: '^IXIC', name: 'NASDAQ Composite' },
  'NDX': { yahoo: '^NDX', name: 'NASDAQ 100' },
  'RUT': { yahoo: '^RUT', name: 'Russell 2000' },
  'DXY': { yahoo: 'DX-Y.NYB', name: 'US Dollar Index' },
  'TNX': { yahoo: '^TNX', name: '10-Year Treasury Yield' },
  'NIFTY': { yahoo: '^NSEI', name: 'NIFTY 50' },
};

async function fetchIndexQuotes(symbols: string[]): Promise<StockQuote[]> {
  const yahooSymbols = symbols.map(s => INDEX_YAHOO_MAP[s.toUpperCase()]?.yahoo || `^${s}`);
  const originalMap: Record<string, string> = {};
  symbols.forEach((s, i) => { originalMap[yahooSymbols[i]] = s.toUpperCase(); });

  const cacheKey = `index:${symbols.join(',')}`;
  const cached = getCached<StockQuote[]>(cacheKey);
  if (cached) return cached;

  try {
    const symbolStr = yahooSymbols.join(',');
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbolStr)}&fields=symbol,shortName,longName,regularMarketPrice,regularMarketChange,regularMarketChangePercent,regularMarketDayHigh,regularMarketDayLow,fiftyTwoWeekHigh,fiftyTwoWeekLow,regularMarketVolume,regularMarketOpen,regularMarketPreviousClose`;

    console.log(`[Yahoo] Fetching INDEX quotes for: ${symbolStr}`);
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      }
    });

    if (!res.ok) {
      console.error(`[Yahoo] Index quote API returned ${res.status}`);
      return [];
    }

    const data = await res.json();
    const results = data?.quoteResponse?.result || [];

    const quotes: StockQuote[] = results.map((q: any) => {
      const origSymbol = originalMap[q.symbol] || q.symbol?.replace('^', '') || '';
      const meta = INDEX_YAHOO_MAP[origSymbol];
      return {
        symbol: origSymbol,
        name: meta?.name || q.shortName || q.longName || origSymbol,
        companyName: meta?.name || q.longName || q.shortName || origSymbol,
        price: q.regularMarketPrice || 0,
        change: q.regularMarketChange || 0,
        changesPercentage: q.regularMarketChangePercent || 0,
        dayHigh: q.regularMarketDayHigh || 0,
        dayLow: q.regularMarketDayLow || 0,
        yearHigh: q.fiftyTwoWeekHigh || 0,
        yearLow: q.fiftyTwoWeekLow || 0,
        marketCap: 0,
        volume: q.regularMarketVolume || 0,
        avgVolume: 0,
        open: q.regularMarketOpen || 0,
        previousClose: q.regularMarketPreviousClose || 0,
        eps: 0,
        pe: 0,
        earningsAnnouncement: '',
        sector: 'Indices',
        industry: 'Index',
      };
    });

    if (quotes.length > 0) setCache(cacheKey, quotes);
    return quotes;
  } catch (err) {
    console.error('[Yahoo] Index quote fetch failed:', err);
    return [];
  }
}

async function fetchYahooQuotes(symbols: string[]): Promise<StockQuote[]> {
  const cacheKey = `yahoo:${symbols.join(',')}`;
  const cached = getCached<StockQuote[]>(cacheKey);
  if (cached) return cached;

  try {
    const symbolStr = symbols.join(',');
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbolStr)}&fields=symbol,shortName,longName,regularMarketPrice,regularMarketChange,regularMarketChangePercent,regularMarketDayHigh,regularMarketDayLow,fiftyTwoWeekHigh,fiftyTwoWeekLow,marketCap,regularMarketVolume,averageDailyVolume3Month,regularMarketOpen,regularMarketPreviousClose,epsTrailingTwelveMonths,trailingPE,earningsTimestamp,sector,industry`;
    
    console.log(`[Yahoo] Fetching quotes for: ${symbolStr}`);
    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
      }
    });
    
    if (!res.ok) {
      console.error(`[Yahoo] Quote API returned ${res.status}`);
      return fetchYahooChartFallback(symbols);
    }
    
    const data = await res.json();
    const results = data?.quoteResponse?.result || [];
    
    const quotes: StockQuote[] = results.map((q: any) => ({
      symbol: q.symbol || '',
      name: q.shortName || q.longName || q.symbol || '',
      companyName: q.longName || q.shortName || q.symbol || '',
      price: q.regularMarketPrice || 0,
      change: q.regularMarketChange || 0,
      changesPercentage: q.regularMarketChangePercent || 0,
      dayHigh: q.regularMarketDayHigh || 0,
      dayLow: q.regularMarketDayLow || 0,
      yearHigh: q.fiftyTwoWeekHigh || 0,
      yearLow: q.fiftyTwoWeekLow || 0,
      marketCap: q.marketCap || 0,
      volume: q.regularMarketVolume || 0,
      avgVolume: q.averageDailyVolume3Month || 0,
      open: q.regularMarketOpen || 0,
      previousClose: q.regularMarketPreviousClose || 0,
      eps: q.epsTrailingTwelveMonths || 0,
      pe: q.trailingPE || 0,
      earningsAnnouncement: q.earningsTimestamp ? new Date(q.earningsTimestamp * 1000).toISOString() : '',
      sector: q.sector || 'Unknown',
      industry: q.industry || 'Unknown',
    }));
    
    if (quotes.length > 0) {
      setCache(cacheKey, quotes);
    }
    return quotes;
  } catch (err) {
    console.error('[Yahoo] Quote fetch failed:', err);
    return fetchYahooChartFallback(symbols);
  }
}

async function fetchYahooChartFallback(symbols: string[]): Promise<StockQuote[]> {
  const results: StockQuote[] = [];
  
  for (const symbol of symbols) {
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=5d&includePrePost=false`;
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        }
      });
      
      if (!res.ok) continue;
      const data = await res.json();
      const meta = data?.chart?.result?.[0]?.meta;
      if (!meta) continue;
      
      const indicators = data?.chart?.result?.[0]?.indicators?.quote?.[0];
      const closes = indicators?.close || [];
      const prevClose = closes.length >= 2 ? closes[closes.length - 2] : meta.chartPreviousClose || meta.previousClose || 0;
      const currentPrice = meta.regularMarketPrice || 0;
      const change = currentPrice - prevClose;
      const changePct = prevClose > 0 ? (change / prevClose) * 100 : 0;
      
      results.push({
        symbol: meta.symbol || symbol,
        name: meta.shortName || meta.longName || symbol,
        companyName: meta.longName || meta.shortName || symbol,
        price: currentPrice,
        change: change,
        changesPercentage: changePct,
        dayHigh: meta.regularMarketDayHigh || 0,
        dayLow: meta.regularMarketDayLow || 0,
        yearHigh: meta.fiftyTwoWeekHigh || 0,
        yearLow: meta.fiftyTwoWeekLow || 0,
        marketCap: 0,
        volume: meta.regularMarketVolume || 0,
        avgVolume: 0,
        open: meta.regularMarketOpen || 0,
        previousClose: prevClose,
        eps: 0,
        pe: 0,
        earningsAnnouncement: '',
        sector: 'Unknown',
        industry: 'Unknown',
      });
    } catch (err) {
      console.error(`[Yahoo] Chart fallback failed for ${symbol}:`, err);
    }
  }
  
  if (results.length > 0) {
    setCache(`yahoo:${symbols.join(',')}`, results);
  }
  return results;
}

const PROFILE_CACHE_TTL = 3600_000;
const profileCache = new Map<string, { data: any; timestamp: number }>();

async function enrichWithFMPProfile(quotes: StockQuote[]): Promise<StockQuote[]> {
  const needsEnrichment = quotes.filter(q => {
    const cached = profileCache.get(q.symbol.toUpperCase());
    if (cached && Date.now() - cached.timestamp < PROFILE_CACHE_TTL) {
      return false;
    }
    return (!q.sector || q.sector === 'Unknown') && (!q.marketCap || q.marketCap === 0);
  });

  const enrichedFromCache = quotes.map(q => {
    const cached = profileCache.get(q.symbol.toUpperCase());
    if (cached && Date.now() - cached.timestamp < PROFILE_CACHE_TTL && cached.data) {
      const p = cached.data;
      return { ...q, sector: p.sector || q.sector, industry: p.industry || q.industry, marketCap: p.mktCap || p.marketCap || q.marketCap, companyName: p.companyName || q.companyName };
    }
    return q;
  });

  if (needsEnrichment.length === 0 || !FMP_API_KEY) return enrichedFromCache;

  const symbols = needsEnrichment.map(q => q.symbol);
  console.log(`[PORTFOLIO] Fetching FMP profile for: ${symbols.join(',')}`);

  try {
    const url = `${FMP_BASE}/profile?symbol=${symbols.join(',')}&apikey=${FMP_API_KEY}`;
    const res = await fetch(url);
    if (!res.ok) {
      console.error(`[PORTFOLIO] FMP profile returned ${res.status}`);
      symbols.forEach(s => profileCache.set(s.toUpperCase(), { data: null, timestamp: Date.now() }));
      return enrichedFromCache;
    }
    const profiles: any[] = await res.json();
    if (!Array.isArray(profiles)) return enrichedFromCache;

    const profileMap = new Map<string, any>();
    profiles.forEach(p => {
      if (p.symbol) {
        profileMap.set(p.symbol.toUpperCase(), p);
        profileCache.set(p.symbol.toUpperCase(), { data: p, timestamp: Date.now() });
      }
    });

    return enrichedFromCache.map(q => {
      const p = profileMap.get(q.symbol.toUpperCase());
      if (!p) return q;
      console.log(`[PORTFOLIO] ${q.symbol} enriched: sector=${p.sector}, mktCap=${p.mktCap}`);
      return { ...q, sector: p.sector || q.sector, industry: p.industry || q.industry, marketCap: p.mktCap || p.marketCap || q.marketCap, companyName: p.companyName || q.companyName };
    });
  } catch (err) {
    console.error('[PORTFOLIO] FMP profile fetch failed:', err);
    return enrichedFromCache;
  }
}

async function fetchFMP<T>(endpoint: string, params: Record<string, string> = {}): Promise<T> {
  const queryParams = new URLSearchParams({ ...params, apikey: FMP_API_KEY });
  const url = `${FMP_BASE}${endpoint}?${queryParams}`;
  const cacheKey = `fmp:${endpoint}:${JSON.stringify(params)}`;
  const cached = getCached<T>(cacheKey);
  if (cached) return cached;

  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`FMP API error ${res.status}: ${text.substring(0, 200)}`);
  }
  const data = await res.json();
  setCache(cacheKey, data);
  return data as T;
}

const CRYPTO_COINGECKO_IDS: Record<string, string> = {
  'BTC': 'bitcoin', 'ETH': 'ethereum', 'SOL': 'solana', 'DOGE': 'dogecoin',
  'XRP': 'ripple', 'ADA': 'cardano', 'AVAX': 'avalanche-2', 'LINK': 'chainlink',
  'DOT': 'polkadot', 'UNI': 'uniswap', 'SHIB': 'shiba-inu', 'NEAR': 'near',
  'SUI': 'sui', 'APT': 'aptos', 'ARB': 'arbitrum', 'OP': 'optimism',
  'PEPE': 'pepe', 'WIF': 'dogwifcoin', 'FET': 'artificial-superintelligence-alliance',
  'INJ': 'injective-protocol', 'RENDER': 'render-token', 'FIL': 'filecoin',
  'LTC': 'litecoin', 'BCH': 'bitcoin-cash', 'AAVE': 'aave', 'MATIC': 'matic-network',
  'HYPE': 'hyperliquid', 'TAO': 'bittensor', 'TIA': 'celestia', 'SEI': 'sei-network',
};

const COMMODITY_YAHOO_SYMBOLS: Record<string, string> = {
  'GOLD': 'GC=F', 'SILVER': 'SI=F', 'OIL': 'CL=F', 'CRUDE': 'CL=F',
  'NATGAS': 'NG=F', 'COPPER': 'HG=F', 'PLATINUM': 'PL=F', 'PALLADIUM': 'PA=F',
  'WHEAT': 'ZW=F', 'CORN': 'ZC=F',
};

async function fetchCryptoQuotes(symbols: string[], retryCount = 0): Promise<StockQuote[]> {
  const cacheKey = `crypto:${symbols.join(',')}`;
  const cached = cache.get(cacheKey);
  if (cached && Date.now() - cached.timestamp < CRYPTO_CACHE_TTL) return cached.data as StockQuote[];

  const ids = symbols.map(s => CRYPTO_COINGECKO_IDS[s.toUpperCase()] || s.toLowerCase()).filter(Boolean);
  if (ids.length === 0) return [];

  try {
    const url = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${ids.join(',')}&order=market_cap_desc&sparkline=false&price_change_percentage=24h`;
    console.log(`[PORTFOLIO] CoinGecko fetching: ${ids.join(',')}`);
    const res = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (res.status === 429 && retryCount < 2) {
      const delay = (retryCount + 1) * 3000;
      console.log(`[PORTFOLIO] CoinGecko rate limited, retrying in ${delay}ms...`);
      await new Promise(r => setTimeout(r, delay));
      return fetchCryptoQuotes(symbols, retryCount + 1);
    }
    if (!res.ok) {
      console.error(`[PORTFOLIO] CoinGecko returned ${res.status}`);
      return fetchCryptoFromYahoo(symbols);
    }
    const data = await res.json();
    const idToTicker = new Map<string, string>();
    symbols.forEach(s => { idToTicker.set(CRYPTO_COINGECKO_IDS[s.toUpperCase()] || s.toLowerCase(), s.toUpperCase()); });

    const quotes: StockQuote[] = (Array.isArray(data) ? data : []).map((c: any) => {
      const ticker = idToTicker.get(c.id) || c.symbol?.toUpperCase() || c.id;
      const price = c.current_price || 0;
      const prevPrice = price / (1 + (c.price_change_percentage_24h || 0) / 100);
      console.log(`[PORTFOLIO] CoinGecko: ${ticker} = $${price}`);
      return {
        symbol: ticker,
        name: c.name || ticker,
        companyName: c.name || ticker,
        price,
        change: c.price_change_24h || 0,
        changesPercentage: c.price_change_percentage_24h || 0,
        dayHigh: c.high_24h || 0,
        dayLow: c.low_24h || 0,
        yearHigh: c.ath || 0,
        yearLow: c.atl || 0,
        marketCap: c.market_cap || 0,
        volume: c.total_volume || 0,
        avgVolume: 0,
        open: prevPrice,
        previousClose: prevPrice,
        eps: 0, pe: 0, earningsAnnouncement: '',
        sector: 'Crypto', industry: 'Cryptocurrency',
      };
    });

    if (quotes.length > 0) setCache(cacheKey, quotes);
    return quotes;
  } catch (err) {
    console.error('[PORTFOLIO] CoinGecko fetch failed:', err);
    return fetchCryptoFromYahoo(symbols);
  }
}

const YAHOO_CRYPTO_SKIP = new Set(['HYPE', 'TAO', 'WIF', 'TIA', 'SEI', 'PEPE', 'ARB', 'OP', 'APT', 'SUI', 'RENDER', 'FET', 'INJ', 'NEAR']);

async function fetchCryptoFromYahoo(symbols: string[]): Promise<StockQuote[]> {
  console.log(`[PORTFOLIO] Falling back to Yahoo for crypto: ${symbols.join(',')}`);
  const results: StockQuote[] = [];
  for (const sym of symbols) {
    if (YAHOO_CRYPTO_SKIP.has(sym.toUpperCase())) {
      console.log(`[PORTFOLIO] Skipping Yahoo for ${sym} (no reliable Yahoo symbol)`);
      continue;
    }
    try {
      const yahooSym = `${sym.toUpperCase()}-USD`;
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSym)}?interval=1d&range=5d&includePrePost=false`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
      });
      if (!res.ok) continue;
      const data = await res.json();
      const meta = data?.chart?.result?.[0]?.meta;
      if (!meta) continue;
      const indicators = data?.chart?.result?.[0]?.indicators?.quote?.[0];
      const closes = indicators?.close || [];
      const prevClose = closes.length >= 2 ? closes[closes.length - 2] : meta.chartPreviousClose || meta.previousClose || 0;
      const currentPrice = meta.regularMarketPrice || 0;
      const change = currentPrice - prevClose;
      const changePct = prevClose > 0 ? (change / prevClose) * 100 : 0;
      console.log(`[PORTFOLIO] Yahoo crypto: ${sym} = $${currentPrice}`);
      results.push({
        symbol: sym.toUpperCase(),
        name: meta.shortName || sym, companyName: meta.longName || sym,
        price: currentPrice, change, changesPercentage: changePct,
        dayHigh: meta.regularMarketDayHigh || 0, dayLow: meta.regularMarketDayLow || 0,
        yearHigh: meta.fiftyTwoWeekHigh || 0, yearLow: meta.fiftyTwoWeekLow || 0,
        marketCap: 0, volume: meta.regularMarketVolume || 0, avgVolume: 0,
        open: meta.regularMarketOpen || 0, previousClose: prevClose,
        eps: 0, pe: 0, earningsAnnouncement: '',
        sector: 'Crypto', industry: 'Cryptocurrency',
      });
    } catch (err) {
      console.error(`[PORTFOLIO] Yahoo crypto fallback failed for ${sym}:`, err);
    }
  }
  if (results.length > 0) {
    setCache(`crypto:${symbols.join(',')}`, results);
  }
  return results;
}

async function fetchCommodityQuotes(symbols: string[]): Promise<StockQuote[]> {
  const yahooSymbols = symbols.map(s => COMMODITY_YAHOO_SYMBOLS[s.toUpperCase()] || `${s}=F`);
  const results: StockQuote[] = [];

  for (let i = 0; i < symbols.length; i++) {
    const ticker = symbols[i].toUpperCase();
    const yahooSym = yahooSymbols[i];
    try {
      const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSym)}?interval=1d&range=5d&includePrePost=false`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
      });
      if (!res.ok) {
        console.error(`[PORTFOLIO] Commodity Yahoo chart failed for ${ticker} (${yahooSym}): ${res.status}`);
        continue;
      }
      const data = await res.json();
      const meta = data?.chart?.result?.[0]?.meta;
      if (!meta) continue;

      const indicators = data?.chart?.result?.[0]?.indicators?.quote?.[0];
      const closes = indicators?.close || [];
      const prevClose = closes.length >= 2 ? closes[closes.length - 2] : meta.chartPreviousClose || meta.previousClose || 0;
      const currentPrice = meta.regularMarketPrice || 0;
      const change = currentPrice - prevClose;
      const changePct = prevClose > 0 ? (change / prevClose) * 100 : 0;

      console.log(`[PORTFOLIO] Commodity: ${ticker} = $${currentPrice}`);
      results.push({
        symbol: ticker,
        name: meta.shortName || ticker,
        companyName: meta.longName || meta.shortName || ticker,
        price: currentPrice,
        change, changesPercentage: changePct,
        dayHigh: meta.regularMarketDayHigh || 0, dayLow: meta.regularMarketDayLow || 0,
        yearHigh: meta.fiftyTwoWeekHigh || 0, yearLow: meta.fiftyTwoWeekLow || 0,
        marketCap: 0, volume: meta.regularMarketVolume || 0, avgVolume: 0,
        open: meta.regularMarketOpen || 0, previousClose: prevClose,
        eps: 0, pe: 0, earningsAnnouncement: '',
        sector: 'Commodities', industry: 'Commodity',
      });
    } catch (err) {
      console.error(`[PORTFOLIO] Commodity fetch failed for ${ticker}:`, err);
    }
  }
  return results;
}

export const fmpService = {
  async getQuotes(symbols: string[]): Promise<StockQuote[]> {
    if (symbols.length === 0) return [];
    return fetchYahooQuotes(symbols);
  },

  async getStockDetails(symbols: string[], assetTypes?: Record<string, string>): Promise<StockQuote[]> {
    if (symbols.length === 0) return [];

    if (!assetTypes || Object.keys(assetTypes).length === 0) {
      return fetchYahooQuotes(symbols);
    }

    const stockSymbols: string[] = [];
    const cryptoSymbols: string[] = [];
    const commoditySymbols: string[] = [];
    const indexSymbols: string[] = [];

    for (const sym of symbols) {
      const type = (assetTypes[sym] || assetTypes[sym.toUpperCase()] || 'stock').toLowerCase();
      if (type === 'crypto') cryptoSymbols.push(sym);
      else if (type === 'commodity') commoditySymbols.push(sym);
      else if (type === 'index') indexSymbols.push(sym);
      else stockSymbols.push(sym);
    }

    console.log(`[PORTFOLIO] Routing: stocks=${stockSymbols.join(',')}, crypto=${cryptoSymbols.join(',')}, commodities=${commoditySymbols.join(',')}, indices=${indexSymbols.join(',')}`);

    const [rawStockQuotes, cryptoQuotes, commodityQuotes, indexQuotes] = await Promise.all([
      stockSymbols.length > 0 ? fetchYahooQuotes(stockSymbols) : Promise.resolve([]),
      cryptoSymbols.length > 0 ? fetchCryptoQuotes(cryptoSymbols) : Promise.resolve([]),
      commoditySymbols.length > 0 ? fetchCommodityQuotes(commoditySymbols) : Promise.resolve([]),
      indexSymbols.length > 0 ? fetchIndexQuotes(indexSymbols) : Promise.resolve([]),
    ]);

    const stockQuotes = await enrichWithFMPProfile(rawStockQuotes);

    return [...stockQuotes, ...cryptoQuotes, ...commodityQuotes, ...indexQuotes];
  },

  async getPriceTarget(symbol: string): Promise<PriceTarget[]> {
    try {
      return await fetchFMP<PriceTarget[]>('/price-target-consensus', { symbol });
    } catch {
      return [];
    }
  },

  async getEarningsCalendar(from?: string, to?: string): Promise<EarningsEvent[]> {
    try {
      const params: Record<string, string> = {};
      if (from) params.from = from;
      if (to) params.to = to;
      return await fetchFMP<EarningsEvent[]>('/earning_calendar', params);
    } catch {
      return [];
    }
  },

  async getDividendCalendar(from?: string, to?: string): Promise<DividendEvent[]> {
    try {
      const params: Record<string, string> = {};
      if (from) params.from = from;
      if (to) params.to = to;
      return await fetchFMP<DividendEvent[]>('/stock_dividend_calendar', params);
    } catch {
      return [];
    }
  },

  async getHoldingsEvents(symbols: string[]) {
    if (symbols.length === 0) return { earnings: [], dividends: [] };

    const today = new Date();
    const threeMonthsLater = new Date(today);
    threeMonthsLater.setMonth(threeMonthsLater.getMonth() + 3);

    const from = today.toISOString().split('T')[0];
    const to = threeMonthsLater.toISOString().split('T')[0];

    const [allEarnings, allDividends] = await Promise.all([
      this.getEarningsCalendar(from, to),
      this.getDividendCalendar(from, to),
    ]);

    const symbolSet = new Set(symbols.map(s => s.toUpperCase()));

    return {
      earnings: (Array.isArray(allEarnings) ? allEarnings : []).filter(e => symbolSet.has(e.symbol?.toUpperCase())),
      dividends: (Array.isArray(allDividends) ? allDividends : []).filter(d => symbolSet.has(d.symbol?.toUpperCase())),
    };
  },

  async getPriceTargets(symbols: string[]) {
    const results = await Promise.all(
      symbols.map(s => this.getPriceTarget(s).then(r => Array.isArray(r) && r.length > 0 ? r[0] : null).catch(() => null))
    );
    return results.filter(Boolean);
  },

  async searchTickers(query: string): Promise<Array<{symbol: string; name: string; type: string; exchange: string}>> {
    if (!query || query.length < 1) return [];
    try {
      const results = await fetchFMP<Array<{symbol: string; name: string; type: string; exchangeShortName: string}>>('/search', { query, limit: '10' });
      return (Array.isArray(results) ? results : []).map(r => ({
        symbol: r.symbol || '',
        name: r.name || '',
        type: r.type || 'stock',
        exchange: r.exchangeShortName || '',
      }));
    } catch { return []; }
  },
};
