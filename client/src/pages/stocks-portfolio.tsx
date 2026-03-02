import { Fragment, useState, useEffect, useCallback, useMemo, type ReactNode } from 'react';
import { Card } from "@/components/ui/card";
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid } from 'recharts';
import { Plus, Trash2, ArrowUpDown, ChevronDown, ChevronRight, Bot, Calendar, TrendingUp, TrendingDown, ExternalLink, RefreshCw, Briefcase } from 'lucide-react';


interface Holding {
  id: string;
  ticker: string;
  shares: number;
  avgCost: number;
  addedAt: string;
  assetType?: string;
}

interface QuoteData {
  symbol: string;
  name: string;
  companyName: string;
  price: number;
  change: number;
  changesPercentage: number;
  previousClose: number;
  sector: string;
  industry: string;
  earningsAnnouncement: string;
  pe: number;
  eps: number;
  marketCap: number;
  volume: number;
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
}

interface DividendEvent {
  date: string;
  symbol: string;
  dividend: number;
  adjDividend: number;
  paymentDate: string;
}

const GlassCard = ({ children, className = "" }: { children: ReactNode; className?: string }) => (
  <Card className={`backdrop-blur-lg ${className}`} style={{ background: '#141632', border: '1px solid #1e2148', boxShadow: '0 0 15px rgba(59, 130, 246, 0.05)', borderRadius: 12 }}>
    {children}
  </Card>
);

const SECTOR_COLORS: Record<string, string> = {
  'Technology': '#3b82f6',
  'Healthcare': '#22c55e',
  'Financial Services': '#f59e0b',
  'Consumer Cyclical': '#ec4899',
  'Communication Services': '#8b5cf6',
  'Industrials': '#6b7280',
  'Consumer Defensive': '#14b8a6',
  'Energy': '#ef4444',
  'Real Estate': '#a78bfa',
  'Utilities': '#06b6d4',
  'Basic Materials': '#d97706',
  'Crypto': '#f97316',
  'Commodities': '#78716c',
  'ETFs': '#06b6d4',
  'Indices': '#a78bfa',
  'Other': '#4b5563',
};

const PIE_COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ec4899', '#8b5cf6', '#14b8a6', '#ef4444', '#06b6d4', '#a78bfa', '#d97706', '#6366f1', '#f97316'];

const CRYPTO_TV_SYMBOLS: Record<string, string> = {
  "BTC": "BINANCE:BTCUSDT", "ETH": "BINANCE:ETHUSDT", "SOL": "BINANCE:SOLUSDT",
  "DOGE": "BINANCE:DOGEUSDT", "XRP": "BINANCE:XRPUSDT", "ADA": "BINANCE:ADAUSDT",
  "AVAX": "BINANCE:AVAXUSDT", "LINK": "BINANCE:LINKUSDT", "DOT": "BINANCE:DOTUSDT",
  "UNI": "BINANCE:UNIUSDT", "SHIB": "BINANCE:SHIBUSDT", "NEAR": "BINANCE:NEARUSDT",
  "SUI": "BINANCE:SUIUSDT", "APT": "BINANCE:APTUSDT", "ARB": "BINANCE:ARBUSDT",
  "OP": "BINANCE:OPUSDT", "PEPE": "BINANCE:PEPEUSDT", "FET": "BINANCE:FETUSDT",
  "INJ": "BINANCE:INJUSDT", "RENDER": "BINANCE:RENDERUSDT",
  "FIL": "BINANCE:FILUSDT", "LTC": "BINANCE:LTCUSDT", "BCH": "BINANCE:BCHUSDT",
  "AAVE": "BINANCE:AAVEUSDT", "MATIC": "BINANCE:MATICUSDT",
  "HYPE": "BYBIT:HYPEUSDT", "TAO": "BYBIT:TAOUSDT", "WIF": "BYBIT:WIFUSDT",
  "TIA": "BYBIT:TIAUSDT", "SEI": "BYBIT:SEIUSDT",
};

const COMMODITY_TV_SYMBOLS: Record<string, string> = {
  "SILVER": "TVC:SILVER", "GOLD": "TVC:GOLD", "OIL": "TVC:USOIL",
  "CRUDE": "TVC:USOIL", "NATGAS": "PEPPERSTONE:NATGAS",
  "COPPER": "PEPPERSTONE:COPPER", "PLATINUM": "TVC:PLATINUM",
  "PALLADIUM": "TVC:PALLADIUM", "WHEAT": "PEPPERSTONE:WHEAT",
  "CORN": "PEPPERSTONE:CORN",
};

const INDEX_TV_SYMBOLS: Record<string, string> = {
  "VIX": "AMEX:VIXY", "SPX": "AMEX:SPY", "SPY": "AMEX:SPY", "DJI": "AMEX:DIA",
  "IXIC": "NASDAQ:QQQ", "NDX": "NASDAQ:QQQ", "QQQ": "NASDAQ:QQQ",
  "RUT": "AMEX:IWM", "DXY": "AMEX:UUP", "TNX": "AMEX:TLT",
};

const INDEX_ETF_LABELS: Record<string, string> = {
  "VIX": "VIXY", "SPX": "SPY", "DJI": "DIA", "IXIC": "QQQ",
  "NDX": "QQQ", "RUT": "IWM", "DXY": "UUP", "TNX": "TLT",
};

const CRYPTO_DISPLAY_NAMES: Record<string, string> = {
  "BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana", "HYPE": "Hyperliquid",
  "DOGE": "Dogecoin", "XRP": "Ripple", "ADA": "Cardano", "AVAX": "Avalanche",
  "LINK": "Chainlink", "DOT": "Polkadot", "UNI": "Uniswap", "SHIB": "Shiba Inu",
  "NEAR": "NEAR Protocol", "SUI": "Sui", "APT": "Aptos", "ARB": "Arbitrum",
  "OP": "Optimism", "PEPE": "Pepe", "WIF": "dogwifhat", "RENDER": "Render",
  "FET": "Fetch.ai", "TAO": "Bittensor", "FIL": "Filecoin", "INJ": "Injective",
  "TIA": "Celestia", "SEI": "Sei", "LTC": "Litecoin", "AAVE": "Aave",
  "MATIC": "Polygon", "BCH": "Bitcoin Cash",
};

function getTradingViewSymbol(ticker: string, assetType?: string, tvSymbolFromQuote?: string): string {
  if (tvSymbolFromQuote) return tvSymbolFromQuote;
  const t = (ticker || '').toUpperCase();
  const type = (assetType || 'stock').toLowerCase();
  if (type === 'crypto' || type === 'cryptocurrency') return CRYPTO_TV_SYMBOLS[t] || `BINANCE:${t}USDT`;
  if (type === 'commodity' || type === 'commodities') return COMMODITY_TV_SYMBOLS[t] || t;
  return t;
}

function getDisplayName(ticker: string, assetType: string | undefined, quoteName?: string): string {
  if (assetType === 'crypto') return CRYPTO_DISPLAY_NAMES[ticker.toUpperCase()] || quoteName || ticker;
  return quoteName || ticker;
}

const SHARES_LABEL: Record<string, string> = {
  stock: 'Shares', etf: 'Shares', index: 'Units', crypto: 'Amount', commodity: 'Units',
};

const INDEX_TO_ETF: Record<string, { etf: string; name: string; index: string }> = {
  'SPX': { etf: 'SPY', name: 'SPDR S&P 500 ETF', index: 'S&P 500' },
  'GSPC': { etf: 'SPY', name: 'SPDR S&P 500 ETF', index: 'S&P 500' },
  'DJI': { etf: 'DIA', name: 'SPDR Dow Jones ETF', index: 'Dow Jones' },
  'DJIA': { etf: 'DIA', name: 'SPDR Dow Jones ETF', index: 'Dow Jones' },
  'IXIC': { etf: 'QQQ', name: 'Invesco QQQ Trust', index: 'NASDAQ Composite' },
  'NDX': { etf: 'QQQ', name: 'Invesco QQQ Trust', index: 'NASDAQ 100' },
  'RUT': { etf: 'IWM', name: 'iShares Russell 2000 ETF', index: 'Russell 2000' },
  'VIX': { etf: 'UVXY', name: 'ProShares Ultra VIX Short-Term Futures ETF', index: 'VIX' },
};

type SortKey = 'ticker' | 'shares' | 'avgCost' | 'currentPrice' | 'dailyPL' | 'totalPL' | 'weight';

export default function StocksPortfolioPage() {
  const [holdings, setHoldings] = useState<Holding[]>([]);
  const [quotes, setQuotes] = useState<Record<string, QuoteData>>({});
  const [priceTargets, setPriceTargets] = useState<Record<string, PriceTarget>>({});
  const [earnings, setEarnings] = useState<EarningsEvent[]>([]);
  const [dividends, setDividends] = useState<DividendEvent[]>([]);
  const [newTicker, setNewTicker] = useState('');
  const [newShares, setNewShares] = useState('');
  const [newAvgCost, setNewAvgCost] = useState('');
  const [sortKey, setSortKey] = useState<SortKey>('weight');
  const [sortAsc, setSortAsc] = useState(false);
  const [expandedCard, setExpandedCard] = useState<string | null>(null);
  const [portfolioChartInterval, setPortfolioChartInterval] = useState('D');
  const [aiReview, setAiReview] = useState<string | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [aiStage, setAiStage] = useState('');
  const [loadingQuotes, setLoadingQuotes] = useState(false);
  const [quotesError, setQuotesError] = useState(false);
  const [addingHolding, setAddingHolding] = useState(false);
  const [selectedAssetType, setSelectedAssetType] = useState('stock');

  const fetchHoldings = useCallback(async () => {
    try {
      const res = await fetch('/api/stock-holdings');
      if (res.ok) {
        const data = await res.json();
        setHoldings(data);
      }
    } catch (err) {
      console.error('Failed to fetch holdings:', err);
    }
  }, []);

  const fetchQuotes = useCallback(async (holdingsList: Holding[]) => {
    if (holdingsList.length === 0) return;
    setLoadingQuotes(true);
    setQuotesError(false);
    try {
      const tickers = holdingsList.map(h => h.ticker.toUpperCase());
      const assetTypes: Record<string, string> = {};
      holdingsList.forEach(h => { assetTypes[h.ticker.toUpperCase()] = (h.assetType || 'stock').toLowerCase(); });
      const res = await fetch(`/api/fmp/quotes?symbols=${tickers.join(',')}&asset_types=${encodeURIComponent(JSON.stringify(assetTypes))}`);
      if (res.ok) {
        const data: QuoteData[] = await res.json();
        console.log("[PORTFOLIO] Quotes response:", JSON.stringify(data));
        if (Array.isArray(data) && data.length > 0) {
          const map: Record<string, QuoteData> = {};
          data.forEach(q => {
            map[q.symbol] = q;
            map[q.symbol.toUpperCase()] = q;
          });
          setQuotes(map);
        } else {
          setQuotesError(true);
        }
      } else {
        setQuotesError(true);
      }
    } catch (err) {
      console.error('Failed to fetch quotes:', err);
      setQuotesError(true);
    } finally {
      setLoadingQuotes(false);
    }
  }, []);

  const fetchPriceTargets = useCallback(async (tickers: string[]) => {
    if (tickers.length === 0) return;
    try {
      const res = await fetch(`/api/fmp/price-targets?symbols=${tickers.join(',')}`);
      if (res.ok) {
        const data: PriceTarget[] = await res.json();
        const map: Record<string, PriceTarget> = {};
        data.forEach(t => { if (t) map[t.symbol] = t; });
        setPriceTargets(map);
      }
    } catch (err) {
      console.error('Failed to fetch price targets:', err);
    }
  }, []);

  const fetchEvents = useCallback(async (tickers: string[]) => {
    if (tickers.length === 0) return;
    try {
      const res = await fetch(`/api/fmp/events?symbols=${tickers.join(',')}`);
      if (res.ok) {
        const data = await res.json();
        setEarnings(data.earnings || []);
        setDividends(data.dividends || []);
      }
    } catch (err) {
      console.error('Failed to fetch events:', err);
    }
  }, []);

  useEffect(() => { fetchHoldings(); }, [fetchHoldings]);

  useEffect(() => {
    if (holdings.length > 0) {
      fetchQuotes(holdings);
      fetchPriceTargets(holdings.map(h => h.ticker));
      fetchEvents(holdings.map(h => h.ticker));
    }
  }, [holdings, fetchQuotes, fetchPriceTargets, fetchEvents]);

  const addHolding = async () => {
    if (!newTicker.trim() || !newShares || !newAvgCost) return;
    setAddingHolding(true);
    try {
      const res = await fetch('/api/stock-holdings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: newTicker.trim(), shares: parseFloat(newShares), avgCost: parseFloat(newAvgCost), assetType: selectedAssetType || 'stock' }),
      });
      if (res.ok) {
        setNewTicker('');
        setNewShares('');
        setNewAvgCost('');
        setSelectedAssetType('stock');
        await fetchHoldings();
      }
    } catch (err) {
      console.error('Failed to add holding:', err);
    } finally {
      setAddingHolding(false);
    }
  };

  const deleteHolding = async (id: string) => {
    try {
      await fetch(`/api/stock-holdings/${id}`, { method: 'DELETE' });
      await fetchHoldings();
    } catch (err) {
      console.error('Failed to delete holding:', err);
    }
  };

  const enrichedHoldings = useMemo(() => {
    return holdings.map(h => {
      const q = quotes[h.ticker] || quotes[h.ticker.toUpperCase()];
      const currentPrice = q?.price || 0;
      const dailyChange = q?.change || 0;
      const dailyPL = dailyChange * h.shares;
      const totalPL = (currentPrice - h.avgCost) * h.shares;
      const totalValue = currentPrice * h.shares;
      return { ...h, currentPrice, dailyChange, dailyPL, totalPL, totalValue, quote: q };
    });
  }, [holdings, quotes]);

  const totalPortfolioValue = useMemo(() => enrichedHoldings.reduce((sum, h) => sum + h.totalValue, 0), [enrichedHoldings]);
  const totalDailyPL = useMemo(() => enrichedHoldings.reduce((sum, h) => sum + h.dailyPL, 0), [enrichedHoldings]);
  const totalOverallPL = useMemo(() => enrichedHoldings.reduce((sum, h) => sum + h.totalPL, 0), [enrichedHoldings]);
  const totalCostBasis = useMemo(() => enrichedHoldings.reduce((sum, h) => sum + (h.avgCost * h.shares), 0), [enrichedHoldings]);

  const sortedHoldings = useMemo(() => {
    const sorted = [...enrichedHoldings];
    sorted.sort((a, b) => {
      let va: number, vb: number;
      switch (sortKey) {
        case 'ticker': return sortAsc ? a.ticker.localeCompare(b.ticker) : b.ticker.localeCompare(a.ticker);
        case 'shares': va = a.shares; vb = b.shares; break;
        case 'avgCost': va = a.avgCost; vb = b.avgCost; break;
        case 'currentPrice': va = a.currentPrice; vb = b.currentPrice; break;
        case 'dailyPL': va = a.dailyPL; vb = b.dailyPL; break;
        case 'totalPL': va = a.totalPL; vb = b.totalPL; break;
        case 'weight': va = a.totalValue; vb = b.totalValue; break;
        default: va = 0; vb = 0;
      }
      return sortAsc ? va - vb : vb - va;
    });
    return sorted;
  }, [enrichedHoldings, sortKey, sortAsc]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  };

  const ASSET_COLORS: Record<string, string> = { Stocks: '#10b981', ETFs: '#3b82f6', Crypto: '#f59e0b', Commodities: '#ef4444' };
  const sectorData = useMemo(() => {
    const cats: Record<string, number> = {};
    enrichedHoldings.forEach(h => {
      const t = (h.assetType || 'stock').toLowerCase();
      let cat = 'Stocks';
      if (t === 'etf' || t === 'index' || t === 'indices') cat = 'ETFs';
      else if (t === 'crypto' || t === 'cryptocurrency') cat = 'Crypto';
      else if (t === 'commodity' || t === 'commodities') cat = 'Commodities';
      cats[cat] = (cats[cat] || 0) + h.totalValue;
    });
    return Object.entries(cats)
      .filter(([, v]) => v > 0)
      .map(([name, value]) => ({ name, value, pct: totalPortfolioValue > 0 ? ((value / totalPortfolioValue) * 100).toFixed(1) : '0', color: ASSET_COLORS[name] || '#64748b' }))
      .sort((a, b) => b.value - a.value);
  }, [enrichedHoldings, totalPortfolioValue]);

  const plBarData = useMemo(() => {
    return [...enrichedHoldings]
      .sort((a, b) => Math.abs(b.dailyPL) - Math.abs(a.dailyPL))
      .map(h => ({ ticker: h.ticker, dailyPL: parseFloat(h.dailyPL.toFixed(2)), fill: h.dailyPL >= 0 ? '#22c55e' : '#ef4444' }));
  }, [enrichedHoldings]);

  const runAIReview = async () => {
    if (holdings.length === 0) return;
    setAiLoading(true);
    setAiReview(null);
    const stages = ['Analyzing portfolio...', 'Pulling price data...', 'Scanning technicals...', 'Checking fundamentals...', 'Reading sentiment...', 'Building portfolio view...', 'Generating ratings...', 'Almost done — this can take up to 30 seconds...'];
    let idx = 0;
    setAiStage(stages[0]);
    const iv = setInterval(() => { idx++; if (idx < stages.length) setAiStage(stages[idx]); }, 2000);
    try {
      const holdingsPayload = holdings.map(h => ({
        ticker: h.ticker,
        shares: h.shares,
        avg_cost: h.avgCost,
      }));
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 90000);
      const res = await fetch('/api/portfolio-review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ holdings: holdingsPayload }),
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`Server returned ${res.status}${errText ? ': ' + errText : ''}`);
      }
      const data = await res.json();
      const analysisText = data.message || data.text || data.analysis || 'No analysis returned.';
      setAiReview(analysisText);
    } catch (err: any) {
      if (err.name === 'AbortError') {
        setAiReview('Portfolio review timed out. The analysis is taking longer than expected — please try again.');
      } else {
        setAiReview(`Failed to get portfolio review. Please try again. (${err.message})`);
      }
    } finally {
      clearInterval(iv);
      setAiStage('');
      setAiLoading(false);
    }
  };

  const allEvents = useMemo(() => {
    const events: Array<{ date: string; type: 'earnings' | 'dividend'; symbol: string; detail: string }> = [];
    earnings.forEach(e => {
      events.push({ date: e.date, type: 'earnings', symbol: e.symbol, detail: `EPS Est: ${e.epsEstimated ?? 'N/A'} | ${e.time === 'bmo' ? 'Before Open' : e.time === 'amc' ? 'After Close' : e.time || ''}` });
    });
    dividends.forEach(d => {
      events.push({ date: d.date, type: 'dividend', symbol: d.symbol, detail: `$${d.dividend?.toFixed(4) || '0'}/share | Pay: ${d.paymentDate || 'TBD'}` });
    });
    return events.sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());
  }, [earnings, dividends]);

  const fmt = (n: number) => n.toLocaleString('en-US', { style: 'currency', currency: 'USD' });
  const fmtPL = (n: number) => (n >= 0 ? '+' : '') + fmt(n);
  const pctPL = (n: number, base: number) => base === 0 ? '0.0%' : ((n / base) * 100).toFixed(1) + '%';

  const SortHeader = ({ label, keyName }: { label: string; keyName: SortKey }) => (
    <button onClick={() => handleSort(keyName)} className="flex items-center gap-1 text-xs text-crypto-silver hover:text-white transition-colors font-medium">
      {label}
      <ArrowUpDown className="w-3 h-3" />
    </button>
  );

  function escapeHtml(str: string) {
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function formatAnalysis(text: string) {
    if (!text) return '';
    const safe = escapeHtml(text);
    return safe
      .replace(/\*\*(.*?)\*\*/g, '<strong style="color:#fff">$1</strong>')
      .replace(/\n/g, '<br/>');
  }

  interface ParsedPosition {
    ticker: string;
    weight: string;
    pnl: string;
    verdict: string;
    thesis: string;
    keyRisk: string;
    catalyst: string;
    positionSize: string;
    extra: string[];
  }

  interface ParsedReview {
    positions: ParsedPosition[];
    overallAssessment: string;
    grade: string;
    actionItems: string[];
    newPosition: string;
    otherSections: { title: string; body: string }[];
  }

  function parsePortfolioReview(message: string): ParsedReview | null {
    try {
      const sections: ParsedReview = { positions: [], overallAssessment: '', grade: '', actionItems: [], newPosition: '', otherSections: [] };
      const parts = message.split(/^## /gm).filter(Boolean);
      if (parts.length < 2) return null;

      for (const part of parts) {
        if (/^INDIVIDUAL\s*POSITIONS?/i.test(part)) {
          const positionBlocks = part.split(/^### /gm).filter(Boolean).slice(1);
          for (const block of positionBlocks) {
            const lines = block.trim().split('\n').filter(l => l.trim());
            const headerLine = lines[0].replace(/\*\*/g, '');
            const headerMatch = headerLine.match(/([A-Z0-9.]+)\s*\((.+?)\)\s*[-–—]\s*(.+)/);
            const position: ParsedPosition = {
              ticker: headerMatch ? headerMatch[1] : headerLine.split(/\s/)[0],
              weight: headerMatch ? headerMatch[2].trim() : '',
              pnl: headerMatch ? headerMatch[3].trim() : '',
              verdict: '',
              thesis: '',
              keyRisk: '',
              catalyst: '',
              positionSize: '',
              extra: [],
            };

            for (const line of lines.slice(1)) {
              const clean = line.replace(/\*\*/g, '').trim();
              if (/^VERDICT:/i.test(clean)) position.verdict = clean.replace(/^VERDICT:\s*/i, '');
              else if (/^THESIS:/i.test(clean)) position.thesis = clean.replace(/^THESIS:\s*/i, '');
              else if (/^KEY RISK:/i.test(clean)) position.keyRisk = clean.replace(/^KEY RISK:\s*/i, '');
              else if (/^CATALYST:/i.test(clean)) position.catalyst = clean.replace(/^CATALYST:\s*/i, '');
              else if (/^POSITION SIZE:/i.test(clean)) position.positionSize = clean.replace(/^POSITION SIZE:\s*/i, '');
              else if (clean) position.extra.push(clean);
            }
            sections.positions.push(position);
          }
        } else if (/^OVERALL/i.test(part)) {
          sections.overallAssessment = part.replace(/^OVERALL\s*ASSESSMENT\s*/i, '').trim();
          const gradeMatch = part.match(/PORTFOLIO GRADE:\s*([A-F][+-]?)/i);
          if (gradeMatch) sections.grade = gradeMatch[1];
        } else if (/ACTION\s*ITEM/i.test(part)) {
          sections.actionItems = part.split('\n').filter(l => l.trim() && !/^#|^ACTION\s*ITEM/i.test(l.trim())).map(l => l.replace(/^\d+[\.\)]\s*/, '').replace(/\*\*/g, '').trim());
        } else if (/NEW\s*POSITION|ADD.*POSITION|SUGGESTED.*ADDITION/i.test(part)) {
          sections.newPosition = part.replace(/^.*?\n/, '').trim();
        } else {
          const titleEnd = part.indexOf('\n');
          if (titleEnd > 0) {
            sections.otherSections.push({ title: part.slice(0, titleEnd).trim(), body: part.slice(titleEnd).trim() });
          }
        }
      }

      return sections.positions.length > 0 ? sections : null;
    } catch {
      return null;
    }
  }

  function getVerdictStyle(verdict: string) {
    const v = verdict.toUpperCase();
    if (v.includes('BUY MORE') || v.includes('STRONG BUY') || v === 'BUY') return { bg: '#059669', text: '#fff' };
    if (v.includes('HOLD')) return { bg: '#475569', text: '#fff' };
    if (v.includes('TRIM')) return { bg: '#d97706', text: '#000' };
    if (v.includes('SELL')) return { bg: '#dc2626', text: '#fff' };
    return { bg: '#475569', text: '#fff' };
  }

  function getGradeColor(grade: string) {
    const g = grade.charAt(0).toUpperCase();
    if (g === 'A') return '#34d399';
    if (g === 'B') return '#60a5fa';
    if (g === 'C') return '#fbbf24';
    if (g === 'D') return '#fb923c';
    if (g === 'F') return '#f87171';
    return '#94a3b8';
  }

  function pnlIsPositive(pnl: string) {
    return pnl.startsWith('+') || (!pnl.startsWith('-') && !pnl.includes('loss'));
  }

  function renderStyledReview(review: ParsedReview): ReactNode {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        {review.positions.length > 0 && (
          <div>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 12 }}>Individual Positions</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))', gap: 10 }}>
              {review.positions.map((pos, i) => {
                const vs = getVerdictStyle(pos.verdict);
                const pnlPositive = pnlIsPositive(pos.pnl);
                return (
                  <div key={i} style={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, overflow: 'hidden' }}>
                    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '10px 14px', borderBottom: '1px solid #1e2148', background: '#0f1129' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                        <span style={{ fontSize: 16, fontWeight: 800, color: '#fff', letterSpacing: '0.02em' }}>{pos.ticker}</span>
                        {pos.weight && <span style={{ fontSize: 11, color: '#94a3b8', fontWeight: 500 }}>{pos.weight}</span>}
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        {pos.pnl && <span style={{ fontSize: 12, fontWeight: 600, color: pnlPositive ? '#34d399' : '#f87171' }}>{pos.pnl}</span>}
                        {pos.verdict && <span style={{ fontSize: 10, fontWeight: 700, padding: '3px 8px', borderRadius: 4, background: vs.bg, color: vs.text, textTransform: 'uppercase', letterSpacing: '0.04em' }}>{pos.verdict}</span>}
                      </div>
                    </div>
                    <div style={{ padding: '10px 14px', display: 'flex', flexDirection: 'column', gap: 8 }}>
                      {pos.thesis && (
                        <div style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.6 }}>{pos.thesis}</div>
                      )}
                      {pos.keyRisk && (
                        <div style={{ borderLeft: '3px solid #ef4444', paddingLeft: 10, fontSize: 12, color: '#fca5a5', lineHeight: 1.5 }}>
                          <span style={{ fontWeight: 700, fontSize: 10, color: '#f87171', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Risk: </span>{pos.keyRisk}
                        </div>
                      )}
                      {pos.catalyst && (
                        <div style={{ borderLeft: '3px solid #10b981', paddingLeft: 10, fontSize: 12, color: '#6ee7b7', lineHeight: 1.5 }}>
                          <span style={{ fontWeight: 700, fontSize: 10, color: '#34d399', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Catalyst: </span>{pos.catalyst}
                        </div>
                      )}
                      {pos.positionSize && (
                        <div style={{ borderLeft: '3px solid #3b82f6', paddingLeft: 10, fontSize: 12, color: '#93c5fd', lineHeight: 1.5 }}>
                          <span style={{ fontWeight: 700, fontSize: 10, color: '#60a5fa', textTransform: 'uppercase', letterSpacing: '0.04em' }}>Size: </span>{pos.positionSize}
                        </div>
                      )}
                      {pos.extra.length > 0 && pos.extra.map((e, j) => (
                        <div key={j} style={{ fontSize: 12, color: '#94a3b8', lineHeight: 1.5 }}>{e}</div>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {review.overallAssessment && (
          <div style={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, padding: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 12 }}>
              <span style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Overall Assessment</span>
              {review.grade && (
                <span style={{ fontSize: 22, fontWeight: 900, color: getGradeColor(review.grade), textShadow: `0 0 20px ${getGradeColor(review.grade)}40` }}>{review.grade}</span>
              )}
            </div>
            <div style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.7, whiteSpace: 'pre-wrap' }}>
              {review.overallAssessment.split('\n').filter(l => l.trim()).map((line, i) => {
                const clean = line.replace(/\*\*/g, '');
                const isGrade = /PORTFOLIO GRADE:/i.test(clean);
                if (isGrade) return null;
                const isBullet = /^[-•*]/.test(clean.trim());
                return (
                  <div key={i} style={{ padding: isBullet ? '3px 0 3px 8px' : '3px 0', borderLeft: isBullet ? '2px solid #3b82f640' : 'none', marginBottom: 2, marginLeft: isBullet ? 4 : 0 }}>
                    {clean.replace(/^[-•*]\s*/, '')}
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {review.actionItems.length > 0 && (
          <div style={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 12 }}>Action Items</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
              {review.actionItems.map((item, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'flex-start', gap: 10, padding: '8px 12px', background: '#0f1129', borderRadius: 6, border: '1px solid #1e2148' }}>
                  <span style={{ fontSize: 13, fontWeight: 800, color: '#60a5fa', minWidth: 20, flexShrink: 0 }}>{i + 1}</span>
                  <span style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.6 }}>{item}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        {review.newPosition && (
          <div style={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#34d399', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 8 }}>Suggested New Position</div>
            <div style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.7, whiteSpace: 'pre-wrap' }}>{review.newPosition.replace(/\*\*/g, '')}</div>
          </div>
        )}

        {review.otherSections.map((sec, i) => (
          <div key={i} style={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, padding: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 10 }}>{sec.title}</div>
            <div style={{ fontSize: 12, color: '#e2e8f0', lineHeight: 1.7, whiteSpace: 'pre-wrap' }}>{sec.body.replace(/\*\*/g, '')}</div>
          </div>
        ))}
      </div>
    );
  }

  const daysUntil = (dateStr: string) => {
    const diff = Math.ceil((new Date(dateStr).getTime() - Date.now()) / (1000 * 60 * 60 * 24));
    return diff;
  };

  const openInNewTab = (url: string) => window.open(url, '_blank', 'noopener,noreferrer');

  return (
    <div className="min-h-screen text-white portfolio-page" style={{ background: 'radial-gradient(ellipse at top center, #0f1129 0%, #0a0b1a 50%, #050510 100%)' }}>
      <style>{`
        .portfolio-page::-webkit-scrollbar { width: 6px; }
        .portfolio-page::-webkit-scrollbar-track { background: #0a0b1a; }
        .portfolio-page::-webkit-scrollbar-thumb { background: #1e2148; border-radius: 3px; }
        .portfolio-page::-webkit-scrollbar-thumb:hover { background: #2d3066; }
      `}</style>
      <main className="max-w-[95vw] mx-auto px-2 sm:px-3 py-4">
        <div className="space-y-4 lg:space-y-6">

          {/* AI Review Button + Summary Stats */}
          <div style={{ borderBottom: '1px solid transparent', borderImage: 'linear-gradient(90deg, transparent, #6366f1, #3b82f6, #06b6d4, transparent) 1', paddingBottom: 16, marginBottom: 24 }}>
          <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <div>
                <h1 style={{ fontSize: '1.75rem', fontWeight: 700, background: 'linear-gradient(135deg, #e2e8f0, #38bdf8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>Portfolio Dashboard</h1>
                {holdings.length > 0 && totalPortfolioValue > 0 && (
                  <div className="flex items-center gap-3 text-sm">
                    <span className="text-crypto-silver">Total: <span style={{ color: '#38bdf8', fontWeight: 700, textShadow: '0 0 10px rgba(56, 189, 248, 0.3)' }}>{fmt(totalPortfolioValue)}</span></span>
                    <span className={totalDailyPL >= 0 ? 'text-green-400' : 'text-red-400'}>
                      Day: {fmtPL(totalDailyPL)}
                    </span>
                    <span className={totalOverallPL >= 0 ? 'text-green-400' : 'text-red-400'}>
                      Total: {fmtPL(totalOverallPL)} ({pctPL(totalOverallPL, totalCostBasis)})
                    </span>
                  </div>
                )}
              </div>
            </div>
            <div className="flex items-center gap-2">
              {holdings.length > 0 && (
                <>
                  <button onClick={() => fetchQuotes(holdings)} disabled={loadingQuotes} className="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 transition-all disabled:opacity-50" style={{ background: 'rgba(99, 102, 241, 0.1)', border: '1px solid rgba(99, 102, 241, 0.3)' }}>
                    <RefreshCw className={`w-3.5 h-3.5 ${loadingQuotes ? 'animate-spin' : ''}`} />
                    Refresh
                  </button>
                  <button onClick={runAIReview} disabled={aiLoading} className="flex items-center gap-1.5 text-sm font-medium text-white transition-all disabled:opacity-50" style={{ background: 'linear-gradient(135deg, #6366f1, #3b82f6, #06b6d4)', boxShadow: '0 0 20px rgba(99, 102, 241, 0.4), 0 0 40px rgba(59, 130, 246, 0.2)', borderRadius: 8, padding: '10px 24px' }}>
                    <Bot className="w-4 h-4" />
                    {aiLoading ? aiStage || 'Analyzing...' : 'AI Portfolio Review'}
                  </button>
                </>
              )}
            </div>
          </div>
          </div>

          {/* AI Review Result */}
          {aiReview && (() => {
            const parsed = parsePortfolioReview(aiReview);
            return (
              <div style={{
                background: "#141632",
                border: "1px solid #1e2148",
                borderRadius: "12px",
                padding: "20px",
                marginTop: "16px",
                boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
              }}>
                <div className="flex items-center gap-2 mb-4">
                  <Bot className="w-5 h-5 text-cyan-400" />
                  <h3 className="text-base font-semibold text-white">AI Portfolio Analysis</h3>
                  <button onClick={() => setAiReview(null)} className="ml-auto text-crypto-silver hover:text-white text-xs">Dismiss</button>
                </div>
                {parsed ? renderStyledReview(parsed) : (
                  <div style={{ lineHeight: 1.7, fontSize: 14, color: '#ccc', whiteSpace: 'pre-wrap' }} dangerouslySetInnerHTML={{ __html: formatAnalysis(aiReview) }} />
                )}
              </div>
            );
          })()}

          {/* Section 1: Portfolio Input */}
          <GlassCard className="p-3 sm:p-4">
            <div className="flex items-center gap-2 mb-3">
              <Plus className="w-4 h-4 text-green-400" />
              <h3 className="text-sm font-semibold text-white">Add Holding</h3>
            </div>
            <div className="flex flex-col sm:flex-row gap-2">
              <input type="text" placeholder="Ticker (e.g. NVDA)" value={newTicker} onChange={e => setNewTicker(e.target.value.toUpperCase())} onKeyDown={e => e.key === 'Enter' && addHolding()} className="rounded-lg px-3 py-2 text-sm text-white placeholder-crypto-silver/50 focus:outline-none focus:border-cyan-500/50 w-full sm:w-36" style={{ background: '#0f1129', border: '1px solid #1e2148' }} />
              <select value={selectedAssetType} onChange={e => setSelectedAssetType(e.target.value)} className="rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-cyan-500/50 w-full sm:w-32 appearance-none cursor-pointer" style={{ background: '#0f1129', border: '1px solid #1e2148', backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2'%3E%3Cpath d='M6 9l6 6 6-6'/%3E%3C/svg%3E\")", backgroundRepeat: 'no-repeat', backgroundPosition: 'right 8px center' }}>
                <option value="stock" style={{ background: '#0f1129' }}>Stock</option>
                <option value="etf" style={{ background: '#0f1129' }}>ETF</option>
                <option value="crypto" style={{ background: '#0f1129' }}>Crypto</option>
                <option value="commodity" style={{ background: '#0f1129' }}>Commodity</option>
              </select>
              <input type="number" placeholder={SHARES_LABEL[selectedAssetType] || 'Shares'} value={newShares} onChange={e => setNewShares(e.target.value)} onKeyDown={e => e.key === 'Enter' && addHolding()} className="rounded-lg px-3 py-2 text-sm text-white placeholder-crypto-silver/50 focus:outline-none focus:border-cyan-500/50 w-full sm:w-28" style={{ background: '#0f1129', border: '1px solid #1e2148' }} />
              <input type="number" placeholder="Avg Cost ($)" value={newAvgCost} onChange={e => setNewAvgCost(e.target.value)} onKeyDown={e => e.key === 'Enter' && addHolding()} className="rounded-lg px-3 py-2 text-sm text-white placeholder-crypto-silver/50 focus:outline-none focus:border-cyan-500/50 w-full sm:w-32" style={{ background: '#0f1129', border: '1px solid #1e2148' }} />
              <button onClick={addHolding} disabled={addingHolding || !newTicker.trim() || !newShares || !newAvgCost} className="flex items-center justify-center gap-1.5 px-4 py-2 rounded-lg text-sm font-medium text-white transition-all disabled:opacity-40" style={{ background: 'linear-gradient(135deg, #6366f1, #3b82f6)', boxShadow: '0 0 12px rgba(99, 102, 241, 0.3)' }}>
                <Plus className="w-4 h-4" />
                Add
              </button>
            </div>
            {newTicker.trim() && INDEX_TO_ETF[newTicker.trim()] && (
              <div className="mt-2 flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-500/10 border border-blue-500/20 text-xs">
                <span className="text-blue-300">
                  {newTicker.trim() === 'VIX'
                    ? <>VIX index data is available but limited. For active trading, consider <strong>{INDEX_TO_ETF[newTicker.trim()].etf}</strong> ({INDEX_TO_ETF[newTicker.trim()].name}) which has full price tracking and charts.</>
                    : <><strong>{newTicker.trim()}</strong> is an index and can't be directly traded. Consider adding <strong>{INDEX_TO_ETF[newTicker.trim()].etf}</strong> ({INDEX_TO_ETF[newTicker.trim()].name}) instead for accurate price tracking.</>
                  }
                </span>
                <button
                  onClick={() => { setNewTicker(INDEX_TO_ETF[newTicker.trim()].etf); setSelectedAssetType('etf'); }}
                  className="ml-auto flex-shrink-0 px-2.5 py-1 bg-blue-500/20 hover:bg-blue-500/30 border border-blue-500/30 rounded text-blue-200 font-medium transition-all"
                >
                  Use {INDEX_TO_ETF[newTicker.trim()].etf}
                </button>
              </div>
            )}
          </GlassCard>

          {/* Holdings Table */}
          {holdings.length > 0 && (
            <GlassCard className="p-3 sm:p-4">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr style={{ borderBottom: '1px solid #1e2148', background: '#0f1129' }}>
                      <th className="pb-2 pr-1 w-6"></th>
                      <th className="text-left pb-2 pr-3"><SortHeader label="Ticker" keyName="ticker" /></th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Shares" keyName="shares" /></th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Avg Cost" keyName="avgCost" /></th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Price" keyName="currentPrice" /></th>
                      <th className="text-right pb-2 px-3" style={{color:'#94a3b8',fontSize:11}}>Invested</th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Daily P&L" keyName="dailyPL" /></th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Total P&L" keyName="totalPL" /></th>
                      <th className="text-right pb-2 px-3"><SortHeader label="Weight%" keyName="weight" /></th>
                      <th className="text-right pb-2 pl-3 w-10"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedHoldings.map(h => {
                      const isExpanded = expandedCard === h.id;
                      const target = priceTargets[h.ticker];
                      const q = h.quote;
                      return (
                        <Fragment key={h.id}>
                          <tr onClick={() => setExpandedCard(isExpanded ? null : h.id)} className="transition-colors cursor-pointer" style={{ borderBottom: '1px solid #1a1c3a' }} onMouseEnter={e => (e.currentTarget.style.background = 'rgba(99, 102, 241, 0.06)')} onMouseLeave={e => (e.currentTarget.style.background = 'transparent')}>
                            <td className="py-2.5 pr-1 w-6">
                              {isExpanded ? <ChevronDown className="w-3.5 h-3.5" style={{ color: '#38bdf8' }} /> : <ChevronRight className="w-3.5 h-3.5 text-crypto-silver hover:text-[#38bdf8]" />}
                            </td>
                            <td className="py-2.5 pr-3">
                              <div style={{ color: '#e2e8f0', fontWeight: 700, fontSize: '1rem' }}>{h.ticker}</div>
                              <div className="truncate max-w-[120px]" style={{ color: '#64748b', fontSize: '0.8rem' }}>{getDisplayName(h.ticker, h.assetType, h.quote?.companyName || h.quote?.name)}</div>
                            </td>
                            <td className="text-right py-2.5 px-3 text-crypto-silver">{h.shares}</td>
                            <td className="text-right py-2.5 px-3 text-crypto-silver">{fmt(h.avgCost)}</td>
                            <td className="text-right py-2.5 px-3" style={{ color: '#38bdf8', fontWeight: 600 }}>
                              {loadingQuotes && !h.currentPrice ? <span className="animate-pulse text-crypto-silver">Loading...</span> : quotesError && !h.currentPrice ? <span className="text-yellow-500 text-xs">Unavailable</span> : h.currentPrice > 0 ? fmt(h.currentPrice) : <span className="text-crypto-silver/50">—</span>}
                            </td>
                            <td className="text-right py-2.5 px-3 font-medium" style={{ color: '#a78bfa' }}>
                              {fmt(h.avgCost * h.shares)}
                            </td>
                            <td className={`text-right py-2.5 px-3 font-medium ${h.dailyPL >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                              {loadingQuotes && !h.currentPrice ? <span className="animate-pulse text-crypto-silver">...</span> : quotesError && !h.currentPrice ? <span className="text-yellow-500 text-xs">—</span> : h.currentPrice > 0 ? fmtPL(h.dailyPL) : <span className="text-crypto-silver/50">—</span>}
                            </td>
                            <td className={`text-right py-2.5 px-3 font-medium ${h.totalPL >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                              {loadingQuotes && !h.currentPrice ? <span className="animate-pulse text-crypto-silver">...</span> : quotesError && !h.currentPrice ? <span className="text-yellow-500 text-xs">—</span> : h.currentPrice > 0 ? <><div>{fmtPL(h.totalPL)}</div><div className="text-[10px] opacity-70">{pctPL(h.totalPL, h.avgCost * h.shares)}</div></> : <span className="text-crypto-silver/50">—</span>}
                            </td>
                            <td className="text-right py-2.5 px-3 text-crypto-silver">
                              {totalPortfolioValue > 0 ? ((h.totalValue / totalPortfolioValue) * 100).toFixed(1) + '%' : <span className="text-crypto-silver/50">—</span>}
                            </td>
                            <td className="text-right py-2.5 pl-3">
                              <button onClick={(e) => { e.stopPropagation(); deleteHolding(h.id); }} className="opacity-50 hover:opacity-100 transition-all p-1" style={{ color: '#475569' }} onMouseEnter={e => (e.currentTarget.style.color = '#ef4444')} onMouseLeave={e => (e.currentTarget.style.color = '#475569')}>
                                <Trash2 className="w-3.5 h-3.5" />
                              </button>
                            </td>
                          </tr>
                          {isExpanded && (
                            <tr style={{ borderBottom: '1px solid #1a1c3a' }}>
                              <td colSpan={9} className="p-0">
                                <div style={{ background: '#0f1129', borderTop: '1px solid #1e2148', padding: '12px' }}>
                                  <div className="flex gap-1 mb-1.5">
                                    {[{l:'1H',v:'60'},{l:'4H',v:'240'},{l:'1D',v:'D'},{l:'1W',v:'W'},{l:'1M',v:'M'}].map(iv => (
                                      <button key={iv.v} onClick={(e) => { e.stopPropagation(); setPortfolioChartInterval(iv.v); }}
                                        className="text-[9px] font-semibold px-2 py-0.5 rounded cursor-pointer transition-colors"
                                        style={{ background: portfolioChartInterval === iv.v ? 'rgba(56, 189, 248, 0.12)' : 'transparent', color: portfolioChartInterval === iv.v ? '#38bdf8' : '#64748b', border: `1px solid ${portfolioChartInterval === iv.v ? 'rgba(56, 189, 248, 0.25)' : '#1e2148'}` }}
                                      >{iv.l}</button>
                                    ))}
                                  </div>
                                  <div className="rounded-lg overflow-hidden my-2" style={{ border: '1px solid rgba(56, 78, 119, 0.2)' }}>
                                    <iframe
                                      src={`https://s.tradingview.com/widgetembed/?symbol=${encodeURIComponent(getTradingViewSymbol(h.ticker, h.assetType, h.quote?.tradingview_symbol))}&interval=${portfolioChartInterval}&theme=dark&style=1&locale=en&hide_top_toolbar=1&hide_side_toolbar=1&allow_symbol_change=0&save_image=0&width=100%25&height=220`}
                                      style={{ width: '100%', height: 220, border: 'none', display: 'block' }}
                                      title={`${h.ticker} chart`}
                                    />
                                  </div>
                                  <div className="flex flex-wrap gap-2.5 mt-3">
                                    {q?.changesPercentage != null && (
                                      <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                        <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Price Change</div>
                                        <div className={`text-sm font-semibold ${q.changesPercentage >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                                          {q.changesPercentage >= 0 ? '+' : ''}{q.changesPercentage.toFixed(2)}%
                                        </div>
                                      </div>
                                    )}
                                    {(h.assetType === 'stock' || h.assetType === 'crypto' || !h.assetType) && q?.marketCap != null && q.marketCap > 0 && (
                                      <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                        <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Market Cap</div>
                                        <div className="text-sm font-semibold text-white">
                                          ${q.marketCap >= 1e12 ? (q.marketCap / 1e12).toFixed(1) + 'T' : q.marketCap >= 1e9 ? (q.marketCap / 1e9).toFixed(1) + 'B' : q.marketCap >= 1e6 ? (q.marketCap / 1e6).toFixed(1) + 'M' : q.marketCap.toLocaleString()}
                                        </div>
                                      </div>
                                    )}
                                    {q?.volume != null && q.volume > 0 && (
                                      <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                        <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Volume</div>
                                        <div className="text-sm font-semibold text-white">
                                          {q.volume >= 1e9 ? (q.volume / 1e9).toFixed(1) + 'B' : q.volume >= 1e6 ? (q.volume / 1e6).toFixed(1) + 'M' : q.volume >= 1e3 ? (q.volume / 1e3).toFixed(1) + 'K' : q.volume.toLocaleString()}
                                        </div>
                                      </div>
                                    )}
                                    {(() => {
                                      const sectorLabel = h.assetType === 'crypto' ? 'Crypto' : h.assetType === 'commodity' ? 'Commodities' : h.assetType === 'etf' ? (q?.sector && q.sector !== 'Unknown' ? q.sector : 'ETFs') : (q?.sector && q.sector !== 'Unknown' ? q.sector : null);
                                      return sectorLabel ? (
                                        <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                          <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Sector</div>
                                          <div className="text-sm font-semibold text-white">{sectorLabel}</div>
                                        </div>
                                      ) : null;
                                    })()}
                                    {(h.assetType === 'stock' || !h.assetType) && q?.pe != null && q.pe > 0 && (
                                      <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                        <div className="text-[10px] text-crypto-silver uppercase tracking-wider">P/E Ratio</div>
                                        <div className="text-sm font-semibold text-white">{q.pe.toFixed(1)}</div>
                                      </div>
                                    )}
                                    {(h.assetType === 'stock' || !h.assetType) && q?.eps != null && q.eps !== 0 && (
                                      <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                        <div className="text-[10px] text-crypto-silver uppercase tracking-wider">EPS</div>
                                        <div className="text-sm font-semibold text-white">${q.eps.toFixed(2)}</div>
                                      </div>
                                    )}
                                    {(h.assetType === 'stock' || !h.assetType) && target && (
                                      <>
                                        <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                          <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Target Consensus</div>
                                          <div className={`text-sm font-semibold ${target.targetConsensus > h.currentPrice ? 'text-green-400' : 'text-red-400'}`}>
                                            ${target.targetConsensus?.toFixed(2)}
                                          </div>
                                          <div className="text-[10px] text-crypto-silver">
                                            {h.currentPrice > 0 ? ((((target.targetConsensus - h.currentPrice) / h.currentPrice) * 100).toFixed(1) + '% upside') : ''}
                                          </div>
                                        </div>
                                        <div className="rounded-lg px-3 py-2" style={{ background: 'rgba(99, 102, 241, 0.06)', border: '1px solid #1e2148' }}>
                                          <div className="text-[10px] text-crypto-silver uppercase tracking-wider">Target Range</div>
                                          <div className="text-sm font-semibold text-white">${target.targetLow?.toFixed(0)} – ${target.targetHigh?.toFixed(0)}</div>
                                        </div>
                                      </>
                                    )}
                                  </div>
                                </div>
                              </td>
                            </tr>
                          )}
                        </Fragment>
                      );
                    })}
                  </tbody>
                  {totalPortfolioValue > 0 && (
                    <tfoot>
                      <tr style={{ borderTop: '2px solid #1e2148', background: '#0f1129' }}>
                        <td></td>
                        <td colSpan={3} className="py-3 text-right text-xs text-crypto-silver font-medium">TOTAL</td>
                        <td className="text-right py-3 px-3 font-bold" style={{ color: '#38bdf8', textShadow: '0 0 8px rgba(56, 189, 248, 0.2)' }}>{fmt(totalPortfolioValue)}</td>
                        <td className="text-right py-3 px-3 font-bold" style={{ color: '#a78bfa' }}>{fmt(totalCostBasis)}</td>
                        <td className={`text-right py-3 px-3 font-bold ${totalDailyPL >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmtPL(totalDailyPL)}</td>
                        <td className={`text-right py-3 px-3 font-bold ${totalOverallPL >= 0 ? 'text-green-400' : 'text-red-400'}`}>{fmtPL(totalOverallPL)}</td>
                        <td></td>
                        <td></td>
                      </tr>
                    </tfoot>
                  )}
                </table>
              </div>
            </GlassCard>
          )}

          {/* Section 2: Portfolio Visualization */}
          {holdings.length > 0 && totalPortfolioValue > 0 && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <GlassCard className="p-3 sm:p-4">
                <h3 style={{ color: '#e2e8f0', fontWeight: 600, fontSize: '1.1rem', marginBottom: 16 }}>Asset Allocation</h3>
                <div className="h-[280px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={sectorData} cx="50%" cy="50%" innerRadius={55} outerRadius={95} paddingAngle={2} dataKey="value" nameKey="name" label={({ name, pct }) => `${name} ${pct}%`} labelLine={false}>
                        {sectorData.map((entry) => (
                          <Cell key={entry.name} fill={entry.color} />
                        ))}
                      </Pie>
                      <Tooltip formatter={(value: number) => fmt(value)} contentStyle={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, color: '#e2e8f0', fontSize: 12 }} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              </GlassCard>

              <GlassCard className="p-3 sm:p-4">
                <h3 style={{ color: '#e2e8f0', fontWeight: 600, fontSize: '1.1rem', marginBottom: 16 }}>Daily P&L by Position</h3>
                <div className="h-[280px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={plBarData} layout="vertical" margin={{ left: 10, right: 20 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#1e2148" />
                      <XAxis type="number" tick={{ fill: '#64748b', fontSize: 11 }} tickFormatter={(v) => `$${v}`} />
                      <YAxis type="category" dataKey="ticker" tick={{ fill: '#e2e8f0', fontSize: 11, fontWeight: 600 }} width={50} />
                      <Tooltip formatter={(value: number) => fmt(value)} contentStyle={{ background: '#141632', border: '1px solid #1e2148', borderRadius: 8, color: '#e2e8f0', fontSize: 12 }} />
                      <Bar dataKey="dailyPL" radius={[0, 4, 4, 0]}>
                        {plBarData.map((entry, i) => (
                          <Cell key={i} fill={entry.fill} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </GlassCard>
            </div>
          )}

          {/* Section 3: Upcoming Events */}
          {allEvents.length > 0 && (
            <GlassCard className="p-3 sm:p-4">
              <div className="flex items-center gap-2 mb-3">
                <Calendar className="w-4 h-4 text-blue-400" />
                <h3 className="text-sm font-semibold text-white">Upcoming Events</h3>
              </div>
              <div className="space-y-2">
                {allEvents.slice(0, 20).map((evt, i) => {
                  const days = daysUntil(evt.date);
                  const isUrgent = days <= 7;
                  const isEarnings = evt.type === 'earnings';
                  return (
                    <div key={i} className={`flex items-center gap-3 p-2.5 rounded-lg border ${isEarnings ? (isUrgent ? 'border-orange-500/30 bg-orange-500/5' : 'border-orange-500/10 bg-orange-500/5') : (isUrgent ? 'border-blue-500/30 bg-blue-500/5' : 'border-blue-500/10 bg-blue-500/5')}`}>
                      <div className={`w-2 h-2 rounded-full ${isEarnings ? 'bg-orange-400' : 'bg-blue-400'}`} />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="font-semibold text-white text-sm">{evt.symbol}</span>
                          <span className={`text-[10px] px-1.5 py-0.5 rounded uppercase font-medium ${isEarnings ? 'bg-orange-500/20 text-orange-400' : 'bg-blue-500/20 text-blue-400'}`}>
                            {evt.type}
                          </span>
                          {isUrgent && <span className="text-[10px] px-1.5 py-0.5 rounded bg-red-500/20 text-red-400 uppercase font-medium">Soon</span>}
                        </div>
                        <div className="text-xs text-crypto-silver mt-0.5">{evt.detail}</div>
                      </div>
                      <div className="text-right">
                        <div className="text-xs text-crypto-silver">{new Date(evt.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}</div>
                        <div className="text-[10px] text-crypto-silver">{days === 0 ? 'Today' : days === 1 ? 'Tomorrow' : `${days}d`}</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </GlassCard>
          )}

          {/* Empty State */}
          {holdings.length === 0 && (
            <GlassCard className="p-8 text-center">
              <Briefcase className="w-12 h-12 text-slate-600 mx-auto mb-3" />
              <h3 className="text-lg font-semibold text-white mb-2">No Holdings Yet</h3>
              <p className="text-sm text-crypto-silver mb-4">Add your first stock holding above to start tracking your portfolio with real-time data, charts, and AI analysis.</p>
            </GlassCard>
          )}

          {/* Section 6: Quick Links */}
          <div className="flex flex-wrap items-center justify-center gap-2 pt-2 pb-4">
            {[
              { name: 'Schwab', url: 'https://client.schwab.com/clientapps/accounts/summary/' },
              { name: 'Robinhood', url: 'https://robinhood.com/us/en/' },
              { name: 'Yahoo Finance', url: 'https://finance.yahoo.com/portfolios' },
              { name: 'Empower', url: 'https://home.personalcapital.com/page/login/app#/dashboard' },
              { name: 'Snowball', url: 'https://snowball-analytics.com/dashboard' },
              { name: 'Simply Wall St', url: 'https://simplywall.st/portfolio/65b1f9ab-7fa4-4d25-95c6-b8fa93d94d77/holdings' },
            ].map(link => (
              <button key={link.name} onClick={() => openInNewTab(link.url)} className="transition-all duration-300 text-xs hover:text-white flex items-center gap-1.5" style={{ background: 'rgba(99, 102, 241, 0.08)', border: '1px solid #1e2148', color: '#94a3b8', borderRadius: 8, padding: '8px 16px' }}>
                <ExternalLink className="w-3 h-3" />
                {link.name}
              </button>
            ))}
          </div>

        </div>
      </main>
    </div>
  );
}
