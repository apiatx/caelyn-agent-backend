import { useState, useEffect, useCallback, useRef } from 'react';
import { Newspaper, Send, Loader2, MessageSquare, ExternalLink, Clock, RefreshCw, Sparkles } from 'lucide-react';
import { openSecureLink } from '@/utils/security';
import { Button } from '@/components/ui/button';

// ─── Constants ────────────────────────────────────────────────────

const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";

function getToken(): string | null {
  return localStorage.getItem('caelyn_token') || sessionStorage.getItem('caelyn_token');
}

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json', 'X-API-Key': AGENT_API_KEY, ...extra };
  const t = getToken();
  if (t) h['Authorization'] = `Bearer ${t}`;
  return h;
}

const CATEGORIES = [
  { id: 'finance', label: 'Finance' },
  { id: 'crypto', label: 'Crypto' },
  { id: 'politics', label: 'Politics' },
  { id: 'world', label: 'World' },
] as const;

type Category = typeof CATEGORIES[number]['id'];

interface NewsArticle {
  title: string;
  description: string;
  source: string;
  url: string;
  published: string | number;
  image: string;
  symbol?: string;
}

interface AgentMessage {
  role: 'user' | 'assistant';
  content: string;
  timestamp: number;
}

const NEWS_SUGGESTED_PROMPTS = [
  "How are today's top headlines likely to impact the S&P 500 and crypto markets this week?",
  "What geopolitical risks in the news right now pose the biggest threat to my portfolio?",
  "Which sectors benefit most from the current news cycle and policy trends?",
  "Are there any breaking news catalysts that could trigger a major market move today?",
  "How should I position around the current tariff and trade policy developments?",
];

// ─── Time Formatting ──────────────────────────────────────────────

function formatTime(published: string | number): string {
  if (!published) return '';
  let date: Date;
  if (typeof published === 'number') {
    // Unix timestamp (seconds or ms)
    date = new Date(published > 1e12 ? published : published * 1000);
  } else {
    date = new Date(published);
  }
  if (isNaN(date.getTime())) return '';
  const now = Date.now();
  const diff = now - date.getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'Just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function extractDomain(url: string): string {
  try {
    const hostname = new URL(url).hostname;
    return hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

// ─── News Feed Component ──────────────────────────────────────────

function NewsFeed() {
  const [category, setCategory] = useState<Category>('finance');
  const [articles, setArticles] = useState<NewsArticle[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchNews = useCallback(async (cat: Category) => {
    setLoading(true);
    setError('');
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 20000);
      const url = `/api/proxy/news/feed?category=${cat}`;
      console.log('[NOTIFAI] Fetching:', url);
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timeout);
      console.log('[NOTIFAI] Response status:', res.status);
      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`HTTP ${res.status}: ${body.slice(0, 200)}`);
      }
      const data = await res.json();
      const arts = data.articles || [];
      setArticles(arts);
      if (arts.length === 0) setError('No articles found. News sources may be loading — try refreshing.');
    } catch (err: any) {
      console.error('[NOTIFAI] Fetch error:', err);
      if (err.name === 'AbortError') {
        setError('Request timed out. Please try again.');
      } else {
        setError('Failed to load news. Please try again.');
      }
      setArticles([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchNews(category);
  }, [category, fetchNews]);

  return (
    <div className="flex-1 min-w-0">
      {/* Category Tabs */}
      <div className="flex items-center gap-2 mb-5">
        {CATEGORIES.map((cat) => (
          <button
            key={cat.id}
            onClick={() => setCategory(cat.id)}
            className="px-4 py-2 rounded-lg text-xs font-semibold tracking-wide uppercase transition-all"
            style={{
              background: category === cat.id ? 'rgba(92,200,240,0.12)' : 'rgba(255,255,255,0.03)',
              border: `1px solid ${category === cat.id ? 'rgba(92,200,240,0.3)' : 'rgba(255,255,255,0.06)'}`,
              color: category === cat.id ? '#5cc8f0' : 'rgba(255,255,255,0.4)',
            }}
          >
            {cat.label}
          </button>
        ))}
        <button
          onClick={() => fetchNews(category)}
          disabled={loading}
          className="ml-auto p-2 rounded-lg transition-all hover:bg-white/[0.05]"
          style={{ color: 'rgba(255,255,255,0.3)' }}
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* Error State */}
      {error && (
        <div className="text-center py-8 text-white/40 text-sm">{error}</div>
      )}

      {/* Loading State */}
      {loading && articles.length === 0 && (
        <div className="flex items-center justify-center py-12 gap-2 text-white/30 text-sm">
          <Loader2 className="w-4 h-4 animate-spin" />
          Loading {category} news...
        </div>
      )}

      {/* Articles Grid */}
      {!loading && articles.length === 0 && !error && (
        <div className="text-center py-12 text-white/30 text-sm">
          No articles found for this category.
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {articles.map((article, i) => (
          <NewsCard key={`${article.title}-${i}`} article={article} />
        ))}
      </div>
    </div>
  );
}

// ─── News Card ────────────────────────────────────────────────────

function NewsCard({ article }: { article: NewsArticle }) {
  const [imgError, setImgError] = useState(false);
  const domain = extractDomain(article.url);
  const timeStr = formatTime(article.published);

  return (
    <div
      className="group rounded-xl overflow-hidden transition-all duration-200 hover:border-white/[0.12] cursor-pointer"
      style={{
        background: 'rgba(255,255,255,0.02)',
        border: '1px solid rgba(255,255,255,0.06)',
      }}
      onClick={() => article.url && openSecureLink(article.url)}
    >
      {/* Image */}
      {article.image && !imgError ? (
        <div className="relative h-40 overflow-hidden">
          <img
            src={article.image}
            alt=""
            className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
            onError={() => setImgError(true)}
            loading="lazy"
          />
          <div className="absolute inset-0" style={{ background: 'linear-gradient(to top, rgba(5,6,8,0.8) 0%, transparent 60%)' }} />
          {article.symbol && (
            <span className="absolute top-2 right-2 text-[10px] font-bold px-2 py-0.5 rounded"
              style={{ background: 'rgba(92,200,240,0.2)', color: '#5cc8f0', border: '1px solid rgba(92,200,240,0.3)' }}>
              ${article.symbol}
            </span>
          )}
        </div>
      ) : (
        <div className="h-28 flex items-center justify-center" style={{ background: 'rgba(255,255,255,0.015)' }}>
          <Newspaper className="w-8 h-8" style={{ color: 'rgba(255,255,255,0.06)' }} />
        </div>
      )}

      {/* Content */}
      <div className="p-4">
        <h3 className="text-sm font-semibold text-white/90 leading-snug mb-2 line-clamp-2 group-hover:text-white transition-colors">
          {article.title}
        </h3>
        {article.description && (
          <p className="text-xs text-white/35 leading-relaxed mb-3 line-clamp-2">
            {article.description}
          </p>
        )}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2 text-[10px] text-white/25">
            {domain && <span>{domain}</span>}
            {timeStr && (
              <>
                <span>·</span>
                <span className="flex items-center gap-1">
                  <Clock className="w-2.5 h-2.5" />
                  {timeStr}
                </span>
              </>
            )}
          </div>
          <span className="text-[10px] text-white/25 group-hover:text-[#5cc8f0] transition-colors flex items-center gap-1">
            Read <ExternalLink className="w-2.5 h-2.5" />
          </span>
        </div>
      </div>
    </div>
  );
}

// ─── News Intelligence Agent ──────────────────────────────────────

function NewsAgent() {
  const [messages, setMessages] = useState<AgentMessage[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = useCallback(async (text: string) => {
    if (!text.trim() || loading) return;
    const userMsg: AgentMessage = { role: 'user', content: text.trim(), timestamp: Date.now() };
    setMessages((prev) => [...prev, userMsg]);
    setInput('');
    setLoading(true);

    try {
      const history = messages.map((m) => ({ role: m.role, content: m.content }));
      const payload: Record<string, unknown> = {
        query: text.trim(),
        preset_intent: 'news_intelligence',
        history: history.length > 0 ? history : undefined,
        conversation_id: conversationId,
      };

      const res = await fetch(`${AGENT_BACKEND_URL}/api/query`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => '');
        throw new Error(`Backend returned ${res.status}: ${errText.slice(0, 200)}`);
      }

      const rawText = (await res.text()).trim();
      const data = JSON.parse(rawText);
      const convId = data.conversation_id || conversationId;
      if (convId) setConversationId(convId);

      let analysisText = '';
      if (data.analysis) analysisText = data.analysis;
      else if (data.structured?.message) analysisText = data.structured.message;
      else if (data.structured?.analysis) analysisText = data.structured.analysis;
      else if (typeof data.message === 'string') analysisText = data.message;
      else analysisText = 'Received response but could not extract analysis.';

      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: analysisText, timestamp: Date.now() },
      ]);
      // Auto-save to history (fire-and-forget)
      fetch(`${AGENT_BACKEND_URL}/api/history`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ category: 'news_intelligence', intent: 'news_intelligence', content: analysisText }),
      }).catch(() => {});
    } catch (err) {
      console.error('[NEWS_AGENT]', err);
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: `Error: ${err instanceof Error ? err.message : 'Failed to reach agent.'}`,
          timestamp: Date.now(),
        },
      ]);
    } finally {
      setLoading(false);
    }
  }, [loading, messages, conversationId]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendMessage(input);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage(input);
    }
  };

  return (
    <div className="w-full lg:w-[380px] xl:w-[420px] flex-shrink-0">
      <div className="rounded-xl p-5 sticky top-4" style={{
        background: 'rgba(255,255,255,0.02)',
        border: '1px solid rgba(255,255,255,0.06)',
        boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.03), 0 2px 16px rgba(0,0,0,0.3)',
      }}>
        {/* Header */}
        <div className="flex items-center gap-3 mb-4">
          <div className="w-8 h-8 rounded-lg flex items-center justify-center"
            style={{ background: 'linear-gradient(135deg, #2090d0, #5cc8f0, #80d8f8)' }}>
            <Sparkles className="w-4 h-4 text-white" />
          </div>
          <div>
            <h2 className="text-sm font-bold text-white flex items-center gap-2">
              Ask Caelyn
            </h2>
            <p className="text-[10px] text-white/25">
              News impact analysis &amp; market implications
            </p>
          </div>
        </div>

        {/* Suggested Prompts */}
        {messages.length === 0 && (
          <div className="flex flex-col gap-2 mb-4">
            {NEWS_SUGGESTED_PROMPTS.map((prompt) => (
              <button
                key={prompt}
                onClick={() => sendMessage(prompt)}
                disabled={loading}
                className="text-left text-[11px] text-white/45 bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2.5 hover:bg-white/[0.06] hover:text-white/65 hover:border-white/10 transition-all disabled:opacity-40"
              >
                <MessageSquare className="w-3 h-3 inline mr-1.5 opacity-40" />
                {prompt}
              </button>
            ))}
          </div>
        )}

        {/* Messages */}
        {messages.length > 0 && (
          <div className="mb-4 max-h-[500px] overflow-y-auto space-y-3 scrollbar-hide">
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`rounded-lg px-3.5 py-3 text-xs leading-relaxed ${
                  msg.role === 'user'
                    ? 'bg-[rgba(32,144,208,0.1)] border border-[rgba(32,144,208,0.2)] text-blue-100'
                    : 'bg-white/[0.03] border border-white/[0.06] text-white/80'
                }`}
              >
                <div className="flex items-center gap-1.5 mb-1.5">
                  <span className={`text-[9px] font-bold uppercase tracking-wider ${
                    msg.role === 'user' ? 'text-[#5cc8f0]' : 'text-[#80d8f8]'
                  }`}>
                    {msg.role === 'user' ? 'You' : 'Caelyn'}
                  </span>
                </div>
                <div className="whitespace-pre-wrap">{msg.content}</div>
              </div>
            ))}
            {loading && (
              <div className="rounded-lg px-3.5 py-3 bg-white/[0.03] border border-white/[0.06] text-xs text-white/40">
                <div className="flex items-center gap-2">
                  <Loader2 className="w-3 h-3 animate-spin" />
                  Analyzing current news and market implications...
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}

        {/* Input */}
        <form onSubmit={handleSubmit} className="flex gap-2">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about news impact on markets..."
            disabled={loading}
            rows={1}
            className="flex-1 bg-white/[0.04] border border-white/[0.08] rounded-lg px-3 py-2.5 text-xs text-white placeholder-white/25 resize-none focus:outline-none focus:border-white/20 focus:ring-1 focus:ring-white/10 disabled:opacity-40 transition-all"
          />
          <Button
            type="submit"
            disabled={loading || !input.trim()}
            className="text-white px-3 py-2 rounded-lg transition-all disabled:opacity-30 flex-shrink-0"
            style={{ background: 'linear-gradient(135deg, #2090d0, #5cc8f0, #80d8f8)' }}
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Send className="w-4 h-4" />}
          </Button>
        </form>

        {/* Clear */}
        {messages.length > 0 && (
          <button
            onClick={() => { setMessages([]); setConversationId(null); }}
            className="mt-2 text-[9px] text-white/20 hover:text-white/40 transition-colors"
          >
            Clear conversation
          </button>
        )}
      </div>
    </div>
  );
}

// ─── Main Page ────────────────────────────────────────────────────

export default function NotifAIPage() {
  return (
    <div className="min-h-screen text-white relative" style={{ background: '#050608', fontFamily: "'Outfit', sans-serif" }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
        .line-clamp-2 { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
      `}</style>

      {/* Background gradient */}
      <div style={{
        position: 'fixed', top: '-40%', left: '-20%', width: '140%', height: '140%',
        background: 'radial-gradient(ellipse 800px 600px at 20% 15%, rgba(32,144,208,0.04) 0%, transparent 70%), radial-gradient(ellipse 600px 500px at 80% 70%, rgba(92,200,240,0.03) 0%, transparent 70%)',
        pointerEvents: 'none', zIndex: 0,
      }} />

      {/* Header */}
      <div className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8 pt-6 pb-2 relative" style={{ zIndex: 1 }}>
        <div className="flex items-center gap-4 mb-1">
          <div className="w-10 h-10 rounded-xl flex items-center justify-center"
            style={{ background: 'linear-gradient(135deg, #2090d0, #5cc8f0)', boxShadow: '0 0 20px rgba(92,200,240,0.2)' }}>
            <Newspaper className="w-5 h-5 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold" style={{
              background: 'linear-gradient(135deg, #e2e8f0, #5cc8f0)',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
            }}>
              NotifAI
            </h1>
            <p className="text-xs text-white/30">Real-time market news intelligence</p>
          </div>
        </div>
        <div className="w-24 h-0.5 rounded-full mt-3 mb-4" style={{ background: 'linear-gradient(135deg, #2090d0, #5cc8f0, #80d8f8)' }} />
      </div>

      {/* Main Content — News Grid + Agent Sidebar */}
      <main className="max-w-[1600px] mx-auto px-4 sm:px-6 lg:px-8 pb-8 relative" style={{ zIndex: 1 }}>
        <div className="flex flex-col lg:flex-row gap-6">
          <NewsFeed />
          <NewsAgent />
        </div>
      </main>
    </div>
  );
}
