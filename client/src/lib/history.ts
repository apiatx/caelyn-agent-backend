export interface HistoryEntry {
  id: string;
  timestamp: number;
  content: string;
  display_type: string | null;
  query?: string;
  model_used?: string;
  tickers?: { ticker: string; rec_price: number | null; current_price?: number | null; pct_change?: number | null }[];
  conversation?: { role: string; content: string }[];
}

export interface HistoryBucket {
  category: string;
  intent: string;
  entries: HistoryEntry[];
}

export interface NormalizedHistoryEntry extends HistoryEntry {
  key: string;
  category: string;
  intent: string;
  // Populated from sidebar/conversation endpoints
  conversation_id?: string;
  title?: string;
}

export type HistoryApiResponse = Record<string, Partial<HistoryBucket> | null | undefined>;

function toSafeTimestamp(input: unknown): number {
  const n = typeof input === 'number' ? input : Number(input);
  return Number.isFinite(n) ? n : 0;
}

export function normalizeHistoryBuckets(apiData: unknown): Record<string, HistoryBucket> {
  const source = (apiData && typeof apiData === 'object') ? (apiData as HistoryApiResponse) : {};
  const out: Record<string, HistoryBucket> = {};

  for (const [rawKey, rawBucket] of Object.entries(source)) {
    if (!rawBucket || typeof rawBucket !== 'object') continue;

    const key = String(rawKey || 'unknown::unknown');
    const [keyCategory, keyIntent] = key.split('::');
    const category = String(rawBucket.category || keyCategory || 'unknown');
    const intent = String(rawBucket.intent || keyIntent || 'unknown');

    const entriesSource = Array.isArray(rawBucket.entries) ? rawBucket.entries : [];
    const entries: HistoryEntry[] = entriesSource
      .filter((entry): entry is Record<string, unknown> => !!entry && typeof entry === 'object')
      .map((entry) => ({
        id: String(entry.id || `${category}-${intent}-${toSafeTimestamp(entry.timestamp)}`),
        timestamp: toSafeTimestamp(entry.timestamp),
        content: typeof entry.content === 'string' ? entry.content : '',
        display_type: entry.display_type == null ? null : String(entry.display_type),
        query: typeof entry.query === 'string' ? entry.query : undefined,
        model_used: typeof entry.model_used === 'string' ? entry.model_used : undefined,
        tickers: Array.isArray(entry.tickers) ? entry.tickers as any : undefined,
        conversation: Array.isArray(entry.conversation) ? entry.conversation as any : undefined,
      }))
      .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));

    out[`${category}::${intent}`] = { category, intent, entries };
  }

  return out;
}

export function normalizeHistory(apiData: unknown): NormalizedHistoryEntry[] {
  const buckets = normalizeHistoryBuckets(apiData);
  const flat: NormalizedHistoryEntry[] = [];

  for (const [key, bucket] of Object.entries(buckets)) {
    for (const entry of bucket.entries) {
      flat.push({ ...entry, key, category: bucket.category, intent: bucket.intent });
    }
  }

  return flat.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
}

/**
 * Normalizes the NEW backend format: { items: [...], recent: [...], total_count: N }
 * into the bucket structure the HistoryPanel expects.
 */
export function normalizeNewHistoryApiResponse(apiData: unknown): Record<string, HistoryBucket> {
  if (!apiData || typeof apiData !== 'object') return {};
  const data = apiData as any;

  const items: any[] = Array.isArray(data.items)
    ? data.items
    : Array.isArray(data.recent)
    ? data.recent
    : [];

  const out: Record<string, HistoryBucket> = {};

  for (const item of items) {
    if (!item || typeof item !== 'object') continue;
    const category = String(item.category || 'terminal');
    const intent = String(item.intent || 'freeform_query');
    const key = `${category}::${intent}`;
    if (!out[key]) out[key] = { category, intent, entries: [] };

    const ts = toSafeTimestamp(item.timestamp);
    out[key].entries.push({
      id: String(item.id || `${category}-${intent}-${ts}-${Math.random()}`),
      timestamp: ts,
      content: typeof item.content === 'string' ? item.content : '',
      display_type: item.display_type != null ? String(item.display_type) : null,
      query: typeof item.query === 'string' ? item.query : undefined,
      model_used: typeof item.model_used === 'string' ? item.model_used : undefined,
      tickers: Array.isArray(item.tickers) ? item.tickers as any : undefined,
      conversation_id: typeof item.conversation_id === 'string' ? item.conversation_id : undefined,
    } as any);
  }

  for (const bucket of Object.values(out)) {
    bucket.entries.sort((a, b) => b.timestamp - a.timestamp);
  }

  return out;
}

/**
 * Flattens the new { items: [...] } format directly into NormalizedHistoryEntry[].
 * Use this in TradingAgent for the recent history sidebar.
 */
export function normalizeNewHistoryFlat(apiData: unknown): NormalizedHistoryEntry[] {
  if (!apiData || typeof apiData !== 'object') return [];
  const data = apiData as any;

  const items: any[] = Array.isArray(data.items)
    ? data.items
    : Array.isArray(data.recent)
    ? data.recent
    : [];

  return items.map((item: any) => {
    const category = String(item.category || 'terminal');
    const intent = String(item.intent || 'freeform_query');
    const ts = toSafeTimestamp(item.timestamp);
    return {
      id: String(item.id || `${category}-${intent}-${ts}`),
      timestamp: ts,
      content: typeof item.content === 'string' ? item.content : '',
      display_type: item.display_type != null ? String(item.display_type) : null,
      query: typeof item.query === 'string' ? item.query : undefined,
      model_used: typeof item.model_used === 'string' ? item.model_used : undefined,
      tickers: Array.isArray(item.tickers) ? item.tickers as any : undefined,
      conversation_id: typeof item.conversation_id === 'string' ? item.conversation_id : undefined,
      key: `${category}::${intent}`,
      category,
      intent,
    };
  }).sort((a, b) => b.timestamp - a.timestamp);
}

/**
 * Normalizes the GET /api/history/sidebar response.
 * Expected shape: { conversations: [...] } or [ ...conversations ] directly.
 * Each conversation has: conversation_id, title, last_updated, last_model_used, preview, intent?, category?
 */
export function normalizeSidebarResponse(apiData: unknown): NormalizedHistoryEntry[] {
  if (!apiData || typeof apiData !== 'object') return [];
  const data = apiData as any;

  const items: any[] = Array.isArray(data.conversations)
    ? data.conversations
    : Array.isArray(data.items)
    ? data.items
    : Array.isArray(data)
    ? data
    : [];

  return items.slice(0, 5).map((item: any) => {
    const ts = toSafeTimestamp(item.last_updated || item.updated_at || item.timestamp || item.created_at);
    const category = String(item.category || 'terminal');
    const intent = String(item.intent || 'freeform_query');
    return {
      id: String(item.conversation_id || item.id || `sidebar-${ts}`),
      timestamp: ts,
      content: typeof item.preview === 'string' ? item.preview : (typeof item.title === 'string' ? item.title : ''),
      display_type: null,
      query: typeof item.title === 'string' ? item.title : undefined,
      model_used: typeof item.last_model_used === 'string' ? item.last_model_used : (typeof item.model_used === 'string' ? item.model_used : undefined),
      conversation_id: typeof item.conversation_id === 'string' ? item.conversation_id : undefined,
      title: typeof item.title === 'string' ? item.title : undefined,
      key: `${category}::${intent}`,
      category,
      intent,
    };
  });
}
