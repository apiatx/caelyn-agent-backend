/**
 * ProphetikInvestorPage.tsx
 *
 * Prophetik Investor tab — Event Impact Ledger + Equity Signals.
 *
 * Data source: GET /api/predict/investor/intelligence
 *
 * Exposure rendering priority (backend-driven — no frontend ticker maps):
 *   1. Watchlist tickers  (bullish/bearish/conditional_watchlist)
 *   2. Theme Universe tickers (bullish/bearish/conditional_fallback)
 *   3. Theme labels  (bullish/bearish/conditional_themes)
 *   4. "No direct exposure" only when backend returns no_direct_exposure=true
 *
 * "Macro Only" never appears — exposure always comes from the backend.
 *
 * Drop this file into your pages/components tree and import:
 *   import { ProphetikInvestorPage } from "./ProphetikInvestorPage";
 *
 * Usage:
 *   <ProphetikInvestorPage />   (no props required)
 */

import { useState, useEffect, useCallback } from "react";
import {
  TrendingUp, TrendingDown, Activity, RefreshCw, Loader2,
  AlertTriangle, ChevronDown, ChevronRight, Zap, Target,
  Sparkles, BarChart3, Globe, DollarSign,
} from "lucide-react";

// ─── Types ────────────────────────────────────────────────────────────────────

interface Exposure {
  bullish_watchlist:    string[];
  bearish_watchlist:    string[];
  conditional_watchlist: string[];
  bullish_fallback:     string[];
  bearish_fallback:     string[];
  conditional_fallback: string[];
  bullish_themes:       string[];
  bearish_themes:       string[];
  conditional_themes:   string[];
  exposure_source:      string;
  no_direct_exposure:   boolean;
}

interface TrackedOdds {
  family_key:        string;
  label:             string;
  category:          string;
  priority:          number;
  dashboard_enabled: boolean;
  prophetik_enabled: boolean;
  yes_probability:   number | null;
  yes_pct:           number | null;
  market_question:   string | null;
  volume_24h:        number | null;
  delta_1h_pp:       number | null;
  delta_24h_pp:      number | null;
  delta_7d_pp:       number | null;
  market_read:       string | null;
  exposure:          Exposure | null;
}

interface EquitySignal {
  event_family_key:  string;
  title:             string;
  primary_category:  string;
  direction:         string;
  signal_quality:    string;
  yes_probability:   number | null;
  delta_24h_pp:      number | null;
  delta_7d_pp:       number | null;
  why_it_matters:    string | null;
  market_read:       string | null;
  exposure:          Exposure | null;
}

interface IntelligencePayload {
  updated_at:     string;
  _cached_at:     string;
  cache_age_seconds: number;
  diagnostics:    Record<string, unknown>;
  tracked_odds:   TrackedOdds[];
  equity_signals: EquitySignal[];
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmtPct(v: number | null | undefined, signed = false): string {
  if (v == null) return "—";
  return `${signed && v > 0 ? "+" : ""}${v.toFixed(1)}%`;
}

function fmtVol(v: number | null | undefined): string {
  if (v == null) return "";
  if (v >= 1_000_000) return `$${(v / 1_000_000).toFixed(1)}M`;
  if (v >= 1_000)     return `$${(v / 1_000).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function deltaCls(v: number | null | undefined): string {
  if (v == null) return "text-white/25";
  if (v > 0.5)  return "text-emerald-400";
  if (v < -0.5) return "text-red-400";
  return "text-white/40";
}

/** Trim list to at most `max` items; group into chunks of `chunkSize` per line */
function clamp<T>(arr: T[], max = 5): T[] {
  return arr.slice(0, max);
}

// ─── Market Read Badge ────────────────────────────────────────────────────────

const MARKET_READ_STYLE: Record<string, { bg: string; text: string; icon: typeof Activity }> = {
  "rates easing":               { bg: "bg-emerald-500/15 border-emerald-500/25", text: "text-emerald-400",  icon: TrendingDown },
  "rates restrictive":          { bg: "bg-red-500/15 border-red-500/25",         text: "text-red-400",      icon: TrendingUp   },
  "risk-on":                    { bg: "bg-blue-500/15 border-blue-500/25",       text: "text-blue-400",     icon: Zap          },
  "risk-off":                   { bg: "bg-amber-500/15 border-amber-500/25",     text: "text-amber-400",    icon: AlertTriangle },
  "inflationary":               { bg: "bg-orange-500/15 border-orange-500/25",   text: "text-orange-400",   icon: TrendingUp   },
  "disinflationary":            { bg: "bg-teal-500/15 border-teal-500/25",       text: "text-teal-400",     icon: TrendingDown },
  "growth positive":            { bg: "bg-emerald-500/15 border-emerald-500/25", text: "text-emerald-400",  icon: TrendingUp   },
  "growth negative":            { bg: "bg-red-500/15 border-red-500/25",         text: "text-red-400",      icon: TrendingDown },
  "geopolitical stress rising": { bg: "bg-red-500/15 border-red-500/25",         text: "text-red-400",      icon: Globe        },
  "geopolitical stress easing": { bg: "bg-emerald-500/15 border-emerald-500/25", text: "text-emerald-400",  icon: Globe        },
  "tech bullish":               { bg: "bg-violet-500/15 border-violet-500/25",   text: "text-violet-400",   icon: Sparkles     },
  "commodity pressure":         { bg: "bg-amber-500/15 border-amber-500/25",     text: "text-amber-400",    icon: BarChart3    },
  "mixed":                      { bg: "bg-gray-500/15 border-gray-500/25",       text: "text-white/50",     icon: Activity     },
  "conditional":                { bg: "bg-gray-500/15 border-gray-500/25",       text: "text-white/40",     icon: Activity     },
};

function MarketReadBadge({ value }: { value: string | null | undefined }) {
  if (!value) {
    return (
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded border text-[9px] font-semibold uppercase tracking-wider bg-amber-500/10 border-amber-500/20 text-amber-400/60">
        <AlertTriangle className="w-2.5 h-2.5" />
        Needs mapping
      </span>
    );
  }
  const style = MARKET_READ_STYLE[value.toLowerCase()] ?? {
    bg: "bg-gray-500/15 border-gray-500/25",
    text: "text-white/50",
    icon: Activity,
  };
  const Icon = style.icon;
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded border text-[9px] font-semibold uppercase tracking-wider ${style.bg} ${style.text}`}>
      <Icon className="w-2.5 h-2.5" />
      {value}
    </span>
  );
}

// ─── Ticker Chip ──────────────────────────────────────────────────────────────

function TickerChip({ sym, variant }: { sym: string; variant: "bull" | "bear" | "cond" | "theme" }) {
  const cls = {
    bull:  "bg-emerald-500/15 border-emerald-500/25 text-emerald-400",
    bear:  "bg-red-500/15 border-red-500/25 text-red-400",
    cond:  "bg-amber-500/15 border-amber-500/25 text-amber-300",
    theme: "bg-blue-500/10 border-blue-500/20 text-blue-300/80",
  }[variant];
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded border text-[9px] font-mono font-semibold whitespace-nowrap ${cls}`}>
      {sym}
    </span>
  );
}

// ─── Group Label ─────────────────────────────────────────────────────────────

function GroupLabel({ label, variant }: { label: string; variant: "bull" | "bear" | "cond" | "theme" | "universe" }) {
  const cls = {
    bull:    "text-emerald-400/60",
    bear:    "text-red-400/60",
    cond:    "text-amber-400/60",
    theme:   "text-blue-400/60",
    universe:"text-white/30",
  }[variant];
  return <span className={`text-[8px] font-bold uppercase tracking-widest ${cls}`}>{label}</span>;
}

// ─── Exposure Cell ────────────────────────────────────────────────────────────

function ExposureCell({ exposure }: { exposure: Exposure | null | undefined }) {
  if (!exposure) {
    return <span className="text-[10px] text-white/20 italic">No data</span>;
  }

  if (exposure.no_direct_exposure) {
    return <span className="text-[10px] text-white/30 italic">No direct exposure</span>;
  }

  const hasWatchlist =
    exposure.bullish_watchlist.length    > 0 ||
    exposure.bearish_watchlist.length    > 0 ||
    exposure.conditional_watchlist.length > 0;

  const hasFallback =
    exposure.bullish_fallback.length    > 0 ||
    exposure.bearish_fallback.length    > 0 ||
    exposure.conditional_fallback.length > 0;

  const hasThemes =
    exposure.bullish_themes.length    > 0 ||
    exposure.bearish_themes.length    > 0 ||
    exposure.conditional_themes.length > 0;

  return (
    <div className="flex flex-col gap-1.5 min-w-0">

      {/* ── Priority 1: Watchlist tickers ── */}
      {hasWatchlist && (
        <div className="flex flex-col gap-1">
          <GroupLabel label="Watchlist" variant="bull" />
          <div className="flex flex-wrap gap-1">
            {clamp(exposure.bullish_watchlist, 5).map(s => (
              <TickerChip key={`bw-${s}`} sym={s} variant="bull" />
            ))}
            {clamp(exposure.bearish_watchlist, 5).map(s => (
              <TickerChip key={`ww-${s}`} sym={s} variant="bear" />
            ))}
            {clamp(exposure.conditional_watchlist, 3).map(s => (
              <TickerChip key={`cw-${s}`} sym={s} variant="cond" />
            ))}
          </div>
        </div>
      )}

      {/* ── Priority 2: Theme Universe fallback tickers ── */}
      {hasFallback && (
        <div className="flex flex-col gap-1">
          {!hasWatchlist && (
            <GroupLabel label="Theme Universe" variant="universe" />
          )}
          {/* Bullish fallback row */}
          {exposure.bullish_fallback.length > 0 && (
            <div className="flex items-center gap-1 flex-wrap">
              {!hasWatchlist && (
                <GroupLabel label="Bullish" variant="bull" />
              )}
              {clamp(exposure.bullish_fallback, 5).map(s => (
                <TickerChip key={`bf-${s}`} sym={s} variant="bull" />
              ))}
              {exposure.bullish_fallback.length > 5 && (
                <span className="text-[8px] text-white/25">+{exposure.bullish_fallback.length - 5}</span>
              )}
            </div>
          )}
          {/* Bearish fallback row */}
          {exposure.bearish_fallback.length > 0 && (
            <div className="flex items-center gap-1 flex-wrap">
              {!hasWatchlist && (
                <GroupLabel label="Bearish" variant="bear" />
              )}
              {clamp(exposure.bearish_fallback, 5).map(s => (
                <TickerChip key={`bearf-${s}`} sym={s} variant="bear" />
              ))}
              {exposure.bearish_fallback.length > 5 && (
                <span className="text-[8px] text-white/25">+{exposure.bearish_fallback.length - 5}</span>
              )}
            </div>
          )}
          {/* Conditional fallback */}
          {exposure.conditional_fallback.length > 0 && (
            <div className="flex items-center gap-1 flex-wrap">
              {clamp(exposure.conditional_fallback, 4).map(s => (
                <TickerChip key={`cf-${s}`} sym={s} variant="cond" />
              ))}
            </div>
          )}
        </div>
      )}

      {/* ── Priority 3: Theme labels (when no tickers at all) ── */}
      {!hasWatchlist && !hasFallback && hasThemes && (
        <div className="flex flex-col gap-1">
          <GroupLabel label="Themes" variant="theme" />
          <div className="flex flex-wrap gap-1">
            {clamp(exposure.bullish_themes, 3).map(t => (
              <TickerChip key={`bt-${t}`} sym={t} variant="bull" />
            ))}
            {clamp(exposure.bearish_themes, 3).map(t => (
              <TickerChip key={`brt-${t}`} sym={t} variant="bear" />
            ))}
            {clamp(exposure.conditional_themes, 2).map(t => (
              <TickerChip key={`ct-${t}`} sym={t} variant="cond" />
            ))}
          </div>
        </div>
      )}

      {/* ── Theme row (supplemental, when tickers exist) ── */}
      {(hasWatchlist || hasFallback) && hasThemes && (
        <div className="flex flex-wrap gap-1">
          {clamp(exposure.bullish_themes, 2).map(t => (
            <TickerChip key={`btsup-${t}`} sym={t} variant="theme" />
          ))}
          {clamp(exposure.bearish_themes, 2).map(t => (
            <TickerChip key={`brtsup-${t}`} sym={t} variant="theme" />
          ))}
        </div>
      )}

    </div>
  );
}

// ─── YES % Badge ─────────────────────────────────────────────────────────────

function YesBadge({ pct }: { pct: number | null | undefined }) {
  if (pct == null) return <span className="text-white/20 text-xs">—</span>;
  const cls = pct >= 65 ? "text-emerald-400" : pct <= 35 ? "text-red-400" : "text-blue-400";
  return <span className={`text-sm font-bold font-mono ${cls}`}>{pct}%</span>;
}

// ─── Event Impact Ledger ──────────────────────────────────────────────────────

function EventImpactLedger({ rows }: { rows: TrackedOdds[] }) {
  const [expanded, setExpanded] = useState<string | null>(null);

  if (rows.length === 0) {
    return (
      <div className="text-center py-10 text-sm text-white/30">
        No tracked macro events available.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[900px] border-separate border-spacing-0">
        <thead>
          <tr className="border-b border-white/[0.06]">
            {["Event / Family", "Category", "YES %", "24 h Δ", "7 d Δ", "Market Read", "Exposure"].map(h => (
              <th
                key={h}
                className="px-3 py-2.5 text-left text-[10px] font-semibold text-white/30 uppercase tracking-wider whitespace-nowrap"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const isExp = expanded === row.family_key;
            return (
              <>
                <tr
                  key={row.family_key}
                  className={`border-b border-white/[0.03] hover:bg-white/[0.02] transition-colors cursor-pointer ${i % 2 === 0 ? "" : "bg-white/[0.01]"}`}
                  onClick={() => setExpanded(isExp ? null : row.family_key)}
                >
                  {/* Event label */}
                  <td className="px-3 py-2.5">
                    <div className="flex items-center gap-1.5">
                      {isExp
                        ? <ChevronDown className="w-3 h-3 text-white/30 flex-shrink-0" />
                        : <ChevronRight className="w-3 h-3 text-white/20 flex-shrink-0" />
                      }
                      <span className="text-[11px] font-semibold text-white/80 leading-tight">{row.label}</span>
                    </div>
                  </td>

                  {/* Category */}
                  <td className="px-3 py-2.5">
                    <span className="text-[10px] text-white/40 whitespace-nowrap">{row.category}</span>
                  </td>

                  {/* YES % */}
                  <td className="px-3 py-2.5">
                    <YesBadge pct={row.yes_pct} />
                  </td>

                  {/* 24h Δ */}
                  <td className="px-3 py-2.5">
                    <span className={`text-[11px] font-mono font-bold ${deltaCls(row.delta_24h_pp)}`}>
                      {fmtPct(row.delta_24h_pp, true)}
                    </span>
                  </td>

                  {/* 7d Δ */}
                  <td className="px-3 py-2.5">
                    <span className={`text-[11px] font-mono ${deltaCls(row.delta_7d_pp)}`}>
                      {fmtPct(row.delta_7d_pp, true)}
                    </span>
                  </td>

                  {/* Market Read */}
                  <td className="px-3 py-2.5 min-w-[140px]">
                    <MarketReadBadge value={row.market_read} />
                  </td>

                  {/* Exposure */}
                  <td className="px-3 py-2.5 max-w-[340px]">
                    <ExposureCell exposure={row.exposure} />
                  </td>
                </tr>

                {/* Expanded detail row */}
                {isExp && (
                  <tr key={`${row.family_key}-detail`} className="bg-white/[0.015]">
                    <td colSpan={7} className="px-5 py-4">
                      <div className="flex flex-col gap-3">

                        {/* Market question */}
                        {row.market_question && (
                          <div>
                            <span className="text-[9px] font-bold uppercase tracking-widest text-white/30 mr-2">Market Question</span>
                            <span className="text-[11px] text-white/60">{row.market_question}</span>
                          </div>
                        )}

                        {/* Full exposure detail */}
                        {row.exposure && !row.exposure.no_direct_exposure && (
                          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-[10px]">
                            {/* Bullish */}
                            <div>
                              <div className="text-[9px] font-bold uppercase tracking-widest text-emerald-400/60 mb-1.5">Bullish Exposure</div>
                              <div className="flex flex-wrap gap-1 mb-2">
                                {[...row.exposure.bullish_watchlist, ...row.exposure.bullish_fallback].map(s => (
                                  <TickerChip key={`full-b-${s}`} sym={s} variant="bull" />
                                ))}
                                {[...row.exposure.bullish_watchlist, ...row.exposure.bullish_fallback].length === 0 && (
                                  <span className="text-white/20 italic text-[9px]">none</span>
                                )}
                              </div>
                              <div className="flex flex-wrap gap-1">
                                {row.exposure.bullish_themes.map(t => (
                                  <TickerChip key={`full-bt-${t}`} sym={t} variant="theme" />
                                ))}
                              </div>
                            </div>
                            {/* Bearish */}
                            <div>
                              <div className="text-[9px] font-bold uppercase tracking-widest text-red-400/60 mb-1.5">Bearish Exposure</div>
                              <div className="flex flex-wrap gap-1 mb-2">
                                {[...row.exposure.bearish_watchlist, ...row.exposure.bearish_fallback].map(s => (
                                  <TickerChip key={`full-br-${s}`} sym={s} variant="bear" />
                                ))}
                                {[...row.exposure.bearish_watchlist, ...row.exposure.bearish_fallback].length === 0 && (
                                  <span className="text-white/20 italic text-[9px]">none</span>
                                )}
                              </div>
                              <div className="flex flex-wrap gap-1">
                                {row.exposure.bearish_themes.map(t => (
                                  <TickerChip key={`full-brt-${t}`} sym={t} variant="theme" />
                                ))}
                              </div>
                            </div>
                            {/* Conditional */}
                            <div>
                              <div className="text-[9px] font-bold uppercase tracking-widest text-amber-400/60 mb-1.5">Conditional / Source</div>
                              <div className="flex flex-wrap gap-1 mb-2">
                                {[...row.exposure.conditional_watchlist, ...row.exposure.conditional_fallback].map(s => (
                                  <TickerChip key={`full-c-${s}`} sym={s} variant="cond" />
                                ))}
                                {[...row.exposure.conditional_watchlist, ...row.exposure.conditional_fallback].length === 0 && (
                                  <span className="text-white/20 italic text-[9px]">none</span>
                                )}
                              </div>
                              <div className="flex flex-wrap gap-1">
                                {row.exposure.conditional_themes.map(t => (
                                  <TickerChip key={`full-ct-${t}`} sym={t} variant="cond" />
                                ))}
                              </div>
                              <div className="mt-2 text-[8px] text-white/20 uppercase tracking-widest">
                                Source: {row.exposure.exposure_source}
                              </div>
                            </div>
                          </div>
                        )}

                        {/* Volume */}
                        {row.volume_24h != null && (
                          <div className="text-[9px] text-white/25">
                            24h Volume: {fmtVol(row.volume_24h)}
                          </div>
                        )}
                      </div>
                    </td>
                  </tr>
                )}
              </>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ─── Equity Signals Panel ─────────────────────────────────────────────────────

function EquitySignalsPanel({ signals }: { signals: EquitySignal[] }) {
  if (signals.length === 0) return null;
  return (
    <div className="space-y-3">
      {signals.map((s) => {
        const dirCls = s.direction === "rising"
          ? "bg-red-500/15 border-red-500/25 text-red-400"
          : s.direction === "falling"
          ? "bg-emerald-500/15 border-emerald-500/25 text-emerald-400"
          : "bg-gray-500/15 border-gray-500/25 text-white/50";
        return (
          <div
            key={s.event_family_key}
            className="bg-white/[0.025] border border-white/[0.06] rounded-xl p-4"
          >
            {/* Header */}
            <div className="flex items-start justify-between gap-3 mb-3">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap mb-1">
                  <span className="text-sm font-bold text-white leading-tight">{s.title}</span>
                  <span className={`text-[9px] font-bold uppercase tracking-wider px-2 py-0.5 rounded border ${dirCls}`}>
                    {s.direction}
                  </span>
                  {s.signal_quality && (
                    <span className="text-[9px] text-white/30 uppercase tracking-wider">{s.signal_quality}</span>
                  )}
                </div>
                {s.why_it_matters && (
                  <p className="text-[11px] text-white/50 leading-relaxed">{s.why_it_matters}</p>
                )}
              </div>
              {s.yes_probability != null && (
                <div className="flex-shrink-0 text-right">
                  <div className="text-lg font-bold font-mono text-blue-400">{(s.yes_probability * 100).toFixed(0)}%</div>
                  <div className="text-[9px] text-white/25 uppercase tracking-wider">YES odds</div>
                </div>
              )}
            </div>

            {/* Market Read + Exposure */}
            <div className="flex flex-col gap-2">
              <div className="flex items-center gap-2">
                <span className="text-[9px] font-bold uppercase tracking-widest text-white/30 w-20 flex-shrink-0">Market Read</span>
                <MarketReadBadge value={s.market_read} />
              </div>
              <div className="flex items-start gap-2">
                <span className="text-[9px] font-bold uppercase tracking-widest text-white/30 w-20 flex-shrink-0 pt-0.5">Exposure</span>
                <div className="flex-1 min-w-0">
                  <ExposureCell exposure={s.exposure} />
                </div>
              </div>
            </div>

            {/* Deltas */}
            {(s.delta_24h_pp != null || s.delta_7d_pp != null) && (
              <div className="flex items-center gap-4 mt-3 pt-3 border-t border-white/[0.04]">
                {s.delta_24h_pp != null && (
                  <div>
                    <div className="text-[8px] text-white/25 uppercase tracking-widest">24h Δ</div>
                    <div className={`text-xs font-mono font-bold ${deltaCls(s.delta_24h_pp)}`}>{fmtPct(s.delta_24h_pp, true)}</div>
                  </div>
                )}
                {s.delta_7d_pp != null && (
                  <div>
                    <div className="text-[8px] text-white/25 uppercase tracking-widest">7d Δ</div>
                    <div className={`text-xs font-mono ${deltaCls(s.delta_7d_pp)}`}>{fmtPct(s.delta_7d_pp, true)}</div>
                  </div>
                )}
                <div className="text-[9px] text-white/20 ml-auto">{s.primary_category}</div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

// ─── Cache Age Indicator ──────────────────────────────────────────────────────

function CacheAge({ seconds }: { seconds: number | null | undefined }) {
  if (seconds == null) return null;
  const mins = Math.floor(seconds / 60);
  const label = mins < 1 ? "Just updated" : `${mins}m ago`;
  const cls = mins < 5 ? "text-emerald-400/60" : mins < 15 ? "text-amber-400/60" : "text-red-400/60";
  return <span className={`text-[9px] uppercase tracking-widest ${cls}`}>{label}</span>;
}

// ─── Main Page ────────────────────────────────────────────────────────────────

export function ProphetikInvestorPage() {
  const [data, setData]       = useState<IntelligencePayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);
  const [tab, setTab]         = useState<"ledger" | "signals">("ledger");

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/predict/investor/intelligence");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d: IntelligencePayload = await r.json();
      setData(d);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : "Unknown error";
      setError(msg);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  const trackedOdds   = data?.tracked_odds   ?? [];
  const equitySignals = data?.equity_signals ?? [];

  return (
    <div className="space-y-5">

      {/* ── Header ── */}
      <div
        className="rounded-2xl p-5"
        style={{
          background: "linear-gradient(135deg, rgba(139,92,246,0.08) 0%, rgba(59,130,246,0.06) 50%, rgba(16,185,129,0.04) 100%)",
          border: "1px solid rgba(139,92,246,0.15)",
        }}
      >
        <div className="flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-center gap-3">
            <div
              className="w-10 h-10 rounded-xl flex items-center justify-center flex-shrink-0"
              style={{ background: "linear-gradient(135deg, #8b5cf6, #3b82f6)" }}
            >
              <Sparkles className="w-5 h-5 text-white" />
            </div>
            <div>
              <h2 className="text-base font-bold text-white flex items-center gap-2">
                Prophetik Investor
                <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-violet-500/15 border border-violet-500/30">
                  <span className="w-1.5 h-1.5 rounded-full bg-violet-400 animate-pulse" />
                  <span className="text-[9px] text-violet-400 font-semibold uppercase tracking-widest">Live</span>
                </span>
              </h2>
              <p className="text-[10px] text-white/30">
                Macro-to-equity signal engine — powered by canonical theme universe
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {data && <CacheAge seconds={data.cache_age_seconds} />}
            <button
              onClick={fetchData}
              disabled={loading}
              className="p-2 rounded-lg border border-white/10 hover:bg-white/5 transition-colors disabled:opacity-40"
            >
              <RefreshCw className={`w-3.5 h-3.5 text-white/40 ${loading ? "animate-spin" : ""}`} />
            </button>
          </div>
        </div>

        {/* Diagnostics strip */}
        {data?.diagnostics && (
          <div className="flex items-center gap-4 mt-4 pt-4 border-t border-white/[0.05] flex-wrap text-[9px] text-white/25 uppercase tracking-widest">
            <span>Tracked: {data.diagnostics.tracked_odds_families as number ?? "—"}</span>
            <span>Equity Signals: {data.diagnostics.equity_signals_returned as number ?? "—"}</span>
            <span>Theme Universe: {data.diagnostics.theme_universe_theme_count as number ?? "—"} themes</span>
            <span>Fallback hits: {data.diagnostics.exposure_rows_with_fallback as number ?? "—"}</span>
            {(data.diagnostics.hardcoded_sector_stocks_used as boolean) === false && (
              <span className="text-emerald-400/40">✓ No hardcoded maps</span>
            )}
          </div>
        )}
      </div>

      {/* ── Tabs ── */}
      <div className="flex items-center gap-2">
        {(["ledger", "signals"] as const).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 rounded-lg text-xs font-semibold transition-all ${
              tab === t
                ? "bg-violet-500/20 border border-violet-500/30 text-violet-300"
                : "bg-white/[0.03] border border-white/[0.07] text-white/40 hover:text-white/60 hover:bg-white/[0.05]"
            }`}
          >
            {t === "ledger"
              ? `Event Impact Ledger (${trackedOdds.length})`
              : `Equity Signals (${equitySignals.length})`}
          </button>
        ))}
      </div>

      {/* ── Content ── */}
      {loading ? (
        <div className="flex items-center justify-center py-16 gap-3 text-white/30">
          <Loader2 className="w-5 h-5 animate-spin" />
          <span className="text-sm">Loading intelligence data…</span>
        </div>
      ) : error ? (
        <div className="flex items-center gap-3 p-4 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-sm">
          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
          <span>Failed to load: {error}</span>
          <button onClick={fetchData} className="ml-auto text-xs underline">Retry</button>
        </div>
      ) : (
        <>
          {tab === "ledger" && (
            <div
              className="rounded-2xl overflow-hidden"
              style={{ background: "rgba(255,255,255,0.018)", border: "1px solid rgba(255,255,255,0.06)" }}
            >
              {/* Legend */}
              <div className="flex items-center gap-4 px-4 py-2.5 border-b border-white/[0.04] flex-wrap">
                <span className="text-[9px] text-white/25 uppercase tracking-widest">Exposure key:</span>
                <span className="flex items-center gap-1">
                  <span className="w-2 h-2 rounded-sm bg-emerald-500/40" />
                  <span className="text-[9px] text-white/30">Bullish</span>
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-2 h-2 rounded-sm bg-red-500/40" />
                  <span className="text-[9px] text-white/30">Bearish</span>
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-2 h-2 rounded-sm bg-blue-500/25" />
                  <span className="text-[9px] text-white/30">Theme label</span>
                </span>
                <span className="text-[9px] text-white/20 ml-auto">Click row to expand</span>
              </div>

              <EventImpactLedger rows={trackedOdds} />
            </div>
          )}

          {tab === "signals" && (
            <div>
              {equitySignals.length === 0 ? (
                <div className="text-center py-10 text-sm text-white/30">No equity signals available.</div>
              ) : (
                <EquitySignalsPanel signals={equitySignals} />
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default ProphetikInvestorPage;
