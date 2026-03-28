import { useState, useEffect, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  BarChart, Bar, Cell,
} from 'recharts';
import { TrendingUp, TrendingDown } from 'lucide-react';

/* ═══════════════════════════════════════════════════════════════════════════
   Bloomberg-style terminal CSS (injected once)
   ═══════════════════════════════════════════════════════════════════════════ */
const TERMINAL_STYLES = `
  :root {
    --term-green: 142 70% 55%;
    --term-red: 0 72% 55%;
    --term-amber: 45 90% 55%;
    --term-cyan: 185 70% 55%;
    --term-dim: 220 10% 40%;
    --term-bg: 220 20% 4%;
    --term-surface: 220 20% 7%;
    --term-border: 220 15% 14%;
  }
  @keyframes blink { 0%,50%{opacity:1} 51%,to{opacity:0} }
  @keyframes scanline { 0%{transform:translateY(-100%)} to{transform:translateY(100vh)} }
  .cursor-blink { animation: blink 1s step-end infinite; }
  .scanline::after {
    content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(to right, transparent, hsl(142 70% 55% / .03), transparent);
    animation: scanline 8s linear infinite; pointer-events: none; z-index: 100;
  }
  .glow-green { text-shadow: 0 0 8px hsl(142 70% 55% / .4); }
  .glow-red   { text-shadow: 0 0 8px hsl(0 72% 55% / .4); }
  .glow-amber { text-shadow: 0 0 8px hsl(45 90% 55% / .4); }
  .tabular-nums { font-variant-numeric: tabular-nums lining-nums; }
`;

let stylesInjected = false;
function injectTerminalStyles() {
  if (stylesInjected) return;
  const el = document.createElement('style');
  el.textContent = TERMINAL_STYLES;
  document.head.appendChild(el);
  stylesInjected = true;
}

// ─── Design tokens ──────────────────────────────────────────────────────────
const T = {
  bg: 'hsl(var(--term-bg))',
  surface: 'hsl(var(--term-surface))',
  border: 'hsl(var(--term-border))',
  green: 'hsl(var(--term-green))',
  red: 'hsl(var(--term-red))',
  amber: 'hsl(var(--term-amber))',
  cyan: 'hsl(var(--term-cyan))',
  dim: 'hsl(var(--term-dim))',
};

// ─── Tab config ─────────────────────────────────────────────────────────────
const TABS = [
  { id: 'overview', label: 'OVERVIEW', shortcut: '1' },
  { id: 'rates', label: 'RATES', shortcut: '2' },
  { id: 'inflation', label: 'INFLATION', shortcut: '3' },
  { id: 'growth', label: 'GROWTH', shortcut: '4' },
  { id: 'labor', label: 'LABOR', shortcut: '5' },
  { id: 'sentiment', label: 'RISK', shortcut: '6' },
] as const;

type TabId = typeof TABS[number]['id'];

const API_MAP: Record<TabId, string> = {
  overview: '/api/macro/dashboard',
  rates: '/api/macro/rates',
  inflation: '/api/macro/inflation',
  growth: '/api/macro/growth',
  labor: '/api/macro/labor',
  sentiment: '/api/macro/risk',
};

// ─── Shared styles ──────────────────────────────────────────────────────────
const card = 'border border-[hsl(var(--term-border))] bg-[hsl(var(--term-surface))] p-3';
const sectionTitle = 'text-[10px] text-[hsl(var(--term-dim))] tracking-wider uppercase mb-2';

function StatusBadge({ status }: { status: string }) {
  const colors: Record<string, string> = {
    positive: 'text-[hsl(var(--term-green))] bg-[hsl(var(--term-green)/0.08)] border-[hsl(var(--term-green)/0.2)]',
    neutral: 'text-[hsl(var(--term-dim))] bg-white/5 border-white/10',
    elevated: 'text-[hsl(var(--term-amber))] bg-[hsl(var(--term-amber)/0.08)] border-[hsl(var(--term-amber)/0.2)]',
    negative: 'text-[hsl(var(--term-red))] bg-[hsl(var(--term-red)/0.08)] border-[hsl(var(--term-red)/0.2)]',
    high: 'text-[hsl(var(--term-red))] bg-[hsl(var(--term-red)/0.08)] border-[hsl(var(--term-red)/0.2)]',
    low: 'text-[hsl(var(--term-green))] bg-[hsl(var(--term-green)/0.08)] border-[hsl(var(--term-green)/0.2)]',
    inverted: 'text-[hsl(var(--term-red))] bg-[hsl(var(--term-red)/0.06)] border-[hsl(var(--term-red)/0.3)]',
  };
  return (
    <span className={`text-[9px] px-1.5 py-0.5 border ${colors[status] || colors.neutral}`}>
      {status.toUpperCase()}
    </span>
  );
}

function IndicatorCard({ name, value, status }: { name: string; value: string; status: string }) {
  return (
    <div className={card}>
      <div className="flex items-center justify-between mb-1">
        <span className="text-[10px] text-[hsl(var(--term-dim))] uppercase tracking-wide">{name}</span>
        <StatusBadge status={status} />
      </div>
      <div className="text-sm font-semibold text-white tabular-nums">{value}</div>
    </div>
  );
}

function ChartTooltipContent({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-[hsl(var(--term-surface))] border border-[hsl(var(--term-border))] px-3 py-2 text-xs">
      <div className="text-[hsl(var(--term-dim))] mb-1">{label}</div>
      {payload.map((p: any, i: number) => (
        <div key={i} className="text-white tabular-nums">
          <span style={{ color: p.color }}>{p.name}: </span>
          {typeof p.value === 'number' ? p.value.toFixed(2) : p.value}
        </div>
      ))}
    </div>
  );
}

const chartGrid = 'hsl(220 15% 14%)';
const chartTick = { fill: 'hsl(220 10% 40%)', fontSize: 10 };

// ─── TAB 1: OVERVIEW ─────────────────────────────────────────────────────────
function OverviewTab({ data }: { data: any }) {
  if (!data) return null;
  const etfNames: Record<string, string> = {
    SPY: 'S&P 500', QQQ: 'Nasdaq 100', TLT: '20+ Yr Treasury',
    GLD: 'Gold', USO: 'Crude Oil', HYG: 'High Yield Corp',
  };
  return (
    <div className="space-y-4">
      <div className={card}>
        <div className={sectionTitle}>BENCHMARK ETFs</div>
        <div className="grid grid-cols-7 gap-2">
          {data.benchmark_etfs?.map((etf: any) => {
            const up = etf.change_pct >= 0;
            return (
              <div key={etf.ticker} className="text-center">
                <div className="text-[10px] text-[hsl(var(--term-dim))] tracking-wider">{etf.ticker}</div>
                <div className={`text-sm font-semibold tabular-nums ${up ? 'text-[hsl(var(--term-green))] glow-green' : 'text-[hsl(var(--term-red))] glow-red'}`}>
                  ${etf.price?.toFixed(2)}
                </div>
                <div className={`flex items-center justify-center gap-1 text-[10px] tabular-nums ${up ? 'text-[hsl(var(--term-green))]' : 'text-[hsl(var(--term-red))]'}`}>
                  {up ? <TrendingUp className="w-2.5 h-2.5" /> : <TrendingDown className="w-2.5 h-2.5" />}
                  {up ? '+' : ''}{etf.change_pct?.toFixed(2)}%
                </div>
                <div className="text-[9px] text-[hsl(var(--term-dim))]">{etfNames[etf.ticker] || etf.ticker}</div>
                <div className={`text-[9px] ${etf.pct_from_52w_high >= -5 ? 'text-[hsl(var(--term-dim))]' : 'text-[hsl(var(--term-amber))]'}`}>
                  {etf.pct_from_52w_high?.toFixed(1)}% from 52WH
                </div>
              </div>
            );
          })}
          {/* VIX Card */}
          {data.vix && (() => {
            const v = data.vix;
            const level = v.current >= 30 ? 'high' : v.current >= 20 ? 'elevated' : 'low';
            const color = level === 'high' ? '--term-red' : level === 'elevated' ? '--term-amber' : '--term-green';
            const glow = level === 'high' ? 'glow-red' : level === 'elevated' ? 'glow-amber' : 'glow-green';
            const down = v.change_pct < 0;
            return (
              <div className="text-center">
                <div className="text-[10px] text-[hsl(var(--term-dim))] tracking-wider">VIX</div>
                <div className={`text-sm font-semibold tabular-nums text-[hsl(var(${color}))] ${glow}`}>{v.current?.toFixed(2)}</div>
                <div className={`flex items-center justify-center gap-1 text-[10px] tabular-nums ${down ? 'text-[hsl(var(--term-green))]' : 'text-[hsl(var(--term-red))]'}`}>
                  {down ? <TrendingDown className="w-2.5 h-2.5" /> : <TrendingUp className="w-2.5 h-2.5" />}
                  {v.change_pct >= 0 ? '+' : ''}{v.change_pct?.toFixed(2)}%
                </div>
                <div className="text-[9px] text-[hsl(var(--term-dim))]">Volatility</div>
                <div className={`text-[9px] text-[hsl(var(${color}))] capitalize`}>{level}</div>
              </div>
            );
          })()}
        </div>
      </div>

      {/* Yield Snapshot */}
      {data.yield_snapshot && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.cyan }}>$ YIELD SNAPSHOT</div>
          <div className="grid grid-cols-4 gap-4">
            {Object.entries(data.yield_snapshot).map(([mat, val]: [string, any]) => (
              <div key={mat} className="text-center">
                <div className="text-[10px] text-[hsl(var(--term-dim))] uppercase mb-1">{mat}</div>
                <div className="text-sm font-semibold text-[hsl(var(--term-green))] glow-green tabular-nums">
                  {val ? `${val.toFixed(2)}%` : '—'}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Indicator Cards */}
      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── TAB 2: RATES ────────────────────────────────────────────────────────────
function RatesTab({ data }: { data: any }) {
  if (!data) return null;
  return (
    <div className="space-y-4">
      {/* Yield Curve Chart */}
      {data.yield_curve?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.cyan }}>YIELD CURVE</div>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.yield_curve}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="maturity" tick={chartTick} />
                <YAxis tick={chartTick} domain={['auto', 'auto']} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="yield" stroke={T.cyan} strokeWidth={2} dot={{ fill: T.cyan, r: 3 }} name="Current" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Yield Table */}
      {data.yield_curve?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.cyan }}>YIELD CURVE SNAPSHOT</div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-[hsl(var(--term-dim))] border-b border-[hsl(var(--term-border))]">
                <th className="text-left py-2 text-[10px] tracking-wider">Maturity</th>
                <th className="text-right py-2 text-[10px] tracking-wider">Current</th>
                <th className="text-right py-2 text-[10px] tracking-wider">Change</th>
                <th className="text-right py-2 text-[10px] tracking-wider">Prior Close</th>
              </tr>
            </thead>
            <tbody>
              {data.yield_curve.map((y: any) => (
                <tr key={y.maturity} className="border-b border-[hsl(var(--term-border))]/50">
                  <td className="py-2 text-[hsl(var(--term-dim))]">{y.maturity}</td>
                  <td className="py-2 text-right text-[hsl(var(--term-green))] glow-green tabular-nums font-semibold">
                    {y.yield ? y.yield.toFixed(2) + '%' : '—'}
                  </td>
                  <td className={`py-2 text-right tabular-nums font-medium ${y.change > 0 ? 'text-[hsl(var(--term-red))]' : y.change < 0 ? 'text-[hsl(var(--term-green))]' : 'text-[hsl(var(--term-dim))]'}`}>
                    {y.change ? (y.change > 0 ? '+' : '') + y.change.toFixed(2) : '—'}
                  </td>
                  <td className="py-2 text-right text-[hsl(var(--term-dim))] tabular-nums">
                    {y.previousClose ? y.previousClose.toFixed(2) + '%' : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Spreads */}
      {data.spreads && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.cyan }}>KEY SPREADS</div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-[10px] text-[hsl(var(--term-dim))] mb-1">2s10s Spread</div>
              <div className={`text-lg font-bold tabular-nums ${data.spreads['2s10s'] < 0 ? 'text-[hsl(var(--term-red))] glow-red' : 'text-[hsl(var(--term-green))] glow-green'}`}>
                {data.spreads['2s10s'] >= 0 ? '+' : ''}{(data.spreads['2s10s'] * 100).toFixed(0)} bps
              </div>
              {data.spreads['2s10s'] < 0 && <div className="text-[10px] text-[hsl(var(--term-red))]">INVERTED</div>}
            </div>
            <div>
              <div className="text-[10px] text-[hsl(var(--term-dim))] mb-1">10Y-3M Spread</div>
              <div className={`text-lg font-bold tabular-nums ${data.spreads['10y3m'] < 0 ? 'text-[hsl(var(--term-red))] glow-red' : 'text-[hsl(var(--term-green))] glow-green'}`}>
                {data.spreads['10y3m'] >= 0 ? '+' : ''}{(data.spreads['10y3m'] * 100).toFixed(0)} bps
              </div>
            </div>
          </div>
        </div>
      )}

      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── TAB 3: INFLATION ────────────────────────────────────────────────────────
function InflationTab({ data }: { data: any }) {
  if (!data) return null;
  return (
    <div className="space-y-4">
      {data.headline && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.amber }}>HEADLINE NUMBERS</div>
          <div className="grid grid-cols-3 sm:grid-cols-6 gap-3">
            {[
              { label: 'CPI YoY', val: data.headline.cpi_yoy },
              { label: 'Core CPI', val: data.headline.core_cpi_yoy },
              { label: 'Core PCE', val: data.headline.core_pce_yoy },
              { label: 'PPI YoY', val: data.headline.ppi_yoy },
              { label: 'CPI MoM', val: data.headline.cpi_mom },
              { label: 'Target', val: data.headline.target },
            ].map(({ label, val }) => (
              <div key={label} className="text-center">
                <div className="text-[10px] text-[hsl(var(--term-dim))] uppercase mb-1">{label}</div>
                <div className={`text-sm font-bold tabular-nums ${val > data.headline.target ? 'text-[hsl(var(--term-amber))] glow-amber' : 'text-[hsl(var(--term-green))] glow-green'}`}>
                  {val}%
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.history?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.amber }}>INFLATION TREND</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.history}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} domain={[1.5, 4]} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="headline" stroke={T.amber} strokeWidth={2} dot={false} name="Headline" />
                <Line type="monotone" dataKey="core" stroke={T.red} strokeWidth={2} dot={false} name="Core" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="flex gap-4 mt-2 text-[10px]">
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.amber }} /> Headline</span>
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.red }} /> Core</span>
          </div>
        </div>
      )}

      {data.cpi_components?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.amber }}>CPI COMPONENT BREAKDOWN (YoY %)</div>
          <div className="space-y-2">
            {data.cpi_components.map((c: any) => {
              const maxVal = Math.max(...data.cpi_components.map((x: any) => Math.abs(x.value)));
              const pct = Math.abs(c.value) / maxVal * 100;
              return (
                <div key={c.name} className="flex items-center gap-3">
                  <span className="text-xs text-[hsl(var(--term-dim))] w-40 shrink-0 text-right">{c.name}</span>
                  <div className="flex-1 h-4 bg-white/5 overflow-hidden relative">
                    <div
                      className={`h-full ${c.hot ? 'bg-[hsl(var(--term-amber)/0.15)]' : c.value < 0 ? 'bg-[hsl(var(--term-green)/0.15)]' : 'bg-white/10'}`}
                      style={{ width: `${Math.min(pct, 100)}%` }}
                    />
                  </div>
                  <span className={`text-xs font-medium w-14 text-right tabular-nums ${c.hot ? 'text-[hsl(var(--term-amber))]' : c.value < 0 ? 'text-[hsl(var(--term-green))]' : 'text-[hsl(var(--term-dim))]'}`}>
                    {c.value > 0 ? '+' : ''}{c.value}%
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── TAB 4: GROWTH ───────────────────────────────────────────────────────────
function GrowthTab({ data }: { data: any }) {
  if (!data) return null;
  return (
    <div className="space-y-4">
      {data.gdp?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.green }}>REAL GDP GROWTH (QoQ SAAR %)</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.gdp}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="quarter" tick={chartTick} />
                <YAxis tick={chartTick} />
                <Tooltip content={<ChartTooltipContent />} />
                <Bar dataKey="gdp" name="GDP %">
                  {data.gdp.map((entry: any, i: number) => (
                    <Cell key={i} fill={entry.gdp >= 2.5 ? T.green : entry.gdp >= 1.5 ? T.amber : T.red} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {data.pmi?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.green }}>MANUFACTURING vs SERVICES PMI</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.pmi}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} domain={[44, 60]} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="mfg" stroke={T.amber} strokeWidth={2} dot={{ r: 2 }} name="Manufacturing" />
                <Line type="monotone" dataKey="svc" stroke={T.cyan} strokeWidth={2} dot={{ r: 2 }} name="Services" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="flex gap-4 mt-2 text-[10px]">
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.amber }} /> Manufacturing</span>
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.cyan }} /> Services</span>
          </div>
          <div className="text-[10px] text-[hsl(var(--term-dim))] mt-1">— Readings above 50 indicate expansion</div>
        </div>
      )}

      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── TAB 5: LABOR ────────────────────────────────────────────────────────────
function LaborTab({ data }: { data: any }) {
  if (!data) return null;
  return (
    <div className="space-y-4">
      {data.unemployment?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.red }}>UNEMPLOYMENT RATE (%)</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.unemployment}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} domain={[3.8, 4.8]} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="rate" stroke={T.red} strokeWidth={2} dot={{ r: 2 }} name="U-3 Rate" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {data.nfp?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.red }}>NON-FARM PAYROLLS (K)</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.nfp}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} />
                <Tooltip content={<ChartTooltipContent />} />
                <Bar dataKey="nfp" name="NFP (K)">
                  {data.nfp.map((entry: any, i: number) => (
                    <Cell key={i} fill={entry.nfp >= 100 ? T.green : entry.nfp >= 0 ? T.amber : T.red} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
          <div className="text-[10px] text-[hsl(var(--term-dim))] mt-1">— Declining payroll trend signals labor market deterioration</div>
        </div>
      )}

      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── TAB 6: SENTIMENT & RISK ─────────────────────────────────────────────────
function RiskTab({ data }: { data: any }) {
  if (!data) return null;
  const levelColors: Record<string, string> = {
    red: 'text-[hsl(var(--term-red))] border-[hsl(var(--term-red)/0.3)] bg-[hsl(var(--term-red)/0.06)]',
    amber: 'text-[hsl(var(--term-amber))] border-[hsl(var(--term-amber)/0.3)] bg-[hsl(var(--term-amber)/0.08)]',
    green: 'text-[hsl(var(--term-green))] border-[hsl(var(--term-green)/0.2)] bg-[hsl(var(--term-green)/0.06)]',
  };
  return (
    <div className="space-y-4">
      {data.risk_framework?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.red }}>DRUCKENMILLER RISK FRAMEWORK</div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {data.risk_framework.map((r: any) => (
              <div key={r.label} className={`border p-3 text-center ${levelColors[r.color] || levelColors.green}`}>
                <div className="text-[10px] uppercase tracking-wide opacity-80 mb-1">{r.label}</div>
                <div className="text-sm font-bold mb-1">{r.level}</div>
                <div className="text-[9px] opacity-60">{r.detail}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {data.vix_history?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.amber }}>VIX HISTORY</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.vix_history}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} domain={[10, 45]} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="vix" stroke={T.amber} strokeWidth={2} dot={{ r: 2 }} name="VIX" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {data.confidence?.length > 0 && (
        <div className={card}>
          <div className={sectionTitle} style={{ color: T.cyan }}>CONSUMER CONFIDENCE & SENTIMENT</div>
          <div className="h-56">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={data.confidence}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartGrid} />
                <XAxis dataKey="month" tick={chartTick} />
                <YAxis tick={chartTick} domain={[50, 105]} />
                <Tooltip content={<ChartTooltipContent />} />
                <Line type="monotone" dataKey="conf" stroke={T.cyan} strokeWidth={2} dot={false} name="Conference Board" />
                <Line type="monotone" dataKey="umich" stroke={T.amber} strokeWidth={2} dot={false} name="UMich" />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="flex gap-4 mt-2 text-[10px]">
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.cyan }} /> Conference Board</span>
            <span className="flex items-center gap-1"><span className="w-3 h-0.5" style={{ background: T.amber }} /> UMich</span>
          </div>
        </div>
      )}

      {data.indicators && (
        <div className="grid grid-cols-3 gap-3">
          {data.indicators.map((ind: any) => (
            <IndicatorCard key={ind.name} {...ind} />
          ))}
        </div>
      )}
    </div>
  );
}

// ─── MAIN COMPONENT ──────────────────────────────────────────────────────────
export function MacroTerminalLive() {
  const [activeTab, setActiveTab] = useState<TabId>('overview');

  useEffect(() => { injectTerminalStyles(); }, []);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
    const idx = parseInt(e.key) - 1;
    if (idx >= 0 && idx < TABS.length) setActiveTab(TABS[idx].id);
  }, []);

  useEffect(() => {
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [handleKeyDown]);

  const { data, isLoading, dataUpdatedAt } = useQuery({
    queryKey: [API_MAP[activeTab]],
    refetchInterval: 120_000,
    staleTime: 60_000,
    retry: 3,
    refetchOnWindowFocus: true,
  });

  const tabComponents: Record<TabId, React.ReactNode> = {
    overview: <OverviewTab data={data} />,
    rates: <RatesTab data={data} />,
    inflation: <InflationTab data={data} />,
    growth: <GrowthTab data={data} />,
    labor: <LaborTab data={data} />,
    sentiment: <RiskTab data={data} />,
  };

  const tabColor: Record<TabId, string> = {
    overview: '--term-green',
    rates: '--term-cyan',
    inflation: '--term-amber',
    growth: '--term-green',
    labor: '--term-red',
    sentiment: '--term-red',
  };

  return (
    <div
      className="h-screen flex flex-col scanline relative"
      style={{ background: T.bg, color: 'white', fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace" }}
    >
      {/* Terminal Title Bar */}
      <div className="flex items-center gap-3 px-4 py-2 border-b border-[hsl(var(--term-border))] shrink-0">
        <div className="flex gap-1.5">
          <span className="w-2.5 h-2.5 rounded-full bg-[hsl(var(--term-red))] opacity-80" />
          <span className="w-2.5 h-2.5 rounded-full bg-[hsl(var(--term-amber))] opacity-80" />
          <span className="w-2.5 h-2.5 rounded-full bg-[hsl(var(--term-green))] opacity-80" />
        </div>
        <div className="flex items-center gap-2">
          <span className="text-[hsl(var(--term-green))]">&#9608;</span>
          <span className="text-sm font-semibold text-[hsl(var(--term-green))] glow-green tracking-wider">MACRO TERMINAL</span>
          <span className="text-[10px] text-[hsl(var(--term-dim))] ml-1">v2.0</span>
        </div>
        <div className="flex items-center gap-6 ml-auto">
          <div className="text-[10px] text-[hsl(var(--term-dim))] tracking-wider">
            {new Date().toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' }).toUpperCase()}
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-[hsl(var(--term-dim))]">UPD</span>
            <span className="text-xs text-[hsl(var(--term-green))] tabular-nums glow-green">
              {dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : '--:--:--'}
            </span>
            <span className={`w-2 h-2 rounded-full bg-[hsl(var(--term-green))] ${isLoading ? 'animate-pulse' : 'cursor-blink'}`} />
          </div>
        </div>
      </div>

      {/* Tab Bar */}
      <div className="flex border-b border-[hsl(var(--term-border))] px-4 shrink-0">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`relative px-4 py-2.5 text-[10px] font-bold tracking-wider whitespace-nowrap transition-colors ${
              activeTab === tab.id
                ? 'text-white'
                : 'text-[hsl(var(--term-dim))] hover:text-[hsl(var(--term-dim)/0.7)]'
            }`}
          >
            <span className="text-[hsl(var(--term-dim))] mr-1 text-[10px]">[{tab.shortcut}]</span>
            {tab.label}
            {activeTab === tab.id && (
              <span
                className="absolute bottom-0 left-0 right-0 h-px"
                style={{ background: `hsl(var(${tabColor[tab.id]}))` }}
              />
            )}
          </button>
        ))}
        <div className="flex items-center gap-3 ml-auto text-[10px] text-[hsl(var(--term-dim))]">
          <span>AUTO-REFRESH 2m</span>
          <span>KEYS [1-6]</span>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto overscroll-contain p-4">
        {isLoading && !data && (
          <div className="space-y-4 animate-pulse">
            <div className="h-6 bg-white/5 w-64" />
            <div className="grid grid-cols-3 gap-3">
              {Array.from({ length: 6 }).map((_, i) => (
                <div key={i} className="h-24 bg-white/5" />
              ))}
            </div>
            <div className="h-64 bg-white/5" />
          </div>
        )}
        {!isLoading && !data && (
          <div className="text-center py-12 text-[hsl(var(--term-dim))]">
            <div className="text-sm mb-2">Failed to load {activeTab} data</div>
            <div className="text-xs">Check your connection and try again</div>
          </div>
        )}
        {data && tabComponents[activeTab]}
      </div>
    </div>
  );
}
