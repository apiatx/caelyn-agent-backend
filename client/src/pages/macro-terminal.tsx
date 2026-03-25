import { MacroMarketSnapshot } from '@/components/macro-market-snapshot';

export default function MacroTerminalPage() {
  return (
    <div className="w-full min-h-screen bg-[#050608] text-white overflow-auto">
      {/* Live Market Snapshot — reads from /api/macro/dashboard */}
      <MacroMarketSnapshot />

      {/* Macro Terminal (indicators, charts, tabs) */}
      <div style={{ width: "100%", height: "calc(100vh - 200px)" }}>
        <iframe
          src="/macro-terminal/index.html"
          title="Macro Terminal"
          style={{
            width: "100%",
            height: "100%",
            border: "none",
            background: "#050608",
          }}
          sandbox="allow-scripts allow-same-origin"
        />
      </div>
    </div>
  );
}
