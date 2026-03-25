export default function MacroTerminalPage() {
  return (
    <div style={{ width: "100%", height: "calc(100vh - 0px)", overflow: "hidden" }}>
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
  );
}
