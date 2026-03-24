import OptionsPage from "./options";

const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";

export default function TradierPage() {
  return (
    <OptionsPage
      apiBase={`${AGENT_BACKEND_URL}/api/tradier`}
      queryApiBase={AGENT_BACKEND_URL}
      pageTitle="TRADIER"
      queryPresetIntent="tradier_flow"
      enableContractDetail
      enableTimeSales
      dataSourceLabel="Tradier"
    />
  );
}
