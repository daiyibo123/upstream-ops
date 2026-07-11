import { KpiRow } from "@/components/monitor/kpi-row"
import { BalanceOverview } from "@/components/monitor/balance-overview"
import { MultiplierChanges } from "@/components/monitor/multiplier-changes"
import { ChannelCards } from "@/components/monitor/channel-cards"
import { GatewayPanel } from "@/components/monitor/gateway-panel"
import { GatewayStatusDashboard } from "@/components/monitor/gateway-status-dashboard"
import { BottomPanels } from "@/components/monitor/bottom-panels"

export default function Page() {
  return (
    <>
      <KpiRow />

      <GatewayStatusDashboard />

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-5">
        <div className="lg:col-span-3">
          <BalanceOverview />
        </div>
        <div className="lg:col-span-2">
          <MultiplierChanges />
        </div>
      </div>

      <ChannelCards />

      <GatewayPanel />

      <BottomPanels />
    </>
  )
}
