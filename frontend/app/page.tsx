import { LayoutDashboard } from "lucide-react"
import { KpiRow } from "@/components/monitor/kpi-row"
import { BalanceOverview } from "@/components/monitor/balance-overview"
import { MultiplierChanges } from "@/components/monitor/multiplier-changes"
import { GatewayStatusDashboard } from "@/components/monitor/gateway-status-dashboard"
import { PageHeader } from "@/components/page-header"

export default function Page() {
  return (
    <section className="space-y-5">
      <PageHeader
        icon={<LayoutDashboard className="size-[18px]" />}
        title="调度网关"
        description="查看网关运行状态、渠道健康度、Token 用量和倍率变化。"
      />
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
    </section>
  )
}
