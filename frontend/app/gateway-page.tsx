import { Server } from "lucide-react"
import { GatewayPanel } from "@/components/monitor/gateway-panel"
import { PageHeader } from "@/components/page-header"

export default function GatewayPage() {
  return (
    <section className="space-y-5">
      <PageHeader
        icon={<Server className="size-[18px]" />}
        title="可用渠道"
        description="管理参与调度的上游分组、固定号池映射、优先级、并发和健康状态。"
      />
      <GatewayPanel section="groups" />
    </section>
  )
}
