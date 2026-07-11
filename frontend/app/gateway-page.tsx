import { GatewayPanel } from "@/components/monitor/gateway-panel"

export default function GatewayPage() {
  return (
    <section className="space-y-3">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">{"可用渠道"}</h1>
          <p className="text-xs text-muted-foreground">
            {"上游分组列表：状态、启用、公益、格式、优先级、并发上限，可解除冷却或删除。"}
          </p>
        </div>
      </header>
      <GatewayPanel section="groups" />
    </section>
  )
}
