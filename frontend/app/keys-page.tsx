import { GatewayPanel } from "@/components/monitor/gateway-panel"
import { PublicKeyConfigCard } from "@/components/monitor/public-key-config-card"

export default function KeysPage() {
  return (
    <section className="space-y-4">
      <header className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">{"创建 Key"}</h1>
          <p className="text-xs text-muted-foreground">
            {"创建调用 Key、绑定上游分组，并配置首页公益 Key。"}
          </p>
        </div>
        <PublicKeyConfigCard />
      </header>
      <GatewayPanel section="keys" />
    </section>
  )
}
