import { GatewayPanel } from "@/components/monitor/gateway-panel"

export default function KeysPage() {
  return (
    <section className="space-y-4">
      <header className="flex flex-col gap-2">
        <div>
          <h1 className="text-lg font-semibold text-foreground">{"创建 Key"}</h1>
          <p className="text-xs text-muted-foreground">
            {"创建调用 Key、设置额度、绑定上游分组，并查看每个 Key 的用量和费用。"}
          </p>
        </div>
      </header>
      <GatewayPanel section="keys" />
    </section>
  )
}
