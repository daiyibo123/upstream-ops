import { KeyRound } from "lucide-react"
import { GatewayPanel } from "@/components/monitor/gateway-panel"
import { PageHeader } from "@/components/page-header"

export default function KeysPage() {
  return (
    <section className="space-y-5">
      <PageHeader
        icon={<KeyRound className="size-[18px]" />}
        title="API Key"
        description="创建调用密钥、设置额度和渠道范围；默认按倍率优先调度，选中固定号池后再在存活账号中轮询。"
      />
      <GatewayPanel section="keys" />
    </section>
  )
}
