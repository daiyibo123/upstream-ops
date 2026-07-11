import { CaptchaStatus } from "@/components/monitor/bottom-panels"

export default function CaptchaPage() {
  return (
    <section className="space-y-3">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">{"打码平台"}</h1>
          <p className="text-xs text-muted-foreground">
            {"配置 CapSolver / 2Captcha 等 provider；启用 Turnstile 的渠道会自动调用。"}
          </p>
        </div>
      </header>
      <CaptchaStatus />
    </section>
  )
}
