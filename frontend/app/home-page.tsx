"use client"

import { useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import {
  Activity,
  ArrowRight,
  Copy,
  Eye,
  EyeOff,
  Gauge,
  HeartHandshake,
  RadioTower,
  Route,
  ShieldCheck,
  Sparkles,
  Zap,
} from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card } from "@/components/ui/card"
import { ThemeToggle } from "@/components/theme-toggle"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { apiFetch } from "@/lib/api"
import { dateTime, formatRatio, formatTokens, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import type { DashboardGatewayGroup } from "@/lib/api-types"

interface PublicKeyStat {
  enabled: boolean
  name: string
  password_required: boolean
  password_hint?: string
  expires_at?: string
  status: "available" | "expired" | "disabled" | string
  today_tokens: number
  total_tokens: number
  last_used_at?: string | null
}

interface PublicSummary {
  title: string
  total_channels: number
  active_channels: number
  upstream_groups: number
  available_groups: number
  openai_groups: number
  claude_groups: number
  grok_groups: number
  today_tokens: number
  total_tokens: number
  cheapest?: DashboardGatewayGroup | null
  dispatch_preview: DashboardGatewayGroup[]
  supported_formats: string[]
  gateway_status: string
  public_key?: PublicKeyStat
  public_key_enabled: boolean
}

interface PublicKeyReveal {
  key: string
  name: string
  expires_at?: string
}

function maskPublicKey(value: string) {
  if (!value) return ""
  if (value.length <= 12) return "********"
  return `${value.slice(0, 6)}******${value.slice(-4)}`
}

function pct(value: number, total: number) {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) return 0
  return Math.max(0, Math.min(100, Math.round((value / total) * 100)))
}

function publicKeyErrorMessage(message: string, action: "copy" | "view") {
  if (message === "public key expired") {
    return action === "copy" ? "Key 已过期，无法复制" : "Key 已过期，无法查看"
  }
  if (message === "public key password mismatch") return "复制密码不正确"
  if (message === "public key is not available") return "暂无可用的公益 Key"
  return message
}

function StatusMetric({
  label,
  value,
  sub,
  icon: Icon,
  tone = "brand",
}: {
  label: string
  value: string
  sub?: string
  icon: typeof Activity
  tone?: "brand" | "success" | "warning" | "muted"
}) {
  const tones = {
    brand: "bg-brand/10 text-brand",
    success: "bg-success/10 text-success",
    warning: "bg-warning/10 text-warning",
    muted: "bg-muted text-muted-foreground",
  }
  return (
    <div className="rounded-md border border-border bg-background/80 p-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="mt-1 truncate text-xl font-semibold tracking-tight text-foreground">{value}</p>
        </div>
        <span className={cn("flex size-9 shrink-0 items-center justify-center rounded-lg", tones[tone])}>
          <Icon className="size-4" />
        </span>
      </div>
      {sub ? <p className="mt-2 truncate text-[11px] text-muted-foreground">{sub}</p> : null}
    </div>
  )
}

function PublicGatewayStatusCard({
  summary,
  preview,
}: {
  summary: PublicSummary | null
  preview: DashboardGatewayGroup[]
}) {
  const totalGroups = summary?.upstream_groups ?? 0
  const availableGroups = summary?.available_groups ?? 0
  const aliveGroups = summary?.active_channels ?? 0
  const healthPct = pct(availableGroups, totalGroups)
  const cheapest = summary?.cheapest ?? null

  return (
    <Card className="border-border/80 bg-card/90 p-4 shadow-sm backdrop-blur">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <p className="text-base font-semibold text-foreground">实时调度状态</p>
          <p className="mt-1 text-xs text-muted-foreground">公开展示网关、分组和 Token 用量，不暴露敏感配置。</p>
        </div>
        <Badge variant="outline" className="border-success/20 bg-success/10 text-success">
          {summary?.gateway_status ?? "online"}
        </Badge>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <StatusMetric
          label="可用分组"
          value={`${availableGroups}/${totalGroups}`}
          sub={`${aliveGroups} 个存活分组`}
          icon={RadioTower}
          tone={healthPct >= 80 ? "success" : "warning"}
        />
        <StatusMetric
          label="今日 Token"
          value={formatTokens(summary?.today_tokens)}
          sub={`累计 ${formatTokens(summary?.total_tokens)}`}
          icon={Zap}
          tone="warning"
        />
        <StatusMetric
          label="最低倍率"
          value={cheapest ? formatRatio(cheapest.ratio) : "—"}
          sub={cheapest ? `${cheapest.channel_name} / ${cheapest.group_name}` : "暂无可调度分组"}
          icon={Route}
        />
        <StatusMetric
          label="格式覆盖"
          value={`${summary?.openai_groups ?? 0}/${summary?.claude_groups ?? 0}/${summary?.grok_groups ?? 0}`}
          sub="OpenAI / Claude / Grok"
          icon={Gauge}
          tone="muted"
        />
      </div>

      <div className="mt-4 rounded-md border border-border bg-background/80 p-3">
        <div className="mb-2 flex items-center justify-between text-xs">
          <span className="text-muted-foreground">调度可用率</span>
          <span className="font-semibold text-foreground">{healthPct}%</span>
        </div>
        <div className="h-2 overflow-hidden rounded-full bg-muted">
          <div
            className="h-full rounded-full bg-success transition-all"
            style={{ width: `${healthPct}%` }}
          />
        </div>
      </div>

      <div className="mt-4 space-y-2">
        <div className="flex items-center justify-between gap-3">
          <p className="text-sm font-medium text-foreground">调度预览</p>
          <p className="text-xs text-muted-foreground">低倍率优先展示</p>
        </div>
        {preview.slice(0, 4).map((group) => (
          <div key={group.id} className="flex items-center justify-between gap-3 rounded-md border border-border bg-background/70 px-3 py-2">
            <div className="min-w-0">
              <p className="truncate text-xs font-medium text-foreground">
                {group.site_domain || group.channel_name} / {group.group_name}
              </p>
              <p className="truncate text-[11px] text-muted-foreground">
                {group.charity ? "公益 · " : ""}优先级 {group.priority || 0} · {group.last_used_at ? relativeTime(group.last_used_at) : "未使用"}
              </p>
            </div>
            <span className="rounded bg-brand/10 px-2 py-1 font-mono text-xs text-brand">
              {formatRatio(group.ratio)}
            </span>
          </div>
        ))}
        {preview.length === 0 ? (
          <div className="rounded-md border border-dashed border-border px-3 py-6 text-center text-xs text-muted-foreground">
            暂无可展示分组
          </div>
        ) : null}
      </div>
    </Card>
  )
}

export default function HomePage() {
  const [summary, setSummary] = useState<PublicSummary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [publicKeyOpen, setPublicKeyOpen] = useState(false)
  const [publicKeyPassword, setPublicKeyPassword] = useState("")
  const [copyingPublicKey, setCopyingPublicKey] = useState(false)
  const [revealedPublicKey, setRevealedPublicKey] = useState("")
  const [publicKeyVisible, setPublicKeyVisible] = useState(false)
  const [publicKeyIntent, setPublicKeyIntent] = useState<"view" | "copy">("view")
  const appTitle = summary?.title?.trim() || "UpstreamOps"

  useEffect(() => {
    document.title = appTitle
  }, [appTitle])

  useEffect(() => {
    apiFetch<PublicSummary>("/public/summary", { skipAuthErrorHandler: true })
      .then(setSummary)
      .catch((e: Error) => setError(e.message))
  }, [])

  const preview = useMemo(
    () => {
      return (summary?.dispatch_preview ?? [])
        .slice()
        .sort((a, b) => a.ratio - b.ratio || a.id - b.id)
        .slice(0, 10)
    },
    [summary],
  )
  const publicKey = summary?.public_key
  const publicKeyAvailable = Boolean(summary?.public_key_enabled && publicKey?.status === "available")
  const publicKeyExpired = Boolean(summary?.public_key_enabled && publicKey?.status === "expired")
  const publicKeyFieldText = publicKeyAvailable
    ? revealedPublicKey
      ? publicKeyVisible
        ? revealedPublicKey
        : maskPublicKey(revealedPublicKey)
      : publicKey?.password_required
        ? "输入密码后显示或复制"
        : "点击复制或小眼睛获取"
    : publicKeyExpired
      ? "Key 已过期"
      : "暂无可用"

  async function fetchPublicKey(password = "") {
    const res = await apiFetch<PublicKeyReveal>("/public/key/reveal", {
      method: "POST",
      body: JSON.stringify({ password }),
      skipAuthErrorHandler: true,
    })
    setRevealedPublicKey(res.key)
    setPublicKeyOpen(false)
    setPublicKeyPassword("")
    return res.key
  }

  async function revealPublicKey(password = "", copyAfterReveal = false) {
    setCopyingPublicKey(true)
    try {
      const key = await fetchPublicKey(password)
      if (copyAfterReveal) {
        await navigator.clipboard.writeText(key)
        setPublicKeyVisible(false)
        toast.success("公益 Key 已复制")
      } else {
        setPublicKeyVisible(true)
        toast.success("公益 Key 已显示")
      }
    } catch (e) {
      const err = e as Error
      toast.error(publicKeyErrorMessage(err.message, copyAfterReveal ? "copy" : "view"))
    } finally {
      setCopyingPublicKey(false)
    }
  }

  function handlePublicKeyRevealClick() {
    if (publicKeyExpired) {
      toast.error("Key 已过期，无法查看")
      return
    }
    if (!publicKeyAvailable) {
      toast.info("暂无可用的公益 Key")
      return
    }
    if (revealedPublicKey) {
      setPublicKeyVisible((value) => !value)
      return
    }
    if (publicKey?.password_required) {
      setPublicKeyIntent("view")
      setPublicKeyOpen(true)
      return
    }
    void revealPublicKey("")
  }

  async function handlePublicKeyCopy() {
    if (publicKeyExpired) {
      toast.error("Key 已过期，无法复制")
      return
    }
    if (!publicKeyAvailable) {
      toast.info("暂无可用的公益 Key")
      return
    }
    if (!revealedPublicKey && publicKey?.password_required) {
      setPublicKeyIntent("copy")
      setPublicKeyOpen(true)
      return
    }
    setCopyingPublicKey(true)
    try {
      const key = revealedPublicKey || (await fetchPublicKey(""))
      await navigator.clipboard.writeText(key)
      setPublicKeyVisible(false)
      toast.success("公益 Key 已复制")
    } catch (e) {
      const err = e as Error
      toast.error(publicKeyErrorMessage(err.message, "copy"))
    } finally {
      setCopyingPublicKey(false)
    }
  }

  return (
    <main className="relative min-h-screen overflow-hidden bg-background text-foreground">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_18%_8%,rgba(34,211,238,0.14),transparent_32%),radial-gradient(circle_at_85%_20%,rgba(16,185,129,0.10),transparent_28%)] dark:bg-[radial-gradient(circle_at_18%_8%,rgba(34,211,238,0.18),transparent_34%),radial-gradient(circle_at_85%_20%,rgba(16,185,129,0.14),transparent_30%),linear-gradient(135deg,#071018_0%,#0f172a_52%,#111827_100%)]" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-linear-to-r from-transparent via-brand/40 to-transparent" />

      <div className="relative mx-auto flex min-h-screen max-w-7xl flex-col px-4 py-5 sm:px-6 lg:px-8">
        <header className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <span className="flex size-9 items-center justify-center rounded-md bg-brand/10 text-brand ring-1 ring-brand/20">
              <Route className="size-5" />
            </span>
            <div>
              <p className="text-sm font-semibold text-foreground">{appTitle}</p>
              <p className="text-xs text-muted-foreground">AI 调度网关</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <ThemeToggle className="border-border bg-background/80 text-foreground hover:bg-muted" />
            <Button asChild>
              <Link to="/login">
                登录
                <ArrowRight className="size-4" />
              </Link>
            </Button>
          </div>
        </header>

        <section className="grid flex-1 gap-6 py-8 lg:grid-cols-[0.92fr_1.08fr] lg:items-start">
          <aside className="space-y-4 lg:sticky lg:top-5">
            <Card
              className={cn(
                "border-border/80 bg-card/90 p-4 shadow-sm backdrop-blur",
                publicKeyAvailable && "border-success/30",
                publicKeyExpired && "border-danger/30",
              )}
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <HeartHandshake className="size-4 text-success" />
                    <p className="truncate text-base font-semibold text-foreground">{publicKey?.name || "公益 Key"}</p>
                  </div>
                  <p className="mt-1 text-xs text-muted-foreground">
                    今日 {formatTokens(publicKey?.today_tokens)} · 累计 {formatTokens(publicKey?.total_tokens)}
                    {publicKey?.expires_at ? ` · 有效期至 ${dateTime(publicKey.expires_at)}` : ""}
                  </p>
                </div>
                <Badge
                  variant="outline"
                  className={cn(
                    publicKeyAvailable && "border-success/20 bg-success/10 text-success",
                    publicKeyExpired && "border-danger/20 bg-danger/10 text-danger",
                  )}
                >
                  {publicKeyAvailable ? (publicKey?.password_required ? "需密码" : "可直接复制") : publicKeyExpired ? "已过期" : "暂无可用"}
                </Badge>
              </div>

              <div className="mt-4 flex flex-col gap-2 sm:flex-row">
                <div
                  className={cn(
                    "flex min-w-0 flex-1 items-center gap-2 rounded-md border bg-background/80 px-3 py-2",
                    publicKeyExpired ? "border-danger/30" : "border-border",
                  )}
                >
                  <span className="min-w-0 flex-1 truncate font-mono text-sm text-foreground">
                    {publicKeyFieldText}
                  </span>
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="size-8 shrink-0 text-muted-foreground hover:bg-muted hover:text-foreground"
                    disabled={copyingPublicKey}
                    onClick={handlePublicKeyRevealClick}
                    title={publicKeyVisible ? "隐藏公益 Key" : "查看公益 Key"}
                  >
                    {publicKeyVisible ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
                  </Button>
                </div>
                <Button
                  variant="outline"
                  className="sm:w-24"
                  disabled={copyingPublicKey}
                  onClick={() => void handlePublicKeyCopy()}
                >
                  <Copy className="size-4" />
                  复制
                </Button>
              </div>
            </Card>

            <PublicGatewayStatusCard summary={summary} preview={preview} />
          </aside>

          <div className="space-y-4">
            <Card className="border-border/80 bg-card/90 p-6 shadow-sm backdrop-blur sm:p-8">
              <Badge className="mb-5 border-brand/20 bg-brand/10 text-brand hover:bg-brand/10">
                <Sparkles className="size-3.5" />
                实时调度状态公开页
              </Badge>
              <h1 className="text-4xl font-semibold leading-tight tracking-tight text-foreground sm:text-6xl">
                {appTitle}
              </h1>
              <p className="mt-5 max-w-2xl text-base leading-7 text-muted-foreground sm:text-lg">
                按存活状态、人工优先级和低倍率调度，上游恢复后自动回到更合适的线路。公开页只展示概览，敏感配置仍留在控制台内。
              </p>
              <div className="mt-8 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <StatusMetric label="渠道总数" value={String(summary?.total_channels ?? 0)} icon={RadioTower} tone="muted" />
                <StatusMetric label="上游分组" value={String(summary?.upstream_groups ?? 0)} icon={Activity} />
                <StatusMetric label="可用分组" value={String(summary?.available_groups ?? 0)} icon={ShieldCheck} tone="success" />
                <StatusMetric label="今日用量" value={formatTokens(summary?.today_tokens)} icon={Zap} tone="warning" />
              </div>
            </Card>

            <Card className="border-border/80 bg-card/90 p-4 shadow-sm backdrop-blur">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-foreground">省钱调度顺序</p>
                  <p className="text-xs text-muted-foreground">优先级优先，同级低倍率优先</p>
                </div>
                <Badge variant="outline" className="border-success/20 bg-success/10 text-success">
                  {summary?.gateway_status ?? "online"}
                </Badge>
              </div>
              <div className="h-80 overflow-hidden">
                <div className={preview.length > 3 ? "public-dispatch-scroll space-y-2" : "space-y-2"}>
                  {[...preview, ...(preview.length > 3 ? preview : [])].map((group, index) => (
                    <div key={`${group.id}-${index}`} className="rounded-md border border-border bg-background/70 px-3 py-3">
                      <div className="flex items-center justify-between gap-3">
                        <p className="min-w-0 truncate text-sm font-medium text-foreground">
                          {group.site_domain || group.channel_name} / {group.group_name}
                        </p>
                        <span className="rounded bg-brand/10 px-2 py-1 font-mono text-xs text-brand">
                          {formatRatio(group.ratio)}
                        </span>
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {group.charity ? "公益 · " : ""}优先级 {group.priority || 0} · {group.status} · {group.last_used_at ? relativeTime(group.last_used_at) : "未使用"}
                      </p>
                    </div>
                  ))}
                </div>
                {preview.length === 0 ? (
                  <div className="rounded-md border border-dashed border-border px-3 py-8 text-center text-sm text-muted-foreground">
                    暂无可展示分组
                  </div>
                ) : null}
              </div>
              <div className="mt-4 grid grid-cols-3 gap-2 text-xs text-muted-foreground">
                <div className="rounded-md bg-muted/50 p-3">
                  OpenAI 分组
                  <span className="mt-1 block text-lg font-semibold text-foreground">{summary?.openai_groups ?? 0}</span>
                </div>
                <div className="rounded-md bg-muted/50 p-3">
                  Claude 分组
                  <span className="mt-1 block text-lg font-semibold text-foreground">{summary?.claude_groups ?? 0}</span>
                </div>
                <div className="rounded-md bg-muted/50 p-3">
                  Grok 分组
                  <span className="mt-1 block text-lg font-semibold text-foreground">{summary?.grok_groups ?? 0}</span>
                </div>
              </div>
            </Card>
          </div>
        </section>
        {error ? <p className="pb-4 text-xs text-destructive">{error}</p> : null}
      </div>

      <Dialog open={publicKeyOpen} onOpenChange={setPublicKeyOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{publicKeyIntent === "copy" ? "复制公益 Key" : "查看公益 Key"}</DialogTitle>
            <DialogDescription>
              {publicKey?.password_hint || "请输入后台设置的复制密码。"}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="public-key-password">复制密码</Label>
            <Input
              id="public-key-password"
              type="password"
              value={publicKeyPassword}
              onChange={(event) => setPublicKeyPassword(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") void revealPublicKey(publicKeyPassword, publicKeyIntent === "copy")
              }}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPublicKeyOpen(false)} disabled={copyingPublicKey}>
              取消
            </Button>
            <Button onClick={() => void revealPublicKey(publicKeyPassword, publicKeyIntent === "copy")} disabled={copyingPublicKey}>
              {copyingPublicKey ? (publicKeyIntent === "copy" ? "复制中..." : "查看中...") : publicKeyIntent === "copy" ? "复制" : "查看"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </main>
  )
}
