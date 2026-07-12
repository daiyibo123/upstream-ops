"use client"

import { useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import { Activity, ArrowRight, Copy, Eye, EyeOff, RadioTower, Route, ShieldCheck, Sparkles, Zap } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card } from "@/components/ui/card"
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
import { formatRatio, relativeTime } from "@/lib/format"
import { useAppVersion } from "@/lib/queries"
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
  total_channels: number
  active_channels: number
  upstream_groups: number
  available_groups: number
  openai_groups: number
  claude_groups: number
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

function formatTokens(value: number | null | undefined) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "0"
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

function maskPublicKey(value: string) {
  if (!value) return ""
  if (value.length <= 12) return "********"
  return `${value.slice(0, 6)}******${value.slice(-4)}`
}

function Metric({ label, value, icon: Icon }: { label: string; value: string; icon: typeof Activity }) {
  return (
    <div className="rounded-md border border-white/10 bg-white/[0.06] p-4 backdrop-blur">
      <div className="mb-3 flex size-9 items-center justify-center rounded-md bg-cyan-400/15 text-cyan-200">
        <Icon className="size-4" />
      </div>
      <p className="text-xs text-slate-400">{label}</p>
      <p className="mt-1 text-2xl font-semibold text-white">{value}</p>
    </div>
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
  const appVersion = useAppVersion()
  const appTitle = appVersion.data?.title?.trim() || "UpstreamOps"

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

  async function fetchPublicKey(password = "") {
    const res = await apiFetch<PublicKeyReveal>("/public/key/reveal", {
      method: "POST",
      body: JSON.stringify({ password }),
      skipAuthErrorHandler: true,
    })
    setRevealedPublicKey(res.key)
    setPublicKeyVisible(true)
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
        toast.success("公益 Key 已复制")
      } else {
        toast.success("公益 Key 已显示")
      }
    } catch (e) {
      const err = e as Error
      toast.error(err.message || (copyAfterReveal ? "复制公益 Key 失败" : "查看公益 Key 失败"))
    } finally {
      setCopyingPublicKey(false)
    }
  }

  function handlePublicKeyRevealClick() {
    if (revealedPublicKey) {
      setPublicKeyVisible(true)
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
    if (!revealedPublicKey && publicKey?.password_required) {
      setPublicKeyIntent("copy")
      setPublicKeyOpen(true)
      return
    }
    setCopyingPublicKey(true)
    try {
      const key = revealedPublicKey || (await fetchPublicKey(""))
      await navigator.clipboard.writeText(key)
      toast.success("公益 Key 已复制")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "复制公益 Key 失败")
    } finally {
      setCopyingPublicKey(false)
    }
  }

  return (
    <main className="min-h-screen overflow-hidden bg-[#071018] text-white">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_20%_10%,rgba(34,211,238,0.20),transparent_34%),radial-gradient(circle_at_80%_20%,rgba(16,185,129,0.16),transparent_28%),linear-gradient(135deg,#071018_0%,#0f172a_52%,#111827_100%)]" />
      <div className="absolute inset-x-0 top-0 h-px bg-linear-to-r from-transparent via-cyan-300/60 to-transparent" />

      <div className="relative mx-auto flex min-h-screen max-w-7xl flex-col px-4 py-5 sm:px-6 lg:px-8">
        <header className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <span className="flex size-9 items-center justify-center rounded-md bg-cyan-400/15 text-cyan-200 ring-1 ring-cyan-300/20">
              <Route className="size-5" />
            </span>
            <div>
              <p className="text-sm font-semibold">{appTitle}</p>
              <p className="text-xs text-slate-400">AI 调度网关</p>
            </div>
          </div>
          <Button asChild className="bg-white text-slate-950 hover:bg-cyan-100">
            <Link to="/login">
              登录
              <ArrowRight className="size-4" />
            </Link>
          </Button>
        </header>

        <section className="grid flex-1 items-center gap-8 py-10 lg:grid-cols-[1fr_0.9fr]">
          <div className="max-w-3xl">
            <Badge className="mb-5 border-cyan-300/20 bg-cyan-300/10 text-cyan-100 hover:bg-cyan-300/10">
              <Sparkles className="size-3.5" />
              实时调度状态公开页
            </Badge>
            <h1 className="text-4xl font-semibold leading-tight text-white sm:text-6xl">
              {appTitle}
            </h1>
            <p className="mt-5 max-w-2xl text-base leading-7 text-slate-300 sm:text-lg">
              按存活状态、人工优先级和低倍率调度，上游恢复后自动回到更合适的线路。公开页只展示概览，不暴露敏感配置。
            </p>
            {summary?.public_key_enabled && publicKey?.status === "available" ? (
              <div className="mt-6 max-w-2xl rounded-md border border-cyan-300/20 bg-cyan-300/10 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="truncate text-sm font-semibold text-cyan-50">{publicKey.name || "公益 Key"}</p>
                    <p className="mt-1 text-xs text-cyan-100/75">
                      今日 {formatTokens(publicKey.today_tokens)} · 累计 {formatTokens(publicKey.total_tokens)}
                      {publicKey.expires_at ? ` · 有效期至 ${publicKey.expires_at}` : ""}
                    </p>
                  </div>
                  <Badge className="bg-cyan-200 text-slate-950 hover:bg-cyan-200">
                    {publicKey.password_required ? "需密码" : "可直接查看"}
                  </Badge>
                </div>
                <div className="mt-3 flex flex-col gap-2 sm:flex-row">
                  <div className="flex min-w-0 flex-1 items-center gap-2 rounded-md border border-cyan-200/20 bg-slate-950/40 px-3 py-2">
                    <span className="min-w-0 flex-1 truncate font-mono text-sm text-cyan-50">
                      {revealedPublicKey
                        ? publicKeyVisible
                          ? revealedPublicKey
                          : maskPublicKey(revealedPublicKey)
                        : "验证后显示"}
                    </span>
                    <Button
                      type="button"
                      size="icon"
                      variant="ghost"
                      className="size-8 shrink-0 text-cyan-100 hover:bg-cyan-300/10 hover:text-white"
                      disabled={!revealedPublicKey}
                      onClick={() => setPublicKeyVisible((value) => !value)}
                      title={publicKeyVisible ? "隐藏公益 Key" : "查看公益 Key"}
                    >
                      {publicKeyVisible ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
                    </Button>
                  </div>
                  <Button
                    className="bg-cyan-300 text-slate-950 hover:bg-cyan-200 sm:w-24"
                    disabled={copyingPublicKey}
                    onClick={handlePublicKeyRevealClick}
                  >
                    <Eye className="size-4" />
                    {copyingPublicKey ? "处理中..." : "查看"}
                  </Button>
                  <Button
                    variant="outline"
                    className="border-cyan-200/30 bg-white/5 text-cyan-50 hover:bg-cyan-300/10 hover:text-white sm:w-24"
                    disabled={copyingPublicKey}
                    onClick={() => void handlePublicKeyCopy()}
                  >
                    <Copy className="size-4" />
                    复制
                  </Button>
                </div>
              </div>
            ) : null}
            <div className="mt-8 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <Metric label="渠道状态" value={`${summary?.active_channels ?? 0}/${summary?.total_channels ?? 0}`} icon={RadioTower} />
              <Metric label="上游分组" value={String(summary?.upstream_groups ?? 0)} icon={Activity} />
              <Metric label="可用分组" value={String(summary?.available_groups ?? 0)} icon={ShieldCheck} />
              <Metric label="今日用量" value={formatTokens(summary?.today_tokens)} icon={Zap} />
            </div>
          </div>

          <Card className="border-white/10 bg-white/[0.07] p-4 text-white shadow-2xl shadow-cyan-950/30 backdrop-blur">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <p className="text-sm font-semibold">省钱调度顺序</p>
                <p className="text-xs text-slate-400">优先级优先，同级低倍率优先</p>
              </div>
              <Badge className="bg-emerald-400/15 text-emerald-100 hover:bg-emerald-400/15">
                {summary?.gateway_status ?? "online"}
              </Badge>
            </div>
            <div className="h-80 overflow-hidden">
              <div className={preview.length > 3 ? "public-dispatch-scroll space-y-2" : "space-y-2"}>
              {[...preview, ...(preview.length > 3 ? preview : [])].map((group, index) => (
                <div key={`${group.id}-${index}`} className="rounded-md border border-white/10 bg-slate-950/35 px-3 py-3">
                  <div className="flex items-center justify-between gap-3">
                    <p className="min-w-0 truncate text-sm font-medium">
                      {group.site_domain || group.channel_name} / {group.group_name}
                    </p>
                    <span className="rounded bg-cyan-300/10 px-2 py-1 font-mono text-xs text-cyan-100">
                      {formatRatio(group.ratio)}
                    </span>
                  </div>
                  <p className="mt-1 text-xs text-slate-400">
                    {group.charity ? "公益 · " : ""}优先级 {group.priority || 0} · {group.status} · {group.last_used_at ? relativeTime(group.last_used_at) : "未使用"}
                  </p>
                </div>
              ))}
              </div>
              {preview.length === 0 ? (
                <div className="rounded-md border border-dashed border-white/15 px-3 py-8 text-center text-sm text-slate-400">
                  暂无可展示分组
                </div>
              ) : null}
            </div>
            <div className="mt-4 grid grid-cols-2 gap-2 text-xs text-slate-300">
              <div className="rounded-md bg-white/[0.05] p-3">
                OpenAI 分组
                <span className="mt-1 block text-lg font-semibold text-white">{summary?.openai_groups ?? 0}</span>
              </div>
              <div className="rounded-md bg-white/[0.05] p-3">
                Claude 分组
                <span className="mt-1 block text-lg font-semibold text-white">{summary?.claude_groups ?? 0}</span>
              </div>
            </div>
          </Card>
        </section>
        {error ? <p className="pb-4 text-xs text-red-300">{error}</p> : null}
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
                if (event.key === "Enter") void revealPublicKey(publicKeyPassword)
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
