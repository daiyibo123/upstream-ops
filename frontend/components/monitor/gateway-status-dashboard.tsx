"use client"

import {
  Activity,
  Cpu,
  Gauge,
  KeyRound,
  RadioTower,
  Route,
  Server,
  ShieldCheck,
  Zap,
} from "lucide-react"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Pie,
  PieChart,
  RadialBar,
  RadialBarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useDashboardSummary } from "@/lib/queries"
import { formatRatio, formatTokens, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"

function formatBytes(value: number | null | undefined) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "0 B"
  if (n >= 1024 * 1024 * 1024) return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`
  if (n >= 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`
  if (n >= 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${n.toFixed(0)} B`
}

function formatDuration(seconds: number | null | undefined) {
  const n = Math.max(0, Math.floor(Number(seconds ?? 0)))
  const days = Math.floor(n / 86400)
  const hours = Math.floor((n % 86400) / 3600)
  const minutes = Math.floor((n % 3600) / 60)
  if (days > 0) return `${days}天 ${hours}小时`
  if (hours > 0) return `${hours}小时 ${minutes}分钟`
  return `${minutes}分钟`
}

function pct(value: number, total: number) {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) return 0
  return Math.max(0, Math.min(100, Math.round((value / total) * 100)))
}

function chartValue(value: number) {
  if (!Number.isFinite(value)) return "0"
  return value.toLocaleString("en-US")
}

interface TooltipItem {
  name?: string
  value?: number
  payload?: { label?: string; name?: string; value?: number }
}

function ChartTooltip({
  active,
  payload,
  valueFormatter = chartValue,
}: {
  active?: boolean
  payload?: TooltipItem[]
  valueFormatter?: (value: number) => string
}) {
  if (!active || !payload?.length) return null
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      {payload.map((item, index) => {
        const label = item.payload?.label ?? item.payload?.name ?? item.name ?? "数量"
        const value = item.value ?? item.payload?.value ?? 0
        return (
          <p key={`${label}-${index}`} className="flex min-w-32 items-center justify-between gap-4">
            <span className="text-muted-foreground">{label}</span>
            <span className="font-semibold text-foreground">{valueFormatter(value)}</span>
          </p>
        )
      })}
    </div>
  )
}

function Metric({
  label,
  value,
  sub,
  icon: Icon,
  tone,
}: {
  label: string
  value: string
  sub: string
  icon: typeof Activity
  tone: "brand" | "success" | "warning" | "danger" | "muted"
}) {
  const tones = {
    brand: "bg-brand/10 text-brand",
    success: "bg-success/10 text-success",
    warning: "bg-warning/10 text-warning",
    danger: "bg-danger/10 text-danger",
    muted: "bg-muted text-muted-foreground",
  }
  return (
    <div className="flex min-h-24 items-start justify-between gap-3 rounded-md border border-border bg-background p-4">
      <div className="min-w-0">
        <p className="text-xs text-muted-foreground">{label}</p>
        <p className="mt-1 truncate text-2xl font-bold tracking-tight text-foreground">{value}</p>
        <p className="mt-1 truncate text-xs text-muted-foreground">{sub}</p>
      </div>
      <span className={cn("flex size-10 shrink-0 items-center justify-center rounded-lg", tones[tone])}>
        <Icon className="size-5" />
      </span>
    </div>
  )
}

export function GatewayStatusDashboard() {
  const summary = useDashboardSummary()
  const gateway = summary.data?.gateway
  const server = summary.data?.server
  const groups = gateway?.groups ?? []
  const keys = gateway?.keys ?? []
  const totalGroups = gateway?.total_groups ?? 0
  const aliveGroups = gateway?.alive_groups ?? 0
  const deadGroups = gateway?.dead_groups ?? 0
  const unknownGroups = gateway?.unknown_groups ?? 0
  const healthPct = pct(aliveGroups, totalGroups)
  const dailyLimit = keys
    .filter((key) => key.enabled && key.daily_limit > 0)
    .reduce((sum, key) => sum + key.daily_limit, 0)
  const dailyUsed = gateway?.today_tokens ?? 0
  const dailyPct = dailyLimit > 0 ? pct(dailyUsed, dailyLimit) : Math.min(100, dailyUsed > 0 ? 100 : 0)
  const cheapest = gateway?.cheapest ?? null
  const dispatchOrder = groups
    .filter((group) => group.enabled !== false)
    .slice()
    .sort((a, b) => {
      const statusRank = (status: string) => (status === "alive" ? 0 : status === "unknown" ? 1 : 2)
      return (
        statusRank(a.status) - statusRank(b.status) ||
        (b.priority || 0) - (a.priority || 0) ||
        a.ratio - b.ratio ||
        a.failure_count - b.failure_count
      )
    })
  const serverOk = server?.status === "ok" && server?.database === "ok"

  const statusData = [
    { label: "存活", name: "alive", value: aliveGroups, fill: "var(--success)" },
    { label: "死亡", name: "dead", value: deadGroups, fill: "var(--danger)" },
    { label: "未知", name: "unknown", value: unknownGroups, fill: "var(--muted-foreground)" },
  ].filter((item) => item.value > 0)

  const tokenByKey = keys
    .slice()
    .sort((a, b) => b.total_tokens - a.total_tokens)
    .slice(0, 6)
    .map((key) => ({
      name: key.name || key.key_prefix,
      label: key.name || key.key_prefix,
      value: key.total_tokens,
    }))

  const tokenByGroup = groups
    .slice()
    .sort((a, b) => b.total_tokens - a.total_tokens)
    .slice(0, 6)
    .map((group) => ({
      name: `${group.channel_name}/${group.group_name}`,
      label: `${group.channel_name}/${group.group_name}`,
      value: group.total_tokens,
    }))

  const healthGauge = [{ name: "health", value: healthPct, fill: "var(--success)" }]
  const dailyGauge = [{ name: "daily", value: dailyPct, fill: dailyPct >= 90 ? "var(--danger)" : "var(--brand)" }]

  return (
    <Card className="border border-border shadow-none">
      <CardHeader className="flex flex-col gap-2 pb-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <CardTitle className="text-base font-semibold">实时调度状态</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">
            网关、渠道、Token 与服务器运行状态会跟随测活和请求使用动态刷新
          </p>
        </div>
        <div
          className={cn(
            "inline-flex w-fit items-center gap-2 rounded-md border px-3 py-1.5 text-xs",
            serverOk
              ? "border-success/20 bg-success/10 text-success"
              : "border-danger/20 bg-danger/10 text-danger",
          )}
        >
          <span className={cn("size-2 rounded-full", serverOk ? "bg-success" : "bg-danger")} />
          {serverOk ? "服务正常" : "服务异常"}
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
          <Metric
            label="可用渠道"
            value={`${aliveGroups}/${totalGroups}`}
            sub={deadGroups > 0 ? `${deadGroups} 个死亡等待复活检测` : "当前无死亡分组"}
            icon={RadioTower}
            tone={deadGroups > 0 ? "warning" : "success"}
          />
          <Metric
            label="最便宜可用"
            value={cheapest ? formatRatio(cheapest.ratio) : "—"}
            sub={cheapest ? `${cheapest.channel_name} / ${cheapest.group_name}` : "暂无可调度分组"}
            icon={Route}
            tone="brand"
          />
          <Metric
            label="今日 Token"
            value={formatTokens(dailyUsed)}
            sub={dailyLimit > 0 ? `额度 ${formatTokens(dailyLimit)} · ${dailyPct}%` : "未设置每日上限"}
            icon={Zap}
            tone={dailyPct >= 90 ? "danger" : "warning"}
          />
          <Metric
            label="本地 Key"
            value={`${gateway?.enabled_keys ?? 0}/${gateway?.total_keys ?? 0}`}
            sub={`累计 ${formatTokens(gateway?.total_tokens)} token`}
            icon={KeyRound}
            tone="muted"
          />
        </div>

        <div className="grid grid-cols-1 gap-3 xl:grid-cols-[1.05fr_1.4fr_1.05fr]">
          <div className="rounded-md border border-border bg-background p-4">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-foreground">调度健康度</p>
                <p className="text-xs text-muted-foreground">测活结果按分组 Key 统计</p>
              </div>
              <ShieldCheck className="size-4 text-success" />
            </div>
            <div className="grid grid-cols-1 items-center gap-2 sm:grid-cols-[1fr_0.9fr]">
              <div className="h-44">
                <ResponsiveContainer width="100%" height="100%">
                  <RadialBarChart data={healthGauge} innerRadius="72%" outerRadius="100%" startAngle={180} endAngle={0}>
                    <RadialBar dataKey="value" cornerRadius={8} background={{ fill: "var(--muted)" }} />
                  </RadialBarChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-3 text-xs">
                <div>
                  <p className="text-3xl font-bold tracking-tight text-foreground">{healthPct}%</p>
                  <p className="text-muted-foreground">存活率</p>
                </div>
                <div className="space-y-1.5">
                  <p className="flex justify-between gap-2">
                    <span className="text-muted-foreground">存活</span>
                    <span className="font-semibold text-success">{aliveGroups}</span>
                  </p>
                  <p className="flex justify-between gap-2">
                    <span className="text-muted-foreground">死亡</span>
                    <span className="font-semibold text-danger">{deadGroups}</span>
                  </p>
                  <p className="flex justify-between gap-2">
                    <span className="text-muted-foreground">未知</span>
                    <span className="font-semibold text-muted-foreground">{unknownGroups}</span>
                  </p>
                </div>
              </div>
            </div>
          </div>

          <div className="rounded-md border border-border bg-background p-4">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-foreground">Token 用量分布</p>
                <p className="text-xs text-muted-foreground">按本地 Key 统计，便于看每个 Key 的计费情况</p>
              </div>
              <Gauge className="size-4 text-brand" />
            </div>
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-[0.8fr_1.2fr]">
              <div className="h-48">
                <ResponsiveContainer width="100%" height="100%">
                  <RadialBarChart data={dailyGauge} innerRadius="72%" outerRadius="100%" startAngle={90} endAngle={-270}>
                    <RadialBar dataKey="value" cornerRadius={8} background={{ fill: "var(--muted)" }} />
                  </RadialBarChart>
                </ResponsiveContainer>
              </div>
              <div className="h-48 min-w-0">
                {tokenByKey.length === 0 ? (
                  <div className="flex h-full items-center justify-center text-xs text-muted-foreground">
                    暂无 Key 使用量
                  </div>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={tokenByKey} margin={{ top: 4, right: 8, left: -12, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
                      <XAxis
                        dataKey="name"
                        tickLine={false}
                        axisLine={false}
                        tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
                      />
                      <YAxis
                        tickLine={false}
                        axisLine={false}
                        tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
                        tickFormatter={formatTokens}
                      />
                      <Tooltip content={<ChartTooltip valueFormatter={formatTokens} />} cursor={{ fill: "var(--muted)" }} />
                      <Bar dataKey="value" radius={[4, 4, 0, 0]} fill="var(--brand)" />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </div>
            </div>
            <div className="mt-3 grid grid-cols-3 gap-2 border-t border-border pt-3 text-xs">
              <p>
                <span className="block text-muted-foreground">总用量</span>
                <span className="font-semibold text-foreground">{formatTokens(gateway?.total_tokens)}</span>
              </p>
              <p>
                <span className="block text-muted-foreground">Prompt</span>
                <span className="font-semibold text-foreground">{formatTokens(gateway?.prompt_tokens)}</span>
              </p>
              <p>
                <span className="block text-muted-foreground">Completion</span>
                <span className="font-semibold text-foreground">{formatTokens(gateway?.completion_tokens)}</span>
              </p>
            </div>
          </div>

          <div className="rounded-md border border-border bg-background p-4">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-foreground">服务器状态</p>
                <p className="text-xs text-muted-foreground">运行时、数据库和资源占用</p>
              </div>
              <Server className={cn("size-4", serverOk ? "text-success" : "text-danger")} />
            </div>
            <div className="grid grid-cols-1 items-center gap-2 sm:grid-cols-[0.8fr_1fr]">
              <div className="h-44">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={statusData.length > 0 ? statusData : [{ label: "未知", value: 1, fill: "var(--muted)" }]} dataKey="value" nameKey="label" innerRadius={48} outerRadius={72} paddingAngle={2}>
                      {(statusData.length > 0 ? statusData : [{ label: "未知", value: 1, fill: "var(--muted)" }]).map((item) => (
                        <Cell key={item.label} fill={item.fill} />
                      ))}
                    </Pie>
                    <Tooltip content={<ChartTooltip />} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-2 text-xs">
                <p className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground">运行</span>
                  <span className="font-semibold text-foreground">{formatDuration(server?.uptime_seconds)}</span>
                </p>
                <p className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground">数据库</span>
                  <span className={cn("font-semibold", server?.database === "ok" ? "text-success" : "text-danger")}>
                    {server?.database ?? "—"}
                  </span>
                </p>
                <p className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground">内存</span>
                  <span className="font-semibold text-foreground">{formatBytes(server?.alloc_bytes)}</span>
                </p>
                <p className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground">协程</span>
                  <span className="font-semibold text-foreground">{server?.num_goroutine ?? 0}</span>
                </p>
                <p className="flex items-center justify-between gap-2">
                  <span className="text-muted-foreground">刷新</span>
                  <span className="font-semibold text-foreground">{relativeTime(server?.server_time)}</span>
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-3 xl:grid-cols-[1.3fr_0.7fr]">
          <div className="rounded-md border border-border bg-background p-4">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-foreground">上游分组用量</p>
                <p className="text-xs text-muted-foreground">调度器会优先选择存活、优先级更高且同级倍率更低的分组</p>
              </div>
              <Activity className="size-4 text-warning" />
            </div>
            <div className="h-52">
              {tokenByGroup.length === 0 ? (
                <div className="flex h-full items-center justify-center text-xs text-muted-foreground">
                  暂无上游调用记录
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={tokenByGroup} layout="vertical" margin={{ top: 4, right: 16, left: 16, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" horizontal={false} />
                    <XAxis
                      type="number"
                      tickLine={false}
                      axisLine={false}
                      tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
                      tickFormatter={formatTokens}
                    />
                    <YAxis
                      type="category"
                      dataKey="name"
                      width={120}
                      tickLine={false}
                      axisLine={false}
                      tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
                    />
                    <Tooltip content={<ChartTooltip valueFormatter={formatTokens} />} cursor={{ fill: "var(--muted)" }} />
                    <Bar dataKey="value" radius={[0, 4, 4, 0]} fill="var(--warning)" />
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>

          <div className="rounded-md border border-border bg-background p-4">
            <div className="mb-3 flex items-center justify-between gap-2">
              <div>
                <p className="text-sm font-semibold text-foreground">省钱调度顺序</p>
                <p className="text-xs text-muted-foreground">按存活状态、优先级和低倍率展示</p>
              </div>
              <Cpu className="size-4 text-muted-foreground" />
            </div>
            <div className="space-y-2">
              {dispatchOrder
                .slice(0, 5)
                .map((group) => (
                  <div key={group.id} className="flex items-center justify-between gap-2 rounded-md border border-border px-3 py-2">
                    <div className="min-w-0">
                      <p className="truncate text-xs font-medium text-foreground">
                        {group.channel_name} / {group.group_name}
                      </p>
                      <p className="truncate text-[11px] text-muted-foreground">
                        优先级 {group.priority || 0} · 倍率 {formatRatio(group.ratio)} · {relativeTime(group.last_used_at)}
                      </p>
                    </div>
                    <span
                      className={cn(
                        "size-2 shrink-0 rounded-full",
                        group.status === "alive"
                          ? "bg-success"
                          : group.status === "dead"
                            ? "bg-danger"
                            : "bg-muted-foreground/40",
                      )}
                    />
                  </div>
                ))}
              {groups.length === 0 ? (
                <div className="rounded-md border border-border px-3 py-6 text-center text-xs text-muted-foreground">
                  暂无分组 Key
                </div>
              ) : null}
            </div>
          </div>
        </div>

        {summary.error ? <p className="text-xs text-danger">{summary.error}</p> : null}
      </CardContent>
    </Card>
  )
}
