"use client"

import { useEffect, useMemo, useState } from "react"
import { Activity, ChevronLeft, ChevronRight, CircleCheckBig, Gauge, HeartHandshake, KeyRound, Loader2, RefreshCw, ScrollText, Trash2, Zap } from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import { dateTime, formatPercent, formatRatio, formatTokens, relativeTime } from "@/lib/format"
import type { UsageLogsResponse } from "@/lib/api-types"

const PAGE_SIZE = 50
type DetailView = "usage" | "events"

function cacheRateLabel(rate: number | null | undefined, promptTokens: number | null | undefined) {
  if (!promptTokens || promptTokens <= 0) return "—"
  return formatPercent(rate)
}

function cacheTokenLabel(cachedTokens: number | null | undefined, promptTokens: number | null | undefined) {
  if (!promptTokens || promptTokens <= 0) return "暂无输入缓存数据"
  return `${formatTokens(cachedTokens)} cached / ${formatTokens(promptTokens)} input`
}

function durationLabel(ms: number | null | undefined) {
  const n = Number(ms ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "—"
  if (n < 1000) return `${Math.round(n)}ms`
  if (n < 60_000) return `${(n / 1000).toFixed(n < 10_000 ? 2 : 1).replace(/\.0$/, "")}s`
  const minutes = Math.floor(n / 60_000)
  const seconds = Math.round((n % 60_000) / 1000)
  return `${minutes}m ${seconds}s`
}

function usageStatusLabel(status?: string | null) {
  switch ((status ?? "success").toLowerCase()) {
    case "success":
    case "estimated":
      return "完成"
    case "dispatching":
      return "调度中"
    case "switched":
      return "已切换"
    case "saturated":
      return "并发满"
    case "cooldown":
      return "冷却"
    case "interrupted":
      return "中断"
    case "failed":
      return "失败"
    default:
      return status || "完成"
  }
}

function usageStatusTone(status?: string | null) {
  switch ((status ?? "success").toLowerCase()) {
    case "success":
    case "estimated":
      return "border-success/20 bg-success/10 text-success"
    case "dispatching":
      return "border-warning/20 bg-warning/10 text-warning"
    case "switched":
    case "saturated":
    case "cooldown":
    case "interrupted":
      return "border-warning/20 bg-warning/10 text-warning"
    default:
      return "border-danger/20 bg-danger/10 text-danger"
  }
}

export default function UsagePage() {
  const [data, setData] = useState<UsageLogsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [clearing, setClearing] = useState(false)
  const [page, setPage] = useState(1)
  const [refreshTick, setRefreshTick] = useState(0)
  const [detailView, setDetailView] = useState<DetailView>("usage")

  const items = data?.items ?? []
  const keys = data?.keys ?? []
  const total = data?.total ?? 0
  const stats = data?.stats
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const rangeStart = total === 0 ? 0 : (page - 1) * PAGE_SIZE + 1
  const rangeEnd = Math.min(page * PAGE_SIZE, total)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    const offset = (page - 1) * PAGE_SIZE
    apiFetch<UsageLogsResponse>(`/gateway/usage-logs?view=${detailView}&limit=${PAGE_SIZE}&offset=${offset}`)
      .then((res) => {
        if (!cancelled) setData(res)
      })
      .catch((err: Error) => {
        if (!cancelled) toast.error(err.message || "加载使用记录失败")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [detailView, page, refreshTick])

  const rows = useMemo(() => items, [items])
  const keyRows = useMemo(
    () =>
      keys
        .slice()
        .sort((a, b) => b.total_tokens - a.total_tokens || b.today_tokens - a.today_tokens || a.name.localeCompare(b.name)),
    [keys],
  )

  async function clearUsageLogs() {
    if (total <= 0 || clearing) return
    if (!window.confirm("确定清空下面的调用明细吗？每个 Key 的今日/累计统计会保留。")) return
    setClearing(true)
    try {
      const res = await apiFetch<{ deleted: number }>("/gateway/usage-logs", { method: "DELETE" })
      setData((prev) => (prev ? {
        ...prev,
        items: [],
        total: 0,
        stats: {
          total_requests: 0,
          success_requests: 0,
          total_tokens: 0,
          avg_first_token_ms: 0,
          avg_duration_ms: 0,
        },
      } : prev))
      setPage(1)
      toast.success(`已清空 ${res.deleted ?? total} 条明细，Key 汇总未受影响`)
    } catch (err) {
      const error = err as Error
      toast.error(error.message || "清空使用记录失败")
    } finally {
      setClearing(false)
    }
  }

  return (
    <section className="space-y-3">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">{"使用记录"}</h1>
          <p className="text-xs text-muted-foreground">
            {"网关调用明细：时间、Key、渠道、分组、模型、Token 和倍率。"}
          </p>
        </div>
      </header>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {[
          {
            label: "明细请求",
            value: (stats?.total_requests ?? total).toLocaleString("zh-CN"),
            detail: "当前保留的调用记录",
            icon: Activity,
            tone: "border-brand/20 bg-brand/5 text-brand",
          },
          {
            label: "成功请求",
            value: (stats?.success_requests ?? stats?.total_requests ?? 0).toLocaleString("zh-CN"),
            detail: "失败与切换单独归入调度事件",
            icon: CircleCheckBig,
            tone: "border-success/20 bg-success/5 text-success",
          },
          {
            label: "明细 Token",
            value: formatTokens(stats?.total_tokens),
            detail: "随明细清理，不影响 Key 累计",
            icon: Zap,
            tone: "border-warning/20 bg-warning/5 text-warning",
          },
          {
            label: "平均首字",
            value: durationLabel(stats?.avg_first_token_ms),
            detail: `平均总耗时 ${durationLabel(stats?.avg_duration_ms)}`,
            icon: Gauge,
            tone: "border-border bg-muted/20 text-foreground",
          },
        ].map(({ label, value, detail, icon: Icon, tone }) => (
          <Card key={label} className="border border-border shadow-none">
            <CardContent className="flex items-center justify-between gap-3 p-4">
              <div className="min-w-0">
                <p className="text-xs text-muted-foreground">{label}</p>
                <p className="mt-1 truncate text-2xl font-semibold tracking-tight text-foreground">{value}</p>
                <p className="mt-1 truncate text-[11px] text-muted-foreground" title={detail}>{detail}</p>
              </div>
              <span className={`flex size-10 shrink-0 items-center justify-center rounded-xl border ${tone}`}>
                <Icon className="size-4" />
              </span>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card className="border border-border shadow-none">
        <CardHeader className="flex flex-row items-center justify-between pb-3">
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <KeyRound className="size-4 text-brand" />
            调用 Key 汇总
          </CardTitle>
          <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
            {keyRows.length} 个 Key
          </Badge>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto rounded-md border border-border">
            <Table className="min-w-[900px]">
              <TableHeader>
                <TableRow>
                  <TableHead>网关 Key</TableHead>
                  <TableHead className="text-right">今日用量</TableHead>
                  <TableHead className="text-right">总用量</TableHead>
                  <TableHead className="text-right">今日缓存命中</TableHead>
                  <TableHead className="text-right">总缓存命中</TableHead>
                  <TableHead className="text-right">最近使用</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !data ? (
                  <TableRow>
                    <TableCell colSpan={6} className="h-20 text-center text-xs text-muted-foreground">
                      <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                      加载中...
                    </TableCell>
                  </TableRow>
                ) : keyRows.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="h-20 text-center text-xs text-muted-foreground">
                      还没有调用 Key
                    </TableCell>
                  </TableRow>
                ) : (
                  keyRows.map((key) => (
                    <TableRow key={key.id}>
                      <TableCell>
                        <div className="min-w-0">
                          <p className="truncate text-xs font-medium text-foreground">{key.name}</p>
                          <p className="mt-0.5 text-[10px] text-muted-foreground">
                            {key.key_prefix} · {key.enabled ? "启用" : "停用"}
                            {key.is_public ? " · 公益 Key" : ""}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">{formatTokens(key.today_tokens)}</span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          输入 {formatTokens(key.today_prompt_tokens)}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">{formatTokens(key.total_tokens)}</span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          输入 {formatTokens(key.total_prompt_tokens)}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">
                          {cacheRateLabel(key.today_cache_hit_rate, key.today_prompt_tokens)}
                        </span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          {cacheTokenLabel(key.today_cached_tokens, key.today_prompt_tokens)}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">
                          {cacheRateLabel(key.total_cache_hit_rate, key.total_prompt_tokens)}
                        </span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          {cacheTokenLabel(key.total_cached_tokens, key.total_prompt_tokens)}
                        </span>
                      </TableCell>
                      <TableCell className="text-right text-xs text-muted-foreground">
                        {key.last_used_at ? relativeTime(key.last_used_at) : "未使用"}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Card className="border border-border shadow-none">
        <CardHeader className="flex flex-row items-center justify-between pb-3">
          <CardTitle className="flex items-center gap-2 text-base font-semibold">
            <ScrollText className="size-4 text-brand" />
            {detailView === "usage" ? "用量明细" : "调度事件"}
          </CardTitle>
          <div className="flex flex-wrap items-center justify-end gap-2">
            <div className="flex rounded-md border border-border bg-muted/20 p-0.5">
              <Button
                variant={detailView === "usage" ? "secondary" : "ghost"}
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={() => {
                  setDetailView("usage")
                  setPage(1)
                }}
              >
                用量明细
              </Button>
              <Button
                variant={detailView === "events" ? "secondary" : "ghost"}
                size="sm"
                className="h-7 px-2 text-xs"
                onClick={() => {
                  setDetailView("events")
                  setPage(1)
                }}
              >
                调度事件
              </Button>
            </div>
            <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
              共 {total} 条
            </Badge>
            <Button
              variant="outline"
              size="icon-sm"
              className="size-8"
              disabled={loading || clearing}
              title="刷新调用明细"
              aria-label="刷新调用明细"
              onClick={() => setRefreshTick((value) => value + 1)}
            >
              <RefreshCw className={`size-3.5 ${loading ? "animate-spin" : ""}`} />
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 px-2 text-xs"
              disabled={loading || clearing || total <= 0}
              onClick={() => void clearUsageLogs()}
            >
              {clearing ? <Loader2 className="size-3.5 animate-spin" /> : <Trash2 className="size-3.5" />}
              清空明细
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="overflow-x-auto rounded-md border border-border">
            <Table className="min-w-[1020px] table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-32">时间</TableHead>
                  <TableHead className="w-36">Key</TableHead>
                  <TableHead className="w-44">模型</TableHead>
                  <TableHead className="w-24 text-right">首字时间</TableHead>
                  <TableHead className="w-24 text-right">总耗时</TableHead>
                  <TableHead className="w-36 text-right">Token</TableHead>
                  <TableHead className="w-20 text-center">状态</TableHead>
                  <TableHead className="w-36">上游 / 倍率</TableHead>
                  <TableHead className="w-32">IP</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !data ? (
                  <TableRow>
                  <TableCell colSpan={9} className="h-24 text-center text-xs text-muted-foreground">
                      <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                      加载中...
                    </TableCell>
                  </TableRow>
                ) : rows.length === 0 ? (
                  <TableRow>
                  <TableCell colSpan={9} className="h-24 text-center text-xs text-muted-foreground">
                      {detailView === "usage" ? "还没有成功用量记录" : "当前没有调度异常事件"}
                    </TableCell>
                  </TableRow>
                ) : (
                  rows.map((log) => (
                    <TableRow key={log.id}>
                      <TableCell className="text-xs text-muted-foreground" title={log.created_at}>
                        <span className="block font-medium text-foreground">{dateTime(log.created_at)}</span>
                        <span className="text-[10px]">{relativeTime(log.created_at)}</span>
                      </TableCell>
                      <TableCell className="truncate text-xs" title={log.gateway_key_name || ""}>
                        <span className="block truncate font-medium text-foreground">
                          {log.gateway_key_name || (log.gateway_key_id ? `Key #${log.gateway_key_id}` : "未知 Key")}
                        </span>
                        {log.gateway_key_id ? (
                          <span className="block truncate text-[10px] text-muted-foreground">#{log.gateway_key_id}</span>
                        ) : null}
                      </TableCell>
                      <TableCell className="truncate text-xs text-foreground" title={log.model || ""}>
                        {log.model || "—"}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs text-foreground">
                        {durationLabel(log.first_token_ms)}
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs text-foreground">
                        {durationLabel(log.duration_ms)}
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">{formatTokens(log.total_tokens)}</span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          {formatTokens(log.prompt_tokens)} in / {formatTokens(log.completion_tokens)} out
                        </span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          缓存 {cacheRateLabel(log.prompt_tokens > 0 ? log.cached_tokens / log.prompt_tokens : null, log.prompt_tokens)}
                          {log.prompt_tokens > 0 ? ` · ${formatTokens(log.cached_tokens)} hit` : ""}
                        </span>
                      </TableCell>
                      <TableCell className="text-center">
                        <Badge variant="outline" className={`h-5 px-1.5 text-[10px] ${usageStatusTone(log.status)}`}>
                          {usageStatusLabel(log.status)}
                        </Badge>
                      </TableCell>
                      <TableCell className="min-w-0 text-xs">
                        <div className="min-w-0">
                          <div className="flex min-w-0 items-center gap-1">
                            <span className="truncate font-medium text-foreground" title={log.channel_name || log.group_name || ""}>
                              {log.channel_name || log.group_name || "未知上游"}
                            </span>
                            {log.upstream_group_charity ? (
                              <HeartHandshake className="size-3 shrink-0 text-amber-500" aria-label="公益渠道" />
                            ) : null}
                          </div>
                          <span className="mt-0.5 block font-mono text-[10px] text-muted-foreground">
                            倍率 {log.ratio != null ? formatRatio(log.ratio) : "—"}
                          </span>
                          {log.error_message ? (
                            <span className="mt-0.5 block truncate text-[10px] text-danger" title={log.error_message}>
                              {log.error_message}
                            </span>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell className="truncate font-mono text-[11px] text-muted-foreground" title={log.request_ip || ""}>
                        {log.request_ip || "—"}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>

          {total > 0 ? (
            <div className="flex flex-col gap-2 rounded-md border border-border bg-muted/10 px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
              <div className="text-xs text-muted-foreground">
                显示 {rangeStart}-{rangeEnd} / {total} 条
              </div>
              <div className="flex items-center gap-1.5">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 gap-1 px-2 text-xs"
                  disabled={loading || page <= 1}
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                >
                  <ChevronLeft className="size-3.5" />
                  上一页
                </Button>
                <span className="min-w-16 text-center text-xs text-muted-foreground">
                  {page} / {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 gap-1 px-2 text-xs"
                  disabled={loading || page >= totalPages}
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                >
                  下一页
                  <ChevronRight className="size-3.5" />
                </Button>
              </div>
            </div>
          ) : null}
        </CardContent>
      </Card>
    </section>
  )
}
