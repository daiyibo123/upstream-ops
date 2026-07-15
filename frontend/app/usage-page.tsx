"use client"

import { useEffect, useMemo, useState } from "react"
import { ChevronLeft, ChevronRight, HeartHandshake, KeyRound, Loader2, RefreshCw, ScrollText, Trash2 } from "lucide-react"
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

  const items = data?.items ?? []
  const keys = data?.keys ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const rangeStart = total === 0 ? 0 : (page - 1) * PAGE_SIZE + 1
  const rangeEnd = Math.min(page * PAGE_SIZE, total)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    const offset = (page - 1) * PAGE_SIZE
    apiFetch<UsageLogsResponse>(`/gateway/usage-logs?limit=${PAGE_SIZE}&offset=${offset}`)
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
  }, [page, refreshTick])

  const rows = useMemo(() => items, [items])
  const keyRows = useMemo(
    () =>
      keys
        .slice()
        .sort((a, b) => b.total_tokens - a.total_tokens || b.today_tokens - a.today_tokens || a.name.localeCompare(b.name)),
    [keys],
  )
  const publicKeyIDs = useMemo(() => new Set(keys.filter((key) => key.is_public).map((key) => key.id)), [keys])

  async function clearUsageLogs() {
    if (total <= 0 || clearing) return
    if (!window.confirm("确定清空下面的调用明细吗？每个 Key 的今日/累计统计会保留。")) return
    setClearing(true)
    try {
      const res = await apiFetch<{ deleted: number }>("/gateway/usage-logs", { method: "DELETE" })
      setData((prev) => (prev ? { ...prev, items: [], total: 0 } : prev))
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
            调用明细
          </CardTitle>
          <div className="flex items-center gap-2">
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
                      还没有使用记录
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
                            {(log.gateway_key_is_public || (log.gateway_key_id != null && publicKeyIDs.has(log.gateway_key_id))) ? (
                              <HeartHandshake className="size-3 shrink-0 text-amber-500" aria-label="公益 Key" />
                            ) : null}
                          </div>
                          <span className="mt-0.5 block font-mono text-[10px] text-muted-foreground">
                            倍率 {log.ratio != null ? formatRatio(log.ratio) : "—"}
                          </span>
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
