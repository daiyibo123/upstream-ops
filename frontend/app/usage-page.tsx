"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Activity, ChevronLeft, ChevronRight, CircleCheckBig, Copy, Eye, HeartHandshake, KeyRound, Loader2, RefreshCw, ScrollText, Trash2, Zap } from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { PageHeader } from "@/components/page-header"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { apiFetch } from "@/lib/api"
import { dateTime, formatPercent, formatRatio, formatTokens, relativeTime } from "@/lib/format"
import type { DispatchEventDetail, UsageLogDetailResponse, UsageLogsResponse } from "@/lib/api-types"

const PAGE_SIZE_OPTIONS = [50, 100, 200] as const
const DEFAULT_PAGE_SIZE = 50
type DetailView = "usage" | "events"
type UsageLogItem = UsageLogsResponse["items"][number] & {
  account_identifier?: string
  account_name?: string
  masked_account_identifier?: string
  pool_name?: string
  upstream_pool_name?: string
  retry_count?: number
  retries?: number
  attempt_count?: number
  error_code?: string | number
  code?: string | number
  http_status?: string | number
}

function cacheRateLabel(rate: number | null | undefined, promptTokens: number | null | undefined) {
  if (!promptTokens || promptTokens <= 0) return "—"
  return formatPercent(rate)
}

function cacheTokenLabel(cachedTokens: number | null | undefined, promptTokens: number | null | undefined) {
  if (!promptTokens || promptTokens <= 0) return "暂无输入缓存数据"
  return `${formatTokens(cachedTokens)} cached / ${formatTokens(promptTokens)} input`
}

function redactSensitiveText(value?: string | null) {
  if (!value) return ""
  return value
    .replace(/([a-z][a-z0-9+.-]*:\/\/)([^/\s:@]+):([^@\s/]+)@/gi, "$1***:***@")
    .replace(/(\b(?:authorization|proxy-authorization)\s*:\s*)[^\r\n]*/gi, "$1[已隐藏]")
    .replace(/(\b(?:cookie|set-cookie)\s*:\s*)[^\r\n]*/gi, "$1[已隐藏]")
    .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi, "Bearer [已隐藏]")
    .replace(/\bBasic\s+[A-Za-z0-9+/]+=*/gi, "Basic [已隐藏]")
    .replace(/([?&](?:access[_-]?token|refresh[_-]?token|token|api[_-]?key|key|password|secret)=)[^&#\s]*/gi, "$1[已隐藏]")
    .replace(/\b(sk|sess|eyJ)[-_A-Za-z0-9.]{12,}\b/g, "$1***")
    .replace(
      /(["']?(?:access[_-]?token|refresh[_-]?token|authorization|api[_-]?key|cookie|password|secret)["']?\s*[:=]\s*)(["'])[^\r\n]*?\2/gi,
      "$1$2[已隐藏]$2",
    )
    .replace(
      /(["']?(?:access[_-]?token|refresh[_-]?token|authorization|api[_-]?key|cookie|password|secret)["']?\s*[:=]\s*)["']?[^\s,;}"']+["']?/gi,
      "$1[已隐藏]",
    )
}

function maskAccountIdentifier(log: UsageLogItem, detailAccount?: string) {
  const alreadyMasked = detailAccount?.trim() || log.masked_account_identifier?.trim()
  if (alreadyMasked) return redactSensitiveText(alreadyMasked)

  const raw = log.oauth_account?.trim() || log.account_identifier?.trim() || log.account_name?.trim()
  if (raw) {
    const at = raw.indexOf("@")
    if (at > 0) {
      const domain = raw.slice(at)
      return `${raw.slice(0, Math.min(2, at))}***${domain}`
    }
    if (raw.length <= 4) return "****"
    return `${raw.slice(0, 2)}***${raw.slice(-2)}`
  }

  if (log.upstream_group_key_id != null) {
    return `账号 ••••${String(log.upstream_group_key_id).slice(-4)}`
  }
  return "后端未记录"
}

function retryCountLabel(log: UsageLogItem, detailAttempt?: number) {
  const value = detailAttempt ?? log.dispatch_attempt ?? log.retry_count ?? log.retries
  if (value != null && Number.isFinite(value)) return String(Math.max(0, value))
  if (log.attempt_count != null && Number.isFinite(log.attempt_count)) {
    return `${Math.max(1, log.attempt_count)} 次尝试`
  }
  return "后端未记录"
}

function errorCodeLabel(log: UsageLogItem, detail?: DispatchEventDetail | null) {
  const value = detail?.error_code ?? detail?.error_status ?? log.error_code ?? log.code ?? log.error_status ?? log.http_status
  return value == null || value === "" ? "后端未记录" : String(value)
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
  const [pageSize, setPageSize] = useState<number>(DEFAULT_PAGE_SIZE)
  const [refreshTick, setRefreshTick] = useState(0)
  const [detailView, setDetailView] = useState<DetailView>("usage")
  const [selectedLog, setSelectedLog] = useState<UsageLogItem | null>(null)
  const [selectedDetail, setSelectedDetail] = useState<DispatchEventDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState("")
  const detailRequestID = useRef(0)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [loadedScope, setLoadedScope] = useState("")

  const scopeKey = `${detailView}:${page}:${pageSize}`
  const visibleData = loadedScope === scopeKey ? data : null
  const items = visibleData?.items ?? []
  const keys = data?.keys ?? []
  const total = visibleData?.total ?? 0
  const stats = data?.stats
  const totalPages = Math.max(1, Math.ceil(total / pageSize))
  const rangeStart = total === 0 ? 0 : (page - 1) * pageSize + 1
  const rangeEnd = Math.min(page * pageSize, total)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setLoadError(null)
    const offset = (page - 1) * pageSize
    apiFetch<UsageLogsResponse>(`/gateway/usage-logs?view=${detailView}&limit=${pageSize}&offset=${offset}`)
      .then((res) => {
        if (!cancelled) {
          setData(res)
          setLoadedScope(scopeKey)
        }
      })
      .catch((err: Error) => {
        if (!cancelled) {
          const message = redactSensitiveText(err.message || "加载使用记录失败")
          setLoadError(message)
          toast.error(message)
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [detailView, page, pageSize, refreshTick, scopeKey])

  const rows = useMemo(() => items as UsageLogItem[], [items])
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
          ...prev.stats,
          total_requests: 0,
          success_requests: 0,
          total_tokens: 0,
        },
      } : prev))
      setPage(1)
      setLoadedScope(`${detailView}:1:${pageSize}`)
      toast.success(`已清空 ${res.deleted ?? total} 条明细，Key 汇总未受影响`)
    } catch (err) {
      const error = err as Error
      toast.error(error.message || "清空使用记录失败")
    } finally {
      setClearing(false)
    }
  }

  async function copySelectedError() {
    const error = redactSensitiveText(selectedDetail?.error || selectedLog?.error_message)
    if (!error) return
    try {
      await navigator.clipboard.writeText(error)
      toast.success("错误详情已复制")
    } catch {
      toast.error("复制失败，请手动选择文本复制")
    }
  }

  async function openUsageDetail(log: UsageLogItem) {
    const requestID = ++detailRequestID.current
    setSelectedLog(log)
    setSelectedDetail(null)
    setDetailError("")
    setDetailLoading(true)
    try {
      const response = await apiFetch<UsageLogDetailResponse>(`/gateway/usage-logs/${log.id}`)
      if (detailRequestID.current !== requestID) return
      setSelectedLog(response.event as UsageLogItem)
      setSelectedDetail(response.detail ?? null)
    } catch (caught) {
      if (detailRequestID.current !== requestID) return
      setDetailError(redactSensitiveText(caught instanceof Error ? caught.message : "调度事件详情加载失败"))
    } finally {
      if (detailRequestID.current === requestID) setDetailLoading(false)
    }
  }

  function closeUsageDetail() {
    detailRequestID.current++
    setSelectedLog(null)
    setSelectedDetail(null)
    setDetailError("")
    setDetailLoading(false)
  }

  return (
    <section className="space-y-5">
      <PageHeader
        icon={<ScrollText className="size-[18px]" />}
        title="使用记录"
        description="查看调用明细、Key 用量汇总、渠道切换和脱敏后的错误详情。"
      />

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
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
        <CardHeader className="flex flex-col gap-3 pb-3 sm:flex-row sm:items-center sm:justify-between">
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
        <CardHeader className="flex flex-col gap-3 pb-3 sm:flex-row sm:items-center sm:justify-between">
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
          {loadError ? (
            <div className="flex flex-col gap-2 rounded-md border border-danger/20 bg-danger/5 px-3 py-2 text-xs text-danger sm:flex-row sm:items-center sm:justify-between">
              <span className="break-words">加载失败：{loadError}</span>
              <Button
                variant="outline"
                size="sm"
                className="h-7 shrink-0 px-2 text-xs"
                disabled={loading}
                onClick={() => setRefreshTick((value) => value + 1)}
              >
                重试
              </Button>
            </div>
          ) : null}
          <div className="overflow-x-auto rounded-md border border-border">
            <Table className="min-w-[980px] table-fixed">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-32">时间</TableHead>
                  <TableHead className="w-36">Key</TableHead>
                  <TableHead className="w-44">模型</TableHead>
                  <TableHead className="w-36 text-right">Token</TableHead>
                  <TableHead className="w-20 text-center">状态</TableHead>
                  <TableHead className="w-36">上游 / 倍率</TableHead>
                  <TableHead className="w-32">IP</TableHead>
                  <TableHead className="w-20 text-right">详情</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !visibleData ? (
                  <TableRow>
                  <TableCell colSpan={8} className="h-24 text-center text-xs text-muted-foreground">
                      <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                      加载中...
                    </TableCell>
                  </TableRow>
                ) : loadError && !visibleData ? (
                  <TableRow>
                    <TableCell colSpan={8} className="h-24 text-center text-xs text-danger">
                      数据加载失败，请重试
                    </TableCell>
                  </TableRow>
                ) : rows.length === 0 ? (
                  <TableRow>
                  <TableCell colSpan={8} className="h-24 text-center text-xs text-muted-foreground">
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
                            <span
                              className="mt-0.5 block truncate text-[10px] text-danger"
                              title={redactSensitiveText(log.error_message)}
                            >
                              {redactSensitiveText(log.error_message)}
                            </span>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell className="truncate font-mono text-[11px] text-muted-foreground" title={log.request_ip || ""}>
                        {log.request_ip || "—"}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="outline"
                          size="sm"
                          className="h-7 gap-1 px-2 text-xs"
                          onClick={() => void openUsageDetail(log)}
                        >
                          <Eye className="size-3.5" />
                          详情
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>

          {total > 0 ? (
            <div className="flex flex-col gap-2 rounded-md border border-border bg-muted/10 px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <span>显示 {rangeStart}-{rangeEnd} / {total} 条</span>
                <Select
                  value={String(pageSize)}
                  onValueChange={(value) => {
                    setPageSize(Number(value))
                    setPage(1)
                  }}
                >
                  <SelectTrigger className="h-8 w-24 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {PAGE_SIZE_OPTIONS.map((size) => (
                      <SelectItem key={size} value={String(size)}>
                        {size} / 页
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
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

      <Dialog open={selectedLog != null} onOpenChange={(open) => !open && closeUsageDetail()}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>调度事件详情</DialogTitle>
            <DialogDescription>
              {selectedLog ? `事件 #${selectedLog.id} 的后端记录，敏感信息已隐藏。` : "查看调度事件详情。"}
            </DialogDescription>
          </DialogHeader>

          {selectedLog ? (
            <div className="space-y-4">
              {detailLoading ? (
                <div className="flex items-center gap-2 rounded-md border border-border bg-muted/10 px-3 py-2 text-xs text-muted-foreground">
                  <Loader2 className="size-3.5 animate-spin" />
                  正在读取后端调度详情...
                </div>
              ) : null}
              {detailError ? (
                <div className="rounded-md border border-danger/20 bg-danger/5 px-3 py-2 text-xs text-danger">
                  详情加载失败：{detailError}。以下仍显示列表中的兼容字段。
                </div>
              ) : null}
              <div className="grid gap-x-6 gap-y-3 rounded-md border border-border bg-muted/10 p-3 text-sm sm:grid-cols-2">
                {[
                  ["请求时间", selectedDetail?.request_time ? dateTime(selectedDetail.request_time) : selectedLog.created_at ? dateTime(selectedLog.created_at) : "后端未记录"],
                  ["渠道", selectedDetail?.channel || selectedLog.channel_name || (selectedLog.channel_id ? `渠道 #${selectedLog.channel_id}` : "后端未记录")],
                  ["号池 / 分组", selectedDetail?.pool || selectedLog.oauth_pool || selectedLog.upstream_pool_name || selectedLog.pool_name || selectedLog.group_name || "后端未记录"],
                  ["账号标识", maskAccountIdentifier(selectedLog, selectedDetail?.account)],
                  ["模型", selectedDetail?.model || selectedLog.model || "后端未记录"],
                  ["尝试次数", retryCountLabel(selectedLog, selectedDetail?.attempt)],
                  ["错误码", errorCodeLabel(selectedLog, selectedDetail)],
                ].map(([label, value]) => (
                  <div key={label} className="min-w-0">
                    <p className="text-xs text-muted-foreground">{label}</p>
                    <p className="mt-1 break-words font-medium text-foreground">{value}</p>
                  </div>
                ))}
                <div className="min-w-0">
                  <p className="text-xs text-muted-foreground">状态</p>
                  <Badge variant="outline" className={`mt-1 h-5 px-1.5 text-[10px] ${usageStatusTone(selectedDetail?.status || selectedLog.status)}`}>
                    {usageStatusLabel(selectedDetail?.status || selectedLog.status)}
                  </Badge>
                </div>
              </div>

              <div className="space-y-2">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-foreground">错误内容</p>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-8 gap-1.5 px-2 text-xs"
                    disabled={!selectedDetail?.error && !selectedLog.error_message}
                    onClick={() => void copySelectedError()}
                  >
                    <Copy className="size-3.5" />
                    复制
                  </Button>
                </div>
                {selectedDetail?.error || selectedLog.error_message ? (
                  <pre className="max-h-72 overflow-auto whitespace-pre-wrap break-all rounded-md border border-border bg-muted/20 p-3 font-mono text-xs leading-5 text-foreground select-text">
                    {redactSensitiveText(selectedDetail?.error || selectedLog.error_message)}
                  </pre>
                ) : (
                  <div className="rounded-md border border-dashed border-border px-3 py-8 text-center text-sm text-muted-foreground">
                    本次事件没有可用的错误详情，可能是成功请求或历史记录字段不完整。
                  </div>
                )}
              </div>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </section>
  )
}
