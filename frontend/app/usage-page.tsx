"use client"

import { useEffect, useMemo, useState } from "react"
import { ChevronLeft, ChevronRight, Loader2, ScrollText } from "lucide-react"
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
import { formatRatio, relativeTime } from "@/lib/format"
import type { UsageLogsResponse } from "@/lib/api-types"

const PAGE_SIZE = 50

function formatTokens(value?: number | null) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "0"
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

export default function UsagePage() {
  const [data, setData] = useState<UsageLogsResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [page, setPage] = useState(1)

  const items = data?.items ?? []
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
  }, [page])

  const rows = useMemo(() => items, [items])

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
            <ScrollText className="size-4 text-brand" />
            调用明细
          </CardTitle>
          <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
            共 {total} 条
          </Badge>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="overflow-x-auto rounded-md border border-border">
            <Table className="min-w-[980px]">
              <TableHeader>
                <TableRow>
                  <TableHead className="w-36">时间</TableHead>
                  <TableHead>网关 Key</TableHead>
                  <TableHead>渠道</TableHead>
                  <TableHead>分组</TableHead>
                  <TableHead>模型</TableHead>
                  <TableHead>格式</TableHead>
                  <TableHead className="text-right">Token</TableHead>
                  <TableHead className="text-right">倍率</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && !data ? (
                  <TableRow>
                    <TableCell colSpan={8} className="h-24 text-center text-xs text-muted-foreground">
                      <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                      加载中...
                    </TableCell>
                  </TableRow>
                ) : rows.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={8} className="h-24 text-center text-xs text-muted-foreground">
                      还没有使用记录
                    </TableCell>
                  </TableRow>
                ) : (
                  rows.map((log) => (
                    <TableRow key={log.id}>
                      <TableCell className="text-xs text-muted-foreground" title={log.created_at}>
                        {relativeTime(log.created_at)}
                      </TableCell>
                      <TableCell className="text-xs font-medium text-foreground">
                        {log.gateway_key_name || (log.gateway_key_id ? `#${log.gateway_key_id}` : "—")}
                      </TableCell>
                      <TableCell className="text-xs text-foreground">
                        {log.channel_name || (log.channel_id ? `#${log.channel_id}` : "—")}
                      </TableCell>
                      <TableCell className="text-xs text-foreground">{log.group_name || "—"}</TableCell>
                      <TableCell className="max-w-56 truncate text-xs text-foreground" title={log.model || ""}>
                        {log.model || "—"}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline" className="h-5 px-1.5 text-[10px]">
                          {(log.client_format || "openai").toUpperCase()}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right text-xs">
                        <span className="font-medium text-foreground">{formatTokens(log.total_tokens)}</span>
                        <span className="mt-0.5 block text-[10px] text-muted-foreground">
                          {formatTokens(log.prompt_tokens)} in / {formatTokens(log.completion_tokens)} out
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono text-xs text-foreground">
                        {log.ratio != null ? formatRatio(log.ratio) : "—"}
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
