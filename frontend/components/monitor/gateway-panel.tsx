"use client"

import { useEffect, useMemo, useState } from "react"
import { CheckCircle2, Copy, Eye, EyeOff, KeyRound, Loader2, Plus, RefreshCw, Trash2, XCircle } from "lucide-react"
import { toast } from "sonner"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import { channelTypeLabel, formatRatio, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import type {
  GatewayBootstrapResult,
  GatewayHealthResult,
  GatewayKey,
  GatewayKeyReveal,
  UpstreamGroupKey,
} from "@/lib/api-types"

function statusTone(status: string) {
  switch (status) {
    case "alive":
      return "bg-success/10 text-success border-success/20"
    case "dead":
      return "bg-danger/10 text-danger border-danger/20"
    case "disabled":
      return "bg-muted text-muted-foreground border-border"
    default:
      return "bg-muted text-muted-foreground border-border"
  }
}

function statusText(status: string) {
  switch (status) {
    case "alive":
      return "存活"
    case "dead":
      return "死亡"
    case "disabled":
      return "停用"
    default:
      return "未知"
  }
}

async function copyText(text: string) {
  await navigator.clipboard.writeText(text)
  toast.success("已复制")
}

function formatTokens(value: number) {
  if (!Number.isFinite(value) || value <= 0) return "0"
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return String(value)
}

function formatExpiry(value?: string | null) {
  if (!value) return "永不过期"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "未知"
  if (date.getTime() <= Date.now()) return "已过期"
  return date.toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit" })
}

export function GatewayPanel() {
  const [keys, setKeys] = useState<GatewayKey[]>([])
  const [groups, setGroups] = useState<UpstreamGroupKey[]>([])
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [newKeyName, setNewKeyName] = useState("default")
  const [dailyLimit, setDailyLimit] = useState("")
  const [totalLimit, setTotalLimit] = useState("")
  const [expiresInDays, setExpiresInDays] = useState("0")
  const [revealed, setRevealed] = useState<Record<number, string>>({})
  const [visible, setVisible] = useState<Record<number, boolean>>({})
  const [concurrencyDrafts, setConcurrencyDrafts] = useState<Record<number, string>>({})

  const aliveCount = useMemo(() => groups.filter((g) => g.status === "alive").length, [groups])
  const deadCount = useMemo(() => groups.filter((g) => g.status === "dead").length, [groups])

  async function load() {
    setLoading(true)
    try {
      const [keyList, groupList] = await Promise.all([
        apiFetch<GatewayKey[]>("/gateway/keys"),
        apiFetch<UpstreamGroupKey[]>("/gateway/group-keys"),
      ])
      setKeys(Array.isArray(keyList) ? keyList : [])
      const nextGroups = Array.isArray(groupList) ? groupList : []
      setGroups(nextGroups)
      setConcurrencyDrafts(
        Object.fromEntries(nextGroups.map((group) => [group.id, String(group.concurrency_limit || 0)])),
      )
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "加载调度网关失败")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void load()
  }, [])

  async function createGatewayKey() {
    setBusy("create-key")
    try {
      const created = await apiFetch<GatewayKey>("/gateway/keys", {
        method: "POST",
        body: JSON.stringify({
          name: newKeyName.trim() || "default",
          daily_limit: Number(dailyLimit) || 0,
          total_limit: Number(totalLimit) || 0,
          expires_in_days: Number(expiresInDays) || 0,
        }),
      })
      if (created.key) {
        setRevealed((prev) => ({ ...prev, [created.id]: created.key ?? "" }))
        setVisible((prev) => ({ ...prev, [created.id]: false }))
        await copyText(created.key)
      }
      toast.success("网关 Key 已创建")
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "创建网关 Key 失败")
    } finally {
      setBusy(null)
    }
  }

  async function ensureFullKey(key: GatewayKey) {
    if (revealed[key.id]) {
      return revealed[key.id]
    }
    setBusy(`reveal-${key.id}`)
    try {
      const res = await apiFetch<GatewayKeyReveal>(`/gateway/keys/${key.id}/reveal`, { method: "POST" })
      if (!res.key) throw new Error("没有返回完整 Key")
      setRevealed((prev) => ({ ...prev, [key.id]: res.key }))
      return res.key
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "获取 Key 失败")
      return null
    } finally {
      setBusy(null)
    }
  }

  async function toggleKeyVisible(key: GatewayKey) {
    if (visible[key.id]) {
      setVisible((prev) => ({ ...prev, [key.id]: false }))
      return
    }
    const fullKey = await ensureFullKey(key)
    if (!fullKey) return
    setVisible((prev) => ({ ...prev, [key.id]: true }))
  }

  async function copyKey(key: GatewayKey) {
    const fullKey = await ensureFullKey(key)
    if (fullKey) await copyText(fullKey)
  }

  async function deleteKey(key: GatewayKey) {
    setBusy(`delete-${key.id}`)
    try {
      await apiFetch(`/gateway/keys/${key.id}`, { method: "DELETE" })
      toast.success("网关 Key 已删除")
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "删除网关 Key 失败")
    } finally {
      setBusy(null)
    }
  }

  async function bootstrapGroups() {
    setBusy("bootstrap")
    try {
      const res = await apiFetch<GatewayBootstrapResult>("/gateway/group-keys/bootstrap", { method: "POST" })
      toast.success(`分组 Key 完成：新建 ${res.created}，更新 ${res.updated}，失败 ${res.failed}`)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "一键创建分组 Key 失败")
    } finally {
      setBusy(null)
    }
  }

  async function testGroups() {
    setBusy("test")
    try {
      const res = await apiFetch<GatewayHealthResult>("/gateway/group-keys/test", { method: "POST" })
      toast.success(`测活完成：存活 ${res.alive}，死亡 ${res.dead}`)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "一键测活失败")
    } finally {
      setBusy(null)
    }
  }

  async function saveConcurrencyLimit(group: UpstreamGroupKey) {
    const raw = concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)
    const limit = Math.max(0, Math.floor(Number(raw) || 0))
    setBusy(`group-limit-${group.id}`)
    try {
      const updated = await apiFetch<UpstreamGroupKey>(`/gateway/group-keys/${group.id}`, {
        method: "PATCH",
        body: JSON.stringify({ concurrency_limit: limit }),
      })
      setGroups((prev) => prev.map((item) => (item.id === updated.id ? updated : item)))
      setConcurrencyDrafts((prev) => ({ ...prev, [group.id]: String(updated.concurrency_limit || 0) }))
      toast.success("并发上限已保存")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "保存并发上限失败")
    } finally {
      setBusy(null)
    }
  }

  return (
    <Card className="border border-border shadow-none">
      <CardHeader className="flex flex-col gap-3 pb-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <CardTitle className="text-base font-semibold">调度网关</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">
            /v1 兼容入口 · 低倍率优先 · 失败自动切换
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline" className="border-success/20 bg-success/10 text-success">
            存活 {aliveCount}
          </Badge>
          <Badge variant="outline" className="border-danger/20 bg-danger/10 text-danger">
            死亡 {deadCount}
          </Badge>
          <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={bootstrapGroups}>
            {busy === "bootstrap" ? <Loader2 className="size-3.5 animate-spin" /> : <Plus className="size-3.5" />}
            一键创建分组 Key
          </Button>
          <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={testGroups}>
            {busy === "test" ? <Loader2 className="size-3.5 animate-spin" /> : <RefreshCw className="size-3.5" />}
            一键分组测活
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 rounded-md border border-border bg-muted/10 p-3 lg:grid-cols-[0.95fr_2.05fr]">
          <div className="space-y-2">
            <p className="text-xs font-medium text-foreground">新建调用 Key</p>
            <div className="grid gap-2 sm:grid-cols-[1fr_0.75fr_0.75fr_0.75fr_auto]">
              <Input value={newKeyName} onChange={(e) => setNewKeyName(e.target.value)} placeholder="名称" className="h-8 text-xs" />
              <Input
                value={dailyLimit}
                onChange={(e) => setDailyLimit(e.target.value)}
                inputMode="numeric"
                placeholder="每日 token"
                className="h-8 text-xs"
              />
              <Input
                value={totalLimit}
                onChange={(e) => setTotalLimit(e.target.value)}
                inputMode="numeric"
                placeholder="总 token"
                className="h-8 text-xs"
              />
              <Select value={expiresInDays} onValueChange={setExpiresInDays}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="过期时间" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">1 天</SelectItem>
                  <SelectItem value="2">2 天</SelectItem>
                  <SelectItem value="7">7 天</SelectItem>
                  <SelectItem value="30">30 天</SelectItem>
                  <SelectItem value="0">永不过期</SelectItem>
                </SelectContent>
              </Select>
              <Button size="sm" className="h-8 shrink-0 gap-1.5 text-xs" disabled={!!busy} onClick={createGatewayKey}>
                {busy === "create-key" ? <Loader2 className="size-3 animate-spin" /> : <KeyRound className="size-3" />}
                创建
              </Button>
            </div>
          </div>
          <div className="min-w-0">
            <div className="mb-2 flex items-center justify-between gap-2">
              <p className="text-xs font-medium text-foreground">已有调用 Key</p>
              <span className="text-[11px] text-muted-foreground">Bearer Key · {keys.length} 个</span>
            </div>
            {keys.length === 0 ? (
              <div className="rounded-md border border-dashed border-border bg-background px-3 py-6 text-center text-xs text-muted-foreground">
                还没有网关 Key
              </div>
            ) : (
              <div className="overflow-x-auto rounded-md border border-border bg-background">
                <Table className="min-w-[760px]">
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-40">名称</TableHead>
                      <TableHead>密钥</TableHead>
                      <TableHead className="hidden text-right md:table-cell">今日</TableHead>
                      <TableHead className="hidden text-right xl:table-cell">总量</TableHead>
                      <TableHead className="hidden lg:table-cell">到期</TableHead>
                      <TableHead className="hidden md:table-cell">最近使用</TableHead>
                      <TableHead className="w-28 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {keys.map((key) => (
                      <TableRow key={key.id}>
                        <TableCell>
                          <div className="min-w-0">
                            <p className="truncate text-xs font-medium text-foreground">{key.name}</p>
                            <p className={cn("text-[11px]", key.enabled ? "text-success" : "text-muted-foreground")}>
                              {key.enabled ? "启用中" : "已停用"}
                            </p>
                          </div>
                        </TableCell>
                        <TableCell>
                          <code className="block max-w-96 truncate rounded-md bg-muted px-2 py-1 font-mono text-[11px] text-foreground">
                            {visible[key.id] && revealed[key.id]
                              ? revealed[key.id]
                              : `${key.key_prefix}***${revealed[key.id] ? revealed[key.id].slice(-6) : ""}`}
                          </code>
                        </TableCell>
                        <TableCell className="hidden text-right text-xs md:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.today_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.daily_limit > 0 ? formatTokens(key.daily_limit) : "不限"}</span>
                        </TableCell>
                        <TableCell className="hidden text-right text-xs xl:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.total_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.total_limit > 0 ? formatTokens(key.total_limit) : "不限"}</span>
                        </TableCell>
                        <TableCell className="hidden text-xs text-muted-foreground lg:table-cell">
                          {formatExpiry(key.expires_at)}
                        </TableCell>
                        <TableCell className="hidden text-xs text-muted-foreground md:table-cell">
                          {key.last_used_at ? relativeTime(key.last_used_at) : "未使用"}
                        </TableCell>
                        <TableCell>
                          <div className="flex justify-end gap-1">
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              className="size-7"
                              disabled={!!busy}
                              title={visible[key.id] ? "隐藏" : "查看"}
                              onClick={() => void toggleKeyVisible(key)}
                            >
                              {busy === `reveal-${key.id}` ? (
                                <Loader2 className="size-3.5 animate-spin" />
                              ) : visible[key.id] ? (
                                <EyeOff className="size-3.5" />
                              ) : (
                                <Eye className="size-3.5" />
                              )}
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              className="size-7"
                              disabled={!!busy}
                              title="复制"
                              onClick={() => void copyKey(key)}
                            >
                              <Copy className="size-3.5" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              className="size-7 text-destructive hover:text-destructive"
                              disabled={!!busy}
                              title="删除"
                              onClick={() => void deleteKey(key)}
                            >
                              <Trash2 className="size-3.5" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </div>
        </div>

        <div className="overflow-x-auto rounded-md border border-border">
          <Table className="min-w-[980px]">
            <TableHeader>
              <TableRow>
                <TableHead>状态</TableHead>
                <TableHead>渠道</TableHead>
                <TableHead>分组</TableHead>
                <TableHead className="text-right">倍率</TableHead>
                <TableHead className="w-32">并发上限</TableHead>
                <TableHead>测活</TableHead>
                <TableHead>错误</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-24 text-center text-xs text-muted-foreground">
                    <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                    加载中...
                  </TableCell>
                </TableRow>
              ) : groups.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="h-24 text-center text-xs text-muted-foreground">
                    还没有分组 Key，先点“一键创建分组 Key”
                  </TableCell>
                </TableRow>
              ) : (
                groups.map((group) => {
                  const Icon = group.status === "alive" ? CheckCircle2 : group.status === "dead" ? XCircle : RefreshCw
                  return (
                    <TableRow key={group.id}>
                      <TableCell>
                        <Badge variant="outline" className={cn("gap-1.5", statusTone(group.status))}>
                          <Icon className="size-3" />
                          {statusText(group.status)}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm font-medium">{group.channel_name || `#${group.channel_id}`}</div>
                        <div className="text-[11px] text-muted-foreground">{channelTypeLabel(group.channel_type)}</div>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm font-medium">{group.group_name}</div>
                        <div className="max-w-96 truncate text-[11px] text-muted-foreground">{group.group_description || group.group_ref}</div>
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="font-mono text-xs">{formatRatio(group.ratio)}</div>
                        <div className="text-[11px] text-muted-foreground">{formatTokens(group.total_tokens)} tok</div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Input
                            value={concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)}
                            inputMode="numeric"
                            className="h-7 w-16 px-2 text-xs"
                            disabled={!!busy}
                            title="0 表示不限"
                            onChange={(event) =>
                              setConcurrencyDrafts((prev) => ({
                                ...prev,
                                [group.id]: event.target.value.replace(/[^\d]/g, ""),
                              }))
                            }
                            onKeyDown={(event) => {
                              if (event.key === "Enter") {
                                event.currentTarget.blur()
                              }
                            }}
                            onBlur={() => {
                              const draft = concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)
                              if (Number(draft) !== (group.concurrency_limit || 0)) {
                                void saveConcurrencyLimit(group)
                              }
                            }}
                          />
                          {busy === `group-limit-${group.id}` && <Loader2 className="size-3 animate-spin text-muted-foreground" />}
                        </div>
                        <div className="mt-1 text-[11px] text-muted-foreground">{(group.concurrency_limit || 0) > 0 ? `${group.concurrency_limit} 路` : "不限"}</div>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {group.last_checked_at ? relativeTime(group.last_checked_at) : "未测"}
                      </TableCell>
                      <TableCell className="max-w-72 truncate text-xs text-danger" title={group.last_error || ""}>
                        {group.last_error || ""}
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}
