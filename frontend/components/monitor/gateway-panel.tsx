"use client"

import { useEffect, useMemo, useState } from "react"
import { CheckCircle2, ChevronLeft, ChevronRight, Copy, Eye, EyeOff, HeartHandshake, KeyRound, Loader2, Pencil, Plus, RefreshCw, Search, Trash2, XCircle } from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import { channelTypeLabel, formatRatio, formatTokens, relativeTime } from "@/lib/format"
import { testGatewayHealthStream, type ProgressEvent } from "@/lib/sync-stream"
import { cn } from "@/lib/utils"
import type {
  Channel,
  ChannelPage,
  GatewayBootstrapResult,
  GatewayHealthResult,
  GatewayKey,
  GatewayKeyReveal,
  UpstreamGroupKey,
  UpstreamGroupKeyPage,
} from "@/lib/api-types"

const TOKEN_M = 1_000_000

type ClientFormat = "openai" | "claude" | "grok" | "any"
type GroupScope = "all" | "selected"
type UpstreamRequestMode = "responses" | "chat"

interface KeyDraft {
  name: string
  enabled: boolean
  clientFormat: ClientFormat
  scope: GroupScope
  selectedGroupIds: number[]
  dailyLimitM: string
  totalLimitM: string
  costPerMillion: string
  expiresInDays: string
}

function createDefaultDraft(): KeyDraft {
  return {
    name: "default",
    enabled: true,
    clientFormat: "openai",
    scope: "all",
    selectedGroupIds: [],
    dailyLimitM: "",
    totalLimitM: "",
    costPerMillion: "",
    expiresInDays: "0",
  }
}

interface ManualDraft {
  sourceMode: "new" | "existing"
  channelId: string
  channelName: string
  siteUrl: string
  groupName: string
  groupDescription: string
  key: string
  ratio: string
  clientFormat: "openai" | "claude" | "grok"
  requestMode: UpstreamRequestMode
  charity: boolean
  priority: string
}

interface HealthProgress {
  running: boolean
  completed: number
  total: number
  batch: number
  batches: number
  batchSize: number
  message: string
}

function createDefaultManualDraft(): ManualDraft {
  return {
    sourceMode: "new",
    channelId: "",
    channelName: "",
    siteUrl: "",
    groupName: "",
    groupDescription: "",
    key: "",
    ratio: "1",
    clientFormat: "openai",
    requestMode: "responses",
    charity: false,
    priority: "0",
  }
}

function statusTone(status: string) {
  switch (status) {
    case "alive":
      return "bg-success/10 text-success border-success/20"
    case "dead":
      return "bg-danger/10 text-danger border-danger/20"
    case "checking":
      return "bg-brand/10 text-brand border-brand/20"
    case "queued":
      return "bg-warning/10 text-warning border-warning/20"
    case "disabled":
      return "bg-muted text-muted-foreground border-border"
    default:
      return "bg-muted text-muted-foreground border-border"
  }
}

function effectiveStatus(group: UpstreamGroupKey) {
  return group.enabled === false ? "disabled" : group.status
}

function statusText(status: string) {
  switch (status) {
    case "alive":
      return "存活"
    case "dead":
      return "死亡"
    case "checking":
      return "测活中"
    case "queued":
      return "排队中"
    case "disabled":
      return "停用"
    default:
      return "未知"
  }
}

function normalizeClientFormat(value?: string | null): ClientFormat {
  switch ((value ?? "").toLowerCase()) {
    case "claude":
      return "claude"
    case "grok":
    case "xai":
      return "grok"
    case "any":
      return "any"
    default:
      return "openai"
  }
}

function clientFormatLabel(value?: string | null) {
  switch (normalizeClientFormat(value)) {
    case "claude":
      return "Claude"
    case "grok":
      return "Grok"
    case "any":
      return "不限"
    default:
      return "OpenAI"
  }
}

function normalizeRequestMode(value?: string | null): UpstreamRequestMode {
  switch ((value ?? "").toLowerCase()) {
    case "chat":
    case "chat_completions":
    case "chat-completions":
      return "chat"
    default:
      return "responses"
  }
}

function requestModeLabel(value?: string | null) {
  return normalizeRequestMode(value) === "chat" ? "Chat" : "Responses"
}

function groupClientFormat(group: UpstreamGroupKey): ClientFormat {
  return normalizeClientFormat(group.client_format)
}

function groupMatchesFormat(group: UpstreamGroupKey, format: ClientFormat) {
  const groupFormat = groupClientFormat(group)
  return format === "any" || groupFormat === "any" || groupFormat === format
}

function groupStatusRank(status: string) {
  return status === "alive" ? 0 : status === "unknown" ? 1 : status === "dead" ? 2 : 3
}

function sortGroupsForDisplay(groups: UpstreamGroupKey[]) {
  return groups.slice().sort((a, b) => {
    return (
      groupStatusRank(effectiveStatus(a)) - groupStatusRank(effectiveStatus(b)) ||
      Number(Boolean(b.charity)) - Number(Boolean(a.charity)) ||
      (b.priority || 0) - (a.priority || 0) ||
      a.ratio - b.ratio ||
      a.failure_count - b.failure_count ||
      a.id - b.id
    )
  })
}

function cleanGroupIDs(ids: number[], groups: UpstreamGroupKey[], format: ClientFormat) {
  const allowed = new Set(groups.filter((group) => groupMatchesFormat(group, format)).map((group) => group.id))
  return Array.from(new Set(ids.filter((id) => allowed.has(id)))).sort((a, b) => a - b)
}

function tokensToMInput(value?: number | null) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return ""
  const m = n / TOKEN_M
  return Number.isInteger(m) ? String(m) : String(Number(m.toFixed(3)))
}

function mInputToTokens(raw: string) {
  const text = raw.trim()
  if (!text) return 0
  const n = Number(text)
  if (!Number.isFinite(n) || n <= 0) return 0
  return Math.round(n * TOKEN_M)
}

function sanitizeMInput(value: string) {
  return value.replace(/[^\d.]/g, "").replace(/(\..*)\./g, "$1")
}

function formatExpiry(value?: string | null) {
  if (!value) return "永不过期"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "未知"
  if (date.getTime() <= Date.now()) return "已过期"
  return date.toLocaleDateString("zh-CN", { month: "2-digit", day: "2-digit" })
}

async function copyText(text: string) {
  await navigator.clipboard.writeText(text)
  toast.success("已复制")
}

type HealthItem = GatewayHealthResult["items"][number]

function healthItemFromProgress(ev: ProgressEvent): HealthItem | null {
  const data = ev.data as { item?: HealthItem } | undefined
  return data?.item?.id ? data.item : null
}

function healthProgressFromEvent(ev: ProgressEvent, fallback: HealthProgress): HealthProgress {
  const data = (ev.data ?? {}) as {
    completed?: number
    total?: number
    batch?: number
    batches?: number
    batch_size?: number
  }
  return {
    running: true,
    completed: Number(data.completed ?? ev.index ?? fallback.completed ?? 0),
    total: Number(data.total ?? ev.total ?? fallback.total ?? 0),
    batch: Number(data.batch ?? fallback.batch ?? 0),
    batches: Number(data.batches ?? fallback.batches ?? 0),
    batchSize: Number(data.batch_size ?? fallback.batchSize ?? 10),
    message: ev.message || fallback.message,
  }
}

function draftFromKey(key: GatewayKey): KeyDraft {
  const ids = key.allowed_group_ids ?? []
  return {
    name: key.name || "default",
    enabled: key.enabled !== false,
    clientFormat: normalizeClientFormat(key.client_format),
    scope: ids.length > 0 ? "selected" : "all",
    selectedGroupIds: ids,
    dailyLimitM: tokensToMInput(key.daily_limit),
    totalLimitM: tokensToMInput(key.total_limit),
    costPerMillion: key.cost_per_million > 0 ? String(key.cost_per_million) : "",
    expiresInDays: "keep",
  }
}

function buildGatewayKeyPayload(draft: KeyDraft, includeEnabled: boolean, includeExpiry: boolean) {
  const payload: Record<string, unknown> = {
    name: draft.name.trim() || "default",
    client_format: draft.clientFormat,
    allowed_group_ids: draft.scope === "selected" ? draft.selectedGroupIds : [],
    daily_limit: mInputToTokens(draft.dailyLimitM),
    total_limit: mInputToTokens(draft.totalLimitM),
    cost_per_million: Math.max(0, Number(draft.costPerMillion) || 0),
  }
  if (includeEnabled) {
    payload.enabled = draft.enabled
  }
  if (includeExpiry) {
    payload.expires_in_days = Math.max(0, Math.floor(Number(draft.expiresInDays) || 0))
  }
  return payload
}

function selectedGroupSummary(key: GatewayKey, groups: UpstreamGroupKey[]) {
  const ids = key.allowed_group_ids ?? []
  if (ids.length === 0) return "全部分组，按优先级和低倍率顺序调度"
  const names = ids
    .slice(0, 3)
    .map((id) => groups.find((group) => group.id === id))
    .filter(Boolean)
    .map((group) => `${group?.channel_name || `#${group?.channel_id}`} / ${group?.group_name}`)
  return `指定 ${ids.length} 个${names.length ? `：${names.join("、")}` : ""}`
}

function KeyDraftFields({
  draft,
  groups,
  onChange,
  showEnabled = false,
  showKeepExpiry = false,
}: {
  draft: KeyDraft
  groups: UpstreamGroupKey[]
  onChange: (draft: KeyDraft) => void
  showEnabled?: boolean
  showKeepExpiry?: boolean
}) {
  const eligibleGroups = groups.filter((group) => groupMatchesFormat(group, draft.clientFormat))
  const selected = new Set(draft.selectedGroupIds)

  function updateFormat(format: ClientFormat) {
    onChange({
      ...draft,
      clientFormat: format,
      selectedGroupIds: cleanGroupIDs(draft.selectedGroupIds, groups, format),
    })
  }

  function toggleGroup(id: number, checked: boolean) {
    const next = checked
      ? [...draft.selectedGroupIds, id]
      : draft.selectedGroupIds.filter((item) => item !== id)
    onChange({ ...draft, selectedGroupIds: cleanGroupIDs(next, groups, draft.clientFormat) })
  }

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-[1fr_0.75fr]">
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-name">Key 名称</Label>
          <Input
            id="gateway-key-name"
            value={draft.name}
            onChange={(event) => onChange({ ...draft, name: event.target.value })}
            placeholder="例如：公益 OpenAI Key"
          />
        </div>
        <div className="space-y-1.5">
          <Label>请求格式</Label>
          <Select value={draft.clientFormat} onValueChange={(value) => updateFormat(normalizeClientFormat(value))}>
            <SelectTrigger>
              <SelectValue placeholder="选择格式" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="openai">OpenAI</SelectItem>
              <SelectItem value="claude">Claude</SelectItem>
              <SelectItem value="grok">Grok (xAI)</SelectItem>
              <SelectItem value="any">不限</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {showEnabled ? (
        <div className="flex items-center justify-between gap-3 rounded-md border border-border bg-muted/20 px-3 py-2">
          <div>
            <p className="text-sm font-medium text-foreground">Key 状态</p>
            <p className="text-xs text-muted-foreground">停用后客户端会收到无效 Key 提示</p>
          </div>
          <Switch checked={draft.enabled} onCheckedChange={(checked) => onChange({ ...draft, enabled: checked })} />
        </div>
      ) : null}

      <div className="grid gap-3 sm:grid-cols-3">
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-daily">每日额度（M）</Label>
          <Input
            id="gateway-key-daily"
            value={draft.dailyLimitM}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, dailyLimitM: sanitizeMInput(event.target.value) })}
            placeholder="留空不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-total">总额度（M）</Label>
          <Input
            id="gateway-key-total"
            value={draft.totalLimitM}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, totalLimitM: sanitizeMInput(event.target.value) })}
            placeholder="留空不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-cost">每百万 Token 费用</Label>
          <Input
            id="gateway-key-cost"
            value={draft.costPerMillion}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, costPerMillion: sanitizeMInput(event.target.value) })}
            placeholder="留空不计费"
          />
        </div>
        <div className="space-y-1.5">
          <Label>过期时间</Label>
          <Select value={draft.expiresInDays} onValueChange={(value) => onChange({ ...draft, expiresInDays: value })}>
            <SelectTrigger>
              <SelectValue placeholder="过期时间" />
            </SelectTrigger>
            <SelectContent>
              {showKeepExpiry ? <SelectItem value="keep">保留当前</SelectItem> : null}
              <SelectItem value="1">1 天</SelectItem>
              <SelectItem value="2">2 天</SelectItem>
              <SelectItem value="7">7 天</SelectItem>
              <SelectItem value="30">30 天</SelectItem>
              <SelectItem value="0">永不过期</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <Label>绑定上游分组</Label>
          <Select value={draft.scope} onValueChange={(value) => onChange({ ...draft, scope: value as GroupScope })}>
            <SelectTrigger className="h-8 w-32 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部</SelectItem>
              <SelectItem value="selected">指定</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="rounded-md border border-border bg-background">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2 text-xs">
            <span className="text-muted-foreground">
              {draft.scope === "all"
                ? `将按优先级和低倍率顺序使用 ${eligibleGroups.length} 个匹配分组`
                : `已选择 ${draft.selectedGroupIds.length}/${eligibleGroups.length} 个匹配分组`}
            </span>
            {draft.scope === "selected" ? (
              <div className="flex items-center gap-1">
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-xs"
                  onClick={() => onChange({ ...draft, selectedGroupIds: eligibleGroups.map((group) => group.id) })}
                >
                  全选
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-xs"
                  onClick={() => onChange({ ...draft, selectedGroupIds: [] })}
                >
                  清空
                </Button>
              </div>
            ) : null}
          </div>
          <div className="max-h-56 overflow-y-auto p-2">
            {eligibleGroups.length === 0 ? (
              <div className="px-2 py-6 text-center text-xs text-muted-foreground">
                没有匹配 {clientFormatLabel(draft.clientFormat)} 的分组，先同步或切换格式
              </div>
            ) : (
              <div className="space-y-1">
                {eligibleGroups.map((group) => {
                  const status = effectiveStatus(group)
                  return (
                    <label
                      key={group.id}
                      className={cn(
                        "flex cursor-pointer items-start gap-2 rounded-md px-2 py-2 text-xs hover:bg-muted/60",
                        draft.scope === "all" && "cursor-default opacity-80",
                      )}
                    >
                      {draft.scope === "selected" ? (
                        <Checkbox
                          checked={selected.has(group.id)}
                          onCheckedChange={(checked) => toggleGroup(group.id, checked === true)}
                        />
                      ) : (
                        <span className="mt-0.5 size-4 rounded-[4px] border border-border bg-muted" />
                      )}
                      <span className="min-w-0 flex-1">
                        <span className="flex flex-wrap items-center gap-1.5">
                          <span className="font-medium text-foreground">{group.channel_name || `#${group.channel_id}`}</span>
                          <span className="text-muted-foreground">/</span>
                          <span className="truncate text-foreground">{group.group_name}</span>
                          <Badge variant="outline" className="h-5 px-1.5 text-[10px]">
                            {clientFormatLabel(group.client_format)}
                          </Badge>
                          <Badge variant="outline" className={cn("h-5 px-1.5 text-[10px]", statusTone(status))}>
                            {statusText(status)}
                          </Badge>
                        </span>
                        <span className="mt-1 block text-muted-foreground">
                          优先级 {group.priority || 0} · 倍率 {formatRatio(group.ratio)} · {channelTypeLabel(group.channel_type)}
                        </span>
                      </span>
                    </label>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export function GatewayPanel({ section = "all" }: { section?: "all" | "keys" | "groups" } = {}) {
  const showKeys = section === "all" || section === "keys"
  const showGroups = section === "all" || section === "groups"
  const [keys, setKeys] = useState<GatewayKey[]>([])
  const [groups, setGroups] = useState<UpstreamGroupKey[]>([])
  const [groupTotal, setGroupTotal] = useState(0)
  const [groupPages, setGroupPages] = useState(1)
  const [groupAliveTotal, setGroupAliveTotal] = useState(0)
  const [groupDeadTotal, setGroupDeadTotal] = useState(0)
  const [groupEnabledTotal, setGroupEnabledTotal] = useState(0)
  const [channels, setChannels] = useState<Channel[]>([])
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [createDraft, setCreateDraft] = useState<KeyDraft>(() => createDefaultDraft())
  const [createOpen, setCreateOpen] = useState(false)
  const [editingKey, setEditingKey] = useState<GatewayKey | null>(null)
  const [editDraft, setEditDraft] = useState<KeyDraft>(() => createDefaultDraft())
  const [editOpen, setEditOpen] = useState(false)
  const [revealed, setRevealed] = useState<Record<number, string>>({})
  const [visible, setVisible] = useState<Record<number, boolean>>({})
  const [concurrencyDrafts, setConcurrencyDrafts] = useState<Record<number, string>>({})
  const [priorityDrafts, setPriorityDrafts] = useState<Record<number, string>>({})
  const [healthResults, setHealthResults] = useState<Record<number, GatewayHealthResult["items"][number]>>({})
  const [healthProgress, setHealthProgress] = useState<HealthProgress | null>(null)
  const [manualOpen, setManualOpen] = useState(false)
  const [manualDraft, setManualDraft] = useState<ManualDraft>(() => createDefaultManualDraft())
  const [pageSize, setPageSize] = useState(10)
  const [page, setPage] = useState(1)
  const [groupSearch, setGroupSearch] = useState("")
  const [keySearch, setKeySearch] = useState("")

  const filteredKeys = useMemo(() => {
    const query = keySearch.trim().toLowerCase()
    if (!query) return keys
    return keys.filter((key) =>
      [key.name, key.key_prefix, key.client_format]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query)),
    )
  }, [keySearch, keys])

  const aliveCount = useMemo(() => groups.filter((group) => effectiveStatus(group) === "alive").length, [groups])
  const deadCount = useMemo(() => groups.filter((group) => effectiveStatus(group) === "dead").length, [groups])
  const enabledGroupCount = useMemo(() => groups.filter((group) => group.enabled !== false).length, [groups])

  const serverGroupPaging = showGroups && !showKeys
  const totalGroups = serverGroupPaging ? groupTotal : groups.length
  const displayAliveCount = serverGroupPaging ? groupAliveTotal : aliveCount
  const displayDeadCount = serverGroupPaging ? groupDeadTotal : deadCount
  const displayEnabledCount = serverGroupPaging ? groupEnabledTotal : enabledGroupCount
  const totalPages = serverGroupPaging ? Math.max(1, groupPages) : Math.max(1, Math.ceil(groups.length / pageSize))
  const currentPage = Math.min(page, totalPages)
  const pagedGroups = useMemo(
    () => (serverGroupPaging ? groups : groups.slice((currentPage - 1) * pageSize, currentPage * pageSize)),
    [groups, currentPage, pageSize, serverGroupPaging],
  )
  const rangeStart = totalGroups === 0 ? 0 : (currentPage - 1) * pageSize + 1
  const rangeEnd = Math.min(currentPage * pageSize, totalGroups)

  useEffect(() => {
    if (page > totalPages) setPage(totalPages)
  }, [page, totalPages])

  useEffect(() => {
    setPage(1)
  }, [groupSearch])

  async function load() {
    setLoading(true)
    try {
      const groupQuery = new URLSearchParams({ page: String(currentPage), page_size: String(pageSize) })
      if (groupSearch.trim()) groupQuery.set("search", groupSearch.trim())
      const groupRequest = serverGroupPaging
        ? apiFetch<UpstreamGroupKeyPage>(`/gateway/group-keys?${groupQuery.toString()}`)
        : apiFetch<UpstreamGroupKey[]>("/gateway/group-keys")
      const [keyList, groupResult, channelPage] = await Promise.all([
        apiFetch<GatewayKey[]>("/gateway/keys"),
        groupRequest,
        apiFetch<ChannelPage>("/channels?page=1&page_size=-1"),
      ])
      setKeys(Array.isArray(keyList) ? keyList : [])
      setChannels(Array.isArray(channelPage?.items) ? channelPage.items : [])
      const rawGroups = Array.isArray(groupResult) ? groupResult : groupResult.items ?? []
      const nextGroups = sortGroupsForDisplay(rawGroups)
      if (Array.isArray(groupResult)) {
        setGroupTotal(nextGroups.length)
        setGroupPages(Math.max(1, Math.ceil(nextGroups.length / pageSize)))
        setGroupAliveTotal(nextGroups.filter((group) => effectiveStatus(group) === "alive").length)
        setGroupDeadTotal(nextGroups.filter((group) => effectiveStatus(group) === "dead").length)
        setGroupEnabledTotal(nextGroups.filter((group) => group.enabled !== false).length)
      } else {
        setGroupTotal(groupResult.total ?? nextGroups.length)
        setGroupPages(groupResult.pages ?? 1)
        setGroupAliveTotal(groupResult.alive ?? 0)
        setGroupDeadTotal(groupResult.dead ?? 0)
        setGroupEnabledTotal(groupResult.enabled ?? 0)
      }
      setGroups(nextGroups)
      setConcurrencyDrafts(
        Object.fromEntries(nextGroups.map((group) => [group.id, String(group.concurrency_limit || 0)])),
      )
      setPriorityDrafts(
        Object.fromEntries(nextGroups.map((group) => [group.id, String(group.priority || 0)])),
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
  }, [currentPage, pageSize, serverGroupPaging, groupSearch])

  function validateDraft(draft: KeyDraft) {
    if (draft.scope === "selected" && draft.selectedGroupIds.length === 0) {
      toast.error("指定分组模式下至少选择一个上游分组")
      return false
    }
    if (mInputToTokens(draft.dailyLimitM) < 0 || mInputToTokens(draft.totalLimitM) < 0) {
      toast.error("额度必须是大于等于 0 的数字")
      return false
    }
    return true
  }

  async function createGatewayKey() {
    if (!validateDraft(createDraft)) return
    setBusy("create-key")
    try {
      const created = await apiFetch<GatewayKey>("/gateway/keys", {
        method: "POST",
        body: JSON.stringify(buildGatewayKeyPayload(createDraft, false, true)),
      })
      if (created.key) {
        setRevealed((prev) => ({ ...prev, [created.id]: created.key ?? "" }))
        setVisible((prev) => ({ ...prev, [created.id]: false }))
        await copyText(created.key)
      }
      toast.success("网关 Key 已创建")
      setCreateDraft(createDefaultDraft())
      setCreateOpen(false)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "创建网关 Key 失败")
    } finally {
      setBusy(null)
    }
  }

  function openEditKey(key: GatewayKey) {
    setEditingKey(key)
    setEditDraft(draftFromKey(key))
    setEditOpen(true)
  }

  async function updateGatewayKey() {
    if (!editingKey || !validateDraft(editDraft)) return
    setBusy(`edit-${editingKey.id}`)
    try {
      const updated = await apiFetch<GatewayKey>(`/gateway/keys/${editingKey.id}`, {
        method: "PATCH",
        body: JSON.stringify(buildGatewayKeyPayload(editDraft, true, editDraft.expiresInDays !== "keep")),
      })
      setKeys((prev) => prev.map((key) => (key.id === updated.id ? updated : key)))
      toast.success("网关 Key 已保存")
      setEditOpen(false)
      setEditingKey(null)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "保存网关 Key 失败")
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
    setHealthResults((prev) => {
      const next = { ...prev }
      for (const group of groups) {
        if (group.enabled !== false) {
          next[group.id] = {
            id: group.id,
            channel_id: group.channel_id,
            channel_name: group.channel_name || "",
            group_ref: group.group_ref,
            group_name: group.group_name,
            ratio: group.ratio,
            status: "queued",
            latency_ms: 0,
          }
        }
      }
      return next
    })
    setHealthProgress({
      running: true,
      completed: 0,
      total: displayEnabledCount || totalGroups,
      batch: 0,
      batches: 0,
      batchSize: 30,
      message: "测活排队中...",
    })
    try {
      let finalResult: GatewayHealthResult | undefined
      await testGatewayHealthStream({
        onEvent: (ev) => {
          const item = healthItemFromProgress(ev)
          if (item) {
            setHealthResults((prev) => ({ ...prev, [item.id]: item }))
          }
          setHealthProgress((prev) => healthProgressFromEvent(ev, prev ?? {
            running: true,
            completed: 0,
            total: displayEnabledCount || totalGroups,
            batch: 0,
            batches: 0,
            batchSize: 30,
            message: "测活中...",
          }))
          if (ev.stage === "done" && ev.data && typeof ev.data === "object" && "items" in ev.data) {
            finalResult = ev.data as GatewayHealthResult
          }
        },
      })
      const completedResult = finalResult as GatewayHealthResult | undefined
      if (completedResult) {
        setHealthResults(Object.fromEntries((completedResult.items ?? []).map((item) => [item.id, item])))
        toast.success(`测活完成：存活 ${completedResult.alive}，死亡 ${completedResult.dead}`)
      } else {
        toast.success("测活完成")
      }
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "一键测活失败")
    } finally {
      setBusy(null)
      setHealthProgress((prev) => prev ? { ...prev, running: false } : null)
    }
  }

  async function testGroup(group: UpstreamGroupKey) {
    setBusy(`test-${group.id}`)
    try {
      const result = await apiFetch<GatewayHealthResult["items"][number]>(`/gateway/group-keys/${group.id}/test`, { method: "POST" })
      setHealthResults((prev) => ({ ...prev, [group.id]: result }))
      toast.success(`${group.channel_name || "上游"} / ${group.group_name}：${result.status === "alive" ? "存活" : result.status === "disabled" ? "已禁用" : "不可用"}`)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "分组测活失败")
    } finally {
      setBusy(null)
    }
  }

  async function updateGroup(
    group: UpstreamGroupKey,
    patch: { concurrency_limit?: number; enabled?: boolean; request_mode?: UpstreamRequestMode; priority?: number; client_format?: string; charity?: boolean },
  ) {
    setBusy(`group-${group.id}`)
    try {
      const updated = await apiFetch<UpstreamGroupKey>(`/gateway/group-keys/${group.id}`, {
        method: "PATCH",
        body: JSON.stringify({
          concurrency_limit: patch.concurrency_limit ?? group.concurrency_limit ?? 0,
          ...(patch.enabled == null ? {} : { enabled: patch.enabled }),
          ...(patch.request_mode == null ? {} : { request_mode: patch.request_mode }),
          ...(patch.priority == null ? {} : { priority: patch.priority }),
          ...(patch.client_format == null ? {} : { client_format: patch.client_format }),
          ...(patch.charity == null ? {} : { charity: patch.charity }),
        }),
      })
      setGroups((prev) => sortGroupsForDisplay(prev.map((item) => (item.id === updated.id ? updated : item))))
      setConcurrencyDrafts((prev) => ({ ...prev, [group.id]: String(updated.concurrency_limit || 0) }))
      setPriorityDrafts((prev) => ({ ...prev, [group.id]: String(updated.priority || 0) }))
      return updated
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "保存上游分组失败")
      return null
    } finally {
      setBusy(null)
    }
  }

  async function deleteGroup(group: UpstreamGroupKey) {
    if (!window.confirm(`确认删除分组「${group.group_name}」？\n\n用于清理上游已删除、本地却残留的分组。仅删除本地记录，不影响上游。`)) {
      return
    }
    setBusy(`group-${group.id}`)
    try {
      await apiFetch(`/gateway/group-keys/${group.id}`, { method: "DELETE" })
      setGroups((prev) => prev.filter((item) => item.id !== group.id))
      toast.success("分组已删除")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "删除分组失败")
    } finally {
      setBusy(null)
    }
  }

  async function clearCooldown(group: UpstreamGroupKey) {
    setBusy(`group-${group.id}`)
    try {
      const updated = await apiFetch<UpstreamGroupKey>(`/gateway/group-keys/${group.id}/clear-cooldown`, { method: "POST" })
      setGroups((prev) => sortGroupsForDisplay(prev.map((item) => (item.id === updated.id ? updated : item))))
      toast.success("已解除冷却，恢复调度")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "解除冷却失败")
    } finally {
      setBusy(null)
    }
  }

  async function saveConcurrencyLimit(group: UpstreamGroupKey) {
    const raw = concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)
    const limit = Math.max(0, Math.floor(Number(raw) || 0))
    const updated = await updateGroup(group, { concurrency_limit: limit })
    if (updated) {
      toast.success("并发上限已保存")
    }
  }

  async function savePriority(group: UpstreamGroupKey) {
    const raw = priorityDrafts[group.id] ?? String(group.priority || 0)
    const priority = Math.max(0, Math.floor(Number(raw) || 0))
    const updated = await updateGroup(group, { priority })
    if (updated) {
      toast.success("调度优先级已保存")
    }
  }

  async function toggleGroupEnabled(group: UpstreamGroupKey, enabled: boolean) {
    const updated = await updateGroup(group, { enabled })
    if (updated) {
      toast.success(enabled ? "上游分组已启用" : "上游分组已禁用，调度和测活会跳过它")
    }
  }

  async function changeGroupRequestMode(group: UpstreamGroupKey, requestMode: UpstreamRequestMode) {
    const updated = await updateGroup(group, { request_mode: requestMode })
    if (updated) {
      toast.success(`上游接口已切换为 ${requestModeLabel(requestMode)}`)
    }
  }

  async function changeGroupClientFormat(group: UpstreamGroupKey, format: string) {
    const updated = await updateGroup(group, { client_format: format })
    if (updated) {
      toast.success(`已标记为 ${clientFormatLabel(format)} 渠道`)
    }
  }

  async function toggleGroupCharity(group: UpstreamGroupKey, charity: boolean) {
    const updated = await updateGroup(group, { charity })
    if (updated) {
      toast.success(charity ? "已标记为公益分组" : "已取消公益标记")
    }
  }

  async function submitManualGroup() {
    if (!manualDraft.groupName.trim()) {
      toast.error("请填写分组名")
      return
    }
    if (!manualDraft.key.trim()) {
      toast.error("请填写 Key")
      return
    }
    if (manualDraft.sourceMode === "existing" && !manualDraft.channelId) {
      toast.error("请选择已有渠道")
      return
    }
    if (manualDraft.sourceMode === "new" && !manualDraft.siteUrl.trim()) {
      toast.error("请填写上游地址")
      return
    }
    setBusy("manual-add")
    try {
      const created = await apiFetch<UpstreamGroupKey>("/gateway/group-keys/manual", {
        method: "POST",
        body: JSON.stringify({
          ...(manualDraft.sourceMode === "existing"
            ? { channel_id: Number(manualDraft.channelId) }
            : {
                channel_name: manualDraft.channelName.trim() || undefined,
                site_url: manualDraft.siteUrl.trim() || undefined,
              }),
          group_name: manualDraft.groupName.trim(),
          group_description: manualDraft.groupDescription.trim() || undefined,
          key: manualDraft.key.trim(),
          ratio: Number(manualDraft.ratio) || 0,
          client_format: manualDraft.clientFormat,
          request_mode: manualDraft.requestMode,
          charity: manualDraft.charity,
          priority: Math.max(0, Math.floor(Number(manualDraft.priority) || 0)),
        }),
      })
      try {
        const health = await apiFetch<GatewayHealthResult["items"][number]>(`/gateway/group-keys/${created.id}/test`, { method: "POST" })
        setHealthResults((prev) => ({ ...prev, [created.id]: health }))
        if (health.status === "alive") {
          toast.success("手动渠道已添加，并已通过 Responses 测活")
        } else {
          toast.warning(`手动渠道已保存，但测活未通过：${health.error || "上游无有效响应"}`)
        }
      } catch (e) {
        const err = e as Error
        toast.warning(`手动渠道已保存，但自动测活失败：${err.message || "请稍后单独测活"}`)
      }
      setManualOpen(false)
      setManualDraft(createDefaultManualDraft())
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "手动添加分组失败")
    } finally {
      setBusy(null)
    }
  }

  return (
    <Card className="border border-border shadow-none">
      <CardHeader className="flex flex-col gap-3 pb-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <CardTitle className="text-base font-semibold">{showGroups && !showKeys ? "可用渠道" : "调度网关"}</CardTitle>
          <p className="mt-1 text-xs text-muted-foreground">
            {showGroups && !showKeys
              ? "上游分组列表 · 状态 / 优先级 / 并发 / 公益 / 冷却"
              : "/v1 兼容入口 · 优先级优先 · Key 可绑定指定上游"}
          </p>
        </div>
        {showGroups ? (
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline" className="border-success/20 bg-success/10 text-success">
              存活 {displayAliveCount}
            </Badge>
            <Badge variant="outline" className="border-danger/20 bg-danger/10 text-danger">
              死亡 {displayDeadCount}
            </Badge>
            <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
              启用 {displayEnabledCount}/{totalGroups}
            </Badge>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={() => setManualOpen(true)}>
              <Plus className="size-3.5" />
              手动添加
            </Button>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={bootstrapGroups}>
              {busy === "bootstrap" ? <Loader2 className="size-3.5 animate-spin" /> : <Plus className="size-3.5" />}
              一键创建分组 Key
            </Button>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={testGroups}>
              {busy === "test" ? <Loader2 className="size-3.5 animate-spin" /> : <RefreshCw className="size-3.5" />}
              一键分组测活
            </Button>
            {healthProgress ? (
              <div className="w-full rounded-md border border-border bg-muted/30 px-3 py-2 text-[11px] text-muted-foreground sm:w-auto">
                <span className="font-medium text-foreground">
                  {healthProgress.completed}/{healthProgress.total || totalGroups}
                </span>
                <span className="mx-1">·</span>
                <span>
                  {healthProgress.batch > 0 && healthProgress.batches > 0
                    ? `第 ${healthProgress.batch}/${healthProgress.batches} 批，每批 ${healthProgress.batchSize} 个`
                    : "等待批次开始"}
                </span>
                <span className="mx-1">·</span>
                <span>{healthProgress.running ? healthProgress.message : "测活已结束"}</span>
              </div>
            ) : null}
          </div>
        ) : null}
      </CardHeader>
      <CardContent className="space-y-4">
        {showKeys ? (
        <div className="rounded-md border border-border bg-muted/10 p-3">
          <div className="min-w-0">
            <div className="mb-2 flex items-center justify-between gap-2">
              <div>
                <p className="text-xs font-medium text-foreground">已有调用 Key</p>
                <span className="text-[11px] text-muted-foreground">Bearer Key · {filteredKeys.length}/{keys.length} 个</span>
              </div>
              <Button size="sm" className="h-8 gap-1.5 text-xs" disabled={!!busy} onClick={() => setCreateOpen(true)}>
                <KeyRound className="size-3.5" />
                创建调用 Key
              </Button>
            </div>
            <div className="mb-3 max-w-sm">
              <Input
                value={keySearch}
                onChange={(event) => setKeySearch(event.target.value)}
                placeholder="搜索 Key 名称、前缀或格式"
                aria-label="搜索调用 Key"
              />
            </div>
            {filteredKeys.length === 0 ? (
              <div className="rounded-md border border-dashed border-border bg-background px-3 py-8 text-center text-xs text-muted-foreground">
                {keys.length === 0 ? "还没有网关 Key" : "没有匹配的调用 Key"}
              </div>
            ) : (
              <div className="overflow-x-auto rounded-md border border-border bg-background">
                <Table className="min-w-[880px]">
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-44">名称</TableHead>
                      <TableHead>密钥</TableHead>
                      <TableHead className="hidden lg:table-cell">绑定分组</TableHead>
                      <TableHead className="hidden text-right md:table-cell">今日</TableHead>
                      <TableHead className="hidden text-right xl:table-cell">总量</TableHead>
                      <TableHead className="hidden lg:table-cell">到期</TableHead>
                      <TableHead className="hidden md:table-cell">最近使用</TableHead>
                      <TableHead className="w-36 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredKeys.map((key) => (
                      <TableRow key={key.id}>
                        <TableCell>
                          <div className="min-w-0">
                            <p className="truncate text-xs font-medium text-foreground">{key.name}</p>
                            <p className={cn("text-[11px]", key.enabled ? "text-success" : "text-muted-foreground")}>
                              {key.enabled ? "启用中" : "已停用"} · {clientFormatLabel(key.client_format)}
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
                        <TableCell className="hidden max-w-60 truncate text-xs text-muted-foreground lg:table-cell">
                          {selectedGroupSummary(key, groups)}
                        </TableCell>
                        <TableCell className="hidden text-right text-xs md:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.today_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.daily_limit > 0 ? formatTokens(key.daily_limit) : "不限"}</span>
                          <span className="mt-0.5 block text-[10px] text-muted-foreground">费用 {key.today_cost > 0 ? key.today_cost.toFixed(4) : "0"}</span>
                        </TableCell>
                        <TableCell className="hidden text-right text-xs xl:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.total_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.total_limit > 0 ? formatTokens(key.total_limit) : "不限"}</span>
                          <span className="mt-0.5 block text-[10px] text-muted-foreground">累计费用 {key.total_cost > 0 ? key.total_cost.toFixed(4) : "0"}</span>
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
                              className="size-7"
                              disabled={!!busy}
                              title="编辑"
                              onClick={() => openEditKey(key)}
                            >
                              <Pencil className="size-3.5" />
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
        ) : null}

        {showGroups ? (
        <>
        <div className="relative max-w-xl">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={groupSearch}
            onChange={(event) => setGroupSearch(event.target.value)}
            className="h-9 pl-8 text-xs"
            placeholder="搜索渠道、上游分组、分组说明或分组标识"
            aria-label="搜索可用渠道对应的上游分组"
          />
        </div>
        <div className="overflow-x-auto rounded-md border border-border">
          <Table className="min-w-[1360px]">
            <TableHeader>
              <TableRow>
                <TableHead>状态</TableHead>
                <TableHead className="w-20">启用</TableHead>
                <TableHead className="w-20">公益</TableHead>
                <TableHead>渠道</TableHead>
                <TableHead>分组</TableHead>
                <TableHead className="w-24">格式</TableHead>
                <TableHead className="w-32">上游接口</TableHead>
                <TableHead className="text-right">倍率</TableHead>
                <TableHead className="w-28">优先级</TableHead>
                <TableHead className="w-32">并发上限</TableHead>
                <TableHead>测活</TableHead>
                <TableHead>错误</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={12} className="h-24 text-center text-xs text-muted-foreground">
                    <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                    加载中...
                  </TableCell>
                </TableRow>
              ) : groups.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={12} className="h-24 text-center text-xs text-muted-foreground">
                    还没有分组 Key，先点“一键创建分组 Key”
                  </TableCell>
                </TableRow>
              ) : (
                pagedGroups.map((group) => {
                  const latestHealth = healthResults[group.id]
                  const status = latestHealth?.status ?? effectiveStatus(group)
                  const Icon = status === "alive" ? CheckCircle2 : status === "dead" ? XCircle : RefreshCw
                  const latencyMS = latestHealth?.latency_ms ?? group.last_latency_ms ?? 0
                  return (
                    <TableRow key={group.id} className={cn(group.charity && "bg-success/5")}>
                      <TableCell>
                        <Badge variant="outline" className={cn("gap-1.5", statusTone(status))}>
                          <Icon className={cn("size-3", status === "checking" && "animate-spin")} />
                          {statusText(status)}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Switch
                          checked={group.enabled !== false}
                          disabled={!!busy}
                          title={group.enabled === false ? "启用这个上游分组" : "禁用后不会参与调度和测活"}
                          onCheckedChange={(checked) => void toggleGroupEnabled(group, checked)}
                        />
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-col items-start gap-1">
                          <Switch
                            checked={group.charity === true}
                            disabled={!!busy}
                            title={group.charity ? "取消公益标记" : "标记为公益分组"}
                            onCheckedChange={(checked) => void toggleGroupCharity(group, checked)}
                          />
                          {group.charity ? (
                            <Badge variant="outline" className="gap-1 border-success/20 bg-success/10 px-1.5 text-[10px] text-success">
                              <HeartHandshake className="size-3" />
                              公益
                            </Badge>
                          ) : null}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm font-medium">{group.channel_name || `#${group.channel_id}`}</div>
                        <div className="text-[11px] text-muted-foreground">{channelTypeLabel(group.channel_type)}</div>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm font-medium">{group.group_name}</div>
                        <div className="max-w-96 truncate text-[11px] text-muted-foreground">{group.group_description || group.group_ref}</div>
                      </TableCell>
                      <TableCell>
                        <Select
                          value={normalizeClientFormat(group.client_format)}
                          disabled={!!busy}
                          onValueChange={(value) => void changeGroupClientFormat(group, value)}
                        >
                          <SelectTrigger className="h-8 w-24 text-xs">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="openai">ChatGPT</SelectItem>
                            <SelectItem value="claude">Claude</SelectItem>
                            <SelectItem value="grok">Grok</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Select
                          value={normalizeRequestMode(group.request_mode)}
                          disabled={!!busy}
                          onValueChange={(value) => void changeGroupRequestMode(group, normalizeRequestMode(value))}
                        >
                          <SelectTrigger className="h-8 w-28 text-xs">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="responses">Responses</SelectItem>
                            <SelectItem value="chat">Chat</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="font-mono text-xs">{formatRatio(group.ratio)}</div>
                        <div className="text-[11px] text-muted-foreground">{formatTokens(group.total_tokens)} tok</div>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-1">
                          <Input
                            value={priorityDrafts[group.id] ?? String(group.priority || 0)}
                            inputMode="numeric"
                            className="h-7 w-16 px-2 text-xs"
                            disabled={!!busy}
                            title="数值越大越优先；同优先级再按低倍率调度"
                            onChange={(event) =>
                              setPriorityDrafts((prev) => ({
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
                              const draft = priorityDrafts[group.id] ?? String(group.priority || 0)
                              if (Number(draft) !== (group.priority || 0)) {
                                void savePriority(group)
                              }
                            }}
                          />
                          {busy === `group-${group.id}` && <Loader2 className="size-3 animate-spin text-muted-foreground" />}
                        </div>
                        <div className="mt-1 text-[11px] text-muted-foreground">越大越优先</div>
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
                          {busy === `group-${group.id}` && <Loader2 className="size-3 animate-spin text-muted-foreground" />}
                        </div>
                        <div className="mt-1 text-[11px] text-muted-foreground">{(group.concurrency_limit || 0) > 0 ? `${group.concurrency_limit} 路` : "不限"}</div>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        <div>{latestHealth?.checked_at ? relativeTime(latestHealth.checked_at) : group.last_checked_at ? relativeTime(group.last_checked_at) : "未测"}</div>
                        <div className="mt-1 flex items-center gap-1.5 text-[11px]">
                          <span>{latencyMS > 0 ? `${latencyMS}ms` : "延迟 —"}</span>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-6 px-1.5 text-[11px]"
                            disabled={!!busy || group.enabled === false}
                            title="仅测活此上游分组"
                            onClick={() => void testGroup(group)}
                          >
                            {busy === `test-${group.id}` ? <Loader2 className="size-3 animate-spin" /> : <RefreshCw className="size-3" />}
                            测活
                          </Button>
                        </div>
                      </TableCell>
                      <TableCell className="max-w-72 text-xs text-danger">
                        <div className="flex items-center gap-2">
                          <span className="truncate" title={group.last_error || ""}>{group.last_error || ""}</span>
                          {group.disabled_until && new Date(group.disabled_until).getTime() > Date.now() ? (
                            <Button
                              variant="outline"
                              size="sm"
                              className="h-7 shrink-0 px-2 text-[11px]"
                              disabled={!!busy}
                              title="立即解除冷却，恢复调度"
                              onClick={() => void clearCooldown(group)}
                            >
                              解除冷却
                            </Button>
                          ) : null}
                          <Button
                            variant="ghost"
                            size="icon"
                            className="size-7 shrink-0 text-muted-foreground hover:text-danger"
                            disabled={!!busy}
                            title="删除该分组（清理上游已删除的残留，仅删本地）"
                            onClick={() => void deleteGroup(group)}
                          >
                            <Trash2 className="size-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </div>

        {showGroups && totalGroups > 0 ? (
          <div className="flex flex-col gap-2 rounded-md border border-border bg-muted/10 px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-xs text-muted-foreground">
              显示 {rangeStart}-{rangeEnd} / {totalGroups} 个分组
            </div>
            <div className="flex items-center gap-1.5">
              <div className="flex items-center gap-1 text-xs text-muted-foreground">
                <span>每页</span>
                <Select
                  value={String(pageSize)}
                  onValueChange={(value) => {
                    setPageSize(Number(value))
                    setPage(1)
                  }}
                >
                  <SelectTrigger className="h-8 w-20 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="10">10 条</SelectItem>
                    <SelectItem value="20">20 条</SelectItem>
                    <SelectItem value="50">50 条</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button
                variant="outline"
                size="sm"
                className="h-8 gap-1 px-2 text-xs"
                disabled={currentPage <= 1}
                onClick={() => setPage((prev) => Math.max(1, prev - 1))}
              >
                <ChevronLeft className="size-3.5" />
                上一页
              </Button>
              <span className="min-w-16 text-center text-xs text-muted-foreground">
                {currentPage} / {totalPages}
              </span>
              <Button
                variant="outline"
                size="sm"
                className="h-8 gap-1 px-2 text-xs"
                disabled={currentPage >= totalPages}
                onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
              >
                下一页
                <ChevronRight className="size-3.5" />
              </Button>
            </div>
          </div>
        ) : null}
        </>
        ) : null}
      </CardContent>

      <Dialog
        open={manualOpen}
        onOpenChange={(open) => {
          setManualOpen(open)
          if (!open) setManualDraft(createDefaultManualDraft())
        }}
      >
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>手动添加渠道</DialogTitle>
            <DialogDescription>
              给无法登录、只能拿到 Key 的上游手动创建分组。分组名和 Key 为必填。
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label>接入方式</Label>
              <Select
                value={manualDraft.sourceMode}
                onValueChange={(value) =>
                  setManualDraft((prev) => ({
                    ...prev,
                    sourceMode: value === "existing" ? "existing" : "new",
                  }))
                }
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="new">新建渠道</SelectItem>
                  <SelectItem value="existing">选择已有渠道</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {manualDraft.sourceMode === "existing" ? (
              <div className="space-y-1.5">
                <Label>已有渠道</Label>
                <Select
                  value={manualDraft.channelId}
                  onValueChange={(value) => setManualDraft((prev) => ({ ...prev, channelId: value }))}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="选择渠道" />
                  </SelectTrigger>
                  <SelectContent>
                    {channels.map((channel) => (
                      <SelectItem key={channel.id} value={String(channel.id)}>
                        {channel.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            ) : (
              <>
                <div className="space-y-1.5">
                  <Label htmlFor="manual-channel-name">渠道名</Label>
                  <Input
                    id="manual-channel-name"
                    value={manualDraft.channelName}
                    onChange={(event) => setManualDraft((prev) => ({ ...prev, channelName: event.target.value }))}
                    placeholder="例如：某公益中转"
                  />
                </div>
                <div className="space-y-1.5 sm:col-span-2">
                  <Label htmlFor="manual-site-url">上游地址 *</Label>
                  <Input
                    id="manual-site-url"
                    value={manualDraft.siteUrl}
                    onChange={(event) => setManualDraft((prev) => ({ ...prev, siteUrl: event.target.value }))}
                    placeholder="https://..."
                  />
                </div>
              </>
            )}
            <div className="space-y-1.5">
              <Label htmlFor="manual-group-name">分组名 *</Label>
              <Input
                id="manual-group-name"
                value={manualDraft.groupName}
                onChange={(event) => setManualDraft((prev) => ({ ...prev, groupName: event.target.value }))}
                placeholder="例如：default"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="manual-ratio">倍率</Label>
              <Input
                id="manual-ratio"
                value={manualDraft.ratio}
                inputMode="decimal"
                onChange={(event) => setManualDraft((prev) => ({ ...prev, ratio: sanitizeMInput(event.target.value) }))}
                placeholder="1"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="manual-priority">调度优先级</Label>
              <Input
                id="manual-priority"
                value={manualDraft.priority}
                inputMode="numeric"
                onChange={(event) => setManualDraft((prev) => ({ ...prev, priority: event.target.value.replace(/[^\d]/g, "") }))}
                placeholder="0"
              />
            </div>
            <div className="space-y-1.5 sm:col-span-2">
              <Label htmlFor="manual-group-desc">分组说明</Label>
              <Input
                id="manual-group-desc"
                value={manualDraft.groupDescription}
                onChange={(event) => setManualDraft((prev) => ({ ...prev, groupDescription: event.target.value }))}
                placeholder="可选，例如：手动粘贴的公益线路"
              />
            </div>
            <div className="space-y-1.5 sm:col-span-2">
              <Label htmlFor="manual-key">Key *</Label>
              <Input
                id="manual-key"
                value={manualDraft.key}
                onChange={(event) => setManualDraft((prev) => ({ ...prev, key: event.target.value }))}
                placeholder="上游 API Key"
              />
            </div>
            <div className="space-y-1.5">
              <Label>类型</Label>
              <Select
                value={manualDraft.clientFormat}
                onValueChange={(value) => {
                  const format = normalizeClientFormat(value)
                  setManualDraft((prev) => ({
                    ...prev,
                    clientFormat: format === "claude" || format === "grok" ? format : "openai",
                  }))
                }}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="openai">OpenAI</SelectItem>
                  <SelectItem value="claude">Claude</SelectItem>
                  <SelectItem value="grok">Grok (xAI)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label>请求方式</Label>
              <Select
                value={manualDraft.requestMode}
                onValueChange={(value) => setManualDraft((prev) => ({ ...prev, requestMode: normalizeRequestMode(value) }))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="responses">Responses</SelectItem>
                  <SelectItem value="chat">Chat</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center justify-between gap-3 rounded-md border border-border bg-muted/20 px-3 py-2 sm:col-span-2">
              <div>
                <p className="text-sm font-medium text-foreground">是否公益</p>
                <p className="text-xs text-muted-foreground">标记为公益分组后会有醒目标记</p>
              </div>
              <Switch
                checked={manualDraft.charity}
                onCheckedChange={(checked) => setManualDraft((prev) => ({ ...prev, charity: checked }))}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setManualOpen(false)} disabled={!!busy}>
              取消
            </Button>
            <Button onClick={() => void submitManualGroup()} disabled={!!busy}>
              {busy === "manual-add" ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}
              添加
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={createOpen}
        onOpenChange={(open) => {
          setCreateOpen(open)
          if (!open) setCreateDraft(createDefaultDraft())
        }}
      >
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>创建调用 Key</DialogTitle>
            <DialogDescription>
              设置调用额度、请求格式，并绑定允许使用的上游分组。创建后只显示一次完整 Key。
            </DialogDescription>
          </DialogHeader>
          <KeyDraftFields draft={createDraft} groups={groups} onChange={setCreateDraft} />
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)} disabled={!!busy}>
              取消
            </Button>
            <Button onClick={() => void createGatewayKey()} disabled={!!busy}>
              {busy === "create-key" ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : <KeyRound className="mr-1.5 size-3.5" />}
              创建并复制 Key
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={editOpen}
        onOpenChange={(open) => {
          setEditOpen(open)
          if (!open) setEditingKey(null)
        }}
      >
        <DialogContent className="sm:max-w-3xl">
          <DialogHeader>
            <DialogTitle>编辑调用 Key</DialogTitle>
            <DialogDescription>
              修改 Key 名称、额度、请求格式和可使用的上游分组，原始密钥不会重新生成。
            </DialogDescription>
          </DialogHeader>
          <KeyDraftFields
            draft={editDraft}
            groups={groups}
            onChange={setEditDraft}
            showEnabled
            showKeepExpiry
          />
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditOpen(false)} disabled={!!busy}>
              取消
            </Button>
            <Button onClick={() => void updateGatewayKey()} disabled={!!busy}>
              {busy === `edit-${editingKey?.id}` ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </Card>
  )
}
