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
import { channelTypeLabel, formatRatio, formatTokens, money, relativeTime } from "@/lib/format"
import { testGatewayHealthStream, type ProgressEvent } from "@/lib/sync-stream"
import { cn } from "@/lib/utils"
import type {
  GatewayBootstrapResult,
  GatewayHealthResult,
  GatewayKey,
  GatewayKeyReveal,
  IPPolicy,
  UpstreamGroupKey,
} from "@/lib/api-types"

const TOKEN_M = 1_000_000

type ClientFormat = "openai" | "claude" | "grok" | "any"
type ColumnClientFormat = "openai" | "claude" | "grok"
type GroupScope = "all" | "selected" | "charity" | "normal"
type UpstreamRequestMode = "responses" | "chat"
type GroupFormatFilter = "all" | ColumnClientFormat
type RateFilter = "all" | "0-0.05" | "0.06-0.1" | "0.1-0.2" | "0.2+"
type CharityFilter = "all" | "charity" | "normal"
type GroupStatusFilter = "all" | "alive" | "dead" | "zero_balance" | "rate_limited" | "forbidden"
type MaxGroupRatioLimit = "0" | "0.05" | "0.1"

interface GroupFilters {
  search: string
  format: GroupFormatFilter
  rateBand: RateFilter
  charity: CharityFilter
  status: GroupStatusFilter
}

function createDefaultGroupFilters(): GroupFilters {
  return {
    search: "",
    format: "all",
    rateBand: "all",
    charity: "all",
    status: "all",
  }
}

interface KeyDraft {
  name: string
  enabled: boolean
  clientFormat: ClientFormat
  scope: GroupScope
  selectedGroupIds: number[]
  dailyLimitM: string
  totalLimitM: string
  balanceLimit: string
  concurrencyLimit: string
  maxGroupRatio: MaxGroupRatioLimit
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
    balanceLimit: "",
    concurrencyLimit: "",
    maxGroupRatio: "0",
    expiresInDays: "0",
  }
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

interface IPPolicyDraft {
  ip: string
  blocked: boolean
  publicConcurrencyExempt: boolean
  note: string
}

function createDefaultIPPolicyDraft(): IPPolicyDraft {
  return {
    ip: "",
    blocked: false,
    publicConcurrencyExempt: true,
    note: "",
  }
}

function statusTone(status: string) {
  switch (status) {
    case "alive":
      return "bg-success/10 text-success border-success/20"
    case "dead":
      return "bg-danger/10 text-danger border-danger/20"
    case "zero_balance":
      return "bg-warning/10 text-warning border-warning/20"
    case "rate_limited":
    case "non_generation":
    case "timeout":
    case "server_error":
      return "bg-warning/10 text-warning border-warning/20"
    case "forbidden":
    case "auth_failed":
    case "network_error":
    case "upstream_error":
    case "model_error":
    case "invalid_request":
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
  if (group.enabled === false) return "disabled"
  // Existing databases may still contain the historical auth_failed value.
  // Present it in the single access-refused bucket rather than showing an
  // obsolete sixth status in the available-channels page.
  return group.status === "auth_failed" ? "forbidden" : group.status
}

function statusText(status: string) {
  switch (status) {
    case "alive":
      return "存活"
    case "dead":
      return "死亡"
    case "zero_balance":
      return "零余额"
    case "rate_limited":
      return "限流"
    case "forbidden":
      return "403 拒绝"
    case "non_generation":
      return "非生成"
    case "auth_failed":
      return "认证失败"
    case "timeout":
      return "超时"
    case "network_error":
      return "网络错误"
    case "upstream_error":
      return "上游错误"
    case "model_error":
      return "模型错误"
    case "invalid_request":
      return "请求错误"
    case "server_error":
      return "上游 5xx"
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

function normalizeGroupScope(value?: string | null, ids: number[] = []): GroupScope {
  switch ((value ?? "").toLowerCase()) {
    case "selected":
      return "selected"
    case "charity":
      return "charity"
    case "normal":
    case "non_charity":
    case "non-charity":
      return "normal"
    case "all":
      return "all"
    default:
      return ids.length > 0 ? "selected" : "all"
  }
}

function groupScopeLabel(value: GroupScope) {
  switch (value) {
    case "selected":
      return "指定分组"
    case "charity":
      return "仅公益分组"
    case "normal":
      return "仅非公益分组"
    default:
      return "全部分组"
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

function groupMatchesFormatFilter(group: UpstreamGroupKey, format: GroupFormatFilter) {
  return format === "all" || groupClientFormat(group) === format
}

function isOpenAIHealthGroup(group: UpstreamGroupKey) {
  return groupClientFormat(group) === "openai"
}

function isOpenAIResponsesGroup(group: UpstreamGroupKey) {
  return isOpenAIHealthGroup(group) && normalizeRequestMode(group.request_mode) === "responses"
}

function rateFilterLabel(value: RateFilter) {
  switch (value) {
    case "0-0.05":
      return "0-0.05"
    case "0.06-0.1":
      return "0.06-0.1"
    case "0.1-0.2":
      return "0.1-0.2"
    case "0.2+":
      return "0.2 以上"
    default:
      return "全部倍率"
  }
}

function charityFilterLabel(value: CharityFilter) {
  switch (value) {
    case "charity":
      return "仅公益 Key"
    case "normal":
      return "非公益"
    default:
      return "全部状态"
  }
}

function groupMatchesRateBand(group: UpstreamGroupKey, band: RateFilter) {
  if (band === "all") return true
  const ratio = Number(group.ratio ?? 0)
  if (!Number.isFinite(ratio)) return false
  switch (band) {
    case "0-0.05":
      return ratio >= 0 && ratio <= 0.05
    case "0.06-0.1":
      return ratio > 0.05 && ratio <= 0.1
    case "0.1-0.2":
      return ratio > 0.1 && ratio <= 0.2
    case "0.2+":
      return ratio > 0.2
    default:
      return true
  }
}

function normalizeMaxGroupRatio(value?: number | string | null): MaxGroupRatioLimit {
  const ratio = Number(value ?? 0)
  if (!Number.isFinite(ratio) || ratio <= 0) return "0"
  if (ratio <= 0.05) return "0.05"
  if (ratio <= 0.1) return "0.1"
  return "0"
}

function maxGroupRatioLabel(value: MaxGroupRatioLimit | number | string | undefined | null) {
  const normalized = normalizeMaxGroupRatio(value)
  if (normalized === "0.05") return "0.05 倍率以下"
  if (normalized === "0.1") return "0.1 倍率以下"
  return "不限制倍率"
}

function groupWithinMaxRatio(group: UpstreamGroupKey, limit: MaxGroupRatioLimit) {
  const max = Number(limit)
  if (!Number.isFinite(max) || max <= 0) return true
  const ratio = Number(group.ratio ?? 0)
  return Number.isFinite(ratio) && ratio <= max + 1e-9
}

function groupMatchesCharity(group: UpstreamGroupKey, charity: CharityFilter) {
  if (charity === "all") return true
  return charity === "charity" ? group.charity === true : group.charity !== true
}

function groupMatchesStatus(group: UpstreamGroupKey, status: GroupStatusFilter) {
  return status === "all" || effectiveStatus(group) === status
}

function upstreamKeyLabel(group: UpstreamGroupKey) {
  const id = Number(group.upstream_key_id ?? 0)
  return id > 0 ? `上游 Key #${id}` : "手动/本地 Key"
}

function channelSourceLabel(group: UpstreamGroupKey) {
  const raw = String(group.channel_url ?? "").trim()
  if (!raw) return group.channel_name || `渠道 #${group.channel_id}`
  try {
    return new URL(raw).host || raw
  } catch {
    return raw.replace(/^https?:\/\//i, "").replace(/\/$/, "")
  }
}

function normalizeSearchText(value: unknown) {
  return String(value ?? "").trim().toLowerCase()
}

function groupSearchText(group: UpstreamGroupKey) {
  const format = normalizeClientFormat(group.client_format)
  const status = effectiveStatus(group)
  return [
    group.channel_name,
	group.channel_url,
    group.channel_id,
    group.channel_type,
    channelTypeLabel(group.channel_type),
    group.group_name,
    group.group_description,
    group.group_ref,
    group.ratio,
    formatRatio(group.ratio),
    clientFormatLabel(format),
    format,
    requestModeLabel(group.request_mode),
    normalizeRequestMode(group.request_mode),
    status,
    statusText(status),
    upstreamKeyLabel(group),
    group.charity ? "公益 charity public" : "非公益 normal",
  ]
    .filter((value) => value != null && String(value).trim() !== "")
    .join(" ")
    .toLowerCase()
}

function groupMatchesFilters(group: UpstreamGroupKey, filters: GroupFilters) {
  const query = normalizeSearchText(filters.search)
  const checks: boolean[] = []
  if (query) checks.push(groupSearchText(group).includes(query))
  if (filters.format !== "all") checks.push(groupMatchesFormatFilter(group, filters.format))
  if (filters.rateBand !== "all") checks.push(groupMatchesRateBand(group, filters.rateBand))
  if (filters.charity !== "all") checks.push(groupMatchesCharity(group, filters.charity))
  if (filters.status !== "all") checks.push(groupMatchesStatus(group, filters.status))
  // The unified channel filters are intentionally OR conditions.  For example,
  // selecting Claude and 0.05 finds both matching sets instead of silently
  // hiding a channel that only meets one of the selected search conditions.
  return checks.length === 0 || checks.some(Boolean)
}

function activeGroupFilterCount(filters: GroupFilters) {
  return (
    (filters.search.trim() ? 1 : 0) +
    (filters.format !== "all" ? 1 : 0) +
    (filters.rateBand !== "all" ? 1 : 0) +
    (filters.charity !== "all" ? 1 : 0) +
    (filters.status !== "all" ? 1 : 0)
  )
}

function groupStatusRank(status: string) {
  return status === "alive"
    ? 0
    : status === "unknown"
      ? 1
      : status === "rate_limited"
        ? 2
        : ["dead", "server_error", "timeout", "network_error", "upstream_error"].includes(status)
          ? 3
          : ["zero_balance", "forbidden", "auth_failed", "model_error", "invalid_request", "non_generation"].includes(status)
            ? 4
            : 5
}

function isFailureHealthStatus(status: string) {
  return !["alive", "unknown", "checking", "queued", "disabled"].includes(status)
}

function healthResultSummaryText(result: GatewayHealthResult) {
  const parts = [
    `存活 ${result.alive}`,
    `死亡 ${result.dead}`,
    `零余额 ${result.zero_balance || 0}`,
    `限流 ${result.rate_limited || 0}`,
    `403 ${result.forbidden || 0}`,
    `非生成 ${result.non_generation || 0}`,
  ]
  const other =
    (result.auth_failed || 0) +
    (result.timeout || 0) +
    (result.network_error || 0) +
    (result.upstream_error || 0) +
    (result.model_error || 0) +
    (result.invalid_request || 0) +
    (result.server_error || 0)
  if (other > 0) parts.push(`其它异常 ${other}`)
  return parts.join("，")
}

function sortGroupsForDisplay(groups: UpstreamGroupKey[]) {
  return groups.slice().sort((a, b) => {
    return (
      a.ratio - b.ratio ||
      groupStatusRank(effectiveStatus(a)) - groupStatusRank(effectiveStatus(b)) ||
      (b.priority || 0) - (a.priority || 0) ||
      a.failure_count - b.failure_count ||
      a.id - b.id
    )
  })
}

function cleanGroupIDs(ids: number[], groups: UpstreamGroupKey[], format: ClientFormat, maxRatio: MaxGroupRatioLimit = "0") {
  const allowed = new Set(
    groups
      .filter((group) => groupMatchesFormat(group, format) && groupWithinMaxRatio(group, maxRatio))
      .map((group) => group.id),
  )
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

function sanitizeIntInput(value: string) {
  return value.replace(/[^\d]/g, "")
}

function formatMoneyLimit(value?: number | null, precise = false) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "不限"
  return money(n, { precise })
}

function keyBalanceExhausted(key: GatewayKey) {
  return key.balance_limit > 0 && key.total_cost >= key.balance_limit
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
  const scope = normalizeGroupScope(key.allowed_group_scope, ids)
  return {
    name: key.name || "default",
    enabled: key.enabled !== false,
    clientFormat: normalizeClientFormat(key.client_format),
    scope,
    selectedGroupIds: ids,
    dailyLimitM: tokensToMInput(key.daily_limit),
    totalLimitM: tokensToMInput(key.total_limit),
    balanceLimit: key.balance_limit > 0 ? String(key.balance_limit) : "",
    concurrencyLimit: key.concurrency_limit > 0 ? String(key.concurrency_limit) : "",
    maxGroupRatio: normalizeMaxGroupRatio(key.max_group_ratio),
    expiresInDays: "keep",
  }
}

function buildGatewayKeyPayload(draft: KeyDraft, includeEnabled: boolean, includeExpiry: boolean) {
  const payload: Record<string, unknown> = {
    name: draft.name.trim() || "default",
    client_format: draft.clientFormat,
    allowed_group_scope: draft.scope,
    allowed_group_ids: draft.scope === "selected" ? draft.selectedGroupIds : [],
    daily_limit: mInputToTokens(draft.dailyLimitM),
    total_limit: mInputToTokens(draft.totalLimitM),
    balance_limit: Math.max(0, Number(draft.balanceLimit) || 0),
    concurrency_limit: Math.max(0, Math.floor(Number(draft.concurrencyLimit) || 0)),
    max_group_ratio: Number(draft.maxGroupRatio) || 0,
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
  const scope = normalizeGroupScope(key.allowed_group_scope, ids)
  if (scope === "all") return "全部分组，按优先级和低倍率顺序调度"
  if (scope === "charity") {
    const count = groups.filter((group) => group.charity === true && groupMatchesFormat(group, normalizeClientFormat(key.client_format))).length
    return `仅公益分组${count ? `（${count} 个可匹配）` : ""}`
  }
  if (scope === "normal") {
    const count = groups.filter((group) => group.charity !== true && groupMatchesFormat(group, normalizeClientFormat(key.client_format))).length
    return `仅非公益分组${count ? `（${count} 个可匹配）` : ""}`
  }
  const names = ids
    .slice(0, 3)
    .map((id) => groups.find((group) => group.id === id))
    .filter(Boolean)
    .map((group) => `${group?.channel_name || `#${group?.channel_id}`} / ${group?.group_name}`)
  return `指定 ${ids.length} 个${names.length ? `：${names.join("、")}` : ""}`
}

function formatConcurrencyLimit(value?: number | null) {
  const n = Math.floor(Number(value ?? 0))
  return Number.isFinite(n) && n > 0 ? `${n} 路` : "不限"
}

function keyUsageStatusText(key: GatewayKey) {
  if (keyBalanceExhausted(key)) return "余额已用尽"
  if (!key.enabled) return "已停用"
  if (key.daily_limit > 0 && key.today_tokens >= key.daily_limit) return "今日额度已满"
  if (key.total_limit > 0 && key.total_tokens >= key.total_limit) return "总额度已满"
  return "启用中"
}

function isOpenAIGatewayKey(key: GatewayKey) {
  const format = normalizeClientFormat(key.client_format)
  return format === "openai"
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
  const eligibleGroups = useMemo(
    () => groups.filter((group) => groupMatchesFormat(group, draft.clientFormat) && groupWithinMaxRatio(group, draft.maxGroupRatio)),
    [draft.clientFormat, draft.maxGroupRatio, groups],
  )
  const scopedEligibleGroups = useMemo(() => {
    switch (draft.scope) {
      case "charity":
        return eligibleGroups.filter((group) => group.charity === true)
      case "normal":
        return eligibleGroups.filter((group) => group.charity !== true)
      default:
        return eligibleGroups
    }
  }, [draft.scope, eligibleGroups])
  const [bindingSearch, setBindingSearch] = useState("")
  const [bindingChannel, setBindingChannel] = useState("all")
  const [bindingRateBand, setBindingRateBand] = useState<RateFilter>("all")
  const [bindingKeySearch, setBindingKeySearch] = useState("")
  const selected = new Set(draft.selectedGroupIds)
  const channelOptions = useMemo(() => {
    const map = new Map<number, string>()
    for (const group of scopedEligibleGroups) {
      map.set(group.channel_id, group.channel_name || `#${group.channel_id}`)
    }
    return [...map.entries()]
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.name.localeCompare(b.name, "zh-CN"))
  }, [scopedEligibleGroups])
  const visibleGroups = useMemo(() => {
    const query = normalizeSearchText(bindingSearch)
    const keyQuery = normalizeSearchText(bindingKeySearch)
    return scopedEligibleGroups.filter((group) => {
      const channelOK = bindingChannel === "all" || String(group.channel_id) === bindingChannel
      const rateOK = groupMatchesRateBand(group, bindingRateBand)
      const queryOK = !query || groupSearchText(group).includes(query)
      const keyOK = !keyQuery || upstreamKeyLabel(group).toLowerCase().includes(keyQuery)
      return channelOK && rateOK && queryOK && keyOK
    })
  }, [bindingChannel, bindingKeySearch, bindingRateBand, bindingSearch, scopedEligibleGroups])
  const bindingSummary =
    draft.scope === "all"
      ? `将按优先级和低倍率顺序使用 ${eligibleGroups.length} 个匹配分组，当前筛选显示 ${visibleGroups.length} 个`
      : draft.scope === "selected"
        ? `已选择 ${draft.selectedGroupIds.length}/${eligibleGroups.length} 个匹配分组，当前筛选显示 ${visibleGroups.length} 个`
        : `将使用 ${scopedEligibleGroups.length} 个${draft.scope === "charity" ? "公益" : "非公益"}匹配分组，当前筛选显示 ${visibleGroups.length} 个`

  function updateFormat(format: ClientFormat) {
    onChange({
      ...draft,
      clientFormat: format,
      selectedGroupIds: cleanGroupIDs(draft.selectedGroupIds, groups, format, draft.maxGroupRatio),
    })
  }

  function toggleGroup(id: number, checked: boolean) {
    const next = checked
      ? [...draft.selectedGroupIds, id]
      : draft.selectedGroupIds.filter((item) => item !== id)
    onChange({ ...draft, selectedGroupIds: cleanGroupIDs(next, groups, draft.clientFormat, draft.maxGroupRatio) })
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

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-daily">每日额度（M Token）</Label>
          <Input
            id="gateway-key-daily"
            value={draft.dailyLimitM}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, dailyLimitM: sanitizeMInput(event.target.value) })}
            placeholder="留空不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-total">总额度（M Token）</Label>
          <Input
            id="gateway-key-total"
            value={draft.totalLimitM}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, totalLimitM: sanitizeMInput(event.target.value) })}
            placeholder="留空不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-balance">可消耗余额（$）</Label>
          <Input
            id="gateway-key-balance"
            value={draft.balanceLimit}
            inputMode="decimal"
            onChange={(event) => onChange({ ...draft, balanceLimit: sanitizeMInput(event.target.value) })}
            placeholder="留空不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label htmlFor="gateway-key-concurrency">最大并发数</Label>
          <Input
            id="gateway-key-concurrency"
            value={draft.concurrencyLimit}
            inputMode="numeric"
            onChange={(event) => onChange({ ...draft, concurrencyLimit: sanitizeIntInput(event.target.value) })}
            placeholder="留空或 0 不限"
          />
        </div>
        <div className="space-y-1.5">
          <Label>渠道倍率限制</Label>
          <Select
            value={draft.maxGroupRatio}
            onValueChange={(value) => {
              const maxGroupRatio = normalizeMaxGroupRatio(value)
              onChange({
                ...draft,
                maxGroupRatio,
                selectedGroupIds: cleanGroupIDs(draft.selectedGroupIds, groups, draft.clientFormat, maxGroupRatio),
              })
            }}
          >
            <SelectTrigger>
              <SelectValue placeholder="选择倍率限制" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="0">不限制倍率</SelectItem>
              <SelectItem value="0.05">0.05 倍率以下</SelectItem>
              <SelectItem value="0.1">0.1 倍率以下</SelectItem>
            </SelectContent>
          </Select>
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
        <p className="text-xs leading-5 text-muted-foreground sm:col-span-2 lg:col-span-2 lg:self-end">
          余额按命中的上游分组价格和倍率折算；超过最大并发的请求会排队等待，不会直接失败。
        </p>
      </div>

      <div className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <Label>绑定上游分组</Label>
          <Select value={draft.scope} onValueChange={(value) => onChange({ ...draft, scope: value as GroupScope })}>
            <SelectTrigger className="h-8 w-40 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">全部分组</SelectItem>
              <SelectItem value="charity">仅公益分组</SelectItem>
              <SelectItem value="normal">仅非公益分组</SelectItem>
              <SelectItem value="selected">指定分组</SelectItem>
            </SelectContent>
          </Select>
        </div>
        <div className="rounded-md border border-border bg-background">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-3 py-2 text-xs">
            <span className="text-muted-foreground">{bindingSummary}</span>
            {draft.scope === "selected" ? (
              <div className="flex items-center gap-1">
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-xs"
                  onClick={() =>
                    onChange({
                      ...draft,
                      selectedGroupIds: cleanGroupIDs(
                        [...draft.selectedGroupIds, ...visibleGroups.map((group) => group.id)],
                        groups,
                        draft.clientFormat,
                        draft.maxGroupRatio,
                      ),
                    })
                  }
                >
                  全选当前
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-xs"
                  onClick={() =>
                    onChange({
                      ...draft,
                      selectedGroupIds: cleanGroupIDs(
                        eligibleGroups.map((group) => group.id),
                        groups,
                        draft.clientFormat,
                        draft.maxGroupRatio,
                      ),
                    })
                  }
                >
                  全选全部
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
          <div className="grid gap-2 border-b border-border p-2 md:grid-cols-[minmax(180px,1.2fr)_0.9fr_0.75fr_0.85fr]">
            <div className="relative">
              <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={bindingSearch}
                onChange={(event) => setBindingSearch(event.target.value)}
                className="h-8 pl-8 text-xs"
                placeholder="搜索渠道、分组、倍率"
              />
            </div>
            <Select value={bindingChannel} onValueChange={setBindingChannel}>
              <SelectTrigger className="h-8 w-full text-xs">
                <SelectValue placeholder="选择渠道" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部渠道</SelectItem>
                {channelOptions.map((channel) => (
                  <SelectItem key={channel.id} value={String(channel.id)}>
                    {channel.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select value={bindingRateBand} onValueChange={(value) => setBindingRateBand(value as RateFilter)}>
              <SelectTrigger className="h-8 w-full text-xs">
                <SelectValue placeholder="倍率" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部倍率</SelectItem>
                <SelectItem value="0-0.05">0-0.05</SelectItem>
                <SelectItem value="0.06-0.1">0.06-0.1</SelectItem>
                <SelectItem value="0.1-0.2">0.1-0.2</SelectItem>
                <SelectItem value="0.2+">0.2 以上</SelectItem>
              </SelectContent>
            </Select>
            <Input
              value={bindingKeySearch}
              onChange={(event) => setBindingKeySearch(event.target.value)}
              className="h-8 text-xs"
              placeholder="搜索对应 Key"
            />
          </div>
          <div className="max-h-64 overflow-y-auto p-2">
            {eligibleGroups.length === 0 ? (
              <div className="px-2 py-6 text-center text-xs text-muted-foreground">
                没有匹配 {clientFormatLabel(draft.clientFormat)} 的分组，先同步或切换格式
              </div>
            ) : scopedEligibleGroups.length === 0 ? (
              <div className="px-2 py-6 text-center text-xs text-muted-foreground">
                当前没有匹配 {groupScopeLabel(draft.scope)} 的 {clientFormatLabel(draft.clientFormat)} 分组
              </div>
            ) : visibleGroups.length === 0 ? (
              <div className="px-2 py-6 text-center text-xs text-muted-foreground">
                没有符合当前渠道、倍率或对应 Key 筛选的 {groupScopeLabel(draft.scope)}
              </div>
            ) : (
              <div className="space-y-1">
                {visibleGroups.map((group) => {
                  const status = effectiveStatus(group)
                  return (
                    <label
                      key={group.id}
                      className={cn(
                        "flex cursor-pointer items-start gap-2 rounded-md px-2 py-2 text-xs hover:bg-muted/60",
                        draft.scope !== "selected" && "cursor-default opacity-80",
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
                          {group.charity ? (
                            <Badge variant="outline" className="h-5 gap-1 border-success/20 bg-success/10 px-1.5 text-[10px] text-success">
                              <HeartHandshake className="size-3" />
                              公益
                            </Badge>
                          ) : null}
                          <Badge variant="outline" className={cn("h-5 px-1.5 text-[10px]", statusTone(status))}>
                            {statusText(status)}
                          </Badge>
                        </span>
                        <span className="mt-1 block text-muted-foreground">
                          渠道 {group.channel_name || `#${group.channel_id}`} · 倍率 {formatRatio(group.ratio)} · {upstreamKeyLabel(group)}
                        </span>
                        <span className="mt-0.5 block text-muted-foreground">
                          优先级 {group.priority || 0} · {channelTypeLabel(group.channel_type)}
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
  const [ipPolicies, setIPPolicies] = useState<IPPolicy[]>([])
  const [ipPolicyDraft, setIPPolicyDraft] = useState<IPPolicyDraft>(() => createDefaultIPPolicyDraft())
  const [healthResults, setHealthResults] = useState<Record<number, GatewayHealthResult["items"][number]>>({})
  const [healthProgress, setHealthProgress] = useState<HealthProgress | null>(null)
  const [groupFilterDraft, setGroupFilterDraft] = useState<GroupFilters>(() => createDefaultGroupFilters())
  const [groupFilters, setGroupFilters] = useState<GroupFilters>(() => createDefaultGroupFilters())
  const [groupPage, setGroupPage] = useState(1)
  const [groupPageSize, setGroupPageSize] = useState(10)
  const [keySearch, setKeySearch] = useState("")

  const displayKeys = useMemo(
    () => keys.filter(isOpenAIGatewayKey),
    [keys],
  )
  const displayGroups = useMemo(
    () => groups,
    [groups],
  )
  const filteredKeys = useMemo(() => {
    const query = keySearch.trim().toLowerCase()
    if (!query) return displayKeys
    return displayKeys.filter((key) =>
      [key.name, key.key_prefix, key.client_format]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(query)),
    )
  }, [displayKeys, keySearch])

  const totalGroups = displayGroups.length
  const filteredGroups = useMemo(
    () => sortGroupsForDisplay(displayGroups.filter((group) => groupMatchesFilters(group, groupFilters))),
    [displayGroups, groupFilters],
  )
  const groupPages = Math.max(1, Math.ceil(filteredGroups.length / groupPageSize))
  const safeGroupPage = Math.min(groupPage, groupPages)
  const pagedGroups = useMemo(
    () => filteredGroups.slice((safeGroupPage - 1) * groupPageSize, safeGroupPage * groupPageSize),
    [filteredGroups, groupPageSize, safeGroupPage],
  )
  const displayAliveCount = useMemo(
    () => filteredGroups.filter((group) => effectiveStatus(group) === "alive").length,
    [filteredGroups],
  )
  const displayDeadCount = useMemo(
    () => filteredGroups.filter((group) => effectiveStatus(group) === "dead").length,
    [filteredGroups],
  )
  const displayZeroBalanceCount = useMemo(
    () => filteredGroups.filter((group) => effectiveStatus(group) === "zero_balance").length,
    [filteredGroups],
  )
  const displayRateLimitedCount = useMemo(
    () => filteredGroups.filter((group) => effectiveStatus(group) === "rate_limited").length,
    [filteredGroups],
  )
  const displayForbiddenCount = useMemo(
    () => filteredGroups.filter((group) => effectiveStatus(group) === "forbidden").length,
    [filteredGroups],
  )
  const displayEnabledCount = useMemo(
    () => filteredGroups.filter((group) => group.enabled !== false).length,
    [filteredGroups],
  )
  const activeFilters = activeGroupFilterCount(groupFilters)
  const sortedIPPolicies = useMemo(
    () => [...ipPolicies].sort((a, b) => a.ip.localeCompare(b.ip)),
    [ipPolicies],
  )
  const openAIHealthGroups = useMemo(
    () => groups.filter(isOpenAIHealthGroup),
    [groups],
  )
  const enabledOpenAIHealthGroups = useMemo(
    () => openAIHealthGroups.filter((group) => group.enabled !== false),
    [openAIHealthGroups],
  )

  async function load() {
    setLoading(true)
    try {
      const [keyList, groupResult, policyResult] = await Promise.all([
        apiFetch<GatewayKey[]>("/gateway/keys"),
        apiFetch<UpstreamGroupKey[]>("/gateway/group-keys"),
        apiFetch<IPPolicy[]>("/gateway/ip-policies"),
      ])
      setKeys(Array.isArray(keyList) ? keyList : [])
      setIPPolicies(Array.isArray(policyResult) ? policyResult : [])
      const nextGroups = sortGroupsForDisplay(Array.isArray(groupResult) ? groupResult : [])
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
  }, [])

  useEffect(() => {
    setGroupPage(1)
  }, [groupFilters, groupPageSize])

  function validateDraft(draft: KeyDraft) {
    if (draft.scope === "selected" && draft.selectedGroupIds.length === 0) {
      toast.error("指定分组模式下至少选择一个上游分组")
      return false
    }
    if (mInputToTokens(draft.dailyLimitM) < 0 || mInputToTokens(draft.totalLimitM) < 0) {
      toast.error("额度必须是大于等于 0 的数字")
      return false
    }
    const concurrencyLimit = Number(draft.concurrencyLimit) || 0
    if (!Number.isFinite(concurrencyLimit) || concurrencyLimit < 0) {
      toast.error("最大并发数必须是大于等于 0 的整数")
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

  function upsertIPPolicyState(policy: IPPolicy) {
    setIPPolicies((prev) => {
      const exists = prev.some((item) => item.ip === policy.ip)
      return exists ? prev.map((item) => (item.ip === policy.ip ? policy : item)) : [policy, ...prev]
    })
  }

  async function saveIPPolicyDraft() {
    const ip = ipPolicyDraft.ip.trim()
    if (!ip) {
      toast.error("请填写 IP")
      return
    }
    setBusy("ip-policy-create")
    try {
      const saved = await apiFetch<IPPolicy>("/gateway/ip-policies", {
        method: "PUT",
        body: JSON.stringify({
          ip,
          blocked: ipPolicyDraft.blocked,
          public_concurrency_exempt: ipPolicyDraft.publicConcurrencyExempt,
          note: ipPolicyDraft.note.trim(),
        }),
      })
      upsertIPPolicyState(saved)
      setIPPolicyDraft(createDefaultIPPolicyDraft())
      toast.success(saved.blocked ? "IP 已加入黑名单" : "IP 规则已保存")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "保存 IP 规则失败")
    } finally {
      setBusy(null)
    }
  }

  async function updateIPPolicy(policy: IPPolicy, patch: Partial<Pick<IPPolicy, "blocked" | "public_concurrency_exempt" | "note">>) {
    setBusy(`ip-policy-${policy.id || policy.ip}`)
    try {
      const saved = await apiFetch<IPPolicy>("/gateway/ip-policies", {
        method: "PUT",
        body: JSON.stringify({
          ip: policy.ip,
          blocked: patch.blocked ?? policy.blocked,
          public_concurrency_exempt: patch.public_concurrency_exempt ?? policy.public_concurrency_exempt,
          note: patch.note ?? policy.note ?? "",
        }),
      })
      upsertIPPolicyState(saved)
      toast.success("IP 规则已更新")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "更新 IP 规则失败")
    } finally {
      setBusy(null)
    }
  }

  async function deleteIPPolicy(policy: IPPolicy) {
    if (!window.confirm(`删除 ${policy.ip} 的 IP 规则？`)) return
    setBusy(`ip-policy-delete-${policy.id || policy.ip}`)
    try {
      await apiFetch(`/gateway/ip-policies/${encodeURIComponent(policy.ip)}`, { method: "DELETE" })
      setIPPolicies((prev) => prev.filter((item) => item.ip !== policy.ip))
      toast.success("IP 规则已删除")
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "删除 IP 规则失败")
    } finally {
      setBusy(null)
    }
  }

  async function bootstrapGroups() {
    setBusy("bootstrap")
    try {
      const res = await apiFetch<GatewayBootstrapResult>("/gateway/group-keys/bootstrap", { method: "POST" })
      toast.success(`分组 Key 已覆盖同步：保留/更新 ${res.updated}，新建 ${res.created}，删除 ${res.removed || 0}，跳过 ${res.skipped}，失败 ${res.failed}`)
      await load()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "一键创建分组 Key 失败")
    } finally {
      setBusy(null)
    }
  }

  async function testGroups() {
    const enabledTargets = enabledOpenAIHealthGroups
    if (enabledTargets.length === 0) {
      toast.error("没有可测活的 OpenAI 分组")
      return
    }
    setBusy("test")
    setHealthResults((prev) => {
      const next = { ...prev }
      for (const group of enabledTargets) {
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
      return next
    })
    setHealthProgress({
      running: true,
      completed: 0,
      total: enabledTargets.length,
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
              total: enabledTargets.length,
              batch: 0,
              batches: 0,
              batchSize: 30,
              message: "测活中...",
          }))
          if (ev.stage === "done" && ev.data && typeof ev.data === "object" && "items" in ev.data) {
            finalResult = ev.data as GatewayHealthResult
          }
        },
      }, enabledTargets.map((group) => group.id))
      const completedResult = finalResult as GatewayHealthResult | undefined
      if (completedResult) {
        const nextItems = Object.fromEntries((completedResult.items ?? []).map((item) => [item.id, item]))
        setHealthResults((prev) => ({ ...prev, ...nextItems }))
        toast.success(`测活完成：${healthResultSummaryText(completedResult)}`)
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
      toast.success(`${group.channel_name || "上游"} / ${group.group_name}：${statusText(result.status)}`)
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

  function renderGroupRow(group: UpstreamGroupKey) {
    const latestHealth = healthResults[group.id]
    const status = latestHealth?.status ?? effectiveStatus(group)
    const Icon = status === "alive" ? CheckCircle2 : isFailureHealthStatus(status) ? XCircle : RefreshCw
    const latencyMS = latestHealth?.latency_ms ?? group.last_latency_ms ?? 0
    const format = groupClientFormat(group)
    const canTest = group.enabled !== false

    return (
      <div
        key={group.id}
        className={cn(
          "grid gap-2 border-t border-border p-3 text-xs lg:grid-cols-[minmax(260px,1.6fr)_110px_132px_96px_145px_110px] lg:items-center",
          group.charity && "bg-success/5",
        )}
      >
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-1.5">
            <p className="truncate text-sm font-semibold text-foreground">{group.group_name}</p>
            <Badge variant="outline" className="bg-background">{clientFormatLabel(format)}</Badge>
			<Badge variant="outline" className="max-w-44 truncate bg-background text-muted-foreground" title={group.channel_url || group.channel_name}>
			  {channelSourceLabel(group)}
			</Badge>
            {group.charity ? (
              <Badge variant="outline" className="gap-1 border-success/20 bg-success/10 px-1.5 text-[10px] text-success">
                <HeartHandshake className="size-3" />
                公益
              </Badge>
            ) : null}
          </div>
          <p className="mt-0.5 truncate text-[11px] text-muted-foreground">
            {group.group_description || group.group_ref}
          </p>
          <div className="mt-1.5 flex flex-wrap gap-1.5">
            <Badge variant="outline" className="bg-background">倍率 {formatRatio(group.ratio)}</Badge>
            <Badge variant="outline" className="bg-background">{requestModeLabel(group.request_mode)}</Badge>
            <Badge variant="outline" className="bg-background">{upstreamKeyLabel(group)}</Badge>
            <Badge variant="outline" className="bg-background">{formatTokens(group.total_tokens)} tok</Badge>
          </div>
        </div>

        <Badge variant="outline" className={cn("w-fit gap-1.5", statusTone(status))}>
          <Icon className={cn("size-3", status === "checking" && "animate-spin")} />
          {statusText(status)}
        </Badge>

        <div className="grid gap-1.5">
          <Select
            value={format}
            disabled={!!busy}
            onValueChange={(value) => void changeGroupClientFormat(group, value)}
          >
            <SelectTrigger className="h-8 w-full text-xs" aria-label="渠道格式">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="openai">OpenAI</SelectItem>
              <SelectItem value="claude">Claude</SelectItem>
              <SelectItem value="grok">Grok</SelectItem>
            </SelectContent>
          </Select>
          <Select
            value={normalizeRequestMode(group.request_mode)}
            disabled={!!busy}
            onValueChange={(value) => void changeGroupRequestMode(group, normalizeRequestMode(value))}
          >
            <SelectTrigger className="h-8 w-full text-xs" aria-label="上游接口">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="responses">Responses</SelectItem>
              <SelectItem value="chat">Chat</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div className="grid gap-1.5">
          <Input
            value={priorityDrafts[group.id] ?? String(group.priority || 0)}
            inputMode="numeric"
            className="h-8 px-2 text-xs"
            disabled={!!busy}
            title="优先级：数值越大越优先；同优先级再按低倍率调度"
            onChange={(event) =>
              setPriorityDrafts((prev) => ({
                ...prev,
                [group.id]: event.target.value.replace(/[^\d]/g, ""),
              }))
            }
            onKeyDown={(event) => {
              if (event.key === "Enter") event.currentTarget.blur()
            }}
            onBlur={() => {
              const draft = priorityDrafts[group.id] ?? String(group.priority || 0)
              if (Number(draft) !== (group.priority || 0)) void savePriority(group)
            }}
            placeholder="优先级"
          />
          <Input
            value={concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)}
            inputMode="numeric"
            className="h-8 px-2 text-xs"
            disabled={!!busy}
            title="并发上限，0 表示不限"
            onChange={(event) =>
              setConcurrencyDrafts((prev) => ({
                ...prev,
                [group.id]: event.target.value.replace(/[^\d]/g, ""),
              }))
            }
            onKeyDown={(event) => {
              if (event.key === "Enter") event.currentTarget.blur()
            }}
            onBlur={() => {
              const draft = concurrencyDrafts[group.id] ?? String(group.concurrency_limit || 0)
              if (Number(draft) !== (group.concurrency_limit || 0)) void saveConcurrencyLimit(group)
            }}
            placeholder="并发"
          />
        </div>

        <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
          <label className="flex items-center gap-1.5">
            <Switch
              checked={group.enabled !== false}
              disabled={!!busy}
              title={group.enabled === false ? "启用这个上游分组" : "禁用后不会参与调度和测活"}
              onCheckedChange={(checked) => void toggleGroupEnabled(group, checked)}
            />
            启用
          </label>
          <label className="flex items-center gap-1.5">
            <Switch
              checked={group.charity === true}
              disabled={!!busy}
              title={group.charity ? "取消公益标记" : "标记为公益分组"}
              onCheckedChange={(checked) => void toggleGroupCharity(group, checked)}
            />
            公益
          </label>
        </div>

        <div className="flex items-center justify-start gap-1 lg:justify-end">
          {group.disabled_until && new Date(group.disabled_until).getTime() > Date.now() ? (
            <Button
              variant="outline"
              size="sm"
              className="h-7 px-2 text-[11px]"
              disabled={!!busy}
              title="立即解除冷却，恢复调度"
              onClick={() => void clearCooldown(group)}
            >
              解冷
            </Button>
          ) : null}
          <Button
            variant="outline"
            size="sm"
            className="h-7 gap-1 px-2 text-[11px]"
            disabled={!!busy || !canTest}
            title={`单独测活此 ${clientFormatLabel(format)} 分组`}
            onClick={() => void testGroup(group)}
          >
            {busy === `test-${group.id}` ? <Loader2 className="size-3 animate-spin" /> : <RefreshCw className="size-3" />}
            测活
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="size-7 text-muted-foreground hover:text-danger"
            disabled={!!busy}
            title="删除该分组（清理上游已删除的残留，仅删本地）"
            onClick={() => void deleteGroup(group)}
          >
            <Trash2 className="size-3.5" />
          </Button>
        </div>

        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-foreground lg:col-span-6">
          <span>测活：{latestHealth?.checked_at ? relativeTime(latestHealth.checked_at) : group.last_checked_at ? relativeTime(group.last_checked_at) : "未测"}</span>
          <span>延迟：{latencyMS > 0 ? `${latencyMS}ms` : "—"}</span>
          <span>并发：{(group.concurrency_limit || 0) > 0 ? `${group.concurrency_limit} 路` : "不限"}</span>
          <span>优先级：{group.priority || 0}</span>
        </div>
      </div>
    )
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
            <Badge variant="outline" className="border-warning/20 bg-warning/10 text-warning">
              零余额 {displayZeroBalanceCount}
            </Badge>
            <Badge variant="outline" className="border-warning/20 bg-warning/10 text-warning">
              限流 {displayRateLimitedCount}
            </Badge>
            <Badge variant="outline" className="border-danger/20 bg-danger/10 text-danger">
              403 {displayForbiddenCount}
            </Badge>
            <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
              启用 {displayEnabledCount}/{totalGroups}
            </Badge>
            <Badge variant="outline" className="border-border bg-muted/40 text-muted-foreground">
              OpenAI/Responses 可测 {enabledOpenAIHealthGroups.length}/{openAIHealthGroups.length}
            </Badge>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy} onClick={bootstrapGroups}>
              {busy === "bootstrap" ? <Loader2 className="size-3.5 animate-spin" /> : <Plus className="size-3.5" />}
              覆盖同步分组 Key
            </Button>
            <Button size="sm" variant="outline" className="gap-1.5 text-xs" disabled={!!busy || enabledOpenAIHealthGroups.length === 0} onClick={testGroups}>
              {busy === "test" ? <Loader2 className="size-3.5 animate-spin" /> : <RefreshCw className="size-3.5" />}
              一键测活
            </Button>
            {healthProgress ? (
              <div className="w-full rounded-md border border-border bg-muted/30 px-3 py-2 text-[11px] text-muted-foreground sm:w-auto">
                <span className="font-medium text-foreground">
                  {healthProgress.completed}/{healthProgress.total || enabledOpenAIHealthGroups.length}
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
                <span className="text-[11px] text-muted-foreground">OpenAI Bearer Key · {filteredKeys.length}/{displayKeys.length} 个</span>
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
                              {keyUsageStatusText(key)} · {clientFormatLabel(key.client_format)} · 并发 {formatConcurrencyLimit(key.concurrency_limit)} · {maxGroupRatioLabel(key.max_group_ratio)}
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
                          {selectedGroupSummary(key, displayGroups)}
                        </TableCell>
                        <TableCell className="hidden text-right text-xs md:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.today_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.daily_limit > 0 ? formatTokens(key.daily_limit) : "不限"}</span>
                          <span className="mt-0.5 block text-[10px] text-muted-foreground">余额 {money(key.today_cost, { precise: true })}</span>
                        </TableCell>
                        <TableCell className="hidden text-right text-xs xl:table-cell">
                          <span className="font-medium text-foreground">{formatTokens(key.total_tokens)}</span>
                          <span className="text-muted-foreground"> / {key.total_limit > 0 ? formatTokens(key.total_limit) : "不限"}</span>
                          <span className="mt-0.5 block text-[10px] text-muted-foreground">
                            余额 {money(key.total_cost, { precise: true })} / {formatMoneyLimit(key.balance_limit, true)}
                          </span>
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

        {showKeys ? (
          <div className="rounded-md border border-border bg-muted/10 p-3">
            <div className="mb-3 flex flex-wrap items-start justify-between gap-2">
              <div>
                <p className="text-xs font-medium text-foreground">IP 黑名单 / 公网并发白名单</p>
                <p className="mt-1 text-[11px] text-muted-foreground">
                  黑名单会拒绝所有网关请求；白名单只豁免公益 Key 的单 IP 3 路并发限制。
                </p>
              </div>
              <Badge variant="outline" className="border-border bg-background text-muted-foreground">
                {ipPolicies.length} 条规则
              </Badge>
            </div>
            <div className="grid gap-2 lg:grid-cols-[minmax(180px,0.9fr)_minmax(220px,1.2fr)_auto_auto_auto]">
              <Input
                value={ipPolicyDraft.ip}
                onChange={(event) => setIPPolicyDraft((prev) => ({ ...prev, ip: event.target.value }))}
                placeholder="IP，例如 203.0.113.8"
                className="h-9 text-xs"
                aria-label="IP 地址"
              />
              <Input
                value={ipPolicyDraft.note}
                onChange={(event) => setIPPolicyDraft((prev) => ({ ...prev, note: event.target.value }))}
                placeholder="备注，可选"
                className="h-9 text-xs"
                aria-label="IP 规则备注"
              />
              <label className="flex h-9 items-center gap-2 rounded-md border border-border bg-background px-3 text-xs text-muted-foreground">
                <Checkbox
                  checked={ipPolicyDraft.blocked}
                  onCheckedChange={(checked) => setIPPolicyDraft((prev) => ({ ...prev, blocked: checked === true }))}
                />
                封禁
              </label>
              <label className="flex h-9 items-center gap-2 rounded-md border border-border bg-background px-3 text-xs text-muted-foreground">
                <Checkbox
                  checked={ipPolicyDraft.publicConcurrencyExempt}
                  onCheckedChange={(checked) =>
                    setIPPolicyDraft((prev) => ({ ...prev, publicConcurrencyExempt: checked === true }))
                  }
                />
                公网并发白名单
              </label>
              <Button
                size="sm"
                className="h-9 text-xs"
                disabled={!!busy}
                onClick={() => void saveIPPolicyDraft()}
              >
                {busy === "ip-policy-create" ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}
                保存 IP 规则
              </Button>
            </div>
            {sortedIPPolicies.length === 0 ? (
              <div className="mt-3 rounded-md border border-dashed border-border bg-background px-3 py-6 text-center text-xs text-muted-foreground">
                暂无 IP 规则。公益 Key 默认对同一 IP 最多 3 路并发，超过会排队等待。
              </div>
            ) : (
              <div className="mt-3 overflow-x-auto rounded-md border border-border bg-background">
                <Table className="min-w-[720px]">
                  <TableHeader>
                    <TableRow>
                      <TableHead>IP</TableHead>
                      <TableHead>状态</TableHead>
                      <TableHead>备注</TableHead>
                      <TableHead className="hidden md:table-cell">更新时间</TableHead>
                      <TableHead className="w-48 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {sortedIPPolicies.map((policy) => {
                      const rowBusy = busy === `ip-policy-${policy.id || policy.ip}` || busy === `ip-policy-delete-${policy.id || policy.ip}`
                      return (
                        <TableRow key={policy.ip}>
                          <TableCell className="font-mono text-xs text-foreground">{policy.ip}</TableCell>
                          <TableCell>
                            <div className="flex flex-wrap gap-1">
                              {policy.blocked ? (
                                <Badge variant="outline" className="border-danger/20 bg-danger/10 text-danger">黑名单</Badge>
                              ) : (
                                <Badge variant="outline" className="border-success/20 bg-success/10 text-success">允许</Badge>
                              )}
                              {policy.public_concurrency_exempt ? (
                                <Badge variant="outline" className="border-primary/20 bg-primary/10 text-primary">公网并发白名单</Badge>
                              ) : null}
                            </div>
                          </TableCell>
                          <TableCell className="max-w-80 truncate text-xs text-muted-foreground">
                            {policy.note?.trim() || "—"}
                          </TableCell>
                          <TableCell className="hidden text-xs text-muted-foreground md:table-cell">
                            {policy.updated_at ? relativeTime(policy.updated_at) : "—"}
                          </TableCell>
                          <TableCell>
                            <div className="flex justify-end gap-1">
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-[11px]"
                                disabled={!!busy}
                                onClick={() => void updateIPPolicy(policy, { blocked: !policy.blocked })}
                              >
                                {rowBusy ? <Loader2 className="mr-1 size-3 animate-spin" /> : null}
                                {policy.blocked ? "解封" : "封禁"}
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-[11px]"
                                disabled={!!busy}
                                onClick={() => void updateIPPolicy(policy, { public_concurrency_exempt: !policy.public_concurrency_exempt })}
                              >
                                {policy.public_concurrency_exempt ? "取消白名单" : "白名单"}
                              </Button>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                className="size-7 text-destructive hover:text-destructive"
                                disabled={!!busy}
                                title="删除 IP 规则"
                                onClick={() => void deleteIPPolicy(policy)}
                              >
                                <Trash2 className="size-3.5" />
                              </Button>
                            </div>
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </div>
        ) : null}

        {showGroups ? (
          <>
            <div className="rounded-md border border-border bg-muted/10 p-3">
              <div className="mb-3 flex flex-wrap items-start justify-between gap-2">
                <div>
                  <p className="text-xs font-medium text-foreground">统一筛选区</p>
                  <p className="mt-1 text-[11px] text-muted-foreground">
                    搜索、格式、倍率、状态按条件筛选；不填条件时展示全部渠道。
                  </p>
                </div>
                <Badge variant="outline" className="border-border bg-background text-muted-foreground">
                  命中 {filteredGroups.length}/{totalGroups}
                </Badge>
              </div>
              <div className="grid gap-2 lg:grid-cols-[minmax(220px,1.4fr)_0.8fr_0.8fr_0.8fr_0.8fr_auto]">
                <div className="relative">
                  <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={groupFilterDraft.search}
                    onChange={(event) => setGroupFilterDraft((prev) => ({ ...prev, search: event.target.value }))}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        setGroupFilters({ ...groupFilterDraft, search: groupFilterDraft.search.trim() })
                      }
                    }}
                    className="h-9 pl-8 text-xs"
                    placeholder="模糊搜索渠道、分组、格式、状态、倍率"
                    aria-label="搜索可用渠道对应的上游分组"
                  />
                </div>
                <Select
                  value={groupFilterDraft.format}
                  onValueChange={(value) =>
                    setGroupFilterDraft((prev) => ({ ...prev, format: value as GroupFormatFilter }))
                  }
                >
                  <SelectTrigger className="h-9 w-full text-xs">
                    <SelectValue placeholder="格式筛选" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部格式</SelectItem>
                    <SelectItem value="openai">ChatGPT / OpenAI</SelectItem>
                    <SelectItem value="claude">Claude</SelectItem>
                    <SelectItem value="grok">Grok</SelectItem>
                  </SelectContent>
                </Select>
                <Select
                  value={groupFilterDraft.rateBand}
                  onValueChange={(value) =>
                    setGroupFilterDraft((prev) => ({ ...prev, rateBand: value as RateFilter }))
                  }
                >
                  <SelectTrigger className="h-9 w-full text-xs">
                    <SelectValue placeholder="倍率筛选" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部倍率</SelectItem>
                    <SelectItem value="0-0.05">0-0.05</SelectItem>
                    <SelectItem value="0.06-0.1">0.06-0.1</SelectItem>
                    <SelectItem value="0.1-0.2">0.1-0.2</SelectItem>
                    <SelectItem value="0.2+">0.2 以上</SelectItem>
                  </SelectContent>
                </Select>
                <Select
                  value={groupFilterDraft.charity}
                  onValueChange={(value) =>
                    setGroupFilterDraft((prev) => ({ ...prev, charity: value as CharityFilter }))
                  }
                >
                  <SelectTrigger className="h-9 w-full text-xs">
                    <SelectValue placeholder="状态筛选" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部渠道</SelectItem>
                    <SelectItem value="charity">公益渠道</SelectItem>
                    <SelectItem value="normal">非公益渠道</SelectItem>
                  </SelectContent>
                </Select>
                <Select
                  value={groupFilterDraft.status}
                  onValueChange={(value) =>
                    setGroupFilterDraft((prev) => ({ ...prev, status: value as GroupStatusFilter }))
                  }
                >
                  <SelectTrigger className="h-9 w-full text-xs">
                    <SelectValue placeholder="状态筛选" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">全部状态</SelectItem>
                    <SelectItem value="alive">存活</SelectItem>
                    <SelectItem value="dead">死亡</SelectItem>
                    <SelectItem value="zero_balance">零余额</SelectItem>
                    <SelectItem value="rate_limited">限流</SelectItem>
                    <SelectItem value="forbidden">403</SelectItem>
                  </SelectContent>
                </Select>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    className="h-9 flex-1 gap-1.5 text-xs lg:flex-none"
                    onClick={() => setGroupFilters({ ...groupFilterDraft, search: groupFilterDraft.search.trim() })}
                  >
                    <Search className="size-3.5" />
                    搜索
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-9 flex-1 text-xs lg:flex-none"
                    onClick={() => {
                      const next = createDefaultGroupFilters()
                      setGroupFilterDraft(next)
                      setGroupFilters(next)
                    }}
                  >
                    重置
                  </Button>
                </div>
              </div>
              {activeFilters > 0 ? (
                <div className="mt-3 flex flex-wrap gap-1.5 text-[11px]">
                  {groupFilters.search.trim() ? (
                    <Badge variant="outline" className="bg-background">搜索：{groupFilters.search.trim()}</Badge>
                  ) : null}
                  {groupFilters.format !== "all" ? (
                    <Badge variant="outline" className="bg-background">格式：{clientFormatLabel(groupFilters.format)}</Badge>
                  ) : null}
                  {groupFilters.rateBand !== "all" ? (
                    <Badge variant="outline" className="bg-background">倍率：{rateFilterLabel(groupFilters.rateBand)}</Badge>
                  ) : null}
                  {groupFilters.charity !== "all" ? (
                    <Badge variant="outline" className="bg-background">公益：{charityFilterLabel(groupFilters.charity)}</Badge>
                  ) : null}
                  {groupFilters.status !== "all" ? (
                    <Badge variant="outline" className="bg-background">状态：{statusText(groupFilters.status)}</Badge>
                  ) : null}
                </div>
              ) : null}
            </div>

            {loading ? (
              <div className="rounded-md border border-dashed border-border bg-background px-3 py-12 text-center text-xs text-muted-foreground">
                <Loader2 className="mx-auto mb-2 size-4 animate-spin" />
                加载中...
              </div>
            ) : groups.length === 0 ? (
              <div className="rounded-md border border-dashed border-border bg-background px-3 py-12 text-center text-xs text-muted-foreground">
                还没有分组 Key，先点“一键创建分组 Key”
              </div>
            ) : filteredGroups.length === 0 ? (
              <div className="rounded-md border border-dashed border-border bg-background px-3 py-12 text-center text-xs text-muted-foreground">
                没有符合当前筛选条件的渠道
              </div>
            ) : (
              <div className="space-y-2">
                {pagedGroups.map((group) => renderGroupRow(group))}
              </div>
            )}
            {filteredGroups.length > 0 ? (
              <div className="flex flex-col gap-2 border-t border-border px-3 py-3 text-xs sm:flex-row sm:items-center sm:justify-between">
                <span className="text-muted-foreground">
                  共 {filteredGroups.length} 个渠道，第 {safeGroupPage}/{groupPages} 页
                </span>
                <div className="flex items-center gap-2">
                  <Select value={String(groupPageSize)} onValueChange={(value) => setGroupPageSize(Number(value))}>
                    <SelectTrigger className="h-8 w-24 text-xs"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="10">10 / 页</SelectItem>
                      <SelectItem value="50">50 / 页</SelectItem>
                      <SelectItem value="100">100 / 页</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button size="icon-sm" variant="outline" className="size-8" disabled={safeGroupPage <= 1} onClick={() => setGroupPage((page) => Math.max(1, page - 1))} title="上一页">
                    <ChevronLeft className="size-4" />
                  </Button>
                  <Button size="icon-sm" variant="outline" className="size-8" disabled={safeGroupPage >= groupPages} onClick={() => setGroupPage((page) => Math.min(groupPages, page + 1))} title="下一页">
                    <ChevronRight className="size-4" />
                  </Button>
                </div>
              </div>
            ) : null}
          </>
        ) : null}
      </CardContent>

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
          <KeyDraftFields draft={createDraft} groups={displayGroups} onChange={setCreateDraft} />
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
            groups={displayGroups}
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
