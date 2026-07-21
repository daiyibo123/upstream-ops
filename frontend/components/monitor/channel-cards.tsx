"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { useNavigate } from "react-router-dom"
import { toast } from "sonner"
import {
  ArrowUpDown,
  CheckCircle2,
  ChevronDown,
  ExternalLink,
  KeyRound,
  Loader2,
  LogIn,
  MoreHorizontal,
  Pause,
  Pencil,
  Pin,
  Play,
  Plus,
  RefreshCw,
  Search,
  Server,
  Tags,
  Trash2,
  ChevronsLeft,
  ChevronsRight,
  XCircle,
  Upload,
  Users,
  Activity,
} from "lucide-react"
import { Card } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { useChannels, useChannelsPage, useChannelRates } from "@/lib/queries"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { channelTypeLabel, decimal, formatRatio, money, relativeTime } from "@/lib/format"
import { cn } from "@/lib/utils"
import { syncAllChannelsStream, syncChannelStream, testLoginStream, type ProgressEvent } from "@/lib/sync-stream"
import type { Channel, RateSnapshot } from "@/lib/api-types"
import { ChannelFormDialog } from "@/components/monitor/channel-form-dialog"
import { ChannelAPIKeysDialog } from "@/components/monitor/channel-api-keys-dialog"
import { ManualGroupKeyDialog } from "@/components/monitor/manual-group-key-dialog"

type Status = "healthy" | "low" | "failed" | "idle"
type ChannelPageSize = 9 | 18 | 36 | 72 | 81 | "all"
type GroupSortMode = "channel-asc" | "channel-desc" | "ratio-asc" | "ratio-desc"

interface OAuthPoolStatsView {
  total: number
  rate_limited: number
  available: number
}

type OAuthPoolKind = "chatgpt" | "grok"
type OAuthPoolStatsState = Record<OAuthPoolKind, OAuthPoolStatsView | null>

const FIXED_POOL_CHANNELS: Array<{ pool: OAuthPoolKind; name: string; type: Channel["type"] }> = [
  { pool: "chatgpt", name: "chatgpt号池", type: "chatgpt_pool" },
  { pool: "grok", name: "grok号池", type: "grok_pool" },
]

const channelPageSizeOptions: ChannelPageSize[] = [9, 18, 36, 72, 81, "all"]

function pageNumbers(currentPage: number, totalPages: number) {
  const first = Math.max(1, currentPage - 3)
  const last = Math.min(totalPages, currentPage + 3)
  return Array.from({ length: last - first + 1 }, (_, i) => first + i)
}

function statusOf(c: Channel): Status {
  if (c.last_error) return "failed"
  if (c.last_balance == null) return "idle"
  if (c.balance_threshold > 0 && c.last_balance < c.balance_threshold) return "low"
  return "healthy"
}

function isManualChannel(c?: Channel | null) {
  return !!c?.manual || (c?.credential_mode === "token" && c?.username?.trim().toLowerCase() === "manual")
}

function isFixedPoolChannel(c?: Channel | null) {
	return c?.fixed === true || c?.type === "chatgpt_pool" || c?.type === "grok_pool"
}

const statusMap: Record<Status, { label: string; cls: string }> = {
  healthy: { label: "健康", cls: "text-success bg-success/10" },
  low: { label: "低余额", cls: "text-warning bg-warning/10" },
  failed: { label: "登录失败", cls: "text-danger bg-danger/10" },
  idle: { label: "尚未采集", cls: "text-muted-foreground bg-muted/40" },
}

// statusDotCls 是行式布局里名称前那颗状态点的填充色，与 statusMap 的语义一一对应。
const statusDotCls: Record<Status, string> = {
  healthy: "bg-success",
  low: "bg-warning",
  failed: "bg-danger",
  idle: "bg-muted-foreground/40",
}

function rechargeMultiplierTip(c: Channel) {
  const mode = c.recharge_multiplier_mode === "multiply" ? "余额 × 倍率" : "余额 / 倍率"
  if (c.recharge_multiplier != null && c.recharge_multiplier > 0) {
    return `充值倍率：${decimal(c.recharge_multiplier, 4)}（${mode}）`
  }
  return `充值倍率：跟随上游（${mode}）`
}

/** ratioTone 按倍率给 chip 上色，与 ChannelRatesPanel 共用同一套规则。 */
function ratioTone(r: number): string {
  if (r <= 0.8) return "bg-success/10 text-success ring-success/20"
  if (r > 2) return "bg-danger/10 text-danger ring-danger/20"
  if (r > 1.2) return "bg-warning/10 text-warning ring-warning/20"
  return "bg-muted text-foreground ring-border"
}

/** InlineRates 在渠道卡片内部展示当前所有分组倍率，默认 2 行折叠 + 展开按钮。 */
function InlineRates({ channelID }: { channelID: number }) {
  const { data, loading } = useChannelRates(channelID)
  const rates = [...(data ?? [])].sort((a, b) => a.ratio - b.ratio)
  const [expanded, setExpanded] = useState(false)
  const [hasOverflow, setHasOverflow] = useState(false)
  const chipBoxRef = useRef<HTMLDivElement>(null)

  // 监听 chip 容器尺寸变化，决定是否要显示"展开"按钮。
  // 收起状态下 scrollHeight > clientHeight 表示有内容被裁剪。
  useEffect(() => {
    const el = chipBoxRef.current
    if (!el) return
    const check = () => {
      if (expanded) return
      setHasOverflow(el.scrollHeight > el.clientHeight + 1)
    }
    check()
    const ro = new ResizeObserver(check)
    ro.observe(el)
    return () => ro.disconnect()
  }, [rates.length, expanded])

  if (loading) return null
  if (rates.length === 0) return null

  const showToggle = hasOverflow || expanded
  const latest = rates.reduce<string | null>((acc, rate) => {
    if (!rate.last_seen_at) return acc
    if (!acc || new Date(rate.last_seen_at).getTime() > new Date(acc).getTime()) return rate.last_seen_at
    return acc
  }, null)

  return (
    <div className="mt-3 border-t border-border pt-2.5">
      <div className="mb-1.5 flex items-center justify-between">
        <p className="text-[11px] text-muted-foreground">
          自动采集 · {rates.length} 个分组{latest ? ` · ${relativeTime(latest)}` : ""}
        </p>
        {showToggle ? (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            className="inline-flex items-center gap-0.5 text-[11px] text-muted-foreground hover:text-foreground"
          >
            {expanded ? "收起" : "展开"}
            <ChevronDown
              className={cn(
                "size-3 transition-transform duration-200",
                expanded && "rotate-180",
              )}
            />
          </button>
        ) : null}
      </div>

      <div className="relative min-h-16">
        <div
          ref={chipBoxRef}
          className={cn(
            "flex flex-wrap gap-1 overflow-hidden transition-[max-height] duration-300 ease-out",
            // 收起：max-h-12 (~48px) 约 2 行；展开：足够大的上限，留点缓冲让 transition 不立即消失。
            expanded ? "max-h-150" : "max-h-12",
          )}
        >
          {rates.map((r) => (
            <Tooltip key={r.id} delayDuration={150}>
              <TooltipTrigger asChild>
                <span
                  className={cn(
                    "inline-flex cursor-default items-center gap-1 rounded px-1.5 py-0.5 text-[11px] ring-1 ring-inset transition-colors hover:bg-muted/60",
                    ratioTone(r.ratio),
                  )}
                >
                  <span className="font-medium">{r.model_name}</span>
                  <span className="rounded bg-primary/10 px-1 font-semibold tabular-nums text-primary ring-1 ring-inset ring-primary/15">
                    {formatRatio(r.ratio)}
                  </span>
                </span>
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-xs text-xs">
                <p className="font-medium">{r.model_name}</p>
                {r.description ? (
                  <p className="mt-0.5 text-muted-foreground">{r.description}</p>
                ) : (
                  <p className="mt-0.5 italic text-muted-foreground">{"(无描述)"}</p>
                )}
                <p className="mt-0.5 text-muted-foreground">
                  {"最近更新："}
                  {relativeTime(r.last_seen_at)}
                </p>
              </TooltipContent>
            </Tooltip>
          ))}
        </div>
        {/* 折叠时底部淡出，提示还有更多内容 */}
        {!expanded && hasOverflow ? (
          <div className="pointer-events-none absolute inset-x-0 bottom-0 h-4 bg-linear-to-t from-background to-transparent" />
        ) : null}
      </div>
    </div>
  )
}

interface ChannelGroupRow {
  key: string
  channel: Channel
  rate: RateSnapshot
}

function ChannelGroupsDialog({
  open,
  onOpenChange,
  channels,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  channels: Channel[]
}) {
  const [query, setQuery] = useState("")
  const [sortMode, setSortMode] = useState<GroupSortMode>("channel-asc")
  const [rows, setRows] = useState<ChannelGroupRow[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoading(true)
    setError(null)

    Promise.all(
      channels.map(async (channel) => {
        const rates = await apiFetch<RateSnapshot[]>(`/channels/${channel.id}/rates`)
        return { channel, rates }
      }),
    )
      .then((result) => {
        if (cancelled) return
        setRows(
          result.flatMap(({ channel, rates }) =>
            rates.map((rate) => ({
              key: `${channel.id}-${rate.id}`,
              channel,
              rate,
            })),
          ),
        )
      })
      .catch((e: Error) => {
        if (!cancelled) setError(e.message || "加载分组失败")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [open, channels])

  const filteredRows = useMemo(() => {
    const q = query.trim().toLowerCase()
    return rows
      .filter(({ channel, rate }) => {
        if (!q) return true
        return [
          channel.name,
          channelTypeLabel(channel.type),
          rate.model_name,
          rate.description ?? "",
          formatRatio(rate.ratio),
        ].some((value) => value.toLowerCase().includes(q))
      })
      .sort((a, b) => {
        if (sortMode === "ratio-asc" || sortMode === "ratio-desc") {
          const diff = a.rate.ratio - b.rate.ratio
          return sortMode === "ratio-asc" ? diff : -diff
        }
        const diff = a.channel.name.localeCompare(b.channel.name, "zh-CN")
          || a.rate.model_name.localeCompare(b.rate.model_name, "zh-CN")
        return sortMode === "channel-asc" ? diff : -diff
      })
  }, [query, rows, sortMode])

  const channelCount = new Set(rows.map((row) => row.channel.id)).size

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle className="text-base font-medium">{"分组"}</DialogTitle>
          <DialogDescription className="text-xs">
            {loading ? "正在加载全部渠道分组" : `${rows.length} 个分组 · ${channelCount} 个渠道`}
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
          <div className="relative min-w-0 flex-1">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="搜索渠道、分组或倍率"
              className="h-9 pl-8 text-xs"
            />
          </div>
          <Select value={sortMode} onValueChange={(value) => setSortMode(value as GroupSortMode)}>
            <SelectTrigger className="h-9 w-full gap-2 text-xs sm:w-40">
              <ArrowUpDown className="size-4 text-muted-foreground" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem value="channel-asc">{"渠道 A-Z"}</SelectItem>
              <SelectItem value="channel-desc">{"渠道 Z-A"}</SelectItem>
              <SelectItem value="ratio-asc">{"倍率从低到高"}</SelectItem>
              <SelectItem value="ratio-desc">{"倍率从高到低"}</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <ScrollArea className="h-[60vh] rounded-md border">
          <Table className="text-xs">
            <TableHeader>
              <TableRow>
                <TableHead className="h-9 font-medium text-muted-foreground">{"渠道"}</TableHead>
                <TableHead className="h-9 font-medium text-muted-foreground">{"类型"}</TableHead>
                <TableHead className="h-9 font-medium text-muted-foreground">{"分组"}</TableHead>
                <TableHead className="h-9 text-right font-medium text-muted-foreground">{"倍率"}</TableHead>
                <TableHead className="h-9 text-right font-medium text-muted-foreground">{"更新"}</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-muted-foreground">
                    {"加载中…"}
                  </TableCell>
                </TableRow>
              ) : error ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-danger">
                    {error}
                  </TableCell>
                </TableRow>
              ) : filteredRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={5} className="h-24 text-center text-xs text-muted-foreground">
                    {rows.length === 0 ? "暂无分组数据" : "没有匹配的分组"}
                  </TableCell>
                </TableRow>
              ) : (
                filteredRows.map(({ key, channel, rate }) => (
                  <TableRow key={key}>
                    <TableCell>{channel.name}</TableCell>
                    <TableCell>
                      <span
                        className={cn(
                          "inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-normal ring-1 ring-inset",
                          channel.type === "newapi"
                            ? "bg-brand/10 text-brand ring-brand/20"
                            : "bg-brand/10 text-brand ring-brand/20",
                        )}
                      >
                        {channelTypeLabel(channel.type)}
                      </span>
                    </TableCell>
                    <TableCell className="min-w-60 whitespace-normal">
                      <div>{rate.model_name}</div>
                      {rate.description ? (
                        <div className="mt-0.5 line-clamp-2 text-xs text-muted-foreground">
                          {rate.description}
                        </div>
                      ) : null}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      <span className={cn("rounded px-1.5 py-0.5 ring-1 ring-inset", ratioTone(rate.ratio))}>
                        {formatRatio(rate.ratio)}
                      </span>
                    </TableCell>
                    <TableCell className="text-right text-muted-foreground">
                      {relativeTime(rate.last_seen_at)}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  )
}

interface ChannelSyncState {
  running: boolean
  events: ProgressEvent[]
  latest: ProgressEvent | null
  finalOk: boolean | null
  fading: boolean
}

function emptySyncState(): ChannelSyncState {
  return { running: false, events: [], latest: null, finalOk: null, fading: false }
}

interface BulkSyncState {
  running: boolean
  completed: number
  total: number
}

const stageLabel: Record<ProgressEvent["stage"], string> = {
  captcha: "Key/Token",
  session: "会话",
  login: "登录",
  balance: "余额",
  cost: "消费",
  subscription: "订阅",
  rates: "倍率",
  gateway_health: "测活",
  done: "完成",
  error: "失败",
}

const stageOrder: Record<ProgressEvent["stage"], number> = {
  captcha: 1,
  session: 2,
  login: 3,
  balance: 4,
  cost: 5,
  subscription: 6,
  rates: 7,
  gateway_health: 8,
  done: 9,
  error: 9,
}

/** 按 stage 去重，每个 stage 只留最后一条事件（"在做中→完成"会被覆盖成完成态）。 */
function deriveSteps(events: ProgressEvent[]): ProgressEvent[] {
  const byStage = new Map<ProgressEvent["stage"], ProgressEvent>()
  for (const ev of events) byStage.set(ev.stage, ev)
  return [...byStage.values()].sort((a, b) => stageOrder[a.stage] - stageOrder[b.stage])
}

function SyncProgressStrip({ state }: { state: ChannelSyncState }) {
  if (!state.running && state.latest == null) return null
  const steps = deriveSteps(state.events)

  return (
    <div
      className={cn(
        "mt-3 rounded-lg border border-border bg-muted/30 px-3 py-2.5",
        // 入场：上方滑入 + 淡入
        "animate-in fade-in slide-in-from-top-1 duration-300",
        // 出场：和 scheduleHide 里的 500ms 对齐
        "transition-all duration-500 ease-out",
        state.fading ? "-translate-y-0.5 opacity-0" : "opacity-100",
      )}
    >
      {steps.length === 0 ? (
        <div className="flex items-center gap-2 text-xs">
          <Loader2 className="size-3.5 shrink-0 animate-spin text-muted-foreground" />
          <span className="text-foreground/80">{"准备中…"}</span>
        </div>
      ) : (
        <ul className="space-y-1.5">
          {steps.map((ev) => {
            // 终止态：stage=done 或 error；显式 ok=true / false 也算
            const failed = ev.stage === "error" || ev.ok === false
            const succeeded = ev.stage === "done" || ev.ok === true
            const running = !failed && !succeeded
            const Icon = running ? Loader2 : failed ? XCircle : CheckCircle2
            const tone = running ? "text-muted-foreground" : failed ? "text-danger" : "text-success"
            return (
              <li
                key={ev.stage}
                className="flex items-start gap-2 text-xs animate-in fade-in duration-200"
              >
                <Icon
                  className={cn("size-3.5 shrink-0", tone, running && "animate-spin")}
                />
                <span className="w-9 shrink-0 text-[11px] text-muted-foreground">
                  {stageLabel[ev.stage]}
                </span>
                <div className="min-w-0 flex-1 overflow-x-auto">
                  <span
                    className={cn(
                      "block whitespace-pre-wrap",
                      failed ? "text-danger" : running ? "text-foreground/80" : "text-foreground",
                    )}
                  >
                    {ev.message}
                  </span>
                </div>
              </li>
            )
          })}
        </ul>
      )}
    </div>
  )
}

function FixedPoolChannelRow({
	channel,
	stats,
	statsError,
	busy,
	onInspect,
	onOpen,
}: {
	channel: Channel
	stats: OAuthPoolStatsView | null
	statsError: boolean
	busy: boolean
	onInspect: () => void
	onOpen: (mode: "import" | "manage") => void
}) {
	const provider = channel.type === "grok_pool" ? "Grok OAuth" : "ChatGPT OAuth"
	const usable = (stats?.available ?? 0) > 0
	const statusLabel = statsError ? "状态未知" : stats == null ? "加载中" : usable ? "可调度" : "无可用账号"
	return (
		<Card className="border border-border px-3 py-3 shadow-none">
			<div className="flex h-full flex-col gap-3">
				<div className="flex min-w-0 flex-1 items-center gap-3">
					<div className={cn("flex size-9 shrink-0 items-center justify-center rounded-md", statsError || stats == null ? "bg-muted text-muted-foreground" : usable ? "bg-success/10 text-success" : "bg-danger/10 text-danger")}>
						<Users className="size-4" />
					</div>
					<div className="min-w-0">
						<div className="flex flex-wrap items-center gap-2">
							<h3 className="truncate text-sm font-semibold text-foreground">{channel.name}</h3>
							<span className="rounded bg-brand/10 px-1.5 py-0.5 text-[10px] font-medium text-brand">固定号池</span>
							<span className={cn("rounded px-1.5 py-0.5 text-[10px] font-medium", statsError || stats == null ? "bg-muted text-muted-foreground" : usable ? "bg-success/10 text-success" : "bg-danger/10 text-danger")}>
								{statusLabel}
							</span>
						</div>
						<p className="mt-1 text-xs text-muted-foreground">{provider} · 仅存活且当前可调度的账号会参与 API Key 轮询</p>
					</div>
				</div>
				<div className="grid grid-cols-3 gap-2 sm:min-w-90">
					{[
						["总账号数", stats?.total, "text-foreground"],
						["限流账号数", stats?.rate_limited, (stats?.rate_limited ?? 0) > 0 ? "text-warning" : "text-foreground"],
						["可用账号数", stats?.available, stats == null ? "text-muted-foreground" : usable ? "text-success" : "text-danger"],
					].map(([label, value, tone]) => (
						<div key={String(label)} className="rounded-md border border-border bg-muted/20 px-3 py-2 text-center">
							<p className="text-[10px] text-muted-foreground">{label}</p>
							<p className={cn("mt-0.5 text-base font-semibold tabular-nums", String(tone))}>{value ?? "-"}</p>
						</div>
					))}
				</div>
				<div className="mt-auto flex shrink-0 flex-wrap items-center gap-2 border-t border-border pt-3">
					<Button size="sm" variant="outline" className="gap-1.5 text-xs" onClick={() => onOpen("import")}><Upload className="size-3.5" />导入</Button>
					<Button size="sm" variant="outline" className="gap-1.5 text-xs" onClick={() => onOpen("manage")}><Users className="size-3.5" />管理</Button>
					<Button size="sm" className="gap-1.5 text-xs" disabled={busy || stats == null || stats.total === 0} onClick={onInspect}>
						<Activity className={cn("size-3.5", busy && "animate-pulse")} />{busy ? "巡检中" : "巡检"}
					</Button>
				</div>
			</div>
		</Card>
	)
}

export function ChannelCards() {
	const navigate = useNavigate()
  const { data: channels, loading: channelsLoading } = useChannels()
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState<ChannelPageSize>(9)
  const [channelSearch, setChannelSearch] = useState("")
  const pageQuery = useChannelsPage(page, pageSize === "all" ? -1 : pageSize, channelSearch)
  const refresh = useTriggerRefresh()
  const { confirm, dialog: confirmDialog } = useConfirm()
  const [editing, setEditing] = useState<Channel | null>(null)
  const [creating, setCreating] = useState(false)
  const [groupsOpen, setGroupsOpen] = useState(false)
  const [manualOpen, setManualOpen] = useState(false)
  const [managingKeys, setManagingKeys] = useState<Channel | null>(null)
  const [busyAction, setBusyAction] = useState<string | null>(null)
	const [poolStats, setPoolStats] = useState<OAuthPoolStatsState>({ chatgpt: null, grok: null })
	const [poolStatsErrors, setPoolStatsErrors] = useState<Record<OAuthPoolKind, boolean>>({ chatgpt: false, grok: false })
  // 每个渠道当前 sync 进度（最新一条事件） + 历史事件
  const [syncState, setSyncState] = useState<Record<number, ChannelSyncState>>({})
  const [bulkSync, setBulkSync] = useState<BulkSyncState>({ running: false, completed: 0, total: 0 })
  const anySyncRunning = bulkSync.running || Object.values(syncState).some((s) => s.running)
  const channelPage = pageQuery.data
  const visibleChannels = channelPage?.items ?? []
	const fixedPoolChannels = FIXED_POOL_CHANNELS.map((definition, index) => (
		(channels ?? []).find((channel) => channel.type === definition.type) ?? {
			id: -(index + 1),
			name: definition.name,
			type: definition.type,
			fixed: true,
		} as Channel
	))
	const orderedVisibleChannels = [
		...fixedPoolChannels,
		...visibleChannels.filter((channel) => !isFixedPoolChannel(channel)),
	]
	const syncableChannels = (channels ?? []).filter((channel) => !isManualChannel(channel) && !isFixedPoolChannel(channel))
  const totalChannels = channelPage?.total ?? 0
	const displayedChannelCount = Math.max(totalChannels, FIXED_POOL_CHANNELS.length)
  const pageSizeAll = pageSize === "all"

	async function loadPoolStats() {
		const pools: OAuthPoolKind[] = ["chatgpt", "grok"]
		const results = await Promise.allSettled(
			pools.map((pool) => apiFetch<OAuthPoolStatsView>(`/oauth-accounts/${pool}/stats`)),
		)
		setPoolStats((current) => {
			const next = { ...current }
			results.forEach((result, index) => {
				if (result.status === "fulfilled") next[pools[index]] = result.value
			})
			return next
		})
		setPoolStatsErrors({
			chatgpt: results[0].status === "rejected",
			grok: results[1].status === "rejected",
		})
	}

	useEffect(() => {
		void loadPoolStats()
	}, [])

	async function inspectPool(pool: OAuthPoolKind) {
		let job = await apiFetch<{ status?: string; total?: number; completed?: number; alive?: number; failed?: number; last_error?: string }>(`/oauth-accounts/${pool}/inspect`, { method: "POST" })
		for (let attempt = 0; job.status === "running" || job.status === "queued"; attempt += 1) {
			if (attempt >= 180) throw new Error("巡检仍在后台运行，请稍后到账号管理页查看进度")
			await new Promise((resolve) => window.setTimeout(resolve, 1000))
			job = await apiFetch(`/oauth-accounts/${pool}/inspect`)
		}
		if (job.status === "failed") throw new Error(job.last_error || "巡检失败")
		toast.success(`巡检完成：存活 ${job.alive ?? 0}，失败 ${job.failed ?? 0}`)
		await loadPoolStats()
	}
  const totalPages = pageSizeAll ? 1 : (channelPage?.pages ?? 1)
  const currentPage = pageSizeAll ? 1 : Math.min(page, totalPages)
  const effectivePageSize = pageSizeAll ? Math.max(totalChannels, 1) : pageSize
  const rangeStart = totalChannels === 0 ? 0 : (currentPage - 1) * effectivePageSize + 1
  const rangeEnd = Math.min((currentPage - 1) * effectivePageSize + visibleChannels.length, totalChannels)
  const pagerNumbers = pageNumbers(currentPage, totalPages)

  // 成功后自动消失需要的两段定时器：先 5s 显示，再 500ms 过渡（与 strip 的 transition-opacity duration-500 对齐）。
  const hideTimers = useRef<Map<number, ReturnType<typeof setTimeout>>>(new Map())

  useEffect(() => {
    const timers = hideTimers.current
    return () => {
      timers.forEach((t) => clearTimeout(t))
      timers.clear()
    }
  }, [])

  useEffect(() => {
    setPage((prev) => Math.min(prev, totalPages))
  }, [totalPages])

  useEffect(() => {
    setPage(1)
  }, [channelSearch])

  function clearHideTimer(id: number) {
    const t = hideTimers.current.get(id)
    if (t != null) {
      clearTimeout(t)
      hideTimers.current.delete(id)
    }
  }

  function scheduleHide(id: number) {
    clearHideTimer(id)
    const t1 = setTimeout(() => {
      patchSync(id, (prev) => ({ ...prev, fading: true }))
      const t2 = setTimeout(() => {
        setSyncState((s) => {
          const { [id]: _gone, ...rest } = s
          void _gone
          return rest
        })
        hideTimers.current.delete(id)
      }, 500)
      hideTimers.current.set(id, t2)
    }, 5000)
    hideTimers.current.set(id, t1)
  }

  function patchSync(id: number, fn: (prev: ChannelSyncState) => ChannelSyncState) {
    setSyncState((s) => ({ ...s, [id]: fn(s[id] ?? emptySyncState()) }))
  }

  async function startStream(channel: Channel, action: "sync" | "test-login") {
    clearHideTimer(channel.id)
    patchSync(channel.id, () => ({
      running: true,
      events: [],
      latest: null,
      finalOk: null,
      fading: false,
    }))
    let sawError = false
    const stream = action === "sync" ? syncChannelStream : testLoginStream
    try {
      await stream(channel.id, {
        onEvent: (ev) => {
          if (ev.stage === "error" || ev.ok === false) sawError = true
          patchSync(channel.id, (prev) => ({
            ...prev,
            events: [...prev.events, ev],
            latest: ev,
          }))
        },
      })
      const ok = !sawError
      patchSync(channel.id, (prev) => ({
        ...prev,
        running: false,
        finalOk: ok,
      }))
      if (ok) scheduleHide(channel.id)
    } catch (e) {
      const err = e as Error
      const failureLabel = action === "sync" ? "同步失败" : "测试登录失败"
      patchSync(channel.id, (prev) => ({
        ...prev,
        running: false,
        finalOk: false,
        latest: {
          stage: "error",
          message: err.message || failureLabel,
          time: new Date().toISOString(),
        },
      }))
      // 失败保留，不调度自动隐藏
    } finally {
      refresh()
    }
  }

  async function startBulkSync() {
    const list = syncableChannels
    if (list.length === 0) {
      toast.info("没有可同步的登录账号渠道")
      return
    }

    for (const channel of list) {
      clearHideTimer(channel.id)
      patchSync(channel.id, () => ({
        running: true,
        events: [],
        latest: null,
        finalOk: null,
        fading: false,
      }))
    }

    setBulkSync({ running: true, completed: 0, total: list.length })
    try {
      await syncAllChannelsStream({
        onEvent: (ev) => {
          if (ev.channel_id != null) {
            patchSync(ev.channel_id, (prev) => ({
              ...prev,
              events: [...prev.events, ev],
              latest: ev,
              running: ev.stage !== "done" && ev.stage !== "error",
              finalOk: ev.stage === "done" ? true : ev.stage === "error" ? false : prev.finalOk,
              fading: false,
            }))
            if (ev.stage === "done") {
              scheduleHide(ev.channel_id)
            }
          }

          if (ev.index != null && ev.total != null) {
            setBulkSync((prev) => ({
              ...prev,
              completed: Math.max(prev.completed, ev.index ?? prev.completed),
              total: ev.total ?? prev.total,
            }))
          }

          if (ev.channel_id == null && (ev.stage === "done" || ev.stage === "error")) {
            if (ev.stage === "done") {
              toast.success(ev.message)
            } else {
              toast.error(ev.message)
            }
          }
        },
      })
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "批量同步失败")
    } finally {
      setSyncState((s) => {
        const next: Record<number, ChannelSyncState> = {}
        for (const [id, state] of Object.entries(s)) {
          next[Number(id)] = { ...state, running: false }
        }
        return next
      })
      setBulkSync((prev) => ({ ...prev, running: false }))
      refresh()
    }
  }

  async function withBusy(key: string, fn: () => Promise<unknown>) {
    setBusyAction(key)
    try {
      await fn()
      refresh()
    } catch (e) {
      const err = e as Error
      toast.error(err.message || "操作失败")
    } finally {
      setBusyAction(null)
    }
  }

  return (
    <section className="space-y-4">
      <div className="flex flex-col gap-4 border-b border-border/70 pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div className="flex min-w-0 items-start gap-3">
          <span className="mt-0.5 flex size-10 shrink-0 items-center justify-center rounded-lg border border-brand/15 bg-brand/8 text-brand shadow-xs">
            <Server className="size-[18px]" />
          </span>
          <div>
            <h1 className="text-xl font-semibold text-foreground">上游渠道</h1>
            <p className="mt-1 text-sm text-muted-foreground">管理普通渠道与固定号池，查看健康、倍率和账号可用性。</p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2 sm:gap-3">
          <span className="text-xs text-muted-foreground">
            {displayedChannelCount}{" 个渠道"}
          </span>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            disabled={anySyncRunning || syncableChannels.length === 0}
            onClick={() => void startBulkSync()}
          >
            <RefreshCw className={cn("size-3.5", bulkSync.running && "animate-spin")} />
            {bulkSync.running ? `同步中 ${bulkSync.completed}/${bulkSync.total}` : "同步"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => setManualOpen(true)}
          >
            <Plus className="size-3.5" />
            {"手动添加"}
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            disabled={channelsLoading || totalChannels === 0}
            onClick={() => setGroupsOpen(true)}
          >
            <Tags className="size-3.5" />
            {"分组"}
          </Button>
          <Button
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => {
              setEditing(null)
              setCreating(true)
            }}
          >
            <Plus className="size-3.5" />
            {"新增"}
          </Button>
        </div>
      </div>

      <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={channelSearch}
            onChange={(event) => setChannelSearch(event.target.value)}
            className="h-9 pl-8 text-xs"
            placeholder="搜索渠道、地址、账号或对应上游分组"
            aria-label="搜索渠道或上游分组"
          />
        </div>
        <p className="text-xs text-muted-foreground">置顶仅影响渠道列表展示，不影响调度成本排序</p>
      </div>

      {pageQuery.loading && !channelPage ? (
        <p className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
          {"加载中…"}
        </p>
      ) : totalChannels === 0 && orderedVisibleChannels.length === 0 ? (
        <div className="rounded-lg border border-dashed border-border px-4 py-10 text-center">
          <p className="text-sm text-muted-foreground">{"还没有任何渠道。"}</p>
          <Button
            size="sm"
            className="mt-3 gap-1.5"
            onClick={() => {
              setEditing(null)
              setCreating(true)
            }}
          >
            <Plus className="size-3.5" />
            {"添加第一个渠道"}
          </Button>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
            {orderedVisibleChannels.map((c) => {
						if (isFixedPoolChannel(c)) {
							const provider = c.type === "grok_pool" ? "grok" : "chatgpt"
							return (
								<FixedPoolChannelRow
									key={c.id}
									channel={c}
									stats={poolStats[provider]}
									statsError={poolStatsErrors[provider]}
									busy={busyAction === `inspect-${provider}`}
									onOpen={(mode) => navigate(`/oauth?pool=${provider}${mode === "import" ? "&import=1" : ""}`)}
									onInspect={() => void withBusy(`inspect-${provider}`, () => inspectPool(provider))}
								/>
							)
						}
              const status = statusOf(c)
              const meta = statusMap[status]
              const manual = isManualChannel(c)
              return (
                <Card key={c.id} className="flex flex-col gap-0 border border-border px-3 py-2 shadow-none md:col-span-2">
                  <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5">
                    {/* 左：状态点 + 名称 + 徽章 */}
                    <div className="flex min-w-0 flex-1 items-center gap-2">
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <span className={cn("size-2 shrink-0 rounded-full", statusDotCls[status])} aria-label={meta.label} />
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">
                          {meta.label}
                        </TooltipContent>
                      </Tooltip>
                      <span className="truncate text-sm font-semibold text-foreground">{c.name}</span>
                      {c.pinned ? (
                        <span className="inline-flex shrink-0 items-center gap-0.5 rounded bg-warning/10 px-1 py-0.5 text-[10px] font-medium text-warning ring-1 ring-inset ring-warning/20">
                          <Pin className="size-2.5 fill-current" />
                          置顶
                        </span>
                      ) : null}
                      <span className="inline-flex shrink-0 items-center rounded bg-brand/10 px-1 py-0.5 text-[10px] font-medium text-brand ring-1 ring-inset ring-brand/20">
                        {channelTypeLabel(c.type)}
                      </span>
                      {manual ? (
                        <span className="inline-flex shrink-0 items-center rounded bg-muted px-1 py-0.5 text-[10px] font-medium text-muted-foreground ring-1 ring-inset ring-border">
                          手动
                        </span>
                      ) : null}
                      {!c.monitor_enabled ? (
                        <span className="inline-flex shrink-0 items-center rounded bg-warning/10 px-1 py-0.5 text-[10px] font-medium text-warning ring-1 ring-inset ring-warning/20">
                          {"已暂停"}
                        </span>
                      ) : null}
                    </div>

                    {/* 中：余额 / 今日 / 累计 紧凑数值 */}
                    <div className="flex shrink-0 items-center gap-3 text-[11px] tabular-nums">
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <span className="flex items-baseline gap-1">
                            <span className="text-[10px] text-muted-foreground">余额</span>
                            <span className="font-semibold text-foreground">{money(c.last_balance)}</span>
                          </span>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">
                          {rechargeMultiplierTip(c)}
                        </TooltipContent>
                      </Tooltip>
                      <span className="flex items-baseline gap-1">
                        <span className="text-[10px] text-muted-foreground">今日</span>
                        <span className="font-medium text-foreground">{money(c.today_cost)}</span>
                      </span>
                      <span className="hidden items-baseline gap-1 sm:flex">
                        <span className="text-[10px] text-muted-foreground">累计</span>
                        <span className="font-medium text-foreground">{money(c.total_cost)}</span>
                      </span>
                      <span className="hidden text-[10px] text-muted-foreground lg:inline">
                        {relativeTime(c.last_balance_at ?? c.updated_at)}
                      </span>
                    </div>

                    {/* 右：操作 */}
                    <div className="flex shrink-0 items-center gap-1">
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            disabled={manual || !!syncState[c.id]?.running || anySyncRunning}
                            onClick={() => startStream(c, "sync")}
                            aria-label="同步"
                          >
                            <RefreshCw className={cn("size-3.5", syncState[c.id]?.running && "animate-spin")} />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">{"同步"}</TooltipContent>
                      </Tooltip>
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            disabled={manual || !!syncState[c.id]?.running || anySyncRunning}
                            onClick={() => startStream(c, "test-login")}
                            aria-label="测试登录"
                          >
                            <LogIn className="size-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">{"测试登录"}</TooltipContent>
                      </Tooltip>
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            onClick={() => setManagingKeys(c)}
                            aria-label="密钥"
                          >
                            <KeyRound className="size-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">{"密钥"}</TooltipContent>
                      </Tooltip>
                      <Tooltip delayDuration={150}>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            onClick={() => {
                              setEditing(c)
                              setCreating(true)
                            }}
                            aria-label="编辑"
                          >
                            <Pencil className="size-3.5" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="text-xs">{"编辑"}</TooltipContent>
                      </Tooltip>
                      <Button
                        asChild
                        variant="ghost"
                        size="icon-sm"
                        className="size-7 text-muted-foreground hover:text-foreground"
                      >
                        <a
                          href={c.site_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          aria-label={`新窗口打开 ${c.name} 站点地址`}
                        >
                          <ExternalLink className="size-3.5" />
                        </a>
                      </Button>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="size-7 text-muted-foreground hover:text-foreground"
                            disabled={busyAction === `clear-login-${c.id}` || busyAction === `delete-${c.id}`}
                            aria-label="更多"
                          >
                            <MoreHorizontal className="size-3.5" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" className="w-44">
                          <DropdownMenuItem
                            disabled={manual || busyAction === `pin-${c.id}`}
                            onSelect={(e) => {
                              e.preventDefault()
                              void withBusy(`pin-${c.id}`, () =>
                                apiFetch(`/channels/${c.id}`, {
                                  method: "PUT",
                                  body: JSON.stringify({ pinned: !c.pinned }),
                                }),
                              )
                            }}
                          >
                            <Pin className={cn("size-3.5", c.pinned && "fill-current text-warning")} />
                            {c.pinned ? "取消置顶" : "置顶"}
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            disabled={manual || busyAction === `toggle-${c.id}`}
                            onSelect={(e) => {
                              e.preventDefault()
                              void withBusy(`toggle-${c.id}`, () =>
                                apiFetch(`/channels/${c.id}/${c.monitor_enabled ? "disable" : "enable"}`, {
                                  method: "POST",
                                }),
                              )
                            }}
                          >
                            {c.monitor_enabled ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
                            {c.monitor_enabled ? "暂停监控" : "恢复监控"}
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            disabled={manual || busyAction === `rates-${c.id}` || !!syncState[c.id]?.running || anySyncRunning}
                            onSelect={(e) => {
                              e.preventDefault()
                              void withBusy(`rates-${c.id}`, async () => {
                                await apiFetch(`/channels/${c.id}/refresh-rates`, { method: "POST" })
                                toast.success("已刷新该渠道倍率")
                              })
                            }}
                          >
                            <RefreshCw className={cn("size-3.5", busyAction === `rates-${c.id}` && "animate-spin")} />
                            {"仅刷新倍率"}
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            disabled={manual || busyAction === `clear-login-${c.id}`}
                            onSelect={async (e) => {
                              e.preventDefault()
                              const ok = await confirm({
                                title: `清空 ${c.name} 的登录信息？`,
                                description: "将清空缓存会话；Token 模式还会清空已保存的 Access Token、Refresh Token 和 NewAPI Cookie。账号密码本身不会删除。",
                                confirmLabel: "清空",
                                destructive: true,
                              })
                              if (!ok) return
                              void withBusy(`clear-login-${c.id}`, async () => {
                                await apiFetch(`/channels/${c.id}/clear-login-info`, { method: "POST" })
                                toast.success("已清空登录信息")
                              })
                            }}
                          >
                            <XCircle className="size-3.5" />
                            {"清空登录信息"}
                          </DropdownMenuItem>
                          <DropdownMenuSeparator />
                          <DropdownMenuItem
                            variant="destructive"
                            disabled={busyAction === `delete-${c.id}`}
                            onSelect={async (e) => {
                              e.preventDefault()
                              const ok = await confirm({
                                title: `删除渠道 ${c.name}？`,
                                description: "删除后该渠道的余额历史、倍率快照与登录凭据都将一并清除，且无法恢复。",
                                confirmLabel: "删除",
                                destructive: true,
                              })
                              if (!ok) return
                              void withBusy(`delete-${c.id}`, () =>
                                apiFetch(`/channels/${c.id}`, { method: "DELETE" }),
                              )
                            }}
                          >
                            <Trash2 className="size-3.5" />
                            {"删除"}
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>

                  {c.last_error ? (
                    <p className="mt-1.5 max-h-12 overflow-y-auto whitespace-pre-wrap break-words rounded border border-border bg-muted/20 px-2 py-1 text-[11px] leading-4 text-danger" title={c.last_error}>
                      {c.last_error}
                    </p>
                  ) : null}

                  <InlineRates channelID={c.id} />

                  <SyncProgressStrip state={syncState[c.id] ?? emptySyncState()} />
                </Card>
              )
            })}
          </div>

          <div className="mt-3 flex flex-col gap-2 rounded-lg border border-border bg-muted/10 px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-xs text-muted-foreground">
              {pageSizeAll
                ? `显示全部 ${totalChannels} 个渠道`
                : `显示 ${rangeStart}-${rangeEnd} / ${totalChannels} 个渠道`}
            </div>
            <div className="flex flex-wrap items-center gap-2 sm:justify-end">
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <span>{"每页"}</span>
                <Select
                  value={String(pageSize)}
                  onValueChange={(value) => {
                    setPageSize(value === "all" ? "all" : Number(value) as ChannelPageSize)
                    setPage(1)
                  }}
                >
                  <SelectTrigger size="sm" className="h-8 w-20 text-xs">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent align="end">
                    {channelPageSizeOptions.map((value) => (
                      <SelectItem key={value} value={String(value)}>
                        {value === "all" ? "全部" : value}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex flex-wrap items-center gap-1.5">
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-2 text-xs"
                  disabled={pageSizeAll || currentPage <= 1}
                  onClick={() => setPage(1)}
                >
                  <ChevronsLeft className="size-3.5" />
                  <span className="hidden sm:inline">{"首页"}</span>
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-2 text-xs"
                  disabled={pageSizeAll || currentPage <= 1}
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                >
                  {"上一页"}
                </Button>
                {pageSizeAll ? (
                  <span className="min-w-12 text-center text-xs text-muted-foreground">
                    {"全部"}
                  </span>
                ) : (
                  pagerNumbers.map((pageNumber) => (
                    <Button
                      key={pageNumber}
                      variant={pageNumber === currentPage ? "default" : "outline"}
                      size="sm"
                      className="h-8 min-w-8 px-2 text-xs"
                      onClick={() => setPage(pageNumber)}
                    >
                      {pageNumber}
                    </Button>
                  ))
                )}
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-2 text-xs"
                  disabled={pageSizeAll || currentPage >= totalPages}
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                >
                  {"下一页"}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  className="h-8 px-2 text-xs"
                  disabled={pageSizeAll || currentPage >= totalPages}
                  onClick={() => setPage(totalPages)}
                >
                  <span className="hidden sm:inline">{"末页"}</span>
                  <ChevronsRight className="size-3.5" />
                </Button>
              </div>
            </div>
          </div>
        </>
      )}

      <ChannelFormDialog
        open={creating}
        onOpenChange={(v) => {
          setCreating(v)
          if (!v) setEditing(null)
        }}
        channel={editing}
      />

      <ChannelGroupsDialog
        open={groupsOpen}
        onOpenChange={setGroupsOpen}
        channels={channels ?? []}
      />

      <ManualGroupKeyDialog
        open={manualOpen}
        onOpenChange={setManualOpen}
        channels={channels ?? []}
        onCreated={() => {
          refresh()
        }}
      />

      <ChannelAPIKeysDialog
        open={managingKeys != null}
        onOpenChange={(v) => {
          if (!v) setManagingKeys(null)
        }}
        channel={managingKeys}
      />

      {confirmDialog}
    </section>
  )
}
