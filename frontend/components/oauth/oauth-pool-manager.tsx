import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  Activity,
  AlertCircle,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  Clock3,
  FileUp,
  Gauge,
  Loader2,
  RefreshCw,
  SearchX,
  ShieldAlert,
  Trash2,
  Users,
  X,
  XCircle,
} from "lucide-react"
import { toast } from "sonner"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { Progress } from "@/components/ui/progress"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  batchDeleteOAuthAccounts,
  checkOAuthAccount,
  deleteOAuthAccount,
  getOAuthAccounts,
  getOAuthInspection,
  getOAuthPoolStats,
  queryOAuthAccountQuota,
  startOAuthInspection,
} from "@/components/oauth/oauth-api"
import { maskIdentifier, OAuthImportDialog, redactSensitiveText } from "@/components/oauth/oauth-import-dialog"
import type {
  OAuthAccount,
  OAuthAccountFilter,
  OAuthAccountStatus,
  OAuthInspectionJob,
  OAuthPoolKind,
  OAuthPoolQuery,
  OAuthPoolStats,
} from "@/components/oauth/types"
import { cn } from "@/lib/utils"

interface OAuthPoolManagerProps {
  pool: OAuthPoolKind
  query: OAuthPoolQuery
  onQueryChange: (next: OAuthPoolQuery) => void
	importRequested?: boolean
	onImportRequestHandled?: () => void
}

const FILTERS: Array<{ value: OAuthAccountFilter; label: string }> = [
  { value: "all", label: "全部" },
  { value: "alive", label: "存活" },
  { value: "rate_limited", label: "限流" },
  { value: "dead", label: "死亡" },
]

interface BatchDeleteFeedback {
  success: number
  failed: number
  failures: Array<{ id: string; reason: string }>
}

export function OAuthPoolManager({ pool, query, onQueryChange, importRequested = false, onImportRequestHandled }: OAuthPoolManagerProps) {
  const [stats, setStats] = useState<OAuthPoolStats | null>(null)
  const [accounts, setAccounts] = useState<OAuthAccount[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [statsError, setStatsError] = useState("")
  const [reloadToken, setReloadToken] = useState(0)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [testingIDs, setTestingIDs] = useState<Set<string>>(new Set())
  const [quotaLoadingIDs, setQuotaLoadingIDs] = useState<Set<string>>(new Set())
  const [deletingIDs, setDeletingIDs] = useState<Set<string>>(new Set())
  const [batchDeleting, setBatchDeleting] = useState(false)
  const [batchDeleteFeedback, setBatchDeleteFeedback] = useState<BatchDeleteFeedback | null>(null)
  const [importOpen, setImportOpen] = useState(false)
  const [inspection, setInspection] = useState<OAuthInspectionJob | null>(null)
  const [inspectionRunning, setInspectionRunning] = useState(false)
  const requestID = useRef(0)
  const { confirm, dialog: confirmDialog } = useConfirm()

	useEffect(() => {
		if (!importRequested) return
		setImportOpen(true)
		onImportRequestHandled?.()
	}, [importRequested, onImportRequestHandled])

  const refresh = useCallback(() => setReloadToken((value) => value + 1), [])

  useEffect(() => {
    const currentRequest = ++requestID.current
    setLoading(true)
    setError("")
    setStatsError("")
    Promise.allSettled([
      getOAuthPoolStats(pool),
      getOAuthAccounts(pool, query),
    ])
      .then(([statsResult, accountsResult]) => {
        if (requestID.current !== currentRequest) return
        if (statsResult.status === "fulfilled") {
          setStats(statsResult.value)
        } else {
          setStats(null)
          setStatsError(safeErrorMessage(statsResult.reason, "号池统计加载失败，请稍后重试。"))
        }
        if (accountsResult.status === "rejected") {
          setError(safeErrorMessage(accountsResult.reason, "账号列表加载失败，请稍后重试。"))
          return
        }
        const page = accountsResult.value
        setAccounts(page.items)
        setTotal(page.total)
        setSelected((current) => {
          const visible = new Set(page.items.map((account) => account.id))
          return new Set([...current].filter((id) => visible.has(id)))
        })
      })
      .finally(() => {
        if (requestID.current === currentRequest) setLoading(false)
      })
  }, [pool, query, reloadToken])

  useEffect(() => {
    let cancelled = false
    getOAuthInspection(pool)
      .then((job) => {
        if (cancelled || !job) return
        setInspection(job)
        setInspectionRunning(job.status === "queued" || job.status === "running")
      })
      .catch(() => {
        // Inspection history is supplementary; account loading remains usable.
      })
    return () => {
      cancelled = true
    }
  }, [pool])

  useEffect(() => {
    if (!inspectionRunning) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let consecutiveFailures = 0

    const poll = async () => {
      try {
        const job = await getOAuthInspection(pool)
        if (cancelled) return
        if (!job) {
          timer = setTimeout(poll, 1200)
          return
        }
        consecutiveFailures = 0
        setInspection(job)
        if (job.status === "queued" || job.status === "running") {
          timer = setTimeout(poll, 1200)
          return
        }
        setInspectionRunning(false)
        refresh()
        if (job.status === "completed") {
          toast.success(`巡检完成：存活 ${job.alive}，限流 ${job.limited}，死亡 ${job.dead}，失败 ${job.failed}`)
        } else {
          toast.error(redactSensitiveText(job.error || "号池巡检失败"))
        }
      } catch (caught) {
        if (cancelled) return
        consecutiveFailures++
        if (consecutiveFailures < 3) {
          timer = setTimeout(poll, 1500)
          return
        }
        setInspectionRunning(false)
        setInspection((current) => current ? {
          ...current,
          status: "failed",
          error: safeErrorMessage(caught, "巡检状态读取失败，请稍后重试。"),
        } : null)
      }
    }

    timer = setTimeout(poll, 600)
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [inspectionRunning, pool, refresh])

  const pageCount = Math.max(1, Math.ceil(total / query.pageSize))
  const currentPage = Math.min(query.page, pageCount)
  const allVisibleSelected = accounts.length > 0 && accounts.every((account) => selected.has(account.id))
  const someVisibleSelected = accounts.some((account) => selected.has(account.id))
  const inspectionProgress = inspection && inspection.total > 0
    ? Math.round((inspection.completed / inspection.total) * 100)
    : inspection?.status === "completed" ? 100 : 0

  const statCards = useMemo(() => [
    { label: "总账号数", value: stats?.total, icon: Users, tone: "text-foreground bg-muted" },
    { label: "可参与轮询", value: stats?.schedulable, icon: CheckCircle2, tone: "text-success bg-success/10" },
    { label: "限流账号", value: stats?.rateLimited, icon: ShieldAlert, tone: "text-warning bg-warning/10" },
    {
      label: "当前不可调度",
      value: stats ? Math.max(0, stats.total - stats.schedulable) : undefined,
      icon: AlertCircle,
      tone: "text-destructive bg-destructive/10",
    },
  ], [stats])

  function updateQuery(patch: Partial<OAuthPoolQuery>) {
    onQueryChange({ ...query, ...patch })
  }

  function toggleAll(checked: boolean) {
    if (batchDeleting) return
    setSelected(checked ? new Set(accounts.map((account) => account.id)) : new Set())
  }

  function toggleAccount(id: string, checked: boolean) {
    if (batchDeleting) return
    setSelected((current) => {
      const next = new Set(current)
      if (checked) next.add(id)
      else next.delete(id)
      return next
    })
  }

  async function handleHealthCheck(account: OAuthAccount) {
    if (testingIDs.has(account.id)) return
    setTestingIDs((current) => new Set(current).add(account.id))
    try {
      const checked = await checkOAuthAccount(pool, account.id)
      toast.success(checked.success ? "测活完成，账号可参与轮询" : "测活完成，账号当前不可参与轮询")
      refresh()
    } catch (caught) {
      toast.error(safeErrorMessage(caught, "账号测活失败，请查看后端状态。"))
      refresh()
    } finally {
      setTestingIDs((current) => {
        const next = new Set(current)
        next.delete(account.id)
        return next
      })
    }
  }

  async function handleQuotaQuery(account: OAuthAccount) {
    if (quotaLoadingIDs.has(account.id)) return
    setQuotaLoadingIDs((current) => new Set(current).add(account.id))
    try {
      const updated = await queryOAuthAccountQuota(pool, account.id)
      setAccounts((current) => current.map((item) => item.id === account.id ? updated : item))
      toast.success("额度与用量已更新")
    } catch (caught) {
      toast.error(safeErrorMessage(caught, "额度查询失败，请稍后重试。"))
    } finally {
      setQuotaLoadingIDs((current) => {
        const next = new Set(current)
        next.delete(account.id)
        return next
      })
    }
  }

  async function handleDelete(account: OAuthAccount) {
    const accepted = await confirm({
      title: "删除 OAuth 账号？",
      description: `将删除 ${maskIdentifier(account.maskedIdentifier)}。删除后该账号会立即退出号池轮询。`,
      confirmLabel: "确认删除",
      destructive: true,
    })
    if (!accepted) return

    setDeletingIDs((current) => new Set(current).add(account.id))
    try {
      await deleteOAuthAccount(pool, account.id)
      toast.success("账号已删除")
      if (accounts.length === 1 && query.page > 1) updateQuery({ page: query.page - 1 })
      else refresh()
    } catch (caught) {
      toast.error(safeErrorMessage(caught, "账号删除失败，请稍后重试。"))
    } finally {
      setDeletingIDs((current) => {
        const next = new Set(current)
        next.delete(account.id)
        return next
      })
    }
  }

  async function handleBatchDelete() {
    const ids = [...selected]
    if (ids.length === 0 || batchDeleting) return
    const accepted = await confirm({
      title: `删除已选中的 ${ids.length} 个账号？`,
      description: "删除后这些账号会立即退出号池轮询，失败项会保留并显示汇总结果。",
      confirmLabel: "批量删除",
      destructive: true,
    })
    if (!accepted) return

    setBatchDeleting(true)
    setBatchDeleteFeedback(null)
    try {
      const result = await batchDeleteOAuthAccounts(pool, ids)
      const failures = result.failures ?? []
      const failedIDs = new Set(failures.map((failure) => failure.id))
      setSelected(new Set(ids.filter((id) => failedIDs.has(id))))
      setBatchDeleteFeedback({
        success: result.success,
        failed: result.failed,
        failures,
      })
      if (result.failed > 0) toast.warning(`批量删除完成：成功 ${result.success}，失败 ${result.failed}`)
      else toast.success(`已删除 ${result.success} 个账号`)
      if (result.success >= accounts.length && query.page > 1) updateQuery({ page: query.page - 1 })
      else refresh()
    } catch (caught) {
      toast.error(safeErrorMessage(caught, "批量删除失败，请稍后重试。"))
    } finally {
      setBatchDeleting(false)
    }
  }

  async function handleInspection() {
    if (inspectionRunning) return
    setInspectionRunning(true)
    setInspection({
      id: "",
      status: "running",
      total: stats?.total ?? 0,
      completed: 0,
      succeeded: 0,
      alive: 0,
      limited: 0,
      dead: 0,
      cooling: 0,
      failed: 0,
    })
    try {
      const job = await startOAuthInspection(pool)
      setInspection(job)
      const running = job.status === "queued" || job.status === "running"
      setInspectionRunning(running)
      if (!running) {
        toast.success(`巡检完成：存活 ${job.alive}，限流 ${job.limited}，死亡 ${job.dead}，失败 ${job.failed}`)
        refresh()
      }
    } catch (caught) {
      setInspectionRunning(false)
      setInspection((current) => current ? { ...current, status: "failed", error: safeErrorMessage(caught, "批量巡检提交失败，请稍后重试。") } : null)
      toast.error(safeErrorMessage(caught, "批量巡检提交失败，请稍后重试。"))
    }
  }

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {statCards.map((item) => {
          const Icon = item.icon
          return (
            <Card key={item.label} className="gap-3 rounded-lg py-4 shadow-none">
              <CardContent className="flex items-center justify-between gap-3 px-4">
                <div>
                  <p className="text-xs text-muted-foreground">{item.label}</p>
                  <p className="mt-1 text-xl font-semibold text-foreground">{item.value ?? "-"}</p>
                </div>
                <span className={cn("flex size-9 items-center justify-center rounded-md", item.tone)}>
                  <Icon className="size-4" />
                </span>
              </CardContent>
            </Card>
          )
        })}
      </div>

      <PoolAvailability stats={stats} />

      {statsError ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>号池统计加载失败</AlertTitle>
          <AlertDescription>{statsError}</AlertDescription>
        </Alert>
      ) : null}

      {inspection ? (
        <Alert>
          {inspectionRunning ? <Loader2 className="animate-spin" /> : <Activity />}
          <AlertTitle>{inspectionRunning ? "号池巡检进行中" : inspection.status === "completed" ? "号池巡检已完成" : "号池巡检状态"}</AlertTitle>
          <AlertDescription className="gap-2">
            <div className="flex w-full flex-wrap items-center justify-between gap-2">
              <span>
                已处理 {inspection.completed}/{inspection.total}，存活 {inspection.alive}，限流 {inspection.limited}，死亡 {inspection.dead}，冷却 {inspection.cooling}，失败 {inspection.failed}
              </span>
              <span>{inspectionProgress}%</span>
            </div>
            <Progress value={inspectionProgress} />
            {inspection.currentAccount ? (
              <p>当前账号：{maskIdentifier(inspection.currentAccount)}</p>
            ) : null}
            {inspection.error ? (
              <p className="break-words text-destructive">{redactSensitiveText(inspection.error)}</p>
            ) : null}
          </AlertDescription>
        </Alert>
      ) : null}

      <Card className="gap-0 rounded-lg py-0 shadow-none">
        <CardHeader className="gap-3 border-b px-4 py-4 sm:px-5">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div>
              <CardTitle className="text-sm">账号管理</CardTitle>
              <p className="mt-1 text-xs text-muted-foreground">
                只有后端标记为可调度的存活账号会进入 API Key 号池轮询。
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" size="sm" onClick={refresh} disabled={loading}>
                <RefreshCw className={cn(loading && "animate-spin")} />
                刷新
              </Button>
              <Button variant="outline" size="sm" onClick={handleInspection} disabled={inspectionRunning || loading}>
                {inspectionRunning ? <Loader2 className="animate-spin" /> : <Activity />}
                {inspectionRunning ? "巡检中" : "批量巡检"}
              </Button>
              <Button size="sm" onClick={() => setImportOpen(true)}>
                <FileUp />
                JSON 导入
              </Button>
            </div>
          </div>

          <div className="flex flex-col gap-3 border-t pt-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex max-w-full gap-1 overflow-x-auto rounded-md bg-muted p-1">
              {FILTERS.map((filter) => (
                <Button
                  key={filter.value}
                  type="button"
                  size="sm"
                  variant={query.status === filter.value ? "secondary" : "ghost"}
                  className="h-7 min-w-max px-3 text-xs"
                  onClick={() => updateQuery({ status: filter.value, page: 1 })}
                >
                  {filter.label}
                  <span className="text-[10px] text-muted-foreground">{filterCount(filter.value, stats)}</span>
                </Button>
              ))}
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Select
                value={String(query.pageSize)}
                onValueChange={(value) => updateQuery({ pageSize: Number(value) as OAuthPoolQuery["pageSize"], page: 1 })}
              >
                <SelectTrigger size="sm" className="w-28">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {[10, 50, 100, 200].map((size) => (
                    <SelectItem key={size} value={String(size)}>{size} 条/页</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {!loading && accounts.length > 0 ? (
            <div
              className={cn(
                "flex flex-col gap-3 rounded-md border px-3 py-2.5 sm:flex-row sm:items-center sm:justify-between",
                selected.size > 0 ? "border-primary/30 bg-primary/5" : "bg-muted/15",
              )}
            >
              <label className="flex cursor-pointer items-center gap-2 text-sm font-medium text-foreground">
                <Checkbox
                  aria-label="选择当前页全部账号"
                  checked={allVisibleSelected ? true : someVisibleSelected ? "indeterminate" : false}
                  disabled={batchDeleting}
                  onCheckedChange={(checked) => toggleAll(checked === true)}
                />
                <span>选择本页</span>
                <span className="text-xs font-normal text-muted-foreground">共 {accounts.length} 个账号</span>
              </label>
              <div className="flex flex-wrap items-center gap-2">
                {selected.size > 0 ? (
                  <Badge variant="secondary" className="h-7 px-2.5">已选 {selected.size} 个</Badge>
                ) : (
                  <span className="text-xs text-muted-foreground">选择账号后可批量删除</span>
                )}
                {selected.size > 0 ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelected(new Set())}
                    disabled={batchDeleting}
                  >
                    <X />
                    清空选择
                  </Button>
                ) : null}
                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  onClick={handleBatchDelete}
                  disabled={selected.size === 0 || batchDeleting}
                >
                  {batchDeleting ? <Loader2 className="animate-spin" /> : <Trash2 />}
                  {batchDeleting ? "正在删除" : `批量删除${selected.size > 0 ? ` (${selected.size})` : ""}`}
                </Button>
              </div>
            </div>
          ) : null}
        </CardHeader>

        <CardContent className="px-0">
          {batchDeleteFeedback ? (
            <div className="border-b px-4 py-3 sm:px-5">
              <Alert
                variant={batchDeleteFeedback.failed > 0 ? "destructive" : "default"}
                className={batchDeleteFeedback.failed === 0 ? "border-success/25 bg-success/5" : undefined}
              >
                {batchDeleteFeedback.failed > 0 ? <AlertCircle /> : <CheckCircle2 className="text-success" />}
                <AlertTitle>
                  批量删除完成：成功 {batchDeleteFeedback.success} 个，失败 {batchDeleteFeedback.failed} 个
                </AlertTitle>
                <AlertDescription className="gap-2">
                  {batchDeleteFeedback.failures.length > 0 ? (
                    <ul className="space-y-1 text-xs">
                      {batchDeleteFeedback.failures.slice(0, 3).map((failure) => (
                        <li key={failure.id} className="break-words">
                          {maskIdentifier(failure.id)}：{redactSensitiveText(failure.reason)}
                        </li>
                      ))}
                      {batchDeleteFeedback.failures.length > 3 ? (
                        <li>另有 {batchDeleteFeedback.failures.length - 3} 个失败项未展开。</li>
                      ) : null}
                    </ul>
                  ) : batchDeleteFeedback.failed > 0 ? (
                    <p className="text-xs">后端未返回失败账号明细，请刷新后重试仍存在的账号。</p>
                  ) : (
                    <p className="text-xs">已刷新账号列表和号池统计。</p>
                  )}
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="w-fit"
                    onClick={() => setBatchDeleteFeedback(null)}
                  >
                    <X />
                    关闭结果
                  </Button>
                </AlertDescription>
              </Alert>
            </div>
          ) : null}
          {error ? (
            <ErrorState message={error} onRetry={refresh} />
          ) : loading ? (
            <LoadingState />
          ) : accounts.length === 0 ? (
            <EmptyState filtered={query.status !== "all"} onImport={() => setImportOpen(true)} />
          ) : (
            <>
              <div className="hidden md:block">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10 pl-4">
                        <Checkbox
                          aria-label="选择当前页全部账号"
                          checked={allVisibleSelected ? true : someVisibleSelected ? "indeterminate" : false}
                          disabled={batchDeleting}
                          onCheckedChange={(checked) => toggleAll(checked === true)}
                        />
                      </TableHead>
                      <TableHead>账号</TableHead>
                      <TableHead>状态</TableHead>
                      <TableHead>用量 / 额度</TableHead>
                      <TableHead>最近测活</TableHead>
                      <TableHead>最近错误</TableHead>
                      <TableHead>参与轮询</TableHead>
                      <TableHead className="pr-4 text-right">操作</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {accounts.map((account) => (
                      <AccountTableRow
                        key={account.id}
                        account={account}
                        selected={selected.has(account.id)}
                        testing={testingIDs.has(account.id)}
                        quotaLoading={quotaLoadingIDs.has(account.id)}
                        deleting={deletingIDs.has(account.id)}
                        selectionDisabled={batchDeleting}
                        onSelect={(checked) => toggleAccount(account.id, checked)}
                        onTest={() => handleHealthCheck(account)}
                        onQuota={() => handleQuotaQuery(account)}
                        onDelete={() => handleDelete(account)}
                      />
                    ))}
                  </TableBody>
                </Table>
              </div>

              <div className="divide-y md:hidden">
                {accounts.map((account) => (
                  <AccountMobileCard
                    key={account.id}
                    account={account}
                    selected={selected.has(account.id)}
                    testing={testingIDs.has(account.id)}
                    quotaLoading={quotaLoadingIDs.has(account.id)}
                    deleting={deletingIDs.has(account.id)}
                    selectionDisabled={batchDeleting}
                    onSelect={(checked) => toggleAccount(account.id, checked)}
                    onTest={() => handleHealthCheck(account)}
                    onQuota={() => handleQuotaQuery(account)}
                    onDelete={() => handleDelete(account)}
                  />
                ))}
              </div>
            </>
          )}
        </CardContent>

        {!loading && !error && total > 0 ? (
          <div className="flex flex-col gap-3 border-t px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-xs text-muted-foreground">
              第 {currentPage}/{pageCount} 页，共 {total} 个账号
            </p>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage <= 1}
                onClick={() => updateQuery({ page: currentPage - 1 })}
              >
                <ChevronLeft />
                上一页
              </Button>
              <Button
                variant="outline"
                size="sm"
                disabled={currentPage >= pageCount}
                onClick={() => updateQuery({ page: currentPage + 1 })}
              >
                下一页
                <ChevronRight />
              </Button>
            </div>
          </div>
        ) : null}
      </Card>

      <OAuthImportDialog
        pool={pool}
        open={importOpen}
        onOpenChange={setImportOpen}
        onImported={(result) => {
          updateQuery({ page: 1 })
          refresh()
          if (result.inspection) {
            setInspection(result.inspection)
            setInspectionRunning(result.inspection.status === "queued" || result.inspection.status === "running")
          }
        }}
      />
      {confirmDialog}
    </div>
  )
}

function PoolAvailability({ stats }: { stats: OAuthPoolStats | null }) {
  if (!stats) return null
  const unavailable = stats.schedulable === 0
  return (
    <Alert variant={unavailable ? "destructive" : "default"}>
      {unavailable ? <AlertCircle /> : <CheckCircle2 className="text-success" />}
      <AlertTitle className="flex flex-wrap items-center gap-2">
        {unavailable ? "号池当前不可用" : "号池可用"}
        <Badge variant="outline" className="h-5 bg-background px-1.5 text-[10px]">
          后端状态：{redactSensitiveText(stats.status)}
        </Badge>
      </AlertTitle>
      <AlertDescription>
        {unavailable
          ? "后端当前没有可参与 API Key 轮询的账号。死亡、限流、冷却中和临时不可调度账号均不会进入轮询。"
          : `当前有 ${stats.schedulable} 个经后端确认可调度的存活账号。限流、冷却和临时不可调度账号已排除。`}
      </AlertDescription>
    </Alert>
  )
}

interface AccountRowProps {
  account: OAuthAccount
  selected: boolean
  testing: boolean
  quotaLoading: boolean
  deleting: boolean
  selectionDisabled: boolean
  onSelect: (checked: boolean) => void
  onTest: () => void
  onQuota: () => void
  onDelete: () => void
}

function AccountTableRow(props: AccountRowProps) {
  const { account } = props
  const safeError = redactSensitiveText(account.lastError || "")
  return (
    <TableRow data-state={props.selected ? "selected" : undefined}>
      <TableCell className="pl-4">
        <Checkbox
          aria-label={`选择 ${maskIdentifier(account.maskedIdentifier)}`}
          checked={props.selected}
          disabled={props.selectionDisabled}
          onCheckedChange={(checked) => props.onSelect(checked === true)}
        />
      </TableCell>
      <TableCell>
        <AccountIdentity account={account} />
      </TableCell>
      <TableCell><AccountStatusBadge status={account.status} /></TableCell>
      <TableCell><QuotaText account={account} /></TableCell>
      <TableCell className="text-xs text-muted-foreground">{formatDateTime(account.lastCheckedAt)}</TableCell>
      <TableCell>
        <p className="max-w-56 truncate text-xs text-muted-foreground" title={safeError}>{safeError || "-"}</p>
      </TableCell>
      <TableCell><PollingBadge account={account} /></TableCell>
      <TableCell className="pr-4">
        <div className="flex justify-end gap-1">
          <Button variant="ghost" size="icon-sm" title="查询额度与用量" onClick={props.onQuota} disabled={props.testing || props.quotaLoading || props.deleting}>
            {props.quotaLoading ? <Loader2 className="animate-spin" /> : <Gauge />}
            <span className="sr-only">查询额度</span>
          </Button>
          <Button variant="ghost" size="icon-sm" title="发送 hi 流式请求测活" onClick={props.onTest} disabled={props.testing || props.quotaLoading || props.deleting}>
            {props.testing ? <Loader2 className="animate-spin" /> : <Activity />}
            <span className="sr-only">测活</span>
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            className="text-destructive hover:bg-destructive/10 hover:text-destructive"
            title="删除账号"
            onClick={props.onDelete}
            disabled={props.testing || props.quotaLoading || props.deleting}
          >
            {props.deleting ? <Loader2 className="animate-spin" /> : <Trash2 />}
            <span className="sr-only">删除</span>
          </Button>
        </div>
      </TableCell>
    </TableRow>
  )
}

function AccountMobileCard(props: AccountRowProps) {
  const { account } = props
  const safeError = redactSensitiveText(account.lastError || "")
  return (
    <article className="space-y-3 p-4">
      <div className="flex items-start gap-3">
        <Checkbox
          className="mt-1"
          aria-label={`选择 ${maskIdentifier(account.maskedIdentifier)}`}
          checked={props.selected}
          disabled={props.selectionDisabled}
          onCheckedChange={(checked) => props.onSelect(checked === true)}
        />
        <div className="min-w-0 flex-1">
          <AccountIdentity account={account} />
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <AccountStatusBadge status={account.status} />
            <PollingBadge account={account} />
          </div>
        </div>
      </div>
      <dl className="grid grid-cols-2 gap-2 text-xs">
        <InfoCell label="用量 / 额度"><QuotaText account={account} /></InfoCell>
        <InfoCell label="最近测活">{formatDateTime(account.lastCheckedAt)}</InfoCell>
      </dl>
      {safeError ? (
        <div className="rounded-md border border-destructive/15 bg-destructive/5 px-3 py-2">
          <p className="text-[11px] font-medium text-destructive">最近错误</p>
          <p className="mt-1 break-words text-xs text-muted-foreground">{safeError}</p>
        </div>
      ) : null}
      <div className="flex justify-end gap-2">
        <Button variant="outline" size="sm" onClick={props.onQuota} disabled={props.testing || props.quotaLoading || props.deleting}>
          {props.quotaLoading ? <Loader2 className="animate-spin" /> : <Gauge />}
          查询额度
        </Button>
        <Button variant="outline" size="sm" onClick={props.onTest} disabled={props.testing || props.quotaLoading || props.deleting}>
          {props.testing ? <Loader2 className="animate-spin" /> : <Activity />}
          测活
        </Button>
        <Button variant="destructive" size="sm" onClick={props.onDelete} disabled={props.testing || props.quotaLoading || props.deleting}>
          {props.deleting ? <Loader2 className="animate-spin" /> : <Trash2 />}
          删除
        </Button>
      </div>
    </article>
  )
}

function AccountIdentity({ account }: { account: OAuthAccount }) {
  return (
    <div className="min-w-0">
      <p className="max-w-52 truncate text-sm font-medium text-foreground">{redactSensitiveText(account.displayName)}</p>
      <div className="mt-1 flex max-w-64 flex-wrap items-center gap-1.5">
        <span className="truncate font-mono text-[11px] text-muted-foreground">{maskIdentifier(account.maskedIdentifier)}</span>
        <Badge variant="outline" className="h-5 px-1.5 text-[10px]">{account.sourceFormat}</Badge>
      </div>
    </div>
  )
}

function AccountStatusBadge({ status }: { status: OAuthAccountStatus }) {
  const config: Record<OAuthAccountStatus, { label: string; className: string; icon: typeof CheckCircle2 }> = {
    unchecked: { label: "待测活", className: "border-border bg-muted text-muted-foreground", icon: AlertCircle },
    alive: { label: "存活", className: "border-success/20 bg-success/10 text-success", icon: CheckCircle2 },
    rate_limited: { label: "限流", className: "border-warning/20 bg-warning/10 text-warning", icon: ShieldAlert },
    dead: { label: "死亡", className: "border-destructive/20 bg-destructive/10 text-destructive", icon: XCircle },
    cooling: { label: "冷却中", className: "border-warning/20 bg-warning/10 text-warning", icon: Clock3 },
    temporary_unavailable: { label: "临时不可调度", className: "border-warning/20 bg-warning/10 text-warning", icon: AlertCircle },
    checking: { label: "检测中", className: "border-primary/20 bg-primary/10 text-primary", icon: Loader2 },
    unknown: { label: "待确认", className: "border-border bg-muted text-muted-foreground", icon: AlertCircle },
  }
  const item = config[status]
  const Icon = item.icon
  return (
    <Badge variant="outline" className={cn("gap-1", item.className)}>
      <Icon className={cn(status === "checking" && "animate-spin")} />
      {item.label}
    </Badge>
  )
}

function PollingBadge({ account }: { account: OAuthAccount }) {
  return account.schedulable ? (
    <Badge variant="outline" className="border-success/20 bg-success/10 text-success">可参与</Badge>
  ) : (
    <Badge
      variant="outline"
      className="max-w-40 border-border bg-muted text-muted-foreground"
      title={account.schedulableReason ? redactSensitiveText(account.schedulableReason) : "后端未标记为可调度"}
    >
      不参与
    </Badge>
  )
}

function QuotaText({ account }: { account: OAuthAccount }) {
  const quota = account.quota
  if (!quota) return <span className="text-xs text-muted-foreground">后端未提供</span>
  if (quota.display) return <span className="text-xs text-foreground">{redactSensitiveText(quota.display)}</span>
  const unit = quota.unit ? ` ${quota.unit}` : ""
  if (Number.isFinite(quota.remaining)) return <span className="text-xs text-foreground">剩余 {quota.remaining}{unit}</span>
  if (Number.isFinite(quota.used) && Number.isFinite(quota.limit)) {
    return <span className="text-xs text-foreground">{quota.used} / {quota.limit}{unit}</span>
  }
  return <span className="text-xs text-muted-foreground">后端未提供</span>
}

function InfoCell({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="rounded-md border bg-muted/15 px-3 py-2">
      <dt className="text-[11px] text-muted-foreground">{label}</dt>
      <dd className="mt-1 text-foreground">{children}</dd>
    </div>
  )
}

function LoadingState() {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center gap-3 text-muted-foreground">
      <Loader2 className="size-6 animate-spin" />
      <p className="text-sm">正在读取后端账号状态...</p>
    </div>
  )
}

function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center gap-3 px-4 text-center">
      <AlertCircle className="size-7 text-destructive" />
      <div>
        <p className="text-sm font-medium text-foreground">账号数据加载失败</p>
        <p className="mt-1 max-w-lg text-xs text-muted-foreground">{message}</p>
      </div>
      <Button variant="outline" size="sm" onClick={onRetry}><RefreshCw />重新加载</Button>
    </div>
  )
}

function EmptyState({ filtered, onImport }: { filtered: boolean; onImport: () => void }) {
  return (
    <div className="flex min-h-64 flex-col items-center justify-center gap-3 px-4 text-center">
      {filtered ? <SearchX className="size-7 text-muted-foreground" /> : <Users className="size-7 text-muted-foreground" />}
      <div>
        <p className="text-sm font-medium text-foreground">{filtered ? "没有符合筛选条件的账号" : "号池中还没有账号"}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          {filtered ? "切换状态筛选查看其他账号。" : "通过 JSON 导入账号，导入后仍需等待实际测活确认。"}
        </p>
      </div>
      {!filtered ? <Button size="sm" onClick={onImport}><FileUp />JSON 导入</Button> : null}
    </div>
  )
}

function filterCount(filter: OAuthAccountFilter, stats: OAuthPoolStats | null): number | null {
  if (!stats) return null
  if (filter === "alive") return stats.alive >= 0 ? stats.alive : null
  if (filter === "rate_limited") return stats.rateLimited
  if (filter === "dead") return stats.dead >= 0 ? stats.dead : null
  return stats.total
}

function formatDateTime(value?: string): string {
  if (!value) return "从未测活"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "时间未知"
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date)
}

function safeErrorMessage(caught: unknown, fallback: string): string {
  const message = caught instanceof Error ? caught.message : fallback
  return redactSensitiveText(message) || fallback
}
