import { useMemo, useState } from "react"
import { useNavigate } from "react-router-dom"
import { Activity, Github, Home, LogIn, LogOut, RefreshCw, Settings } from "lucide-react"
import { Button } from "@/components/ui/button"
import { ThemeToggle } from "@/components/theme-toggle"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import { useAuth } from "@/lib/auth-context"
import { useTriggerRefresh } from "@/lib/refresh-context"
import { useChannels } from "@/lib/queries"
import { relativeTime } from "@/lib/format"
import type { AppVersion } from "@/lib/api-types"
import { PROJECT_REPOSITORY_URL, projectReleaseURL } from "@/lib/project-links"

interface MonitorHeaderProps {
  appTitle: string
  versionInfo: AppVersion | null
  checkingVersion: boolean
  onCheckVersion: () => void
}

function formatVersion(version?: string | null) {
  const value = version?.trim()
  if (!value) return null
  return value.toLowerCase().startsWith("v") ? value : `v${value}`
}

export function MonitorHeader({ appTitle, versionInfo, checkingVersion, onCheckVersion }: MonitorHeaderProps) {
  const navigate = useNavigate()
  const { username, authDisabled, logout } = useAuth()
  const refresh = useTriggerRefresh()
  const channels = useChannels()
  const [syncing, setSyncing] = useState(false)

  const lastCollectedAt = useMemo(() => {
    const list = channels.data ?? []
    let best: string | null = null
    let bestT = -Infinity
    for (const channel of list) {
      if (!channel.last_balance_at) continue
      const time = new Date(channel.last_balance_at).getTime()
      if (Number.isFinite(time) && time > bestT) {
        bestT = time
        best = channel.last_balance_at
      }
    }
    return best
  }, [channels.data])

  function handleRefresh() {
    setSyncing(true)
    refresh()
    window.setTimeout(() => setSyncing(false), 800)
  }

  return (
    <header className="sticky top-0 z-30 border-b border-border/80 bg-background/95 backdrop-blur-xl">
      <div className="flex h-14 items-center gap-3 px-3 sm:px-5 lg:px-7 xl:px-9">
        <div className="flex min-w-0 items-center gap-2.5 lg:hidden">
          <div className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-foreground text-background">
            <Activity className="size-4" strokeWidth={2.4} />
          </div>
          <div className="min-w-0">
            <p className="truncate text-sm font-semibold leading-4 text-foreground">{appTitle}</p>
            <div className="mt-0.5 flex items-center gap-1 text-[10px] leading-3">
              {versionInfo?.version ? (
                <a
                  href={projectReleaseURL(versionInfo.version)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-muted-foreground underline-offset-2 hover:text-foreground hover:underline"
                  title={`打开 ${formatVersion(versionInfo.version)} 发布页`}
                >
                  {formatVersion(versionInfo.version)}
                </a>
              ) : (
                <span className="text-muted-foreground">版本加载中</span>
              )}
              <button
                type="button"
                className="flex size-3.5 items-center justify-center rounded text-muted-foreground hover:bg-muted hover:text-foreground disabled:cursor-wait"
                onClick={onCheckVersion}
                disabled={checkingVersion}
                title="检测更新"
                aria-label="检测应用更新"
              >
                <RefreshCw className={cn("size-2.5", checkingVersion && "animate-spin")} />
              </button>
            </div>
          </div>
        </div>

        <div className="ml-auto flex shrink-0 items-center gap-1.5">
          <span className="mr-1 hidden text-xs text-muted-foreground lg:inline">
            上次采集 <span className="font-medium text-foreground">{relativeTime(lastCollectedAt)}</span>
          </span>

          <Tooltip delayDuration={200}>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="icon-sm"
                onClick={handleRefresh}
                disabled={syncing}
                className="border-border bg-background text-foreground hover:bg-muted"
                aria-label="刷新视图"
              >
                <RefreshCw className={cn("size-3.5", syncing && "animate-spin")} />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">刷新视图</TooltipContent>
          </Tooltip>

          <Tooltip delayDuration={200}>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="icon-sm"
                onClick={() => navigate(username ? "/dashboard" : "/")}
                className="hidden border-border bg-background text-foreground hover:bg-muted sm:inline-flex"
                aria-label="主页"
              >
                <Home className="size-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">主页</TooltipContent>
          </Tooltip>

          <Tooltip delayDuration={200}>
            <TooltipTrigger asChild>
              <Button
                variant="outline"
                size="icon-sm"
                onClick={() => navigate("/settings")}
                className="hidden border-border bg-background text-foreground hover:bg-muted sm:inline-flex"
                aria-label="系统设置"
              >
                <Settings className="size-3.5" />
              </Button>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">系统设置</TooltipContent>
          </Tooltip>

          <Tooltip delayDuration={200}>
            <TooltipTrigger asChild>
              <Button
                asChild
                variant="outline"
                size="icon-sm"
                className="hidden border-border bg-background text-foreground hover:bg-muted sm:inline-flex"
              >
                <a
                  href={PROJECT_REPOSITORY_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="打开 GitHub 项目"
                >
                  <Github className="size-3.5" />
                </a>
              </Button>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">GitHub · daiyibo123/upstream-ops</TooltipContent>
          </Tooltip>

          <Tooltip delayDuration={200}>
            <TooltipTrigger asChild>
              <ThemeToggle className="border-border bg-background text-foreground hover:bg-muted" />
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">切换主题</TooltipContent>
          </Tooltip>

          {authDisabled ? (
            <Tooltip delayDuration={200}>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon-sm"
                  onClick={() => navigate("/login")}
                  className="border-border bg-background text-foreground hover:bg-muted"
                  aria-label="登录"
                >
                  <LogIn className="size-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="text-xs">登录</TooltipContent>
            </Tooltip>
          ) : (
            <Tooltip delayDuration={200}>
              <TooltipTrigger asChild>
                <Button
                  variant="outline"
                  size="icon-sm"
                  onClick={logout}
                  className="border-border bg-background text-foreground hover:bg-muted"
                  aria-label="退出登录"
                >
                  <LogOut className="size-3.5" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="bottom" className="text-xs">
                {username ? `${username} · 退出登录` : "退出登录"}
              </TooltipContent>
            </Tooltip>
          )}
        </div>
      </div>
    </header>
  )
}
