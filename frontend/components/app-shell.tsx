"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Outlet, useLocation, useNavigate } from "react-router-dom"
import {
  ChevronLeft,
  ChevronRight,
  History,
  KeyRound,
  LayoutDashboard,
  MoreHorizontal,
  Network,
  Plus,
  RefreshCw,
  Search,
  Server,
  Settings,
  ShieldCheck,
  type LucideIcon,
} from "lucide-react"
import { MonitorHeader } from "@/components/monitor/monitor-header"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { apiFetch } from "@/lib/api"
import type { AppVersion } from "@/lib/api-types"
import { projectReleaseURL } from "@/lib/project-links"
import { useAppVersion } from "@/lib/queries"
import { cn } from "@/lib/utils"
import { toast } from "sonner"

interface NavItem {
  icon: LucideIcon
  label: string
  path: string
  keywords: string
}

const SIDEBAR_COLLAPSED_KEY = "upstream_ops_sidebar_collapsed"

const NAV_ITEMS: NavItem[] = [
  { icon: LayoutDashboard, label: "调度网关", path: "/dashboard", keywords: "首页 仪表板 dashboard 网关" },
  { icon: KeyRound, label: "API Key", path: "/keys", keywords: "密钥 key 调用" },
  { icon: Server, label: "可用渠道", path: "/gateway", keywords: "分组 上游 可用 轮询" },
  { icon: Plus, label: "上游渠道", path: "/channels", keywords: "渠道 账号 添加 upstream" },
  { icon: ShieldCheck, label: "OAuth 登录", path: "/oauth", keywords: "oauth 账号 号池 chatgpt grok 导入" },
  { icon: History, label: "使用记录", path: "/usage", keywords: "事件 日志 用量 错误 重试 使用记录" },
  { icon: Settings, label: "系统设置", path: "/settings", keywords: "设置 代理 通知 系统 config" },
]

const MOBILE_PRIMARY_PATHS = ["/dashboard", "/channels", "/oauth", "/usage"]

function matchesPath(pathname: string, path: string) {
  return pathname === path || pathname.startsWith(`${path}/`)
}

function formatVersion(version?: string | null) {
  const value = version?.trim()
  if (!value) return null
  return value.toLowerCase().startsWith("v") ? value : `v${value}`
}

interface VersionDisplayProps {
  appTitle: string
  versionInfo: AppVersion | null
  checkingVersion: boolean
  onCheckVersion: () => void
}

function SideNav({ appTitle, versionInfo, checkingVersion, onCheckVersion }: VersionDisplayProps) {
  const navigate = useNavigate()
  const location = useLocation()
  const searchRef = useRef<HTMLInputElement>(null)
  const [query, setQuery] = useState("")
  const [collapsed, setCollapsed] = useState(() => {
    try {
      return window.localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === "true"
    } catch {
      return false
    }
  })

  const filteredItems = useMemo(() => {
    const normalized = query.trim().toLowerCase()
    if (!normalized) return NAV_ITEMS
    return NAV_ITEMS.filter((item) => `${item.label} ${item.keywords}`.toLowerCase().includes(normalized))
  }, [query])

  useEffect(() => {
    try {
      window.localStorage.setItem(SIDEBAR_COLLAPSED_KEY, String(collapsed))
    } catch {
      // Local storage is optional; the sidebar still works without persistence.
    }
  }, [collapsed])

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault()
        setCollapsed(false)
        window.setTimeout(() => searchRef.current?.focus(), 0)
      }
    }
    window.addEventListener("keydown", handleKeyDown)
    return () => window.removeEventListener("keydown", handleKeyDown)
  }, [])

  function openSearch() {
    setCollapsed(false)
    window.setTimeout(() => searchRef.current?.focus(), 0)
  }

  return (
    <aside
      className={cn(
        "sticky top-0 hidden h-screen shrink-0 border-r border-sidebar-border/80 bg-sidebar/95 transition-[width] duration-200 lg:flex lg:flex-col",
        collapsed ? "w-[72px]" : "w-[248px]",
      )}
    >
      <div className={cn("flex h-18 items-center border-b border-sidebar-border/70", collapsed ? "justify-center px-2" : "gap-3 px-4")}>
        <div className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground shadow-sm">
          <Network className="size-[18px]" strokeWidth={2.2} />
        </div>
        {!collapsed ? (
          <div className="min-w-0 flex-1">
            <p className="truncate text-sm font-semibold text-sidebar-foreground" title={appTitle}>{appTitle}</p>
            <div className="mt-0.5 flex min-w-0 items-center gap-1.5 text-[11px] leading-4">
              {versionInfo?.version ? (
                <a
                  href={projectReleaseURL(versionInfo.version)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="shrink-0 font-medium text-muted-foreground underline-offset-2 transition-colors hover:text-sidebar-foreground hover:underline"
                  title={`打开 ${formatVersion(versionInfo.version)} 发布页`}
                >
                  {formatVersion(versionInfo.version)}
                </a>
              ) : (
                <span className="shrink-0 font-medium text-muted-foreground">版本加载中</span>
              )}
              <button
                type="button"
                className="flex size-4 shrink-0 items-center justify-center rounded text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-sidebar-foreground disabled:cursor-wait"
                onClick={onCheckVersion}
                disabled={checkingVersion}
                title="检测更新"
                aria-label="检测应用更新"
              >
                <RefreshCw className={cn("size-2.5", checkingVersion && "animate-spin")} />
              </button>
              {versionInfo?.update_available && versionInfo.latest_version ? (
                <a
                  href={projectReleaseURL(versionInfo.latest_version)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="min-w-0 truncate font-medium text-warning hover:underline"
                  title={`查看新版本 ${formatVersion(versionInfo.latest_version)}`}
                >
                  有更新 {formatVersion(versionInfo.latest_version)}
                </a>
              ) : null}
            </div>
          </div>
        ) : null}
      </div>

      <div className={cn("px-3 pt-3", collapsed && "px-2")}>
        {collapsed ? (
          <Button variant="ghost" size="icon" className="size-10 w-full text-muted-foreground" onClick={openSearch} title="搜索导航" aria-label="搜索导航">
            <Search className="size-4" />
          </Button>
        ) : (
          <div className="relative">
            <Search className="pointer-events-none absolute left-3 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              ref={searchRef}
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              className="h-9 border-sidebar-border/80 bg-background/80 pl-9 pr-3 text-xs shadow-none"
              placeholder="搜索功能"
              aria-label="搜索侧边栏功能"
            />
          </div>
        )}
      </div>

      <nav className={cn("min-h-0 flex-1 space-y-1 overflow-y-auto px-3 py-3", collapsed && "px-2")} aria-label="控制台导航">
        {filteredItems.map((item) => {
          const Icon = item.icon
          const active = matchesPath(location.pathname, item.path)
          return (
            <button
              key={item.path}
              type="button"
              title={collapsed ? item.label : undefined}
              aria-current={active ? "page" : undefined}
              className={cn(
                "group relative flex h-10 w-full items-center rounded-lg text-sm font-medium transition-colors",
                collapsed ? "justify-center px-0" : "gap-3 px-3",
                active
                  ? "bg-sidebar-accent text-sidebar-accent-foreground shadow-[inset_0_0_0_1px_color-mix(in_oklch,var(--sidebar-border)_75%,transparent)]"
                  : "text-muted-foreground hover:bg-sidebar-accent/65 hover:text-sidebar-foreground",
              )}
              onClick={() => navigate(item.path)}
            >
              <span className={cn(
                "flex size-7 shrink-0 items-center justify-center rounded-md transition-colors",
                active ? "bg-sidebar-primary text-sidebar-primary-foreground" : "text-muted-foreground group-hover:text-sidebar-foreground",
              )}>
                <Icon className="size-4" />
              </span>
              {!collapsed ? <span className="min-w-0 flex-1 truncate text-left">{item.label}</span> : null}
              {active && !collapsed ? <span className="size-1.5 rounded-full bg-sidebar-primary" /> : null}
            </button>
          )
        })}
        {!collapsed && filteredItems.length === 0 ? (
          <div className="rounded-lg border border-dashed border-sidebar-border px-3 py-8 text-center text-xs text-muted-foreground">
            没有匹配的功能
          </div>
        ) : null}
      </nav>

      <div className={cn("border-t border-sidebar-border/70 p-3", collapsed && "p-2")}>
        <Button
          variant="ghost"
          className={cn("h-9 w-full text-muted-foreground hover:text-sidebar-foreground", collapsed ? "px-0" : "justify-start gap-3 px-3")}
          onClick={() => setCollapsed((value) => !value)}
          title={collapsed ? "展开侧边栏" : "收起侧边栏"}
          aria-label={collapsed ? "展开侧边栏" : "收起侧边栏"}
        >
          {collapsed ? <ChevronRight className="size-4" /> : <ChevronLeft className="size-4" />}
          {!collapsed ? <span className="text-xs">收起侧边栏</span> : null}
        </Button>
      </div>
    </aside>
  )
}

function MobileNavItem({ item }: { item: NavItem }) {
  const navigate = useNavigate()
  const location = useLocation()
  const Icon = item.icon
  const active = matchesPath(location.pathname, item.path)
  return (
    <button
      type="button"
      className={cn(
        "flex min-w-0 flex-col items-center justify-center gap-1 px-1 py-1.5 text-[10px] font-medium",
        active ? "text-primary" : "text-muted-foreground",
      )}
      onClick={() => navigate(item.path)}
    >
      <Icon className="size-[18px]" />
      <span className="max-w-full truncate">{item.label}</span>
    </button>
  )
}

function MobileNav() {
  const navigate = useNavigate()
  const location = useLocation()
  const menuRef = useRef<HTMLDivElement>(null)
  const [moreOpen, setMoreOpen] = useState(false)
  const primaryItems = NAV_ITEMS.filter((item) => MOBILE_PRIMARY_PATHS.includes(item.path))
  const moreItems = NAV_ITEMS.filter((item) => !MOBILE_PRIMARY_PATHS.includes(item.path))
  const moreActive = moreItems.some((item) => matchesPath(location.pathname, item.path))

  useEffect(() => {
    if (!moreOpen) return
    const close = (event: PointerEvent) => {
      if (!menuRef.current?.contains(event.target as Node)) setMoreOpen(false)
    }
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMoreOpen(false)
    }
    document.addEventListener("pointerdown", close)
    document.addEventListener("keydown", closeOnEscape)
    return () => {
      document.removeEventListener("pointerdown", close)
      document.removeEventListener("keydown", closeOnEscape)
    }
  }, [moreOpen])

  return (
    <nav className="fixed inset-x-0 bottom-0 z-40 border-t border-border/80 bg-background/96 px-2 pb-[max(0.25rem,env(safe-area-inset-bottom))] pt-1 shadow-[0_-8px_24px_rgba(15,23,42,0.06)] backdrop-blur-xl lg:hidden" aria-label="移动端导航">
      <div className="grid h-14 grid-cols-5">
        {primaryItems.map((item) => <MobileNavItem key={item.path} item={item} />)}
        <div ref={menuRef} className="relative min-w-0">
          {moreOpen ? (
            <div className="absolute bottom-[calc(100%+0.65rem)] right-0 w-44 rounded-lg border border-border bg-popover p-1.5 text-popover-foreground shadow-lg">
              {moreItems.map((item) => {
                const Icon = item.icon
                const active = matchesPath(location.pathname, item.path)
                return (
                  <button
                    key={item.path}
                    type="button"
                    className={cn("flex h-9 w-full items-center gap-2.5 rounded-md px-2.5 text-left text-sm", active ? "bg-accent text-accent-foreground" : "hover:bg-accent hover:text-accent-foreground")}
                    onClick={() => {
                      setMoreOpen(false)
                      navigate(item.path)
                    }}
                  >
                    <Icon className="size-4" />
                    {item.label}
                  </button>
                )
              })}
            </div>
          ) : null}
          <button
            type="button"
            aria-expanded={moreOpen}
            className={cn("flex h-full w-full min-w-0 flex-col items-center justify-center gap-1 px-1 py-1.5 text-[10px] font-medium", moreActive || moreOpen ? "text-primary" : "text-muted-foreground")}
            onClick={() => setMoreOpen((value) => !value)}
          >
            <MoreHorizontal className="size-[18px]" />
            <span>更多</span>
          </button>
        </div>
      </div>
    </nav>
  )
}

export function AppShell() {
  const appVersion = useAppVersion()
  const [checkingVersion, setCheckingVersion] = useState(false)
  const appTitle = appVersion.data?.title?.trim() || "AI Gateway"

  useEffect(() => {
    document.title = appTitle
  }, [appTitle])

  async function handleCheckVersion() {
    if (checkingVersion) return
    setCheckingVersion(true)
    try {
      const result = await apiFetch<AppVersion>("/version?force=1")
      appVersion.setData(result)
      if (result.update_error) {
        toast.error(result.update_error)
      } else if (result.update_available && result.latest_version) {
        toast.warning(`发现新版本 ${formatVersion(result.latest_version)}`)
      } else {
        toast.success("当前已是最新版本")
      }
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "检测更新失败")
    } finally {
      setCheckingVersion(false)
    }
  }

  return (
    <div className="min-h-screen bg-muted/25 lg:flex">
      <SideNav
        appTitle={appTitle}
        versionInfo={appVersion.data}
        checkingVersion={checkingVersion}
        onCheckVersion={handleCheckVersion}
      />
      <div className="min-w-0 flex-1">
        <MonitorHeader
          appTitle={appTitle}
          versionInfo={appVersion.data}
          checkingVersion={checkingVersion}
          onCheckVersion={handleCheckVersion}
        />
        <main className="console-main min-w-0 flex-1 px-3 py-4 pb-24 sm:px-5 sm:py-5 lg:px-7 lg:py-6 lg:pb-8 xl:px-9">
          <div className="mx-auto w-full max-w-[1480px] space-y-5">
            <Outlet />
          </div>
        </main>
      </div>
      <MobileNav />
    </div>
  )
}
