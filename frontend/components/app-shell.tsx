"use client"

import { Outlet, useLocation, useNavigate } from "react-router-dom"
import { Bell, History, KeyRound, LayoutDashboard, Plus, Server, Settings, type LucideIcon } from "lucide-react"
import { MonitorHeader } from "@/components/monitor/monitor-header"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

/**
 * AppShell 是所有路由共享的外壳：顶部 header + 中间 Outlet（+ 可选底部 dock）。
 *
 * 当前 Dock 暂时隐藏 —— 单用户 / 少量数据下单页布局比拆页好。
 * 把 SHOW_DOCK 改成 true 即可恢复底部导航 + 路由跳转。
 */
interface NavItem {
  icon: LucideIcon
  label: string
  path?: string
  action?: () => void
}

const NAV_ITEMS: NavItem[] = [
  { icon: LayoutDashboard, label: "调度网关", path: "/dashboard" },
  { icon: Plus, label: "渠道", path: "/channels" },
  { icon: KeyRound, label: "创建 Key", path: "/keys" },
  { icon: Server, label: "可用渠道", path: "/gateway" },
  { icon: History, label: "使用记录", path: "/usage" },
  { icon: Bell, label: "通知渠道", path: "/notifications" },
  { icon: Settings, label: "系统设置", path: "/settings" },
]

function SideNav() {
  const navigate = useNavigate()
  const location = useLocation()

  return (
    <aside className="sticky top-14 hidden h-[calc(100vh-3.5rem)] w-52 shrink-0 border-r border-border bg-background/95 px-3 py-4 lg:block">
      <nav className="space-y-1">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon
          const active = item.path != null && location.pathname === item.path
          return (
            <Button
              key={item.label}
              variant="ghost"
              className={cn(
                "h-9 w-full justify-start gap-2 px-2 text-xs font-medium",
                active ? "bg-muted text-foreground" : "text-muted-foreground hover:text-foreground",
              )}
              onClick={() => {
                if (item.action) item.action()
                else if (item.path) navigate(item.path)
              }}
            >
              <Icon className="size-4" />
              <span>{item.label}</span>
            </Button>
          )
        })}
      </nav>
    </aside>
  )
}

/**
 * MobileNav 是手机端底部固定导航条（lg 以下显示，lg 及以上隐藏，交给左侧 SideNav）。
 * 5 个入口平铺，保证手机端也能在各页之间切换，不用手动改 URL。
 */
function MobileNav() {
  const navigate = useNavigate()
  const location = useLocation()
  return (
    <nav className="fixed inset-x-0 bottom-0 z-40 border-t border-border bg-background/95 backdrop-blur lg:hidden">
      <div className="mx-auto flex max-w-lg items-stretch justify-around px-1 py-1">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon
          const active = item.path != null && location.pathname === item.path
          return (
            <button
              key={item.label}
              type="button"
              className={cn(
                "flex flex-1 flex-col items-center gap-0.5 rounded-md px-1 py-1.5 text-[10px] font-medium transition-colors",
                active ? "text-foreground" : "text-muted-foreground",
              )}
              onClick={() => {
                if (item.action) item.action()
                else if (item.path) navigate(item.path)
              }}
            >
              <Icon className={cn("size-5", active && "text-primary")} />
              <span className="leading-none">{item.label}</span>
            </button>
          )
        })}
      </div>
    </nav>
  )
}

export function AppShell() {
  return (
    <div className="min-h-screen bg-background">
      <MonitorHeader />
      <div className="mx-auto flex max-w-420">
        <SideNav />
        <main className="min-w-0 flex-1 space-y-4 px-3 py-3 pb-24 sm:space-y-5 sm:px-5 sm:py-5 lg:pb-5">
          <Outlet />
        </main>
      </div>
      <MobileNav />
    </div>
  )
}
