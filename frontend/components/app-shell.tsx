"use client"

import { Outlet, useLocation, useNavigate } from "react-router-dom"
import { Bell, LayoutDashboard, Plus, Settings, ShieldCheck, type LucideIcon } from "lucide-react"
import { MonitorHeader } from "@/components/monitor/monitor-header"
import { DockBar } from "@/components/monitor/dock-bar"
import { Button } from "@/components/ui/button"
import { useAddChannel } from "@/lib/add-channel-context"
import { cn } from "@/lib/utils"

/**
 * AppShell 是所有路由共享的外壳：顶部 header + 中间 Outlet（+ 可选底部 dock）。
 *
 * 当前 Dock 暂时隐藏 —— 单用户 / 少量数据下单页布局比拆页好。
 * 把 SHOW_DOCK 改成 true 即可恢复底部导航 + 路由跳转。
 */
const SHOW_DOCK = false

interface NavItem {
  icon: LucideIcon
  label: string
  path?: string
  action?: () => void
}

function SideNav() {
  const navigate = useNavigate()
  const location = useLocation()
  const { openAdd } = useAddChannel()
  const items: NavItem[] = [
    { icon: LayoutDashboard, label: "监控面板", path: "/" },
    { icon: Plus, label: "添加渠道", action: openAdd },
    { icon: ShieldCheck, label: "打码平台", path: "/captcha" },
    { icon: Bell, label: "通知渠道", path: "/notifications" },
    { icon: Settings, label: "系统设置", path: "/settings" },
  ]

  return (
    <aside className="sticky top-14 hidden h-[calc(100vh-3.5rem)] w-52 shrink-0 border-r border-border bg-background/95 px-3 py-4 lg:block">
      <nav className="space-y-1">
        {items.map((item) => {
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

export function AppShell() {
  return (
    <div className="min-h-screen bg-background">
      <MonitorHeader />
      <div className="mx-auto flex max-w-420">
        <SideNav />
        <main
          className={
            SHOW_DOCK
              ? "min-w-0 flex-1 space-y-4 px-3 py-3 pb-24 sm:space-y-5 sm:px-5 sm:py-5"
              : "min-w-0 flex-1 space-y-4 px-3 py-3 sm:space-y-5 sm:px-5 sm:py-5"
          }
        >
          <Outlet />
        </main>
      </div>
      {SHOW_DOCK ? <DockBar /> : null}
    </div>
  )
}
