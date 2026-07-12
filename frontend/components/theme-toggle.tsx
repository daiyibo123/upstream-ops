"use client"

import { useEffect, useState } from "react"
import { Moon, Sun } from "lucide-react"
import { useTheme } from "next-themes"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

export function ThemeToggle({ className }: { className?: string }) {
  const { theme, resolvedTheme, setTheme } = useTheme()
  const [mounted, setMounted] = useState(false)

  useEffect(() => setMounted(true), [])

  const current = mounted ? (theme === "system" ? resolvedTheme : theme) : "light"
  const dark = current === "dark"

  return (
    <Button
      type="button"
      variant="outline"
      size="icon"
      onClick={() => setTheme(dark ? "light" : "dark")}
      className={cn("size-8", className)}
      aria-label="切换主题"
      title={dark ? "深色模式 · 点击切换浅色" : "浅色模式 · 点击切换深色"}
    >
      {dark ? <Moon className="size-3.5" /> : <Sun className="size-3.5" />}
    </Button>
  )
}
