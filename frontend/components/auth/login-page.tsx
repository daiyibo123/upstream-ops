"use client"

import { useEffect, useState, type FormEvent } from "react"
import { Link, useNavigate } from "react-router-dom"
import { Activity, ArrowRight, Eye, EyeOff, KeyRound, LockKeyhole, ShieldCheck, UserRound } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Button } from "@/components/ui/button"
import { ThemeToggle } from "@/components/theme-toggle"
import { useAuth } from "@/lib/auth-context"
import { useAppVersion } from "@/lib/queries"
import type { ApiError } from "@/lib/api"

const highlights = [
  { icon: Activity, title: "实时调度", description: "根据状态、优先级和倍率选择合适的上游。" },
  { icon: ShieldCheck, title: "隔离与保护", description: "失败渠道自动冷却，恢复后再进入调度池。" },
  { icon: KeyRound, title: "统一入口", description: "集中管理渠道、调用 Key 与使用记录。" },
]

export function LoginPage() {
  const { login } = useAuth()
  const navigate = useNavigate()
  const appVersion = useAppVersion()
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [passwordVisible, setPasswordVisible] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const appTitle = appVersion.data?.title?.trim() || "AI Gateway"

  useEffect(() => {
    document.title = `${appTitle} · 登录`
  }, [appTitle])

  async function handleSubmit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    setError(null)
    setSubmitting(true)
    try {
      await login(username.trim(), password)
      navigate("/dashboard", { replace: true })
    } catch (err) {
      const apiError = err as ApiError
      if (apiError.status === 401) {
        setError("账号或密码错误，请重新输入。")
      } else {
        setError(apiError.message || "登录失败，请稍后重试。")
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <main className="app-page px-4 py-4 sm:px-6 sm:py-5 lg:px-8">
      <div className="app-ambient" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-linear-to-r from-transparent via-brand/40 to-transparent" />

      <div className="relative mx-auto flex min-h-[calc(100vh-2.5rem)] max-w-6xl flex-col justify-between gap-10">
        <header className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <Link to="/" className="group inline-flex items-center gap-3" aria-label={`返回 ${appTitle} 首页`}>
            <span className="flex size-10 items-center justify-center rounded-xl bg-brand/10 text-brand ring-1 ring-brand/20 transition-transform group-hover:scale-105">
              <Activity className="size-5" />
            </span>
            <span>
              <span className="block text-sm font-semibold tracking-tight text-foreground">{appTitle}</span>
              <span className="block text-xs text-muted-foreground">AI 调度网关</span>
            </span>
          </Link>
          <div className="flex w-full items-center justify-between gap-2 sm:w-auto sm:justify-end">
            <ThemeToggle className="border-border bg-background/80 text-foreground hover:bg-muted" />
            <Button asChild variant="ghost" className="text-muted-foreground hover:bg-muted hover:text-foreground">
              <Link to="/">
                返回首页
                <ArrowRight className="size-4" />
              </Link>
            </Button>
          </div>
        </header>

        <section className="grid flex-1 items-center gap-10 py-4 lg:grid-cols-[1.1fr_0.9fr] lg:gap-16">
          <div className="hidden max-w-xl lg:block">
            <span className="inline-flex items-center gap-2 rounded-full border border-brand/20 bg-brand/10 px-3 py-1 text-xs font-medium text-brand">
              <span className="size-1.5 rounded-full bg-brand" />
              安全进入控制台
            </span>
            <h1 className="mt-6 text-5xl font-semibold leading-[1.08] tracking-tight text-foreground">
              让每一次调用，
              <span className="block text-brand">都有更好的路径。</span>
            </h1>
            <p className="mt-6 max-w-lg text-base leading-7 text-muted-foreground">
              登录后统一查看渠道状态、调度表现和使用记录，在一个控制台完成日常运维。
            </p>
            <div className="mt-10 space-y-4">
              {highlights.map(({ icon: Icon, title, description }) => (
                <div key={title} className="app-card flex gap-4 rounded-xl p-4">
                  <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-brand/10 text-brand">
                    <Icon className="size-4" />
                  </span>
                  <span>
                    <span className="block text-sm font-medium text-foreground">{title}</span>
                    <span className="mt-1 block text-xs leading-5 text-muted-foreground">{description}</span>
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="mx-auto w-full max-w-md">
            <div className="app-card rounded-2xl p-1">
              <div className="rounded-[0.9rem] border border-border bg-card/95 p-5 sm:p-7">
                <div className="mb-7">
                  <span className="mb-4 flex size-11 items-center justify-center rounded-xl bg-brand/10 text-brand ring-1 ring-brand/20 lg:hidden">
                    <LockKeyhole className="size-5" />
                  </span>
                  <p className="text-2xl font-semibold tracking-tight text-foreground">欢迎回来</p>
                  <p className="mt-2 text-sm leading-6 text-muted-foreground">登录 {appTitle} 控制台，继续管理你的上游与调度。</p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-5">
                  <div className="space-y-2">
                    <Label htmlFor="username" className="text-sm text-foreground">账号</Label>
                    <div className="relative">
                      <UserRound className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        id="username"
                        name="username"
                        autoComplete="username"
                        value={username}
                        onChange={(event) => setUsername(event.target.value)}
                        placeholder="输入管理员账号"
                        className="h-11 bg-background pl-10 text-foreground placeholder:text-muted-foreground"
                        required
                        disabled={submitting}
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <Label htmlFor="password" className="text-sm text-foreground">密码</Label>
                      <span className="text-[11px] text-muted-foreground">支持显示或隐藏</span>
                    </div>
                    <div className="relative">
                      <LockKeyhole className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
                      <Input
                        id="password"
                        name="password"
                        type={passwordVisible ? "text" : "password"}
                        autoComplete="current-password"
                        value={password}
                        onChange={(event) => setPassword(event.target.value)}
                        placeholder="输入密码"
                        className="h-11 bg-background px-10 text-foreground placeholder:text-muted-foreground"
                        required
                        disabled={submitting}
                      />
                      <button
                        type="button"
                        className="absolute right-1.5 top-1/2 flex size-8 -translate-y-1/2 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50"
                        onClick={() => setPasswordVisible((visible) => !visible)}
                        aria-label={passwordVisible ? "隐藏密码" : "显示密码"}
                        title={passwordVisible ? "隐藏密码" : "显示密码"}
                      >
                        {passwordVisible ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
                      </button>
                    </div>
                  </div>

                  {error ? (
                    <div className="soft-danger rounded-lg border px-3 py-2.5 text-sm" role="alert">
                      {error}
                    </div>
                  ) : null}

                  <Button
                    type="submit"
                    className="h-11 w-full text-sm font-semibold"
                    disabled={submitting}
                  >
                    {submitting ? "正在验证身份…" : "登录控制台"}
                    {!submitting ? <ArrowRight className="size-4" /> : null}
                  </Button>
                </form>
              </div>
            </div>
            <p className="mt-4 text-center text-xs text-muted-foreground">请使用已配置的管理员账号登录。</p>
          </div>
        </section>

        <footer className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
          <span>安全认证 · 仅限授权访问</span>
          <span>{appVersion.data?.version ? `版本 ${appVersion.data.version}` : "正在连接服务"}</span>
        </footer>
      </div>
    </main>
  )
}
