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
  const appTitle = appVersion.data?.title?.trim() || "UpstreamOps"

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
    <main className="relative min-h-screen overflow-hidden bg-[#071018] px-4 py-5 text-white sm:px-6 lg:px-8">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_12%_12%,rgba(34,211,238,0.22),transparent_30%),radial-gradient(circle_at_85%_85%,rgba(16,185,129,0.15),transparent_27%),linear-gradient(135deg,#071018_0%,#0f172a_56%,#111827_100%)]" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-linear-to-r from-transparent via-cyan-200/60 to-transparent" />

      <div className="relative mx-auto flex min-h-[calc(100vh-2.5rem)] max-w-6xl flex-col justify-between gap-10">
        <header className="flex items-center justify-between gap-4">
          <Link to="/" className="group inline-flex items-center gap-3" aria-label={`返回 ${appTitle} 首页`}>
            <span className="flex size-10 items-center justify-center rounded-xl bg-cyan-300/15 text-cyan-100 ring-1 ring-cyan-200/25 transition-transform group-hover:scale-105">
              <Activity className="size-5" />
            </span>
            <span>
              <span className="block text-sm font-semibold tracking-tight text-white">{appTitle}</span>
              <span className="block text-xs text-slate-400">AI 调度网关</span>
            </span>
          </Link>
          <div className="flex items-center gap-2">
            <ThemeToggle className="border-white/15 bg-white/5 text-white hover:bg-white/10 hover:text-white" />
            <Button asChild variant="ghost" className="text-slate-300 hover:bg-white/10 hover:text-white">
              <Link to="/">
                返回首页
                <ArrowRight className="size-4" />
              </Link>
            </Button>
          </div>
        </header>

        <section className="grid flex-1 items-center gap-10 py-4 lg:grid-cols-[1.1fr_0.9fr] lg:gap-16">
          <div className="hidden max-w-xl lg:block">
            <span className="inline-flex items-center gap-2 rounded-full border border-cyan-200/15 bg-cyan-300/10 px-3 py-1 text-xs font-medium text-cyan-100">
              <span className="size-1.5 rounded-full bg-cyan-200 shadow-[0_0_10px_rgba(165,243,252,0.9)]" />
              安全进入控制台
            </span>
            <h1 className="mt-6 text-5xl font-semibold leading-[1.08] tracking-tight text-white">
              让每一次调用，
              <span className="block bg-linear-to-r from-cyan-200 via-sky-100 to-emerald-200 bg-clip-text text-transparent">都有更好的路径。</span>
            </h1>
            <p className="mt-6 max-w-lg text-base leading-7 text-slate-300">
              登录后统一查看渠道状态、调度表现和使用记录，在一个控制台完成日常运维。
            </p>
            <div className="mt-10 space-y-4">
              {highlights.map(({ icon: Icon, title, description }) => (
                <div key={title} className="flex gap-4 rounded-xl border border-white/10 bg-white/[0.045] p-4 backdrop-blur-sm">
                  <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-white/[0.07] text-cyan-200">
                    <Icon className="size-4" />
                  </span>
                  <span>
                    <span className="block text-sm font-medium text-white">{title}</span>
                    <span className="mt-1 block text-xs leading-5 text-slate-400">{description}</span>
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="mx-auto w-full max-w-md">
            <div className="rounded-2xl border border-white/12 bg-slate-950/50 p-1 shadow-2xl shadow-cyan-950/35 backdrop-blur-xl">
              <div className="rounded-[0.9rem] border border-white/8 bg-slate-900/80 p-5 sm:p-7">
                <div className="mb-7">
                  <span className="mb-4 flex size-11 items-center justify-center rounded-xl bg-cyan-300/15 text-cyan-100 ring-1 ring-cyan-200/20 lg:hidden">
                    <LockKeyhole className="size-5" />
                  </span>
                  <p className="text-2xl font-semibold tracking-tight text-white">欢迎回来</p>
                  <p className="mt-2 text-sm leading-6 text-slate-400">登录 {appTitle} 控制台，继续管理你的上游与调度。</p>
                </div>

                <form onSubmit={handleSubmit} className="space-y-5">
                  <div className="space-y-2">
                    <Label htmlFor="username" className="text-sm text-slate-200">账号</Label>
                    <div className="relative">
                      <UserRound className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
                      <Input
                        id="username"
                        name="username"
                        autoComplete="username"
                        value={username}
                        onChange={(event) => setUsername(event.target.value)}
                        placeholder="输入管理员账号"
                        className="h-11 border-white/10 bg-white/[0.055] pl-10 text-white placeholder:text-slate-500 focus-visible:border-cyan-300/50 focus-visible:ring-cyan-300/20"
                        required
                        disabled={submitting}
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between gap-3">
                      <Label htmlFor="password" className="text-sm text-slate-200">密码</Label>
                      <span className="text-[11px] text-slate-500">支持显示或隐藏</span>
                    </div>
                    <div className="relative">
                      <LockKeyhole className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-slate-500" />
                      <Input
                        id="password"
                        name="password"
                        type={passwordVisible ? "text" : "password"}
                        autoComplete="current-password"
                        value={password}
                        onChange={(event) => setPassword(event.target.value)}
                        placeholder="输入密码"
                        className="h-11 border-white/10 bg-white/[0.055] px-10 text-white placeholder:text-slate-500 focus-visible:border-cyan-300/50 focus-visible:ring-cyan-300/20"
                        required
                        disabled={submitting}
                      />
                      <button
                        type="button"
                        className="absolute right-1.5 top-1/2 flex size-8 -translate-y-1/2 items-center justify-center rounded-md text-slate-400 transition-colors hover:bg-white/10 hover:text-cyan-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cyan-300/50"
                        onClick={() => setPasswordVisible((visible) => !visible)}
                        aria-label={passwordVisible ? "隐藏密码" : "显示密码"}
                        title={passwordVisible ? "隐藏密码" : "显示密码"}
                      >
                        {passwordVisible ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
                      </button>
                    </div>
                  </div>

                  {error ? (
                    <div className="rounded-lg border border-red-300/20 bg-red-400/10 px-3 py-2.5 text-sm text-red-100" role="alert">
                      {error}
                    </div>
                  ) : null}

                  <Button
                    type="submit"
                    className="h-11 w-full bg-cyan-300 text-sm font-semibold text-slate-950 shadow-lg shadow-cyan-950/30 hover:bg-cyan-200"
                    disabled={submitting}
                  >
                    {submitting ? "正在验证身份…" : "登录控制台"}
                    {!submitting ? <ArrowRight className="size-4" /> : null}
                  </Button>
                </form>
              </div>
            </div>
            <p className="mt-4 text-center text-xs text-slate-500">请使用已配置的管理员账号登录。</p>
          </div>
        </section>

        <footer className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
          <span>安全认证 · 仅限授权访问</span>
          <span>{appVersion.data?.version ? `版本 ${appVersion.data.version}` : "正在连接服务"}</span>
        </footer>
      </div>
    </main>
  )
}
