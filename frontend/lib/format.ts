/**
 * 各种小格式化工具：相对时间、金额、倍率箭头等。
 */

const RELATIVE_THRESHOLDS: Array<{ limit: number; unit: string; divisor: number }> = [
  { limit: 60, unit: "秒前", divisor: 1 },
  { limit: 60 * 60, unit: "分钟前", divisor: 60 },
  { limit: 24 * 60 * 60, unit: "小时前", divisor: 60 * 60 },
  { limit: 30 * 24 * 60 * 60, unit: "天前", divisor: 24 * 60 * 60 },
]

/** 把 ISO 时间转成"X 分钟前"等相对描述。 */
export function relativeTime(iso?: string | null, now: Date = new Date()): string {
  if (!iso) return "—"
  const t = new Date(iso).getTime()
  if (!Number.isFinite(t)) return "—"
  const diff = Math.max(0, Math.floor((now.getTime() - t) / 1000))
  if (diff < 5) return "刚刚"
  for (const r of RELATIVE_THRESHOLDS) {
    if (diff < r.limit) {
      return `${Math.floor(diff / r.divisor)} ${r.unit}`
    }
  }
  return new Date(iso).toLocaleDateString("zh-CN")
}

/** 把 ISO 时间转成简短的"HH:MM"。 */
export function shortTime(iso?: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })
}

export function dateTime(iso?: string | null): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleString("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  })
}

/** 货币格式：$1,234.56。 */
export function money(value: number | null | undefined, opts?: { precise?: boolean }) {
  if (value == null || !Number.isFinite(value)) return "—"
  return (
    "$" +
    value.toLocaleString("en-US", {
      minimumFractionDigits: opts?.precise ? 4 : 2,
      maximumFractionDigits: opts?.precise ? 4 : 2,
    })
  )
}

export function decimal(value: number | null | undefined, digits = 2) {
  if (value == null || !Number.isFinite(value)) return "—"
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  })
}

export function formatTokens(value: number | null | undefined) {
  const n = Number(value ?? 0)
  if (!Number.isFinite(n) || n <= 0) return "0M"
  if (n >= 1_000_000_000) return `${trimFixed(n / 1_000_000_000, 2)}B`
  const millions = n / 1_000_000
  if (millions < 0.001) return "<0.001M"
  if (millions < 1) return `${trimFixed(millions, 3)}M`
  if (millions < 10) return `${trimFixed(millions, 2)}M`
  if (millions < 100) return `${trimFixed(millions, 1)}M`
  return `${trimFixed(millions, 0)}M`
}

function trimFixed(value: number, digits: number) {
  return value.toFixed(digits).replace(/\.0+$|(\.\d*[1-9])0+$/, "$1")
}

export function formatRatio(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return "—"
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  })
}

export function formatPercent(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return "—"
  const pct = value * 100
  return `${trimFixed(pct, digits)}%`
}

/** 把倍率渲染成"1.20 → 1.50"。 */
export function ratioArrow(from: number | null | undefined, to: number) {
  return `${formatRatio(from)} → ${formatRatio(to)}`
}

/** 计算变化方向 / 百分比文案，比如 "+25.0%"。 */
export function ratioDelta(from: number | null | undefined, to: number) {
  if (from == null || from === 0) {
    return { direction: "up" as const, pct: "新增" }
  }
  const pct = ((to - from) / Math.abs(from)) * 100
  const direction = pct >= 0 ? ("up" as const) : ("down" as const)
  return { direction, pct: `${pct >= 0 ? "+" : ""}${pct.toFixed(1)}%` }
}

/** 把 ChannelType "newapi"/"sub2api" 转成显示名 / 角标颜色。 */
export function channelTypeLabel(t: string) {
  switch (t) {
    case "newapi":
      return "NewAPI"
    case "sub2api":
      return "Sub2API"
    default:
      return t
  }
}
