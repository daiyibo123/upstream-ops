import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

interface PageHeaderProps {
  icon: ReactNode
  title: string
  description: string
  meta?: ReactNode
  actions?: ReactNode
  className?: string
}

export function PageHeader({ icon, title, description, meta, actions, className }: PageHeaderProps) {
  return (
    <header className={cn("flex flex-col gap-4 border-b border-border/70 pb-5 sm:flex-row sm:items-end sm:justify-between", className)}>
      <div className="flex min-w-0 items-start gap-3">
        <span className="mt-0.5 flex size-10 shrink-0 items-center justify-center rounded-lg border border-brand/15 bg-brand/8 text-brand shadow-xs">
          {icon}
        </span>
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold text-foreground">{title}</h1>
            {meta}
          </div>
          <p className="mt-1 max-w-3xl text-sm leading-5 text-muted-foreground">{description}</p>
        </div>
      </div>
      {actions ? <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  )
}
