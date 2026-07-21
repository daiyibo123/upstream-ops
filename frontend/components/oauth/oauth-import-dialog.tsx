import { useRef, useState } from "react"
import { AlertCircle, CheckCircle2, FileJson, Loader2, Upload, XCircle } from "lucide-react"
import { toast } from "sonner"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { importOAuthAccounts } from "@/components/oauth/oauth-api"
import type { OAuthImportResult, OAuthPoolKind } from "@/components/oauth/types"
import { cn } from "@/lib/utils"

const MAX_FILE_SIZE = 10 * 1024 * 1024

interface OAuthImportDialogProps {
  pool: OAuthPoolKind
  open: boolean
  onOpenChange: (open: boolean) => void
  onImported: (result: OAuthImportResult) => void
}

export function OAuthImportDialog({
  pool,
  open,
  onOpenChange,
  onImported,
}: OAuthImportDialogProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const [file, setFile] = useState<File | null>(null)
  const [error, setError] = useState("")
  const [importing, setImporting] = useState(false)
  const [result, setResult] = useState<OAuthImportResult | null>(null)

  const formats = pool === "chatgpt"
    ? "支持 sub2api、CPA / CLIProxyAPI JSON，系统自动识别格式。"
    : "支持 sub2api、CPA / CLIProxyAPI、SSO JSON，系统自动识别格式。"

  function reset() {
    setFile(null)
    setError("")
    setResult(null)
    if (inputRef.current) inputRef.current.value = ""
  }

  function handleOpenChange(next: boolean) {
    if (importing) return
    onOpenChange(next)
    if (!next) reset()
  }

  function validateFile(nextFile: File): string {
    if (!nextFile.name.toLowerCase().endsWith(".json")) return "仅支持选择 .json 文件。"
    if (nextFile.size === 0) return "文件为空，请选择包含账号信息的 JSON 文件。"
    if (nextFile.size > MAX_FILE_SIZE) return "文件超过 10 MB 限制，请拆分后重试。"
    return ""
  }

  function handleFile(nextFile?: File) {
    if (!nextFile) return
    const validationError = validateFile(nextFile)
    setFile(validationError ? null : nextFile)
    setError(validationError)
    setResult(null)
  }

  async function handleImport() {
    if (!file || importing) return
    const validationError = validateFile(file)
    if (validationError) {
      setError(validationError)
      return
    }

    setImporting(true)
    setError("")
    try {
      const raw = await file.text()
      if (!raw.trim()) throw new Error("文件为空，请选择包含账号信息的 JSON 文件。")

      let payload: unknown
      try {
        payload = JSON.parse(raw)
      } catch {
        throw new Error("JSON 格式错误，请检查括号、引号和逗号后重试。")
      }

      const importResult = await importOAuthAccounts(pool, file.name, payload)
      setResult(importResult)
      onImported(importResult)
      if (importResult.failed > 0 || importResult.duplicate > 0) {
        toast.warning(`导入完成：新增 ${importResult.created}，更新 ${importResult.updated}，失败 ${importResult.failed}`)
      } else {
        toast.success(`导入完成：新增 ${importResult.created}，更新 ${importResult.updated}，等待后端测活确认`)
      }
    } catch (caught) {
      const message = safeErrorMessage(caught)
      setError(message)
      toast.error(message)
    } finally {
      setImporting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <FileJson className="size-5 text-primary" />
            导入 {pool === "chatgpt" ? "ChatGPT" : "Grok"} OAuth 账号
          </DialogTitle>
          <DialogDescription>
            {formats} 导入成功只代表数据已接收，账号是否参与轮询以实际测活结果为准。
          </DialogDescription>
        </DialogHeader>

        <input
          ref={inputRef}
          className="sr-only"
          type="file"
          accept="application/json,.json"
          disabled={importing}
          onChange={(event) => handleFile(event.target.files?.[0])}
        />

        <button
          type="button"
          className={cn(
            "flex min-h-32 w-full flex-col items-center justify-center gap-3 rounded-lg border border-dashed px-4 py-6 text-center transition-colors",
            "border-border bg-muted/20 hover:border-primary/50 hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
            importing && "pointer-events-none opacity-60",
          )}
          onClick={() => inputRef.current?.click()}
          onDragOver={(event) => event.preventDefault()}
          onDrop={(event) => {
            event.preventDefault()
            handleFile(event.dataTransfer.files?.[0])
          }}
        >
          <span className="flex size-10 items-center justify-center rounded-lg border bg-background text-muted-foreground">
            <Upload className="size-5" />
          </span>
          <span className="text-sm font-medium text-foreground">
            {file ? file.name : "选择或拖入 JSON 文件"}
          </span>
          <span className="text-xs text-muted-foreground">
            {file ? formatBytes(file.size) : "单个文件最大 10 MB，不会在页面展示文件中的凭据"}
          </span>
        </button>

        {error ? (
          <Alert variant="destructive">
            <AlertCircle />
            <AlertTitle>导入失败</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        {result ? <ImportResultPanel result={result} /> : null}

        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)} disabled={importing}>
            {result ? "完成" : "取消"}
          </Button>
          <Button onClick={handleImport} disabled={!file || importing}>
            {importing ? <Loader2 className="animate-spin" /> : <Upload />}
            {importing ? "导入中" : "开始导入"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function ImportResultPanel({ result }: { result: OAuthImportResult }) {
  return (
    <section className="space-y-3 rounded-lg border bg-muted/15 p-3">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-5">
        <ResultMetric label="识别" value={result.total} />
        <ResultMetric label="新增" value={result.created} tone="success" />
        <ResultMetric label="更新" value={result.updated} tone="success" />
        <ResultMetric label="成功" value={result.success} tone="success" />
        <ResultMetric label="失败" value={result.failed} tone="danger" />
      </div>

      {result.items.length > 0 ? (
        <div className="max-h-52 space-y-1 overflow-y-auto pr-1">
          {result.items.map((item, index) => (
            <div
              key={`${item.reference}-${index}`}
              className="flex items-start gap-2 rounded-md border bg-background px-3 py-2 text-xs"
            >
              {item.status === "success" ? (
                <CheckCircle2 className="mt-0.5 size-3.5 shrink-0 text-success" />
              ) : item.status === "duplicate" ? (
                <AlertCircle className="mt-0.5 size-3.5 shrink-0 text-warning" />
              ) : (
                <XCircle className="mt-0.5 size-3.5 shrink-0 text-destructive" />
              )}
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="truncate font-medium text-foreground">{maskIdentifier(item.reference)}</span>
                  <Badge
                    variant="outline"
                    className={cn(
                      "h-5 px-1.5 text-[10px]",
                      item.status === "success" && "border-success/20 bg-success/10 text-success",
                      item.status === "duplicate" && "border-warning/20 bg-warning/10 text-warning",
                      item.status === "failed" && "border-destructive/20 bg-destructive/10 text-destructive",
                    )}
                  >
                    {item.status === "success"
                      ? item.action === "updated" ? "已更新" : item.action === "created" ? "已新增" : "成功"
                      : item.status === "duplicate" ? "重复" : "失败"}
                  </Badge>
                </div>
                {item.reason ? (
                  <p className="mt-1 break-words text-muted-foreground">{redactSensitiveText(item.reason)}</p>
                ) : null}
              </div>
            </div>
          ))}
        </div>
      ) : null}
    </section>
  )
}

function ResultMetric({
  label,
  value,
  tone = "default",
}: {
  label: string
  value: number
  tone?: "default" | "success" | "warning" | "danger"
}) {
  return (
    <div className="rounded-md border bg-background px-3 py-2">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p
        className={cn(
          "mt-0.5 text-base font-semibold",
          tone === "default" && "text-foreground",
          tone === "success" && "text-success",
          tone === "warning" && "text-warning",
          tone === "danger" && "text-destructive",
        )}
      >
        {value}
      </p>
    </div>
  )
}

export function redactSensitiveText(value: string): string {
  return value
    .replace(/(["'](?:access[_-]?token|refresh[_-]?token|id[_-]?token|session[_-]?token|sso[_-]?token|auth[_-]?token|authorization|api[_-]?key|client[_-]?secret|cookie|set[_-]?cookie|password|secret|credential)["']\s*:\s*["'])[^"']+/gi, "$1[已隐藏]")
    .replace(/(bearer\s+)[a-z0-9._~+\/-]+/gi, "$1[已隐藏]")
    .replace(/((?:access[_-]?token|refresh[_-]?token|id[_-]?token|session[_-]?token|sso[_-]?token|auth[_-]?token|authorization|api[_-]?key|client[_-]?secret|cookie|set[_-]?cookie|password|secret|credential)\s*[:=]\s*)[^\s,;]+/gi, "$1[已隐藏]")
    .replace(/[A-Za-z0-9_-]{18,}\.[A-Za-z0-9_-]{18,}\.[A-Za-z0-9_-]{18,}/g, "[JWT 已隐藏]")
    .slice(0, 800)
}

export function maskIdentifier(value: string): string {
  const safe = redactSensitiveText(value.trim())
  if (!safe) return "未命名账号"
  if (safe.includes("*") || safe.includes("已隐藏")) return safe
  const emailMatch = safe.match(/^([^@]+)@(.+)$/)
  if (emailMatch) {
    const local = emailMatch[1]
    return `${local.slice(0, Math.min(2, local.length))}***@${emailMatch[2]}`
  }
  if (safe.length <= 8) return `${safe.slice(0, 2)}***`
  return `${safe.slice(0, 4)}****${safe.slice(-4)}`
}

function safeErrorMessage(caught: unknown): string {
  const message = caught instanceof Error ? caught.message : "导入接口请求失败，请稍后重试。"
  return redactSensitiveText(message) || "导入接口请求失败，请稍后重试。"
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}
