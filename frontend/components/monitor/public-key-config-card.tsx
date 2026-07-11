"use client"

import { useEffect, useState } from "react"
import { HeartHandshake, Loader2 } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { apiFetch } from "@/lib/api"
import { useSystemConfig } from "@/lib/queries"
import { useTriggerRefresh } from "@/lib/refresh-context"
import type { GatewayKey, GatewayKeyReveal, SystemConfig, SystemPublicKeyConfig } from "@/lib/api-types"

function defaultPublicKey(cfg: SystemConfig): SystemPublicKeyConfig {
  const app = cfg.app as SystemConfig["app"] & { publicKey?: Partial<SystemPublicKeyConfig> }
  const pk = app.publicKey ?? {}
  return {
    enabled: pk.enabled ?? false,
    name: pk.name ?? "公益 Key",
    key: pk.key ?? "",
    password: pk.password ?? "",
    passwordHint: pk.passwordHint ?? "",
    expiresAt: pk.expiresAt ?? "",
  }
}

/**
 * PublicKeyConfigCard 把「首页公益 Key」配置搬到创建 Key 页：页面上一个按钮，点击弹出配置对话框。
 *
 * - 复用 settings-page 的读取与保存逻辑：读 /settings/config，只改 app.publicKey，再把完整配置 PUT 回去。
 * - 关键：公益 Key 的「key」字段必须是系统里真实存在且启用的网关 Key（后端按 hash 匹配），
 *   否则首页会显示 unavailable。所以这里改成从已创建 Key 列表里选，选中后用 reveal 接口拿明文。
 */
export function PublicKeyConfigCard() {
  const query = useSystemConfig()
  const refresh = useTriggerRefresh()
  const [open, setOpen] = useState(false)
  const [publicKey, setPublicKey] = useState<SystemPublicKeyConfig | null>(null)
  const [saving, setSaving] = useState(false)
  const [keys, setKeys] = useState<GatewayKey[]>([])
  const [keysLoading, setKeysLoading] = useState(false)
  const [selectedKeyId, setSelectedKeyId] = useState<string>("")
  const [revealing, setRevealing] = useState(false)

  useEffect(() => {
    if (query.data?.config) {
      setPublicKey(defaultPublicKey(query.data.config))
    }
  }, [query.data])

  useEffect(() => {
    if (!open) return
    let cancelled = false
    setKeysLoading(true)
    apiFetch<GatewayKey[]>("/gateway/keys")
      .then((list) => {
        if (!cancelled) setKeys(Array.isArray(list) ? list.filter((k) => k.enabled !== false) : [])
      })
      .catch((err: Error) => {
        if (!cancelled) toast.error(err.message || "加载已创建 Key 失败")
      })
      .finally(() => {
        if (!cancelled) setKeysLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open])

  async function handleSelectKey(id: string) {
    setSelectedKeyId(id)
    const key = keys.find((k) => String(k.id) === id)
    if (!key) return
    setRevealing(true)
    try {
      const res = await apiFetch<GatewayKeyReveal>(`/gateway/keys/${key.id}/reveal`, { method: "POST" })
      if (!res.key) throw new Error("没有返回完整 Key")
      setPublicKey((prev) => (prev ? { ...prev, key: res.key, name: prev.name?.trim() ? prev.name : key.name } : prev))
      toast.success(`已选择 ${key.name}`)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "获取 Key 明文失败")
      setSelectedKeyId("")
    } finally {
      setRevealing(false)
    }
  }

  async function handleSave() {
    if (!query.data?.config || !publicKey) return
    setSaving(true)
    try {
      const next: SystemConfig = {
        ...query.data.config,
        app: {
          ...query.data.config.app,
          publicKey,
        },
      }
      await apiFetch("/settings/config", {
        method: "PUT",
        body: JSON.stringify(next),
      })
      toast.success("公益 Key 配置已保存")
      query.refetch()
      refresh()
      setOpen(false)
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存失败")
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <Button variant="outline" size="sm" className="gap-1.5 text-xs" onClick={() => setOpen(true)}>
        <HeartHandshake className="size-3.5 text-success" />
        配置公益 Key
      </Button>

      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <HeartHandshake className="size-4 text-success" />
              首页公益 Key
            </DialogTitle>
            <DialogDescription>
              在公开首页展示一个可复制的公益 Key，可设置复制密码、提示词和到期时间。
            </DialogDescription>
          </DialogHeader>

          {publicKey == null ? (
            <div className="flex items-center gap-2 py-6 text-xs text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              加载配置中...
            </div>
          ) : (
            <div className="space-y-4">
              <div className="flex items-center justify-between gap-3 rounded-md border border-border bg-muted/20 px-3 py-2">
                <div>
                  <p className="text-sm font-medium text-foreground">启用首页公益 Key</p>
                  <p className="text-xs text-muted-foreground">开启后公开首页会显示公益 Key 入口，但不会直接暴露明文。</p>
                </div>
                <Switch
                  checked={publicKey.enabled}
                  onCheckedChange={(checked) => setPublicKey((prev) => (prev ? { ...prev, enabled: checked } : prev))}
                />
              </div>

              <div className="space-y-1.5">
                <Label>公益 Key</Label>
                <Select value={selectedKeyId} disabled={keysLoading || revealing} onValueChange={(v) => void handleSelectKey(v)}>
                  <SelectTrigger>
                    <SelectValue placeholder={keysLoading ? "加载中..." : "选择一个已创建的密钥"} />
                  </SelectTrigger>
                  <SelectContent>
                    {keys.length === 0 ? (
                      <SelectItem value="__none__" disabled>
                        暂无已启用的 Key，请先在下方创建
                      </SelectItem>
                    ) : (
                      keys.map((k) => (
                        <SelectItem key={k.id} value={String(k.id)}>
                          {k.name}（{k.key_prefix}***）
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
                <p className="text-[11px] text-muted-foreground">
                  {revealing ? "正在读取密钥明文..." : "选择一个已创建的密钥作为公益 Key，首页将展示它。"}
                </p>
                {publicKey.key ? (
                  <p className="text-[11px] text-success">已配置密钥（明文已隐藏，保存即生效）</p>
                ) : null}
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <Label htmlFor="pk-name">展示名称</Label>
                  <Input
                    id="pk-name"
                    value={publicKey.name}
                    placeholder="公益 OpenAI Key"
                    onChange={(e) => setPublicKey((prev) => (prev ? { ...prev, name: e.target.value } : prev))}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="pk-expires">到期时间</Label>
                  <Input
                    id="pk-expires"
                    value={publicKey.expiresAt}
                    placeholder="2026-08-01（留空不过期）"
                    onChange={(e) => setPublicKey((prev) => (prev ? { ...prev, expiresAt: e.target.value } : prev))}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="pk-password">复制密码</Label>
                  <Input
                    id="pk-password"
                    type="password"
                    value={publicKey.password}
                    placeholder="留空表示无需密码"
                    onChange={(e) => setPublicKey((prev) => (prev ? { ...prev, password: e.target.value } : prev))}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="pk-hint">密码提示词</Label>
                  <Input
                    id="pk-hint"
                    value={publicKey.passwordHint}
                    placeholder="例如：关注公告获取复制密码"
                    onChange={(e) => setPublicKey((prev) => (prev ? { ...prev, passwordHint: e.target.value } : prev))}
                  />
                </div>
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)} disabled={saving}>
              取消
            </Button>
            <Button onClick={() => void handleSave()} disabled={saving || publicKey == null}>
              {saving ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}
              保存
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
