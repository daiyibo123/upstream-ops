"use client"

import { useEffect, useState } from "react"
import { HeartHandshake, Loader2 } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { apiFetch } from "@/lib/api"
import { useTriggerRefresh } from "@/lib/refresh-context"
import type { GatewayKey, PublicGatewayKey } from "@/lib/api-types"

export function PublicKeyConfigCard() {
  const refresh = useTriggerRefresh()
  const [open, setOpen] = useState(false)
  const [keys, setKeys] = useState<GatewayKey[]>([])
  const [selectedKeyId, setSelectedKeyId] = useState("")
  const [enabled, setEnabled] = useState(true)
  const [name, setName] = useState("")
  const [password, setPassword] = useState("")
  const [passwordTouched, setPasswordTouched] = useState(false)
  const [passwordRequired, setPasswordRequired] = useState(false)
  const [passwordHint, setPasswordHint] = useState("")
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoading(true)
    Promise.all([
      apiFetch<GatewayKey[]>("/gateway/keys"),
      apiFetch<PublicGatewayKey | null>("/gateway/public-key"),
    ])
      .then(([list, current]) => {
        if (cancelled) return
        const available = Array.isArray(list) ? list.filter((key) => key.enabled !== false) : []
        setKeys(available)
        setSelectedKeyId(current?.id ? String(current.id) : "")
        setEnabled(current?.enabled ?? true)
        setName(current?.name ?? "")
        setPasswordRequired(Boolean(current?.password_required))
        setPasswordHint(current?.password_hint ?? "")
        setPassword("")
        setPasswordTouched(false)
      })
      .catch((error: Error) => !cancelled && toast.error(error.message || "加载公益 Key 配置失败"))
      .finally(() => !cancelled && setLoading(false))
    return () => { cancelled = true }
  }, [open])

  async function save() {
    if (!selectedKeyId) {
      toast.error("请选择一个已创建的调用 Key")
      return
    }
    setSaving(true)
    try {
      await apiFetch<PublicGatewayKey>("/gateway/public-key", {
        method: "PUT",
        body: JSON.stringify({
          gateway_key_id: Number(selectedKeyId),
          enabled,
          name: name.trim(),
          ...(passwordTouched ? { password } : {}),
          password_hint: passwordHint.trim(),
        }),
      })
      toast.success("公益 Key 已保存，首页立即生效")
      refresh()
      setOpen(false)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存公益 Key 失败")
    } finally {
      setSaving(false)
    }
  }

  const passwordStatus = passwordTouched
    ? password
      ? "保存后将更新复制密码。"
      : passwordRequired
        ? "保存后将清空复制密码，首页可直接复制完整 Key。"
        : "保存后仍不设置复制密码。"
    : passwordRequired
      ? "当前已设置复制密码；留空保存会保持不变。"
      : "当前未设置复制密码，首页可直接复制。"

  return <>
    <Button variant="outline" size="sm" className="gap-1.5 text-xs" onClick={() => setOpen(true)}>
      <HeartHandshake className="size-3.5 text-success" />
      配置公益 Key
    </Button>
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2"><HeartHandshake className="size-4 text-success" />首页公益 Key</DialogTitle>
          <DialogDescription>公益 Key 属于调用 Key 业务数据，不写入系统设置的 config.yaml。</DialogDescription>
        </DialogHeader>
        {loading ? <div className="flex items-center gap-2 py-6 text-xs text-muted-foreground"><Loader2 className="size-4 animate-spin" />加载中…</div> : <div className="space-y-4">
          <div className="flex items-center justify-between rounded-md border border-border bg-muted/20 px-3 py-2">
            <div><p className="text-sm font-medium">在首页展示</p><p className="text-xs text-muted-foreground">关闭后首页不再提供该 Key。</p></div>
            <Switch checked={enabled} onCheckedChange={setEnabled} />
          </div>
          <div className="space-y-1.5"><Label>调用 Key</Label><Select value={selectedKeyId} onValueChange={setSelectedKeyId}><SelectTrigger><SelectValue placeholder="选择已创建的调用 Key" /></SelectTrigger><SelectContent>{keys.map((key) => <SelectItem key={key.id} value={String(key.id)}>{key.name}（{key.key_prefix}***）</SelectItem>)}</SelectContent></Select></div>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5"><Label>展示名称</Label><Input value={name} onChange={(event) => setName(event.target.value)} placeholder="公益 OpenAI Key" /></div>
            <div className="space-y-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label>复制密码</Label>
                {passwordRequired ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 px-2 text-xs text-destructive hover:text-destructive"
                    onClick={() => { setPasswordTouched(true); setPassword("") }}
                  >
                    清空密码
                  </Button>
                ) : null}
              </div>
              <Input
                type="password"
                value={password}
                onChange={(event) => { setPasswordTouched(true); setPassword(event.target.value) }}
                placeholder={passwordRequired ? "留空不修改，输入新密码可替换" : "留空表示不设密码"}
              />
              <p className="text-xs leading-5 text-muted-foreground">{passwordStatus}</p>
            </div>
            <div className="space-y-1.5 sm:col-span-2"><Label>密码提示</Label><Input value={passwordHint} onChange={(event) => setPasswordHint(event.target.value)} placeholder="例如：关注公告获取复制密码" /></div>
          </div>
        </div>}
        <DialogFooter><Button variant="outline" onClick={() => setOpen(false)} disabled={saving}>取消</Button><Button onClick={() => void save()} disabled={loading || saving}>{saving ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}保存</Button></DialogFooter>
      </DialogContent>
    </Dialog>
  </>
}
