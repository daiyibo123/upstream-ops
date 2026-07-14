"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"
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
import type { Channel, GatewayHealthResult, UpstreamGroupKey } from "@/lib/api-types"

type ManualClientFormat = "openai" | "claude" | "grok"

interface ManualDraft {
  sourceMode: "new" | "existing"
  channelId: string
  channelName: string
  siteUrl: string
  groupName: string
  groupDescription: string
  key: string
  ratio: string
  clientFormat: ManualClientFormat
  charity: boolean
  priority: string
}

function createDefaultManualDraft(): ManualDraft {
  return {
    sourceMode: "new",
    channelId: "",
    channelName: "",
    siteUrl: "",
    groupName: "",
    groupDescription: "",
    key: "",
    ratio: "1",
    clientFormat: "openai",
    charity: false,
    priority: "0",
  }
}

function sanitizeDecimal(value: string) {
  return value.replace(/[^\d.]/g, "").replace(/(\..*)\./g, "$1")
}

function normalizeManualClientFormat(value?: string | null): ManualClientFormat {
  switch ((value ?? "").toLowerCase()) {
    case "claude":
      return "claude"
    case "grok":
    case "xai":
      return "grok"
    default:
      return "openai"
  }
}

export function ManualGroupKeyDialog({
  open,
  onOpenChange,
  channels,
  fixedChannel,
  initialClientFormat,
  onCreated,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  channels: Channel[]
  fixedChannel?: Channel | null
  initialClientFormat?: string | null
  onCreated?: (group: UpstreamGroupKey) => void | Promise<void>
}) {
  const [draft, setDraft] = useState<ManualDraft>(() => createDefaultManualDraft())
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    if (!open) {
      setDraft(createDefaultManualDraft())
      return
    }
    if (fixedChannel) {
      setDraft({
        ...createDefaultManualDraft(),
        sourceMode: "existing",
        channelId: String(fixedChannel.id),
        clientFormat: normalizeManualClientFormat(initialClientFormat),
      })
    }
  }, [open, fixedChannel, initialClientFormat])

  async function submit() {
    if (!draft.groupName.trim()) {
      toast.error("请填写分组名")
      return
    }
    if (!draft.key.trim()) {
      toast.error("请填写对应 Key")
      return
    }
    if (draft.sourceMode === "existing" && !draft.channelId) {
      toast.error("请选择已有渠道")
      return
    }
    if (draft.sourceMode === "new" && !draft.siteUrl.trim()) {
      toast.error("请填写上游 URL")
      return
    }

    setBusy(true)
    try {
      const created = await apiFetch<UpstreamGroupKey>("/gateway/group-keys/manual", {
        method: "POST",
        body: JSON.stringify({
          ...(fixedChannel || draft.sourceMode === "existing"
            ? { channel_id: Number(draft.channelId) }
            : {
                channel_name: draft.channelName.trim() || undefined,
                site_url: draft.siteUrl.trim() || undefined,
              }),
          group_name: draft.groupName.trim(),
          group_description: draft.groupDescription.trim() || undefined,
          key: draft.key.trim(),
          ratio: Number(draft.ratio) || 0,
          client_format: draft.clientFormat,
          request_mode: "auto",
          charity: draft.charity,
          priority: Math.max(0, Math.floor(Number(draft.priority) || 0)),
        }),
      })

      try {
        const health = await apiFetch<GatewayHealthResult["items"][number]>(
          `/gateway/group-keys/${created.id}/test`,
          { method: "POST" },
        )
        if (health.status === "alive") {
          toast.success("手动渠道已添加，并已通过测活")
        } else {
          toast.warning(`手动渠道已保存，但测活未通过：${health.error || "上游无有效响应"}`)
        }
      } catch (error) {
        const err = error as Error
        toast.warning(`手动渠道已保存，但自动测活失败：${err.message || "请稍后单独测活"}`)
      }

      await onCreated?.(created)
      onOpenChange(false)
    } catch (error) {
      const err = error as Error
      toast.error(err.message || "手动添加渠道失败")
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>{fixedChannel ? "添加上游 Key" : "手动添加渠道"}</DialogTitle>
          <DialogDescription>
            {fixedChannel
              ? `为 ${fixedChannel.name} 添加可调度的上游 Key；分组、倍率和 Key 会直接写入可用渠道。`
              : "给无法登录、只能拿到 Key 的上游手动创建分组。URL、分组、倍率和对应 Key 会直接写入可用渠道。"}
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-3 sm:grid-cols-2">
          {fixedChannel ? (
            <div className="space-y-1.5">
              <Label>目标渠道</Label>
              <div className="flex h-10 items-center rounded-md border border-border bg-muted/20 px-3 text-sm text-foreground">
                {fixedChannel.name}
              </div>
            </div>
          ) : (
            <>
              <div className="space-y-1.5">
                <Label>接入方式</Label>
                <Select
                  value={draft.sourceMode}
                  onValueChange={(value) =>
                    setDraft((prev) => ({
                      ...prev,
                      sourceMode: value === "existing" ? "existing" : "new",
                    }))
                  }
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="new">新建渠道</SelectItem>
                    {channels.length > 0 ? <SelectItem value="existing">选择已有渠道</SelectItem> : null}
                  </SelectContent>
                </Select>
              </div>
              {draft.sourceMode === "existing" ? (
                <div className="space-y-1.5">
                  <Label>已有渠道</Label>
                  <Select
                    value={draft.channelId}
                    onValueChange={(value) => setDraft((prev) => ({ ...prev, channelId: value }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="选择渠道" />
                    </SelectTrigger>
                    <SelectContent>
                      {channels.map((channel) => (
                        <SelectItem key={channel.id} value={String(channel.id)}>
                          {channel.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              ) : (
                <>
              <div className="space-y-1.5">
                <Label htmlFor="manual-channel-name">渠道名</Label>
                <Input
                  id="manual-channel-name"
                  value={draft.channelName}
                  onChange={(event) => setDraft((prev) => ({ ...prev, channelName: event.target.value }))}
                  placeholder="例如：某公益中转"
                />
              </div>
              <div className="space-y-1.5 sm:col-span-2">
                <Label htmlFor="manual-site-url">URL *</Label>
                <Input
                  id="manual-site-url"
                  value={draft.siteUrl}
                  onChange={(event) => setDraft((prev) => ({ ...prev, siteUrl: event.target.value }))}
                  placeholder="https://..."
                />
              </div>
                </>
              )}
            </>
          )}
          <div className="space-y-1.5">
            <Label htmlFor="manual-group-name">分组 *</Label>
            <Input
              id="manual-group-name"
              value={draft.groupName}
              onChange={(event) => setDraft((prev) => ({ ...prev, groupName: event.target.value }))}
              placeholder="例如：default / claude / grok"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="manual-ratio">倍率</Label>
            <Input
              id="manual-ratio"
              value={draft.ratio}
              inputMode="decimal"
              onChange={(event) => setDraft((prev) => ({ ...prev, ratio: sanitizeDecimal(event.target.value) }))}
              placeholder="1"
            />
          </div>
          <div className="space-y-1.5 sm:col-span-2">
            <Label htmlFor="manual-key">对应 Key *</Label>
            <Input
              id="manual-key"
              value={draft.key}
              onChange={(event) => setDraft((prev) => ({ ...prev, key: event.target.value }))}
              placeholder="上游 API Key"
            />
          </div>
          <div className="space-y-1.5">
            <Label>客户端格式</Label>
            <Select
              value={draft.clientFormat}
              onValueChange={(value) =>
                setDraft((prev) => ({ ...prev, clientFormat: normalizeManualClientFormat(value) }))
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="openai">OpenAI</SelectItem>
                <SelectItem value="claude">Claude</SelectItem>
                <SelectItem value="grok">Grok (xAI)</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label>上游接口（自动检测）</Label>
            <div className="flex h-10 items-center rounded-md border border-border bg-muted/20 px-3 text-xs text-muted-foreground">
              {draft.clientFormat === "openai"
                ? "自动检测 Responses / Chat"
                : draft.clientFormat === "claude"
                  ? "自动使用 Claude Messages"
                  : "自动使用 Grok Chat"}
            </div>
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="manual-priority">调度优先级</Label>
            <Input
              id="manual-priority"
              value={draft.priority}
              inputMode="numeric"
              onChange={(event) =>
                setDraft((prev) => ({ ...prev, priority: event.target.value.replace(/[^\d]/g, "") }))
              }
              placeholder="0"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="manual-group-desc">分组说明</Label>
            <Input
              id="manual-group-desc"
              value={draft.groupDescription}
              onChange={(event) => setDraft((prev) => ({ ...prev, groupDescription: event.target.value }))}
              placeholder="可选，例如：手动粘贴的公益线路"
            />
          </div>
          <div className="flex items-center justify-between gap-3 rounded-md border border-border bg-muted/20 px-3 py-2 sm:col-span-2">
            <div>
              <p className="text-sm font-medium text-foreground">是否公益</p>
              <p className="text-xs text-muted-foreground">标记为公益分组后会优先参与公益调度展示</p>
            </div>
            <Switch checked={draft.charity} onCheckedChange={(checked) => setDraft((prev) => ({ ...prev, charity: checked }))} />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={busy}>
            取消
          </Button>
          <Button onClick={() => void submit()} disabled={busy}>
            {busy ? <Loader2 className="mr-1.5 size-3.5 animate-spin" /> : null}
            添加
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
