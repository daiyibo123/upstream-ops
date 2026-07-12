import { useEffect, useState } from "react";
import { toast } from "sonner";
import {
  Bell,
  Clock3,
  Copy,
  MonitorCog,
  KeyRound,
  Network,
  PencilLine,
  Plus,
  Power,
  RefreshCw,
  Send,
  Server,
  ShieldCheck,
  Terminal,
  Trash2,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useConfirm } from "@/components/ui/confirm-dialog";
import { NotificationFormDialog } from "@/components/monitor/notification-form-dialog";
import { apiFetch } from "@/lib/api";
import { useTriggerRefresh } from "@/lib/refresh-context";
import type {
  AppVersion,
  ApplyConfigResult,
  NotificationChannel,
  NotificationChannelType,
  SystemConfig,
  SystemRestartResponse,
  SystemUpdateResponse,
  UpgradeCommand,
} from "@/lib/api-types";
import { relativeTime } from "@/lib/format";
import {
  useDashboardSummary,
  useNotificationLogs,
  useNotificationChannels,
  useAppVersion,
  useSystemConfig,
} from "@/lib/queries";
import { cn } from "@/lib/utils";

function num(v: string) {
  return Number(v || 0);
}

function withConfigDefaults(cfg: SystemConfig): SystemConfig {
  const app = cfg.app as SystemConfig["app"] & {
    publicKey?: SystemConfig["app"]["publicKey"];
  };
  const publicKey = (app.publicKey ?? {}) as Partial<
    SystemConfig["app"]["publicKey"]
  >;
  return {
    ...cfg,
    app: {
      ...app,
      publicKey: {
        enabled: publicKey.enabled ?? false,
        name: publicKey.name ?? "公益 Key",
        key: publicKey.key ?? "",
        password: publicKey.password ?? "",
        passwordHint: publicKey.passwordHint ?? "",
        expiresAt: publicKey.expiresAt ?? "",
      },
    },
  };
}

interface ProxyTestResult {
  ok: boolean;
  latency_ms: number;
  ip: string;
  provider: string;
  error?: string;
}

export default function SettingsPage() {
  const query = useSystemConfig();
  const notifications = useNotificationChannels();
  const summary = useDashboardSummary();
  const notificationLogs = useNotificationLogs(1, 10);
  const appVersion = useAppVersion();
  const refresh = useTriggerRefresh();
  const { confirm, dialog: confirmDialog } = useConfirm();
  const [form, setForm] = useState<SystemConfig | null>(null);
  const [saving, setSaving] = useState(false);
  const [applying, setApplying] = useState(false);
  const [configSavedPendingApply, setConfigSavedPendingApply] = useState(false);
  const [testingProxy, setTestingProxy] = useState(false);
  const [checkingVersion, setCheckingVersion] = useState(false);
  const [updatingSystem, setUpdatingSystem] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [copyingUpgrade, setCopyingUpgrade] = useState(false);
  const [editingNotification, setEditingNotification] =
    useState<NotificationChannel | null>(null);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [busyNotificationID, setBusyNotificationID] = useState<number | null>(
    null,
  );
  const [activeTab, setActiveTab] = useState("system");
  const [versionInfo, setVersionInfo] = useState<AppVersion | null>(null);

  useEffect(() => {
    if (query.data?.config) {
      setForm(withConfigDefaults(query.data.config));
    }
  }, [query.data]);

  useEffect(() => {
    if (appVersion.data) {
      setVersionInfo(appVersion.data);
    }
  }, [appVersion.data]);

  if (query.loading && !form) {
    return (
      <section className="text-sm text-muted-foreground">加载配置中...</section>
    );
  }

  if (query.error && !form) {
    return (
      <section className="text-sm text-destructive">{query.error}</section>
    );
  }

  if (!form) return null;

  const recentLogs = notificationLogs.data?.items ?? [];
  const lastSent = recentLogs[0]?.sent_at ?? null;
  const recentFailed = recentLogs.filter((item) => !item.success).length;

  async function handleDeleteNotification(channel: NotificationChannel) {
    const ok = await confirm({
      title: `删除通知渠道 ${channel.name}？`,
      description: "删除后该渠道将不再接收系统通知。",
      confirmLabel: "删除",
      destructive: true,
    });
    if (!ok) return;
    setBusyNotificationID(channel.id);
    try {
      await apiFetch(`/notifications/channels/${channel.id}`, {
        method: "DELETE",
      });
      toast.success(`已删除 ${channel.name}`);
      refresh();
      notifications.refetch();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "删除失败");
    } finally {
      setBusyNotificationID(null);
    }
  }

  async function handleTestNotification(channel: NotificationChannel) {
    setBusyNotificationID(channel.id);
    try {
      const res = await apiFetch<{ ok: boolean; error?: string }>(
        `/notifications/channels/${channel.id}/test`,
        {
          method: "POST",
        },
      );
      if (res.ok) {
        toast.success(`已发送测试消息到 ${channel.name}`);
      } else {
        toast.error(res.error ?? "测试失败");
      }
      refresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "测试失败");
    } finally {
      setBusyNotificationID(null);
    }
  }

  async function handleSave() {
    setSaving(true);
    try {
      await apiFetch("/settings/config", {
        method: "PUT",
        body: JSON.stringify(form),
      });
      toast.success("已写入配置文件");
      setConfigSavedPendingApply(true);
      query.refetch();
      appVersion.refetch();
      refresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function handleApply() {
    setApplying(true);
    try {
      const result = await apiFetch<ApplyConfigResult>("/settings/apply", {
        method: "POST",
      });
      toast.success(result.message);
      setConfigSavedPendingApply(false);
      query.refetch();
      refresh();
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "应用失败");
    } finally {
      setApplying(false);
    }
  }

  async function handleTestProxy() {
    setTestingProxy(true);
    try {
      const result = await apiFetch<ProxyTestResult>("/settings/proxy/test", {
        method: "POST",
        body: JSON.stringify(form?.proxy ?? {}),
      });
      if (result.ok) {
        toast.success(
          `代理可用，出口 IP ${result.ip}，延迟 ${result.latency_ms}ms`,
        );
      } else {
        toast.error(result.error ?? "代理测试失败");
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "代理测试失败");
    } finally {
      setTestingProxy(false);
    }
  }

  async function handleCheckVersion() {
    setCheckingVersion(true);
    try {
      const result = await apiFetch<AppVersion>("/version?force=1");
      setVersionInfo(result);
      appVersion.setData(result);
      if (result.update_error) {
        toast.error(result.update_error);
      } else if (result.update_available && result.latest_version) {
        toast.warning(`发现新版本 ${result.latest_version}`);
      } else {
        toast.success("当前已是最新版本");
      }
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "检测更新失败");
    } finally {
      setCheckingVersion(false);
    }
  }

  async function handleSystemUpdate() {
    const ok = await confirm({
      title: "立即更新并重启？",
      description:
        "系统会触发内网更新侧车拉取最新镜像并重建 app 容器，./data 里的配置和数据库不会被覆盖。完成时服务会短暂中断。",
      confirmLabel: "立即更新",
      destructive: true,
    });
    if (!ok) return;
    setUpdatingSystem(true);
    try {
      const result = await apiFetch<SystemUpdateResponse>("/system/update", {
        method: "POST",
      });
      toast.success(result.message || "已开始更新，服务稍后会自动重启");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "触发系统更新失败");
    } finally {
      setUpdatingSystem(false);
    }
  }

  // 复制服务器上的备用升级命令，方便 updater 侧车不可用时 SSH 执行。
  async function handleCopyUpgrade() {
    setCopyingUpgrade(true);
    try {
      const info = await apiFetch<UpgradeCommand>("/system/upgrade-command");
      const lines = [
        `# 拉取最新镜像并热替换`,
        info.command,
        info.auto_update ? `\n# 让服务器发现新版自动更新（可选）` : "",
        info.auto_update ?? "",
        info.rollback ? `\n# 更新翻车时一行回退到旧版本` : "",
        info.rollback ?? "",
      ]
        .filter(Boolean)
        .join("\n");
      await navigator.clipboard.writeText(lines);
      toast.success("升级命令已复制，到服务器 SSH 里执行即可");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "获取升级命令失败");
    } finally {
      setCopyingUpgrade(false);
    }
  }

  // 仅重启：进程主动退出，靠 docker compose 的 restart 策略拉起。
  async function handleRestart() {
    const ok = await confirm({
      title: "仅重启服务？",
      description:
        "服务会主动退出并由容器 restart 策略自动拉起，约 5 秒内恢复。这个动作不会拉取新镜像。期间正在进行的请求会中断。",
      confirmLabel: "仅重启",
      destructive: true,
    });
    if (!ok) return;
    setRestarting(true);
    try {
      await apiFetch<SystemRestartResponse>("/system/restart", {
        method: "POST",
      });
      toast.success("重启指令已发送，约 5 秒后自动恢复…");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "重启失败");
    } finally {
      setRestarting(false);
    }
  }

  function setRectifierOption(
    key: keyof SystemConfig["upstream"]["requestRectifier"],
    checked: boolean,
  ) {
    setForm((prev) =>
      prev
        ? {
            ...prev,
            upstream: {
              ...prev.upstream,
              requestRectifier: {
                ...prev.upstream.requestRectifier,
                [key]: checked,
              },
            },
          }
        : prev,
    );
  }

  return (
    <section className="space-y-4">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-lg font-semibold text-foreground">系统设置</h1>
          <Badge
            variant="outline"
            className="border-border bg-muted/40 text-muted-foreground"
          >
            动态配置中心
          </Badge>
        </div>
        <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
          这里集中管理鉴权、调度、通知策略和通知渠道。保存只写入配置文件，应用会让鉴权、调度和通知策略立即生效；通知渠道本身是实时写库生效。
        </p>
        <p className="text-xs text-muted-foreground">
          配置文件路径：{query.data?.config_path ?? "—"}
        </p>
      </header>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-4">
        <TabsList className="h-auto w-full justify-start rounded-2xl border border-border bg-muted/40 p-1">
          <TabsTrigger value="system" className="px-4 py-2">
            系统设置
          </TabsTrigger>
          <TabsTrigger value="notifications" className="px-4 py-2">
            通知渠道
          </TabsTrigger>
        </TabsList>

        <TabsContent value="system">
          <Card className="overflow-hidden border-border shadow-none">
            <CardContent className="space-y-8 p-4 sm:p-6">
              <SectionCard
                icon={<MonitorCog className="size-4 text-violet-600" />}
                title="应用信息"
                description="控制页面标题和通知标题前缀。"
              >
                <div className="mb-4 flex flex-wrap items-center gap-2 text-xs">
                  <Badge variant="outline" className="border-border bg-background">
                    当前版本 {versionInfo?.version || "加载中"}
                  </Badge>
                  {versionInfo?.latest_version ? (
                    <Badge
                      variant="outline"
                      className={cn(
                        "border-transparent",
                        versionInfo.update_available
                          ? "bg-amber-50 text-amber-700"
                          : "bg-emerald-50 text-emerald-700",
                      )}
                    >
                      {versionInfo.update_available
                        ? `可更新 ${versionInfo.latest_version}`
                        : "已是最新"}
                    </Badge>
                  ) : null}
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 border-border bg-background px-2 text-xs"
                    onClick={handleCheckVersion}
                    disabled={checkingVersion}
                  >
                    <RefreshCw
                      className={cn(
                        "size-3.5",
                        checkingVersion ? "animate-spin" : "",
                      )}
                    />
                    {checkingVersion ? "检测中..." : "检测更新"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 border-cyan-200 bg-cyan-50 px-2 text-xs text-cyan-700 hover:bg-cyan-100 hover:text-cyan-800"
                    onClick={handleSystemUpdate}
                    disabled={updatingSystem}
                  >
                    <RefreshCw
                      className={cn(
                        "size-3.5",
                        updatingSystem ? "animate-spin" : "",
                      )}
                    />
                    {updatingSystem ? "更新中..." : "立即更新并重启"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 border-border bg-background px-2 text-xs"
                    onClick={handleCopyUpgrade}
                    disabled={copyingUpgrade}
                  >
                    <Copy className="size-3.5" />
                    {copyingUpgrade ? "获取中..." : "复制升级命令"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="h-7 border-amber-200 bg-amber-50 px-2 text-xs text-amber-700 hover:bg-amber-100 hover:text-amber-800"
                    onClick={handleRestart}
                    disabled={restarting}
                  >
                    <Power
                      className={cn("size-3.5", restarting ? "animate-pulse" : "")}
                    />
                    {restarting ? "重启中..." : "仅重启"}
                  </Button>
                </div>
                <div className="grid gap-4 md:grid-cols-2">
                  <Field
                    label="应用标题"
                    description="用于顶部标题和浏览器标签标题。"
                  >
                    <Input
                      value={form.app.title}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: { ...prev.app, title: e.target.value },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field
                    label="通知前缀"
                    description="为空时通知标题不添加前缀。"
                  >
                    <Input
                      value={form.app.notificationPrefix}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  notificationPrefix: e.target.value,
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                </div>
              </SectionCard>

              <SectionCard
                icon={<KeyRound className="size-4 text-cyan-600" />}
                title="首页公益 Key"
                description="在公开首页展示一个可复制的公益 Key，可设置复制密码、提示词和到期时间。"
              >
                <div className="grid gap-4 md:grid-cols-2">
                  <InlineSwitch
                    id="public-key-enabled"
                    label="启用首页公益 Key"
                    description="开启后公开首页会显示公益 Key 入口，但不会直接暴露明文。"
                    checked={form.app.publicKey.enabled}
                    onCheckedChange={(checked) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              app: {
                                ...prev.app,
                                publicKey: {
                                  ...prev.app.publicKey,
                                  enabled: checked,
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                  <NoteBox title="推荐做法">
                    先在“创建 Key”页面创建一个只绑定低价分组的 Key，再把完整 Key 填到这里。
                  </NoteBox>
                </div>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <Field label="展示名称" description="公开首页按钮和卡片展示的名称。">
                    <Input
                      value={form.app.publicKey.name}
                      placeholder="公益 OpenAI Key"
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    name: e.target.value,
                                  },
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field label="到期时间" description="留空表示不过期；可填 2026-08-01 或 RFC3339。">
                    <Input
                      value={form.app.publicKey.expiresAt}
                      placeholder="2026-08-01"
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    expiresAt: e.target.value,
                                  },
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field label="公益 Key" description="填写完整网关 Key；公开摘要不会返回明文。">
                    <Input
                      type="password"
                      value={form.app.publicKey.key}
                      placeholder="sk-..."
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    key: e.target.value,
                                  },
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field label="复制密码" description="留空表示公开页无需密码即可复制。">
                    <Input
                      type="password"
                      value={form.app.publicKey.password}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    password: e.target.value,
                                  },
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                  <Field label="密码提示词" description="公开页密码输入框上方展示。">
                    <Input
                      value={form.app.publicKey.passwordHint}
                      placeholder="例如：关注公告获取复制密码"
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    passwordHint: e.target.value,
                                  },
                                },
                              }
                            : prev,
                        )
                      }
                    />
                  </Field>
                </div>
              </SectionCard>

              <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.05fr_1fr]">
            <SectionCard
              icon={<ShieldCheck className="size-4 text-emerald-600" />}
              title="登录鉴权"
              description="控制后台是否需要登录，以及登录令牌的签发方式。"
            >
              <div className="grid gap-4 md:grid-cols-2">
                <InlineSwitch
                  id="auth-enabled"
                  label="启用登录鉴权"
                  description="关闭后前端将直接进入系统，不显示登录页。"
                  checked={form.auth.enabled}
                  onCheckedChange={(checked) =>
                    setForm((prev) =>
                      prev
                        ? { ...prev, auth: { ...prev.auth, enabled: checked } }
                        : prev,
                    )
                  }
                />
                <NoteBox title="热应用说明">
                  应用后新的鉴权配置立即生效，现有无效令牌会在后续请求时被拦截。
                </NoteBox>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-2">
                <Field
                  label="管理员账号"
                  description="用于后台登录的固定账号。"
                >
                  <Input
                    value={form.auth.username}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: { ...prev.auth, username: e.target.value },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="登录有效期（小时）"
                  description="登录后令牌的有效时长。"
                >
                  <Input
                    type="number"
                    value={String(form.auth.sessionTTLHours)}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: {
                                ...prev.auth,
                                sessionTTLHours: num(e.target.value),
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="管理员密码"
                  description="保存后写入配置文件，应用后用于新登录。"
                >
                  <Input
                    value={form.auth.password}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: { ...prev.auth, password: e.target.value },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="令牌签名密钥"
                  description="留空时回退使用安全主密钥。"
                >
                  <Input
                    value={form.auth.tokenSecret}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              auth: {
                                ...prev.auth,
                                tokenSecret: e.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
              </div>
            </SectionCard>

            <SectionCard
              icon={<Clock3 className="size-4 text-sky-600" />}
              title="调度与保留策略"
              description="管理余额采集、倍率采集和历史清理任务。"
            >
              <div className="grid gap-4 md:grid-cols-2">
                <Field
                  label="余额采集 Cron"
                  description="控制余额与消费同步的执行周期。"
                >
                  <Input
                    value={form.scheduler.balanceCron}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                balanceCron: e.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="倍率采集 Cron"
                  description="控制分组倍率扫描的执行周期。"
                >
                  <Input
                    value={form.scheduler.rateCron}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                rateCron: e.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="网关测活 Cron"
                  description="定时测活上游分组 Key，死亡渠道冷却后可自动恢复。"
                >
                  <Input
                    value={form.scheduler.gatewayHealthCron ?? ""}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                gatewayHealthCron: e.target.value,
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="并发数"
                  description="调度器每轮最多同时处理的任务数。"
                >
                  <Input
                    type="number"
                    value={String(form.scheduler.concurrency)}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                concurrency: num(e.target.value),
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="清理任务 Cron"
                  description="留空则不执行历史数据清理。"
                >
                  <Input
                    value={form.scheduler.retention.cron}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  cron: e.target.value,
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
              </div>
              <div className="mt-4 grid gap-4 md:grid-cols-3">
                <Field
                  label="监控日志保留天数"
                  description="超过该天数的监控日志会被清理。"
                >
                  <Input
                    type="number"
                    value={String(form.scheduler.retention.monitorLogsDays)}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  monitorLogsDays: num(e.target.value),
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="余额快照保留天数"
                  description="余额与消费趋势依赖这部分历史快照。"
                >
                  <Input
                    type="number"
                    value={String(
                      form.scheduler.retention.balanceSnapshotsDays,
                    )}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  balanceSnapshotsDays: num(e.target.value),
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
                <Field
                  label="通知日志保留天数"
                  description="通知发送结果的历史留存时长。"
                >
                  <Input
                    type="number"
                    value={String(
                      form.scheduler.retention.notificationLogsDays,
                    )}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  notificationLogsDays: num(e.target.value),
                                },
                              },
                            }
                          : prev,
                      )
                    }
                  />
                </Field>
              </div>
            </SectionCard>
          </div>

          <SectionCard
            icon={<Bell className="size-4 text-amber-600" />}
            title="通知策略"
            description="这些项决定系统怎么合并、过滤和重试通知。"
          >
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <InlineSwitch
                id="batch-rate"
                label="合并倍率变化"
                description="同一次扫描中的多条倍率变化合并发送。"
                checked={form.notifications.batchRateChanges}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          notifications: {
                            ...prev.notifications,
                            batchRateChanges: checked,
                          },
                        }
                      : prev,
                  )
                }
              />
              <Field
                label="最小涨跌幅百分比"
                description="低于该值的倍率变化不发送通知。"
              >
                <Input
                  type="number"
                  step="0.01"
                  value={String(form.notifications.minChangePct)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              minChangePct: Number(e.target.value || 0),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="余额不足冷却分钟"
                description="同一渠道重复告警的抑制时间。"
              >
                <Input
                  type="number"
                  value={String(form.notifications.balanceLowCooldownMinutes)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              balanceLowCooldownMinutes: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="每日剩余提醒百分比"
                description="Sub2API 订阅每日剩余额度低于该百分比时提醒，0 为关闭。"
              >
                <Input
                  type="number"
                  step="0.1"
                  value={String(form.notifications.subscriptionDailyRemainingThresholdPct)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              subscriptionDailyRemainingThresholdPct: Number(e.target.value || 0),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="每周剩余提醒百分比"
                description="Sub2API 订阅每周剩余额度低于该百分比时提醒，0 为关闭。"
              >
                <Input
                  type="number"
                  step="0.1"
                  value={String(form.notifications.subscriptionWeeklyRemainingThresholdPct)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              subscriptionWeeklyRemainingThresholdPct: Number(e.target.value || 0),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="每月剩余提醒百分比"
                description="Sub2API 订阅每月剩余额度低于该百分比时提醒，0 为关闭。"
              >
                <Input
                  type="number"
                  step="0.1"
                  value={String(form.notifications.subscriptionMonthlyRemainingThresholdPct)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              subscriptionMonthlyRemainingThresholdPct: Number(e.target.value || 0),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="订阅到期提醒小时"
                description="Sub2API 订阅剩余小时数低于该值时提醒，0 为关闭。"
              >
                <Input
                  type="number"
                  value={String(form.notifications.subscriptionExpiryThresholdHours)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              subscriptionExpiryThresholdHours: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="订阅提醒冷却分钟"
                description="同一渠道同一类订阅提醒的冷却时间。"
              >
                <Input
                  type="number"
                  value={String(form.notifications.subscriptionAlertCooldownMinutes)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              subscriptionAlertCooldownMinutes: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="通知最大重试次数"
                description="发送失败后的最大尝试次数。"
              >
                <Input
                  type="number"
                  value={String(form.notifications.sendMaxAttempts)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            notifications: {
                              ...prev.notifications,
                              sendMaxAttempts: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
            </div>
          </SectionCard>

          <SectionCard
            icon={<Server className="size-4 text-indigo-600" />}
            title="上游请求"
            description="配置渠道访问上游站点时使用的超时时间和 User-Agent。"
          >
            <div className="grid gap-4 md:grid-cols-2">
              <Field label="超时时间（秒）" description="小于等于 0 时使用默认 30 秒。">
                <Input
                  type="number"
                  min={0}
                  value={String(form.upstream.timeoutSeconds)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              timeoutSeconds: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field label="User-Agent" description="为空时使用 upstream-ops/0.1。">
                <Input
                  value={form.upstream.userAgent}
                  placeholder="upstream-ops/0.1"
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              userAgent: e.target.value,
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <InlineSwitch
                id="rectifier-enabled"
                label="启用整流器"
                description="关闭后所有请求整流功能都会停用。"
                checked={form.upstream.requestRectifier?.enabled ?? true}
                onCheckedChange={(checked) =>
                  setRectifierOption("enabled", checked)
                }
              />
              <InlineSwitch
                id="rectifier-thinking-signature"
                label="Thinking 签名整流"
                description="thinking 签名不兼容时清理 thinking 相关块并重试一次。"
                checked={
                  form.upstream.requestRectifier?.thinkingSignature ?? true
                }
                onCheckedChange={(checked) =>
                  setRectifierOption("thinkingSignature", checked)
                }
              />
              <InlineSwitch
                id="rectifier-thinking-budget"
                label="Thinking Budget 整流"
                description="budget_tokens 约束错误时提升 thinking budget 并重试一次。"
                checked={form.upstream.requestRectifier?.thinkingBudget ?? true}
                onCheckedChange={(checked) =>
                  setRectifierOption("thinkingBudget", checked)
                }
              />
              <InlineSwitch
                id="rectifier-image-fallback"
                label="不支持图片降级"
                description="上游不支持图片时用 [Unsupported Image] 标记替换图片块。"
                checked={
                  form.upstream.requestRectifier?.unsupportedImageFallback ??
                  true
                }
                onCheckedChange={(checked) =>
                  setRectifierOption("unsupportedImageFallback", checked)
                }
              />
              <InlineSwitch
                id="rectifier-text-only-heuristic"
                label="启发式纯文本模型识别"
                description="按内置模型名列表提前剥离图片，默认关闭以避免误删多模态输入。"
                checked={
                  form.upstream.requestRectifier?.heuristicTextOnlyModels ??
                  false
                }
                onCheckedChange={(checked) =>
                  setRectifierOption("heuristicTextOnlyModels", checked)
                }
              />
            </div>
          </SectionCard>

          <SectionCard
            icon={<Network className="size-4 text-cyan-600" />}
            title="代理 IP"
            description="配置渠道上游请求使用的全局代理。只有渠道里开启代理 IP 的账号会使用这里的配置。"
            action={
              <Button
                size="sm"
                variant="outline"
                className="border-border bg-background"
                onClick={handleTestProxy}
                disabled={testingProxy}
              >
                <RefreshCw
                  className={cn("size-3.5", testingProxy ? "animate-spin" : "")}
                />
                {testingProxy ? "测试中..." : "测试代理"}
              </Button>
            }
          >
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              <InlineSwitch
                id="proxy-enabled"
                label="启用全局代理"
                description="关闭后所有已勾选代理 IP 的对象也会保持直连。"
                checked={form.proxy.enabled}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          proxy: { ...prev.proxy, enabled: checked },
                        }
                      : prev,
                  )
                }
              />
              <InlineSwitch
                id="proxy-version-check"
                label="检测更新走代理"
                description="开启后顶部自动检测更新和这里的检测更新会使用代理。"
                checked={form.proxy.versionCheckEnabled}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          proxy: {
                            ...prev.proxy,
                            versionCheckEnabled: checked,
                          },
                        }
                      : prev,
                  )
                }
              />
              <Field label="协议" description="支持 HTTP、HTTPS 和 SOCKS5。">
                <Select
                  value={form.proxy.protocol}
                  onValueChange={(value) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: {
                              ...prev.proxy,
                              protocol: value as "http" | "https" | "socks5",
                            },
                          }
                        : prev,
                    )
                  }
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="http">HTTP</SelectItem>
                    <SelectItem value="https">HTTPS</SelectItem>
                    <SelectItem value="socks5">SOCKS5</SelectItem>
                  </SelectContent>
                </Select>
              </Field>
              <Field label="主机" description="代理服务器地址，不含协议。">
                <Input
                  value={form.proxy.host}
                  placeholder="127.0.0.1"
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, host: e.target.value },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field label="端口" description="代理服务监听端口。">
                <Input
                  type="number"
                  value={String(form.proxy.port || "")}
                  placeholder="7890"
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, port: num(e.target.value) },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field label="账号（可选）" description="代理认证用户名。">
                <Input
                  value={form.proxy.username}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, username: e.target.value },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field label="密码（可选）" description="代理认证密码。">
                <Input
                  type="password"
                  value={form.proxy.password}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            proxy: { ...prev.proxy, password: e.target.value },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
            </div>
          </SectionCard>

          <div className="flex flex-wrap items-center gap-3 border-t border-border pt-5">
            <Button onClick={handleSave} disabled={saving || applying}>
              {saving ? "保存中..." : "保存"}
            </Button>
            <Button
              variant="outline"
              onClick={handleApply}
              disabled={saving || applying}
            >
              {applying ? "应用中..." : "应用"}
            </Button>
            <span
              className={cn(
                "text-xs",
                configSavedPendingApply
                  ? "font-medium text-amber-700"
                  : "text-muted-foreground",
              )}
            >
              {configSavedPendingApply
                ? "配置已保存但尚未应用，点击应用后才会立即生效。"
                : "保存写入配置文件，应用让鉴权、调度、通知策略、代理和上游请求配置立即更新。"}
            </span>
          </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="notifications">
          <SectionCard
            icon={<Send className="size-4 text-violet-600" />}
            title="通知渠道"
            description="管理邮件、企业微信、飞书通知出口。"
            action={
              <Button
                size="sm"
                variant="outline"
                className="border-border bg-background"
                onClick={() => {
                  setEditingNotification(null);
                  setNotificationOpen(true);
                }}
              >
                <Plus className="size-3.5" />
                新增渠道
              </Button>
            }
          >
            <div className="mb-4 grid gap-3 md:grid-cols-3">
              <MiniMetric
                title="渠道总数"
                value={String(notifications.data?.length ?? 0)}
              />
              <MiniMetric
                title="最近发送"
                value={lastSent ? relativeTime(lastSent) : "—"}
              />
              <MiniMetric
                title="最近失败"
                value={String(recentFailed)}
                danger={recentFailed > 0}
              />
            </div>
            {notifications.loading ? (
              <EmptyLine text="通知渠道加载中..." />
            ) : !notifications.data || notifications.data.length === 0 ? (
              <EmptyPanel
                title="还没有通知渠道"
                description="新增一个通知渠道后，就可以用于余额告警、登录失败和倍率变化提醒。"
              />
            ) : (
              <div className="space-y-3">
                {notifications.data.map((channel) => {
                  const Icon = notifyIcon(channel.type);
                  const subCount = parseSubCount(channel.subscriptions);
                  return (
                    <div
                      key={channel.id}
                      className="rounded-2xl border border-border bg-background/80 p-4"
                    >
                      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                        <div className="flex min-w-0 items-start gap-3">
                          <div
                            className={cn(
                              "mt-0.5 flex size-9 shrink-0 items-center justify-center rounded-xl border",
                              channel.enabled
                                ? "border-violet-200 bg-violet-50 text-violet-700"
                                : "border-border bg-muted/40 text-muted-foreground",
                            )}
                          >
                            <Icon className="size-4" />
                          </div>
                          <div className="min-w-0 space-y-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <p className="truncate text-sm font-semibold text-foreground">
                                {channel.name}
                              </p>
                              <Badge
                                variant="outline"
                                className="border-border bg-muted/40"
                              >
                                {typeLabel(channel.type)}
                              </Badge>
                              <Badge
                                variant="outline"
                                className={cn(
                                  "border-transparent",
                                  channel.enabled
                                    ? "bg-emerald-50 text-emerald-700"
                                    : "bg-slate-100 text-slate-500",
                                )}
                              >
                                {channel.enabled ? "启用中" : "已禁用"}
                              </Badge>
                              {channel.proxy_enabled ? (
                                <Badge
                                  variant="outline"
                                  className="border-transparent bg-cyan-50 text-cyan-700"
                                >
                                  代理 IP
                                </Badge>
                              ) : null}
                            </div>
                            <p className="text-xs text-muted-foreground">
                              {subCount === 0
                                ? "订阅全部渠道和分组"
                                : `已配置 ${subCount} 条订阅规则`}
                            </p>
                          </div>
                        </div>
                        <div className="flex flex-wrap items-center gap-2">
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={busyNotificationID === channel.id}
                            onClick={() => handleTestNotification(channel)}
                          >
                            测试发送
                          </Button>
                          <Button
                            size="icon-sm"
                            variant="ghost"
                            onClick={() => {
                              setEditingNotification(channel);
                              setNotificationOpen(true);
                            }}
                          >
                            <PencilLine className="size-4" />
                          </Button>
                          <Button
                            size="icon-sm"
                            variant="ghost"
                            className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                            disabled={busyNotificationID === channel.id}
                            onClick={() => handleDeleteNotification(channel)}
                          >
                            <Trash2 className="size-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </SectionCard>
        </TabsContent>

      </Tabs>

      <NotificationFormDialog
        open={notificationOpen}
        onOpenChange={(open) => {
          setNotificationOpen(open);
          if (!open) setEditingNotification(null);
        }}
        channel={editingNotification}
      />

      {confirmDialog}
    </section>
  );
}

function Field({
  label,
  description,
  children,
}: {
  label: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <div className="space-y-1">
        <Label className="text-xs font-medium text-foreground">{label}</Label>
        {description ? (
          <p className="text-[11px] leading-5 text-muted-foreground">
            {description}
          </p>
        ) : null}
      </div>
      {children}
    </div>
  );
}

function SectionCard({
  icon,
  title,
  description,
  action,
  children,
}: {
  icon: React.ReactNode;
  title: string;
  description: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="rounded-3xl border border-border/80 bg-muted/20 p-5">
      <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1.5">
          <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
            {icon}
            {title}
          </div>
          <p className="max-w-2xl text-sm leading-6 text-muted-foreground">
            {description}
          </p>
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}

function InlineSwitch({
  id,
  label,
  description,
  checked,
  onCheckedChange,
}: {
  id: string;
  label: string;
  description: string;
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-start justify-between gap-4 rounded-2xl border border-border bg-background/90 px-4 py-3">
      <div className="space-y-1">
        <Label htmlFor={id} className="text-sm font-medium text-foreground">
          {label}
        </Label>
        <p className="text-[11px] leading-5 text-muted-foreground">
          {description}
        </p>
      </div>
      <Switch id={id} checked={checked} onCheckedChange={onCheckedChange} />
    </div>
  );
}

function NoteBox({
  title,
  children,
}: {
  title: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-emerald-200 bg-emerald-50/70 px-4 py-3 text-sm text-emerald-900">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-700">
        {title}
      </p>
      <p className="mt-1 leading-6">{children}</p>
    </div>
  );
}

function StatusBox({
  title,
  value,
  hint,
  danger = false,
}: {
  title: string;
  value: string;
  hint: string;
  danger?: boolean;
}) {
  return (
    <div className="rounded-xl border border-border bg-background px-3 py-2.5">
      <p className="text-[11px] text-muted-foreground">{title}</p>
      <p
        className={cn(
          "mt-1 text-sm font-semibold",
          danger ? "text-destructive" : "text-foreground",
        )}
      >
        {value}
      </p>
      <p className="mt-1 text-[11px] text-muted-foreground">{hint}</p>
    </div>
  );
}

function MiniMetric({
  title,
  value,
  hint,
  danger = false,
}: {
  title: string;
  value: string;
  hint?: string;
  danger?: boolean;
}) {
  return (
    <div className="rounded-2xl border border-border bg-background/80 px-4 py-3">
      <p className="text-[11px] text-muted-foreground">{title}</p>
      <p
        className={cn(
          "mt-1 text-sm font-semibold",
          danger ? "text-destructive" : "text-foreground",
        )}
      >
        {value}
      </p>
      {hint ? (
        <p className="mt-1 text-[11px] text-muted-foreground">{hint}</p>
      ) : null}
    </div>
  );
}

function EmptyPanel({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="rounded-2xl border border-dashed border-border bg-background/70 px-4 py-6">
      <p className="text-sm font-medium text-foreground">{title}</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        {description}
      </p>
    </div>
  );
}

function EmptyLine({ text }: { text: string }) {
  return <p className="text-sm text-muted-foreground">{text}</p>;
}

function typeLabel(type: NotificationChannelType) {
  const map: Partial<Record<NotificationChannelType, string>> = {
    email: "邮件",
    wecom: "企业微信",
    feishu: "飞书",
  };
  return map[type] ?? type;
}

function notifyIcon(type: NotificationChannelType) {
  const map: Partial<Record<NotificationChannelType, typeof Send>> = {
    email: Send,
    wecom: Send,
    feishu: Send,
  };
  return map[type] ?? Send;
}

function parseSubCount(raw?: string) {
  if (!raw) return 0;
  try {
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr.length : 0;
  } catch {
    return 0;
  }
}
