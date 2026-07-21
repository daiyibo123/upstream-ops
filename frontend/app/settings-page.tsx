import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { toast } from "sonner";
import {
  Bell,
  Clock3,
  HeartHandshake,
  MonitorCog,
  Network,
  PencilLine,
  Plus,
  Power,
  RefreshCw,
  Search,
  Send,
  Server,
  Settings2,
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
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useConfirm } from "@/components/ui/confirm-dialog";
import { NotificationFormDialog } from "@/components/monitor/notification-form-dialog";
import { PublicKeyConfigCard } from "@/components/monitor/public-key-config-card";
import { apiFetch } from "@/lib/api";
import { useTriggerRefresh } from "@/lib/refresh-context";
import type {
  AppVersion,
  NotificationChannel,
  NotificationChannelType,
  SystemConfig,
  SystemConfigResponse,
  SystemRestartResponse,
  SystemUpdateResponse,
  SystemUpdateStatus,
} from "@/lib/api-types";
import { relativeTime } from "@/lib/format";
import { projectReleaseURL } from "@/lib/project-links";
import {
  useDashboardSummary,
  useNotificationLogs,
  useNotificationChannels,
  useAppVersion,
  useSystemConfig,
  useChannels,
} from "@/lib/queries";
import { cn } from "@/lib/utils";
import { PageHeader } from "@/components/page-header";

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
  const routeAffinity = (app.routeAffinity ?? {}) as Partial<
    NonNullable<SystemConfig["app"]["routeAffinity"]>
  >;
  const upstream = cfg.upstream as SystemConfig["upstream"] & {
    healthProbeModels?: string[];
    healthProbeMaxRatio?: number;
    temporaryFailureCooldownSeconds?: number;
    streamInterceptionScanEvents?: number;
  };
  return {
    ...cfg,
    app: {
      ...app,
		homepageCheapestEnabled: app.homepageCheapestEnabled ?? true,
      publicKey: {
        enabled: publicKey.enabled ?? false,
        name: publicKey.name ?? "公益 Key",
        key: publicKey.key ?? "",
        password: publicKey.password ?? "",
        passwordHint: publicKey.passwordHint ?? "",
        expiresAt: publicKey.expiresAt ?? "",
        ipConcurrencyLimit: publicKey.ipConcurrencyLimit ?? 3,
      },
      routeAffinity: {
        enabled: routeAffinity.enabled ?? true,
        promoteMinSavingsRatio: routeAffinity.promoteMinSavingsRatio ?? 0.3,
      },
    },
    upstream: {
      ...upstream,
      healthProbeModels:
        Array.isArray(upstream.healthProbeModels) && upstream.healthProbeModels.length > 0
          ? upstream.healthProbeModels
          : ["gpt-5.4", "gpt-5.5"],
      healthProbeMaxRatio: upstream.healthProbeMaxRatio ?? 0.1,
      temporaryFailureCooldownSeconds: upstream.temporaryFailureCooldownSeconds ?? 300,
      streamInterceptionScanEvents: upstream.streamInterceptionScanEvents ?? 0,
    },
		proxy: {
			...cfg.proxy,
			selectedTargets: normalizeProxyTargets(cfg.proxy.selectedTargets),
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

interface ProxyTargetResponse {
  id: string;
  name: string;
  kind: string;
  fixed?: boolean;
  channel_id?: number;
}

function normalizeProxyTargets(values: string[] | undefined) {
  const seen = new Set<string>();
  return (Array.isArray(values) ? values : [])
    .map((value) => {
      const normalized = value.trim().toLowerCase();
      if (normalized === "available:gpt") return "fixed-channel:gpt";
      if (normalized === "available:grok") return "fixed-channel:grok";
      return normalized;
    })
    .filter((value) => {
      if (!value || seen.has(value)) return false;
      seen.add(value);
      return true;
    });
}

function proxyKindLabel(kind: string) {
  switch (kind) {
    case "oauth_pool":
      return "固定号池";
    case "fixed_channel":
      return "固定渠道";
    default:
      return "普通渠道";
  }
}

function isMaskedProxyCredential(value: string) {
  return value === "********" || /^.\*{3}.$/u.test(value);
}

function redactProxyMessage(value: string) {
  return value
    .replace(/([a-z][a-z0-9+.-]*:\/\/)([^/\s:@]+):([^@\s/]+)@/gi, "$1***:***@")
    .replace(/((?:username|password|proxy[_-]?(?:user|password))\s*[:=]\s*)[^\s,;]+/gi, "$1[已隐藏]");
}

export default function SettingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const query = useSystemConfig();
  const notifications = useNotificationChannels();
  const summary = useDashboardSummary();
  const notificationLogs = useNotificationLogs(1, 10);
  const appVersion = useAppVersion();
  const channels = useChannels();
  const refresh = useTriggerRefresh();
  const { confirm, dialog: confirmDialog } = useConfirm();
  const [form, setForm] = useState<SystemConfig | null>(null);
  const [saving, setSaving] = useState(false);
  const [healthProbeModelDraft, setHealthProbeModelDraft] = useState("");
  const [testingProxy, setTestingProxy] = useState(false);
  const [proxySearch, setProxySearch] = useState("");
  const [proxyTargets, setProxyTargets] = useState<ProxyTargetResponse[]>([]);
  const [proxyTargetsLoading, setProxyTargetsLoading] = useState(true);
  const [proxyTargetsError, setProxyTargetsError] = useState<string | null>(null);
  const [checkingVersion, setCheckingVersion] = useState(false);
  const [updatingSystem, setUpdatingSystem] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [editingNotification, setEditingNotification] =
    useState<NotificationChannel | null>(null);
  const [notificationOpen, setNotificationOpen] = useState(false);
  const [busyNotificationID, setBusyNotificationID] = useState<number | null>(
    null,
  );
  const [activeTab, setActiveTab] = useState(
    searchParams.get("tab") === "notifications" ? "notifications" : "system",
  );
  const [versionInfo, setVersionInfo] = useState<AppVersion | null>(null);

  // 只在首次加载时自动填充表单，避免后台刷新覆盖尚未保存的编辑内容。
  // 保存流程会显式 PUT 后再 GET，并直接用服务端确认结果回填。
  const formInitializedRef = useRef(false);
  useEffect(() => {
    if (!formInitializedRef.current && query.data?.config) {
      formInitializedRef.current = true;
      setForm(withConfigDefaults(query.data.config));
    }
  }, [query.data]);

  useEffect(() => {
    let cancelled = false;
    setProxyTargetsLoading(true);
    setProxyTargetsError(null);
    apiFetch<ProxyTargetResponse[]>("/settings/proxy/targets")
      .then((items) => {
        if (!cancelled) setProxyTargets(Array.isArray(items) ? items : []);
      })
      .catch((error: Error) => {
        if (!cancelled) setProxyTargetsError(redactProxyMessage(error.message || "加载代理渠道失败"));
      })
      .finally(() => {
        if (!cancelled) setProxyTargetsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (appVersion.data) {
      setVersionInfo(appVersion.data);
    }
  }, [appVersion.data]);

  useEffect(() => {
    const next = searchParams.get("tab") === "notifications" ? "notifications" : "system";
    setActiveTab(next);
  }, [searchParams]);

  function handleTabChange(value: string) {
    const next = value === "notifications" ? "notifications" : "system";
    setActiveTab(next);
    setSearchParams(next === "notifications" ? { tab: "notifications" } : {}, {
      replace: true,
    });
  }

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
	const fixedProxyTargets = [
		{ value: "pool:chatgpt", label: "chatgpt号池", kind: "固定号池" },
		{ value: "pool:grok", label: "grok号池", kind: "固定号池" },
		{ value: "fixed-channel:gpt", label: "gpt渠道", kind: "固定渠道" },
		{ value: "fixed-channel:grok", label: "grok渠道", kind: "固定渠道" },
	];
	const discoveredProxyTargets = proxyTargets.length > 0
		? proxyTargets.map((item) => ({
			value: normalizeProxyTargets([item.id])[0] ?? item.id,
			label: item.name,
			kind: proxyKindLabel(item.kind),
		}))
		: (channels.data ?? [])
			.filter((channel) => channel.type !== "chatgpt_pool" && channel.type !== "grok_pool")
			.map((channel) => ({
				value: `channel:${channel.id}`,
				label: channel.name,
				kind: "普通渠道",
			}));
	const proxyTargetOptions = [...fixedProxyTargets, ...discoveredProxyTargets]
		.filter((item, index, list) => list.findIndex((candidate) => candidate.value === item.value) === index);
	const normalizedProxySearch = proxySearch.trim().toLowerCase();
	const visibleProxyTargets = proxyTargetOptions.filter((item) =>
		!normalizedProxySearch || `${item.label} ${item.kind}`.toLowerCase().includes(normalizedProxySearch),
	);

  function addHealthProbeModel() {
    if (!form) return;
    const model = healthProbeModelDraft.trim();
    if (!model) return;
    const current = form.upstream.healthProbeModels ?? [];
    if (current.some((item) => item.toLowerCase() === model.toLowerCase())) {
      toast.info("该测活模型已在清单中");
      return;
    }
    setForm((prev) =>
      prev
        ? {
            ...prev,
            upstream: {
              ...prev.upstream,
              healthProbeModels: [...(prev.upstream.healthProbeModels ?? []), model],
            },
          }
        : prev,
    );
    setHealthProbeModelDraft("");
  }

  function removeHealthProbeModel(model: string) {
    setForm((prev) =>
      prev
        ? {
            ...prev,
            upstream: {
              ...prev.upstream,
              healthProbeModels: (prev.upstream.healthProbeModels ?? []).filter(
                (item) => item !== model,
              ),
            },
          }
        : prev,
    );
  }

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
    if (!form) return;
		if (form.proxy.enabled && (!form.proxy.host.trim() || form.proxy.port <= 0)) {
			toast.error("启用代理后必须填写有效的代理主机和端口");
			return;
		}
    setSaving(true);
    try {
      // 后端 PUT /settings/config 保存即生效；随后重新读取一次，以服务端持久化结果为准。
      const result = await apiFetch<{ message?: string }>("/settings/config", {
        method: "PUT",
        body: JSON.stringify(form),
      });
			const confirmed = await apiFetch<SystemConfigResponse>("/settings/config");
			setForm(withConfigDefaults(confirmed.config));
			query.setData(confirmed);
			toast.success(result?.message || "已保存并确认生效");
      refresh();
    } catch (err) {
      toast.error(redactProxyMessage(err instanceof Error ? err.message : "保存失败"));
    } finally {
      setSaving(false);
    }
  }

  async function handleTestProxy() {
    if (form && (isMaskedProxyCredential(form.proxy.username) || isMaskedProxyCredential(form.proxy.password))) {
      toast.info("代理认证信息已脱敏，请重新输入账号和密码后再测试");
      return;
    }
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
        toast.error(redactProxyMessage(result.error ?? "代理测试失败"));
      }
    } catch (err) {
      toast.error(redactProxyMessage(err instanceof Error ? err.message : "代理测试失败"));
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
      void watchSystemUpdate();
      toast.success(result.message || "已开始更新，服务稍后会自动重启");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "触发系统更新失败");
    } finally {
      setUpdatingSystem(false);
    }
  }

  // 复制服务器上的备用升级命令，方便 updater 侧车不可用时 SSH 执行。
  /* Legacy SSH upgrade fallback intentionally kept out of the UI.
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
  */

  async function watchSystemUpdate() {
    for (let attempt = 0; attempt < 150; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 2000));
      try {
        const status = await apiFetch<SystemUpdateStatus>("/system/update/status");
        if (status.status === "failed") {
          toast.error(status.message || "更新失败，当前版本未替换");
          return;
        }
        if (status.status === "restarting") {
          toast.success(status.message || "新版本已下载，服务正在重启");
          return;
        }
      } catch {
        // Successful container replacement temporarily interrupts this request.
      }
    }
  }

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
    <section className="space-y-5">
      <PageHeader
        icon={<Settings2 className="size-[18px]" />}
        title="系统设置"
        description="集中管理鉴权、调度、代理和通知；保存后立即写入配置并应用。"
        meta={<Badge variant="outline" className="border-border bg-background text-muted-foreground">动态配置</Badge>}
      />

      <Tabs value={activeTab} onValueChange={handleTabChange} className="space-y-4">
        <TabsList className="h-auto w-full justify-start p-1">
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
                icon={<MonitorCog className="size-4 text-brand" />}
                title="应用信息"
                description="控制页面标题和通知标题前缀。"
              >
                <div className="mb-4 flex flex-wrap items-center gap-2 text-xs">
                  <Badge variant="outline" className="border-border bg-background">
                    当前版本 {versionInfo?.version || "加载中"}
                  </Badge>
                  {versionInfo?.latest_version && versionInfo.update_available ? (
                    <Badge
                      asChild
                      variant="outline"
                      className={cn(
                        "border-transparent",
                        "bg-warning/10 text-warning",
                      )}
                    >
                      <a
                        href={projectReleaseURL(versionInfo.latest_version)}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        可更新 {versionInfo.latest_version}
                      </a>
                    </Badge>
                  ) : versionInfo?.latest_version ? (
                    <Badge variant="outline" className="border-transparent bg-success/10 text-success">
                      已是最新
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
                    className="h-7 border-brand/20 bg-brand/10 px-2 text-xs text-brand hover:bg-brand/15 hover:text-brand"
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
                    className="h-7 border-warning/20 bg-warning/10 px-2 text-xs text-warning hover:bg-warning/15 hover:text-warning"
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
                <div className="mt-4">
                  <InlineSwitch
                    id="homepage-cheapest-enabled"
                    label="首页展示 OpenAI 最低倍率前五"
                    description="关闭后首页同位置展示网关服务器状态。"
                    checked={form.app.homepageCheapestEnabled}
                    onCheckedChange={(checked) =>
                      setForm((prev) => prev ? { ...prev, app: { ...prev.app, homepageCheapestEnabled: checked } } : prev)
                    }
                  />
                </div>
              </SectionCard>

              <SectionCard
                icon={<HeartHandshake className="size-4 text-success" />}
                title="首页公益 Key"
                description="从已创建的调用 Key 中选择一个展示到首页，可设置复制密码和展示名称。"
                action={<PublicKeyConfigCard />}
              >
                <div className="rounded-2xl border border-border bg-background/90 px-4 py-3 text-sm text-muted-foreground">
                  公益 Key 使用数据库中的调用 Key 配置，首页会读取这里选择的 Key；不会再读取旧的 config.yaml app.publicKey 字段，避免配置入口不一致。
                </div>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <Field
                    label="单 IP 并发数"
                    description="限制公益 Key 对同一个客户端 IP 的并发路数，防止单个 IP 占满公益额度导致他人排队。命中公网并发白名单的 IP 不受此限制。小于等于 0 时使用默认 3 路。"
                  >
                    <Input
                      type="number"
                      min={0}
                      value={String(form.app.publicKey.ipConcurrencyLimit ?? 0)}
                      onChange={(e) =>
                        setForm((prev) =>
                          prev
                            ? {
                                ...prev,
                                app: {
                                  ...prev.app,
                                  publicKey: {
                                    ...prev.app.publicKey,
                                    ipConcurrencyLimit: num(e.target.value),
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
              icon={<ShieldCheck className="size-4 text-success" />}
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
              icon={<Clock3 className="size-4 text-brand" />}
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
              <div className="mt-4 grid gap-4 md:grid-cols-4">
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
                <Field
                  label="使用记录保留天数"
                  description="只清理调用使用记录；0 表示永久保留。"
                >
                  <Input
                    type="number"
                    value={String(form.scheduler.retention.usageLogsDays ?? 1)}
                    onChange={(e) =>
                      setForm((prev) =>
                        prev
                          ? {
                              ...prev,
                              scheduler: {
                                ...prev.scheduler,
                                retention: {
                                  ...prev.scheduler.retention,
                                  usageLogsDays: num(e.target.value),
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
            icon={<Bell className="size-4 text-warning" />}
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
              <Field
                label="User-Agent"
                description="为空时使用当前 Codex CLI 请求身份。"
              >
                <Input
                  value={form.upstream.userAgent}
                  placeholder="codex_cli_rs/0.144.1 (Ubuntu 22.4.0; x86_64) xterm-256color"
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
              <Field
                label="首字节等待窗口（秒）"
                description="等上游吐出第一个可见输出的最长时间。推理模型出字前有较长思考阶段，过短会把可用渠道误判成卡死。小于等于 0 时使用默认 45 秒。"
              >
                <Input
                  type="number"
                  min={0}
                  value={String(form.upstream.streamFirstEventTimeoutSeconds ?? 0)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              streamFirstEventTimeoutSeconds: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="测活等待窗口（秒）"
                description="单次测活等一个可见输出的最长时间。小于等于 0 时使用默认 30 秒。"
              >
                <Input
                  type="number"
                  min={0}
                  value={String(form.upstream.healthProbeTimeoutSeconds ?? 0)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              healthProbeTimeoutSeconds: num(e.target.value),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="全量测活倍率上限"
                description="定时测活或未指定分组的一键测活，只扫描真实倍率不高于该值的低成本渠道。明确选择分组时不受限制。小于等于 0 时使用默认 0.1。"
              >
                <Input
                  type="number"
                  min={0}
                  step={0.01}
                  value={String(form.upstream.healthProbeMaxRatio ?? 0.1)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              healthProbeMaxRatio: Number(e.target.value || 0),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <div className="space-y-2 md:col-span-2">
                <div className="space-y-1">
                  <Label className="text-xs font-medium text-foreground">OpenAI 测活模型顺序</Label>
                  <p className="text-[11px] leading-5 text-muted-foreground">
                    测活会按从左到右的顺序尝试，前一个模型不支持或未生成内容时再尝试下一个。至少保留一个常用模型；清空后后端会恢复默认 gpt-5.4 → gpt-5.5。
                  </p>
                </div>
                <div className="flex flex-col gap-2 sm:flex-row">
                  <Input
                    value={healthProbeModelDraft}
                    placeholder="例如 gpt-5.6"
                    onChange={(e) => setHealthProbeModelDraft(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        addHealthProbeModel();
                      }
                    }}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    className="shrink-0"
                    disabled={!healthProbeModelDraft.trim()}
                    onClick={addHealthProbeModel}
                  >
                    <Plus className="size-3.5" />
                    添加模型
                  </Button>
                </div>
                <div className="flex min-h-11 flex-wrap gap-2 rounded-xl border border-border bg-background/80 p-2.5">
                  {(form.upstream.healthProbeModels ?? []).length === 0 ? (
                    <span className="self-center text-xs text-muted-foreground">保存时将使用后端默认模型清单</span>
                  ) : (
                    (form.upstream.healthProbeModels ?? []).map((model, index) => (
                      <Badge
                        key={`${model}-${index}`}
                        variant="outline"
                        className="gap-1.5 border-brand/20 bg-brand/5 py-1 pl-2.5 pr-1 text-foreground"
                      >
                        <span className="font-mono text-[11px]">{index + 1}. {model}</span>
                        <button
                          type="button"
                          className="rounded-md p-1 text-muted-foreground transition hover:bg-destructive/10 hover:text-destructive"
                          aria-label={`删除测活模型 ${model}`}
                          onClick={() => removeHealthProbeModel(model)}
                        >
                          <Trash2 className="size-3" />
                        </button>
                      </Badge>
                    ))
                  )}
                </div>
              </div>
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <InlineSwitch
                id="route-affinity-enabled"
                label="缓存粘性优先"
                description="同一对话尽量固定用同一个上游，保住上游的 prompt 缓存前缀，避免每次换上游都重喂上下文导致变慢、降智。仅当出现明显更便宜的上游时才切换。"
                checked={form.app.routeAffinity?.enabled ?? true}
                onCheckedChange={(checked) =>
                  setForm((prev) =>
                    prev
                      ? {
                          ...prev,
                          app: {
                            ...prev.app,
                            routeAffinity: {
                              ...prev.app.routeAffinity,
                              enabled: checked,
                            },
                          },
                        }
                      : prev,
                  )
                }
              />
              <Field
                label="切换省钱阈值"
                description="缓存粘性开启时的“逃生阀”：只有新上游比当前上游便宜达到该比例才切换。0.3 表示至少便宜 30% 才切。取值 0~1，超出范围时使用默认 0.3。"
              >
                <Input
                  type="number"
                  min={0}
                  max={1}
                  step={0.05}
                  value={String(form.app.routeAffinity?.promoteMinSavingsRatio ?? 0.3)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            app: {
                              ...prev.app,
                              routeAffinity: {
                                ...prev.app.routeAffinity,
                                promoteMinSavingsRatio: Number(e.target.value || 0),
                              },
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <Field
                label="临时不可调度时间（秒）"
                description="上游返回 503、命中内容拦截或发生网络错误后，立即退出调度池的时间。默认 300 秒；冷却期间不会再用用户请求探测该渠道。上游明确返回 Retry-After 时优先遵循上游时间。"
              >
                <Input
                  type="number"
                  min={1}
                  max={86400}
                  step={30}
                  value={String(form.upstream.temporaryFailureCooldownSeconds ?? 300)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              temporaryFailureCooldownSeconds: Math.max(1, Number(e.target.value || 300)),
                            },
                          }
                        : prev,
                    )
                  }
                />
              </Field>
              <Field
                label="首字节前拦截扫描事件数"
                description="0 = 关闭（默认，低延迟）：命中拦截词前的正常文本可能已透传。大于 0 时首字节落地前额外多缓冲这些可见事件做完整拦截扫描，命中拦截词就在写首字节前无缝切换到下一个候选（连命中前那段都不显示），代价是首字节延迟增加。"
              >
                <Input
                  type="number"
                  min={0}
                  max={24}
                  step={1}
                  value={String(form.upstream.streamInterceptionScanEvents ?? 0)}
                  onChange={(e) =>
                    setForm((prev) =>
                      prev
                        ? {
                            ...prev,
                            upstream: {
                              ...prev.upstream,
                              streamInterceptionScanEvents: Math.max(0, Number(e.target.value || 0)),
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
            icon={<Network className="size-4 text-brand" />}
            title="代理 IP"
            description="配置上游请求代理及其作用范围。未选择渠道代表全局使用代理，不代表关闭代理。"
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
                label="启用代理 IP"
                description="关闭后所有渠道按原方式直连，已选范围会保留供下次启用。"
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

			<div className={cn(
				"mt-4 rounded-lg border px-4 py-3 text-sm",
				!form.proxy.enabled
					? "border-border bg-muted/40 text-muted-foreground"
					: form.proxy.selectedTargets.length === 0
						? "border-warning/30 bg-warning/10 text-foreground"
						: "border-success/30 bg-success/10 text-foreground",
			)}>
				<p className="font-medium">
					{!form.proxy.enabled
						? "代理当前不生效"
						: form.proxy.selectedTargets.length === 0
							? "当前代理将全局应用于所有渠道。"
							: "当前代理仅应用于已选择的渠道。"}
				</p>
				<p className="mt-1 text-xs text-muted-foreground">
					{form.proxy.enabled && form.proxy.selectedTargets.length === 0
						? "普通渠道、OAuth 号池和固定可用渠道都会使用该代理。"
						: form.proxy.enabled
							? `已选择 ${form.proxy.selectedTargets.length} 个渠道目标，其他渠道保持直连。`
							: "所有渠道按原方式连接。"}
				</p>
			</div>

			<div className="mt-4 space-y-3 rounded-lg border border-border bg-background p-4">
				<div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
					<div>
						<p className="text-sm font-medium text-foreground">代理范围</p>
						<p className="mt-1 text-xs text-muted-foreground">不选择时全局使用；选择后仅应用于选中的渠道。</p>
					</div>
					<Button
						type="button"
						variant="outline"
						size="sm"
						disabled={form.proxy.selectedTargets.length === 0}
						onClick={() => setForm((prev) => prev ? { ...prev, proxy: { ...prev.proxy, selectedTargets: [] } } : prev)}
					>
						清空选择
					</Button>
				</div>
				<div className="relative">
					<Search className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
					<Input value={proxySearch} onChange={(event) => setProxySearch(event.target.value)} placeholder="搜索渠道或号池" className="pl-9" />
				</div>
				{proxyTargetsLoading ? (
					<p className="text-xs text-muted-foreground">正在读取可选渠道...</p>
				) : proxyTargetsError ? (
					<p className="rounded-md bg-warning/8 px-3 py-2 text-xs text-warning">
						暂时无法读取后端目标，当前已使用本地渠道列表。
					</p>
				) : null}
				<div className="max-h-64 overflow-y-auto rounded-md border border-border">
					{visibleProxyTargets.length === 0 ? (
						<p className="px-3 py-6 text-center text-xs text-muted-foreground">没有匹配的渠道</p>
					) : visibleProxyTargets.map((item) => {
						const checked = form.proxy.selectedTargets.includes(item.value);
						return (
							<label key={item.value} className="flex cursor-pointer items-center gap-3 border-b border-border px-3 py-2.5 last:border-b-0 hover:bg-muted/40">
								<Checkbox
									checked={checked}
									onCheckedChange={(next) => setForm((prev) => {
										if (!prev) return prev;
										const selected = next === true
											? [...prev.proxy.selectedTargets, item.value]
											: prev.proxy.selectedTargets.filter((value) => value !== item.value);
										return { ...prev, proxy: { ...prev.proxy, selectedTargets: selected } };
									})}
								/>
								<span className="min-w-0 flex-1 truncate text-sm text-foreground">{item.label}</span>
								<span className="shrink-0 text-xs text-muted-foreground">{item.kind}</span>
							</label>
						);
					})}
				</div>
			</div>
          </SectionCard>

          <div className="flex flex-wrap items-center gap-3 border-t border-border pt-5">
            <Button onClick={handleSave} disabled={saving}>
              {saving ? "保存中..." : "保存并生效"}
            </Button>
            <span className="text-xs text-muted-foreground">
              保存后立即写入配置文件并应用到运行时，鉴权、调度、通知策略、代理和上游请求配置即时更新，无需重启。
            </span>
          </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="notifications">
          <SectionCard
            icon={<Send className="size-4 text-brand" />}
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
                                ? "border-brand/20 bg-brand/10 text-brand"
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
                                    ? "bg-success/10 text-success"
                                    : "bg-muted text-muted-foreground",
                                )}
                              >
                                {channel.enabled ? "启用中" : "已禁用"}
                              </Badge>
                              {channel.proxy_enabled ? (
                                <Badge
                                  variant="outline"
                                  className="border-transparent bg-brand/10 text-brand"
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
    <section className="border-b border-border/70 pb-7 last:border-b-0 last:pb-0">
      <div className="mb-5 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 items-start gap-3">
          <span className="flex size-8 shrink-0 items-center justify-center rounded-md border border-border bg-muted/45">
            {icon}
          </span>
          <div className="space-y-1">
            <h2 className="text-sm font-semibold text-foreground">{title}</h2>
            <p className="max-w-2xl text-xs leading-5 text-muted-foreground">
              {description}
            </p>
          </div>
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
    <div className="rounded-2xl border border-success/20 bg-success/10 px-4 py-3 text-sm text-success">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-success">
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
