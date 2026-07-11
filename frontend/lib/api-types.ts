/**
 * API response shapes for UpstreamOps backend.
 * Keep in sync with backend/storage/*.go and backend/api/*.go.
 */

export type ChannelType = "newapi" | "sub2api"

export type CredentialMode = "password" | "token"

export type RechargeMultiplierMode = "divide" | "multiply"

export type NotificationChannelType =
  | "email"
  | "wecom"
  | "feishu"

export type MonitorJob = "login" | "balance" | "rates"

export type NotificationEvent =
  | "balance_low"
  | "rate_changed"
  | "rate_structure_changed"
  | "rate_added"
  | "rate_removed"
  | "announcement"
  | "login_failed"
  | "monitor_failed"
  | "subscription_daily_remaining_low"
  | "subscription_weekly_remaining_low"
  | "subscription_monthly_remaining_low"
  | "subscription_expiring"

export interface Channel {
  id: number
  name: string
  type: ChannelType
  site_url: string
  username: string
  sort_order: number
  user_id?: string
  credential_mode: CredentialMode
  login_extra_params: string
  turnstile_enabled: boolean
  ignore_announcements: boolean
  subscription_enabled: boolean
  proxy_enabled: boolean
  captcha_config_id?: number | null
  balance_threshold: number
  recharge_multiplier?: number | null
  recharge_multiplier_mode: RechargeMultiplierMode
  monitor_enabled: boolean
  last_balance?: number | null
  last_balance_at?: string | null
  today_cost?: number | null
  total_cost?: number | null
  last_error?: string
  created_at: string
  updated_at: string
}

export interface ChannelPage {
  items: Channel[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface RateSnapshot {
  id: number
  channel_id: number
  model_name: string
  description?: string
  ratio: number
  completion_ratio: number
  first_seen_at: string
  last_seen_at: string
}

export interface RateChangeLog {
  id: number
  channel_id: number
  model_name: string
  old_ratio: number | null
  new_ratio: number
  old_completion_ratio?: number | null
  new_completion_ratio?: number
  changed_at: string
}

export interface RateChangeLogPage {
  items: RateChangeLog[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface BalanceSnapshot {
  id: number
  channel_id: number
  balance: number
  sampled_at: string
}

export interface NotificationSubscription {
  channel_ids: number[]
  mode: "all" | "groups"
  groups?: string[]
  events?: NotificationEvent[]
}

export interface NotificationChannel {
  id: number
  name: string
  type: NotificationChannelType
  enabled: boolean
  proxy_enabled: boolean
  subscriptions?: string
  created_at: string
  updated_at: string
}

export interface NotificationLog {
  id: number
  channel_id: number
  upstream_channel_id?: number
  channel_name?: string
  channel_type?: string
  event: NotificationEvent
  subject: string
  body: string
  success: boolean
  error_message?: string
  sent_at: string
}

export interface UpstreamAnnouncement {
  id: number
  channel_id: number
  source_key: string
  title?: string
  content: string
  type?: string
  link?: string
  published_at?: string | null
  source_updated_at?: string | null
  first_seen_at: string
}

export interface MonitorLog {
  id: number
  channel_id: number
  job: MonitorJob
  success: boolean
  error_message?: string
  duration_ms: number
  started_at: string
  finished_at: string
}

export interface DashboardLowest {
  channel_id: number
  name: string
  balance: number | null
}

export interface DashboardChannelStat {
  id: number
  name: string
  type: string
  monitor_enabled: boolean
  last_balance?: number | null
  today_cost?: number | null
  total_cost?: number | null
  last_error?: string
}

export interface DashboardGatewayGroup {
  id: number
  channel_id: number
  channel_name: string
  client_format?: "openai" | "claude" | "any" | string
  group_name: string
  ratio: number
  priority: number
  enabled: boolean
  status: string
  failure_count: number
  total_tokens: number
  last_checked_at?: string | null
  last_used_at?: string | null
  last_error?: string
}

export interface DashboardGatewayKey {
  id: number
  name: string
  key_prefix: string
  enabled: boolean
  daily_limit: number
  total_limit: number
  today_tokens: number
  total_tokens: number
  expires_at?: string | null
  last_used_at?: string | null
}

export interface DashboardGatewayStat {
  total_keys: number
  enabled_keys: number
  total_groups: number
  alive_groups: number
  dead_groups: number
  unknown_groups: number
  today_tokens: number
  total_tokens: number
  prompt_tokens: number
  completion_tokens: number
  cheapest?: DashboardGatewayGroup | null
  groups: DashboardGatewayGroup[]
  keys: DashboardGatewayKey[]
}

export interface DashboardServerStat {
  status: string
  database: string
  uptime_seconds: number
  started_at: string
  server_time: string
  go_version: string
  num_goroutine: number
  alloc_bytes: number
  sys_bytes: number
  last_error?: string
}

export interface DashboardSummary {
  total_channels: number
  active_channels: number
  failed_channels: number
  total_balance: number
  today_total_cost: number
  total_cost: number
  lowest_balance: DashboardLowest | null
  channels: DashboardChannelStat[]
  recent_rate_changes: RateChangeLog[]
  gateway: DashboardGatewayStat
  server: DashboardServerStat
}

export interface BalanceTrendPoint {
  day: string
  balance: number
}

export interface CostTrendPoint {
  day: string
  cost: number
}

export interface SystemAuthConfig {
  enabled: boolean
  username: string
  password: string
  tokenSecret: string
  sessionTTLHours: number
}

export interface SystemPublicKeyConfig {
  enabled: boolean
  name: string
  key: string
  password: string
  passwordHint: string
  expiresAt: string
}

export interface AppConfig {
  title: string
  notificationPrefix: string
  publicKey: SystemPublicKeyConfig
}

export interface SystemSchedulerRetentionConfig {
  cron: string
  monitorLogsDays: number
  balanceSnapshotsDays: number
  notificationLogsDays: number
  announcementsDays: number
}

export interface SystemSchedulerConfig {
  balanceCron: string
  rateCron: string
  gatewayHealthCron: string
  concurrency: number
  retention: SystemSchedulerRetentionConfig
}

export interface SystemNotificationsConfig {
  batchRateChanges: boolean
  minChangePct: number
  balanceLowCooldownMinutes: number
  subscriptionDailyRemainingThresholdPct: number
  subscriptionWeeklyRemainingThresholdPct: number
  subscriptionMonthlyRemainingThresholdPct: number
  subscriptionExpiryThresholdHours: number
  subscriptionAlertCooldownMinutes: number
  sendMaxAttempts: number
}

export interface SystemProxyConfig {
  enabled: boolean
  versionCheckEnabled: boolean
  protocol: "http" | "https" | "socks5"
  host: string
  port: number
  username: string
  password: string
}

export interface SystemUpstreamConfig {
  timeoutSeconds: number
  userAgent: string
  requestRectifier: SystemRequestRectifierConfig
}

export interface SystemRequestRectifierConfig {
  enabled: boolean
  thinkingSignature: boolean
  thinkingBudget: boolean
  unsupportedImageFallback: boolean
  heuristicTextOnlyModels: boolean
}

export interface SystemConfig {
  app: AppConfig
  auth: SystemAuthConfig
  scheduler: SystemSchedulerConfig
  notifications: SystemNotificationsConfig
  proxy: SystemProxyConfig
  upstream: SystemUpstreamConfig
}

export interface SystemConfigResponse {
  config_path: string
  config: SystemConfig
}

export interface AppVersion {
  name: string
  title: string
  version: string
  latest_version?: string
  update_available?: boolean
  repo_url?: string
  release_url?: string
  update_error?: string
}

export interface UpgradeCommand {
  command: string
  auto_update?: string
  rollback?: string
  description: string
  restart_after: boolean
  repo_url: string
}

export interface SystemRestartResponse {
  status: string
  message: string
}

export interface ApplyConfigResult {
  applied_sections: string[]
  message: string
}

export interface ChannelRedeemResult {
  message: string
  type: string
  value: number
  new_balance?: number
  new_concurrency?: number
  group_name?: string
  validity_days?: number
}

export interface ChannelSubscriptionUsageWindow {
  limit_usd: number
  used_usd: number
  remaining_usd: number
  remaining_percent: number
  used_percent: number
  window_start?: string | null
  resets_at?: string | null
  resets_in_seconds: number
}

export interface ChannelSubscriptionUsage {
  id: number
  group_id: number
  group_name: string
  status: string
  starts_at?: string | null
  expires_at?: string | null
  expires_in_days: number
  daily?: ChannelSubscriptionUsageWindow | null
  weekly?: ChannelSubscriptionUsageWindow | null
  monthly?: ChannelSubscriptionUsageWindow | null
}

export interface ChannelSubscriptionUsageInfo {
  items: ChannelSubscriptionUsage[]
}

export type ChannelAPIKeyStatus = "active" | "disabled" | "expired" | "quota_exhausted" | "unknown"

export interface ChannelAPIKey {
  id: number
  key: string
  name: string
  status: ChannelAPIKeyStatus | string
  group?: string
  group_name?: string
  group_description?: string
  group_ratio: number
  group_id?: number | null
  quota: number
  quota_used: number
  unlimited_quota: boolean
  expired_time: number
  expires_at?: string | null
  created_at?: string | null
  updated_at?: string | null
  last_used_at?: string | null
  allow_ips?: string
  ip_whitelist?: string[]
  ip_blacklist?: string[]
  model_limits_enabled: boolean
  model_limits?: string
  cross_group_retry: boolean
  rate_limit_5h: number
  rate_limit_1d: number
  rate_limit_7d: number
  usage_5h: number
  usage_1d: number
  usage_7d: number
}

export interface ChannelAPIKeyPage {
  items: ChannelAPIKey[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface NotificationLogPage {
  items: NotificationLog[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface UpstreamAnnouncementPage {
  items: UpstreamAnnouncement[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface ChannelAPIKeyGroup {
  id?: number | null
  name: string
  description?: string
  ratio: number
}

export interface ChannelAPIKeyReveal {
  key: string
}

export interface GatewayKey {
  id: number
  name: string
  key_prefix: string
  key?: string
  enabled: boolean
  client_format: "openai" | "claude" | "any" | string
  allowed_group_ids?: number[]
  daily_limit: number
  total_limit: number
  today_tokens: number
  total_tokens: number
  usage_date?: string
  expires_at?: string | null
  last_used_at?: string | null
  last_used_ip?: string
  created_at: string
  updated_at: string
}

export interface GatewayKeyReveal {
  key: string
}

export interface UpstreamGroupKey {
  id: number
  channel_id: number
  channel_name?: string
  channel_type: ChannelType
  client_format?: "openai" | "claude" | "any" | string
  request_mode?: "responses" | "chat" | string
  group_ref: string
  group_name: string
  group_description?: string
  ratio: number
  priority: number
  charity?: boolean
  enabled: boolean
  upstream_key_id: number
  status: "unknown" | "alive" | "dead" | "disabled" | string
  concurrency_limit: number
  failure_count: number
  prompt_tokens: number
  completion_tokens: number
  total_tokens: number
  last_checked_at?: string | null
  last_latency_ms?: number
  last_success_at?: string | null
  last_used_at?: string | null
  disabled_until?: string | null
  last_error?: string
  created_at: string
  updated_at: string
}

export interface UsageLog {
  id: number
  gateway_key_id?: number
  gateway_key_name?: string
  channel_id?: number
  channel_name?: string
  group_name?: string
  model?: string
  client_format?: string
  prompt_tokens: number
  completion_tokens: number
  total_tokens: number
  ratio?: number
  created_at: string
}

export interface UsageLogsResponse {
  items: UsageLog[]
  total: number
}

export interface GatewayBootstrapResult {
  created: number
  updated: number
  skipped: number
  failed: number
  items: Array<{
    channel_id: number
    channel_name: string
    group_ref: string
    group_name: string
    ratio: number
    created: boolean
    error?: string
  }>
}

export interface GatewayHealthResult {
  checked: number
  alive: number
  dead: number
  items: Array<{
    id: number
    channel_id: number
    channel_name: string
    group_ref: string
    group_name: string
    ratio: number
    status: string
    latency_ms: number
    error?: string
    checked_at?: string | null
  }>
}
