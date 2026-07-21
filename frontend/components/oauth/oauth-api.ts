import { apiFetch } from "@/lib/api"
import type {
  OAuthAccount,
  OAuthAccountFilter,
  OAuthAccountPage,
  OAuthAccountStatus,
  OAuthBatchDeleteResult,
  OAuthImportItem,
  OAuthImportResult,
  OAuthInspectionJob,
  OAuthPoolKind,
  OAuthPoolStats,
} from "@/components/oauth/types"

type UnknownRecord = Record<string, unknown>

function record(value: unknown): UnknownRecord {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? (value as UnknownRecord)
    : {}
}

function textValue(value: unknown, fallback = ""): string {
  if (typeof value === "string") return value
  if (typeof value === "number" || typeof value === "bigint") return String(value)
  return fallback
}

function numberValue(value: unknown, fallback = 0): number {
  const parsed = typeof value === "number" ? value : Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

function booleanValue(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined
}

function first(source: UnknownRecord, keys: string[]): unknown {
  for (const key of keys) {
    if (source[key] !== undefined && source[key] !== null) return source[key]
  }
  return undefined
}

function normalizeStatus(value: unknown): OAuthAccountStatus {
  const status = textValue(value).trim().toLowerCase().replaceAll("-", "_")
  if (["unchecked", "untested", "pending"].includes(status)) return "unchecked"
  if (["alive", "healthy", "active", "ok", "available"].includes(status)) return "alive"
  if (["rate_limited", "ratelimited", "throttled", "limited"].includes(status)) return "rate_limited"
  if (["dead", "invalid", "expired", "disabled", "revoked"].includes(status)) return "dead"
  if (["cooling", "cooldown", "cool_down"].includes(status)) return "cooling"
  if (["temporary_unavailable", "unavailable", "temporary_error"].includes(status)) return "temporary_unavailable"
  if (["checking", "testing", "probing"].includes(status)) return "checking"
  return "unknown"
}

function normalizeAccount(value: unknown, pool: OAuthPoolKind): OAuthAccount {
  const source = record(value)
  const nestedQuota = record(first(source, ["quota", "usage", "limit_info"]))
  const quotaSource = Object.keys(nestedQuota).length > 0 ? nestedQuota : source
  const status = normalizeStatus(first(source, ["status", "health_status", "state"]))
  const explicitSchedulable = booleanValue(
    first(source, ["schedulable", "available_for_polling", "can_schedule", "eligible"]),
  )
  const enabled = booleanValue(source.enabled) ?? true
  const inRotation = booleanValue(first(source, ["in_rotation", "inRotation"])) ?? false
  const disabledUntilRaw = textValue(first(source, ["disabled_until", "cooldown_until"]))
  const disabledUntil = disabledUntilRaw ? new Date(disabledUntilRaw) : null
  const cooldownActive = disabledUntil != null && !Number.isNaN(disabledUntil.getTime()) && disabledUntil.getTime() > Date.now()
  const id = textValue(first(source, ["id", "account_id", "uuid"]))

  return {
    id,
    pool,
    displayName: textValue(first(source, ["display_name", "name", "label"]), "OAuth 账号"),
    maskedIdentifier: textValue(
      first(source, ["masked_identifier", "masked_account", "email", "username", "account"]),
      id,
    ),
    sourceFormat: textValue(first(source, ["source_format", "format", "source"]), "auto"),
    status,
    enabled,
    inRotation,
    quota: first(source, ["quota", "usage", "limit_info", "quota_used", "quota_limit", "quota_display"]) !== undefined
      ? {
          used: numberValue(first(quotaSource, ["used", "usage", "quota_used"]), Number.NaN),
          limit: numberValue(first(quotaSource, ["limit", "total", "quota_limit"]), Number.NaN),
          remaining: numberValue(first(quotaSource, ["remaining", "available"]), Number.NaN),
          unit: textValue(first(quotaSource, ["unit", "currency", "quota_unit"])),
          display: textValue(first(source, ["quota_display", "usage_display"])) || textValue(quotaSource.display),
          resetAt: textValue(first(quotaSource, ["reset_at", "quota_reset_at"])) || undefined,
        }
      : undefined,
    lastCheckedAt: textValue(first(source, ["last_checked_at", "last_health_check_at", "checked_at"])) || undefined,
    lastError: textValue(first(source, ["last_error", "error_message", "health_error"])) || undefined,
    schedulable: explicitSchedulable ?? (enabled && inRotation && status === "alive" && !cooldownActive),
    schedulableReason: textValue(first(source, ["schedulable_reason", "unavailable_reason", "polling_reason"])) || undefined,
    createdAt: textValue(first(source, ["created_at", "imported_at"])) || undefined,
    updatedAt: textValue(source.updated_at) || undefined,
    disabledUntil: disabledUntilRaw || undefined,
  }
}

function normalizeStats(value: unknown): OAuthPoolStats {
  const source = record(value)
  const total = numberValue(first(source, ["total", "total_accounts", "account_count"]))
  const rateLimited = numberValue(first(source, ["rate_limited", "rate_limited_accounts", "limited_count"]))
  const dead = numberValue(first(source, ["dead", "dead_accounts", "invalid_count"]))
  const cooling = numberValue(first(source, ["cooling", "cooldown_accounts", "cooling_count"]))
  const unchecked = numberValue(first(source, ["unchecked", "unchecked_accounts", "pending_count"]))
  const schedulable = numberValue(
    first(source, ["schedulable", "schedulable_accounts", "available", "available_accounts"]),
  )
  return {
    total,
    alive: first(source, ["alive", "alive_accounts", "healthy_count"]) === undefined
      ? Math.max(0, total - rateLimited - dead - cooling - unchecked)
      : numberValue(first(source, ["alive", "alive_accounts", "healthy_count"])),
    rateLimited,
    dead,
    cooling,
    temporaryUnavailable: first(source, ["temporary_unavailable", "temporary_unavailable_accounts", "unavailable_count"]) === undefined
      ? -1
      : numberValue(first(source, ["temporary_unavailable", "temporary_unavailable_accounts", "unavailable_count"])),
    unchecked,
    schedulable,
    status: textValue(first(source, ["status", "pool_status"]), schedulable > 0 ? "available" : "unavailable"),
  }
}

function normalizeImportItem(value: unknown): OAuthImportItem {
  const source = record(value)
  const rawStatus = textValue(first(source, ["status", "result"])).toLowerCase()
  const status = ["success", "imported", "created", "updated"].includes(rawStatus)
    ? "success"
    : rawStatus === "duplicate" || rawStatus === "skipped"
      ? "duplicate"
      : "failed"
  const index = numberValue(source.index, -1)
  const sourceFormat = textValue(first(source, ["source_format", "format"]))
  const fallbackReference = index >= 0
    ? `${sourceFormat ? `${sourceFormat} · ` : ""}第 ${index + 1} 条`
    : "未命名账号"
  return {
    reference: textValue(first(source, ["masked_identifier", "reference", "account", "name"]), fallbackReference),
    status,
    reason: textValue(first(source, ["reason", "error", "message"])) || undefined,
    action: textValue(first(source, ["action", "operation"])) || undefined,
  }
}

function normalizeImportResult(value: unknown): OAuthImportResult {
  const source = record(value)
  const itemsSource = first(source, ["items", "results", "details"])
  const failuresSource = source.failures
  const items = Array.isArray(itemsSource)
    ? itemsSource.map(normalizeImportItem)
    : Array.isArray(failuresSource)
      ? failuresSource.map((item) => {
          const failure = record(item)
          return {
            reference: `第 ${numberValue(failure.index) + 1} 条`,
            status: "failed" as const,
            reason: textValue(failure.reason) || "导入失败",
          }
        })
      : []
  const success = numberValue(
    first(source, ["success", "succeeded", "imported"]),
    items.filter((item) => item.status === "success").length,
  )
  const updated = numberValue(source.updated)
  return {
    total: numberValue(source.total, items.length),
    success,
    duplicate: numberValue(first(source, ["duplicate", "duplicates", "skipped"]), items.filter((item) => item.status === "duplicate").length),
    created: source.created === undefined ? Math.max(0, success - updated) : numberValue(source.created),
    updated,
    failed: numberValue(first(source, ["failed", "failure"]), items.filter((item) => item.status === "failed").length),
    items,
    inspection: source.inspection == null ? undefined : normalizeInspection(source.inspection),
  }
}

function normalizeInspection(value: unknown): OAuthInspectionJob {
  const source = record(value)
  const rawStatus = textValue(source.status, "running").toLowerCase()
  const status = ["queued", "running", "completed", "failed"].includes(rawStatus)
    ? (rawStatus as OAuthInspectionJob["status"])
    : "running"
  const alive = numberValue(source.alive)
  const limited = numberValue(first(source, ["limited", "rate_limited"]))
  const dead = numberValue(source.dead)
  const cooling = numberValue(source.cooling)
  const failed = numberValue(source.failed)
  return {
    id: textValue(first(source, ["id", "job_id"])),
    status,
    total: numberValue(source.total),
    completed: numberValue(first(source, ["completed", "processed"])),
    succeeded: numberValue(first(source, ["succeeded", "success"]), alive + limited + dead + cooling),
    alive,
    limited,
    dead,
    cooling,
    failed,
    currentAccount: textValue(first(source, ["current_account", "current_masked_identifier"])) || undefined,
    error: textValue(first(source, ["error", "message", "last_error"])) || undefined,
  }
}

export async function getOAuthPoolStats(pool: OAuthPoolKind): Promise<OAuthPoolStats> {
  return normalizeStats(await apiFetch<unknown>(`/oauth-accounts/${pool}/stats`))
}

export async function getOAuthAccounts(
  pool: OAuthPoolKind,
  params: { status: OAuthAccountFilter; page: number; pageSize: number },
): Promise<OAuthAccountPage> {
  const query = new URLSearchParams({
    page: String(params.page),
    page_size: String(params.pageSize),
  })
  if (params.status !== "all") query.set("status", params.status)
  const raw = await apiFetch<unknown>(`/oauth-accounts/${pool}?${query.toString()}`)
  const source = record(raw)
  const rawItems = first(source, ["items", "accounts", "data"])
  const items = Array.isArray(rawItems)
    ? rawItems.map((item) => normalizeAccount(item, pool)).filter((item) => item.id)
    : []
  return {
    items,
    total: numberValue(source.total, items.length),
    page: numberValue(source.page, params.page),
    pageSize: numberValue(first(source, ["page_size", "pageSize"]), params.pageSize),
  }
}

export async function importOAuthAccounts(
  pool: OAuthPoolKind,
  fileName: string,
  payload: unknown,
): Promise<OAuthImportResult> {
  const raw = await apiFetch<unknown>(`/oauth-accounts/${pool}/import`, {
    method: "POST",
    headers: { "X-Import-Filename": fileName },
    body: JSON.stringify(payload),
  })
  return normalizeImportResult(raw)
}

export async function checkOAuthAccount(
  pool: OAuthPoolKind,
  accountID: string,
): Promise<{ success: boolean; status: OAuthAccountStatus; error?: string }> {
  const source = record(await apiFetch<unknown>(
    `/oauth-accounts/${pool}/${encodeURIComponent(accountID)}/check`,
    { method: "POST" },
  ))
  return {
    success: booleanValue(source.success) ?? false,
    status: normalizeStatus(source.status),
    error: textValue(source.error) || undefined,
  }
}

export async function queryOAuthAccountQuota(
  pool: OAuthPoolKind,
  accountID: string,
): Promise<OAuthAccount> {
  const source = record(await apiFetch<unknown>(
    `/oauth-accounts/${pool}/${encodeURIComponent(accountID)}/quota`,
    { method: "POST" },
  ))
  const account = normalizeAccount(source.account, pool)
  if (!account.id) throw new Error("额度接口未返回账号状态")
  return account
}

export async function deleteOAuthAccount(pool: OAuthPoolKind, accountID: string): Promise<void> {
  await apiFetch(`/oauth-accounts/${pool}/${encodeURIComponent(accountID)}`, { method: "DELETE" })
}

export async function batchDeleteOAuthAccounts(
  pool: OAuthPoolKind,
  accountIDs: string[],
): Promise<OAuthBatchDeleteResult> {
  const raw = record(await apiFetch<unknown>(`/oauth-accounts/${pool}/batch-delete`, {
    method: "POST",
    body: JSON.stringify({ ids: accountIDs.map(Number) }),
  }))
  const failedIDs = first(raw, ["failed_ids", "failures"])
  const failures = Array.isArray(failedIDs)
    ? failedIDs.map((item) => {
        const source = record(item)
        return typeof item === "object"
          ? { id: textValue(source.id), reason: textValue(first(source, ["reason", "error"]), "删除失败") }
          : { id: String(item), reason: "删除失败" }
      })
    : undefined
  return {
    success: numberValue(first(raw, ["success", "succeeded", "deleted"])),
    failed: numberValue(raw.failed, failures?.length ?? 0),
    failures,
  }
}

export async function startOAuthInspection(pool: OAuthPoolKind): Promise<OAuthInspectionJob> {
  return normalizeInspection(await apiFetch<unknown>(`/oauth-accounts/${pool}/inspect`, { method: "POST" }))
}

export async function getOAuthInspection(pool: OAuthPoolKind): Promise<OAuthInspectionJob | null> {
  const raw = await apiFetch<unknown>(`/oauth-accounts/${pool}/inspect`)
  return raw == null ? null : normalizeInspection(raw)
}
