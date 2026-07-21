export type OAuthPoolKind = "chatgpt" | "grok"

export type OAuthAccountStatus =
  | "unchecked"
  | "alive"
  | "rate_limited"
  | "dead"
  | "cooling"
  | "temporary_unavailable"
  | "checking"
  | "unknown"

export type OAuthAccountFilter = "all" | "alive" | "rate_limited" | "dead"

export interface OAuthPoolStats {
  total: number
  alive: number
  rateLimited: number
  dead: number
  cooling: number
  temporaryUnavailable: number
  unchecked: number
  schedulable: number
  status: string
}

export interface OAuthAccountQuota {
  used?: number
  limit?: number
  remaining?: number
  unit?: string
  display?: string
  resetAt?: string
}

export interface OAuthAccount {
  id: string
  pool: OAuthPoolKind
  displayName: string
  maskedIdentifier: string
  sourceFormat: string
  status: OAuthAccountStatus
  enabled: boolean
  inRotation: boolean
  quota?: OAuthAccountQuota
  lastCheckedAt?: string
  lastError?: string
  schedulable: boolean
  schedulableReason?: string
  createdAt?: string
  updatedAt?: string
  disabledUntil?: string
}

export interface OAuthAccountPage {
  items: OAuthAccount[]
  total: number
  page: number
  pageSize: number
}

export type OAuthImportItemStatus = "success" | "duplicate" | "failed"

export interface OAuthImportItem {
  reference: string
  status: OAuthImportItemStatus
  reason?: string
  action?: "created" | "updated" | string
}

export interface OAuthImportResult {
  total: number
  success: number
  duplicate: number
  created: number
  updated: number
  failed: number
  items: OAuthImportItem[]
  inspection?: OAuthInspectionJob
}

export interface OAuthInspectionJob {
  id: string
  status: "queued" | "running" | "completed" | "failed"
  total: number
  completed: number
  succeeded: number
  alive: number
  limited: number
  dead: number
  cooling: number
  failed: number
  currentAccount?: string
  error?: string
}

export interface OAuthBatchDeleteResult {
  success: number
  failed: number
  failures?: Array<{ id: string; reason: string }>
}

export interface OAuthPoolQuery {
  status: OAuthAccountFilter
  page: number
  pageSize: 10 | 50 | 100 | 200
}
