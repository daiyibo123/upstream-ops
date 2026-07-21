package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	maxOAuthImportBytes    = 10 << 20
	maxOAuthImportAccounts = 2000
	maxOAuthCredentialSize = 256 << 10
	maxOAuthCookieSize     = 64 << 10
	maxOAuthTokenSize      = 128 << 10
	maxOAuthErrorRunes     = 4096
)

type OAuthPool string

const (
	OAuthPoolChatGPT OAuthPool = "chatgpt"
	OAuthPoolGrok    OAuthPool = "grok"
)

func ParseOAuthPool(value string) (OAuthPool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "chatgpt", "gpt", "openai", "codex":
		return OAuthPoolChatGPT, nil
	case "grok", "xai", "x.ai":
		return OAuthPoolGrok, nil
	default:
		return "", fmt.Errorf("unsupported OAuth pool %q", value)
	}
}

type OAuthAccountStatus string

const (
	OAuthStatusUnchecked   OAuthAccountStatus = "unchecked"
	OAuthStatusAlive       OAuthAccountStatus = "alive"
	OAuthStatusRateLimited OAuthAccountStatus = "rate_limited"
	OAuthStatusCooling     OAuthAccountStatus = "cooling"
	OAuthStatusDead        OAuthAccountStatus = "dead"
)

type OAuthAccount struct {
	ID               uint               `gorm:"primaryKey" json:"id"`
	Pool             OAuthPool          `gorm:"size:32;not null;uniqueIndex:idx_oauth_accounts_pool_identity,priority:1;index:idx_oauth_accounts_schedulable,priority:1" json:"pool"`
	IdentityHash     string             `gorm:"size:64;not null;uniqueIndex:idx_oauth_accounts_pool_identity,priority:2" json:"-"`
	WeakIdentity     bool               `gorm:"not null;default:false" json:"weak_identity"`
	SourceFormat     string             `gorm:"size:64;not null" json:"source_format"`
	ExternalID       string             `gorm:"size:255;index" json:"external_id,omitempty"`
	Email            string             `gorm:"size:320;index" json:"email,omitempty"`
	DisplayName      string             `gorm:"size:255" json:"display_name,omitempty"`
	CredentialCipher string             `gorm:"type:text;not null" json:"-"`
	Status           OAuthAccountStatus `gorm:"size:32;not null;default:unchecked;index:idx_oauth_accounts_schedulable,priority:2" json:"status"`
	Enabled          bool               `gorm:"not null;default:true;index:idx_oauth_accounts_schedulable,priority:3" json:"enabled"`
	InRotation       bool               `gorm:"not null;default:false;index:idx_oauth_accounts_schedulable,priority:4" json:"in_rotation"`
	QuotaUsed        *float64           `json:"quota_used,omitempty"`
	QuotaLimit       *float64           `json:"quota_limit,omitempty"`
	QuotaUnit        string             `gorm:"size:64" json:"quota_unit,omitempty"`
	QuotaResetAt     *time.Time         `json:"quota_reset_at,omitempty"`
	LastCheckedAt    *time.Time         `json:"last_checked_at,omitempty"`
	LastError        string             `gorm:"type:text" json:"last_error,omitempty"`
	ConsecutiveFails int                `gorm:"not null;default:0" json:"consecutive_failures"`
	DisabledUntil    *time.Time         `gorm:"index:idx_oauth_accounts_schedulable,priority:5" json:"disabled_until,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

func (OAuthAccount) TableName() string { return "oauth_accounts" }

func (a OAuthAccount) CurrentlySchedulable(now time.Time) bool {
	return a.Enabled && a.InRotation && a.Status == OAuthStatusAlive &&
		(a.DisabledUntil == nil || !a.DisabledUntil.After(now)) && strings.TrimSpace(a.CredentialCipher) != ""
}

type OAuthCredentials struct {
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
	Cookie       string `json:"cookie,omitempty"`
	SSOToken     string `json:"sso_token,omitempty"`

	ClientID      string `json:"client_id,omitempty"`
	TokenType     string `json:"token_type,omitempty"`
	ExpiresIn     int64  `json:"expires_in,omitempty"`
	ExpiresAt     string `json:"expired,omitempty"`
	LastRefresh   string `json:"last_refresh,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
	Subject       string `json:"subject,omitempty"`
	UserID        string `json:"user_id,omitempty"`
	TeamID        string `json:"team_id,omitempty"`
	Organization  string `json:"organization,omitempty"`
	PlanType      string `json:"plan_type,omitempty"`
	BaseURL       string `json:"base_url,omitempty"`
	TokenEndpoint string `json:"token_endpoint,omitempty"`
	AuthKind      string `json:"auth_kind,omitempty"`
	UsingAPI      *bool  `json:"using_api,omitempty"`
}

type OAuthImportFailure struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

type OAuthImportItem struct {
	Index        int    `json:"index"`
	Status       string `json:"status"`
	AccountID    uint   `json:"account_id,omitempty"`
	SourceFormat string `json:"source_format,omitempty"`
	WeakIdentity bool   `json:"weak_identity,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

type OAuthImportResult struct {
	Total      int                  `json:"total"`
	Succeeded  int                  `json:"succeeded"`
	Created    int                  `json:"created"`
	Updated    int                  `json:"updated"`
	Duplicates int                  `json:"duplicates"`
	Failed     int                  `json:"failed"`
	Items      []OAuthImportItem    `json:"items"`
	Failures   []OAuthImportFailure `json:"failures,omitempty"`
}

type OAuthAccountStats struct {
	Total       int64 `json:"total"`
	Available   int64 `json:"available"`
	RateLimited int64 `json:"rate_limited"`
	Dead        int64 `json:"dead"`
	Cooling     int64 `json:"cooling"`
	Unchecked   int64 `json:"unchecked"`
}

type OAuthHealthResult struct {
	Status        OAuthAccountStatus `json:"status"`
	Schedulable   bool               `json:"schedulable"`
	Error         string             `json:"error,omitempty"`
	DisabledUntil *time.Time         `json:"disabled_until,omitempty"`
	QuotaUsed     *float64           `json:"quota_used,omitempty"`
	QuotaLimit    *float64           `json:"quota_limit,omitempty"`
	QuotaUnit     string             `json:"quota_unit,omitempty"`
	QuotaResetAt  *time.Time         `json:"quota_reset_at,omitempty"`
	Transient     bool               `json:"-"`
}

type OAuthBatchDeleteResult struct {
	Requested int             `json:"requested"`
	Succeeded int             `json:"succeeded"`
	Failed    int             `json:"failed"`
	Failures  map[uint]string `json:"failures,omitempty"`
}

type OAuthAccounts struct {
	db     *gorm.DB
	cipher *appcrypto.Cipher
}

func NewOAuthAccounts(db *gorm.DB, cipher *appcrypto.Cipher) *OAuthAccounts {
	return &OAuthAccounts{db: db, cipher: cipher}
}

func AutoMigrateOAuthAccounts(db *gorm.DB) error {
	if db == nil {
		return errors.New("OAuth account database is nil")
	}
	return db.AutoMigrate(&OAuthAccount{})
}

func (r *OAuthAccounts) ImportJSON(pool OAuthPool, raw []byte) (OAuthImportResult, error) {
	var result OAuthImportResult
	if r == nil || r.db == nil || r.cipher == nil {
		return result, errors.New("OAuth account repository is not configured")
	}
	if _, err := ParseOAuthPool(string(pool)); err != nil {
		return result, err
	}
	items, envelope, err := decodeOAuthImport(raw)
	if err != nil {
		return result, err
	}
	result.Total = len(items)
	result.Items = make([]OAuthImportItem, 0, len(items))
	for index, item := range items {
		parsed, parseErr := parseOAuthImportItem(pool, item, envelope)
		if parseErr != nil {
			reason := RedactSensitiveText(parseErr.Error())
			result.Failed++
			result.Items = append(result.Items, OAuthImportItem{Index: index, Status: "failed", Reason: reason})
			result.Failures = append(result.Failures, OAuthImportFailure{Index: index, Reason: reason})
			continue
		}
		account, action, saveErr := r.upsertImported(parsed)
		if saveErr != nil {
			reason := RedactOAuthCredentialValues(RedactSensitiveText(saveErr.Error()), parsed.credentials)
			result.Failed++
			result.Items = append(result.Items, OAuthImportItem{Index: index, Status: "failed", SourceFormat: parsed.source, WeakIdentity: parsed.weakIdentity, Reason: reason})
			result.Failures = append(result.Failures, OAuthImportFailure{Index: index, Reason: reason})
			continue
		}
		result.Succeeded++
		if action == "created" {
			result.Created++
		} else {
			result.Updated++
		}
		result.Items = append(result.Items, OAuthImportItem{Index: index, Status: action, AccountID: account.ID, SourceFormat: account.SourceFormat, WeakIdentity: account.WeakIdentity})
	}
	return result, nil
}

func (r *OAuthAccounts) List(pool OAuthPool, page, pageSize int, status OAuthAccountStatus, search string) ([]OAuthAccount, int64, error) {
	if r == nil || r.db == nil {
		return nil, 0, errors.New("OAuth account repository is not configured")
	}
	page, pageSize = normalizeOAuthPage(page, pageSize)
	query := r.db.Model(&OAuthAccount{}).Where("pool = ?", pool)
	if strings.TrimSpace(string(status)) != "" {
		query = query.Where("status = ?", status)
	}
	if search = strings.TrimSpace(search); search != "" {
		pattern := "%" + escapeLike(search) + "%"
		query = query.Where("email LIKE ? ESCAPE '\\' OR display_name LIKE ? ESCAPE '\\' OR external_id LIKE ? ESCAPE '\\'", pattern, pattern, pattern)
	}
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var accounts []OAuthAccount
	if err := query.Omit("credential_cipher").Order("id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&accounts).Error; err != nil {
		return nil, 0, err
	}
	return accounts, total, nil
}

func (r *OAuthAccounts) Stats(pool OAuthPool, now time.Time) (OAuthAccountStats, error) {
	var result OAuthAccountStats
	if r == nil || r.db == nil {
		return result, errors.New("OAuth account repository is not configured")
	}
	counts := []struct {
		destination *int64
		condition   string
		arguments   []any
	}{
		{destination: &result.Total},
		{&result.Available, "enabled = ? AND in_rotation = ? AND status = ? AND credential_cipher <> '' AND (disabled_until IS NULL OR disabled_until <= ?)", []any{true, true, OAuthStatusAlive, now}},
		{&result.RateLimited, "status = ?", []any{OAuthStatusRateLimited}},
		{&result.Dead, "status = ?", []any{OAuthStatusDead}},
		{&result.Cooling, "status = ?", []any{OAuthStatusCooling}},
		{&result.Unchecked, "status = ?", []any{OAuthStatusUnchecked}},
	}
	for _, count := range counts {
		query := r.db.Model(&OAuthAccount{}).Where("pool = ?", pool)
		if count.condition != "" {
			query = query.Where(count.condition, count.arguments...)
		}
		if err := query.Count(count.destination).Error; err != nil {
			return OAuthAccountStats{}, err
		}
	}
	return result, nil
}

func (r *OAuthAccounts) Find(pool OAuthPool, id uint) (OAuthAccount, error) {
	var account OAuthAccount
	if r == nil || r.db == nil {
		return account, errors.New("OAuth account repository is not configured")
	}
	err := r.db.Omit("credential_cipher").Where("pool = ? AND id = ?", pool, id).First(&account).Error
	return account, err
}

func (r *OAuthAccounts) Credentials(pool OAuthPool, id uint) (OAuthCredentials, error) {
	var account OAuthAccount
	if r == nil || r.db == nil || r.cipher == nil {
		return OAuthCredentials{}, errors.New("OAuth account repository is not configured")
	}
	if err := r.db.Select("credential_cipher").Where("pool = ? AND id = ?", pool, id).First(&account).Error; err != nil {
		return OAuthCredentials{}, err
	}
	plain, err := r.cipher.Decrypt(account.CredentialCipher)
	if err != nil {
		return OAuthCredentials{}, fmt.Errorf("decrypt OAuth credentials: %w", err)
	}
	var credentials OAuthCredentials
	if err := json.Unmarshal([]byte(plain), &credentials); err != nil {
		return OAuthCredentials{}, errors.New("stored OAuth credentials are invalid")
	}
	return credentials, nil
}

func (r *OAuthAccounts) ListSchedulable(pool OAuthPool, now time.Time, limit int) ([]OAuthAccount, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("OAuth account repository is not configured")
	}
	// Recovery candidates remain in the lightweight snapshot so the in-process
	// breaker can skip them until DisabledUntil and then grant one half-open
	// probe. Dead, unchecked, disabled, and credential-less rows stay excluded.
	query := r.db.Where(`pool = ? AND enabled = ? AND credential_cipher <> '' AND (
		(status = ? AND in_rotation = ? AND (disabled_until IS NULL OR disabled_until <= ?)) OR
		(status IN ? AND disabled_until IS NOT NULL)
	)`, pool, true, OAuthStatusAlive, true, now, []OAuthAccountStatus{OAuthStatusCooling, OAuthStatusRateLimited}).Order("id ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	var accounts []OAuthAccount
	if err := query.Find(&accounts).Error; err != nil {
		return nil, err
	}
	return accounts, nil
}

func (r *OAuthAccounts) ListIDs(pool OAuthPool) ([]uint, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("OAuth account repository is not configured")
	}
	var ids []uint
	if err := r.db.Model(&OAuthAccount{}).Where("pool = ?", pool).Order("id ASC").Pluck("id", &ids).Error; err != nil {
		return nil, err
	}
	return ids, nil
}

func (r *OAuthAccounts) ApplyHealthResult(pool OAuthPool, id uint, result OAuthHealthResult, checkedAt time.Time) error {
	if r == nil || r.db == nil {
		return errors.New("OAuth account repository is not configured")
	}
	if !validOAuthStatus(result.Status) {
		return fmt.Errorf("invalid OAuth account status %q", result.Status)
	}
	checkedAt = checkedAt.UTC()
	schedulable := result.Schedulable && result.Status == OAuthStatusAlive &&
		(result.DisabledUntil == nil || !result.DisabledUntil.After(checkedAt))
	updates := map[string]any{
		"status":          result.Status,
		"in_rotation":     schedulable,
		"last_checked_at": checkedAt,
		"last_error":      truncateUTF8(RedactSensitiveText(result.Error), maxOAuthErrorRunes),
		"disabled_until":  result.DisabledUntil,
		"quota_used":      result.QuotaUsed,
		"quota_limit":     result.QuotaLimit,
		"quota_unit":      strings.TrimSpace(result.QuotaUnit),
		"quota_reset_at":  result.QuotaResetAt,
	}
	if result.Status == OAuthStatusAlive {
		updates["consecutive_fails"] = 0
		updates["last_error"] = ""
		updates["disabled_until"] = nil
	} else {
		updates["consecutive_fails"] = gorm.Expr("consecutive_fails + 1")
	}
	response := r.db.Model(&OAuthAccount{}).Where("pool = ? AND id = ?", pool, id).Updates(updates)
	if response.Error != nil {
		return response.Error
	}
	if response.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// UpdateQuota records a best-effort quota refresh without changing health,
// breaker, or rotation state. Quota endpoints can fail independently from the
// credential's ability to serve normal requests.
func (r *OAuthAccounts) UpdateQuota(pool OAuthPool, id uint, used, limit *float64, unit string, resetAt *time.Time) error {
	if r == nil || r.db == nil {
		return errors.New("OAuth account repository is not configured")
	}
	response := r.db.Model(&OAuthAccount{}).Where("pool = ? AND id = ?", pool, id).Updates(map[string]any{
		"quota_used": used, "quota_limit": limit, "quota_unit": strings.TrimSpace(unit), "quota_reset_at": resetAt,
	})
	if response.Error != nil {
		return response.Error
	}
	if response.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

// RecordRuntimeFailure persists a dispatch failure using the same three-strike
// policy as the process-local scheduler. Cooling is treated as a transient
// failure; dead and rate_limited take effect immediately.
func (r *OAuthAccounts) RecordRuntimeFailure(pool OAuthPool, id uint, status OAuthAccountStatus, failure string, disabledUntil *time.Time, threshold int) error {
	if r == nil || r.db == nil {
		return errors.New("OAuth account repository is not configured")
	}
	if threshold <= 0 {
		threshold = 3
	}
	if status != OAuthStatusCooling && status != OAuthStatusDead && status != OAuthStatusRateLimited {
		return fmt.Errorf("invalid runtime failure status %q", status)
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		var account OAuthAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("pool = ? AND id = ?", pool, id).First(&account).Error; err != nil {
			return err
		}
		failures := account.ConsecutiveFails + 1
		updates := map[string]any{
			"consecutive_fails": failures,
			"last_error":        truncateUTF8(RedactSensitiveText(failure), maxOAuthErrorRunes),
		}
		if status == OAuthStatusDead || status == OAuthStatusRateLimited || failures >= threshold {
			updates["status"] = strongerOAuthFailureStatus(account.Status, status)
			updates["in_rotation"] = false
			if retained := cloneLaterTime(account.DisabledUntil, disabledUntil); retained != nil {
				updates["disabled_until"] = retained
			}
		}
		return tx.Model(&account).Updates(updates).Error
	})
}

func strongerOAuthFailureStatus(current, proposed OAuthAccountStatus) OAuthAccountStatus {
	if oauthFailureStatusSeverity(current) > oauthFailureStatusSeverity(proposed) {
		return current
	}
	return proposed
}

func oauthFailureStatusSeverity(status OAuthAccountStatus) int {
	switch status {
	case OAuthStatusDead:
		return 3
	case OAuthStatusRateLimited:
		return 2
	case OAuthStatusCooling:
		return 1
	default:
		return 0
	}
}

// RecordRuntimeSuccess closes a successful recovery probe or request. Disabled
// operator accounts remain disabled and therefore outside rotation.
func (r *OAuthAccounts) RecordRuntimeSuccess(pool OAuthPool, id uint, checkedAt time.Time) error {
	if r == nil || r.db == nil {
		return errors.New("OAuth account repository is not configured")
	}
	return r.db.Transaction(func(tx *gorm.DB) error {
		var account OAuthAccount
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("pool = ? AND id = ?", pool, id).First(&account).Error; err != nil {
			return err
		}
		return tx.Model(&account).Updates(map[string]any{
			"status": OAuthStatusAlive, "in_rotation": account.Enabled, "consecutive_fails": 0,
			"disabled_until": nil, "last_error": "", "last_checked_at": checkedAt.UTC(),
		}).Error
	})
}

func (r *OAuthAccounts) Delete(pool OAuthPool, id uint) error {
	if r == nil || r.db == nil {
		return errors.New("OAuth account repository is not configured")
	}
	response := r.db.Where("pool = ? AND id = ?", pool, id).Delete(&OAuthAccount{})
	if response.Error != nil {
		return response.Error
	}
	if response.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (r *OAuthAccounts) BatchDelete(pool OAuthPool, ids []uint) (OAuthBatchDeleteResult, error) {
	result := OAuthBatchDeleteResult{Requested: len(ids)}
	if r == nil || r.db == nil {
		return result, errors.New("OAuth account repository is not configured")
	}
	unique := make([]uint, 0, len(ids))
	seen := make(map[uint]struct{}, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}
	result.Requested = len(unique)
	if len(unique) == 0 {
		return result, nil
	}
	var existing []uint
	if err := r.db.Model(&OAuthAccount{}).Where("pool = ? AND id IN ?", pool, unique).Pluck("id", &existing).Error; err != nil {
		return result, err
	}
	if len(existing) > 0 {
		response := r.db.Where("pool = ? AND id IN ?", pool, existing).Delete(&OAuthAccount{})
		if response.Error != nil {
			return result, response.Error
		}
		result.Succeeded = int(response.RowsAffected)
	}
	if result.Succeeded != len(unique) {
		result.Failures = make(map[uint]string)
		found := make(map[uint]bool, len(existing))
		for _, id := range existing {
			found[id] = true
		}
		for _, id := range unique {
			if !found[id] {
				result.Failures[id] = "account not found"
			}
		}
	}
	result.Failed = result.Requested - result.Succeeded
	return result, nil
}

type parsedOAuthImport struct {
	pool         OAuthPool
	credentials  OAuthCredentials
	source       string
	identityHash string
	weakIdentity bool
	externalID   string
	email        string
	displayName  string
}

func (r *OAuthAccounts) upsertImported(value parsedOAuthImport) (OAuthAccount, string, error) {
	rawCredentials, err := json.Marshal(value.credentials)
	if err != nil {
		return OAuthAccount{}, "", err
	}
	if len(rawCredentials) > maxOAuthCredentialSize {
		return OAuthAccount{}, "", errors.New("OAuth credentials are too large")
	}
	ciphertext, err := r.cipher.Encrypt(string(rawCredentials))
	if err != nil {
		return OAuthAccount{}, "", errors.New("encrypt OAuth credentials")
	}
	pool := value.pool
	var saved OAuthAccount
	action := "created"
	err = r.db.Transaction(func(tx *gorm.DB) error {
		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("pool = ? AND identity_hash = ?", pool, value.identityHash).First(&saved).Error
		switch {
		case findErr == nil:
			action = "updated"
			updates := map[string]any{
				"weak_identity": value.weakIdentity, "source_format": value.source,
				"external_id": value.externalID, "email": value.email, "display_name": value.displayName,
				"credential_cipher": ciphertext, "status": OAuthStatusUnchecked, "in_rotation": false,
				"quota_used": nil, "quota_limit": nil, "quota_unit": "", "quota_reset_at": nil,
				"last_checked_at": nil, "last_error": "", "consecutive_fails": 0, "disabled_until": nil,
			}
			if err := tx.Model(&saved).Updates(updates).Error; err != nil {
				return err
			}
			return tx.Where("id = ?", saved.ID).First(&saved).Error
		case !errors.Is(findErr, gorm.ErrRecordNotFound):
			return findErr
		}
		saved = OAuthAccount{
			Pool: pool, IdentityHash: value.identityHash, WeakIdentity: value.weakIdentity,
			SourceFormat: value.source, ExternalID: value.externalID, Email: value.email,
			DisplayName: value.displayName, CredentialCipher: ciphertext,
			Status: OAuthStatusUnchecked, Enabled: true, InRotation: false,
		}
		if createErr := tx.Create(&saved).Error; createErr != nil {
			// MySQL/SQLite can both race between the lookup and insert. A unique
			// conflict is resolved by a fresh update instead of creating duplicates.
			var concurrent OAuthAccount
			if lookupErr := tx.Where("pool = ? AND identity_hash = ?", pool, value.identityHash).First(&concurrent).Error; lookupErr != nil {
				return createErr
			}
			action = "updated"
			saved = concurrent
			return tx.Model(&saved).Updates(map[string]any{
				"weak_identity": value.weakIdentity, "source_format": value.source,
				"external_id": value.externalID, "email": value.email, "display_name": value.displayName,
				"credential_cipher": ciphertext, "status": OAuthStatusUnchecked, "in_rotation": false,
				"quota_used": nil, "quota_limit": nil, "quota_unit": "", "quota_reset_at": nil,
				"last_checked_at": nil, "last_error": "", "consecutive_fails": 0, "disabled_until": nil,
			}).Error
		}
		return nil
	})
	return saved, action, err
}

func decodeOAuthImport(raw []byte) ([]map[string]any, map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil, errors.New("OAuth import JSON is empty")
	}
	if len(raw) > maxOAuthImportBytes {
		return nil, nil, fmt.Errorf("OAuth import JSON exceeds %d bytes", maxOAuthImportBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return nil, nil, fmt.Errorf("invalid OAuth import JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("OAuth import JSON contains trailing values")
	}
	var values []any
	envelope := map[string]any{}
	switch typed := root.(type) {
	case []any:
		values = typed
	case map[string]any:
		envelope = typed
		if accounts, ok := lookupSlice(typed, "accounts"); ok {
			values = accounts
		} else if data, ok := lookupMap(typed, "data"); ok {
			if accounts, exists := lookupSlice(data, "accounts"); exists {
				values = accounts
			} else {
				values = []any{typed}
			}
		} else {
			values = []any{typed}
		}
	default:
		return nil, nil, errors.New("OAuth import must be an account object or account array")
	}
	if len(values) == 0 {
		return nil, nil, errors.New("OAuth import contains no accounts")
	}
	if len(values) > maxOAuthImportAccounts {
		return nil, nil, fmt.Errorf("OAuth import contains more than %d accounts", maxOAuthImportAccounts)
	}
	items := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if item, ok := value.(map[string]any); ok {
			items = append(items, item)
		} else {
			items = append(items, map[string]any{"__invalid_account_value": value})
		}
	}
	return items, envelope, nil
}

func parseOAuthImportItem(pool OAuthPool, item, envelope map[string]any) (parsedOAuthImport, error) {
	if _, invalid := item["__invalid_account_value"]; invalid {
		return parsedOAuthImport{}, errors.New("account entry must be a JSON object")
	}
	nested := []map[string]any{item}
	for _, key := range []string{"credentials", "credential", "auth", "tokens", "session"} {
		if value, ok := lookupMap(item, key); ok {
			nested = append(nested, value)
		}
	}
	get := func(keys ...string) any {
		for _, key := range keys {
			for _, source := range nested {
				if value, ok := lookupAny(source, key); ok {
					return value
				}
			}
		}
		return nil
	}
	declared := strings.ToLower(strings.TrimSpace(stringValue(get("type", "provider", "service", "platform"))))
	if pool == OAuthPoolChatGPT && containsAnyWord(declared, "grok", "xai", "x.ai") {
		return parsedOAuthImport{}, errors.New("Grok/xAI credential cannot be imported into the ChatGPT pool")
	}
	if pool == OAuthPoolGrok && containsAnyWord(declared, "chatgpt", "openai", "codex") {
		return parsedOAuthImport{}, errors.New("ChatGPT/OpenAI credential cannot be imported into the Grok pool")
	}

	credentials := OAuthCredentials{
		AccessToken:   cleanSecret(stringValue(get("access_token", "accessToken", "token"))),
		RefreshToken:  cleanSecret(stringValue(get("refresh_token", "refreshToken"))),
		IDToken:       cleanSecret(stringValue(get("id_token", "idToken"))),
		SessionToken:  cleanSecret(stringValue(get("session_token", "sessionToken", "session_access_token"))),
		SSOToken:      cleanSecret(stringValue(get("sso_token", "ssoToken", "sso", "sso-rw"))),
		ClientID:      strings.TrimSpace(stringValue(get("client_id", "clientId"))),
		TokenType:     strings.TrimSpace(stringValue(get("token_type", "tokenType"))),
		ExpiresIn:     int64Value(get("expires_in", "expiresIn")),
		ExpiresAt:     strings.TrimSpace(stringValue(get("expires_at", "expiresAt", "expired", "expiration", "expiry"))),
		LastRefresh:   strings.TrimSpace(firstString(stringValue(get("last_refresh", "lastRefresh", "refreshed_at")), stringValue(envelopeValue(envelope, "exported_at")))),
		AccountID:     strings.TrimSpace(stringValue(get("chatgpt_account_id", "account_id", "accountId"))),
		Subject:       strings.TrimSpace(stringValue(get("subject", "sub"))),
		UserID:        strings.TrimSpace(stringValue(get("chatgpt_user_id", "user_id", "userId"))),
		TeamID:        strings.TrimSpace(stringValue(get("team_id", "teamId"))),
		Organization:  strings.TrimSpace(stringValue(get("organization", "organization_id", "organizationId", "poid"))),
		PlanType:      strings.TrimSpace(stringValue(get("plan_type", "planType", "plan"))),
		BaseURL:       strings.TrimSpace(stringValue(get("base_url", "baseUrl", "api_base"))),
		TokenEndpoint: strings.TrimSpace(stringValue(get("token_endpoint", "tokenEndpoint"))),
		AuthKind:      strings.TrimSpace(stringValue(get("auth_kind", "authKind", "auth_type"))),
	}
	if usingAPI, ok := boolValue(get("using_api", "usingApi")); ok {
		credentials.UsingAPI = &usingAPI
	}
	cookieInput := get("cookie", "cookies")
	if extraCookies := oauthTopLevelCookies(item); len(extraCookies) > 0 {
		if cookieInput == nil {
			cookieInput = extraCookies
		} else {
			cookieInput = []any{cookieInput, extraCookies}
		}
	}
	cookie, err := normalizeCookie(cookieInput)
	if err != nil {
		return parsedOAuthImport{}, err
	}
	credentials.Cookie = cookie
	if credentials.SSOToken == "" {
		credentials.SSOToken = firstString(cookieNamedValue(cookie, "sso"), cookieNamedValue(cookie, "sso-rw"))
	}
	endpointHints := strings.ToLower(credentials.BaseURL + " " + credentials.TokenEndpoint)
	if pool == OAuthPoolChatGPT && (credentials.SSOToken != "" || strings.Contains(endpointHints, "x.ai")) {
		return parsedOAuthImport{}, errors.New("Grok/xAI credential cannot be imported into the ChatGPT pool")
	}
	if pool == OAuthPoolGrok && (strings.TrimSpace(stringValue(get("chatgpt_account_id"))) != "" ||
		strings.Contains(endpointHints, "chatgpt.com") || strings.Contains(endpointHints, "openai.com")) {
		return parsedOAuthImport{}, errors.New("ChatGPT/OpenAI credential cannot be imported into the Grok pool")
	}
	if err := validateCredentialStrings(credentials); err != nil {
		return parsedOAuthImport{}, err
	}
	claims, err := validateAndExtractClaims(credentials)
	if err != nil {
		return parsedOAuthImport{}, err
	}
	mergeOAuthClaims(&credentials, claims)
	if err := validateOAuthExpiry(credentials); err != nil {
		return parsedOAuthImport{}, err
	}
	if err := validateOAuthURLs(pool, credentials); err != nil {
		return parsedOAuthImport{}, err
	}
	if pool == OAuthPoolChatGPT {
		if credentials.AccessToken == "" && credentials.SessionToken == "" && credentials.Cookie == "" {
			return parsedOAuthImport{}, errors.New("ChatGPT account has no usable access token, session token, or cookie")
		}
	} else if credentials.AccessToken == "" && credentials.RefreshToken == "" && credentials.SSOToken == "" {
		return parsedOAuthImport{}, errors.New("Grok account has no usable access, refresh, or SSO token")
	}

	email := normalizeEmail(firstString(stringValue(get("email", "mail")), claimString(claims, "email")))
	displayName := strings.TrimSpace(firstString(stringValue(get("display_name", "displayName", "name")), claimString(claims, "name")))
	source := recognizeOAuthSource(pool, item, envelope, credentials)
	identityHash, weak, externalID := oauthIdentity(pool, email, displayName, credentials)
	return parsedOAuthImport{
		pool: pool, credentials: credentials, source: source, identityHash: identityHash,
		weakIdentity: weak, externalID: externalID, email: email, displayName: displayName,
	}, nil
}

func recognizeOAuthSource(pool OAuthPool, item, envelope map[string]any, credentials OAuthCredentials) string {
	if _, ok := lookupSlice(envelope, "accounts"); ok {
		if _, exported := lookupAny(envelope, "exported_at"); exported {
			return "sub2api"
		}
	}
	if data, ok := lookupMap(envelope, "data"); ok {
		if _, accounts := lookupSlice(data, "accounts"); accounts {
			return "sub2api"
		}
	}
	source := strings.ToLower(strings.TrimSpace(stringValue(firstLookup(item, "source", "client", "format"))))
	switch {
	case strings.Contains(source, "sub2api"):
		return "sub2api"
	case strings.Contains(source, "cli"):
		return "cliproxyapi"
	case strings.Contains(source, "cpa"):
		return "cpa"
	}
	declared := strings.ToLower(strings.TrimSpace(stringValue(firstLookup(item, "type", "provider"))))
	if pool == OAuthPoolGrok && (credentials.SSOToken != "" || strings.Contains(declared, "sso")) {
		if strings.Contains(source, "console") || strings.Contains(strings.ToLower(credentials.BaseURL), "console.x.ai") {
			return "grok_console"
		}
		return "grok_sso"
	}
	if strings.Contains(declared, "codex") || strings.Contains(declared, "xai") || strings.Contains(declared, "grok") {
		return "cliproxyapi"
	}
	if pool == OAuthPoolChatGPT && credentials.SessionToken != "" {
		return "chatgpt_session"
	}
	if pool == OAuthPoolGrok {
		return "grok_oauth"
	}
	return "chatgpt_oauth"
}

func oauthIdentity(pool OAuthPool, email, displayName string, credentials OAuthCredentials) (hash string, weak bool, externalID string) {
	kind, value := "", ""
	if pool == OAuthPoolChatGPT {
		switch {
		case credentials.AccountID != "":
			kind, value, externalID = "account", credentials.AccountID, credentials.AccountID
		case credentials.Subject != "":
			kind, value, externalID = "subject", credentials.Subject, credentials.Subject
		case email != "":
			kind, value = "email", email
		}
	} else {
		switch {
		case credentials.Subject != "":
			kind, value, externalID = "subject", credentials.Subject, credentials.Subject
		case credentials.UserID != "":
			kind, value, externalID = "user", credentials.UserID, credentials.UserID
		case email != "":
			kind, value = "email", email
		case credentials.TeamID != "":
			kind, value, externalID, weak = "team", credentials.TeamID, credentials.TeamID, true
		}
	}
	if kind == "" {
		metadata := strings.Join([]string{
			strings.ToLower(strings.TrimSpace(credentials.ClientID)), strings.ToLower(strings.TrimSpace(credentials.BaseURL)),
			strings.ToLower(strings.TrimSpace(credentials.TokenEndpoint)),
			strings.ToLower(strings.TrimSpace(credentials.Organization)), strings.ToLower(strings.TrimSpace(credentials.PlanType)),
			strings.ToLower(strings.TrimSpace(credentials.AuthKind)), strings.ToLower(strings.TrimSpace(displayName)),
		}, "\x00")
		if strings.Trim(metadata, "\x00") != "" {
			kind, value, weak = "metadata", metadata, true
		} else {
			kind, value, weak = "credential", credentialFingerprint(credentials), true
		}
	}
	digest := sha256.Sum256([]byte(string(pool) + "\x00" + kind + "\x00" + strings.ToLower(strings.TrimSpace(value))))
	return string(pool) + ":" + hex.EncodeToString(digest[:]), weak, externalID
}

func credentialFingerprint(credentials OAuthCredentials) string {
	value := firstString(credentials.AccessToken, credentials.RefreshToken, credentials.SessionToken, credentials.SSOToken, credentials.Cookie, credentials.IDToken)
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func validateAndExtractClaims(credentials OAuthCredentials) (map[string]any, error) {
	merged := make(map[string]any)
	if credentials.IDToken != "" {
		claims, err := decodeJWTClaims(credentials.IDToken, true)
		if err != nil {
			return nil, fmt.Errorf("invalid ID token: %w", err)
		}
		copyClaims(merged, claims)
	}
	for _, token := range []string{credentials.AccessToken, credentials.SessionToken} {
		if strings.Count(token, ".") != 2 {
			continue
		}
		claims, err := decodeJWTClaims(token, false)
		if err != nil {
			return nil, fmt.Errorf("invalid JWT credential: %w", err)
		}
		copyClaimsMissing(merged, claims)
	}
	return merged, nil
}

func decodeJWTClaims(token string, required bool) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		if required {
			return nil, errors.New("JWT must contain exactly three segments")
		}
		return nil, errors.New("JWT must contain exactly three segments")
	}
	for _, part := range parts {
		if len(part) > maxOAuthTokenSize {
			return nil, errors.New("JWT segment is too large")
		}
	}
	if parts[2] == "" {
		return nil, errors.New("JWT signature is empty")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.New("JWT header is not base64url")
	}
	var header map[string]any
	if json.Unmarshal(headerBytes, &header) != nil {
		return nil, errors.New("JWT header is not JSON")
	}
	if strings.EqualFold(strings.TrimSpace(stringValue(header["alg"])), "none") {
		return nil, errors.New("JWT alg none is forbidden")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("JWT payload is not base64url")
	}
	var claims map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if decoder.Decode(&claims) != nil {
		return nil, errors.New("JWT payload is not JSON")
	}
	if expiry := numericUnixTime(claims["exp"]); expiry != nil && !expiry.After(time.Now().UTC()) {
		return nil, errors.New("JWT credential is expired")
	}
	return claims, nil
}

func mergeOAuthClaims(credentials *OAuthCredentials, claims map[string]any) {
	auth, _ := lookupMap(claims, "https://api.openai.com/auth")
	credentials.AccountID = firstString(credentials.AccountID, stringValue(firstLookup(auth, "chatgpt_account_id", "account_id")))
	credentials.UserID = firstString(credentials.UserID, stringValue(firstLookup(auth, "chatgpt_user_id", "user_id")), claimString(claims, "user_id"))
	credentials.PlanType = firstString(credentials.PlanType, stringValue(firstLookup(auth, "chatgpt_plan_type", "plan_type")))
	credentials.Organization = firstString(credentials.Organization, stringValue(firstLookup(auth, "organization_id", "poid")), claimString(claims, "organization_id"))
	credentials.Subject = firstString(credentials.Subject, claimString(claims, "sub"))
	credentials.TeamID = firstString(credentials.TeamID, claimString(claims, "team_id"))
}

func validateOAuthExpiry(credentials OAuthCredentials) error {
	if strings.TrimSpace(credentials.ExpiresAt) != "" {
		expires, err := parseFlexibleTime(credentials.ExpiresAt)
		if err != nil {
			return errors.New("credential expiry is invalid")
		}
		if !expires.After(time.Now().UTC()) {
			return errors.New("credential is expired")
		}
	}
	if credentials.ExpiresIn < 0 {
		return errors.New("expires_in cannot be negative")
	}
	return nil
}

func validateOAuthURLs(pool OAuthPool, credentials OAuthCredentials) error {
	for label, raw := range map[string]string{"base URL": credentials.BaseURL, "token endpoint": credentials.TokenEndpoint} {
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.User != nil {
			return fmt.Errorf("%s is invalid", label)
		}
	}
	if pool == OAuthPoolGrok && credentials.TokenEndpoint != "" {
		parsed, _ := url.Parse(credentials.TokenEndpoint)
		host := strings.ToLower(parsed.Hostname())
		if parsed.Scheme != "https" || (host != "x.ai" && !strings.HasSuffix(host, ".x.ai")) {
			return errors.New("Grok token endpoint must use HTTPS on x.ai")
		}
	}
	return nil
}

func validateCredentialStrings(credentials OAuthCredentials) error {
	for name, value := range map[string]string{
		"access token": credentials.AccessToken, "refresh token": credentials.RefreshToken,
		"ID token": credentials.IDToken, "session token": credentials.SessionToken,
		"SSO token": credentials.SSOToken,
	} {
		if strings.ContainsAny(value, "\r\n\x00") {
			return fmt.Errorf("%s contains forbidden control characters", name)
		}
		if len(value) > maxOAuthTokenSize {
			return fmt.Errorf("%s is too large", name)
		}
	}
	return nil
}

func normalizeCookie(value any) (string, error) {
	if value == nil {
		return "", nil
	}
	values := make(map[string]string)
	add := func(name, item string) error {
		name, item = strings.TrimSpace(name), strings.TrimSpace(item)
		if strings.ContainsAny(name+item, "\r\n\x00") {
			return errors.New("cookie contains forbidden control characters")
		}
		if name == "" || item == "" {
			return nil
		}
		values[name] = item
		return nil
	}
	var ingest func(any) error
	ingest = func(raw any) error {
		switch typed := raw.(type) {
		case string:
			for _, part := range strings.Split(typed, ";") {
				name, item, ok := strings.Cut(strings.TrimSpace(part), "=")
				if !ok {
					continue
				}
				if err := add(name, item); err != nil {
					return err
				}
			}
		case map[string]any:
			if name := stringValue(firstLookup(typed, "name", "key")); name != "" {
				return add(name, stringValue(firstLookup(typed, "value", "val")))
			}
			for name, item := range typed {
				if err := add(name, stringValue(item)); err != nil {
					return err
				}
			}
		case []any:
			for _, item := range typed {
				if err := ingest(item); err != nil {
					return err
				}
			}
		default:
			return errors.New("cookie must be a string, object, or array")
		}
		return nil
	}
	if err := ingest(value); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	result := strings.Join(parts, "; ")
	if len(result) > maxOAuthCookieSize {
		return "", errors.New("cookie is too large")
	}
	return result, nil
}

func oauthTopLevelCookies(item map[string]any) map[string]any {
	result := make(map[string]any)
	for key, value := range item {
		name := strings.ToLower(strings.TrimSpace(key))
		switch {
		case name == "sso", name == "sso-rw", name == "cf_clearance", name == "__cf_bm", name == "_cfuvid", strings.HasPrefix(name, "cf_chl_"):
			if strings.TrimSpace(stringValue(value)) != "" {
				result[name] = value
			}
		}
	}
	return result
}

var sensitiveKeyPattern = regexp.MustCompile(`(?i)(authorization|proxy-authorization|access[_-]?token|refresh[_-]?token|id[_-]?token|session[_-]?token|sso(?:[_-]?token)?|cookie|set-cookie|password|passwd|secret|client[_-]?secret|api[_-]?key)`)
var bearerPattern = regexp.MustCompile(`(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+`)
var keyValueSecretPattern = regexp.MustCompile(`(?i)((?:authorization|proxy-authorization|access[_-]?token|refresh[_-]?token|id[_-]?token|session[_-]?token|sso(?:[_-]?token)?|cookie|set-cookie|password|passwd|secret|client[_-]?secret|api[_-]?key)\s*[:=]\s*)([^\s,;]+)`)
var cookieLinePattern = regexp.MustCompile(`(?im)((?:cookie|set-cookie)\s*[:=]\s*)[^\r\n]+`)
var proxyUserInfoPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^/@\s:]+:[^/@\s]+@`)

func RedactSensitiveText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var structured any
	if json.Unmarshal([]byte(value), &structured) == nil {
		redactStructuredValue(structured)
		if encoded, err := json.Marshal(structured); err == nil {
			value = string(encoded)
		}
	}
	value = bearerPattern.ReplaceAllString(value, "${1}[REDACTED]")
	value = cookieLinePattern.ReplaceAllString(value, "${1}[REDACTED]")
	value = keyValueSecretPattern.ReplaceAllString(value, "${1}[REDACTED]")
	value = proxyUserInfoPattern.ReplaceAllString(value, "${1}[REDACTED]@")
	return truncateUTF8(value, maxOAuthErrorRunes)
}

func RedactOAuthCredentialValues(value string, credentials OAuthCredentials) string {
	secrets := []string{credentials.AccessToken, credentials.RefreshToken, credentials.IDToken, credentials.SessionToken, credentials.SSOToken, credentials.Cookie}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return RedactSensitiveText(value)
}

func redactStructuredValue(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			if sensitiveKeyPattern.MatchString(key) {
				typed[key] = "[REDACTED]"
			} else {
				redactStructuredValue(item)
			}
		}
	case []any:
		for _, item := range typed {
			redactStructuredValue(item)
		}
	}
}

func validOAuthStatus(value OAuthAccountStatus) bool {
	switch value {
	case OAuthStatusUnchecked, OAuthStatusAlive, OAuthStatusRateLimited, OAuthStatusCooling, OAuthStatusDead:
		return true
	default:
		return false
	}
}

func normalizeOAuthPage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	switch pageSize {
	case 10, 50, 100, 200:
	default:
		pageSize = 10
	}
	return page, pageSize
}

func escapeLike(value string) string {
	return strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(value)
}

func cleanSecret(value string) string { return strings.TrimSpace(value) }

func normalizeEmail(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func truncateUTF8(value string, maxRunes int) string {
	if maxRunes <= 0 || utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes])
}

func lookupAny(values map[string]any, keys ...string) (any, bool) {
	if values == nil {
		return nil, false
	}
	// Preserve alias priority. In particular, a specific access_token or sso
	// field must win over generic token/cookie fallbacks even though Go map
	// iteration order is deliberately random.
	for _, wanted := range keys {
		normalizedWanted := normalizeJSONKey(wanted)
		for key, value := range values {
			if normalizeJSONKey(key) == normalizedWanted {
				return value, true
			}
		}
	}
	return nil, false
}

func firstLookup(values map[string]any, keys ...string) any {
	value, _ := lookupAny(values, keys...)
	return value
}
func envelopeValue(values map[string]any, keys ...string) any { return firstLookup(values, keys...) }

func lookupMap(values map[string]any, keys ...string) (map[string]any, bool) {
	value, ok := lookupAny(values, keys...)
	result, valid := value.(map[string]any)
	return result, ok && valid
}

func lookupSlice(values map[string]any, keys ...string) ([]any, bool) {
	value, ok := lookupAny(values, keys...)
	result, valid := value.([]any)
	return result, ok && valid
}

func normalizeJSONKey(value string) string {
	value = strings.ToLower(value)
	return strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(value)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return ""
	}
}

func int64Value(value any) int64 {
	text := strings.TrimSpace(stringValue(value))
	result, _ := strconv.ParseInt(text, 10, 64)
	return result
}

func boolValue(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, err == nil
	case json.Number:
		integer, err := typed.Int64()
		return integer != 0, err == nil
	default:
		return false, false
	}
}

func firstString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func claimString(values map[string]any, key string) string {
	return stringValue(firstLookup(values, key))
}

func copyClaims(destination, source map[string]any) {
	for key, value := range source {
		destination[key] = value
	}
}
func copyClaimsMissing(destination, source map[string]any) {
	for key, value := range source {
		if _, exists := destination[key]; !exists {
			destination[key] = value
		}
	}
}

func numericUnixTime(value any) *time.Time {
	text := strings.TrimSpace(stringValue(value))
	if text == "" {
		return nil
	}
	number, err := strconv.ParseInt(strings.SplitN(text, ".", 2)[0], 10, 64)
	if err != nil {
		return nil
	}
	if number > 1_000_000_000_000 {
		number /= 1000
	}
	result := time.Unix(number, 0).UTC()
	return &result
}

func parseFlexibleTime(value string) (time.Time, error) {
	if numeric := numericUnixTime(json.Number(strings.TrimSpace(value))); numeric != nil {
		return *numeric, nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, errors.New("unsupported time format")
}

func containsAnyWord(value string, words ...string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, word := range words {
		if value == word || strings.Contains(value, word) {
			return true
		}
	}
	return false
}

func cookieNamedValue(cookie, wanted string) string {
	for _, part := range strings.Split(cookie, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok && strings.EqualFold(strings.TrimSpace(name), wanted) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
