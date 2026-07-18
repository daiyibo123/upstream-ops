package storage

import (
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GatewayKeys struct{ db *gorm.DB }

func NewGatewayKeys(db *gorm.DB) *GatewayKeys { return &GatewayKeys{db: db} }

func (r *GatewayKeys) Create(key *GatewayKey) error { return r.db.Create(key).Error }

func (r *GatewayKeys) List() ([]GatewayKey, error) {
	var list []GatewayKey
	if err := r.db.Order("created_at DESC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *GatewayKeys) FindByID(id uint) (*GatewayKey, error) {
	var key GatewayKey
	if err := r.db.First(&key, id).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *GatewayKeys) FindPublic() (*GatewayKey, error) {
	var key GatewayKey
	err := r.db.Where("is_public = ?", true).Order("updated_at DESC").First(&key).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *GatewayKeys) SetPublic(key *GatewayKey) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&GatewayKey{}).Where("is_public = ?", true).Updates(map[string]any{
			"is_public":              false,
			"public_name":            "",
			"public_password_cipher": "",
			"public_password_hint":   "",
		}).Error; err != nil {
			return err
		}
		return tx.Save(key).Error
	})
}

func (r *GatewayKeys) FindEnabledByHash(hash string) (*GatewayKey, error) {
	var key GatewayKey
	err := r.db.Where("key_hash = ? AND enabled = ?", hash, true).First(&key).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *GatewayKeys) FindByHash(hash string) (*GatewayKey, error) {
	var key GatewayKey
	err := r.db.Where("key_hash = ?", hash).First(&key).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *GatewayKeys) Update(key *GatewayKey) error { return r.db.Save(key).Error }

func (r *GatewayKeys) ResetPublicVerification(id uint) error {
	return r.db.Model(&GatewayKey{}).Where("id = ?", id).UpdateColumns(map[string]any{
		"public_password_cipher": "",
		"public_password_hint":   "",
	}).Error
}

func (r *GatewayKeys) Disable(id uint) error {
	return r.db.Model(&GatewayKey{}).Where("id = ?", id).Update("enabled", false).Error
}

func (r *GatewayKeys) Delete(id uint) error {
	return r.db.Delete(&GatewayKey{}, id).Error
}

func (r *GatewayKeys) Touch(id uint, ip string) error {
	now := time.Now()
	return r.db.Model(&GatewayKey{}).Where("id = ?", id).Updates(map[string]any{
		"last_used_at": &now,
		"last_used_ip": ip,
	}).Error
}

func (r *GatewayKeys) AddUsage(id uint, promptTokens, completionTokens, totalTokens, cachedTokens int64, cost float64, now time.Time) error {
	day := now.Format("2006-01-02")
	return r.db.Transaction(func(tx *gorm.DB) error {
		var key GatewayKey
		if err := tx.First(&key, id).Error; err != nil {
			return err
		}
		if key.UsageDate != day {
			if err := tx.Model(&GatewayKey{}).Where("id = ?", id).Updates(map[string]any{
				"usage_date":          day,
				"today_tokens":        0,
				"today_prompt_tokens": 0,
				"today_cached_tokens": 0,
				"today_cost":          0,
			}).Error; err != nil {
				return err
			}
		}
		if promptTokens < 0 {
			promptTokens = 0
		}
		if completionTokens < 0 {
			completionTokens = 0
		}
		if totalTokens <= 0 {
			totalTokens = promptTokens + completionTokens
		}
		if totalTokens < 0 {
			totalTokens = 0
		}
		if cachedTokens < 0 {
			cachedTokens = 0
		}
		if promptTokens > 0 && cachedTokens > promptTokens {
			cachedTokens = promptTokens
		}
		if cost < 0 {
			cost = 0
		}
		return tx.Model(&GatewayKey{}).Where("id = ?", id).Updates(map[string]any{
			"today_tokens":        gorm.Expr("today_tokens + ?", totalTokens),
			"total_tokens":        gorm.Expr("total_tokens + ?", totalTokens),
			"today_prompt_tokens": gorm.Expr("today_prompt_tokens + ?", promptTokens),
			"total_prompt_tokens": gorm.Expr("total_prompt_tokens + ?", promptTokens),
			"today_cached_tokens": gorm.Expr("today_cached_tokens + ?", cachedTokens),
			"total_cached_tokens": gorm.Expr("total_cached_tokens + ?", cachedTokens),
			"today_cost":          gorm.Expr("today_cost + ?", cost),
			"total_cost":          gorm.Expr("total_cost + ?", cost),
			"enabled":             gorm.Expr("CASE WHEN balance_limit > 0 AND total_cost + ? >= balance_limit THEN ? ELSE enabled END", cost, false),
			"last_used_at":        &now,
		}).Error
	})
}

type GatewayAffinities struct{ db *gorm.DB }

func NewGatewayAffinities(db *gorm.DB) *GatewayAffinities {
	return &GatewayAffinities{db: db}
}

func (r *GatewayAffinities) Find(hash string, now time.Time) (*GatewayAffinity, error) {
	var item GatewayAffinity
	err := r.db.Where("affinity_hash = ? AND expires_at > ?", hash, now).First(&item).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (r *GatewayAffinities) Upsert(hash string, groupKeyID uint, expiresAt time.Time, now time.Time) error {
	// 原子 upsert：affinity_hash 上有唯一索引，并发请求可能同时命中"查不到→插入"，
	// 读后写会撞 UNIQUE 约束。用 ON CONFLICT DO UPDATE 一条语句完成，避免竞态。
	return r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "affinity_hash"}},
		DoUpdates: clause.Assignments(map[string]any{
			"group_key_id": groupKeyID,
			"expires_at":   expiresAt,
			"last_used_at": now,
			"updated_at":   now,
		}),
	}).Create(&GatewayAffinity{
		AffinityHash: hash,
		GroupKeyID:   groupKeyID,
		ExpiresAt:    expiresAt,
		LastUsedAt:   now,
	}).Error
}

func (r *GatewayAffinities) Delete(hash string) error {
	return r.db.Where("affinity_hash = ?", hash).Delete(&GatewayAffinity{}).Error
}

type UpstreamGroupKeys struct{ db *gorm.DB }

func NewUpstreamGroupKeys(db *gorm.DB) *UpstreamGroupKeys {
	return &UpstreamGroupKeys{db: db}
}

type UpstreamGroupKeyCounts struct {
	Total   int64
	Alive   int64
	Dead    int64
	Enabled int64
}

func orderUpstreamGroupKeys(q *gorm.DB, table string) *gorm.DB {
	col := func(name string) string {
		if table == "" {
			return name
		}
		return table + "." + name
	}
	return q.
		Order("CASE " + col("status") + " WHEN 'alive' THEN 0 WHEN 'unknown' THEN 1 WHEN 'rate_limited' THEN 2 WHEN 'dead' THEN 3 WHEN 'server_error' THEN 3 WHEN 'timeout' THEN 3 WHEN 'network_error' THEN 3 WHEN 'upstream_error' THEN 3 WHEN 'zero_balance' THEN 4 WHEN 'forbidden' THEN 4 WHEN 'auth_failed' THEN 4 WHEN 'model_error' THEN 4 WHEN 'invalid_request' THEN 4 WHEN 'non_generation' THEN 4 ELSE 5 END ASC").
		Order(col("charity") + " DESC").
		Order(col("ratio") + " ASC").
		Order(col("priority") + " DESC").
		Order(col("failure_count") + " ASC").
		Order(col("id") + " ASC")
}

// groupKeysWithChannelSource makes the provider URL available to the control
// panel even for groups created before ChannelURL was added to the model.
func groupKeysWithChannelSource(q *gorm.DB) *gorm.DB {
	return q.Model(&UpstreamGroupKey{}).
		Select("upstream_group_keys.*, channels.name AS channel_name, channels.site_url AS channel_url").
		Joins("LEFT JOIN channels ON channels.id = upstream_group_keys.channel_id")
}

func (r *UpstreamGroupKeys) Upsert(key *UpstreamGroupKey) error {
	normalizeGroupKeyPrices(key)
	key.RequestModeSource = normalizeRequestModeSource(key.RequestModeSource)
	var existing UpstreamGroupKey
	err := r.db.Where("channel_id = ? AND group_ref = ?", key.ChannelID, key.GroupRef).First(&existing).Error
	switch {
	case err == nil:
		existing.ChannelName = key.ChannelName
		existing.ChannelURL = key.ChannelURL
		existing.ChannelType = key.ChannelType
		existing.ClientFormat = key.ClientFormat
		// Automatic upstream sync must not overwrite a manual protocol repair.
		// A manual group may intentionally be changed back to auto, so only the
		// non-manual synchronization path preserves the existing override.
		preserveManualMode := !strings.HasPrefix(strings.ToLower(strings.TrimSpace(key.GroupRef)), "manual:") &&
			normalizeRequestModeSource(existing.RequestModeSource) == "manual" &&
			key.RequestModeSource == "auto"
		if !preserveManualMode {
			existing.RequestMode = key.RequestMode
			existing.RequestModeSource = key.RequestModeSource
		}
		existing.GroupName = key.GroupName
		existing.GroupDesc = key.GroupDesc
		existing.Ratio = key.Ratio
		if existing.RatioScalePercent <= 0 {
			existing.RatioScalePercent = 100
		}
		existing.InputPricePerMillion = key.InputPricePerMillion
		existing.OutputPricePerMillion = key.OutputPricePerMillion
		if existing.ClientFormat == "" {
			existing.ClientFormat = "openai"
		}
		if existing.RequestMode == "" {
			existing.RequestMode = "responses"
		}
		if existing.AuthMode == "" {
			existing.AuthMode = "bearer"
		}
		existing.RequestModeSource = normalizeRequestModeSource(existing.RequestModeSource)
		if key.UpstreamKeyID > 0 {
			existing.UpstreamKeyID = key.UpstreamKeyID
		}
		if key.KeyCipher != "" {
			existing.KeyCipher = key.KeyCipher
		}
		if existing.Status == "" {
			existing.Status = "alive"
		}
		return r.db.Save(&existing).Error
	case errors.Is(err, gorm.ErrRecordNotFound):
		if !key.Enabled {
			key.Enabled = true
		}
		if key.ClientFormat == "" {
			key.ClientFormat = "openai"
		}
		if key.RequestMode == "" {
			key.RequestMode = "responses"
		}
		if key.AuthMode == "" {
			key.AuthMode = "bearer"
		}
		key.RequestModeSource = normalizeRequestModeSource(key.RequestModeSource)
		if key.Status == "" {
			key.Status = "alive"
		}
		return r.db.Create(key).Error
	default:
		return err
	}
}

func normalizeGroupKeyPrices(key *UpstreamGroupKey) {
	if key == nil {
		return
	}
	if key.InputPricePerMillion <= 0 {
		key.InputPricePerMillion = DefaultInputPricePerMillion
	}
	if key.OutputPricePerMillion <= 0 {
		key.OutputPricePerMillion = DefaultOutputPricePerMillion
	}
	if key.RatioScalePercent <= 0 {
		key.RatioScalePercent = 100
	}
}

func (r *UpstreamGroupKeys) List() ([]UpstreamGroupKey, error) {
	var list []UpstreamGroupKey
	if err := orderUpstreamGroupKeys(groupKeysWithChannelSource(r.db), "upstream_group_keys").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListPage filters before pagination so an operator can locate a group even
// when it belongs to a channel on a later page.
func (r *UpstreamGroupKeys) ListPage(limit, offset int, search string) ([]UpstreamGroupKey, int64, error) {
	q := groupKeysWithChannelSource(r.db)
	if search = strings.TrimSpace(search); search != "" {
		like := "%" + strings.ToLower(search) + "%"
		q = q.Where(`LOWER(upstream_group_keys.channel_name) LIKE ? OR LOWER(channels.name) LIKE ? OR LOWER(channels.site_url) LIKE ? OR LOWER(upstream_group_keys.channel_url) LIKE ? OR LOWER(group_name) LIKE ? OR LOWER(group_desc) LIKE ? OR LOWER(group_ref) LIKE ?`, like, like, like, like, like, like, like)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []UpstreamGroupKey
	err := orderUpstreamGroupKeys(q, "upstream_group_keys").Limit(limit).Offset(offset).Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *UpstreamGroupKeys) Counts() (UpstreamGroupKeyCounts, error) {
	var out UpstreamGroupKeyCounts
	count := func(dest *int64, where ...any) error {
		q := r.db.Model(&UpstreamGroupKey{})
		if len(where) > 0 {
			q = q.Where(where[0], where[1:]...)
		}
		return q.Count(dest).Error
	}
	if err := count(&out.Total); err != nil {
		return out, err
	}
	if err := count(&out.Alive, "enabled = ? AND status = ?", true, "alive"); err != nil {
		return out, err
	}
	if err := count(&out.Dead, "enabled = ? AND status = ?", true, "dead"); err != nil {
		return out, err
	}
	if err := count(&out.Enabled, "enabled = ?", true); err != nil {
		return out, err
	}
	return out, nil
}

// ListByChannel 返回某个渠道下的全部分组密钥，用于同步时对比"上游还剩哪些分组"。
func (r *UpstreamGroupKeys) ListByChannel(channelID uint) ([]UpstreamGroupKey, error) {
	var list []UpstreamGroupKey
	if err := r.db.Where("channel_id = ?", channelID).Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *UpstreamGroupKeys) ListCandidates(now time.Time) ([]UpstreamGroupKey, error) {
	if err := r.reactivateExpiredCooldowns(now); err != nil {
		return nil, err
	}
	var list []UpstreamGroupKey
	q := r.db.
		Joins("JOIN channels ON channels.id = upstream_group_keys.channel_id").
		Where("upstream_group_keys.key_cipher <> ''").
		Where("upstream_group_keys.enabled = ?", true).
		Where("upstream_group_keys.status <> ?", "disabled")
	q = orderUpstreamGroupKeys(q, "upstream_group_keys")
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *UpstreamGroupKeys) reactivateExpiredCooldowns(now time.Time) error {
	if now.IsZero() {
		now = time.Now()
	}
	if err := r.db.Model(&UpstreamGroupKey{}).
		Where("enabled = ?", true).
		Where("status IN ?", []string{"rate_limited", "dead", "server_error", "timeout", "network_error", "upstream_error"}).
		Where("disabled_until IS NOT NULL AND disabled_until <= ?", now).
		Updates(map[string]any{
			"status":         "alive",
			"failure_count":  0,
			"disabled_until": nil,
		}).Error; err != nil {
		return err
	}
	return r.db.Model(&UpstreamGroupKey{}).
		Where("enabled = ?", true).
		Where("status = ?", "alive").
		Where("disabled_until IS NOT NULL AND disabled_until <= ?", now).
		Updates(map[string]any{
			"failure_count":  0,
			"disabled_until": nil,
		}).Error
}

func (r *UpstreamGroupKeys) FindByID(id uint) (*UpstreamGroupKey, error) {
	var key UpstreamGroupKey
	if err := groupKeysWithChannelSource(r.db).Where("upstream_group_keys.id = ?", id).First(&key).Error; err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *UpstreamGroupKeys) FindByChannelGroup(channelID uint, groupRef string) (*UpstreamGroupKey, error) {
	var key UpstreamGroupKey
	err := r.db.Where("channel_id = ? AND group_ref = ?", channelID, groupRef).First(&key).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *UpstreamGroupKeys) MarkSuccess(id uint) error {
	now := time.Now()
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":          "alive",
		"failure_count":   0,
		"last_checked_at": &now,
		"last_success_at": &now,
		"disabled_until":  nil,
		"last_error":      "",
	}).Error
}

// ClearCooldown 手动解除冷却：清掉 disabled_until 和 failure_count，让候选立即回到调度池。
// 不改 status（下一轮测活会刷新真实状态），只是撤销"临时不可调度"这个限制。
func (r *UpstreamGroupKeys) ClearCooldown(id uint) error {
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"failure_count":  0,
		"disabled_until": nil,
	}).Error
}

func (r *UpstreamGroupKeys) MarkHealthSuccess(id uint, latencyMS int64) error {
	return r.MarkHealthSuccessWithModel(id, latencyMS, "")
}

func (r *UpstreamGroupKeys) MarkHealthSuccessWithModel(id uint, latencyMS int64, model string) error {
	now := time.Now()
	if latencyMS < 0 {
		latencyMS = 0
	}
	updates := map[string]any{
		"status":          "alive",
		"failure_count":   0,
		"last_checked_at": &now,
		"last_success_at": &now,
		"last_latency_ms": latencyMS,
		"disabled_until":  nil,
		"last_error":      "",
	}
	if strings.TrimSpace(model) != "" {
		updates["health_probe_model"] = strings.TrimSpace(model)
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(updates).Error
}

func (r *UpstreamGroupKeys) MarkSuccessWithUsage(id uint, promptTokens, completionTokens, totalTokens int64) error {
	now := time.Now()
	if totalTokens <= 0 {
		totalTokens = promptTokens + completionTokens
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":            "alive",
		"failure_count":     0,
		"last_checked_at":   &now,
		"last_success_at":   &now,
		"last_used_at":      &now,
		"disabled_until":    nil,
		"last_error":        "",
		"prompt_tokens":     gorm.Expr("prompt_tokens + ?", promptTokens),
		"completion_tokens": gorm.Expr("completion_tokens + ?", completionTokens),
		"total_tokens":      gorm.Expr("total_tokens + ?", totalTokens),
	}).Error
}

func (r *UpstreamGroupKeys) UpdateConcurrencyLimit(id uint, limit int) error {
	if limit < 0 {
		limit = 0
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("concurrency_limit", limit).Error
}

func (r *UpstreamGroupKeys) UpdateEnabled(id uint, enabled bool) error {
	updates := map[string]any{"enabled": enabled, "disabled_until": nil}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(updates).Error
}

func (r *UpstreamGroupKeys) UpdateRequestMode(id uint, mode string) error {
	if strings.TrimSpace(mode) == "" {
		mode = "responses"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("request_mode", mode).Error
}

// UpdateSupportedModels 写回渠道的支持模型清单（JSON 数组文本）。传入已规整好的
// JSON 字符串；空清单存 "" 表示"未同步/未知"，调度按软过滤视其正常参与。
func (r *UpstreamGroupKeys) UpdateSupportedModels(id uint, modelsJSON string) error {
	if id == 0 {
		return nil
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("supported_models", modelsJSON).Error
}

func (r *UpstreamGroupKeys) UpdateRequestModeConfig(id uint, mode, source string) error {
	if strings.TrimSpace(mode) == "" {
		mode = "responses"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"request_mode":        mode,
		"request_mode_source": normalizeRequestModeSource(source),
	}).Error
}

// UpdateAuthMode records the authentication header contract detected for a
// single upstream key. Different keys on the same channel may legitimately
// need different headers, so this is intentionally not channel-scoped.
func (r *UpstreamGroupKeys) UpdateAuthMode(id uint, mode string) error {
	if strings.EqualFold(strings.TrimSpace(mode), "x-api-key") || strings.EqualFold(strings.TrimSpace(mode), "x_api_key") {
		mode = "x_api_key"
	} else {
		mode = "bearer"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("auth_mode", mode).Error
}

func normalizeRequestModeSource(source string) string {
	if strings.EqualFold(strings.TrimSpace(source), "manual") {
		return "manual"
	}
	return "auto"
}

// UpdateManualKey replaces a manually maintained upstream secret. Updating a
// key is an intentional recovery action, so clear stale failure state and put
// the group back into scheduling without touching any automatic group record.
// Protocol detection is advisory only: some otherwise usable upstreams block
// probe endpoints or expose a model list different from the requested model.
// A failed probe must never leave a newly replaced manual key outside normal
// routing.
func (r *UpstreamGroupKeys) UpdateManualKey(id uint, keyCipher string) error {
	if strings.TrimSpace(keyCipher) == "" {
		return errors.New("manual key cipher cannot be empty")
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"key_cipher":     keyCipher,
		"enabled":        true,
		"status":         "alive",
		"failure_count":  0,
		"disabled_until": nil,
		"last_error":     "",
	}).Error
}

func (r *UpstreamGroupKeys) UpdatePriority(id uint, priority int) error {
	if priority < 0 {
		priority = 0
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("priority", priority).Error
}

func (r *UpstreamGroupKeys) UpdateRatioScalePercent(id uint, percent float64) error {
	if percent <= 0 {
		percent = 100
	}
	if percent > 10000 {
		percent = 10000
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("ratio_scale_percent", percent).Error
}

// UpdateClientFormat 手动纠正某个分组的请求格式（openai / claude）。
// 自动推断可能出错（比如分组名没带 claude 字样但其实是 claude 模型），允许手动覆盖，
// 避免用 openai 格式打到 claude 模型导致报错。
func (r *UpstreamGroupKeys) UpdateClientFormat(id uint, format string) error {
	if format == "" {
		format = "openai"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("client_format", format).Error
}

// UpdateCharity 设置分组是否为公益渠道（调度时公益优先于付费）。
func (r *UpstreamGroupKeys) UpdateCharity(id uint, charity bool) error {
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Update("charity", charity).Error
}

func (r *UpstreamGroupKeys) MarkFailure(id uint, errMsg string, disabledUntil time.Time) error {
	now := time.Now()
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":          "dead",
		"failure_count":   gorm.Expr("failure_count + ?", 1),
		"last_checked_at": &now,
		"disabled_until":  &disabledUntil,
		"last_error":      errMsg,
	}).Error
}

func (r *UpstreamGroupKeys) MarkProxyFailureStatus(id uint, status string, errMsg string, disabledUntil *time.Time) error {
	now := time.Now()
	if strings.TrimSpace(status) == "" {
		status = "dead"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":          status,
		"failure_count":   gorm.Expr("failure_count + ?", 1),
		"last_checked_at": &now,
		"disabled_until":  disabledUntil,
		"last_error":      errMsg,
	}).Error
}

func (r *UpstreamGroupKeys) MarkHealthFailure(id uint, errMsg string, disabledUntil time.Time, latencyMS int64) error {
	return r.MarkHealthFailureStatus(id, "dead", errMsg, &disabledUntil, latencyMS)
}

func (r *UpstreamGroupKeys) MarkHealthFailureStatus(id uint, status string, errMsg string, disabledUntil *time.Time, latencyMS int64) error {
	now := time.Now()
	if latencyMS < 0 {
		latencyMS = 0
	}
	if strings.TrimSpace(status) == "" {
		status = "dead"
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":          status,
		"failure_count":   gorm.Expr("failure_count + ?", 1),
		"last_checked_at": &now,
		"last_latency_ms": latencyMS,
		"disabled_until":  disabledUntil,
		"last_error":      errMsg,
	}).Error
}

// MarkHealthInconclusive records a probe result that could not prove an
// upstream is unusable (for example a probe-model or endpoint mismatch).  It
// deliberately does not increase failure_count or create a cooldown, so an
// otherwise working route remains eligible for real user traffic.
func (r *UpstreamGroupKeys) MarkHealthInconclusive(id uint, errMsg string, latencyMS int64) error {
	now := time.Now()
	if latencyMS < 0 {
		latencyMS = 0
	}
	return r.db.Model(&UpstreamGroupKey{}).Where("id = ?", id).Updates(map[string]any{
		"status":          "alive",
		"last_checked_at": &now,
		"last_latency_ms": latencyMS,
		"disabled_until":  nil,
		"last_error":      errMsg,
	}).Error
}

func (r *UpstreamGroupKeys) Delete(id uint) error {
	return r.db.Delete(&UpstreamGroupKey{}, id).Error
}

// UsageLogs 存取网关请求使用记录。
type UsageLogs struct{ db *gorm.DB }

type IPPolicies struct{ db *gorm.DB }

func NewIPPolicies(db *gorm.DB) *IPPolicies { return &IPPolicies{db: db} }

func (r *IPPolicies) List() ([]IPPolicy, error) {
	var items []IPPolicy
	err := r.db.Order("blocked DESC").Order("updated_at DESC").Find(&items).Error
	return items, err
}

func (r *IPPolicies) Find(ip string) (*IPPolicy, error) {
	var item IPPolicy
	err := r.db.Where("ip = ?", strings.TrimSpace(ip)).First(&item).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (r *IPPolicies) Upsert(item *IPPolicy) error {
	if item == nil || strings.TrimSpace(item.IP) == "" {
		return errors.New("IP is required")
	}
	item.IP = strings.TrimSpace(item.IP)
	return r.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "ip"}},
		DoUpdates: clause.AssignmentColumns([]string{"blocked", "public_concurrency_exempt", "note", "blocked_message", "updated_at"}),
	}).Create(item).Error
}

func (r *IPPolicies) Delete(ip string) error {
	return r.db.Where("ip = ?", strings.TrimSpace(ip)).Delete(&IPPolicy{}).Error
}

func NewUsageLogs(db *gorm.DB) *UsageLogs { return &UsageLogs{db: db} }

func (r *UsageLogs) Add(entry *UsageLog) error {
	return r.db.Create(entry).Error
}

func (r *UsageLogs) Update(id uint, updates map[string]any) error {
	if id == 0 || len(updates) == 0 {
		return nil
	}
	return r.db.Model(&UsageLog{}).Where("id = ?", id).Updates(updates).Error
}

// List 分页返回使用记录，按时间倒序。
func (r *UsageLogs) List(limit, offset int) ([]UsageLog, int64, error) {
	return r.ListView(limit, offset, "all")
}

// ListView keeps successful usage separate from dispatch/error diagnostics,
// matching Sub2API's usage/error tabs while preserving the same storage table.
func (r *UsageLogs) ListView(limit, offset int, view string) ([]UsageLog, int64, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	query := usageLogViewQuery(r.db.Model(&UsageLog{}), view)
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []UsageLog
	if err := usageLogViewQuery(r.db.Model(&UsageLog{}), view).Order("created_at DESC, id DESC").Limit(limit).Offset(offset).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// Stats 汇总当前保留的全部使用明细。平均耗时忽略尚未产生有效耗时的零值记录，
// 避免正在调度中的请求把延迟指标人为拉低。
func (r *UsageLogs) Stats() (UsageLogStats, error) {
	return r.StatsView("all")
}

func (r *UsageLogs) StatsView(view string) (UsageLogStats, error) {
	var stats UsageLogStats
	err := usageLogViewQuery(r.db.Model(&UsageLog{}), view).Select(`
		COUNT(*) AS total_requests,
		COALESCE(SUM(CASE WHEN status IN ('success', 'estimated') THEN 1 ELSE 0 END), 0) AS success_requests,
		COALESCE(SUM(total_tokens), 0) AS total_tokens,
		COALESCE(AVG(CASE WHEN first_token_ms > 0 THEN first_token_ms END), 0) AS avg_first_token_ms,
		COALESCE(AVG(CASE WHEN duration_ms > 0 THEN duration_ms END), 0) AS avg_duration_ms
	`).Scan(&stats).Error
	return stats, err
}

func usageLogViewQuery(query *gorm.DB, view string) *gorm.DB {
	switch strings.ToLower(strings.TrimSpace(view)) {
	case "usage", "success":
		return query.Where("status IN ?", []string{"success", "estimated"})
	case "events", "errors", "dispatch":
		return query.Where("status NOT IN ?", []string{"success", "estimated"})
	default:
		return query
	}
}

// DeleteOlderThan 清理指定时间点之前的记录，返回删除条数。
func (r *UsageLogs) DeleteOlderThan(cutoff time.Time) (int64, error) {
	res := r.db.Where("created_at < ?", cutoff).Delete(&UsageLog{})
	return res.RowsAffected, res.Error
}

// Clear 删除全部使用明细日志；不触碰 GatewayKey 上的累计统计。
func (r *UsageLogs) Clear() (int64, error) {
	res := r.db.Where("1 = 1").Delete(&UsageLog{})
	return res.RowsAffected, res.Error
}
