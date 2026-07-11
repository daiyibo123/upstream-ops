package storage

import (
	"errors"
	"time"

	"gorm.io/gorm"
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

func (r *GatewayKeys) Update(key *GatewayKey) error { return r.db.Save(key).Error }

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

func (r *GatewayKeys) AddUsage(id uint, promptTokens, completionTokens, totalTokens int64, now time.Time) error {
	day := now.Format("2006-01-02")
	return r.db.Transaction(func(tx *gorm.DB) error {
		var key GatewayKey
		if err := tx.First(&key, id).Error; err != nil {
			return err
		}
		if key.UsageDate != day {
			if err := tx.Model(&GatewayKey{}).Where("id = ?", id).Updates(map[string]any{
				"usage_date":   day,
				"today_tokens": 0,
			}).Error; err != nil {
				return err
			}
		}
		if totalTokens <= 0 {
			totalTokens = promptTokens + completionTokens
		}
		return tx.Model(&GatewayKey{}).Where("id = ?", id).Updates(map[string]any{
			"today_tokens": gorm.Expr("today_tokens + ?", totalTokens),
			"total_tokens": gorm.Expr("total_tokens + ?", totalTokens),
			"last_used_at": &now,
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
	var item GatewayAffinity
	err := r.db.Where("affinity_hash = ?", hash).First(&item).Error
	switch {
	case err == nil:
		item.GroupKeyID = groupKeyID
		item.ExpiresAt = expiresAt
		item.LastUsedAt = now
		return r.db.Save(&item).Error
	case errors.Is(err, gorm.ErrRecordNotFound):
		return r.db.Create(&GatewayAffinity{
			AffinityHash: hash,
			GroupKeyID:   groupKeyID,
			ExpiresAt:    expiresAt,
			LastUsedAt:   now,
		}).Error
	default:
		return err
	}
}

func (r *GatewayAffinities) Delete(hash string) error {
	return r.db.Where("affinity_hash = ?", hash).Delete(&GatewayAffinity{}).Error
}

type UpstreamGroupKeys struct{ db *gorm.DB }

func NewUpstreamGroupKeys(db *gorm.DB) *UpstreamGroupKeys {
	return &UpstreamGroupKeys{db: db}
}

func (r *UpstreamGroupKeys) Upsert(key *UpstreamGroupKey) error {
	var existing UpstreamGroupKey
	err := r.db.Where("channel_id = ? AND group_ref = ?", key.ChannelID, key.GroupRef).First(&existing).Error
	switch {
	case err == nil:
		existing.ChannelName = key.ChannelName
		existing.ChannelType = key.ChannelType
		existing.GroupName = key.GroupName
		existing.GroupDesc = key.GroupDesc
		existing.Ratio = key.Ratio
		if key.UpstreamKeyID > 0 {
			existing.UpstreamKeyID = key.UpstreamKeyID
		}
		if key.KeyCipher != "" {
			existing.KeyCipher = key.KeyCipher
		}
		if existing.Status == "" {
			existing.Status = "unknown"
		}
		return r.db.Save(&existing).Error
	case errors.Is(err, gorm.ErrRecordNotFound):
		if key.Status == "" {
			key.Status = "unknown"
		}
		return r.db.Create(key).Error
	default:
		return err
	}
}

func (r *UpstreamGroupKeys) List() ([]UpstreamGroupKey, error) {
	var list []UpstreamGroupKey
	if err := r.db.Order("ratio ASC").Order("channel_id ASC").Order("group_name ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *UpstreamGroupKeys) ListCandidates(now time.Time) ([]UpstreamGroupKey, error) {
	var list []UpstreamGroupKey
	q := r.db.
		Joins("JOIN channels ON channels.id = upstream_group_keys.channel_id").
		Where("upstream_group_keys.key_cipher <> ''").
		Where("channels.monitor_enabled = ?", true).
		Where("(disabled_until IS NULL OR disabled_until <= ?)", now).
		Where("upstream_group_keys.status <> ?", "disabled").
		Order("upstream_group_keys.ratio ASC").
		Order("upstream_group_keys.failure_count ASC").
		Order("upstream_group_keys.channel_id ASC").
		Order("upstream_group_keys.group_name ASC")
	if err := q.Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

func (r *UpstreamGroupKeys) FindByID(id uint) (*UpstreamGroupKey, error) {
	var key UpstreamGroupKey
	if err := r.db.First(&key, id).Error; err != nil {
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

func (r *UpstreamGroupKeys) Delete(id uint) error {
	return r.db.Delete(&UpstreamGroupKey{}, id).Error
}
