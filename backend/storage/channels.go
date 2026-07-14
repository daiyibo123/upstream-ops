package storage

import (
	"errors"
	"strings"

	"gorm.io/gorm"
)

// Channels 渠道仓库。
type Channels struct{ db *gorm.DB }

func NewChannels(db *gorm.DB) *Channels { return &Channels{db: db} }

func (r *Channels) Create(c *Channel) error { return r.db.Create(c).Error }
func (r *Channels) Update(c *Channel) error { return r.db.Save(c).Error }
func (r *Channels) Delete(id uint) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Select("id", "name").First(&channel, id).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&AuthSession{}).Error; err != nil {
			return err
		}
		for _, model := range []any{
			&RateSnapshot{},
			&RateChangeLog{},
			&BalanceSnapshot{},
			&CostSnapshot{},
			&MonitorLog{},
			&NotificationCooldown{},
			&UpstreamAnnouncement{},
			&UpstreamGroupKey{},
		} {
			if err := tx.Where("channel_id = ?", id).Delete(model).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("upstream_channel_id = ?", id).Delete(&NotificationLog{}).Error; err != nil {
			return err
		}
		if channel.Name != "" {
			pattern := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(channel.Name) + "%"
			if err := tx.Where("upstream_channel_id = 0 AND (subject LIKE ? ESCAPE '!' OR body LIKE ? ESCAPE '!')", pattern, pattern).
				Delete(&NotificationLog{}).Error; err != nil {
				return err
			}
		}
		return tx.Delete(&Channel{}, id).Error
	})
}
func (r *Channels) FindByID(id uint) (*Channel, error) {
	var c Channel
	if err := r.db.First(&c, id).Error; err != nil {
		return nil, err
	}
	return &c, nil
}
func (r *Channels) List() ([]Channel, error) {
	var list []Channel
	if err := r.db.Order("pinned DESC").Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}

// ListPage returns channels in the operator-friendly order: pinned first,
// then by the lowest upstream group ratio.  Search is intentionally done in
// SQL so pagination never hides a matching channel on another page.
func (r *Channels) ListPage(page, pageSize int, search string) ([]Channel, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 && pageSize != -1 {
		pageSize = 20
	}
	q := r.db.Model(&Channel{})
	if search = strings.TrimSpace(search); search != "" {
		like := "%" + strings.ToLower(search) + "%"
		q = q.Where(`
			LOWER(channels.name) LIKE ? OR LOWER(channels.site_url) LIKE ? OR LOWER(channels.username) LIKE ?
			OR EXISTS (SELECT 1 FROM upstream_group_keys g WHERE g.channel_id = channels.id AND (LOWER(g.group_name) LIKE ? OR LOWER(g.group_desc) LIKE ?))
		`, like, like, like, like, like)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []Channel
	// 按"该渠道下最低倍率的分组"从低到高排序：越便宜的渠道越靠前。
	// 没有可用分组（min 为 NULL）的渠道排最后；同价再按 sort_order / id。
	minRatioSub := "(SELECT MIN(ratio) FROM upstream_group_keys g WHERE g.channel_id = channels.id AND g.key_cipher <> '')"
	q = q.
		Order("pinned DESC").
		Order("CASE WHEN " + minRatioSub + " IS NULL THEN 1 ELSE 0 END ASC").
		Order(minRatioSub + " ASC").
		Order("sort_order DESC").
		Order("id ASC")
	if pageSize != -1 {
		q = q.Offset((page - 1) * pageSize).Limit(pageSize)
	}
	if err := q.Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}
func (r *Channels) ListMonitorEnabled() ([]Channel, error) {
	var list []Channel
	if err := r.db.
		Where("monitor_enabled = ?", true).
		Where("NOT (credential_mode = ? AND LOWER(TRIM(username)) = ?)", CredentialModeToken, "manual").
		Order("sort_order DESC").
		Order("id ASC").
		Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
func (r *Channels) UpdateBalance(id uint, balance float64, at any, lastErr string) error {
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"last_balance":    balance,
		"last_balance_at": at,
		"last_error":      lastErr,
	}).Error
}

func (r *Channels) UpdateCosts(id uint, todayCost float64, totalCost float64) error {
	return r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"today_cost": todayCost,
		"total_cost": totalCost,
	}).Error
}
func (r *Channels) SetLastError(id uint, msg string) error {
	return r.db.Model(&Channel{}).Where("id = ?", id).Update("last_error", msg).Error
}
