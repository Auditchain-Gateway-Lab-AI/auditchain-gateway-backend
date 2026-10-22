package audit

import (
	"context"
	"log"
	"time"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/workerstatus"

	"gorm.io/gorm"
)

const integrityHistoryRetention = 90 * 24 * time.Hour
const integrityHistoryDeleteBatchSize = 1000
const integrityHistoryDeletePause = 50 * time.Millisecond

func RunIntegrityHistoryRetention(ctx context.Context, db *gorm.DB) {
	if db == nil {
		return
	}
	started := time.Now().UTC()
	workerstatus.Save(db, "INTEGRITY_RETENTION", true, &started, nil, nil, nil, "90 days", "")
	defer workerstatus.Save(db, "INTEGRITY_RETENTION", false, &started, nil, nil, nil, "90 days", "")
	clean := func() {
		cutoff := time.Now().UTC().Add(-integrityHistoryRetention)
		message := ""
		var deleted int64
		for {
			if err := ctx.Err(); err != nil {
				message = err.Error()
				break
			}
			ids := db.Model(&models.IntegrityCheck{}).
				Select("id").Where("checked_at < ?", cutoff).
				Order("checked_at ASC").Limit(integrityHistoryDeleteBatchSize)
			result := db.WithContext(ctx).Where("id IN (?)", ids).Delete(&models.IntegrityCheck{})
			if result.Error != nil {
				message = result.Error.Error()
				break
			}
			deleted += result.RowsAffected
			if result.RowsAffected < integrityHistoryDeleteBatchSize {
				break
			}
			timer := time.NewTimer(integrityHistoryDeletePause)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				message = ctx.Err().Error()
			case <-timer.C:
			}
			if message != "" {
				break
			}
		}
		if message != "" {
			log.Printf("[IntegrityHistoryRetention] cleanup failed: %s", message)
		}
		checkedAt := time.Now().UTC()
		workerstatus.Save(db, "INTEGRITY_RETENTION", true, &started, &checkedAt, workerstatus.Count(deleted), nil, "90 days", message)
	}
	clean()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			clean()
		case <-heartbeat.C:
			workerstatus.Save(db, "INTEGRITY_RETENTION", true, &started, nil, nil, nil, "90 days", "")
		}
	}
}
