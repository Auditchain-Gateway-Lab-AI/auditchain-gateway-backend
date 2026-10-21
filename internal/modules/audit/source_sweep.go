package audit

import (
	"context"
	"fmt"
	"log"
	"time"

	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

// SourceSweep checks the current client row represented by each resource's
// latest client event. Recent events are checked on every tick; older resources
// rotate by their last source-check time. It never issues a recovery command.
type SourceSweep struct {
	db       *gorm.DB
	verifier Service
	interval time.Duration
	recent   int
	backfill int
}

func NewSourceSweep(db *gorm.DB, verifier Service, interval time.Duration, recent, backfill int) *SourceSweep {
	return &SourceSweep{db: db, verifier: verifier, interval: interval, recent: recent, backfill: backfill}
}

func (s *SourceSweep) Run(ctx context.Context) {
	if s == nil || s.db == nil || s.verifier == nil || s.interval <= 0 {
		return
	}
	if err := s.Scan(ctx); err != nil {
		log.Printf("[SourceSweep] initial scan failed: %v", err)
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Scan(ctx); err != nil {
				log.Printf("[SourceSweep] scan failed: %v", err)
			}
		}
	}
}

const latestSourceEventQuery = `
	SELECT log_id, client_id, timestamp, source_integrity_checked_at FROM (
		SELECT DISTINCT ON (client_id, resource)
			log_id, client_id, resource, timestamp, db_timestamp, status, source_integrity_checked_at
		FROM audit_logs
		WHERE resource <> '' AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
		ORDER BY client_id, resource, timestamp DESC, db_timestamp DESC NULLS LAST, log_id DESC
	) latest WHERE status = 'ANCHORED'`

func (s *SourceSweep) Scan(ctx context.Context) error {
	if s == nil || s.db == nil || s.verifier == nil {
		return nil
	}
	seen := make(map[string]bool)
	check := func(rows []models.AuditLog) {
		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}
			if seen[row.LogID] {
				continue
			}
			seen[row.LogID] = true
			result, err := s.verifier.VerifyLogIntegrity(row.LogID, row.ClientID)
			if err != nil {
				log.Printf("[SourceSweep] client=%s log=%s: %v", row.ClientID, row.LogID, err)
			}
			if result == nil || result.SourceStatus == "" {
				// A failed proof or a concurrent newer event can prevent a source
				// comparison. Record the attempt so one row cannot starve the sweep.
				if updateErr := s.db.WithContext(ctx).Model(&models.AuditLog{}).
					Where("log_id = ? AND client_id = ?", row.LogID, row.ClientID).
					Update("source_integrity_checked_at", time.Now().UTC()).Error; updateErr != nil {
					log.Printf("[SourceSweep] failed to record attempt log=%s: %v", row.LogID, updateErr)
				}
			}
		}
	}
	if s.recent > 0 {
		var rows []models.AuditLog
		err := s.db.WithContext(ctx).Raw(latestSourceEventQuery+
			" AND timestamp >= ? AND (source_integrity_checked_at IS NULL OR source_integrity_checked_at <= ?) ORDER BY timestamp DESC, log_id DESC LIMIT ?",
			time.Now().UTC().Add(-10*time.Minute), time.Now().UTC().Add(-s.interval), s.recent).Scan(&rows).Error
		if err != nil {
			return fmt.Errorf("load recent client events: %w", err)
		}
		check(rows)
	}
	if s.backfill > 0 && ctx.Err() == nil {
		var rows []models.AuditLog
		err := s.db.WithContext(ctx).Raw(latestSourceEventQuery+
			" ORDER BY source_integrity_checked_at ASC NULLS FIRST, timestamp ASC, log_id ASC LIMIT ?",
			s.backfill+len(seen)).Scan(&rows).Error
		if err != nil {
			return fmt.Errorf("load older client events: %w", err)
		}
		checked := 0
		for _, row := range rows {
			if !seen[row.LogID] && checked < s.backfill {
				check([]models.AuditLog{row})
				checked++
			}
		}
	}
	return ctx.Err()
}
