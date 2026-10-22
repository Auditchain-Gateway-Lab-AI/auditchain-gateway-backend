package audit

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/workerstatus"
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
	clientID string
	started  time.Time
}

func NewSourceSweep(db *gorm.DB, verifier Service, interval time.Duration, recent, backfill int) *SourceSweep {
	return &SourceSweep{db: db, verifier: verifier, interval: interval, recent: recent, backfill: backfill}
}

func (s *SourceSweep) SetClientScope(clientID string) {
	if s != nil {
		s.clientID = strings.TrimSpace(clientID)
	}
}

func (s *SourceSweep) scope() string {
	if s.clientID != "" {
		return "selected tenant latest event per resource"
	}
	return "all tenants latest event per resource"
}

func (s *SourceSweep) Run(ctx context.Context) {
	if s == nil || s.db == nil || s.verifier == nil || s.interval <= 0 {
		return
	}
	s.started = time.Now().UTC()
	workerstatus.Save(s.db, "SOURCE_SWEEP", true, &s.started, nil, nil, nil, s.scope(), "", s.clientID)
	defer workerstatus.Save(s.db, "SOURCE_SWEEP", false, &s.started, nil, nil, nil, s.scope(), "", s.clientID)
	heartbeatDone := make(chan struct{})
	defer close(heartbeatDone)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				workerstatus.Save(s.db, "SOURCE_SWEEP", true, &s.started, nil, nil, nil, s.scope(), "", s.clientID)
			}
		}
	}()
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
	SELECT log_id, client_id, timestamp, db_timestamp, source_integrity_checked_at FROM (
		SELECT DISTINCT ON (client_id, resource)
			log_id, client_id, resource, timestamp, db_timestamp, status, source_integrity_checked_at
		FROM audit_logs
		WHERE resource <> '' AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
		ORDER BY client_id, resource, timestamp DESC, db_timestamp DESC NULLS LAST, log_id DESC
	) latest WHERE status = 'ANCHORED'`

func (s *SourceSweep) Scan(ctx context.Context) (scanErr error) {
	if s == nil || s.db == nil || s.verifier == nil {
		return nil
	}
	var startedAt *time.Time
	if !s.started.IsZero() {
		startedAt = &s.started
	}
	seen := make(map[string]bool)
	var scanErrors []error
	defer func() {
		lastError := ""
		if scanErr != nil {
			lastError = scanErr.Error()
		}
		var queued int64
		filter, scopeArgs := s.clientFilter()
		if err := s.db.WithContext(ctx).Raw("SELECT COUNT(*) FROM ("+latestSourceEventQuery+") AS candidates WHERE source_integrity_checked_at IS NULL"+filter, scopeArgs...).Scan(&queued).Error; err != nil {
			log.Printf("[SourceSweep] gagal menghitung backlog: %v", err)
		}
		workerstatus.Save(s.db, "SOURCE_SWEEP", true, startedAt, workerstatus.Time(time.Now().UTC()), workerstatus.Count(int64(len(seen))), workerstatus.Count(queued), s.scope(), lastError, s.clientID)
	}()
	attemptID := uuid.NewString()
	check := func(rows []models.AuditLog) {
		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}
			if seen[row.LogID] {
				continue
			}
			seen[row.LogID] = true
			result, err := s.verifier.VerifyClientSourceIntegrity(row.LogID, row.ClientID, attemptID)
			if err != nil {
				log.Printf("[SourceSweep] client=%s log=%s: %v", row.ClientID, row.LogID, err)
				scanErrors = append(scanErrors, fmt.Errorf("%s/%s: %w", row.ClientID, row.LogID, err))
			}
			if result == nil || result.SourceStatus == "" {
				scanErrors = append(scanErrors, fmt.Errorf("%s/%s: source check did not return a result", row.ClientID, row.LogID))
			}
		}
	}
	if s.recent > 0 {
		var rows []models.AuditLog
		filter, scopeArgs := s.clientFilter()
		args := []interface{}{time.Now().UTC().Add(-10 * time.Minute), time.Now().UTC().Add(-s.interval)}
		args = append(args, scopeArgs...)
		args = append(args, s.recent)
		err := s.db.WithContext(ctx).Raw("SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY db_timestamp DESC NULLS LAST, timestamp DESC, log_id DESC) AS tenant_rank FROM ("+latestSourceEventQuery+
			") AS candidates WHERE db_timestamp >= ? AND (source_integrity_checked_at IS NULL OR source_integrity_checked_at <= ?)"+filter+" ORDER BY tenant_rank, db_timestamp DESC NULLS LAST, timestamp DESC, log_id DESC LIMIT ?", args...).Scan(&rows).Error
		if err != nil {
			return fmt.Errorf("load recent client events: %w", err)
		}
		check(rows)
	}
	if s.backfill > 0 && ctx.Err() == nil {
		var rows []models.AuditLog
		filter, scopeArgs := s.clientFilter()
		args := append([]interface{}{}, scopeArgs...)
		args = append(args, s.backfill+len(seen))
		err := s.db.WithContext(ctx).Raw("SELECT *, ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY source_integrity_checked_at ASC NULLS FIRST, timestamp ASC, log_id ASC) AS tenant_rank FROM ("+latestSourceEventQuery+
			") AS candidates WHERE TRUE"+filter+" ORDER BY tenant_rank, source_integrity_checked_at ASC NULLS FIRST, timestamp ASC, log_id ASC LIMIT ?", args...).Scan(&rows).Error
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
	if err := ctx.Err(); err != nil {
		scanErrors = append(scanErrors, err)
	}
	return errors.Join(scanErrors...)
}

func (s *SourceSweep) clientFilter() (string, []interface{}) {
	if s.clientID == "" {
		return "", nil
	}
	return " AND client_id = ?", []interface{}{s.clientID}
}
