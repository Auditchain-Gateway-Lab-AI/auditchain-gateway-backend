package tamperscanner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"gorm.io/gorm"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/workerstatus"
)

// Verifier is intentionally narrower than the dashboard audit service. The
// scanner must verify only the gateway PostgreSQL row and Fabric anchor; it
// must never contact a client database or Agent.
type Verifier interface {
	VerifyGatewayIntegrity(logID, clientID string) (string, error)
}

// BatchVerificationResult is returned by the optional batch verifier. A
// batch verifier lets the audit module cache one Fabric anchor read for all
// logs that share the same anchor during a scanner cycle.
type BatchVerificationResult struct {
	Status string
	Err    error
}

type BatchVerifier interface {
	// Implementations own persistence of status and history for each result.
	VerifyGatewayIntegrityBatch(logs []models.AuditLog) map[string]BatchVerificationResult
}

type Config struct {
	Enabled        bool
	ClientID       string
	Interval       time.Duration
	BatchSize      int
	Concurrency    int
	RecoveryCutoff *time.Time
}

type Worker struct {
	db       *gorm.DB
	verifier Verifier
	cfg      Config
	started  time.Time
}

func New(db *gorm.DB, verifier Verifier, cfg Config) (*Worker, error) {
	if db == nil {
		return nil, errors.New("tamper scanner membutuhkan database")
	}
	if verifier == nil {
		return nil, errors.New("tamper scanner membutuhkan verifier")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("interval tamper scanner harus positif")
	}
	if cfg.BatchSize <= 0 {
		return nil, errors.New("batch size tamper scanner harus positif")
	}
	if cfg.Concurrency <= 0 {
		return nil, errors.New("concurrency tamper scanner harus positif")
	}
	return &Worker{db: db, verifier: verifier, cfg: cfg}, nil
}

func (w *Worker) Run(ctx context.Context) {
	if !w.cfg.Enabled {
		workerstatus.Save(w.db, "PROOF_SCANNER", false, nil, nil, nil, nil, w.scope(), "", w.cfg.ClientID)
		return
	}
	w.started = time.Now().UTC()
	workerstatus.Save(w.db, "PROOF_SCANNER", true, &w.started, nil, nil, nil, w.scope(), "", w.cfg.ClientID)
	defer workerstatus.Save(w.db, "PROOF_SCANNER", false, &w.started, nil, nil, nil, w.scope(), "", w.cfg.ClientID)
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
				workerstatus.Save(w.db, "PROOF_SCANNER", true, &w.started, nil, nil, nil, w.scope(), "", w.cfg.ClientID)
			}
		}
	}()

	if err := w.Scan(ctx); err != nil {
		log.Printf("⚠️ [TamperScanner] scan awal gagal: %v", err)
	}
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Scan(ctx); err != nil {
				log.Printf("⚠️ [TamperScanner] scan gagal: %v", err)
			}
		}
	}
}

// Scan verifies the least-recently checked anchored logs. A single failing
// log does not stop the remainder of the batch; the returned error is only a
// signal for operational logging.
func (w *Worker) Scan(ctx context.Context) (scanErr error) {
	var checked int64
	var startedAt *time.Time
	if !w.started.IsZero() {
		startedAt = &w.started
	}
	defer func() {
		lastError := ""
		if scanErr != nil {
			lastError = scanErr.Error()
		}
		cutoff := time.Now().UTC().Add(-w.cfg.Interval)
		backlogQuery := w.db.Model(&models.AuditLog{}).
			Where("status = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'", "ANCHORED").
			Where("integrity_checked_at IS NULL OR integrity_checked_at <= ?", cutoff)
		if w.cfg.ClientID != "" {
			backlogQuery = backlogQuery.Where("client_id = ?", w.cfg.ClientID)
		}
		if w.cfg.RecoveryCutoff != nil {
			backlogQuery = backlogQuery.Where("db_timestamp IS NOT NULL AND db_timestamp >= ?", *w.cfg.RecoveryCutoff)
		}
		var queued int64
		if err := backlogQuery.Count(&queued).Error; err != nil {
			log.Printf("[TamperScanner] gagal menghitung backlog: %v", err)
		}
		workerstatus.Save(w.db, "PROOF_SCANNER", true, startedAt, workerstatus.Time(time.Now().UTC()), workerstatus.Count(checked), workerstatus.Count(queued), w.scope(), lastError, w.cfg.ClientID)
	}()
	var logs []models.AuditLog
	// Legacy RECOVERY rows may still exist in audit_logs for forensics, but
	// they are no longer client-originated audit records and must not create
	// normal tamper incidents. New recovery executions live in recovery_events.
	cutoff := time.Now().UTC().Add(-w.cfg.Interval)
	query := w.db.WithContext(ctx).Model(&models.AuditLog{}).
		Select(`audit_logs.*,
			CASE
				WHEN integrity_checked_at IS NULL AND db_timestamp >= NOW() - INTERVAL '10 minutes' THEN 0
				WHEN integrity_checked_at IS NULL THEN 1
				ELSE 2
			END AS priority_rank,
			ROW_NUMBER() OVER (
			PARTITION BY client_id
			ORDER BY CASE
				WHEN integrity_checked_at IS NULL AND db_timestamp >= NOW() - INTERVAL '10 minutes' THEN 0
				WHEN integrity_checked_at IS NULL THEN 1
				ELSE 2
			END ASC,
			         integrity_checked_at ASC NULLS FIRST,
			         blockchain_timestamp DESC NULLS LAST, log_id ASC
		) AS tenant_rank`).
		Where("status = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'", "ANCHORED").
		Where("integrity_checked_at IS NULL OR integrity_checked_at <= ?", cutoff)
	if w.cfg.ClientID != "" {
		query = query.Where("client_id = ?", w.cfg.ClientID)
	}
	if w.cfg.RecoveryCutoff != nil {
		// Legacy history remains available for manual integrity checks, but
		// the scheduled recovery scanner is scoped to snapshot-backed rows.
		query = query.Where("db_timestamp IS NOT NULL AND db_timestamp >= ?", *w.cfg.RecoveryCutoff)
	}
	if err := w.db.WithContext(ctx).Table("(?) AS fair", query).
		Order("priority_rank ASC, tenant_rank ASC, integrity_checked_at ASC NULLS FIRST, blockchain_timestamp DESC NULLS LAST, client_id ASC").
		Limit(w.cfg.BatchSize).Find(&logs).Error; err != nil {
		return fmt.Errorf("ambil batch tamper scanner gagal: %w", err)
	}
	checked = int64(len(logs))

	if len(logs) == 0 {
		return nil
	}
	if batchVerifier, ok := w.verifier.(BatchVerifier); ok {
		return w.persistBatchResults(logs, batchVerifier.VerifyGatewayIntegrityBatch(logs))
	}

	sem := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var scanErrors []error

	for _, logRow := range logs {
		if err := ctx.Err(); err != nil {
			return err
		}
		row := logRow
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			status, err := w.verifier.VerifyGatewayIntegrity(row.LogID, row.ClientID)
			if err != nil {
				message := err.Error()
				if updateErr := w.mark(row, models.IntegrityStatusUnreachable, message); updateErr != nil {
					message = message + "; persist status gagal: " + updateErr.Error()
				}
				mu.Lock()
				scanErrors = append(scanErrors, fmt.Errorf("%s/%s: %s", row.ClientID, row.LogID, message))
				mu.Unlock()
				return
			}
			if updateErr := w.mark(row, status, ""); updateErr != nil {
				mu.Lock()
				scanErrors = append(scanErrors, fmt.Errorf("%s/%s: persist status gagal: %w", row.ClientID, row.LogID, updateErr))
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if len(scanErrors) > 0 {
		return errors.Join(scanErrors...)
	}
	return nil
}

func (w *Worker) scope() string {
	if w.cfg.ClientID != "" {
		return "selected tenant gateway proof"
	}
	return "all tenants gateway proof"
}

func (w *Worker) persistBatchResults(logs []models.AuditLog, results map[string]BatchVerificationResult) error {
	var scanErrors []error
	for _, row := range logs {
		result, ok := results[row.LogID]
		if !ok {
			result = BatchVerificationResult{
				Status: models.IntegrityStatusUnreachable,
				Err:    errors.New("verifier tidak mengembalikan hasil untuk log"),
			}
		}
		if result.Err != nil {
			scanErrors = append(scanErrors, fmt.Errorf("%s/%s: %w", row.ClientID, row.LogID, result.Err))
		} else if result.Status == "" {
			scanErrors = append(scanErrors, fmt.Errorf("%s/%s: verifier returned no status", row.ClientID, row.LogID))
		}
	}
	if len(scanErrors) > 0 {
		return errors.Join(scanErrors...)
	}
	return nil
}

func (w *Worker) mark(row models.AuditLog, status, message string) error {
	if status == "" {
		status = models.IntegrityStatusUnreachable
	}
	now := time.Now().UTC()
	return w.db.Model(&models.AuditLog{}).
		Where("log_id = ? AND client_id = ?", row.LogID, row.ClientID).
		Updates(map[string]interface{}{
			"integrity_status":     status,
			"integrity_checked_at": now,
			"integrity_error":      message,
			"integrity_source":     models.IntegritySourceTamperScanner,
			"integrity_run_id":     "",
		}).Error
}
