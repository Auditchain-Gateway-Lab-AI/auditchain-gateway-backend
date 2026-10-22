package audit

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/workerstatus"
)

const (
	defaultVerificationBatchSize = 100
	maxVerificationBatchSize     = 500
	// Empty types are legacy internal jobs. Older Agent reports used the
	// requested_by marker before run_type was introduced.
	internalVerificationRunPredicate = "(run_type = 'INTERNAL' OR run_type = '' OR run_type IS NULL) AND COALESCE(requested_by, '') <> 'agent'"
)

// VerificationRunResponse is the API-safe projection of a background run.
// It intentionally contains summary/progress only; per-log results remain
// available through the gateway dashboard's normal log endpoints.
type VerificationRunResponse struct {
	ID              string     `json:"id"`
	ClientID        string     `json:"client_id"`
	RunType         string     `json:"run_type"`
	From            string     `json:"from"`
	To              string     `json:"to"`
	Status          string     `json:"status"`
	BatchSize       int        `json:"batch_size"`
	TotalItems      int64      `json:"total_items"`
	ProcessedItems  int64      `json:"processed_items"`
	ProgressPercent float64    `json:"progress_percent"`
	TotalValid      int64      `json:"total_valid"`
	TotalInvalid    int64      `json:"total_invalid"`
	TotalPending    int64      `json:"total_pending"`
	AlreadyVerified int64      `json:"already_verified"`
	VerifiedNow     int64      `json:"verified_now"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	RequestedBy     string     `json:"requested_by,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// VerificationJobService owns durable, batch-oriented range verification.
// Jobs are stored in PostgreSQL so a process restart can safely requeue a run;
// per-log integrity markers make replaying a batch idempotent.
type VerificationJobService struct {
	db        *gorm.DB
	audit     Service
	batchSize int
	wake      chan struct{}
	ownerID   string
}

var (
	ErrActiveVerificationRun      = errors.New("client sudah memiliki verification run aktif")
	ErrManualVerificationCooldown = errors.New("pemeriksaan manual tersedia kembali setelah 60 detik")
)

func NewVerificationJobService(db *gorm.DB, audit Service) *VerificationJobService {
	batchSize := configuredVerificationBatchSize()
	return &VerificationJobService{
		db:        db,
		audit:     audit,
		batchSize: batchSize,
		wake:      make(chan struct{}, 1),
		ownerID:   uuid.NewString(),
	}
}

func configuredVerificationBatchSize() int {
	raw := strings.TrimSpace(os.Getenv("VERIFICATION_JOB_BATCH_SIZE"))
	if raw == "" {
		return defaultVerificationBatchSize
	}
	size, err := strconv.Atoi(raw)
	if err != nil || size <= 0 {
		return defaultVerificationBatchSize
	}
	if size > maxVerificationBatchSize {
		return maxVerificationBatchSize
	}
	return size
}

func integritySourceForVerificationRun(requestedBy string) string {
	if strings.EqualFold(strings.TrimSpace(requestedBy), "system:scheduler") {
		return models.IntegritySourceScheduledRun
	}
	return models.IntegritySourceBackgroundRun
}

// Run starts a single process worker. PostgreSQL row locking prevents two
// gateway instances from claiming the same queued run.
func (s *VerificationJobService) Run(ctx context.Context) {
	if s == nil || s.db == nil || s.audit == nil {
		return
	}

	started := time.Now().UTC()
	workerstatus.Save(s.db, "RANGE_WORKER", true, &started, nil, nil, nil, "all active clients", "")
	defer workerstatus.Save(s.db, "RANGE_WORKER", false, &started, nil, nil, nil, "all active clients", "")
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
				queued, _ := s.queuedRunCount()
				workerstatus.Save(s.db, "RANGE_WORKER", true, &started, nil, nil, workerstatus.Count(queued), "all active clients", "")
			}
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	log.Printf("[VerificationJob] worker aktif; batch=%d", s.batchSize)

	for {
		select {
		case <-ctx.Done():
			log.Println("[VerificationJob] worker berhenti")
			return
		case <-ticker.C:
			err := s.processNext(ctx)
			queued, _ := s.queuedRunCount()
			workerstatus.Save(s.db, "RANGE_WORKER", true, &started, nil, nil, workerstatus.Count(queued), "all active clients", errorMessage(err))
		case <-s.wake:
			err := s.processNext(ctx)
			queued, _ := s.queuedRunCount()
			workerstatus.Save(s.db, "RANGE_WORKER", true, &started, nil, nil, workerstatus.Count(queued), "all active clients", errorMessage(err))
		}
	}
}

func (s *VerificationJobService) Enqueue(from, to time.Time, clientID, requestedBy string, batchSize int) (*models.VerificationRun, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("verification job service tidak tersedia")
	}
	if clientID == "" {
		return nil, errors.New("client_id wajib diisi")
	}
	if batchSize <= 0 {
		batchSize = s.batchSize
	}
	if batchSize > maxVerificationBatchSize {
		batchSize = maxVerificationBatchSize
	}

	var run *models.VerificationRun
	created := false
	err := s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", clientID).Error; err != nil {
			return err
		}
		var active models.VerificationRun
		lookupErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where(internalVerificationRunPredicate).
			Where("client_id = ? AND status IN ?", clientID, []string{models.VerificationRunQueued, models.VerificationRunRunning}).
			Order("created_at DESC").First(&active).Error
		if lookupErr == nil {
			if active.FromTime.Equal(from) && active.ToTime.Equal(to) {
				run = &active
				return nil
			}
			return ErrActiveVerificationRun
		}
		if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			return lookupErr
		}
		if !strings.EqualFold(strings.TrimSpace(requestedBy), "system:scheduler") {
			var latestManual models.VerificationRun
			cooldownErr := tx.Where(internalVerificationRunPredicate).Where("client_id = ? AND requested_by <> ?", clientID, "system:scheduler").
				Order("created_at DESC").First(&latestManual).Error
			if cooldownErr == nil && time.Since(latestManual.CreatedAt) < 60*time.Second {
				return ErrManualVerificationCooldown
			}
			if cooldownErr != nil && !errors.Is(cooldownErr, gorm.ErrRecordNotFound) {
				return cooldownErr
			}
		}
		totalItems, estimateErr := s.audit.EstimateLogRange(from, to, clientID)
		if estimateErr != nil {
			return estimateErr
		}
		run = &models.VerificationRun{
			ID: uuid.NewString(), ClientID: clientID, RequestedBy: requestedBy,
			RunType:  models.VerificationRunTypeInternal,
			FromTime: from, ToTime: to, Status: models.VerificationRunQueued,
			BatchSize: batchSize, TotalItems: totalItems,
		}
		if err := tx.Create(run).Error; err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, err
	}

	if created || run.Status == models.VerificationRunQueued {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return run, nil
}

func (s *VerificationJobService) GetRun(id, clientID string) (*models.VerificationRun, error) {
	var run models.VerificationRun
	err := s.db.Where("id = ? AND client_id = ?", id, clientID).First(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, gorm.ErrRecordNotFound
	}
	return &run, err
}

func (s *VerificationJobService) GetLatestRun(clientID string) (*models.VerificationRun, error) {
	var run models.VerificationRun
	err := s.db.Where(internalVerificationRunPredicate).Where("client_id = ?", clientID).Order("created_at DESC").First(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &run, err
}

func ToVerificationRunResponse(run *models.VerificationRun) VerificationRunResponse {
	progress := float64(0)
	if run.TotalItems > 0 {
		progress = float64(run.ProcessedItems) / float64(run.TotalItems) * 100
		if progress > 100 {
			progress = 100
		}
	}
	runType := run.RunType
	if runType == "" {
		runType = models.VerificationRunTypeInternal
		if run.RequestedBy == "agent" {
			runType = models.VerificationRunTypeExternal
		}
	}
	return VerificationRunResponse{
		ID:              run.ID,
		ClientID:        run.ClientID,
		RunType:         runType,
		From:            run.FromTime.UTC().Format(time.RFC3339Nano),
		To:              run.ToTime.UTC().Format(time.RFC3339Nano),
		Status:          run.Status,
		BatchSize:       run.BatchSize,
		TotalItems:      run.TotalItems,
		ProcessedItems:  run.ProcessedItems,
		ProgressPercent: progress,
		TotalValid:      run.TotalValid,
		TotalInvalid:    run.TotalInvalid,
		TotalPending:    run.TotalPending,
		AlreadyVerified: run.AlreadyVerified,
		VerifiedNow:     run.VerifiedNow,
		ErrorMessage:    run.ErrorMessage,
		RequestedBy:     run.RequestedBy,
		StartedAt:       run.StartedAt,
		CompletedAt:     run.CompletedAt,
		CreatedAt:       run.CreatedAt,
		UpdatedAt:       run.UpdatedAt,
	}
}

func (s *VerificationJobService) processNext(ctx context.Context) error {
	run, claimed, err := s.claimNextRun()
	if err != nil {
		log.Printf("[VerificationJob] gagal mengambil queued run: %v", err)
		return err
	}
	if !claimed {
		return nil
	}
	if err := s.processRun(ctx, run); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[VerificationJob] run %s gagal: %v", run.ID, err)
		_ = s.markFailed(run.ID, s.ownerID, err)
		return err
	}
	return nil
}

func errorMessage(err error) string {
	if err == nil || errors.Is(err, context.Canceled) {
		return ""
	}
	return err.Error()
}

func (s *VerificationJobService) claimNextRun() (*models.VerificationRun, bool, error) {
	var run models.VerificationRun
	err := s.db.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(internalVerificationRunPredicate).
			Where("(status = ? OR (status = ? AND (lease_expires_at <= ? OR (lease_expires_at IS NULL AND started_at <= ?))))", models.VerificationRunQueued, models.VerificationRunRunning, now, now.Add(-15*time.Minute)).
			Order("created_at ASC").
			First(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		updates := map[string]interface{}{
			"status":           models.VerificationRunRunning,
			"started_at":       now,
			"error_message":    "",
			"lease_owner":      s.ownerID,
			"lease_expires_at": now.Add(5 * time.Minute),
		}
		if run.Status == models.VerificationRunRunning {
			updates["processed_items"] = 0
			updates["total_valid"] = 0
			updates["total_invalid"] = 0
			updates["total_pending"] = 0
			updates["already_verified"] = 0
			updates["verified_now"] = 0
		}
		return tx.Model(&run).Updates(updates).Error
	})
	if err != nil {
		return nil, false, err
	}
	return &run, run.ID != "", nil
}

func (s *VerificationJobService) processRun(ctx context.Context, run *models.VerificationRun) error {
	if run == nil {
		return errors.New("verification run kosong")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leaseErrors := make(chan error, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if err := s.renewLease(run.ID); err != nil {
					select {
					case leaseErrors <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-heartbeatDone
	}()

	offset := 0
	attemptID := uuid.NewString()
	for {
		if err := runCtx.Err(); err != nil {
			select {
			case leaseErr := <-leaseErrors:
				return leaseErr
			default:
				return err
			}
		}

		if err := s.renewLease(run.ID); err != nil {
			return err
		}
		requestID := "verification-run-" + run.ID + "-" + attemptID
		integritySource := integritySourceForVerificationRun(run.RequestedBy)
		summary, loaded, err := s.audit.VerifyRangeBatch(
			run.FromTime,
			run.ToTime,
			run.ClientID,
			requestID,
			run.ID,
			integritySource,
			run.BatchSize,
			offset,
		)
		if err != nil {
			return err
		}
		if err := runCtx.Err(); err != nil {
			select {
			case leaseErr := <-leaseErrors:
				return leaseErr
			default:
				return err
			}
		}
		if loaded == 0 {
			break
		}

		if err := s.updateProgress(run.ID, s.ownerID, loaded, summary); err != nil {
			return err
		}
		offset += loaded
		if loaded < run.BatchSize {
			break
		}
	}

	now := time.Now().UTC()
	result := s.db.Model(&models.VerificationRun{}).
		Where("id = ? AND lease_owner = ? AND status = ?", run.ID, s.ownerID, models.VerificationRunRunning).
		Updates(map[string]interface{}{
			"status":           models.VerificationRunCompleted,
			"processed_items":  gorm.Expr("GREATEST(processed_items, total_items)"),
			"completed_at":     now,
			"lease_owner":      "",
			"lease_expires_at": nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("completion ditolak karena lease verification run sudah berubah")
	}
	queued, _ := s.queuedRunCount()
	workerstatus.Save(s.db, "RANGE_WORKER", true, nil, workerstatus.Time(now), workerstatus.Count(run.TotalItems), workerstatus.Count(queued), "all active clients", "")
	return nil
}

func (s *VerificationJobService) renewLease(id string) error {
	result := s.db.Model(&models.VerificationRun{}).
		Where("id = ? AND status = ? AND lease_owner = ?", id, models.VerificationRunRunning, s.ownerID).
		Update("lease_expires_at", time.Now().UTC().Add(5*time.Minute))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("verification lease sudah tidak dimiliki worker ini")
	}
	return nil
}

func (s *VerificationJobService) updateProgress(id, ownerID string, loaded int, summary RangeSummary) error {
	result := s.db.Model(&models.VerificationRun{}).Where("id = ? AND status = ? AND lease_owner = ?", id, models.VerificationRunRunning, ownerID).Updates(map[string]interface{}{
		"processed_items":  gorm.Expr("processed_items + ?", loaded),
		"total_valid":      gorm.Expr("total_valid + ?", summary.Valid),
		"total_invalid":    gorm.Expr("total_invalid + ?", summary.Invalid),
		"total_pending":    gorm.Expr("total_pending + ?", summary.Pending),
		"already_verified": gorm.Expr("already_verified + ?", summary.AlreadyVerified),
		"verified_now":     gorm.Expr("verified_now + ?", summary.VerifiedNow),
		"lease_expires_at": time.Now().UTC().Add(5 * time.Minute),
	})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return errors.New("progress verification run tidak diperbarui karena lease berubah")
	}
	return nil
}

func (s *VerificationJobService) markFailed(id, ownerID string, runErr error) error {
	message := "verification job gagal"
	if runErr != nil {
		message = runErr.Error()
	}
	return s.db.Model(&models.VerificationRun{}).Where("id = ? AND status = ? AND lease_owner = ?", id, models.VerificationRunRunning, ownerID).Updates(map[string]interface{}{
		"status":           models.VerificationRunFailed,
		"error_message":    message,
		"lease_owner":      "",
		"lease_expires_at": nil,
	}).Error
}

func (s *VerificationJobService) queuedRunCount() (int64, error) {
	var count int64
	err := s.db.Model(&models.VerificationRun{}).Where(internalVerificationRunPredicate).Where("status = ?", models.VerificationRunQueued).Count(&count).Error
	return count, err
}
