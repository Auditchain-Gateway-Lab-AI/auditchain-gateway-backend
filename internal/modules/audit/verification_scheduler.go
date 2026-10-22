package audit

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/workerstatus"

	"gorm.io/gorm"
)

const lateIngestionCursorOverlap = 5 * time.Minute

// VerificationSchedulerConfig controls the opt-in backend scheduler. The
// scheduler creates durable verification runs; the existing worker performs
// the actual batch verification.
type VerificationSchedulerConfig struct {
	Enabled    bool
	ClientID   string
	Interval   time.Duration
	Lookback   time.Duration
	Overlap    time.Duration
	BatchSize  int
	Location   *time.Location
	RunOnStart bool
}

// VerificationScheduler creates one range run per active client on each tick.
// It intentionally has no HTTP dependency, so validation continues when both
// dashboards are closed.
type VerificationScheduler struct {
	db     *gorm.DB
	jobs   *VerificationJobService
	config VerificationSchedulerConfig
}

func NewVerificationScheduler(db *gorm.DB, jobs *VerificationJobService, config VerificationSchedulerConfig) *VerificationScheduler {
	location := config.Location
	if location == nil {
		location = time.UTC
	}
	if config.Interval <= 0 {
		config.Interval = 24 * time.Hour
	}
	if config.Lookback <= 0 {
		config.Lookback = config.Interval
	}
	if config.Overlap < 0 {
		config.Overlap = 0
	}
	config.Location = location

	return &VerificationScheduler{db: db, jobs: jobs, config: config}
}

func (s *VerificationScheduler) Run(ctx context.Context) {
	if s == nil || !s.config.Enabled {
		log.Println("[VerificationScheduler] disabled")
		if s != nil {
			workerstatus.Save(s.db, "RANGE_SCHEDULER", false, nil, nil, nil, nil, schedulerClientScope(s.config.ClientID), "", s.config.ClientID)
		}
		return
	}
	if s.db == nil || s.jobs == nil {
		log.Println("[VerificationScheduler] disabled: database atau worker tidak tersedia")
		return
	}

	log.Printf(
		"[VerificationScheduler] aktif; client_scope=%s interval=%s lookback=%s overlap=%s timezone=%s batch=%d",
		schedulerClientScope(s.config.ClientID),
		s.config.Interval,
		s.config.Lookback,
		s.config.Overlap,
		s.config.Location,
		s.config.BatchSize,
	)
	started := time.Now().UTC()
	workerstatus.Save(s.db, "RANGE_SCHEDULER", true, &started, nil, nil, nil, schedulerClientScope(s.config.ClientID), "", s.config.ClientID)
	defer workerstatus.Save(s.db, "RANGE_SCHEDULER", false, &started, nil, nil, nil, schedulerClientScope(s.config.ClientID), "", s.config.ClientID)

	if s.config.RunOnStart {
		err := s.enqueueDueRuns(ctx)
		s.saveStatus(started, err, true)
	}

	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	heartbeat := time.NewTicker(30 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[VerificationScheduler] berhenti")
			return
		case <-ticker.C:
			err := s.enqueueDueRuns(ctx)
			s.saveStatus(started, err, true)
		case <-heartbeat.C:
			s.saveStatus(started, nil, false)
		}
	}
}

func (s *VerificationScheduler) saveStatus(started time.Time, runErr error, checked bool) {
	queued, err := s.jobs.queuedRunCount()
	if err != nil && runErr == nil {
		runErr = err
	}
	var checkedAt *time.Time
	var checkedItems *int64
	if checked {
		checkedAt = workerstatus.Time(time.Now().UTC())
		checkedItems = workerstatus.Count(0)
	}
	message := ""
	if runErr != nil {
		message = runErr.Error()
	}
	workerstatus.Save(s.db, "RANGE_SCHEDULER", true, &started, checkedAt, checkedItems, workerstatus.Count(queued), schedulerClientScope(s.config.ClientID), message, s.config.ClientID)
}

func (s *VerificationScheduler) enqueueDueRuns(ctx context.Context) error {
	clientIDs, err := s.activeClientIDs(ctx)
	if err != nil {
		log.Printf("[VerificationScheduler] gagal membaca client aktif: %v", err)
		return err
	}

	now := time.Now().In(s.config.Location)
	var schedulerErrors []error
	for _, clientID := range clientIDs {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(schedulerErrors, err)...)
		}

		active, err := s.hasActiveRun(ctx, clientID)
		if err != nil {
			log.Printf("[VerificationScheduler] gagal mengecek run aktif client=%s: %v", clientID, err)
			schedulerErrors = append(schedulerErrors, err)
			continue
		}
		if active {
			log.Printf("[VerificationScheduler] melewati client=%s karena masih ada run aktif", clientID)
			continue
		}

		latest, err := s.latestCompletedRun(ctx, clientID)
		if err != nil {
			log.Printf("[VerificationScheduler] gagal membaca cursor client=%s: %v", clientID, err)
			schedulerErrors = append(schedulerErrors, err)
			continue
		}

		from, to := calculateVerificationRange(now, latest, s.config.Lookback, s.config.Overlap)
		if latest != nil {
			ingestionCursor := latest.CreatedAt.Add(-lateIngestionCursorOverlap)
			if err := s.expandRangeForLateIngestion(ctx, clientID, ingestionCursor, now.UTC(), &from, &to); err != nil {
				log.Printf("[VerificationScheduler] gagal mencari event terlambat client=%s: %v", clientID, err)
				schedulerErrors = append(schedulerErrors, err)
				continue
			}
		}
		run, err := s.jobs.Enqueue(from, to, clientID, "system:scheduler", s.config.BatchSize)
		if err != nil {
			log.Printf("[VerificationScheduler] gagal enqueue client=%s range=%s..%s: %v", clientID, from.Format(time.RFC3339), to.Format(time.RFC3339), err)
			schedulerErrors = append(schedulerErrors, err)
			continue
		}
		log.Printf("[VerificationScheduler] enqueue client=%s run=%s range=%s..%s", clientID, run.ID, from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	return errors.Join(schedulerErrors...)
}

// expandRangeForLateIngestion extends the event-time range to include records
// ingested since the last successfully completed scheduler run. The completed
// run's CreatedAt is the automatic ingestion cursor; manual runs never move it.
func (s *VerificationScheduler) expandRangeForLateIngestion(ctx context.Context, clientID string, ingestedAfter, ingestedBefore time.Time, from, to *time.Time) error {
	if s == nil || s.db == nil || from == nil || to == nil || !ingestedAfter.Before(ingestedBefore) {
		return nil
	}
	var bounds struct {
		Min sql.NullTime `gorm:"column:min_timestamp"`
		Max sql.NullTime `gorm:"column:max_timestamp"`
	}
	err := s.db.WithContext(ctx).Model(&models.AuditLog{}).
		Select("MIN(timestamp) AS min_timestamp, MAX(timestamp) AS max_timestamp").
		Where("client_id = ? AND db_timestamp > ? AND db_timestamp <= ? AND "+clientAuditEventPredicate, clientID, ingestedAfter, ingestedBefore).
		Scan(&bounds).Error
	if err != nil {
		return err
	}
	var minAt, maxAt *time.Time
	if bounds.Min.Valid {
		minAt = &bounds.Min.Time
	}
	if bounds.Max.Valid {
		maxAt = &bounds.Max.Time
	}
	includeVerificationBounds(from, to, minAt, maxAt)
	return nil
}

func includeVerificationBounds(from, to, minAt, maxAt *time.Time) {
	if from == nil || to == nil {
		return
	}
	if minAt != nil && minAt.Before(*from) {
		*from = minAt.UTC()
	}
	if maxAt != nil && maxAt.After(*to) {
		*to = maxAt.UTC()
	}
}

func (s *VerificationScheduler) activeClientIDs(ctx context.Context) ([]string, error) {
	var clientIDs []string
	query := s.db.WithContext(ctx).
		Model(&models.Client{}).
		Where("status = ?", "active")
	if s.config.ClientID != "" {
		query = query.Where("id = ?", s.config.ClientID)
	}
	err := query.Order("id ASC").Pluck("id", &clientIDs).Error
	return clientIDs, err
}

func (s *VerificationScheduler) hasActiveRun(ctx context.Context, clientID string) (bool, error) {
	var count int64
	err := s.db.WithContext(ctx).
		Model(&models.VerificationRun{}).
		Where("client_id = ? AND status IN ?", clientID, []string{
			models.VerificationRunQueued,
			models.VerificationRunRunning,
		}).Count(&count).Error
	return count > 0, err
}

func (s *VerificationScheduler) latestCompletedRun(ctx context.Context, clientID string) (*models.VerificationRun, error) {
	var run models.VerificationRun
	err := s.db.WithContext(ctx).
		Where("client_id = ? AND status = ? AND requested_by = ?", clientID, models.VerificationRunCompleted, "system:scheduler").
		Order("to_time DESC, completed_at DESC, created_at DESC").
		First(&run).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &run, err
}

func calculateVerificationRange(now time.Time, latest *models.VerificationRun, lookback, overlap time.Duration) (time.Time, time.Time) {
	to := now.UTC()
	from := to.Add(-lookback)
	if latest != nil {
		from = latest.ToTime.UTC().Add(-overlap)
		if !from.Before(to) {
			from = to.Add(-lookback)
		}
	}
	return from, to
}

func schedulerClientScope(clientID string) string {
	if clientID == "" {
		return "all-active"
	}
	return "selected tenant"
}
