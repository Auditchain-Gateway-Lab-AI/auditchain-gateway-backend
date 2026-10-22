package client

import (
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type IntegrityHistoryResponse struct {
	Range         string                          `json:"range"`
	Dimension     string                          `json:"dimension"`
	From          time.Time                       `json:"from"`
	To            time.Time                       `json:"to"`
	BucketSeconds int64                           `json:"bucket_seconds"`
	AvailableFrom *time.Time                      `json:"available_from,omitempty"`
	GeneratedAt   time.Time                       `json:"generated_at"`
	Points        []IntegrityHistoryPointResponse `json:"points"`
}

type IntegrityHistoryPointResponse struct {
	BucketStart   time.Time `json:"bucket_start"`
	Checked       int64     `json:"checked"`
	Valid         int64     `json:"valid"`
	Tampered      int64     `json:"tampered"`
	Pending       int64     `json:"pending"`
	Unavailable   int64     `json:"unavailable"`
	Limited       int64     `json:"limited"`
	NotComparable int64     `json:"not_comparable"`
}

type WorkerStatusResponse struct {
	GeneratedAt time.Time          `json:"generated_at"`
	Workers     []WorkerStatusItem `json:"workers"`
}

type WorkerStatusItem struct {
	Worker           string     `json:"worker"`
	Enabled          bool       `json:"enabled"`
	Healthy          bool       `json:"healthy"`
	LastHeartbeatAt  *time.Time `json:"last_heartbeat_at,omitempty"`
	LastCheckAt      *time.Time `json:"last_check_at,omitempty"`
	LastCheckedItems int64      `json:"last_checked_items"`
	QueuedItems      int64      `json:"queued_items"`
	Scope            string     `json:"scope"`
	LastError        string     `json:"last_error,omitempty"`
}

type integrityHistoryWindow struct {
	name     string
	duration time.Duration
	buckets  int
}

func parseIntegrityHistoryWindow(value string) (integrityHistoryWindow, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "8H":
		return integrityHistoryWindow{name: "8H", duration: 8 * time.Hour, buckets: 8}, true
	case "24H":
		return integrityHistoryWindow{name: "24H", duration: 24 * time.Hour, buckets: 12}, true
	case "7D":
		return integrityHistoryWindow{name: "7D", duration: 7 * 24 * time.Hour, buckets: 7}, true
	case "30D":
		return integrityHistoryWindow{name: "30D", duration: 30 * 24 * time.Hour, buckets: 10}, true
	default:
		return integrityHistoryWindow{}, false
	}
}

type integrityHistoryAggregate struct {
	BucketIndex   int   `gorm:"column:bucket_index"`
	Checked       int64 `gorm:"column:checked"`
	Valid         int64 `gorm:"column:valid"`
	Tampered      int64 `gorm:"column:tampered"`
	Pending       int64 `gorm:"column:pending"`
	Unavailable   int64 `gorm:"column:unavailable"`
	Limited       int64 `gorm:"column:limited"`
	NotComparable int64 `gorm:"column:not_comparable"`
}

func loadIntegrityHistory(db *gorm.DB, clientID, dimension string, window integrityHistoryWindow, now time.Time) (*IntegrityHistoryResponse, error) {
	from := now.UTC().Add(-window.duration)
	to := now.UTC()
	bucketSeconds := int64(window.duration.Seconds()) / int64(window.buckets)
	var aggregates []integrityHistoryAggregate
	query := `
		SELECT LEAST(?, GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (checked_at - ?)) / ?)::int)) AS bucket_index,
		       COUNT(*) AS checked,
		       COUNT(*) FILTER (WHERE result = 'VALID') AS valid,
		       COUNT(*) FILTER (WHERE result = 'TAMPERED') AS tampered,
		       COUNT(*) FILTER (WHERE result = 'PENDING') AS pending,
		       COUNT(*) FILTER (WHERE result = 'UNAVAILABLE') AS unavailable,
		       COUNT(*) FILTER (WHERE result = 'LIMITED') AS limited,
		       COUNT(*) FILTER (WHERE result = 'NOT_COMPARABLE') AS not_comparable
		FROM integrity_checks
		WHERE client_id = ? AND dimension = ? AND checked_at >= ? AND checked_at < ?
		GROUP BY bucket_index ORDER BY bucket_index`
	if err := db.Raw(query, window.buckets-1, from, bucketSeconds, clientID, dimension, from, to).Scan(&aggregates).Error; err != nil {
		return nil, err
	}
	var availableFrom *time.Time
	var first sql.NullTime
	if err := db.Model(&models.IntegrityCheck{}).
		Select("MIN(checked_at)").
		Where("client_id = ? AND dimension = ?", clientID, dimension).
		Scan(&first).Error; err != nil {
		return nil, err
	}
	if first.Valid {
		availableFrom = &first.Time
	}
	points := make([]IntegrityHistoryPointResponse, window.buckets)
	for i := range points {
		points[i].BucketStart = from.Add(time.Duration(i) * window.duration / time.Duration(window.buckets))
	}
	for _, row := range aggregates {
		if row.BucketIndex < 0 || row.BucketIndex >= len(points) {
			continue
		}
		points[row.BucketIndex] = IntegrityHistoryPointResponse{
			BucketStart: points[row.BucketIndex].BucketStart, Checked: row.Checked,
			Valid: row.Valid, Tampered: row.Tampered, Pending: row.Pending,
			Unavailable: row.Unavailable, Limited: row.Limited, NotComparable: row.NotComparable,
		}
	}
	return &IntegrityHistoryResponse{
		Range: window.name, Dimension: dimension, From: from, To: to,
		BucketSeconds: bucketSeconds, AvailableFrom: availableFrom,
		GeneratedAt: now.UTC(), Points: points,
	}, nil
}

func (h *Handler) GetIntegrityHistory(c *gin.Context) {
	clientID, ok := clientIDFromContext(c)
	if !ok {
		return
	}
	window, validRange := parseIntegrityHistoryWindow(c.Query("range"))
	if !validRange {
		c.JSON(http.StatusBadRequest, gin.H{"error": "range harus 8H, 24H, 7D, atau 30D"})
		return
	}
	dimension := strings.ToLower(strings.TrimSpace(c.DefaultQuery("dimension", models.IntegrityDimensionProof)))
	if dimension != models.IntegrityDimensionProof && dimension != models.IntegrityDimensionSource {
		c.JSON(http.StatusBadRequest, gin.H{"error": "dimension harus proof atau source"})
		return
	}
	if h.DB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Database histori integrity tidak tersedia"})
		return
	}
	response, err := loadIntegrityHistory(h.DB.WithContext(c.Request.Context()), clientID, dimension, window, time.Now().UTC())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal membaca histori integrity"})
		return
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) GetWorkerStatus(c *gin.Context) {
	clientID, ok := clientIDFromContext(c)
	if !ok {
		return
	}
	if h.DB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Status worker tidak tersedia"})
		return
	}
	var rows []models.BackgroundWorkerStatus
	if err := h.DB.WithContext(c.Request.Context()).Order("worker_name ASC, last_heartbeat_at DESC").Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal membaca status worker"})
		return
	}
	now := time.Now().UTC()
	byName := make(map[string]WorkerStatusItem)
	latestCheckByName := make(map[string]time.Time)
	configuredElsewhere := make(map[string]bool)
	for _, row := range rows {
		if row.TargetClientID != "" && row.TargetClientID != clientID {
			configuredElsewhere[row.WorkerName] = true
			continue
		}
		item, exists := byName[row.WorkerName]
		if !exists {
			item = WorkerStatusItem{Worker: row.WorkerName, Scope: row.Scope}
		}
		item.Enabled = item.Enabled || row.Enabled
		if item.LastHeartbeatAt == nil || row.LastHeartbeatAt.After(*item.LastHeartbeatAt) {
			last := row.LastHeartbeatAt
			item.LastHeartbeatAt = &last
		}
		if row.LastCheckAt != nil && row.LastCheckAt.After(latestCheckByName[row.WorkerName]) {
			checkedAt := *row.LastCheckAt
			latestCheckByName[row.WorkerName] = checkedAt
			item.LastCheckAt = &checkedAt
			item.LastCheckedItems = row.LastCheckedItems
			if strings.TrimSpace(row.LastError) != "" {
				item.LastError = "Worker melaporkan kegagalan; administrator perlu memeriksa log."
			} else {
				item.LastError = ""
			}
		}
		if row.QueuedItems > item.QueuedItems {
			item.QueuedItems = row.QueuedItems
		}
		if strings.TrimSpace(item.Scope) == "" {
			item.Scope = row.Scope
		}
		byName[row.WorkerName] = item
	}
	for worker := range configuredElsewhere {
		if _, exists := byName[worker]; !exists {
			byName[worker] = WorkerStatusItem{Worker: worker, Scope: "not assigned to this tenant"}
		}
	}
	workers := make([]WorkerStatusItem, 0, len(byName))
	for _, item := range byName {
		if err := h.applyTenantWorkerMetrics(c, clientID, &item); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghitung status pemeriksaan tenant"})
			return
		}
		item.Healthy = item.Enabled && item.LastHeartbeatAt != nil && now.Sub(*item.LastHeartbeatAt) <= 90*time.Second
		workers = append(workers, item)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].Worker < workers[j].Worker })
	c.JSON(http.StatusOK, WorkerStatusResponse{GeneratedAt: now, Workers: workers})
}

func (h *Handler) applyTenantWorkerMetrics(c *gin.Context, clientID string, item *WorkerStatusItem) error {
	if item == nil {
		return nil
	}
	db := h.DB.WithContext(c.Request.Context())
	switch item.Worker {
	case "PROOF_SCANNER":
		if err := db.Model(&models.AuditLog{}).
			Where("client_id = ? AND status = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY' AND integrity_checked_at IS NULL", clientID, "ANCHORED").
			Count(&item.QueuedItems).Error; err != nil {
			return err
		}
		return setLatestAttemptMetrics(db, clientID, models.IntegrityDimensionProof, models.IntegritySourceTamperScanner, item)
	case "SOURCE_SWEEP":
		const sourceBacklogQuery = `
			SELECT COUNT(*) FROM (
				SELECT DISTINCT ON (resource) log_id, status, source_integrity_checked_at
				FROM audit_logs
				WHERE client_id = ? AND resource <> '' AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
				ORDER BY resource, timestamp DESC, db_timestamp DESC NULLS LAST, log_id DESC
			) latest
			WHERE status = 'ANCHORED' AND source_integrity_checked_at IS NULL`
		if err := db.Raw(sourceBacklogQuery, clientID).Scan(&item.QueuedItems).Error; err != nil {
			return err
		}
		return setLatestAttemptMetrics(db, clientID, models.IntegrityDimensionSource, "SOURCE_SWEEP", item)
	case "RANGE_WORKER":
		if err := db.Model(&models.VerificationRun{}).
			Where("(run_type = 'INTERNAL' OR run_type = '' OR run_type IS NULL) AND COALESCE(requested_by, '') <> 'agent'").
			Where("client_id = ? AND status = ?", clientID, models.VerificationRunQueued).
			Count(&item.QueuedItems).Error; err != nil {
			return err
		}
		var run models.VerificationRun
		err := db.Where("(run_type = 'INTERNAL' OR run_type = '' OR run_type IS NULL) AND COALESCE(requested_by, '') <> 'agent'").
			Where("client_id = ? AND status = ?", clientID, models.VerificationRunCompleted).
			Order("completed_at DESC, created_at DESC").First(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		item.LastCheckAt = run.CompletedAt
		item.LastCheckedItems = run.ProcessedItems
		return nil
	case "RANGE_SCHEDULER":
		if err := db.Model(&models.VerificationRun{}).
			Where("client_id = ? AND status = ? AND requested_by = ?", clientID, models.VerificationRunQueued, "system:scheduler").
			Count(&item.QueuedItems).Error; err != nil {
			return err
		}
		var run models.VerificationRun
		err := db.Where("client_id = ? AND status = ? AND requested_by = ?", clientID, models.VerificationRunCompleted, "system:scheduler").
			Order("created_at DESC").First(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		item.LastCheckAt = &run.CreatedAt
		return nil
	default:
		return nil
	}
}

func setLatestAttemptMetrics(db *gorm.DB, clientID, dimension, trigger string, item *WorkerStatusItem) error {
	var latest models.IntegrityCheck
	err := db.Where("client_id = ? AND dimension = ? AND trigger = ?", clientID, dimension, trigger).
		Order("checked_at DESC").First(&latest).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		item.LastCheckAt = nil
		item.LastCheckedItems = 0
		return nil
	}
	if err != nil {
		return err
	}
	item.LastCheckAt = &latest.CheckedAt
	return db.Model(&models.IntegrityCheck{}).
		Where("client_id = ? AND dimension = ? AND attempt_id = ?", clientID, dimension, latest.AttemptID).
		Count(&item.LastCheckedItems).Error
}

func clientIDFromContext(c *gin.Context) (string, bool) {
	value, exists := c.Get("client_id")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Client ID tidak ditemukan pada token"})
		return "", false
	}
	clientID, ok := value.(string)
	if !ok || strings.TrimSpace(clientID) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Client ID pada token tidak valid"})
		return "", false
	}
	return clientID, true
}
