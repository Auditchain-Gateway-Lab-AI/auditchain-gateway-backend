package workerstatus

import (
	"os"
	"strings"
	"time"

	"go-blockchain-api/internal/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func InstanceID() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) == "" {
		host = "unknown"
	}
	return host + ":" + os.Getenv("PORT")
}

// Save records a process heartbeat and, when checkAt is supplied, the latest
// completed unit of work. A failed health write never changes job outcomes.
func Save(db *gorm.DB, name string, enabled bool, startedAt, checkAt *time.Time, checked, queued *int64, scope, lastError string, targetClientID ...string) {
	if db == nil || name == "" {
		return
	}
	if len(lastError) > 240 {
		lastError = lastError[:240]
	}
	target := ""
	if len(targetClientID) > 0 {
		target = strings.TrimSpace(targetClientID[0])
	}
	values := models.BackgroundWorkerStatus{
		WorkerName:      name,
		InstanceID:      InstanceID(),
		TargetClientID:  target,
		Enabled:         enabled,
		StartedAt:       startedAt,
		LastHeartbeatAt: time.Now().UTC(),
		LastCheckAt:     checkAt,
		Scope:           scope,
		LastError:       lastError,
	}
	if checked != nil {
		values.LastCheckedItems = *checked
	}
	if queued != nil {
		values.QueuedItems = *queued
	}
	updates := map[string]interface{}{
		"enabled":           values.Enabled,
		"target_client_id":  values.TargetClientID,
		"last_heartbeat_at": values.LastHeartbeatAt,
		"scope":             values.Scope,
	}
	if startedAt != nil {
		updates["started_at"] = startedAt
	}
	if lastError != "" || checkAt != nil || !enabled {
		updates["last_error"] = values.LastError
	}
	if checkAt != nil {
		updates["last_check_at"] = checkAt
		updates["last_checked_items"] = values.LastCheckedItems
		updates["queued_items"] = values.QueuedItems
	}
	_ = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "worker_name"}, {Name: "instance_id"}},
		DoUpdates: clause.Assignments(updates),
	}).Create(&values).Error
}

func Count(value int64) *int64        { return &value }
func Time(value time.Time) *time.Time { return &value }
