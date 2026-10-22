package models

import "time"

const (
	IntegrityDimensionProof  = "proof"
	IntegrityDimensionSource = "source"
)

// IntegrityCheck stores one observed result for a log and verification attempt.
// Rows intentionally begin only when the new history writer is deployed.
type IntegrityCheck struct {
	ID        string    `gorm:"primaryKey;type:varchar(36)" json:"id"`
	AttemptID string    `gorm:"type:varchar(100);not null;uniqueIndex:idx_integrity_checks_attempt_log_dimension" json:"attempt_id"`
	ClientID  string    `gorm:"type:varchar(36);not null;index:idx_integrity_checks_tenant_time,priority:1" json:"client_id"`
	LogID     string    `gorm:"type:varchar(100);not null;uniqueIndex:idx_integrity_checks_attempt_log_dimension" json:"log_id"`
	RunID     string    `gorm:"type:varchar(36);not null;default:''" json:"run_id,omitempty"`
	Dimension string    `gorm:"type:varchar(16);not null;index:idx_integrity_checks_tenant_time,priority:3;uniqueIndex:idx_integrity_checks_attempt_log_dimension" json:"dimension"`
	Trigger   string    `gorm:"type:varchar(40);not null" json:"trigger"`
	Result    string    `gorm:"type:varchar(24);not null" json:"result"`
	CheckedAt time.Time `gorm:"not null;index:idx_integrity_checks_tenant_time,priority:2" json:"checked_at"`
}

type BackgroundWorkerStatus struct {
	WorkerName       string     `gorm:"primaryKey;type:varchar(40)" json:"worker"`
	InstanceID       string     `gorm:"primaryKey;type:varchar(100)" json:"instance_id"`
	TargetClientID   string     `gorm:"type:varchar(36);not null;default:''" json:"-"`
	Enabled          bool       `gorm:"not null" json:"enabled"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	LastHeartbeatAt  time.Time  `gorm:"not null;index" json:"last_heartbeat_at"`
	LastCheckAt      *time.Time `json:"last_check_at,omitempty"`
	LastCheckedItems int64      `gorm:"not null;default:0" json:"last_checked_items"`
	QueuedItems      int64      `gorm:"not null;default:0" json:"queued_items"`
	Scope            string     `gorm:"type:varchar(80);not null;default:''" json:"scope"`
	LastError        string     `gorm:"type:varchar(240);not null;default:''" json:"last_error,omitempty"`
}
