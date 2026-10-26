package models

import "time"

// ExternalAuditStats menyimpan statistik agregat audit external
// (verifikasi baris database operasional klien vs offchain log & blockchain).
type ExternalAuditStats struct {
	ID                   uint       `gorm:"primaryKey;autoIncrement" json:"id"`
	ClientID             string     `gorm:"type:varchar(36);not null;uniqueIndex" json:"client_id"`
	TotalAudited         int64      `gorm:"type:bigint;not null;default:0" json:"total_audited"`
	TotalValid           int64      `gorm:"type:bigint;not null;default:0" json:"total_valid"`
	TotalTampered        int64      `gorm:"type:bigint;not null;default:0" json:"total_tampered"`
	TotalPending         int64      `gorm:"type:bigint;not null;default:0" json:"total_pending"`
	IntegrityScore       float64    `gorm:"type:decimal(5,2);not null;default:100" json:"integrity_score"`
	LastAuditedAt        *time.Time `gorm:"type:timestamptz" json:"last_audited_at"`
	TotalRecoveries      int64      `gorm:"type:bigint;not null;default:0" json:"total_recoveries"`
	TotalTamperIncidents int64      `gorm:"type:bigint;not null;default:0" json:"total_tamper_incidents"`
	OpenTamperIncidents  int64      `gorm:"type:bigint;not null;default:0" json:"open_tamper_incidents"`
	TableVerifyResults   string     `gorm:"type:jsonb;default:'{}'" json:"table_verify_results"`
	CreatedAt            time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt            time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}
