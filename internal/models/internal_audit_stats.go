package models

import "time"

// InternalAuditStats menyimpan statistik agregat audit internal
// (verifikasi log audit offchain di PostgreSQL Gateway vs Blockchain Merkle Root).
type InternalAuditStats struct {
	ID                 uint       `gorm:"primaryKey;autoIncrement" json:"id"`
	ClientID           string     `gorm:"type:varchar(36);not null;uniqueIndex" json:"client_id"`
	TotalVerifications int64      `gorm:"type:bigint;not null;default:0" json:"total_verifications"`
	TotalRowsVerified  int64      `gorm:"type:bigint;not null;default:0" json:"total_rows_verified"`
	TotalValid         int64      `gorm:"type:bigint;not null;default:0" json:"total_valid"`
	TotalTampered      int64      `gorm:"type:bigint;not null;default:0" json:"total_tampered"`
	TotalPending       int64      `gorm:"type:bigint;not null;default:0" json:"total_pending"`
	IntegrityScore     float64    `gorm:"type:decimal(5,2);not null;default:100" json:"integrity_score"`
	LastVerifiedAt     *time.Time `gorm:"type:timestamptz" json:"last_verified_at"`
	LastVerifiedTable  string     `gorm:"type:varchar(255)" json:"last_verified_table"`
	TableVerifyResults string     `gorm:"type:jsonb;default:'{}'" json:"table_verify_results"`
	CreatedAt          time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}
