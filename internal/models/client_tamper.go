package models

import "time"

const (
	ClientTamperStatusOpen      = "OPEN"
	ClientTamperStatusRecovered = "RECOVERED"
	ClientTamperStatusDismissed = "DISMISSED"
)

// ClientDataTamperRecord stores incidents where client operational database data
// differs from the latest authorized offchain log recorded in AuditChain (clientHash != offchainHash).
type ClientDataTamperRecord struct {
	ID              string     `gorm:"primaryKey;type:varchar(36)" json:"id"`
	ClientID        string     `gorm:"type:varchar(36);not null;index" json:"client_id"`
	RunID           string     `gorm:"type:varchar(36);index" json:"run_id"`
	Resource        string     `gorm:"type:varchar(255);not null;index" json:"resource"` // e.g. "SATUPETA_POLYGON:1001"
	TableName       string     `gorm:"type:varchar(100);not null;index" json:"table_name"`
	RecordID        string     `gorm:"type:varchar(100);not null;index" json:"record_id"`
	LogID           string     `gorm:"type:varchar(100);not null;index" json:"log_id"`
	Action          string     `gorm:"type:varchar(30)" json:"action"`
	ExpectedHash    string     `gorm:"type:varchar(64);not null" json:"expected_hash"` // latest offchain log hash_value
	ActualHash      string     `gorm:"type:varchar(64);not null" json:"actual_hash"`   // current client database canonical hash
	Discrepancies   string     `gorm:"type:jsonb" json:"discrepancies,omitempty"`      // JSON array of column diffs
	BlockchainTxID  string     `gorm:"type:varchar(128)" json:"blockchain_tx_id,omitempty"`
	Status          string     `gorm:"type:varchar(30);not null;index;default:'OPEN'" json:"status"` // OPEN | RECOVERED | DISMISSED
	AutoRecovered   bool       `gorm:"not null;default:false" json:"auto_recovered"`
	RecoveryError   string     `gorm:"type:text" json:"recovery_error,omitempty"`
	DetectedAt      time.Time  `gorm:"autoCreateTime;index" json:"detected_at"`
	LastConfirmedAt *time.Time `gorm:"index" json:"last_confirmed_at,omitempty"`
	ResolvedAt      *time.Time `json:"resolved_at,omitempty"`
	ResolvedBy      string     `gorm:"type:varchar(100)" json:"resolved_by,omitempty"`
	ResolutionNotes string     `gorm:"type:text" json:"resolution_notes,omitempty"`
}
