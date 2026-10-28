package models

import "time"

const (
	ClientRecoveryStatusSuccess = "SUCCESS"
	ClientRecoveryStatusFailed  = "FAILED"
)

// ClientRecoveryEvent stores immutable audit logs of client operational row recovery executions.
type ClientRecoveryEvent struct {
	ID               string    `gorm:"primaryKey;type:varchar(36)" json:"id"`
	ClientID         string    `gorm:"type:varchar(36);not null;index" json:"client_id"`
	TamperRecordID   string    `gorm:"type:varchar(36);index" json:"tamper_record_id"`
	Resource         string    `gorm:"type:varchar(255);not null;index" json:"resource"` // e.g. "RUANGAN:1812"
	TableName        string    `gorm:"type:varchar(100);not null;index" json:"table_name"`
	RecordID         string    `gorm:"type:varchar(100);not null;index" json:"record_id"`
	LogID            string    `gorm:"type:varchar(100);index" json:"log_id"`
	Operation        string    `gorm:"type:varchar(20)" json:"operation"` // e.g. "UPDATE" | "INSERT"
	RecoveredColumns string    `gorm:"type:jsonb" json:"recovered_columns,omitempty"`
	RecoveredData    string    `gorm:"type:jsonb" json:"recovered_data,omitempty"`
	Status           string    `gorm:"type:varchar(30);not null;index" json:"status"` // SUCCESS | FAILED
	ErrorMessage     string    `gorm:"type:text" json:"error_message,omitempty"`
	RecoveredBy      string    `gorm:"type:varchar(100);not null;index" json:"recovered_by"`
	ExecutedAt       time.Time `gorm:"autoCreateTime;index" json:"executed_at"`
}
