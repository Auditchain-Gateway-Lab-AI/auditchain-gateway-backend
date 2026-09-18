package models

import "time"

const (
	SnapshotStatusPending       = "PENDING"
	SnapshotStatusUploading     = "UPLOADING"
	SnapshotStatusRetry         = "RETRY"
	SnapshotStatusVerified      = "VERIFIED"
	SnapshotStatusFailed        = "FAILED"
	SnapshotStatusLegacyMissing = "LEGACY_MISSING"

	OutboxStatusPending    = "PENDING"
	OutboxStatusProcessing = "PROCESSING"
	OutboxStatusRetry      = "RETRY"
	OutboxStatusCompleted  = "COMPLETED"
	OutboxStatusDeadLetter = "DEAD_LETTER"

	IncidentStatusOpen        = "OPEN"
	IncidentStatusUnderReview = "UNDER_REVIEW"
	IncidentStatusRecovering  = "RECOVERING"
	IncidentStatusResolved    = "RESOLVED"
	IncidentStatusDismissed   = "DISMISSED"

	RecoveryStatusPendingApproval    = "PENDING_APPROVAL"
	RecoveryStatusApproved           = "APPROVED"
	RecoveryStatusRejected           = "REJECTED"
	RecoveryStatusExecuting          = "EXECUTING"
	RecoveryStatusSucceeded          = "SUCCEEDED"
	RecoveryStatusFailedVerification = "FAILED_VERIFICATION"
	RecoveryStatusFailedExecution    = "FAILED_EXECUTION"
)

type SnapshotOutbox struct {
	ID            string     `gorm:"primaryKey;type:varchar(36)" json:"id"`
	LogID         string     `gorm:"type:varchar(100);not null;uniqueIndex" json:"log_id"`
	ClientID      string     `gorm:"type:varchar(36);not null;index" json:"client_id"`
	EventType     string     `gorm:"type:varchar(50);not null" json:"event_type"`
	Payload       []byte     `gorm:"type:bytea;not null" json:"-"`
	PayloadHash   string     `gorm:"type:varchar(64);not null" json:"payload_hash"`
	Status        string     `gorm:"type:varchar(20);not null;index;default:'PENDING'" json:"status"`
	AttemptCount  int        `gorm:"not null;default:0" json:"attempt_count"`
	NextAttemptAt *time.Time `gorm:"index" json:"next_attempt_at,omitempty"`
	LockedAt      *time.Time `json:"locked_at,omitempty"`
	LockedBy      string     `gorm:"type:varchar(100)" json:"locked_by,omitempty"`
	LastError     string     `gorm:"type:text" json:"last_error,omitempty"`
	CreatedAt     time.Time  `gorm:"autoCreateTime" json:"created_at"`
	ProcessedAt   *time.Time `json:"processed_at,omitempty"`
}

type TamperIncident struct {
	ID              string     `gorm:"primaryKey;type:varchar(36)" json:"id"`
	ClientID        string     `gorm:"type:varchar(36);not null;index" json:"client_id"`
	LogID           string     `gorm:"type:varchar(100);not null;index" json:"log_id"`
	Resource        string     `gorm:"type:varchar(255);not null;index" json:"resource"`
	IncidentType    string     `gorm:"type:varchar(80);not null;index" json:"incident_type"`
	ExpectedHash    string     `gorm:"type:varchar(64)" json:"expected_hash"`
	DetectedHash    string     `gorm:"type:varchar(64)" json:"detected_hash"`
	TamperedPayload []byte     `gorm:"type:bytea" json:"-"`
	Status          string     `gorm:"type:varchar(30);not null;index;default:'OPEN'" json:"status"`
	DetectedAt      time.Time  `gorm:"autoCreateTime;index" json:"detected_at"`
	ResolvedAt      *time.Time `json:"resolved_at,omitempty"`
}

type RecoveryRequest struct {
	ID                string `gorm:"primaryKey;type:varchar(36)" json:"id"`
	ClientID          string `gorm:"type:varchar(36);not null;index;uniqueIndex:idx_recovery_idempotency" json:"client_id"`
	IncidentID        string `gorm:"type:varchar(36);not null;index" json:"incident_id"`
	TargetLogID       string `gorm:"type:varchar(100);not null" json:"target_log_id"`
	SelectedLogID     string `gorm:"type:varchar(100);not null" json:"selected_log_id"`
	SnapshotObjectKey string `gorm:"type:text;not null" json:"snapshot_object_key"`
	SnapshotVersionID string `gorm:"type:varchar(255);not null" json:"snapshot_version_id"`
	// These frozen references are nullable at the database level so AutoMigrate
	// can be applied safely to installations that already contain legacy
	// recovery requests. New requests are rejected unless all values are set.
	SnapshotChecksum      string     `gorm:"type:varchar(64)" json:"snapshot_checksum"`
	SnapshotPlaintextHash string     `gorm:"type:varchar(64)" json:"snapshot_plaintext_hash"`
	AnchorID              string     `gorm:"type:varchar(100)" json:"anchor_id"`
	ExpectedMerkleRoot    string     `gorm:"type:varchar(64)" json:"expected_merkle_root"`
	RequestedBy           string     `gorm:"type:varchar(36);not null" json:"requested_by"`
	ApprovedBy            string     `gorm:"type:varchar(36)" json:"approved_by,omitempty"`
	ExecutedBy            string     `gorm:"type:varchar(36)" json:"executed_by,omitempty"`
	Reason                string     `gorm:"type:text;not null" json:"reason"`
	Status                string     `gorm:"type:varchar(35);not null;index;default:'PENDING_APPROVAL'" json:"status"`
	IdempotencyKey        string     `gorm:"type:varchar(100);not null;uniqueIndex:idx_recovery_idempotency" json:"idempotency_key"`
	BeforeHash            string     `gorm:"type:varchar(64)" json:"before_hash,omitempty"`
	AfterHash             string     `gorm:"type:varchar(64)" json:"after_hash,omitempty"`
	FailureReason         string     `gorm:"type:text" json:"failure_reason,omitempty"`
	RequestedAt           time.Time  `gorm:"autoCreateTime;index" json:"requested_at"`
	ApprovedAt            *time.Time `json:"approved_at,omitempty"`
	ExecutedAt            *time.Time `json:"executed_at,omitempty"`
}
