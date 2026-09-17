package snapshotstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"go-blockchain-api/internal/models"
)

const SnapshotSchemaVersion = 1

// AuditSnapshot contains the fields required to reconstruct and verify an
// AuditLog. Metadata is kept as canonical JSON bytes so recovery does not
// accidentally change the hash input.
type AuditSnapshot struct {
	SchemaVersion        int             `json:"schema_version"`
	SnapshotType         string          `json:"snapshot_type"`
	LogID                string          `json:"log_id"`
	ClientID             string          `json:"client_id"`
	Actor                string          `json:"actor"`
	Action               string          `json:"action"`
	Resource             string          `json:"resource"`
	Timestamp            time.Time       `json:"timestamp"`
	DBTimestamp          *time.Time      `json:"db_timestamp,omitempty"`
	SourceSystem         string          `json:"source_system"`
	AuthorizationContext string          `json:"authorization_context"`
	SourceRecordID       string          `json:"source_record_id,omitempty"`
	Metadata             json.RawMessage `json:"metadata"`
	HashAlgorithm        string          `json:"hash_algorithm"`
	HashValue            string          `json:"hash_value"`
	CapturedAt           time.Time       `json:"captured_at"`
}

func NewAuditSnapshot(log models.AuditLog, capturedAt time.Time) ([]byte, error) {
	metadata := []byte(log.Metadata)
	if len(bytes.TrimSpace(metadata)) == 0 {
		metadata = []byte("null")
	}
	if !json.Valid(metadata) {
		return nil, fmt.Errorf("metadata log %s bukan JSON valid", log.LogID)
	}

	snapshot := AuditSnapshot{
		SchemaVersion:        SnapshotSchemaVersion,
		SnapshotType:         "AUDIT_LOG",
		LogID:                log.LogID,
		ClientID:             log.ClientID,
		Actor:                log.Actor,
		Action:               log.Action,
		Resource:             log.Resource,
		Timestamp:            log.Timestamp,
		DBTimestamp:          log.DBTimestamp,
		SourceSystem:         log.SourceSystem,
		AuthorizationContext: log.AuthorizationContext,
		SourceRecordID:       log.SourceRecordID,
		Metadata:             json.RawMessage(metadata),
		HashAlgorithm:        "SHA3-256",
		HashValue:            log.HashValue,
		CapturedAt:           capturedAt.UTC(),
	}
	return json.Marshal(snapshot)
}

func ParseAuditSnapshot(payload []byte) (AuditSnapshot, error) {
	var snapshot AuditSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return AuditSnapshot{}, fmt.Errorf("snapshot audit tidak valid: %w", err)
	}
	if snapshot.SchemaVersion != SnapshotSchemaVersion {
		return AuditSnapshot{}, fmt.Errorf("schema snapshot tidak didukung: %d", snapshot.SchemaVersion)
	}
	if snapshot.SnapshotType != "AUDIT_LOG" {
		return AuditSnapshot{}, fmt.Errorf("jenis snapshot tidak didukung: %s", snapshot.SnapshotType)
	}
	if snapshot.LogID == "" || snapshot.ClientID == "" || snapshot.HashValue == "" {
		return AuditSnapshot{}, fmt.Errorf("snapshot audit tidak memiliki identitas/hash wajib")
	}
	if !json.Valid(snapshot.Metadata) {
		return AuditSnapshot{}, fmt.Errorf("metadata pada snapshot audit bukan JSON valid")
	}
	return snapshot, nil
}
