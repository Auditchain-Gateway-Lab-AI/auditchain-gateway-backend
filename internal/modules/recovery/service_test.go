package recovery

import (
	"testing"
	"time"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/storage/snapshotstore"
)

func TestValidateSnapshotIdentityRejectsCrossLogAndHashMismatch(t *testing.T) {
	request := &models.RecoveryRequest{
		ClientID:              "client-1",
		TargetLogID:           "log-1",
		SelectedLogID:         "log-1",
		SnapshotPlaintextHash: "hash-1",
	}
	snapshot := snapshotstore.AuditSnapshot{
		LogID:     "log-1",
		ClientID:  "client-1",
		HashValue: "hash-1",
	}
	if err := validateSnapshotIdentity(snapshot, request); err != nil {
		t.Fatalf("valid snapshot identity rejected: %v", err)
	}

	snapshot.LogID = "log-2"
	if err := validateSnapshotIdentity(snapshot, request); err == nil || err.Error() != "cross_log_recovery_not_allowed" {
		t.Fatalf("cross-log error = %v, want cross_log_recovery_not_allowed", err)
	}

	snapshot.LogID = "log-1"
	snapshot.HashValue = "hash-2"
	if err := validateSnapshotIdentity(snapshot, request); err == nil || err.Error() != "snapshot_plaintext_hash_mismatch" {
		t.Fatalf("hash mismatch error = %v, want snapshot_plaintext_hash_mismatch", err)
	}
}

func TestServiceRecoveryCutoffKeepsLegacyOutOfScope(t *testing.T) {
	cutoff := time.Date(2026, 9, 18, 14, 34, 33, 0, time.UTC)
	service := NewService(nil, nil, nil, nil)
	service.SetRecoveryCutoff(&cutoff)

	legacyTime := cutoff.Add(-time.Second)
	legacy := &models.AuditLog{DBTimestamp: &legacyTime}
	if service.inRecoveryScope(legacy) {
		t.Fatal("legacy audit log unexpectedly belongs to recovery scope")
	}

	newTime := cutoff.Add(time.Second)
	newLog := &models.AuditLog{DBTimestamp: &newTime}
	if !service.inRecoveryScope(newLog) {
		t.Fatal("new audit log unexpectedly excluded from recovery scope")
	}
}

func TestClientExecutableStatus(t *testing.T) {
	for _, status := range []string{
		models.RecoveryStatusPendingExecution,
		models.RecoveryStatusPendingApproval,
		models.RecoveryStatusApproved,
	} {
		if !isClientExecutableStatus(status) {
			t.Fatalf("status %q should be executable by an authenticated client user", status)
		}
	}

	for _, status := range []string{
		models.RecoveryStatusRejected,
		models.RecoveryStatusExecuting,
		models.RecoveryStatusSucceeded,
		models.RecoveryStatusFailedVerification,
	} {
		if isClientExecutableStatus(status) {
			t.Fatalf("terminal/in-flight status %q must not be executable", status)
		}
	}
}
