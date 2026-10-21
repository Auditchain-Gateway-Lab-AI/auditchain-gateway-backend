package auditreference

import (
	"testing"
	"time"

	"go-blockchain-api/internal/models"
)

func TestLatestClientEventIndexUsesNewestEvenWhenPending(t *testing.T) {
	older := time.Date(2026, 10, 21, 1, 0, 0, 0, time.UTC)
	newerTime := older.Add(time.Minute)
	logs := []models.AuditLog{
		{LogID: "anchored", Action: "UPDATE", Timestamp: older, Status: "ANCHORED"},
		{LogID: "recovery", Action: "RECOVERY", Timestamp: newerTime.Add(time.Minute)},
		{LogID: "pending", Action: "UPDATE", Timestamp: newerTime, Status: "PENDING"},
	}
	if got := LatestClientEventIndex(logs); got != 2 {
		t.Fatalf("latest index = %d, want pending event at 2", got)
	}
}

func TestLatestClientEventIndexSkipsRecoveryAndUsesDatabaseTimestamp(t *testing.T) {
	stamp := time.Date(2026, 10, 21, 1, 0, 0, 0, time.UTC)
	dbStamp := stamp.Add(time.Second)
	logs := []models.AuditLog{
		{LogID: "a", Action: "UPDATE", Timestamp: stamp},
		{LogID: "b", Action: "INSERT", Timestamp: stamp, DBTimestamp: &dbStamp},
		{LogID: "c", Action: " RECOVERY ", Timestamp: stamp.Add(time.Hour)},
	}
	if got := LatestClientEventIndex(logs); got != 1 {
		t.Fatalf("latest index = %d, want 1", got)
	}
	if got := LatestClientEventIndex(logs[2:]); got != -1 {
		t.Fatalf("recovery-only index = %d, want -1", got)
	}
}
