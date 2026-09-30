package internalaudit

import (
	"testing"
	"time"

	"go-blockchain-api/internal/models"
)

type rangeRepositoryStub struct {
	AuditRepository
	logs []models.AuditLog
}

func (r *rangeRepositoryStub) CountLogsByTimeRange(time.Time, time.Time, string) (int64, error) {
	return int64(len(r.logs)), nil
}

func (r *rangeRepositoryStub) GetLogsByTimeRange(time.Time, time.Time, string) ([]models.AuditLog, error) {
	return r.logs, nil
}

func TestVerifyLogRangeUsesEveryLogAndCachedResults(t *testing.T) {
	checkedAt := time.Date(2026, time.September, 30, 8, 0, 0, 0, time.UTC)
	repo := &rangeRepositoryStub{logs: []models.AuditLog{
		{
			LogID:              "log-1",
			ClientID:           "client-1",
			Resource:           "RUANGAN:283",
			IntegrityStatus:    models.IntegrityStatusValid,
			IntegrityCheckedAt: &checkedAt,
		},
		{
			LogID:              "log-2",
			ClientID:           "client-1",
			Resource:           "RUANGAN:283",
			IntegrityStatus:    models.IntegrityStatusTampered,
			IntegrityCheckedAt: &checkedAt,
		},
	}}

	service := &auditService{repo: repo}
	first, err := service.verifyLogRange(time.Time{}, time.Time{}, "client-1", "test-1")
	if err != nil {
		t.Fatalf("first range verification returned error: %v", err)
	}
	if first.Summary.Total != 2 || first.Summary.Valid != 1 || first.Summary.Invalid != 1 {
		t.Fatalf("unexpected first summary: %+v", first.Summary)
	}
	if first.Summary.AlreadyVerified != 2 || first.Summary.VerifiedNow != 0 {
		t.Fatalf("expected both logs to use cached results: %+v", first.Summary)
	}

	second, err := service.verifyLogRange(time.Time{}, time.Time{}, "client-1", "test-2")
	if err != nil {
		t.Fatalf("replayed range verification returned error: %v", err)
	}
	if second.Summary != first.Summary {
		t.Fatalf("replayed range changed summary: first=%+v second=%+v", first.Summary, second.Summary)
	}
}

func TestCachedRangeStatus(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   string
	}{
		{name: "valid", status: models.IntegrityStatusValid, want: "success"},
		{name: "pending", status: models.IntegrityStatusPending, want: "pending"},
		{name: "tampered", status: models.IntegrityStatusTampered, want: "failed_cached"},
		{name: "unreachable", status: models.IntegrityStatusUnreachable, want: "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := cachedRangeStatus(tt.status)
			if got != tt.want {
				t.Fatalf("cachedRangeStatus(%q) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}
