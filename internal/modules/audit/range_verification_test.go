package audit

import (
	"errors"
	"testing"
	"time"

	"go-blockchain-api/internal/models"
)

type rangeRepositoryStub struct {
	AuditRepository
	logs []models.AuditLog
}

func (r *rangeRepositoryStub) GetLogByID(logID, clientID string) (*models.AuditLog, error) {
	for _, row := range r.logs {
		if row.LogID == logID && row.ClientID == clientID {
			logCopy := row
			return &logCopy, nil
		}
	}
	return nil, errors.New("log_not_found")
}

func (r *rangeRepositoryStub) CountLogsByTimeRange(time.Time, time.Time, string) (int64, error) {
	return int64(len(r.logs)), nil
}

func (r *rangeRepositoryStub) GetLogsByTimeRange(time.Time, time.Time, string) ([]models.AuditLog, error) {
	return r.logs, nil
}

func (r *rangeRepositoryStub) GetLogsByTimeRangePage(_ time.Time, _ time.Time, _ string, limit, offset int) ([]models.AuditLog, error) {
	if offset >= len(r.logs) {
		return []models.AuditLog{}, nil
	}
	end := offset + limit
	if end > len(r.logs) {
		end = len(r.logs)
	}
	return r.logs[offset:end], nil
}

func TestManualVerifyLogRangeRechecksCachedRows(t *testing.T) {
	checkedAt := time.Date(2026, time.September, 30, 8, 0, 0, 0, time.UTC)
	repo := &rangeRepositoryStub{logs: []models.AuditLog{
		{
			LogID:              "log-1019",
			ClientID:           "client-1",
			Action:             "INSERT",
			Resource:           "RUANGAN:1019",
			Metadata:           `{"ID":1019,"NAMA":"tampered"}`,
			HashValue:          "hash-before-metadata-change",
			IntegrityStatus:    models.IntegrityStatusValid,
			IntegrityCheckedAt: &checkedAt,
		},
	}}

	service := &auditService{repo: repo}
	first, err := service.verifyLogRange(time.Time{}, time.Time{}, "client-1", "test-1")
	if err != nil {
		t.Fatalf("first range verification returned error: %v", err)
	}
	if first.Summary.Total != 1 || first.Summary.Valid != 0 || first.Summary.Invalid != 1 {
		t.Fatalf("unexpected first summary: %+v", first.Summary)
	}
	if first.Summary.AlreadyVerified != 0 || first.Summary.VerifiedNow != 1 {
		t.Fatalf("expected the manual range to recheck its cached log: %+v", first.Summary)
	}
	if len(first.Results) != 1 || first.Results[0].VerifyStatus != "failed_local" {
		t.Fatalf("manual range result = %+v, want fresh failed_local result", first.Results)
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
		{name: "unreachable is inconclusive", status: models.IntegrityStatusUnreachable, want: "inconclusive_cached"},
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

func TestAddRangeSummaryKeepsInconclusiveSeparateFromInvalid(t *testing.T) {
	var summary RangeSummary
	addRangeSummary(&summary, "inconclusive")
	addRangeSummary(&summary, "success")
	addRangeSummary(&summary, "failed_onchain")

	if summary.Total != 3 || summary.Inconclusive != 1 || summary.Valid != 1 || summary.Invalid != 1 {
		t.Fatalf("unexpected range summary: %+v", summary)
	}
}

func TestVerifyRangeBatchUsesBoundedPageAndCachedSummary(t *testing.T) {
	checkedAt := time.Date(2026, time.September, 30, 8, 0, 0, 0, time.UTC)
	repo := &rangeRepositoryStub{logs: []models.AuditLog{
		{LogID: "log-1", ClientID: "client-1", IntegrityStatus: models.IntegrityStatusValid, IntegrityCheckedAt: &checkedAt},
		{LogID: "log-2", ClientID: "client-1", IntegrityStatus: models.IntegrityStatusPending, IntegrityCheckedAt: &checkedAt},
	}}

	service := &auditService{repo: repo}
	summary, loaded, err := service.VerifyRangeBatch(time.Time{}, time.Time{}, "client-1", "batch-test", "run-1", models.IntegritySourceScheduledRun, 1, 1)
	if err != nil {
		t.Fatalf("batch verification returned error: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("loaded = %d, want 1", loaded)
	}
	if summary.Total != 1 || summary.Pending != 1 || summary.AlreadyVerified != 1 || summary.VerifiedNow != 0 {
		t.Fatalf("unexpected batch summary: %+v", summary)
	}
}

func TestUserRequestedBackgroundVerifyBatchRechecksCachedRows(t *testing.T) {
	checkedAt := time.Date(2026, time.September, 30, 8, 0, 0, 0, time.UTC)
	repo := &rangeRepositoryStub{logs: []models.AuditLog{
		{
			LogID:              "log-1019",
			ClientID:           "client-1",
			Action:             "INSERT",
			Resource:           "RUANGAN:1019",
			Metadata:           `{"ID":1019,"NAMA":"tampered"}`,
			HashValue:          "hash-before-metadata-change",
			IntegrityStatus:    models.IntegrityStatusValid,
			IntegrityCheckedAt: &checkedAt,
		},
	}}

	service := &auditService{repo: repo}
	summary, loaded, err := service.VerifyRangeBatch(
		time.Time{}, time.Time{}, "client-1", "manual-run", "run-1",
		models.IntegritySourceBackgroundRun, 10, 0,
	)
	if err != nil {
		t.Fatalf("manual background verification returned error: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("loaded = %d, want 1", loaded)
	}
	if summary.Total != 1 || summary.Valid != 0 || summary.Invalid != 1 || summary.AlreadyVerified != 0 || summary.VerifiedNow != 1 {
		t.Fatalf("user-requested background run did not recheck cached log: %+v", summary)
	}
}

func TestIntegritySourceForVerificationRun(t *testing.T) {
	tests := []struct {
		requestedBy string
		want        string
	}{
		{requestedBy: "system:scheduler", want: models.IntegritySourceScheduledRun},
		{requestedBy: " SYSTEM:SCHEDULER ", want: models.IntegritySourceScheduledRun},
		{requestedBy: "user-123", want: models.IntegritySourceBackgroundRun},
		{requestedBy: "", want: models.IntegritySourceBackgroundRun},
	}

	for _, test := range tests {
		if got := integritySourceForVerificationRun(test.requestedBy); got != test.want {
			t.Errorf("integritySourceForVerificationRun(%q) = %q, want %q", test.requestedBy, got, test.want)
		}
	}
}
