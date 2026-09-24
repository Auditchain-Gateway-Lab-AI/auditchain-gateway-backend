package audit

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
)

type rangeGuardRepo struct {
	count int64
}

func (r *rangeGuardRepo) CreateLog(*models.AuditLog) error { return nil }
func (r *rangeGuardRepo) GetLogByHash(string, string) (*models.AuditLog, error) {
	return nil, errors.New("not implemented")
}
func (r *rangeGuardRepo) GetLogByID(string, string) (*models.AuditLog, error) {
	return nil, errors.New("not implemented")
}
func (r *rangeGuardRepo) GetProofsByHash(string) ([]models.MerkleProof, error) { return nil, nil }
func (r *rangeGuardRepo) GetDashboardStats(string) (map[string]int64, error) {
	return map[string]int64{}, nil
}
func (r *rangeGuardRepo) GetLatestLogByResource(string, string) (*models.AuditLog, error) {
	return nil, errors.New("not implemented")
}
func (r *rangeGuardRepo) GetLatestClientLogByResource(string, string) (*models.AuditLog, error) {
	return nil, errors.New("not implemented")
}
func (r *rangeGuardRepo) GetClientDBEngine(string) (string, error) { return "postgres", nil }
func (r *rangeGuardRepo) GetRecentLogsPage(string, int, int, string, string, *time.Time, *time.Time) ([]models.AuditLog, int64, error) {
	return nil, 0, nil
}
func (r *rangeGuardRepo) CountAnchoredLogs(string) (int64, error) { return 0, nil }
func (r *rangeGuardRepo) GetAnchoredLogsPage(string, int, int) ([]models.AuditLog, error) {
	return nil, nil
}
func (r *rangeGuardRepo) GetResourceInventory(string) ([]models.AuditLog, error) { return nil, nil }
func (r *rangeGuardRepo) GetClientTables(string) ([]models.ClientTable, error)   { return nil, nil }
func (r *rangeGuardRepo) UpsertClientTable(string, string, string, string, time.Time) error {
	return nil
}
func (r *rangeGuardRepo) GetLogsByResource(string, string) ([]models.AuditLog, error) {
	return nil, nil
}
func (r *rangeGuardRepo) GetTableResources(string, string) ([]models.AuditLog, error) {
	return nil, nil
}
func (r *rangeGuardRepo) CountLogsByTimeRange(time.Time, time.Time, string) (int64, error) {
	return r.count, nil
}
func (r *rangeGuardRepo) GetLogsByTimeRange(time.Time, time.Time, string) ([]models.AuditLog, error) {
	return nil, nil
}

func TestNormalizeAuditLogPageSize(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{name: "default", raw: "", want: 10},
		{name: "invalid", raw: "abc", want: 10},
		{name: "non-positive", raw: "0", want: 10},
		{name: "accepted", raw: "50", want: 50},
		{name: "clamped", raw: "10000", want: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeAuditLogPageSize(tt.raw); got != tt.want {
				t.Fatalf("normalizeAuditLogPageSize(%q) = %d, want %d", tt.raw, got, tt.want)
			}
		})
	}
}

func TestVerifyLogRangeRejectsMoreThanSyncLimit(t *testing.T) {
	repo := &rangeGuardRepo{count: MaxSynchronousVerifyRange + 1}
	service := &auditService{repo: repo}

	_, err := service.VerifyLogRange(time.Now().Add(-time.Hour), time.Now(), "client-1", "request-1")
	var tooLarge *VerifyRangeTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("VerifyLogRange error = %v, want VerifyRangeTooLargeError", err)
	}
	if tooLarge.EstimatedItems != MaxSynchronousVerifyRange+1 || tooLarge.Limit != MaxSynchronousVerifyRange {
		t.Fatalf("unexpected guard payload: %+v", tooLarge)
	}
}

func TestEstimateLogRangeReturnsRepositoryCount(t *testing.T) {
	repo := &rangeGuardRepo{count: 42}
	service := &auditService{repo: repo}

	got, err := service.EstimateLogRange(time.Now().Add(-time.Hour), time.Now(), "client-1")
	if err != nil {
		t.Fatalf("EstimateLogRange returned error: %v", err)
	}
	if got != 42 {
		t.Fatalf("EstimateLogRange = %d, want 42", got)
	}
}

func newPR0TestRouter(handler *Handler) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("client_id", "client-1")
		c.Next()
	})
	router.GET("/estimate", handler.EstimateLogRange)
	router.GET("/verify", handler.VerifyLogRange)
	router.GET("/logs", handler.GetRecentLogs)
	return router
}

func TestEstimateHandlerAndVerifyHandlerEnforceSyncLimit(t *testing.T) {
	repo := &rangeGuardRepo{count: 296}
	handler := NewHandler(&auditService{repo: repo})
	router := newPR0TestRouter(handler)
	query := "?from=2026-09-23T00:00:00Z&to=2026-09-24T00:00:00Z"

	estimateRecorder := httptest.NewRecorder()
	router.ServeHTTP(estimateRecorder, httptest.NewRequest(http.MethodGet, "/estimate"+query, nil))
	if estimateRecorder.Code != http.StatusOK {
		t.Fatalf("estimate status = %d, want 200", estimateRecorder.Code)
	}
	var estimate VerifyRangeEstimateResponse
	if err := json.Unmarshal(estimateRecorder.Body.Bytes(), &estimate); err != nil {
		t.Fatalf("decode estimate response: %v", err)
	}
	if estimate.EstimatedItems != 296 || estimate.SyncLimit != 100 || estimate.CanVerifySync {
		t.Fatalf("unexpected estimate response: %+v", estimate)
	}

	verifyRecorder := httptest.NewRecorder()
	router.ServeHTTP(verifyRecorder, httptest.NewRequest(http.MethodGet, "/verify"+query, nil))
	if verifyRecorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("verify status = %d, want 422; body=%s", verifyRecorder.Code, verifyRecorder.Body.String())
	}
	var verifyBody map[string]interface{}
	if err := json.Unmarshal(verifyRecorder.Body.Bytes(), &verifyBody); err != nil {
		t.Fatalf("decode verify response: %v", err)
	}
	if verifyBody["code"] != "VERIFY_RANGE_TOO_LARGE" {
		t.Fatalf("verify code = %v, want VERIFY_RANGE_TOO_LARGE", verifyBody["code"])
	}
}

func TestGetRecentLogsHandlerCapsPageSizeAt100(t *testing.T) {
	handler := NewHandler(&auditService{repo: &rangeGuardRepo{}})
	router := newPR0TestRouter(handler)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/logs?page=1&page_size=10000", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("logs status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	var result RecentLogsResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode logs response: %v", err)
	}
	if result.Pagination.PageSize != 100 {
		t.Fatalf("pagination.page_size = %d, want 100", result.Pagination.PageSize)
	}
}
