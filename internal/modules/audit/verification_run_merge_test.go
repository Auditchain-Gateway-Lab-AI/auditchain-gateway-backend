package audit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestLatestVerificationRunQueryKeepsTenantAndAuditDomain(t *testing.T) {
	tests := []struct {
		name, runType string
		internal      bool
	}{
		{"default", "", true},
		{"internal", "INTERNAL", true},
		{"normalized internal", " internal ", true},
		{"external", "EXTERNAL", false},
		{"normalized external", " external ", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := gorm.Open(postgres.New(postgres.Config{DSN: "host=localhost dbname=unused"}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
			if err != nil {
				t.Fatal(err)
			}
			sqlDB, err := db.DB()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			var query string
			if err := db.Callback().Query().After("gorm:query").Register("capture_latest_run", func(tx *gorm.DB) {
				query = tx.Dialector.Explain(tx.Statement.SQL.String(), tx.Statement.Vars...)
			}); err != nil {
				t.Fatal(err)
			}
			service := &VerificationJobService{db: db}
			if _, err := service.GetLatestRunByType("tenant-a", test.runType); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"client_id = 'tenant-a'", "created_at DESC", "LIMIT 1"} {
				if !strings.Contains(query, want) {
					t.Fatalf("query missing %q: %s", want, query)
				}
			}
			if test.internal {
				if !strings.Contains(query, "run_type = 'INTERNAL'") || !strings.Contains(query, "run_type IS NULL") || !strings.Contains(query, "COALESCE(requested_by, '') <> 'agent'") {
					t.Fatalf("internal query must include legacy jobs and exclude Agent reports: %s", query)
				}
			} else if !strings.Contains(query, "run_type = 'EXTERNAL'") || !strings.Contains(query, "run_type IS NULL") || !strings.Contains(query, "requested_by = 'agent'") {
				t.Fatalf("external query must include legacy Agent reports: %s", query)
			}
		})
	}
}

func TestLatestVerificationRunRejectsUnsupportedType(t *testing.T) {
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(response)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/dashboard/verification-runs/latest?run_type=ALL", nil)
	ctx.Set("client_id", "tenant-a")
	h := &Handler{VerificationJobs: &VerificationJobService{}}
	h.GetLatestVerificationRun(ctx)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", response.Code, response.Body.String())
	}
}

func TestVerificationRunResponsePreservesAuditDomain(t *testing.T) {
	tests := []struct {
		name, runType, requestedBy, want string
	}{
		{"internal job", models.VerificationRunTypeInternal, "user-1", models.VerificationRunTypeInternal},
		{"external Agent report", models.VerificationRunTypeExternal, "agent", models.VerificationRunTypeExternal},
		{"legacy internal job", "", "user-1", models.VerificationRunTypeInternal},
		{"legacy Agent report", "", "agent", models.VerificationRunTypeExternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &models.VerificationRun{ID: "run-1", RunType: test.runType, RequestedBy: test.requestedBy}
			if got := ToVerificationRunResponse(run).RunType; got != test.want {
				t.Fatalf("run_type = %q, want %q", got, test.want)
			}
		})
	}
}

func TestVerificationRunLeaseRemainsPrivateAlongsideRunType(t *testing.T) {
	expires := time.Now().Add(time.Minute)
	raw, err := json.Marshal(models.VerificationRun{
		RunType: models.VerificationRunTypeInternal, LeaseOwner: "private-worker",
		LeaseExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-worker") || strings.Contains(string(raw), "lease_") {
		t.Fatalf("worker lease leaked in API JSON: %s", raw)
	}
	var response map[string]interface{}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if response["run_type"] != models.VerificationRunTypeInternal {
		t.Fatalf("mentor run_type contract missing: %s", raw)
	}
}
