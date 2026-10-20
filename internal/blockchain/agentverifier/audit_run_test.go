package agentverifier

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go-blockchain-api/internal/models"
)

func TestPostAgentAuditUsesTenantReadTokenAndDisablesAutoRecover(t *testing.T) {
	from := time.Date(2026, 10, 20, 16, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/audit/run" {
			t.Fatalf("request = %s %s, want POST /audit/run", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tenant-verify-token" {
			t.Fatalf("authorization = %q", got)
		}
		var payload struct {
			From        string `json:"from"`
			To          string `json:"to"`
			AutoRecover bool   `json:"auto_recover"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload.From != from.Format(time.RFC3339Nano) || payload.To != to.Format(time.RFC3339Nano) || payload.AutoRecover {
			t.Fatalf("audit request payload = %#v", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"run-1019","from":"2026-10-20T16:00:00Z","to":"2026-10-20T17:00:00Z","total_logs":1,"audited_logs":1,"data_tampered":1,"results":[{"log_id":"log-1019","resource":"RUANGAN:1019","data_status":"TAMPERED","blockchain_status":"SKIPPED"}]}`))
	}))
	defer server.Close()

	report, err := postAgentAudit(context.Background(), &models.AgentConfig{
		AgentURL: server.URL, VerifyToken: "tenant-verify-token", TimeoutSeconds: 1,
	}, from, to, time.Second)
	if err != nil {
		t.Fatalf("postAgentAudit() error = %v", err)
	}
	if report.RunID != "run-1019" || len(report.Results) != 1 || report.Results[0].DataStatus != "TAMPERED" {
		t.Fatalf("unexpected report: %#v", report)
	}
}

func TestPostAgentAuditRejectsReportsAboveSyncLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"run_id":"large","total_logs":101,"results":[]}`))
	}))
	defer server.Close()

	_, err := postAgentAudit(context.Background(), &models.AgentConfig{AgentURL: server.URL, VerifyToken: "tenant-token"}, time.Now().Add(-time.Hour), time.Now(), time.Second)
	var limitErr *AuditRangeLimitError
	if !errors.As(err, &limitErr) || limitErr.EstimatedLogs != 101 || limitErr.Limit != MaxSynchronousAgentAuditLogs {
		t.Fatalf("error = %v, want bounded-range error", err)
	}
}

func TestPostAgentAuditHonorsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_, _ = w.Write([]byte(`{"run_id":"late"}`))
	}))
	defer server.Close()

	_, err := postAgentAudit(context.Background(), &models.AgentConfig{AgentURL: server.URL, VerifyToken: "tenant-token"}, time.Now().Add(-time.Hour), time.Now(), 20*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestPostAgentAuditDoesNotExposeAgentErrorBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("sensitive agent response"))
	}))
	defer server.Close()

	_, err := postAgentAudit(context.Background(), &models.AgentConfig{AgentURL: server.URL, VerifyToken: "tenant-token"}, time.Now().Add(-time.Hour), time.Now(), time.Second)
	if err == nil || err.Error() != "agent_audit_http_401" {
		t.Fatalf("error = %v, want sanitized Agent status", err)
	}
}
