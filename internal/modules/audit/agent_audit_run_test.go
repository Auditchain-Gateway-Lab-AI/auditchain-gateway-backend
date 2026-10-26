package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"go-blockchain-api/internal/blockchain/agentverifier"
)

type agentAuditRunnerTestStub struct {
	clientID string
	from     time.Time
	to       time.Time
	report   *agentverifier.AuditRunReport
	err      error
	calls    int
}

func (s *agentAuditRunnerTestStub) RunAudit(_ context.Context, clientID string, from, to time.Time) (*agentverifier.AuditRunReport, error) {
	s.calls++
	s.clientID, s.from, s.to = clientID, from, to
	return s.report, s.err
}

type agentAuditReportProcessorStub struct {
	Service
	clientID string
	report   AgentAuditReportRequest
	err      error
}

func (s *agentAuditReportProcessorStub) ProcessAgentAuditReport(clientID string, report AgentAuditReportRequest) error {
	s.clientID, s.report = clientID, report
	return s.err
}

func newAgentAuditRunContext(clientID, body string) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/dashboard/agent/audit-run", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	if clientID != "" {
		ctx.Set("client_id", clientID)
	}
	return ctx, recorder
}

func TestRunAgentAuditUsesJWTTenantAndReturnsSummaryOnly(t *testing.T) {
	to := time.Now().UTC().Add(-5 * time.Minute)
	from := to.Add(-45 * time.Minute)
	runner := &agentAuditRunnerTestStub{report: &agentverifier.AuditRunReport{
		RunID: "run-1019", From: from, To: to, TotalLogs: 2, AuditedLogs: 2,
		DataValid: 1, DataTampered: 1, BlockchainValid: 1,
		Results: []agentverifier.AuditRunLogResult{{LogID: "secret-log", Resource: "RUANGAN:1019", DataStatus: "TAMPERED", Error: "private row detail"}},
	}}
	processor := &agentAuditReportProcessorStub{}
	handler := NewHandler(processor)
	handler.AgentAuditRunner = runner
	body, _ := json.Marshal(agentAuditRunInput{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)})
	ctx, recorder := newAgentAuditRunContext("tenant-from-jwt", string(body))

	handler.RunAgentAudit(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if runner.clientID != "tenant-from-jwt" || processor.clientID != "tenant-from-jwt" {
		t.Fatalf("tenant runner/process = %q/%q", runner.clientID, processor.clientID)
	}
	if len(processor.report.Results) != 1 || processor.report.Results[0].DataStatus != "TAMPERED" {
		t.Fatalf("report was not processed: %#v", processor.report)
	}
	if strings.Contains(recorder.Body.String(), "secret-log") || strings.Contains(recorder.Body.String(), "private row detail") || strings.Contains(recorder.Body.String(), "results") {
		t.Fatalf("response exposed per-log data: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "run-1019") || !strings.Contains(recorder.Body.String(), "data_tampered") {
		t.Fatalf("summary fields missing: %s", recorder.Body.String())
	}
}

func TestRunAgentAuditAllowsWideTimeRanges(t *testing.T) {
	runner := &agentAuditRunnerTestStub{report: &agentverifier.AuditRunReport{RunID: "run-wide", TotalLogs: 1, AuditedLogs: 1}}
	handler := NewHandler(&agentAuditReportProcessorStub{})
	handler.AgentAuditRunner = runner
	to := time.Now().UTC().Add(-5 * time.Minute)
	from := to.Add(-48 * time.Hour) // 2 days
	body, _ := json.Marshal(agentAuditRunInput{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)})
	ctx, recorder := newAgentAuditRunContext("tenant-1", string(body))

	handler.RunAgentAudit(ctx)

	if recorder.Code != http.StatusOK || runner.calls != 1 {
		t.Fatalf("status/calls = %d/%d body=%s", recorder.Code, runner.calls, recorder.Body.String())
	}
}

func TestRunAgentAuditMapsLogLimitAndPersistenceErrors(t *testing.T) {
	to := time.Now().UTC().Add(-5 * time.Minute)
	from := to.Add(-time.Hour)
	runner := &agentAuditRunnerTestStub{err: &agentverifier.AuditRangeLimitError{EstimatedLogs: 101, Limit: 100}}
	handler := NewHandler(&agentAuditReportProcessorStub{})
	handler.AgentAuditRunner = runner
	body, _ := json.Marshal(agentAuditRunInput{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)})
	ctx, recorder := newAgentAuditRunContext("tenant-1", string(body))
	handler.RunAgentAudit(ctx)
	if recorder.Code != http.StatusUnprocessableEntity || !strings.Contains(recorder.Body.String(), "101") {
		t.Fatalf("limit response status/body = %d/%s", recorder.Code, recorder.Body.String())
	}

	processor := &agentAuditReportProcessorStub{err: errors.New("db unavailable")}
	handler = NewHandler(processor)
	handler.AgentAuditRunner = &agentAuditRunnerTestStub{report: &agentverifier.AuditRunReport{RunID: "r", From: from, To: to}}
	ctx, recorder = newAgentAuditRunContext("tenant-1", string(body))
	handler.RunAgentAudit(ctx)
	if recorder.Code != http.StatusInternalServerError || strings.Contains(recorder.Body.String(), "db unavailable") {
		t.Fatalf("persistence response status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}

func TestRunAgentAuditRequiresAuthenticatedTenant(t *testing.T) {
	runner := &agentAuditRunnerTestStub{}
	handler := NewHandler(&agentAuditReportProcessorStub{})
	handler.AgentAuditRunner = runner
	ctx, recorder := newAgentAuditRunContext("", `{"from":"2026-10-20T16:00:00Z","to":"2026-10-20T17:00:00Z"}`)
	handler.RunAgentAudit(ctx)
	if recorder.Code != http.StatusUnauthorized || runner.calls != 0 {
		t.Fatalf("status/calls = %d/%d body=%s", recorder.Code, runner.calls, bytes.TrimSpace(recorder.Body.Bytes()))
	}
}
