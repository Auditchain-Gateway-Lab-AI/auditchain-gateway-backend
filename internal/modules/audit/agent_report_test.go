package audit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type agentReportServiceStub struct {
	Service
	receivedClientID string
	receivedRequest  AgentAuditReportRequest
	err              error
}

func (s *agentReportServiceStub) ProcessAgentAuditReport(clientID string, req AgentAuditReportRequest) error {
	s.receivedClientID = clientID
	s.receivedRequest = req
	return s.err
}

func TestSubmitAgentAuditReport_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	ctx.Set("client_id", "test-client-123")

	now := time.Now().UTC()
	reqPayload := AgentAuditReportRequest{
		RunID:       "run-abc-xyz",
		From:        now.Add(-24 * time.Hour),
		To:          now,
		TotalLogs:   10,
		AuditedLogs: 5,
		DataValid:   4,
		DataTampered: 1,
		Results: []AgentAuditLogResult{
			{
				LogID:            "log-1",
				Resource:         "RUANGAN:101",
				Action:           "UPDATE",
				DataStatus:       "VALID",
				BlockchainStatus: "VALID",
			},
			{
				LogID:            "log-2",
				Resource:         "RUANGAN:102",
				Action:           "UPDATE",
				DataStatus:       "TAMPERED",
				BlockchainStatus: "VALID",
			},
		},
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost, "/api/agent/audit-report", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	stub := &agentReportServiceStub{}
	handler := NewHandler(stub)

	handler.SubmitAgentAuditReport(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
	if resp["run_id"] != "run-abc-xyz" {
		t.Errorf("expected run_id run-abc-xyz, got %v", resp["run_id"])
	}
	if stub.receivedClientID != "test-client-123" {
		t.Errorf("expected client_id test-client-123, got %s", stub.receivedClientID)
	}
	if stub.receivedRequest.AuditedLogs != 5 {
		t.Errorf("expected AuditedLogs 5, got %d", stub.receivedRequest.AuditedLogs)
	}
}

func TestSubmitAgentAuditReport_Unauthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	// No client_id in context

	req, _ := http.NewRequest(http.MethodPost, "/api/agent/audit-report", bytes.NewReader([]byte(`{}`)))
	ctx.Request = req

	stub := &agentReportServiceStub{}
	handler := NewHandler(stub)

	handler.SubmitAgentAuditReport(ctx)

	if recorder.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", recorder.Code)
	}
}
