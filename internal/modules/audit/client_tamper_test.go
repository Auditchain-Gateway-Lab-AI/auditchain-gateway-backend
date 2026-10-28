package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/models"
)

type clientTamperServiceStub struct {
	Service
	listCalled      bool
	getCalled       bool
	recoverCalled   bool
	dismissCalled    bool
	listEventsCalled bool
	lastClientID     string
	lastID          string
	lastRecoveredBy string
	lastDismissedBy string
	lastNotes       string
}

func (s *clientTamperServiceStub) ListClientTamperRecords(clientID string, query ClientTamperQuery) (*ClientTamperListResponse, error) {
	s.listCalled = true
	s.lastClientID = clientID
	return &ClientTamperListResponse{
		Records: []models.ClientDataTamperRecord{
			{
				ID:           "tamper-1",
				ClientID:     clientID,
				Resource:     "SATUPETA_POLYGON:1001",
				TableName:    "SATUPETA_POLYGON",
				RecordID:     "1001",
				ExpectedHash: "hash-expected-123",
				ActualHash:   "hash-actual-456",
				Status:       models.ClientTamperStatusOpen,
			},
		},
		Total:      1,
		Page:       1,
		PageSize:   10,
		TotalPages: 1,
		Summary: ClientTamperSummaryCounts{
			OpenCount:  1,
			TotalCount: 1,
		},
	}, nil
}

func (s *clientTamperServiceStub) GetClientTamperRecord(clientID, id string) (*models.ClientDataTamperRecord, error) {
	s.getCalled = true
	s.lastClientID = clientID
	s.lastID = id
	return &models.ClientDataTamperRecord{
		ID:           id,
		ClientID:     clientID,
		Resource:     "SATUPETA_POLYGON:1001",
		TableName:    "SATUPETA_POLYGON",
		RecordID:     "1001",
		ExpectedHash: "hash-expected-123",
		ActualHash:   "hash-actual-456",
		Status:       models.ClientTamperStatusOpen,
	}, nil
}

func (s *clientTamperServiceStub) RecoverClientTamperRecord(_ context.Context, clientID, id, recoveredBy string) (*ClientTamperRecoverResponse, error) {
	s.recoverCalled = true
	s.lastClientID = clientID
	s.lastID = id
	s.lastRecoveredBy = recoveredBy
	return &ClientTamperRecoverResponse{
		Record: models.ClientDataTamperRecord{
			ID:         id,
			ClientID:   clientID,
			Status:     models.ClientTamperStatusRecovered,
			ResolvedBy: recoveredBy,
		},
		Result: &agentverifier.ClientRecoverResponse{
			Status:    "SUCCESS",
			Table:     "SATUPETA_POLYGON",
			ID:        "1001",
			Operation: "UPDATE",
		},
		Message: "Data berhasil dipulihkan pada database klien",
	}, nil
}

func (s *clientTamperServiceStub) DismissClientTamperRecord(clientID, id, dismissedBy, notes string) (*models.ClientDataTamperRecord, error) {
	s.dismissCalled = true
	s.lastClientID = clientID
	s.lastID = id
	s.lastDismissedBy = dismissedBy
	s.lastNotes = notes
	return &models.ClientDataTamperRecord{
		ID:              id,
		ClientID:        clientID,
		Status:          models.ClientTamperStatusDismissed,
		ResolvedBy:      dismissedBy,
		ResolutionNotes: notes,
	}, nil
}

func (s *clientTamperServiceStub) ListClientRecoveryEvents(clientID string, query ClientRecoveryEventQuery) (*ClientRecoveryEventListResponse, error) {
	s.listEventsCalled = true
	s.lastClientID = clientID
	return &ClientRecoveryEventListResponse{
		Events: []models.ClientRecoveryEvent{
			{
				ID:             "rec-event-1",
				ClientID:       clientID,
				Resource:       "RUANGAN:1812",
				TableName:      "RUANGAN",
				RecordID:       "1812",
				Operation:      "UPDATE",
				Status:         models.ClientRecoveryStatusSuccess,
				RecoveredBy:    "morbis1",
			},
		},
		Total:      1,
		Page:       1,
		PageSize:   10,
		TotalPages: 1,
		Summary: ClientRecoveryEventSummaryCounts{
			SuccessCount: 1,
			TotalCount:   1,
		},
	}, nil
}

func TestListClientTamperRecords(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Set("client_id", "client-test-99")

	req, _ := http.NewRequest(http.MethodGet, "/dashboard/client-tampering?status=OPEN", nil)
	ctx.Request = req

	stub := &clientTamperServiceStub{}
	handler := NewHandler(stub)

	handler.ListClientTamperRecords(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !stub.listCalled {
		t.Error("expected ListClientTamperRecords to be called")
	}
	if stub.lastClientID != "client-test-99" {
		t.Errorf("expected clientID client-test-99, got %s", stub.lastClientID)
	}

	var resp ClientTamperListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed unmarshal: %v", err)
	}
	if len(resp.Records) != 1 || resp.Records[0].Resource != "SATUPETA_POLYGON:1001" {
		t.Errorf("unexpected records: %+v", resp.Records)
	}
}

func TestGetClientTamperRecord(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Set("client_id", "client-test-99")
	ctx.Params = gin.Params{{Key: "id", Value: "tamper-rec-42"}}

	req, _ := http.NewRequest(http.MethodGet, "/dashboard/client-tampering/tamper-rec-42", nil)
	ctx.Request = req

	stub := &clientTamperServiceStub{}
	handler := NewHandler(stub)

	handler.GetClientTamperRecord(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !stub.getCalled || stub.lastID != "tamper-rec-42" {
		t.Errorf("expected GetClientTamperRecord called with tamper-rec-42, got %s", stub.lastID)
	}
}

func TestRecoverClientTamperRecord(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Set("client_id", "client-test-99")
	ctx.Set("username", "admin_satupeta")
	ctx.Params = gin.Params{{Key: "id", Value: "tamper-rec-42"}}

	req, _ := http.NewRequest(http.MethodPost, "/dashboard/client-tampering/tamper-rec-42/recover", nil)
	ctx.Request = req

	stub := &clientTamperServiceStub{}
	handler := NewHandler(stub)

	handler.RecoverClientTamperRecord(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !stub.recoverCalled || stub.lastRecoveredBy != "admin_satupeta" {
		t.Errorf("expected Recover called by admin_satupeta, got %s", stub.lastRecoveredBy)
	}
}

func TestDismissClientTamperRecord(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Set("client_id", "client-test-99")
	ctx.Set("username", "admin_satupeta")
	ctx.Params = gin.Params{{Key: "id", Value: "tamper-rec-42"}}

	body := `{"notes":"False positive after database index rebuild"}`
	req, _ := http.NewRequest(http.MethodPost, "/dashboard/client-tampering/tamper-rec-42/dismiss", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	stub := &clientTamperServiceStub{}
	handler := NewHandler(stub)

	handler.DismissClientTamperRecord(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !stub.dismissCalled || stub.lastNotes != "False positive after database index rebuild" {
		t.Errorf("expected Dismiss with notes, got %s", stub.lastNotes)
	}
}


func TestListClientRecoveryEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Set("client_id", "client-test-99")

	req, _ := http.NewRequest(http.MethodGet, "/dashboard/client-recovery/events?status=SUCCESS", nil)
	ctx.Request = req

	stub := &clientTamperServiceStub{}
	handler := NewHandler(stub)

	handler.ListClientRecoveryEvents(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if !stub.listEventsCalled {
		t.Error("expected ListClientRecoveryEvents to be called")
	}
	if stub.lastClientID != "client-test-99" {
		t.Errorf("expected clientID client-test-99, got %s", stub.lastClientID)
	}

	var resp ClientRecoveryEventListResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed unmarshal: %v", err)
	}
	if len(resp.Events) != 1 || resp.Events[0].Resource != "RUANGAN:1812" {
		t.Errorf("unexpected events: %+v", resp.Events)
	}
}
