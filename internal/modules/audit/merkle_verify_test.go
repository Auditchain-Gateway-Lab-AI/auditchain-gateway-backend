package audit

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

type merkleVerifyServiceStub struct {
	Service
	lastLeafHash   string
	lastMerkleRoot string
	lastTxID       string
	mockResult     *MerkleVerificationResult
	mockErr        error
}

func (s *merkleVerifyServiceStub) VerifyMerkleProofAgainstBlockchain(leafHash, expectedMerkleRoot, blockchainTxID string) (*MerkleVerificationResult, error) {
	s.lastLeafHash = leafHash
	s.lastMerkleRoot = expectedMerkleRoot
	s.lastTxID = blockchainTxID
	return s.mockResult, s.mockErr
}

func TestVerifyMerkleRootForAgent_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	body := VerifyMerkleRootRequest{
		LeafHash:       "leaf-hash-123",
		MerkleRoot:     "root-hash-456",
		BlockchainTxID: "tx-chain-789",
	}
	bodyBytes, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, "/api/agent/verify-merkle-root", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	stub := &merkleVerifyServiceStub{
		mockResult: &MerkleVerificationResult{
			IsMatch:            true,
			LeafHash:           "leaf-hash-123",
			ReconstructedRoot:  "root-hash-456",
			ExpectedMerkleRoot: "root-hash-456",
			ActualMerkleRoot:   "root-hash-456",
			BlockchainTxID:     "tx-chain-789",
		},
	}
	handler := NewHandler(stub)

	handler.VerifyMerkleRootForAgent(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", recorder.Code, recorder.Body.String())
	}
	if stub.lastLeafHash != "leaf-hash-123" || stub.lastMerkleRoot != "root-hash-456" || stub.lastTxID != "tx-chain-789" {
		t.Errorf("unexpected call arguments: leaf=%s root=%s tx=%s", stub.lastLeafHash, stub.lastMerkleRoot, stub.lastTxID)
	}

	var resp MerkleVerificationResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp.IsMatch || resp.ReconstructedRoot != "root-hash-456" {
		t.Errorf("unexpected response: %+v", resp)
	}
}

func TestVerifyMerkleRootForAgent_MissingTxID_BadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	body := `{"leaf_hash":"leaf-123","merkle_root":"root-456"}`
	req, _ := http.NewRequest(http.MethodPost, "/api/agent/verify-merkle-root", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	stub := &merkleVerifyServiceStub{}
	handler := NewHandler(stub)

	handler.VerifyMerkleRootForAgent(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing blockchain_tx_id, got %d", recorder.Code)
	}
}

func TestVerifyMerkleRootForAgent_ServiceError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)

	body := VerifyMerkleRootRequest{
		LeafHash:       "leaf-123",
		BlockchainTxID: "tx-chain-789",
	}
	bodyBytes, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, "/api/agent/verify-merkle-root", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	stub := &merkleVerifyServiceStub{
		mockErr: errors.New("ledger unreachable"),
	}
	handler := NewHandler(stub)

	handler.VerifyMerkleRootForAgent(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 for service error, got %d", recorder.Code)
	}
}
