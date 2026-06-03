package audit

import (
	"encoding/json"
	"errors"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
)

type VerificationResult struct {
	Status       string  `json:"status"`
	Message      string  `json:"message"`
	IsValid      bool    `json:"is_valid"`
	ExpectedHash string  `json:"expected_hash"`
	ActualHash   string  `json:"actual_hash"`
	DBRoot       string  `json:"db_root"`
	ChainRoot    string  `json:"chain_root"`
	LogID        string  `json:"log_id"`
	TxID         *string `json:"blockchain_tx_id,omitempty"`
}

type DataVerificationResult struct {
	Status       string      `json:"status"`
	Message      string      `json:"message"`
	IsValid      bool        `json:"is_valid"`
	Resource     string      `json:"resource"`
	ExpectedData interface{} `json:"expected_data"`
	ActualData   interface{} `json:"actual_data"`
	LastLogID    string      `json:"last_log_id"`
}

type Service interface {
	GetDashboardStats(clientID string) (map[string]int64, error)
	VerifyLogIntegrity(hash, clientID string) (*VerificationResult, error)
	GetFabricRecord(anchorID string) (map[string]interface{}, error)
	VerifyDataIntegrity(resource, clientID string, rawData *map[string]interface{}) (*DataVerificationResult, error)
	GetRecentLogs(limit int, clientID string) ([]models.AuditLog, error)
	GetResourceInventory(clientID string) ([]models.AuditLog, error)
	VerifyResourceHistory(resource, clientID string) (*VerificationResult, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
}

type auditService struct {
	repo   AuditRepository
	fabric *blockchain.FabricService
}

func NewService(repo AuditRepository, fabric *blockchain.FabricService) Service {
	return &auditService{repo: repo, fabric: fabric}
}

func (s *auditService) GetDashboardStats(clientID string) (map[string]int64, error) {
	return s.repo.GetDashboardStats(clientID)
}

func (s *auditService) VerifyLogIntegrity(hash, clientID string) (*VerificationResult, error) {
	auditLog, err := s.repo.GetLogByHash(hash, clientID)
	if err != nil {
		return nil, errors.New("log_not_found")
	}

	// Re-hash untuk cek manipulasi lokal
	recalculatedHash := hasher.GenerateLogHash(auditLog, auditLog.PreviousHash)
	if recalculatedHash != auditLog.HashValue {
		return &VerificationResult{
			Status:       "failed_local",
			Message:      "🚨 DATA TERMANIPULASI: Isi data telah diubah di database.",
			IsValid:      false,
			ExpectedHash: auditLog.HashValue,
			ActualHash:   recalculatedHash,
		}, nil
	}

	if auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "PENDING_OR_FAILED" {
		return &VerificationResult{
			Status:  "pending",
			Message: "Log otentik secara lokal, namun masih dalam proses antrean ke Blockchain.",
			IsValid: true,
		}, nil
	}

	onChainData, err := s.fabric.GetAnchorFromLedger(*auditLog.BlockchainTxID)
	if err != nil {
		return nil, errors.New("fabric_error")
	}

	var fabricResponse struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(onChainData), &fabricResponse); err != nil {
		return nil, errors.New("parse_error")
	}

	if fabricResponse.MerkleRoot != auditLog.MerkleRoot {
		return &VerificationResult{
			Status:    "failed_onchain",
			Message:   "🚨 FATAL MISMATCH: Merkle Root tidak diakui oleh jaringan Blockchain!",
			IsValid:   false,
			DBRoot:    auditLog.MerkleRoot,
			ChainRoot: fabricResponse.MerkleRoot,
		}, nil
	}

	return &VerificationResult{
		Status:       "success",
		Message:      "✅ DATA OTENTIK 100%: Data utuh dan Merkle Root terverifikasi.",
		IsValid:      true,
		LogID:        auditLog.LogID,
		ExpectedHash: auditLog.HashValue,
		DBRoot:       auditLog.MerkleRoot,
		TxID:         auditLog.BlockchainTxID,
	}, nil
}

func (s *auditService) GetFabricRecord(anchorID string) (map[string]interface{}, error) {
	if s.fabric == nil {
		return nil, errors.New("fabric_bypass")
	}

	fabricDataString, err := s.fabric.GetAnchorFromLedger(anchorID)
	if err != nil {
		return nil, errors.New("fabric_not_found")
	}

	var jsonResponse map[string]interface{}
	if err := json.Unmarshal([]byte(fabricDataString), &jsonResponse); err != nil {
		return nil, errors.New("parse_error")
	}

	return jsonResponse, nil
}

func (s *auditService) VerifyDataIntegrity(resource, clientID string, rawData *map[string]interface{}) (*DataVerificationResult, error) {
	lastLog, err := s.repo.GetLatestLogByResource(resource, clientID)
	if err != nil {
		return nil, errors.New("log_not_found")
	}

	var actualHash string
	var actualData interface{}
	isDataEmpty := rawData == nil || len(*rawData) == 0

	if !isDataEmpty {
		dataBytes, _ := json.Marshal(*rawData)
		actualHash = crypto.GenerateSHA3_256(string(dataBytes))
		actualData = *rawData
	}

	var expectedHash string
	var expectedData interface{}

	if lastLog.Metadata != "" && lastLog.Metadata != "{}" && lastLog.Metadata != "null" {
		var parsedMetadata map[string]interface{}
		if err := json.Unmarshal([]byte(lastLog.Metadata), &parsedMetadata); err == nil {
			expectedData = parsedMetadata
			expectedBytes, _ := json.Marshal(parsedMetadata)
			expectedHash = crypto.GenerateSHA3_256(string(expectedBytes))
		} else {
			expectedData = lastLog.Metadata
			expectedHash = crypto.GenerateSHA3_256(lastLog.Metadata)
		}
	}

	isValid := false
	status := "failed"
	var msg string

	isLastActionDelete := lastLog.Action == "DELETE"

	if isLastActionDelete {
		if isDataEmpty {
			isValid = true
			status = "success"
			msg = "✅ DATA VALID: Log terakhir adalah DELETE, dan data aktual memang kosong."
		} else {
			msg = "🔴 DATA TERMANIPULASI (GHOST DATA): Log DELETE ditemukan tapi data masih ada!"
		}
	} else {
		if isDataEmpty {
			msg = "🔴 DATA TERMANIPULASI (ILLEGAL DELETION): Log terakhir bukan DELETE, tapi data HILANG!"
		} else {
			if actualHash == expectedHash {
				isValid = true
				status = "success"
				msg = "✅ DATA VALID: Kondisi data aktual sama persis dengan jejak terakhir."
			} else {
				msg = "🔴 DATA TERMANIPULASI (UNAUTHORIZED MODIFICATION): Isi data berbeda dari jejak sah."
			}
		}
	}

	return &DataVerificationResult{
		Status:       status,
		Message:      msg,
		IsValid:      isValid,
		Resource:     resource,
		ExpectedData: expectedData,
		ActualData:   actualData,
		LastLogID:    lastLog.LogID,
	}, nil
}

func (s *auditService) GetRecentLogs(limit int, clientID string) ([]models.AuditLog, error) {
	return s.repo.GetRecentLogs(limit, clientID)
}

func (s *auditService) GetResourceInventory(clientID string) ([]models.AuditLog, error) {
	return s.repo.GetResourceInventory(clientID)
}

func (s *auditService) VerifyResourceHistory(resource, clientID string) (*VerificationResult, error) {
	logs, err := s.repo.GetLogsByResource(resource, clientID)
	if err != nil || len(logs) == 0 {
		return nil, errors.New("log_not_found")
	}

	var expectedPrevHash string
	hasPending := false
	var lastValidResult *VerificationResult

	for i, log := range logs {
		res, err := s.VerifyLogIntegrity(log.HashValue, clientID)
		if err != nil {
			return &VerificationResult{
				Status:  "failed_system",
				Message: "🚨 Gagal memverifikasi salah satu riwayat masa lalu.",
				IsValid: false,
			}, nil
		}

		if !res.IsValid {
			res.Message = "🚨 RIWAYAT TERMANIPULASI: Log aksi '" + log.Action + "' telah dirusak! (" + res.Message + ")"
			return res, nil
		}

		if i > 0 && log.PreviousHash != expectedPrevHash {
			return &VerificationResult{
				Status:       "failed_chain",
				Message:      "🚨 RANTAI TERPUTUS: Previous Hash tidak cocok (indikasi penyisipan/penghapusan log)!",
				IsValid:      false,
				ExpectedHash: expectedPrevHash,
				ActualHash:   log.PreviousHash,
			}, nil
		}
		expectedPrevHash = log.HashValue

		if res.Status == "pending" {
			hasPending = true
		}
		lastValidResult = res
	}

	if lastValidResult != nil && hasPending {
		lastValidResult.Status = "pending"
		lastValidResult.Message = "✅ RIWAYAT LOKAL AMAN: Beberapa log masih dalam antrean Blockchain."
	} else if lastValidResult != nil {
		lastValidResult.Status = "success"
		lastValidResult.Message = "✅ RIWAYAT OTENTIK 100%: Seluruh rantai transaksi tidak pernah dimanipulasi."
	}

	return lastValidResult, nil
}

func (s *auditService) GetLogsByResource(resource, clientID string) ([]models.AuditLog, error) {
	return s.repo.GetLogsByResource(resource, clientID)
}
