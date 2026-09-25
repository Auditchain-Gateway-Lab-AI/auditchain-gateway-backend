package clientverify

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"

	"gorm.io/gorm"
)

// DTOs
type RowVerificationResult struct {
	Resource       string                      `json:"resource"`
	LogID          string                      `json:"log_id"`
	Status         string                      `json:"status"`
	LastAction     string                      `json:"last_action"`
	LastTimestamp  time.Time                   `json:"last_timestamp"`
	BlockchainTxID string                      `json:"blockchain_tx_id,omitempty"`
	Message        string                      `json:"message"`
	Discrepancies  []agentverifier.Discrepancy `json:"discrepancies,omitempty"`
}

type BatchVerificationResponse struct {
	Table      string                  `json:"table"`
	VerifiedAt time.Time               `json:"verified_at"`
	Summary    map[string]int          `json:"summary"`
	Results    []RowVerificationResult `json:"results"`
}

type Service interface {
	VerifyTable(clientID, tableName string) (*BatchVerificationResponse, error)
}

type clientVerifyService struct {
	db     *gorm.DB
	agent  *agentverifier.Service
	fabric *blockchain.FabricService
}

func NewService(db *gorm.DB, agent *agentverifier.Service, fabric *blockchain.FabricService) Service {
	return &clientVerifyService{
		db:     db,
		agent:  agent,
		fabric: fabric,
	}
}

func (s *clientVerifyService) VerifyTable(clientID, tableName string) (*BatchVerificationResponse, error) {
	// Temukan semua log terbaru per baris dari tabel yang diminta
	var latestLogs []models.AuditLog
	query := s.db.Where("client_id = ? AND is_latest = true AND (resource = ? OR resource LIKE ?)", clientID, tableName, tableName+":%").
		Where("COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'"). // Abaikan recovery events internal
		Order("timestamp DESC")

	if err := query.Find(&latestLogs).Error; err != nil {
		return nil, fmt.Errorf("gagal query log terbaru: %w", err)
	}

	response := &BatchVerificationResponse{
		Table:      tableName,
		VerifiedAt: time.Now().UTC(),
		Summary: map[string]int{
			"total":             len(latestLogs),
			"valid":             0,
			"tampered_offchain": 0,
			"tampered_onchain":  0,
			"pending":           0,
			"agent_error":       0,
			"fabric_error":      0,
		},
		Results: make([]RowVerificationResult, 0, len(latestLogs)),
	}

	// Cache fabric anchors untuk menghindari query berulang
	anchors := make(map[string]string)

	for _, logRow := range latestLogs {
		res := RowVerificationResult{
			Resource:      logRow.Resource,
			LogID:         logRow.LogID,
			LastAction:    logRow.Action,
			LastTimestamp: logRow.Timestamp,
		}

		if logRow.BlockchainTxID != nil {
			res.BlockchainTxID = *logRow.BlockchainTxID
		}

		// 1. Offchain Verification (Agent)
		agentResult, err := s.agent.VerifyAgainstAgent(&logRow)
		if err != nil {
			res.Status = "AGENT_ERROR"
			res.Message = "Gagal menghubungi agen klien: " + err.Error()
			response.Summary["agent_error"]++
			response.Results = append(response.Results, res)
			continue
		}

		if !agentResult.IsMatch {
			res.Status = "TAMPERED_OFFCHAIN"
			res.Message = "Metadata log tidak sesuai dengan data aktual di database klien."
			res.Discrepancies = agentResult.Discrepancies
			response.Summary["tampered_offchain"]++
			response.Results = append(response.Results, res)
			continue
		}

		// 2. Onchain Verification (Fabric)
		if logRow.Status != "ANCHORED" || logRow.BlockchainTxID == nil || strings.TrimSpace(*logRow.BlockchainTxID) == "" {
			res.Status = "PENDING"
			res.Message = "Data klien sesuai log (offchain valid), tetapi belum terjangkar di blockchain."
			response.Summary["pending"]++
			response.Results = append(response.Results, res)
			continue
		}

		anchorID := strings.TrimSpace(*logRow.BlockchainTxID)
		chainRoot, cached := anchors[anchorID]
		if !cached {
			if s.fabric == nil {
				res.Status = "FABRIC_ERROR"
				res.Message = "Koneksi ke blockchain (Fabric) tidak tersedia."
				response.Summary["fabric_error"]++
				response.Results = append(response.Results, res)
				continue
			}

			rawAnchor, err := s.fabric.GetAnchorFromLedger(anchorID)
			if err != nil {
				res.Status = "FABRIC_ERROR"
				res.Message = "Gagal membaca anchor dari blockchain: " + err.Error()
				response.Summary["fabric_error"]++
				response.Results = append(response.Results, res)
				continue
			}

			var fabricResp struct {
				MerkleRoot string `json:"merkle_root"`
			}
			if err := json.Unmarshal([]byte(rawAnchor), &fabricResp); err != nil {
				res.Status = "FABRIC_ERROR"
				res.Message = "Format anchor dari blockchain tidak valid."
				response.Summary["fabric_error"]++
				response.Results = append(response.Results, res)
				continue
			}
			chainRoot = fabricResp.MerkleRoot
			anchors[anchorID] = chainRoot
		}

		// Re-hash lokal untuk memastikan data log di gateway belum dirusak
		recalculatedHash := hasher.GenerateLogHash(&logRow)
		if recalculatedHash != logRow.HashValue {
			res.Status = "TAMPERED_OFFCHAIN"
			res.Message = "Hash lokal tidak cocok (data log di gateway telah dirusak)."
			response.Summary["tampered_offchain"]++
			response.Results = append(response.Results, res)
			continue
		}

		// Reconstruct Merkle Root
		var proofs []models.MerkleProof
		s.db.Where("transaction_hash = ?", logRow.HashValue).Order("tree_level asc").Find(&proofs)

		reconstructedRoot := logRow.HashValue
		if len(proofs) > 0 {
			proofData := make([]crypto.MerkleProofData, len(proofs))
			for i, p := range proofs {
				proofData[i] = crypto.MerkleProofData{
					SiblingHash: p.SiblingHash,
					IsLeft:      p.IsLeft,
				}
			}
			reconstructedRoot = crypto.ReconstructMerkleRoot(logRow.HashValue, proofData)
		}

		if reconstructedRoot != chainRoot {
			res.Status = "TAMPERED_ONCHAIN"
			res.Message = "Merkle Root tidak cocok dengan data anchor di blockchain."
			response.Summary["tampered_onchain"]++
			response.Results = append(response.Results, res)
			continue
		}

		res.Status = "VALID"
		res.Message = "Data klien sesuai dengan log terbaru dan terverifikasi utuh di blockchain."
		response.Summary["valid"]++
		response.Results = append(response.Results, res)
	}

	return response, nil
}
