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
	// 1. Ambil data full dari Agent (Ini menyelesaikan masalah row yang di-insert tanpa lewat Kafka)
	agentRecords, err := s.agent.FetchTableData(clientID, tableName)
	if err != nil {
		return nil, fmt.Errorf("gagal memuat data dari Agent: %w", err)
	}

	// 2. Ambil log terbaru dari Gateway
	var latestLogs []models.AuditLog
	resourcePrefix := tableName + ":"
	query := s.db.Where("client_id = ? AND is_latest = true AND (resource = ? OR resource LIKE ?)", clientID, tableName, resourcePrefix+"%").
		Where("COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'").
		Order("timestamp DESC")

	if err := query.Find(&latestLogs).Error; err != nil {
		return nil, fmt.Errorf("gagal query log terbaru: %w", err)
	}

	gatewayLogsMap := make(map[string]models.AuditLog)
	for _, log := range latestLogs {
		gatewayLogsMap[log.Resource] = log
	}

	// Some gateway logs might not be in agent (deleted), so total could be bigger
	// We will calculate total dynamically
	
	response := &BatchVerificationResponse{
		Table:      tableName,
		VerifiedAt: time.Now().UTC(),
		Summary: map[string]int{
			"total":            0,
			"valid":            0,
			"tampered_onchain": 0,
			"pending":          0,
			"agent_error":      0,
			"fabric_error":     0,
		},
		Results: make([]RowVerificationResult, 0),
	}

	anchors := make(map[string]string)

	// Verifikasi setiap record dari Agent
	for _, agentRec := range agentRecords {
		resourceID := tableName + ":" + agentRec.ID
		logRow, exists := gatewayLogsMap[resourceID]

		res := RowVerificationResult{
			Resource: resourceID,
		}

		if !exists {
			// Data ada di klien tapi tidak ada di blockchain (Tampered)
			res.Status = "TAMPERED_ONCHAIN"
			res.Message = "Data ada di klien tapi tidak ada/hilang dari rekam jejak blockchain."
			response.Summary["tampered_onchain"]++
			response.Results = append(response.Results, res)
			continue
		}

		// Hapus dari map untuk melacak mana yang sudah dihapus di klien
		delete(gatewayLogsMap, resourceID)

		res.LogID = logRow.LogID
		res.LastAction = logRow.Action
		res.LastTimestamp = logRow.Timestamp

		if logRow.BlockchainTxID != nil {
			res.BlockchainTxID = *logRow.BlockchainTxID
		}

		// Bandingkan data dari agent dengan metadata log
		diffs := s.agent.CompareResourceData(&logRow, &agentRec)
		isMatch := len(diffs) == 0

		// Onchain Verification
		if logRow.Status != "ANCHORED" || logRow.BlockchainTxID == nil || strings.TrimSpace(*logRow.BlockchainTxID) == "" {
			res.Status = "PENDING"
			res.Message = "Data menunggu antrean anchoring ke blockchain."
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

		clientHash := logRow.HashValue
		if !isMatch {
			clientHash = "tampered_client_data"
			res.Discrepancies = diffs
		}
		recalculatedHash := hasher.GenerateLogHash(&logRow)
		if recalculatedHash != logRow.HashValue {
			clientHash = "tampered_gateway_log"
		}

		var proofs []models.MerkleProof
		s.db.Where("transaction_hash = ?", logRow.HashValue).Order("tree_level asc").Find(&proofs)

		reconstructedRoot := clientHash
		if len(proofs) > 0 {
			proofData := make([]crypto.MerkleProofData, len(proofs))
			for i, p := range proofs {
				proofData[i] = crypto.MerkleProofData{
					SiblingHash: p.SiblingHash,
					IsLeft:      p.IsLeft,
				}
			}
			reconstructedRoot = crypto.ReconstructMerkleRoot(clientHash, proofData)
		}

		if reconstructedRoot != chainRoot {
			res.Status = "TAMPERED_ONCHAIN"
			res.Message = "Data klien tidak cocok dengan state yang terverifikasi di blockchain."
			if !isMatch {
				res.Discrepancies = diffs
			}
			response.Summary["tampered_onchain"]++
			response.Results = append(response.Results, res)
			continue
		}

		res.Status = "VALID"
		res.Message = "Data klien sesuai dengan log terbaru dan terverifikasi utuh di blockchain."
		response.Summary["valid"]++
		response.Results = append(response.Results, res)
	}

	// Sisanya di gatewayLogsMap berarti dihapus tanpa log (Tampered)
	for resourceID, logRow := range gatewayLogsMap {
		var res RowVerificationResult
		res.Resource = resourceID
		res.LogID = logRow.LogID
		res.LastAction = logRow.Action
		res.LastTimestamp = logRow.Timestamp
		if logRow.BlockchainTxID != nil {
			res.BlockchainTxID = *logRow.BlockchainTxID
		}
		res.Status = "TAMPERED_ONCHAIN"
		res.Message = "Data tercatat di blockchain tapi hilang/dihapus dari database klien tanpa audit."
		response.Summary["tampered_onchain"]++
		response.Results = append(response.Results, res)
	}

	response.Summary["total"] = len(response.Results)
	// Update Verify Stats
	if len(response.Results) > 0 {
		var stat models.ClientDashboardStats
		if err := s.db.Where("client_id = ?", clientID).First(&stat).Error; err == nil {
			var tableResults map[string]interface{}
			json.Unmarshal([]byte(stat.TableVerifyResults), &tableResults)
			if tableResults == nil {
				tableResults = make(map[string]interface{})
			}
			tableResults[tableName] = response.Summary
			newJSON, _ := json.Marshal(tableResults)

			totalValid := int64(response.Summary["valid"])
			totalTampered := int64(response.Summary["tampered_onchain"] + response.Summary["tampered_offchain"])
			totalPending := int64(response.Summary["pending"])
			totalAgentError := int64(response.Summary["agent_error"])
			totalFabricError := int64(response.Summary["fabric_error"])
			totalVerified := totalValid + totalTampered + totalPending + totalAgentError + totalFabricError

			var pct float64
			newTotalVerifications := stat.TotalVerifications + totalVerified
			newTotalValid := stat.TotalValid + totalValid
			if newTotalVerifications > 0 {
				pct = float64(newTotalValid) / float64(newTotalVerifications) * 100
			}

			now := time.Now()
			s.db.Model(&stat).Updates(map[string]interface{}{
				"last_verified_at":     now,
				"last_verified_table":  tableName,
				"total_verifications":  newTotalVerifications,
				"total_rows_verified":  gorm.Expr("total_rows_verified + ?", totalVerified), // or keep track uniquely
				"total_valid":          newTotalValid,
				"total_tampered":       stat.TotalTampered + totalTampered,
				"total_verify_pending": stat.TotalVerifyPending + totalPending,
				"total_agent_error":    stat.TotalAgentError + totalAgentError,
				"total_fabric_error":   stat.TotalFabricError + totalFabricError,
				"integrity_score":      pct,
				"table_verify_results": string(newJSON),
			})
		}
	}

	return response, nil
}

