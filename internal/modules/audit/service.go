package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/engine/kafkaconsumer"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"

	"gorm.io/gorm"
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

	// Lapis 3 Kafka
	KafkaVerified bool   `json:"kafka_verified"`
	KafkaMessage  string `json:"kafka_message,omitempty"`
	KafkaHash     string `json:"kafka_hash,omitempty"`
	KafkaTopic    string `json:"kafka_topic,omitempty"`
	KafkaOffset   int64  `json:"kafka_offset,omitempty"`
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
	VerifyLogIntegrity(logID, clientID string) (*VerificationResult, error)
	GetFabricRecord(anchorID string) (map[string]interface{}, error)
	VerifyDataIntegrity(resource, clientID string, rawData *map[string]interface{}) (*DataVerificationResult, error)
	GetRecentLogs(limit int, clientID string) ([]models.AuditLog, error)
	GetResourceInventory(clientID string) ([]models.AuditLog, error)
	VerifyResourceHistory(resource, clientID string) (*VerificationResult, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
	VerifyLogRange(from, to time.Time, clientID string) (*RangeVerificationResult, error)
}

type auditService struct {
	repo          AuditRepository
	fabric        *blockchain.FabricService
	kafkaVerifier *kafkaconsumer.KafkaVerifier
}

type RangeVerificationResult struct {
	Range   RangeInfo         `json:"range"`
	Summary RangeSummary      `json:"summary"`
	Results []RangeItemResult `json:"results"`
}

type RangeInfo struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type RangeSummary struct {
	Total   int `json:"total"`
	Valid   int `json:"valid"`
	Invalid int `json:"invalid"`
	Pending int `json:"pending"`
}

type FabricAnchorData struct {
	AnchorID      string `json:"anchor_id"`
	MerkleRoot    string `json:"merkle_root"`
	Timestamp     string `json:"timestamp"`
	BatchSize     string `json:"batch_size"`
	SourceGateway string `json:"source_gateway"`
	SignatureNode string `json:"signature_node"`
}

const pgTimestampLayout = "2006-01-02 15:04:05.000000-07"

func formatPgTimestamp(t time.Time) string {
	return t.Local().Format(pgTimestampLayout)
}

func formatFabricTimestamp(raw string) string {
	if raw == "" {
		return raw
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return formatPgTimestamp(t)
}

type RangeItemResult struct {
	LogID       string `json:"log_id"`
	Resource    string `json:"resource"`
	Action      string `json:"action"`
	Timestamp   string `json:"timestamp"`
	DBTimestamp string `json:"db_timestamp,omitempty"`

	HashValue  string `json:"hash_value"`
	RecalcHash string `json:"recalc_hash"`
	HashMatch  bool   `json:"hash_match"`

	Status       string `json:"status"`
	VerifyStatus string `json:"verify_status"`
	Message      string `json:"message"`
	ExpectedHash string `json:"expected_hash,omitempty"`
	ActualHash   string `json:"actual_hash,omitempty"`

	BlockchainTxID *string           `json:"blockchain_tx_id,omitempty"`
	MerkleRoot     string            `json:"merkle_root,omitempty"`
	Fabric         *FabricAnchorData `json:"fabric,omitempty"`
}

func (s *auditService) VerifyLogRange(from, to time.Time, clientID string) (*RangeVerificationResult, error) {
	logs, err := s.repo.GetLogsByTimeRange(from, to, clientID)
	if err != nil {
		return nil, err
	}

	result := &RangeVerificationResult{
		Range: RangeInfo{
			From: formatPgTimestamp(from),
			To:   formatPgTimestamp(to),
		},
		Results: []RangeItemResult{},
	}

	for _, auditLog := range logs {
		item := RangeItemResult{
			LogID:          auditLog.LogID,
			Resource:       auditLog.Resource,
			Action:         auditLog.Action,
			Timestamp:      formatPgTimestamp(auditLog.Timestamp),
			HashValue:      auditLog.HashValue,
			Status:         auditLog.Status,
			BlockchainTxID: auditLog.BlockchainTxID,
			MerkleRoot:     auditLog.MerkleRoot,
		}

		if auditLog.DBTimestamp != nil {
			item.DBTimestamp = formatPgTimestamp(*auditLog.DBTimestamp)
		}

		// Re-hash lokal (Lapis 2) — TESTING: tanpa prevHash
		logCopy := auditLog
		canonicalizeLog(&logCopy)
		recalcHash := hasher.GenerateLogHash(&logCopy)
		item.RecalcHash = recalcHash
		item.HashMatch = (recalcHash == auditLog.HashValue)

		verifyResult, err := s.VerifyLogIntegrity(auditLog.LogID, clientID)
		if err != nil {
			item.VerifyStatus = "error"
			item.Message = err.Error()
		} else {
			item.VerifyStatus = verifyResult.Status
			item.Message = verifyResult.Message
			if verifyResult.Status == "failed_local" {
				item.ExpectedHash = verifyResult.ExpectedHash
				item.ActualHash = verifyResult.ActualHash
			}
		}

		if auditLog.BlockchainTxID != nil &&
			*auditLog.BlockchainTxID != "" &&
			*auditLog.BlockchainTxID != "PENDING_OR_FAILED" &&
			s.fabric != nil {
			fabricData, ferr := s.fetchFabricAnchor(*auditLog.BlockchainTxID)
			if ferr != nil {
				log.Printf("⚠️  [VerifyRange] Gagal fetch Fabric TxID=%s: %v", *auditLog.BlockchainTxID, ferr)
			} else {
				item.Fabric = fabricData
			}
		}

		switch item.VerifyStatus {
		case "success":
			result.Summary.Valid++
		case "pending":
			result.Summary.Pending++
		default:
			result.Summary.Invalid++
		}
		result.Summary.Total++
		result.Results = append(result.Results, item)
	}

	return result, nil
}

func (s *auditService) fetchFabricAnchor(txID string) (*FabricAnchorData, error) {
	raw, err := s.fabric.GetAnchorFromLedger(txID)
	if err != nil {
		return nil, err
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("gagal parse fabric response: %w", err)
	}

	anchor := &FabricAnchorData{}
	if v, ok := parsed["anchor_id"].(string); ok {
		anchor.AnchorID = v
	}
	if v, ok := parsed["merkle_root"].(string); ok {
		anchor.MerkleRoot = v
	}
	if v, ok := parsed["timestamp"].(string); ok {
		anchor.Timestamp = formatFabricTimestamp(v)
	}
	if v, ok := parsed["batch_size"].(string); ok {
		anchor.BatchSize = v
	}
	if v, ok := parsed["source_gateway"].(string); ok {
		anchor.SourceGateway = v
	}
	if v, ok := parsed["signature_node"].(string); ok {
		anchor.SignatureNode = v
	}

	return anchor, nil
}

func NewService(repo AuditRepository, fabric *blockchain.FabricService, db *gorm.DB) Service {
	return &auditService{
		repo:          repo,
		fabric:        fabric,
		kafkaVerifier: &kafkaconsumer.KafkaVerifier{DB: db},
	}
}

func (s *auditService) GetDashboardStats(clientID string) (map[string]int64, error) {
	return s.repo.GetDashboardStats(clientID)
}

// canonicalizeLog memastikan field-field AuditLog dalam format yang identik
// dengan saat log pertama kali di-hash di kafkaconsumer/consumer.go.
func canonicalizeLog(auditLog *models.AuditLog) {
	if auditLog.Metadata != "" && auditLog.Metadata != "null" {
		var metaMap interface{}
		if err := json.Unmarshal([]byte(auditLog.Metadata), &metaMap); err == nil {
			canonicalBytes, err := json.Marshal(metaMap)
			if err == nil {
				auditLog.Metadata = string(canonicalBytes)
			}
		}
	}

	if auditLog.AuthorizationContext == "null" ||
		auditLog.AuthorizationContext == "<nil>" {
		auditLog.AuthorizationContext = ""
	}
}

// VerifyLogIntegrity menerima log_id (bukan hash) sebagai identifier.
func (s *auditService) VerifyLogIntegrity(logID, clientID string) (*VerificationResult, error) {
	// === LAPIS 1: Cek keberadaan di DB by log_id ===
	auditLog, err := s.repo.GetLogByID(logID, clientID)
	if err != nil {
		return nil, errors.New("log_not_found")
	}

	// === LAPIS 2: Re-Hash Lokal (TESTING: tanpa prevHash) ===
	canonicalizeLog(auditLog)
	recalculatedHash := hasher.GenerateLogHash(auditLog)
	if recalculatedHash != auditLog.HashValue {
		return &VerificationResult{
			Status:       "failed_local",
			Message:      "🚨 DATA TERMANIPULASI: Isi data telah diubah di database middleware.",
			IsValid:      false,
			LogID:        auditLog.LogID,
			ExpectedHash: auditLog.HashValue,
			ActualHash:   recalculatedHash,
		}, nil
	}

	// === LAPIS 3: Verifikasi ke Kafka ===
	var kafkaVerified bool
	var kafkaMsg, kafkaHash, kafkaTopic string
	var kafkaOffset int64

	kafkaResult, err := s.kafkaVerifier.VerifyAgainstKafka(auditLog)
	if err != nil {
		log.Printf("⚠️  [Verify] Kafka verify error: %v", err)
		kafkaMsg = "Kafka tidak dapat diverifikasi: " + err.Error()
	} else {
		kafkaVerified = kafkaResult.IsValid
		kafkaMsg = kafkaResult.Message
		kafkaHash = kafkaResult.KafkaHash
		kafkaTopic = kafkaResult.Topic
		kafkaOffset = kafkaResult.Offset

		if !kafkaResult.IsValid {
			return &VerificationResult{
				Status:        "failed_kafka",
				Message:       kafkaResult.Message,
				IsValid:       false,
				LogID:         auditLog.LogID,
				KafkaVerified: false,
				KafkaMessage:  kafkaMsg,
				KafkaHash:     kafkaHash,
				KafkaTopic:    kafkaTopic,
				KafkaOffset:   kafkaOffset,
			}, nil
		}
	}

	// === Pending check ===
	if auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "PENDING_OR_FAILED" {
		return &VerificationResult{
			Status:        "pending",
			Message:       "Log otentik secara lokal dan terverifikasi di Kafka. Menunggu antrean Blockchain.",
			IsValid:       true,
			LogID:         auditLog.LogID,
			KafkaVerified: kafkaVerified,
			KafkaMessage:  kafkaMsg,
			KafkaTopic:    kafkaTopic,
			KafkaOffset:   kafkaOffset,
		}, nil
	}

	// === LAPIS 4: Konsensus Blockchain ===
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
			Status:        "failed_onchain",
			Message:       "🚨 FATAL MISMATCH: Merkle Root tidak diakui oleh jaringan Blockchain!",
			IsValid:       false,
			LogID:         auditLog.LogID,
			DBRoot:        auditLog.MerkleRoot,
			ChainRoot:     fabricResponse.MerkleRoot,
			KafkaVerified: kafkaVerified,
			KafkaMessage:  kafkaMsg,
		}, nil
	}

	return &VerificationResult{
		Status:        "success",
		Message:       "✅ DATA OTENTIK: Terverifikasi di Kafka dan Blockchain.",
		IsValid:       true,
		LogID:         auditLog.LogID,
		ExpectedHash:  auditLog.HashValue,
		DBRoot:        auditLog.MerkleRoot,
		TxID:          auditLog.BlockchainTxID,
		KafkaVerified: kafkaVerified,
		KafkaMessage:  kafkaMsg,
		KafkaHash:     kafkaHash,
		KafkaTopic:    kafkaTopic,
		KafkaOffset:   kafkaOffset,
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
			isValid, status = true, "success"
			msg = "✅ DATA VALID: Log terakhir adalah DELETE, dan data aktual memang kosong."
		} else {
			msg = "🔴 DATA TERMANIPULASI (GHOST DATA): Log DELETE ditemukan tapi data masih ada!"
		}
	} else {
		if isDataEmpty {
			msg = "🔴 DATA TERMANIPULASI (ILLEGAL DELETION): Log terakhir bukan DELETE, tapi data HILANG!"
		} else if actualHash == expectedHash {
			isValid, status = true, "success"
			msg = "✅ DATA VALID: Kondisi data aktual sama persis dengan jejak terakhir."
		} else {
			msg = "🔴 DATA TERMANIPULASI (UNAUTHORIZED MODIFICATION): Isi data berbeda dari jejak sah."
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

// VerifyResourceHistory memverifikasi seluruh riwayat satu resource.
//
// TESTING: validasi local chain ("Previous Hash tidak cocok") DIHAPUS —
// tidak relevan lagi karena setiap log kini berdiri sendiri, tidak lagi
// terikat pada hash log sebelumnya (previousHash tidak ada di model).
func (s *auditService) VerifyResourceHistory(resource, clientID string) (*VerificationResult, error) {
	logs, err := s.repo.GetLogsByResource(resource, clientID)
	if err != nil || len(logs) == 0 {
		return nil, errors.New("log_not_found")
	}

	hasPending := false
	var lastValidResult *VerificationResult

	for _, log := range logs {
		res, err := s.VerifyLogIntegrity(log.LogID, clientID)
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
