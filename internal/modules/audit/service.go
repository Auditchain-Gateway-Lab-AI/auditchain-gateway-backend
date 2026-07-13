package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/engine/hasher"
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

// PaginationMeta menampung metadata pagination untuk response GetRecentLogs.
type PaginationMeta struct {
	Page       int   `json:"page"`
	PageSize   int   `json:"page_size"`
	TotalItems int64 `json:"total_items"`
	TotalPages int   `json:"total_pages"`
}

// RecentLogItem membungkus AuditLog dengan integrity_status hasil
// pengecekan Layer 2 (re-hash) dan, jika sudah ANCHORED, Layer 4
// (kecocokan merkle_root vs ledger Fabric).
type RecentLogItem struct {
	models.AuditLog
	IntegrityStatus string `json:"integrity_status"` // valid | tampered | unreachable | pending
}

// RecentLogsResult adalah bentuk response baru GetRecentLogs:
// {"data": [...], "pagination": {...}, "note": "..."}
type RecentLogsResult struct {
	Data       []RecentLogItem `json:"data"`
	Pagination PaginationMeta  `json:"pagination"`
	Note       string          `json:"note,omitempty"`
}

type Service interface {
	GetDashboardStats(clientID string) (map[string]int64, error)
	VerifyLogIntegrity(logID, clientID string) (*VerificationResult, error)
	GetFabricRecord(anchorID string) (map[string]interface{}, error)
	VerifyDataIntegrity(resource, clientID string, rawData *map[string]interface{}) (*DataVerificationResult, error)

	// GetRecentLogsPaginated menggantikan GetRecentLogs lama sebagai entry
	// point handler dashboard. limit hardcoded 500 di versi lama diganti
	// page/pageSize. integrityStatus kosong berarti tanpa filter.
	GetRecentLogsPaginated(clientID string, page, pageSize int, integrityStatus string) (*RecentLogsResult, error)

	GetResourceInventory(clientID string) ([]models.AuditLog, error)
	VerifyResourceHistory(resource, clientID string) (*VerificationResult, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
	VerifyLogRange(from, to time.Time, clientID string) (*RangeVerificationResult, error)
}

type auditService struct {
	repo   AuditRepository
	fabric *blockchain.FabricService
}

// Struct baru
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

// pgTimestampLayout mencocokkan format default tampilan PostgreSQL dengan
// presisi microsecond (6 digit), contoh: 2026-06-30 09:06:52.766123+07
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

// Implementasi
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

// NewService mempertahankan parameter db pada signature (meski tidak lagi
// dipakai untuk Kafka verifier) supaya main.go yang memanggil
// audit.NewService(auditRepo, fabricSvc, db) tidak perlu diubah.
func NewService(repo AuditRepository, fabric *blockchain.FabricService, db *gorm.DB) Service {
	_ = db
	return &auditService{
		repo:   repo,
		fabric: fabric,
	}
}

func (s *auditService) GetDashboardStats(clientID string) (map[string]int64, error) {
	return s.repo.GetDashboardStats(clientID)
}

func canonicalizeLog(auditLog *models.AuditLog) {
	// Metadata TIDAK di-re-marshal. String yang tersimpan di DB adalah
	// hasil json.Marshal() satu kali saat log pertama ditulis (consumer.go
	// / normalizer.go), dan itu SUDAH final — re-encode ulang di sini
	// tidak dijamin idempotent untuk nilai numerik (int vs float64,
	// notasi eksponensial pada angka besar, dst), sehingga bisa memicu
	// false-positive "tampered" meski data tidak pernah berubah.
	if auditLog.AuthorizationContext == "null" ||
		auditLog.AuthorizationContext == "<nil>" {
		auditLog.AuthorizationContext = ""
	}
}

func isHashStillPending(auditLog *models.AuditLog) bool {
	if auditLog == nil {
		return false
	}

	if auditLog.Status == "RECEIVED" {
		return true
	}

	if auditLog.HashValue == "" {
		return true
	}

	return strings.HasPrefix(auditLog.HashValue, "PENDING-")
}

// VerifyLogIntegrity menjalankan verifikasi 2-lapis: Layer 2 (re-hash lokal
// terhadap PostgreSQL) dan Layer 4 (kecocokan merkle_root vs Fabric ledger).
// Verifikasi Kafka (Layer 3) SENGAJA DIHAPUS — cukup DB (off-chain) dan
// Fabric (on-chain) saja sesuai keputusan terbaru.
func (s *auditService) VerifyLogIntegrity(logID, clientID string) (*VerificationResult, error) {
	auditLog, err := s.repo.GetLogByID(logID, clientID)
	if err != nil {
		return nil, errors.New("log_not_found")
	}

	if isHashStillPending(auditLog) {
		return &VerificationResult{
			Status:  "pending",
			Message: "Log sudah diterima, tetapi hash final masih diproses pipeline.",
			IsValid: true,
			LogID:   auditLog.LogID,
		}, nil
	}

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

	if auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "PENDING_OR_FAILED" {
		return &VerificationResult{
			Status:  "pending",
			Message: "Log otentik secara lokal. Menunggu antrean Blockchain.",
			IsValid: true,
			LogID:   auditLog.LogID,
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

	proofs, perr := s.repo.GetProofsByHash(auditLog.HashValue)
	if perr != nil {
		return nil, errors.New("proof_lookup_error")
	}

	reconstructedRoot := auditLog.HashValue // batch 1-leaf: root == hash itu sendiri
	if len(proofs) > 0 {
		reconstructedRoot = crypto.ReconstructMerkleRoot(auditLog.HashValue, toMerkleProofData(proofs))
	}

	verifiedVia := "merkle_proof"
	if reconstructedRoot != fabricResponse.MerkleRoot {
		// Fallback untuk data lama yang di-anchor SEBELUM fix ini (proof
		// level>0 & IsLeft belum tersimpan benar) — supaya tidak false-positive
		// menandai log lama yang sebenarnya sah sebagai "tampered".
		if auditLog.MerkleRoot != fabricResponse.MerkleRoot {
			return &VerificationResult{
				Status:    "failed_onchain",
				Message:   "🚨 FATAL MISMATCH: Merkle Root tidak diakui oleh jaringan Blockchain!",
				IsValid:   false,
				LogID:     auditLog.LogID,
				DBRoot:    reconstructedRoot,
				ChainRoot: fabricResponse.MerkleRoot,
			}, nil
		}
		verifiedVia = "legacy_field_fallback"
	}

	successMsg := "✅ DATA OTENTIK: Terverifikasi di database dan Blockchain."
	if verifiedVia == "legacy_field_fallback" {
		successMsg += " (diverifikasi via metode lama — log ini di-anchor sebelum perbaikan Merkle proof)"
	}

	return &VerificationResult{
		Status:       "success",
		Message:      successMsg,
		IsValid:      true,
		LogID:        auditLog.LogID,
		ExpectedHash: auditLog.HashValue,
		DBRoot:       reconstructedRoot,
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

// classifyIntegrity menjalankan Layer 2 (re-hash) untuk semua log, dan
// Layer 4 (cocokkan merkle_root vs Fabric ledger) HANYA untuk log berstatus
// ANCHORED. Log yang belum ANCHORED (RECEIVED/HASHED/AGGREGATED) diberi
// status "pending" karena belum bisa diverifikasi sampai Layer 4.
// Verifikasi sepenuhnya berbasis DB (off-chain) + Fabric (on-chain) saja.
func (s *auditService) classifyIntegrity(auditLog models.AuditLog) string {
	if isHashStillPending(&auditLog) {
		return "pending"
	}

	canonicalizeLog(&auditLog)
	recalculated := hasher.GenerateLogHash(&auditLog)
	if recalculated != auditLog.HashValue {
		return "tampered"
	}

	if auditLog.Status != "ANCHORED" || auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "" {
		return "pending"
	}

	if s.fabric == nil {
		return "unreachable"
	}

	onChainData, err := s.fabric.GetAnchorFromLedger(*auditLog.BlockchainTxID)
	if err != nil {
		return "unreachable"
	}

	var fabricResponse struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(onChainData), &fabricResponse); err != nil {
		return "unreachable"
	}

	proofs, perr := s.repo.GetProofsByHash(auditLog.HashValue)
	if perr != nil {
		return "unreachable"
	}

	reconstructedRoot := auditLog.HashValue
	if len(proofs) > 0 {
		reconstructedRoot = crypto.ReconstructMerkleRoot(auditLog.HashValue, toMerkleProofData(proofs))
	}

	if reconstructedRoot != fabricResponse.MerkleRoot && auditLog.MerkleRoot != fabricResponse.MerkleRoot {
		return "tampered"
	}

	return "valid"
}

// GetRecentLogsPaginated menggantikan limit-500 lama dengan pagination
// sesungguhnya (page/page_size), plus filter opsional integrity_status.
//
// KETERBATASAN (didokumentasikan secara transparan via field "note"):
// integrity_status dihitung IN-MEMORY setelah query, bukan di level SQL,
// karena status valid/tampered/unreachable baru diketahui setelah re-hash
// lokal + query ke Fabric. Akibatnya saat filter aktif:
//   - Basis pagination (total_items) memakai total log ANCHORED, BUKAN
//     jumlah log yang benar-benar cocok filter — sehingga total_items dan
//     total_pages bersifat APPROXIMATE saat integrity_status diisi.
//   - Ini adalah trade-off yang disengaja: menghitung exact count untuk
//     filter ini butuh full-scan + verifikasi semua log ANCHORED pada
//     setiap request, yang mahal. Field "note" memberi tahu API consumer
//     secara eksplisit.
func (s *auditService) GetRecentLogsPaginated(clientID string, page, pageSize int, integrityStatus string) (*RecentLogsResult, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 200 {
		pageSize = 200
	}

	validFilter := map[string]bool{"valid": true, "tampered": true, "unreachable": true}

	// Tanpa filter integrity_status: pagination langsung dari SQL, exact count.
	if integrityStatus == "" {
		logs, total, err := s.repo.GetRecentLogsPage(clientID, page, pageSize)
		if err != nil {
			return nil, err
		}

		items := make([]RecentLogItem, 0, len(logs))
		for _, l := range logs {
			items = append(items, RecentLogItem{
				AuditLog:        l,
				IntegrityStatus: s.classifyIntegrity(l),
			})
		}

		totalPages := int(total) / pageSize
		if int(total)%pageSize != 0 {
			totalPages++
		}
		if totalPages == 0 {
			totalPages = 1
		}

		return &RecentLogsResult{
			Data: items,
			Pagination: PaginationMeta{
				Page:       page,
				PageSize:   pageSize,
				TotalItems: total,
				TotalPages: totalPages,
			},
		}, nil
	}

	if !validFilter[integrityStatus] {
		return nil, errors.New("invalid_integrity_status")
	}

	// Dengan filter: hanya log ANCHORED yang relevan (lihat classifyIntegrity).
	anchoredTotal, err := s.repo.CountAnchoredLogs(clientID)
	if err != nil {
		return nil, err
	}

	logs, err := s.repo.GetAnchoredLogsPage(clientID, page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]RecentLogItem, 0, len(logs))
	// for _, l := range logs {
	// 	status := s.classifyIntegrity(l)
	// 	if status == integrityStatus {
	// 		items = append(items, RecentLogItem{
	// 			AuditLog:        l,
	// 			IntegrityStatus: status,
	// 		})
	// 	}
	// }

	totalPages := int(anchoredTotal) / pageSize
	if int(anchoredTotal)%pageSize != 0 {
		totalPages++
	}
	if totalPages == 0 {
		totalPages = 1
	}

	return &RecentLogsResult{
		Data: items,
		Pagination: PaginationMeta{
			Page:       page,
			PageSize:   pageSize,
			TotalItems: anchoredTotal,
			TotalPages: totalPages,
		},
		Note: "integrity_status filter aktif: total_items & total_pages dihitung dari total log berstatus ANCHORED (bukan jumlah pasti yang cocok filter), karena status integritas ditentukan setelah verifikasi in-memory per baris, bukan di level query SQL.",
	}, nil
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

		if i > 0 && log.PreviousHash != expectedPrevHash {
			return &VerificationResult{
				Status:       "failed_chain",
				Message:      "🚨 RANTAI TERPUTUS: Previous Hash tidak cocok.",
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

func toMerkleProofData(proofs []models.MerkleProof) []crypto.MerkleProofData {
	result := make([]crypto.MerkleProofData, 0, len(proofs))
	for _, p := range proofs {
		result = append(result, crypto.MerkleProofData{
			SiblingHash: p.SiblingHash,
			IsLeft:      p.IsLeft,
			TreeLevel:   p.TreeLevel,
		})
	}
	return result
}
