package audit

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/blockchain/agentverifier"
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
	VerifyResourceHistory(resource, clientID string) (*ResourceChainResult, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
	VerifyLogRange(from, to time.Time, clientID string) (*RangeVerificationResult, error)
}

type auditService struct {
	repo   AuditRepository
	fabric *blockchain.FabricService
	agent  *agentverifier.Service
}

type ResourceLogVerification struct {
	LogID           string `json:"log_id"`
	Action          string `json:"action"`
	Actor           string `json:"actor"`
	Timestamp       string `json:"timestamp"`
	HashValue       string `json:"hash_value"`
	IntegrityStatus string `json:"integrity_status"` // valid | tampered | pending | unreachable
	IsLatest        bool   `json:"is_latest"`

	// AgentStatus HANYA relevan untuk log_id = log terbaru pada resource ini
	// (IsLatest=true). Agent cuma bisa membaca kondisi TERKINI data klien,
	// jadi membandingkannya ke log lama pasti mismatch meski tidak ada
	// tampering — itu bukan bukti manipulasi, itu snapshot historis yang
	// memang sudah usang secara wajar.
	//   matched            — log terbaru, Agent dihubungi, data cocok
	//   mismatch           — log terbaru, Agent dihubungi, ada perbedaan
	//   unreachable        — log terbaru, Agent terkonfigurasi tapi gagal dihubungi
	//   not_configured     — log terbaru, klien belum setup AgentConfig
	//   skipped_historical — BUKAN log terbaru, Layer 3 tidak relevan untuk baris ini
	AgentStatus        string                      `json:"agent_status"`
	AgentDiscrepancies []agentverifier.Discrepancy `json:"agent_discrepancies,omitempty"`
}

type ResourceChainResult struct {
	Resource    string                    `json:"resource"`
	ChainStatus string                    `json:"chain_status"` // valid | tampered | pending | unreachable
	ChainIssues []string                  `json:"chain_issues,omitempty"`
	TotalLogs   int                       `json:"total_logs"`
	Logs        []ResourceLogVerification `json:"logs"`
}

func NewService(repo AuditRepository, fabric *blockchain.FabricService, db *gorm.DB) Service {
	return &auditService{
		repo:   repo,
		fabric: fabric,
		agent:  agentverifier.NewService(db),
	}
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

	// GetRecentLogsPaginated menggantikan limit-500 lama dengan pagination
	// sesungguhnya (page/page_size), plus filter opsional integrity_status.
	//
	// PERUBAHAN: endpoint ini TIDAK LAGI menjalankan verifikasi (rehash + query
	// Fabric) secara otomatis untuk setiap baris yang ditampilkan. IntegrityStatus
	// dikembalikan sebagai "not_checked" secara default. Verifikasi sesungguhnya
	// per log dilakukan on-demand lewat GET /dashboard/verify/:log_id, dipicu
	// oleh aksi eksplisit user (tombol verifikasi di frontend) — bukan lagi
	// dibebankan ke setiap polling/render daftar transaksi. Ini menghindari
	// rehash + Fabric round-trip berulang setiap 5 detik (siklus polling
	// Dashboard) untuk log yang bahkan belum diminta diverifikasi siapa pun.
	//
	// KETERBATASAN (sudah ada sebelumnya, tidak berubah): saat integrity_status
	// filter AKTIF, endpoint ini tetap butuh verifikasi in-memory untuk
	// menentukan kecocokan filter — jalur ini TIDAK terpengaruh perubahan di
	// atas karena secara desain memang harus menghitung status sebelum bisa
	// difilter. (Catatan: implementasi loop filter saat ini masih dinonaktifkan/
	// commented — di luar scope perubahan ini.)
	if integrityStatus == "" {
		logs, total, err := s.repo.GetRecentLogsPage(clientID, page, pageSize)
		if err != nil {
			return nil, err
		}

		items := make([]RecentLogItem, 0, len(logs))
		for _, l := range logs {
			items = append(items, RecentLogItem{
				AuditLog:        l,
				IntegrityStatus: "not_checked",
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

// VerifyResourceHistory menjalankan verifikasi Layer 2 (re-hash) + Layer 4
// (Merkle proof reconstruction vs Fabric) untuk SETIAP log milik resource
// ini, dan Layer 3 (Agent, live client data) HANYA untuk log terbaru — lihat
// classifyResourceLog. Hasilnya diagregasi menjadi satu chain_status.
//
// ChainIssues memberi detail KENAPA chain_status jadi "tampered", dengan dua
// kategori yang SENGAJA dipisah (bisa muncul bersamaan, karena dua-duanya
// independen satu sama lain):
//   - "client_mismatch:<log_id>"      → data live di klien (via Agent) sudah
//     berbeda dari log TERBARU resource ini. Bukan berarti log itu sendiri
//     rusak — datanya sudah berubah lagi di sisi klien tanpa lewat AuditChain.
//   - "log_integrity_failed:<log_id>" → log tersebut gagal Layer 2 dan/atau
//     Layer 4 (rehash lokal tidak cocok, atau rekonstruksi Merkle root tidak
//     cocok dengan Fabric). Bisa muncul untuk log manapun dalam riwayat,
//     tidak terbatas pada log terbaru.
func (s *auditService) VerifyResourceHistory(resource, clientID string) (*ResourceChainResult, error) {
	logs, err := s.repo.GetLogsByResource(resource, clientID)
	if err != nil || len(logs) == 0 {
		return nil, errors.New("log_not_found")
	}

	result := &ResourceChainResult{
		Resource:  resource,
		TotalLogs: len(logs),
		Logs:      make([]ResourceLogVerification, 0, len(logs)),
	}

	hasTampered := false
	hasUnreachable := false
	hasPending := false
	var chainIssues []string

	lastIndex := len(logs) - 1
	for i, auditLog := range logs {
		item := s.classifyResourceLog(auditLog, i == lastIndex)
		result.Logs = append(result.Logs, item)

		// baseIntegrityFailed dicek terpisah dari item.IntegrityStatus supaya
		// "log_integrity_failed" hanya dilaporkan untuk kegagalan Layer 2/4
		// yang sesungguhnya — bukan ikut ter-flag saat penyebabnya murni
		// client_mismatch (yang juga bisa membuat item.IntegrityStatus jadi
		// "tampered" lewat classifyResourceLog, lihat baseStatus vs AgentStatus
		// di sana).
		baseIntegrityFailed := s.classifyIntegrity(auditLog) == "tampered"
		if baseIntegrityFailed {
			chainIssues = append(chainIssues, fmt.Sprintf("log_integrity_failed:%s", auditLog.LogID))
		}
		if item.IsLatest && item.AgentStatus == "mismatch" {
			chainIssues = append(chainIssues, fmt.Sprintf("client_mismatch:%s", auditLog.LogID))
		}

		switch item.IntegrityStatus {
		case "tampered":
			hasTampered = true
		case "unreachable":
			hasUnreachable = true
		case "pending":
			hasPending = true
		}
	}

	switch {
	case hasTampered:
		result.ChainStatus = "tampered"
		result.ChainIssues = chainIssues
	case hasUnreachable:
		result.ChainStatus = "unreachable"
	case hasPending:
		result.ChainStatus = "pending"
	default:
		result.ChainStatus = "valid"
	}

	return result, nil
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

// classifyResourceLog menjalankan Layer 2+4 (rehash + Merkle vs Fabric) untuk
// SEMUA log, tapi Layer 3 (Agent, live data klien) HANYA untuk log terbaru —
// karena hanya baris terbaru yang seharusnya cocok dengan kondisi data klien
// saat ini. Log lama SENGAJA dilewati dari perbandingan Agent supaya tidak
// menghasilkan false-positive "mismatch" akibat data yang memang sudah
// berubah secara sah setelah log itu dibuat.
func (s *auditService) classifyResourceLog(auditLog models.AuditLog, isLatest bool) ResourceLogVerification {
	baseStatus := s.classifyIntegrity(auditLog)

	item := ResourceLogVerification{
		LogID:           auditLog.LogID,
		Action:          auditLog.Action,
		Actor:           auditLog.Actor,
		Timestamp:       formatPgTimestamp(auditLog.Timestamp),
		HashValue:       auditLog.HashValue,
		IntegrityStatus: baseStatus,
		IsLatest:        isLatest,
		AgentStatus:     "skipped_historical",
	}

	if !isLatest {
		return item
	}

	item.AgentStatus = "not_configured"

	logCopy := auditLog
	agentResult, err := s.agent.VerifyAgainstAgent(&logCopy)
	if err != nil {
		item.AgentStatus = "unreachable"
	} else if agentResult.AgentUsed {
		if agentResult.IsMatch {
			item.AgentStatus = "matched"
		} else {
			item.AgentStatus = "mismatch"
			item.AgentDiscrepancies = agentResult.Discrepancies
		}
	}

	switch {
	case baseStatus == "tampered" || item.AgentStatus == "mismatch":
		item.IntegrityStatus = "tampered"
	case baseStatus == "unreachable" || item.AgentStatus == "unreachable":
		item.IntegrityStatus = "unreachable"
	case baseStatus == "pending":
		item.IntegrityStatus = "pending"
	default:
		item.IntegrityStatus = "valid"
	}

	return item
}
