package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/engine/tamperscanner"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
)

type VerificationResult struct {
	Status             string                      `json:"status"`
	Assessment         string                      `json:"verification_assessment,omitempty"`
	ProofStatus        string                      `json:"proof_status,omitempty"`
	Message            string                      `json:"message"`
	IsValid            bool                        `json:"is_valid"`
	ExpectedHash       string                      `json:"expected_hash"`
	ActualHash         string                      `json:"actual_hash"`
	DBRoot             string                      `json:"db_root"`
	ChainRoot          string                      `json:"chain_root"`
	LogID              string                      `json:"log_id"`
	IncidentID         string                      `json:"incident_id,omitempty"`
	IncidentScope      string                      `json:"incident_scope,omitempty"`
	IncidentType       string                      `json:"incident_type,omitempty"`
	IncidentStatus     string                      `json:"incident_status,omitempty"`
	TxID               *string                     `json:"blockchain_tx_id,omitempty"`
	AgentStatus        string                      `json:"agent_status,omitempty"`
	SourceStatus       string                      `json:"source_status,omitempty"`
	StateComparison    string                      `json:"state_comparison,omitempty"`
	ComparisonBasis    string                      `json:"comparison_basis,omitempty"`
	OffchainStateHash  string                      `json:"offchain_state_hash,omitempty"`
	ClientStateHash    string                      `json:"client_state_hash,omitempty"`
	SourceCheckedAt    *time.Time                  `json:"source_checked_at,omitempty"`
	AuditTrailStatus   string                      `json:"audit_trail_status,omitempty"`
	AuditTrailDiffs    []agentverifier.Discrepancy `json:"audit_trail_discrepancies,omitempty"`
	SourceStateHash    string                      `json:"-"`
	AgentDiscrepancies []agentverifier.Discrepancy `json:"agent_discrepancies,omitempty"`
}

var errIncidentPersistence = errors.New("recovery incident persistence failed")

type IncidentPersistenceError struct {
	LogID         string
	IncidentType  string
	IncidentScope string
	Cause         error
}

func (e *IncidentPersistenceError) Error() string {
	if e == nil || e.Cause == nil {
		return errIncidentPersistence.Error()
	}
	return fmt.Sprintf("%s: %v", errIncidentPersistence, e.Cause)
}

func (e *IncidentPersistenceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *IncidentPersistenceError) Is(target error) bool {
	return target == errIncidentPersistence
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
	DBEngine        string `json:"db_engine"`
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
	// VerifyGatewayIntegrity checks only the AuditChain PostgreSQL row against
	// its local hash and Fabric anchor. It deliberately does not contact the
	// client database or Agent and is used by the scheduled tamper scanner.
	VerifyGatewayIntegrity(logID, clientID string) (string, error)
	GetFabricRecord(anchorID string) (map[string]interface{}, error)
	VerifyDataIntegrity(resource, clientID string, rawData *map[string]interface{}) (*DataVerificationResult, error)

	// GetRecentLogsPaginated menggantikan GetRecentLogs lama sebagai entry
	// point handler dashboard. limit hardcoded 500 di versi lama diganti
	// page/pageSize. integrityStatus kosong berarti tanpa filter.
	GetRecentLogsPaginated(clientID string, page, pageSize int, integrityStatus, sortOrder, sourceTable, dbEngine string, fromTime, toTime *time.Time) (*RecentLogsResult, error)

	GetResourceInventory(clientID string) (interface{}, error)
	VerifyResourceHistory(resource, clientID string) (*ResourceChainResult, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
	GetLatestClientLogByResource(resource, clientID string) (*models.AuditLog, error)
	GetLogsByTimeRange(from, to time.Time, clientID string) ([]models.AuditLog, error)
	GetTableResources(tableName, clientID string) ([]ResourceLogVerification, error)
	EstimateLogRange(from, to time.Time, clientID string) (int64, error)
	VerifyInternalLogRange(from, to time.Time, clientID, requestID string) (*RangeVerificationResult, error)
	VerifyClientLogRange(from, to time.Time, clientID, requestID string) (*RangeVerificationResult, error)
	VerifyRangeBatch(from, to time.Time, clientID, requestID, runID, integritySource string, limit, offset int) (RangeSummary, int, error)
	ProcessAgentAuditReport(clientID string, req AgentAuditReportRequest) error
	ListClientTamperRecords(clientID string, query ClientTamperQuery) (*ClientTamperListResponse, error)
	GetClientTamperRecord(clientID, id string) (*models.ClientDataTamperRecord, error)
	RecoverClientTamperRecord(ctx context.Context, clientID, id, recoveredBy string) (*ClientTamperRecoverResponse, error)
	DismissClientTamperRecord(clientID, id, dismissedBy, notes string) (*models.ClientDataTamperRecord, error)
}

type auditService struct {
	repo                AuditRepository
	fabric              *blockchain.FabricService
	agent               *agentverifier.Service
	db                  *gorm.DB
	rangeVerificationMu sync.Mutex
}

type ResourceLogVerification struct {
	LogID           string `json:"log_id"`
	Resource        string `json:"resource"` // Menyimpan nama/id resource
	IncidentID      string `json:"incident_id,omitempty"`
	IncidentScope   string `json:"incident_scope,omitempty"`
	Action          string `json:"action"`
	LastAction      string `json:"last_action"` // Alias untuk kompabilitas frontend
	Actor           string `json:"actor"`
	Timestamp       string `json:"timestamp"`
	LastUpdatedAt   string `json:"last_updated_at"` // Alias untuk kompabilitas frontend
	HashValue       string `json:"hash_value"`
	IntegrityStatus string `json:"integrity_status"` // valid | tampered | pending | unreachable
	ChainStatus     string `json:"chain_status"`     // Alias untuk kompabilitas frontend
	ProofStatus     string `json:"proof_status,omitempty"`
	Assessment      string `json:"verification_assessment,omitempty"`
	// RecoveryStatus adalah status workflow recovery untuk log target ini.
	// Status ini sengaja dipisah dari IntegrityStatus: Agent client yang tidak
	// dapat dihubungi tidak boleh membuat log yang sudah pulih terlihat rusak.
	//   not_recovered — belum ada request recovery
	//   pending       — request recovery masih berjalan
	//   recovered     — request recovery berhasil dieksekusi
	//   failed        — request recovery terakhir gagal/ditolak
	RecoveryStatus     string     `json:"recovery_status"`
	RecoveryRequestID  string     `json:"recovery_request_id,omitempty"`
	RecoveryExecutedAt *time.Time `json:"recovery_executed_at,omitempty"`
	// SourceStatus is the live-client comparison dimension. It must not be
	// folded into IntegrityStatus/ChainStatus, which describe Gateway + Fabric.
	SourceStatus      string     `json:"source_status,omitempty"`
	StateComparison   string     `json:"state_comparison,omitempty"`
	ComparisonBasis   string     `json:"comparison_basis,omitempty"`
	OffchainStateHash string     `json:"offchain_state_hash,omitempty"`
	ClientStateHash   string     `json:"client_state_hash,omitempty"`
	SourceCheckedAt   *time.Time `json:"source_checked_at,omitempty"`
	AuditTrailStatus  string     `json:"audit_trail_status,omitempty"`
	SourceStateHash   string     `json:"-"`
	// IsLatest marks the newest event in the timeline. A RECOVERY event can be
	// latest because it is a valid AuditChain event.
	IsLatest bool `json:"is_latest"`
	// IsLatestClientEvent marks the newest client-originated event that should
	// be compared with the live client database through Agent.
	IsLatestClientEvent bool `json:"is_latest_client_event"`

	// AgentStatus HANYA relevan untuk event operasional client terbaru
	// (IsLatestClientEvent=true). Agent cuma bisa membaca kondisi TERKINI data klien,
	// jadi membandingkannya ke log lama pasti mismatch meski tidak ada
	// tampering — itu bukan bukti manipulasi, itu snapshot historis yang
	// memang sudah usang secara wajar.
	//   matched            — event client terbaru, Agent dihubungi, data cocok
	//   mismatch           — event client terbaru, Agent dihubungi, ada perbedaan
	//   unreachable        — event client terbaru, Agent gagal dihubungi
	//   not_configured     — event client terbaru, klien belum setup AgentConfig
	//   skipped_recovery   — event internal Gateway, tidak dibandingkan ke Agent
	//   skipped_historical — event client lama, Layer 3 tidak relevan
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
		db:     db,
	}
}

type RangeVerificationResult struct {
	Range   RangeInfo         `json:"range"`
	Summary RangeSummary      `json:"summary"`
	Results []RangeItemResult `json:"results"`
}

const MaxSynchronousVerifyRange = 100

type VerifyRangeTooLargeError struct {
	EstimatedItems int64
	Limit          int
}

func (e *VerifyRangeTooLargeError) Error() string {
	return fmt.Sprintf("verify range berisi %d log; batas sinkron adalah %d", e.EstimatedItems, e.Limit)
}

type RangeInfo struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type RangeSummary struct {
	Total           int `json:"total"`
	Valid           int `json:"valid"`
	Invalid         int `json:"invalid"`
	Pending         int `json:"pending"`
	Inconclusive    int `json:"inconclusive"`
	AlreadyVerified int `json:"already_verified"`
	VerifiedNow     int `json:"verified_now"`
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

func normalizeDBEngine(engine string) string {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "postgres", "postgresql":
		return "postgres"
	case "mongo", "mongodb":
		return "mongodb"
	case "mysql", "mariadb":
		return "mysql"
	case "sqlserver", "sql_server", "mssql", "sql server":
		return "sqlserver"
	case "oracle":
		return "oracle"
	default:
		return ""
	}
}

type RangeItemResult struct {
	LogID                   string                      `json:"log_id"`
	Log                     models.AuditLog             `json:"log"`
	VerifyStatus            string                      `json:"verify_status"`
	Message                 string                      `json:"message,omitempty"`
	VerificationAssessment  string                      `json:"verification_assessment,omitempty"`
	ProofStatus             string                      `json:"proof_status,omitempty"`
	AgentStatus             string                      `json:"agent_status,omitempty"`
	SourceStatus            string                      `json:"source_status,omitempty"`
	StateComparison         string                      `json:"state_comparison,omitempty"`
	ComparisonBasis         string                      `json:"comparison_basis,omitempty"`
	OffchainStateHash       string                      `json:"offchain_state_hash,omitempty"`
	ClientStateHash         string                      `json:"client_state_hash,omitempty"`
	SourceCheckedAt         *time.Time                  `json:"source_checked_at,omitempty"`
	AuditTrailStatus        string                      `json:"audit_trail_status,omitempty"`
	AgentDiscrepancies      []agentverifier.Discrepancy `json:"agent_discrepancies,omitempty"`
	AuditTrailDiscrepancies []agentverifier.Discrepancy `json:"audit_trail_discrepancies,omitempty"`
}

// EstimateLogRange menghitung jumlah log yang akan diproses sebelum operasi
// verifikasi sinkron dimulai. Handler memakainya untuk preflight UX, sedangkan
// VerifyLogRange tetap menghitung ulang agar limit tidak dapat dilewati karena
// race atau client yang memanggil endpoint verify secara langsung.
func (s *auditService) EstimateLogRange(from, to time.Time, clientID string) (int64, error) {
	return s.repo.CountLogsByTimeRange(from, to, clientID)
}

// Implementasi
func (s *auditService) VerifyInternalLogRange(from, to time.Time, clientID, requestID string) (*RangeVerificationResult, error) {
	return s.verifyLogRange(from, to, clientID, requestID)
}

// VerifyClientLogRange intentionally uses the same all-log verification
// contract as the gateway dashboard. A user-triggered range verification
// rechecks every log in the selected range, including rows with cached results.
// The client portal only renders Summary; it must not silently change the
// meaning of a date-range verification to "latest resource per table".
func (s *auditService) VerifyClientLogRange(from, to time.Time, clientID, requestID string) (*RangeVerificationResult, error) {
	return s.verifyLogRange(from, to, clientID, requestID)
}

func (s *auditService) VerifyRangeBatch(from, to time.Time, clientID, requestID, runID, integritySource string, limit, offset int) (RangeSummary, int, error) {
	return s.verifyRangeBatch(from, to, clientID, requestID, runID, integritySource, limit, offset)
}

func (s *auditService) verifyLogRange(from, to time.Time, clientID, requestID string) (*RangeVerificationResult, error) {
	// The checked timestamp is the durable idempotency marker. The mutex also
	// closes the same-process race where two clicks read the row before either
	// request has persisted its result.
	s.rangeVerificationMu.Lock()
	defer s.rangeVerificationMu.Unlock()

	countStarted := time.Now()
	estimatedItems, err := s.EstimateLogRange(from, to, clientID)
	logRangeTiming(requestID, "count_logs", countStarted, err, "estimated_items", estimatedItems)
	if err != nil {
		return nil, err
	}
	if estimatedItems > MaxSynchronousVerifyRange {
		return nil, &VerifyRangeTooLargeError{
			EstimatedItems: estimatedItems,
			Limit:          MaxSynchronousVerifyRange,
		}
	}

	loadStarted := time.Now()
	logs, err := s.repo.GetLogsByTimeRange(from, to, clientID)
	logRangeTiming(requestID, "load_logs", loadStarted, err, "loaded_items", len(logs))
	if err != nil {
		return nil, err
	}

	summary, results := s.verifyRangeItems(logs, clientID, requestID, models.IntegritySourceManualRange, "", true)
	return &RangeVerificationResult{
		Range: RangeInfo{
			From: formatPgTimestamp(from),
			To:   formatPgTimestamp(to),
		},
		Summary: summary,
		Results: results,
	}, nil
}

// verifyRangeBatch is used by the background verifier. User-requested runs
// recheck cached rows, while scheduled runs may reuse them. It reads only one
// bounded page at a time so a large range never becomes one oversized HTTP
// request or one unbounded in-memory slice.
func (s *auditService) verifyRangeBatch(from, to time.Time, clientID, requestID, runID, integritySource string, limit, offset int) (RangeSummary, int, error) {
	s.rangeVerificationMu.Lock()
	defer s.rangeVerificationMu.Unlock()

	started := time.Now()
	logs, err := s.repo.GetLogsByTimeRangePage(from, to, clientID, limit, offset)
	logRangeTiming(requestID, "load_verification_batch", started, err, "loaded_items", len(logs))
	if err != nil {
		return RangeSummary{}, 0, err
	}

	forceRecheck := integritySource == models.IntegritySourceBackgroundRun || integritySource == models.IntegritySourceScheduledRun
	summary, _ := s.verifyRangeItems(logs, clientID, requestID, integritySource, runID, forceRecheck)
	return summary, len(logs), nil
}

func (s *auditService) verifyRangeItems(logs []models.AuditLog, clientID, requestID, integritySource, runID string, forceRecheck bool) (RangeSummary, []RangeItemResult) {
	result := &RangeVerificationResult{
		Results: make([]RangeItemResult, 0, len(logs)),
	}
	newlyVerified := make([]RangeItemResult, 0, len(logs))

	for _, auditLog := range logs {
		item, verifiedNow := s.verifyRangeLog(auditLog, clientID, requestID, integritySource, runID, forceRecheck)
		if verifiedNow {
			result.Summary.VerifiedNow++
			newlyVerified = append(newlyVerified, item)
		} else {
			result.Summary.AlreadyVerified++
		}

		addRangeSummary(&result.Summary, item.VerifyStatus)
		result.Results = append(result.Results, item)
	}

	// Rows that are actually rechecked contribute to cumulative dashboard stats.
	// Scheduled batches may reuse cached results, while manual range requests
	// deliberately record each fresh verification attempt.
	s.updateRangeDashboardStats(clientID, newlyVerified)
	if len(newlyVerified) > 0 {
		s.recordLatestIntegrityCheck(clientID, integritySource, runID, int64(len(newlyVerified)))
	}

	return result.Summary, result.Results
}

func (s *auditService) verifyRangeLog(auditLog models.AuditLog, clientID, requestID, integritySource, runID string, forceRecheck bool) (RangeItemResult, bool) {
	item := RangeItemResult{
		LogID: auditLog.LogID,
		Log:   auditLog,
	}

	if !forceRecheck && hasCachedIntegrity(&auditLog) {
		item.VerifyStatus, item.Message = cachedRangeStatus(auditLog.IntegrityStatus)
		item.VerificationAssessment = cachedRangeAssessment(auditLog.IntegrityStatus)
		return item, false
	}

	verifyStarted := time.Now()
	verifyResult, err := s.verifyLogIntegrity(auditLog.LogID, clientID, requestID, true)
	logRangeTiming(requestID, "verify_log", verifyStarted, err, "log_id", auditLog.LogID)

	persistedStatus := models.IntegrityStatusUnreachable
	if err != nil || verifyResult == nil {
		item.VerifyStatus = "error"
		if err != nil {
			item.Message = err.Error()
		} else {
			item.Message = "hasil verifikasi kosong"
		}
		s.persistIntegrityStatus(auditLog.LogID, clientID, persistedStatus, integritySource, runID, err)
	} else {
		item.VerifyStatus = rangeResultStatus(verifyResult)
		item.Message = verifyResult.Message
		item.VerificationAssessment = verifyResult.Assessment
		item.ProofStatus = verifyResult.ProofStatus
		item.AgentStatus = verifyResult.AgentStatus
		item.SourceStatus = verifyResult.SourceStatus
		item.StateComparison = verifyResult.StateComparison
		item.ComparisonBasis = verifyResult.ComparisonBasis
		item.OffchainStateHash = verifyResult.OffchainStateHash
		item.ClientStateHash = verifyResult.ClientStateHash
		item.SourceCheckedAt = verifyResult.SourceCheckedAt
		item.AuditTrailStatus = verifyResult.AuditTrailStatus
		item.AgentDiscrepancies = verifyResult.AgentDiscrepancies
		item.AuditTrailDiscrepancies = verifyResult.AuditTrailDiffs
		persistedStatus = integrityStatusFromVerificationResult(verifyResult)
		s.persistIntegrityStatus(auditLog.LogID, clientID, persistedStatus, integritySource, runID, nil)
		if verifyResult.SourceStatus != "" {
			s.persistSourceIntegrityStatus(auditLog.LogID, clientID, verifyResult.SourceStatus, nil)
		}
	}

	item.Log.IntegrityStatus = persistedStatus
	item.Log.IntegritySource = integritySource
	item.Log.IntegrityRunID = runID
	now := time.Now().UTC()
	item.Log.IntegrityCheckedAt = &now
	return item, true
}

func hasCachedIntegrity(auditLog *models.AuditLog) bool {
	if auditLog == nil || auditLog.IntegrityCheckedAt == nil {
		return false
	}

	status := strings.ToUpper(strings.TrimSpace(auditLog.IntegrityStatus))
	return status != "" && status != models.IntegrityStatusNotChecked
}

func cachedRangeStatus(status string) (string, string) {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case models.IntegrityStatusValid:
		return "success", "Hasil verifikasi tersimpan dan digunakan kembali."
	case models.IntegrityStatusPending:
		return "pending", "Hasil verifikasi pending tersimpan dan digunakan kembali."
	case models.IntegrityStatusTampered:
		return "failed_cached", "Hasil verifikasi tampered tersimpan dan digunakan kembali."
	case models.IntegrityStatusUnreachable:
		return "inconclusive_cached", "Pemeriksaan tersimpan belum dapat disimpulkan; verifikasi ulang diperlukan."
	default:
		return "error", "Hasil verifikasi tersimpan dengan status tidak dikenal."
	}
}

func cachedRangeAssessment(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case models.IntegrityStatusValid:
		return "CACHED_GATEWAY_INTEGRITY_ONLY"
	case models.IntegrityStatusPending:
		return "PENDING"
	case models.IntegrityStatusTampered:
		return "CACHED_GATEWAY_INTEGRITY_FAILED"
	case models.IntegrityStatusUnreachable:
		return "INCONCLUSIVE_CACHED_RESULT"
	default:
		return "INCONCLUSIVE_CACHED_RESULT"
	}
}

func integrityStatusFromVerificationResult(result *VerificationResult) string {
	if result == nil {
		return models.IntegrityStatusUnreachable
	}

	switch result.Status {
	case "success":
		return models.IntegrityStatusValid
	case "pending":
		return models.IntegrityStatusPending
	case "inconclusive":
		return models.IntegrityStatusUnreachable
	case "failed_local", "failed_onchain", "failed_source":
		return models.IntegrityStatusTampered
	default:
		return models.IntegrityStatusUnreachable
	}
}

func rangeResultStatus(result *VerificationResult) string {
	if result == nil || result.Status != "success" {
		if result == nil {
			return "inconclusive"
		}
		return result.Status
	}
	if shouldRecordClientSourceIncident(result.ProofStatus, result.SourceStatus) {
		return "failed_source"
	}
	switch result.SourceStatus {
	case models.SourceStatusMismatch, models.SourceStatusMissing, models.SourceStatusUnexpectedPresent,
		models.SourceStatusUnreachable, models.SourceStatusNotComparable:
		return "inconclusive"
	default:
		return result.Status
	}
}

func addRangeSummary(summary *RangeSummary, status string) {
	summary.Total++
	switch status {
	case "success":
		summary.Valid++
	case "pending":
		summary.Pending++
	case "failed_local", "failed_onchain", "failed_source", "failed_cached":
		summary.Invalid++
	default:
		summary.Inconclusive++
	}
}

type rangeTableSummary struct {
	Total            int `json:"total"`
	Valid            int `json:"valid"`
	Invalid          int `json:"invalid"`
	Pending          int `json:"pending"`
	Unavailable      int `json:"unavailable,omitempty"`
	TamperedOnchain  int `json:"tampered_onchain,omitempty"`
	TamperedOffchain int `json:"tampered_offchain,omitempty"`
}

func (s *auditService) updateRangeDashboardStats(clientID string, newlyVerified []RangeItemResult) {
	if s.db == nil || len(newlyVerified) == 0 {
		return
	}

	_ = s.db.Transaction(func(tx *gorm.DB) error {
		var stat models.ClientDashboardStats
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("client_id = ?", clientID).First(&stat).Error; err != nil {
			return nil
		}

		var tableResults map[string]rangeTableSummary
		if err := json.Unmarshal([]byte(stat.TableVerifyResults), &tableResults); err != nil || tableResults == nil {
			tableResults = make(map[string]rangeTableSummary)
		}

		totalValid := int64(0)
		totalTampered := int64(0)
		totalPending := int64(0)
		for _, item := range newlyVerified {
			tableName := strings.TrimSpace(strings.SplitN(item.Log.Resource, ":", 2)[0])
			if tableName == "" {
				tableName = "UNKNOWN"
			}

			table := tableResults[tableName]
			table.Total++
			switch item.VerifyStatus {
			case "success":
				totalValid++
				table.Valid++
			case "pending":
				totalPending++
				table.Pending++
			case "failed_local", "failed_onchain", "failed_source", "failed_cached":
				totalTampered++
				table.Invalid++
			default:
				table.Unavailable++
			}
			tableResults[tableName] = table
		}

		newJSON, _ := json.Marshal(tableResults)
		newTotalVerifications := stat.TotalVerifications + int64(len(newlyVerified))
		newTotalValid := stat.TotalValid + totalValid
		newIntegrityScore := float64(0)
		if newTotalVerifications > 0 {
			newIntegrityScore = float64(newTotalValid) / float64(newTotalVerifications) * 100
		}

		return tx.Model(&stat).Updates(map[string]interface{}{
			"last_verified_at":     time.Now().UTC(),
			"last_verified_table":  "MULTIPLE (Audit Range)",
			"total_verifications":  newTotalVerifications,
			"total_rows_verified":  gorm.Expr("total_rows_verified + ?", len(newlyVerified)),
			"total_valid":          newTotalValid,
			"total_tampered":       stat.TotalTampered + totalTampered,
			"total_verify_pending": stat.TotalVerifyPending + totalPending,
			"integrity_score":      newIntegrityScore,
			"table_verify_results": string(newJSON),
		}).Error
	})
}

func logRangeTiming(requestID, operation string, started time.Time, err error, field string, value interface{}) {
	if requestID == "" {
		requestID = "unknown"
	}
	log.Printf(
		"[AuditTiming] request_id=%s operation=%s duration_ms=%d success=%t %s=%v",
		requestID,
		operation,
		time.Since(started).Milliseconds(),
		err == nil,
		field,
		value,
	)
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
	result, err := s.verifyLogIntegrity(logID, clientID, "", false)
	status := models.IntegrityStatusUnreachable
	if result != nil {
		switch result.Status {
		case "success":
			status = models.IntegrityStatusValid
		case "pending":
			status = models.IntegrityStatusPending
		case "inconclusive":
			status = models.IntegrityStatusUnreachable
		case "failed_local", "failed_onchain", "failed_source":
			status = models.IntegrityStatusTampered
		}
	}
	if result != nil {
		s.persistIntegrityStatus(result.LogID, clientID, status, models.IntegritySourceManualSingleLog, "", err)
		if result.SourceStatus != "" {
			s.persistSourceIntegrityStatus(result.LogID, clientID, result.SourceStatus, err)
		}
		s.recordLatestIntegrityCheck(clientID, models.IntegritySourceManualSingleLog, "", 1)
		if err == nil && result.IncidentType != "" {
			auditLog, lookupErr := s.repo.GetLogByID(result.LogID, clientID)
			if lookupErr != nil {
				return result, &IncidentPersistenceError{
					LogID: result.LogID, IncidentType: result.IncidentType,
					IncidentScope: models.RecoveryScopeGatewayIntegrity, Cause: lookupErr,
				}
			}
			incident, persistErr := s.ensureTamperIncident(*auditLog, result.IncidentType, result.ActualHash)
			if persistErr != nil {
				return result, &IncidentPersistenceError{
					LogID: result.LogID, IncidentType: result.IncidentType,
					IncidentScope: models.RecoveryScopeGatewayIntegrity, Cause: persistErr,
				}
			}
			result.IncidentID = incident.ID
			result.IncidentScope = incident.IncidentScope
			result.IncidentStatus = incident.Status
		} else if err == nil && shouldRecordClientSourceIncident(result.ProofStatus, result.SourceStatus) {
			auditLog, lookupErr := s.repo.GetLogByID(result.LogID, clientID)
			if lookupErr != nil {
				return result, &IncidentPersistenceError{
					LogID: result.LogID, IncidentType: clientSourceIncidentType(result.SourceStatus),
					IncidentScope: models.RecoveryScopeClientSource, Cause: lookupErr,
				}
			}
			incident, persistErr := s.ensureClientSourceIncident(*auditLog, result.SourceStatus, result.SourceStateHash, result.AgentDiscrepancies)
			if persistErr != nil {
				return result, &IncidentPersistenceError{
					LogID: result.LogID, IncidentType: clientSourceIncidentType(result.SourceStatus),
					IncidentScope: models.RecoveryScopeClientSource, Cause: persistErr,
				}
			}
			result.IncidentID = incident.ID
			result.IncidentScope = incident.IncidentScope
			result.IncidentType = incident.IncidentType
			result.IncidentStatus = incident.Status
		}
	}
	return result, err
}

// VerifyGatewayIntegrity is the scanner-safe verification path. It checks
// only the gateway database row, Merkle proof, and Fabric anchor. Client-side
// Agent verification is intentionally outside this path.
func (s *auditService) VerifyGatewayIntegrity(logID, clientID string) (string, error) {
	auditLog, err := s.repo.GetLogByID(logID, clientID)
	if err != nil {
		return models.IntegrityStatusUnreachable, errors.New("log_not_found")
	}
	result := s.VerifyGatewayIntegrityBatch([]models.AuditLog{*auditLog})[logID]
	return result.Status, result.Err
}

// VerifyGatewayIntegrityBatch verifies a scanner batch while caching Fabric
// anchor reads by anchor ID. This avoids querying the same Merkle root once
// per log when a batch contains many siblings from one anchored tree.
func (s *auditService) VerifyGatewayIntegrityBatch(logs []models.AuditLog) map[string]tamperscanner.BatchVerificationResult {
	results := make(map[string]tamperscanner.BatchVerificationResult, len(logs))
	checkedPerClient := make(map[string]int64)
	type anchorResult struct {
		root string
		err  error
	}
	anchors := make(map[string]anchorResult)

	for _, input := range logs {
		auditLog := input
		checkedPerClient[auditLog.ClientID]++
		result := tamperscanner.BatchVerificationResult{Status: models.IntegrityStatusUnreachable}
		if isHashStillPending(&auditLog) {
			result.Status = models.IntegrityStatusPending
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
			continue
		}

		canonicalizeLog(&auditLog)
		recalculated := hasher.GenerateLogHash(&auditLog)
		if recalculated != auditLog.HashValue {
			result.Status = models.IntegrityStatusTampered
			s.recordTamperIncident(auditLog, "METADATA_HASH_MISMATCH", recalculated)
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
			continue
		}

		if auditLog.Status != "ANCHORED" || auditLog.BlockchainTxID == nil || strings.TrimSpace(*auditLog.BlockchainTxID) == "" {
			result.Status = models.IntegrityStatusPending
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
			continue
		}
		if s.fabric == nil {
			result.Err = errors.New("fabric_unavailable")
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", result.Err)
			continue
		}

		anchorID := strings.TrimSpace(*auditLog.BlockchainTxID)
		anchor, cached := anchors[anchorID]
		if !cached {
			anchor = anchorResult{}
			raw, err := s.fabric.GetAnchorFromLedger(anchorID)
			if err != nil {
				anchor.err = fmt.Errorf("fabric_read_failed: %w", err)
			} else {
				var fabricResponse struct {
					MerkleRoot string `json:"merkle_root"`
				}
				if err := json.Unmarshal([]byte(raw), &fabricResponse); err != nil || strings.TrimSpace(fabricResponse.MerkleRoot) == "" {
					if err == nil {
						err = errors.New("merkle_root kosong")
					}
					anchor.err = fmt.Errorf("fabric_anchor_invalid: %w", err)
				} else {
					anchor.root = fabricResponse.MerkleRoot
				}
			}
			anchors[anchorID] = anchor
		}
		if anchor.err != nil {
			result.Err = anchor.err
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", result.Err)
			continue
		}

		if auditLog.MerkleRoot != "" && auditLog.MerkleRoot != anchor.root {
			result.Status = models.IntegrityStatusTampered
			s.recordTamperIncident(auditLog, "MERKLE_ROOT_MISMATCH", recalculated)
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
			continue
		}

		proofs, err := s.repo.GetProofsByHash(auditLog.HashValue)
		if err != nil {
			result.Err = fmt.Errorf("merkle_proof_read_failed: %w", err)
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", result.Err)
			continue
		}
		proofs = proofsForAnchorRoot(proofs, anchor.root)
		for _, proof := range proofs {
			if proof.MerkleRoot != "" && proof.MerkleRoot != anchor.root {
				result.Status = models.IntegrityStatusTampered
				s.recordTamperIncident(auditLog, "MERKLE_PROOF_MISMATCH", recalculated)
				break
			}
		}
		if result.Status == models.IntegrityStatusTampered {
			results[auditLog.LogID] = result
			s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
			continue
		}

		reconstructedRoot := auditLog.HashValue
		if len(proofs) > 0 {
			reconstructedRoot = crypto.ReconstructMerkleRoot(auditLog.HashValue, toMerkleProofData(proofs))
		}
		if reconstructedRoot != anchor.root {
			if auditLog.MerkleRoot == anchor.root {
				result.Status = models.IntegrityStatusUnreachable
				result.Err = errors.New("legacy_merkle_proof_incomplete")
			} else if len(proofs) == 0 && auditLog.MerkleRoot == "" {
				result.Status = models.IntegrityStatusUnreachable
				result.Err = errors.New("merkle_proof_missing")
			} else {
				result.Status = models.IntegrityStatusTampered
				s.recordTamperIncident(auditLog, "MERKLE_PROOF_MISMATCH", reconstructedRoot)
			}
		} else {
			result.Status = models.IntegrityStatusValid
		}
		results[auditLog.LogID] = result
		s.persistIntegrityStatus(auditLog.LogID, auditLog.ClientID, result.Status, models.IntegritySourceTamperScanner, "", nil)
	}
	for clientID, count := range checkedPerClient {
		s.recordLatestIntegrityCheck(clientID, models.IntegritySourceTamperScanner, "", count)
	}
	return results
}

func integrityStatusModelValue(status string) string {
	switch status {
	case "valid":
		return models.IntegrityStatusValid
	case "tampered":
		return models.IntegrityStatusTampered
	case "pending":
		return models.IntegrityStatusPending
	case "unreachable":
		return models.IntegrityStatusUnreachable
	default:
		return models.IntegrityStatusNotChecked
	}
}

func (s *auditService) persistIntegrityStatus(logID, clientID, status, integritySource, runID string, verificationErr error) {
	if s.db == nil || logID == "" || clientID == "" {
		return
	}
	now := time.Now().UTC()
	message := ""
	if verificationErr != nil {
		message = verificationErr.Error()
	}
	_ = s.db.Model(&models.AuditLog{}).
		Where("log_id = ? AND client_id = ?", logID, clientID).
		Updates(map[string]interface{}{
			"integrity_status":     status,
			"integrity_checked_at": now,
			"integrity_error":      message,
			"integrity_source":     integritySource,
			"integrity_run_id":     runID,
		}).Error
}

func (s *auditService) persistSourceIntegrityStatus(logID, clientID, sourceStatus string, verificationErr error) {
	if s.db == nil || logID == "" || clientID == "" || sourceStatus == "" {
		return
	}
	status := models.IntegrityStatusNotChecked
	switch strings.ToUpper(strings.TrimSpace(sourceStatus)) {
	case models.SourceStatusMatched:
		status = models.IntegrityStatusValid
	case models.SourceStatusMismatch, models.SourceStatusMissing, models.SourceStatusUnexpectedPresent:
		status = models.IntegrityStatusTampered
	case models.SourceStatusUnreachable:
		status = models.IntegrityStatusUnreachable
	case models.SourceStatusNotComparable:
		status = models.IntegrityStatusNotChecked
	}
	now := time.Now().UTC()
	message := ""
	if verificationErr != nil {
		message = verificationErr.Error()
	}
	_ = s.db.Model(&models.AuditLog{}).
		Where("log_id = ? AND client_id = ?", logID, clientID).
		Updates(map[string]interface{}{
			"source_integrity_status":     status,
			"source_integrity_checked_at": now,
			"source_integrity_error":      message,
		}).Error
}

func (s *auditService) recordLatestIntegrityCheck(clientID, source, runID string, checkedLogs int64) {
	if s.db == nil || clientID == "" || source == "" || checkedLogs <= 0 {
		return
	}

	checkedAt := time.Now().UTC()
	err := s.db.Model(&models.ClientDashboardStats{}).
		Where("client_id = ? AND (last_integrity_check_at IS NULL OR last_integrity_check_at <= ?)", clientID, checkedAt).
		Updates(map[string]interface{}{
			"last_integrity_check_at":     checkedAt,
			"last_integrity_check_source": source,
			"last_integrity_check_run_id": runID,
			"last_integrity_check_logs":   checkedLogs,
		}).Error
	if err != nil {
		log.Printf("[IntegrityProvenance] gagal menyimpan ringkasan client=%s source=%s: %v", clientID, source, err)
	}
}

func (s *auditService) verifyLogIntegrity(logID, clientID, requestID string, skipAgent bool) (*VerificationResult, error) {
	logLookupStarted := time.Now()
	auditLog, err := s.repo.GetLogByID(logID, clientID)
	logRangeTiming(requestID, "db_log_lookup", logLookupStarted, err, "log_id", logID)
	if err != nil {
		return nil, errors.New("log_not_found")
	}

	if isHashStillPending(auditLog) {
		return &VerificationResult{
			Status:      "pending",
			Assessment:  "PENDING",
			ProofStatus: "PENDING",
			Message:     "Log sudah diterima, tetapi hash final masih diproses pipeline.",
			IsValid:     true,
			LogID:       auditLog.LogID,
		}, nil
	}

	canonicalizeLog(auditLog)
	recalculatedHash := hasher.GenerateLogHash(auditLog)
	if recalculatedHash != auditLog.HashValue {
		s.recordTamperIncident(*auditLog, "METADATA_HASH_MISMATCH", recalculatedHash)
		return &VerificationResult{
			Status:       "failed_local",
			Assessment:   "OFFCHAIN_INTEGRITY_FAILED",
			ProofStatus:  "FAILED_LOCAL_HASH",
			Message:      "Hash log off-chain tidak cocok dengan isi log yang tersimpan. Bukti integritas Gateway gagal.",
			IsValid:      false,
			LogID:        auditLog.LogID,
			IncidentType: "METADATA_HASH_MISMATCH",
			ExpectedHash: auditLog.HashValue,
			ActualHash:   recalculatedHash,
		}, nil
	}

	if auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "PENDING_OR_FAILED" {
		return &VerificationResult{
			Status:      "pending",
			Assessment:  "PENDING",
			ProofStatus: "PENDING_ANCHOR",
			Message:     "Log otentik secara lokal. Menunggu antrean Blockchain.",
			IsValid:     true,
			LogID:       auditLog.LogID,
		}, nil
	}

	if s.fabric == nil {
		return &VerificationResult{
			Status: "inconclusive", Assessment: "INCONCLUSIVE_FABRIC_UNAVAILABLE",
			ProofStatus: "UNAVAILABLE_FABRIC", Message: "Fabric tidak tersedia; bukti anchor belum dapat diperiksa.",
			LogID: auditLog.LogID,
		}, nil
	}
	fabricStarted := time.Now()
	onChainData, err := s.fabric.GetAnchorFromLedger(*auditLog.BlockchainTxID)
	logRangeTiming(requestID, "fabric_anchor_lookup", fabricStarted, err, "log_id", logID)
	if err != nil {
		return &VerificationResult{
			Status: "inconclusive", Assessment: "INCONCLUSIVE_FABRIC_UNAVAILABLE",
			ProofStatus: "UNAVAILABLE_FABRIC", Message: "Fabric tidak merespons; bukti anchor belum dapat diperiksa.",
			LogID: auditLog.LogID,
		}, nil
	}

	var fabricResponse struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(onChainData), &fabricResponse); err != nil {
		return &VerificationResult{
			Status: "inconclusive", Assessment: "INCONCLUSIVE_FABRIC_ANCHOR_INVALID",
			ProofStatus: "UNAVAILABLE_FABRIC_ANCHOR_INVALID", Message: "Respons anchor Fabric tidak dapat dibaca; bukti belum dapat disimpulkan.",
			LogID: auditLog.LogID,
		}, nil
	}
	fabricResponse.MerkleRoot = strings.TrimSpace(fabricResponse.MerkleRoot)
	if fabricResponse.MerkleRoot == "" {
		return &VerificationResult{
			Status:      "inconclusive",
			Assessment:  "INCONCLUSIVE_FABRIC_ANCHOR_INVALID",
			ProofStatus: "UNAVAILABLE_FABRIC_ROOT",
			Message:     "Anchor ditemukan, tetapi Merkle root tidak tersedia untuk pembuktian.",
			LogID:       auditLog.LogID,
		}, nil
	}

	proofLookupStarted := time.Now()
	proofs, perr := s.repo.GetProofsByHash(auditLog.HashValue)
	logRangeTiming(requestID, "db_proof_lookup", proofLookupStarted, perr, "log_id", logID)
	if perr != nil {
		return &VerificationResult{
			Status: "inconclusive", Assessment: "INCONCLUSIVE_MERKLE_PROOF_LOOKUP_FAILED",
			ProofStatus: "UNAVAILABLE_MERKLE_PROOF_LOOKUP", Message: "Proof Merkle gagal dibaca dari off-chain; hasil belum dapat disimpulkan.",
			LogID: auditLog.LogID,
		}, nil
	}
	proofs = proofsForAnchorRoot(proofs, fabricResponse.MerkleRoot)

	reconstructedRoot := auditLog.HashValue // batch 1-leaf: root == hash itu sendiri
	if len(proofs) > 0 {
		reconstructedRoot = crypto.ReconstructMerkleRoot(auditLog.HashValue, toMerkleProofData(proofs))
	}
	if auditLog.MerkleRoot != "" && auditLog.MerkleRoot != fabricResponse.MerkleRoot {
		s.recordTamperIncident(*auditLog, "MERKLE_ROOT_MISMATCH", recalculatedHash)
		return &VerificationResult{
			Status:       "failed_onchain",
			Assessment:   "OFFCHAIN_FABRIC_PROOF_FAILED",
			ProofStatus:  "FAILED_ANCHOR_ROOT",
			Message:      "Merkle root off-chain berbeda dari anchor yang tercatat di Fabric.",
			IsValid:      false,
			LogID:        auditLog.LogID,
			IncidentType: "MERKLE_ROOT_MISMATCH",
			ExpectedHash: auditLog.HashValue,
			ActualHash:   recalculatedHash,
			DBRoot:       auditLog.MerkleRoot,
			ChainRoot:    fabricResponse.MerkleRoot,
		}, nil
	}
	for _, proof := range proofs {
		if proof.MerkleRoot != "" && proof.MerkleRoot != fabricResponse.MerkleRoot {
			s.recordTamperIncident(*auditLog, "MERKLE_PROOF_MISMATCH", recalculatedHash)
			return &VerificationResult{
				Status:       "failed_onchain",
				Assessment:   "OFFCHAIN_FABRIC_PROOF_FAILED",
				ProofStatus:  "FAILED_PROOF_BATCH_ROOT",
				Message:      "Proof off-chain merujuk ke root batch yang berbeda dari anchor Fabric.",
				IsValid:      false,
				LogID:        auditLog.LogID,
				IncidentType: "MERKLE_PROOF_MISMATCH",
				ExpectedHash: auditLog.HashValue,
				ActualHash:   recalculatedHash,
				DBRoot:       proof.MerkleRoot,
				ChainRoot:    fabricResponse.MerkleRoot,
			}, nil
		}
	}

	verifiedVia := "merkle_proof"
	if reconstructedRoot != fabricResponse.MerkleRoot {
		// Fallback untuk data lama yang di-anchor SEBELUM fix ini (proof
		// level>0 & IsLeft belum tersimpan benar) — supaya tidak false-positive
		// menandai log lama yang sebenarnya sah sebagai "tampered".
		if auditLog.MerkleRoot == fabricResponse.MerkleRoot {
			verifiedVia = "legacy_field_fallback"
		} else if len(proofs) == 0 && auditLog.MerkleRoot == "" {
			return &VerificationResult{
				Status:       "inconclusive",
				Assessment:   "INCONCLUSIVE_MERKLE_PROOF_MISSING",
				ProofStatus:  "UNAVAILABLE_MERKLE_PROOF",
				Message:      "Hash log lokal cocok, tetapi proof Merkle atau root off-chain yang dapat dibandingkan tidak tersedia.",
				IsValid:      false,
				LogID:        auditLog.LogID,
				ExpectedHash: auditLog.HashValue,
				ActualHash:   recalculatedHash,
				DBRoot:       reconstructedRoot,
				ChainRoot:    fabricResponse.MerkleRoot,
			}, nil
		} else {
			s.recordTamperIncident(*auditLog, "MERKLE_ROOT_MISMATCH", recalculatedHash)
			return &VerificationResult{
				Status:       "failed_onchain",
				Assessment:   "OFFCHAIN_FABRIC_PROOF_FAILED",
				ProofStatus:  "FAILED_MERKLE_PROOF",
				Message:      "Merkle proof dari log off-chain tidak menghasilkan root anchor Fabric.",
				IsValid:      false,
				LogID:        auditLog.LogID,
				IncidentType: "MERKLE_ROOT_MISMATCH",
				ExpectedHash: auditLog.HashValue,
				ActualHash:   recalculatedHash,
				DBRoot:       reconstructedRoot,
				ChainRoot:    fabricResponse.MerkleRoot,
			}, nil
		}
	}

	successMsg := "✅ DATA OTENTIK: Terverifikasi di database dan Blockchain."
	if verifiedVia == "legacy_field_fallback" {
		successMsg += " (diverifikasi via metode lama — log ini di-anchor sebelum perbaikan Merkle proof)"
	}
	proofStatus := "VERIFIED"
	if verifiedVia == "legacy_field_fallback" {
		proofStatus = "LEGACY_LIMITED"
	}

	agentStatus := "skipped_historical"
	sourceStatus := ""
	sourceStateHash := ""
	offchainStateHash := ""
	var sourceCheckedAt *time.Time
	stateComparison := ""
	comparisonBasis := ""
	auditTrailStatus := ""
	var auditTrailDiffs []agentverifier.Discrepancy
	var agentDiscrepancies []agentverifier.Discrepancy

	if !skipAgent {
		if isRecoveryAction(auditLog.Action) {
			agentStatus = "skipped_recovery"
		} else {
			latestLookupStarted := time.Now()
			latestClientLog, latestErr := s.repo.GetLatestClientLogByResource(auditLog.Resource, clientID)
			logRangeTiming(requestID, "db_latest_resource_lookup", latestLookupStarted, latestErr, "log_id", logID)
			isLatestClientEvent := latestErr == nil && latestClientLog != nil && latestClientLog.LogID == auditLog.LogID
			if shouldVerifyResourceWithAgent(*auditLog, isLatestClientEvent) {
				agentStatus = "not_configured"
				agentStarted := time.Now()
				agentResult, agentErr := s.agent.VerifyAgainstAgent(auditLog)
				logRangeTiming(requestID, "agent_verify", agentStarted, agentErr, "log_id", logID)
				if agentErr != nil {
					agentStatus, sourceStatus = agentFailureStatuses(agentErr)
				} else if agentResult == nil {
					agentStatus = "unreachable"
					sourceStatus = models.SourceStatusUnreachable
				} else if agentResult.AgentUsed {
					offchainStateHash = agentResult.OffchainStateHash
					sourceStateHash = agentResult.ClientStateHash
					sourceCheckedAt = agentResult.SourceCheckedAt
					stateComparison = agentResult.StateComparison
					comparisonBasis = agentResult.ComparisonBasis
					auditTrailStatus = agentResult.AuditTrailStatus
					auditTrailDiffs = agentResult.AuditTrailDiffs
					if !agentResult.StateComparable {
						agentStatus = "not_comparable"
						sourceStatus = models.SourceStatusNotComparable
					} else if agentResult.IsMatch {
						agentStatus = "matched"
						sourceStatus = models.SourceStatusMatched
					} else {
						agentStatus = "mismatch"
						sourceStatus = sourceStatusFromAgentResult(*auditLog, agentResult)
						agentDiscrepancies = agentResult.Discrepancies
						if shouldRecordClientSourceIncident(proofStatus, sourceStatusFromAgentResult(*auditLog, agentResult)) {
							s.recordClientSourceIncident(*auditLog, sourceStatusFromAgentResult(*auditLog, agentResult), agentResult.ClientStateHash, agentResult.Discrepancies)
						}
					}
				} else {
					sourceStatus = models.SourceStatusNotComparable
				}
			}
		}
	}
	assessment := verificationAssessment(proofStatus, sourceStatus, agentStatus, auditTrailStatus)
	if proofStatus == "LEGACY_LIMITED" && (sourceStatus == models.SourceStatusMismatch || sourceStatus == models.SourceStatusMissing || sourceStatus == models.SourceStatusUnexpectedPresent) {
		successMsg = "Proof Merkle legacy terbatas cocok dengan root Fabric, tetapi belum membuktikan membership secara penuh; terdapat perbedaan pada proyeksi data sumber sehingga kesimpulan end-to-end belum dapat dipastikan."
	} else if sourceStatus == models.SourceStatusMismatch || sourceStatus == models.SourceStatusMissing || sourceStatus == models.SourceStatusUnexpectedPresent {
		successMsg = "Bukti log dan Fabric terverifikasi, tetapi proyeksi field data klien berbeda dari event klien terakhir yang tercatat."
	} else if sourceStatus == models.SourceStatusUnreachable || sourceStatus == models.SourceStatusNotComparable {
		successMsg = "Bukti log dan Fabric terverifikasi; pemeriksaan state database klien belum dapat disimpulkan."
	} else if agentStatus == "skipped_historical" || agentStatus == "skipped_recovery" {
		successMsg = "Bukti log dan Fabric terverifikasi; pemeriksaan data live dilewati karena event ini bukan event klien terbaru."
	}

	return &VerificationResult{
		Status:             "success",
		Assessment:         assessment,
		ProofStatus:        proofStatus,
		Message:            successMsg,
		IsValid:            true,
		LogID:              auditLog.LogID,
		ExpectedHash:       auditLog.HashValue,
		DBRoot:             reconstructedRoot,
		TxID:               auditLog.BlockchainTxID,
		AgentStatus:        agentStatus,
		SourceStatus:       sourceStatus,
		StateComparison:    stateComparison,
		ComparisonBasis:    comparisonBasis,
		OffchainStateHash:  offchainStateHash,
		ClientStateHash:    sourceStateHash,
		SourceCheckedAt:    sourceCheckedAt,
		AuditTrailStatus:   auditTrailStatus,
		AuditTrailDiffs:    auditTrailDiffs,
		SourceStateHash:    sourceStateHash,
		AgentDiscrepancies: agentDiscrepancies,
	}, nil
}

func (s *auditService) verifyClientLogIntegrity(logID, clientID, requestID string) (*VerificationResult, error) {
	logLookupStarted := time.Now()
	auditLog, err := s.repo.GetLogByID(logID, clientID)
	logRangeTiming(requestID, "db_log_lookup", logLookupStarted, err, "log_id", logID)
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

	if auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "PENDING_OR_FAILED" {
		return &VerificationResult{
			Status:  "pending",
			Message: "Log otentik secara lokal. Menunggu antrean Blockchain.",
			IsValid: true,
			LogID:   auditLog.LogID,
		}, nil
	}

	if s.fabric == nil {
		return nil, errors.New("fabric_error")
	}

	agentStarted := time.Now()
	agentResult, agentErr := s.agent.VerifyAgainstAgent(auditLog)
	logRangeTiming(requestID, "agent_verify", agentStarted, agentErr, "log_id", logID)

	if agentErr != nil || agentResult == nil {
		return &VerificationResult{
			Status:      "failed_source",
			Message:     "🚨 Gagal menghubungi Agent atau Agent belum dikonfigurasi.",
			IsValid:     false,
			LogID:       auditLog.LogID,
			AgentStatus: "unreachable",
		}, nil
	}

	if !agentResult.AgentUsed {
		return &VerificationResult{
			Status:      "failed_source",
			Message:     "🚨 Agent belum dikonfigurasi.",
			IsValid:     false,
			LogID:       auditLog.LogID,
			AgentStatus: "not_configured",
		}, nil
	}

	// Buat copy log dan rehash dengan data dari agent
	canonicalizeLog(auditLog)
	logCopy := *auditLog
	// HANYA gunakan ClientMetadata dari agen JIKA ada perbedaan data nyata (IsMatch = false).
	// Jika data sebenarnya persis sama, gunakan string Metadata orisinil untuk menjamin
	// urutan key JSON identik, sehingga tidak menghasilkan false-positive saat re-hash.
	if agentResult.ClientMetadata != "" && !agentResult.IsMatch {
		logCopy.Metadata = agentResult.ClientMetadata
	}
	recalculatedHash := hasher.GenerateLogHash(&logCopy)

	fabricStarted := time.Now()
	onChainData, err := s.fabric.GetAnchorFromLedger(*auditLog.BlockchainTxID)
	logRangeTiming(requestID, "fabric_anchor_lookup", fabricStarted, err, "log_id", logID)
	if err != nil {
		return nil, errors.New("fabric_error")
	}

	var fabricResponse struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(onChainData), &fabricResponse); err != nil {
		return nil, errors.New("parse_error")
	}

	proofLookupStarted := time.Now()
	proofs, perr := s.repo.GetProofsByHash(auditLog.HashValue)
	logRangeTiming(requestID, "db_proof_lookup", proofLookupStarted, perr, "log_id", logID)
	if perr != nil {
		return nil, errors.New("proof_lookup_error")
	}

	// Reconstruct root with client rehashed value
	reconstructedRoot := recalculatedHash
	if len(proofs) > 0 {
		reconstructedRoot = crypto.ReconstructMerkleRoot(recalculatedHash, toMerkleProofData(proofs))
	}

	if reconstructedRoot != fabricResponse.MerkleRoot {
		agStatus := "mismatch"
		if agentResult.IsMatch {
			agStatus = "matched"
		}
		return &VerificationResult{
			Status:             "failed_onchain",
			Message:            "🚨 DATA TERMANIPULASI: Data klien saat ini tidak valid di mata Blockchain!",
			IsValid:            false,
			LogID:              auditLog.LogID,
			DBRoot:             reconstructedRoot,
			ChainRoot:          fabricResponse.MerkleRoot,
			AgentStatus:        agStatus,
			AgentDiscrepancies: agentResult.Discrepancies,
		}, nil
	}

	return &VerificationResult{
		Status:             "success",
		Message:            "✅ DATA OTENTIK: Data klien cocok dengan Blockchain.",
		IsValid:            true,
		LogID:              auditLog.LogID,
		ExpectedHash:       auditLog.HashValue,
		DBRoot:             reconstructedRoot,
		TxID:               auditLog.BlockchainTxID,
		AgentStatus:        "matched",
		AgentDiscrepancies: agentResult.Discrepancies,
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
	status, _ := s.classifyIntegrityDetailed(auditLog)
	return status
}

func (s *auditService) classifyIntegrityDetailed(auditLog models.AuditLog) (string, string) {
	if isHashStillPending(&auditLog) {
		return "pending", "PENDING_HASH"
	}

	canonicalizeLog(&auditLog)
	recalculated := hasher.GenerateLogHash(&auditLog)
	if recalculated != auditLog.HashValue {
		s.recordTamperIncident(auditLog, "METADATA_HASH_MISMATCH", recalculated)
		return "tampered", "FAILED_LOCAL_HASH"
	}

	if auditLog.Status != "ANCHORED" || auditLog.BlockchainTxID == nil || *auditLog.BlockchainTxID == "" {
		return "pending", "PENDING_ANCHOR"
	}

	if s.fabric == nil {
		return "unreachable", "UNAVAILABLE_FABRIC"
	}

	onChainData, err := s.fabric.GetAnchorFromLedger(*auditLog.BlockchainTxID)
	if err != nil {
		return "unreachable", "UNAVAILABLE_FABRIC"
	}

	var fabricResponse struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(onChainData), &fabricResponse); err != nil {
		return "unreachable", "UNAVAILABLE_FABRIC_ANCHOR_INVALID"
	}
	fabricResponse.MerkleRoot = strings.TrimSpace(fabricResponse.MerkleRoot)
	if fabricResponse.MerkleRoot == "" {
		return "unreachable", "UNAVAILABLE_FABRIC_ROOT"
	}

	proofs, perr := s.repo.GetProofsByHash(auditLog.HashValue)
	if perr != nil {
		return "unreachable", "UNAVAILABLE_MERKLE_PROOF_LOOKUP"
	}
	proofs = proofsForAnchorRoot(proofs, fabricResponse.MerkleRoot)

	reconstructedRoot := auditLog.HashValue
	if len(proofs) > 0 {
		reconstructedRoot = crypto.ReconstructMerkleRoot(auditLog.HashValue, toMerkleProofData(proofs))
	}

	if auditLog.MerkleRoot != "" && auditLog.MerkleRoot != fabricResponse.MerkleRoot {
		s.recordTamperIncident(auditLog, "MERKLE_ROOT_MISMATCH", recalculated)
		return "tampered", "FAILED_ANCHOR_ROOT"
	}
	for _, proof := range proofs {
		if proof.MerkleRoot != "" && proof.MerkleRoot != fabricResponse.MerkleRoot {
			s.recordTamperIncident(auditLog, "MERKLE_PROOF_MISMATCH", recalculated)
			return "tampered", "FAILED_PROOF_BATCH_ROOT"
		}
	}
	if reconstructedRoot != fabricResponse.MerkleRoot {
		// Legacy batches may have incomplete proof direction data. The fallback
		// is allowed only when the stored off-chain root itself matches Fabric.
		if auditLog.MerkleRoot == fabricResponse.MerkleRoot {
			return "unreachable", "LEGACY_LIMITED"
		}
		if len(proofs) == 0 && auditLog.MerkleRoot == "" {
			return "unreachable", "UNAVAILABLE_MERKLE_PROOF"
		}
		s.recordTamperIncident(auditLog, "MERKLE_PROOF_MISMATCH", recalculated)
		return "tampered", "FAILED_MERKLE_PROOF"
	}

	return "valid", "VERIFIED_MERKLE_PROOF"
}

// recordTamperIncident membuat satu incident aktif per log dan jenis deteksi.
// Verifikasi dashboard dapat dipanggil berkali-kali, sehingga operasi ini
// harus idempoten dan tidak boleh membuat baris incident baru pada setiap GET.
func (s *auditService) recordTamperIncident(auditLog models.AuditLog, incidentType, detectedHash string) {
	if _, err := s.ensureTamperIncident(auditLog, incidentType, detectedHash); err != nil {
		log.Printf("[RecoveryIncident] gagal menyimpan client=%s log=%s type=%s: %v", auditLog.ClientID, auditLog.LogID, incidentType, err)
	}
}

func (s *auditService) ensureTamperIncident(auditLog models.AuditLog, incidentType, detectedHash string) (*models.TamperIncident, error) {
	if s.db == nil {
		return nil, fmt.Errorf("%w: database unavailable", errIncidentPersistence)
	}
	if strings.TrimSpace(auditLog.LogID) == "" || strings.TrimSpace(auditLog.ClientID) == "" || strings.TrimSpace(incidentType) == "" {
		return nil, fmt.Errorf("%w: log, client, or incident type is empty", errIncidentPersistence)
	}

	existing, err := s.dbOpenGatewayIncident(auditLog.ClientID, auditLog.LogID, incidentType)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: lookup active incident: %v", errIncidentPersistence, err)
	}
	incident := &models.TamperIncident{
		ID:                 uuid.NewString(),
		ClientID:           auditLog.ClientID,
		LogID:              auditLog.LogID,
		Resource:           auditLog.Resource,
		IncidentType:       incidentType,
		ExpectedHash:       auditLog.HashValue,
		DetectedHash:       detectedHash,
		DiscrepancySummary: "[]",
		Status:             models.IncidentStatusOpen,
		IncidentScope:      models.RecoveryScopeGatewayIntegrity,
		DetectedAt:         time.Now().UTC(),
	}
	if err := s.db.Create(incident).Error; err != nil {
		// The partial unique index makes concurrent verification idempotent. If
		// another request inserted the active incident first, return that row;
		// otherwise expose the database error to the synchronous verifier.
		existing, lookupErr := s.dbOpenGatewayIncident(auditLog.ClientID, auditLog.LogID, incidentType)
		if lookupErr == nil {
			return existing, nil
		}
		return nil, fmt.Errorf("%w: insert active incident: %v", errIncidentPersistence, err)
	}
	return incident, nil
}

func (s *auditService) dbOpenGatewayIncident(clientID, logID, incidentType string) (*models.TamperIncident, error) {
	if s.db == nil {
		return nil, fmt.Errorf("%w: database unavailable", errIncidentPersistence)
	}
	var existing models.TamperIncident
	err := s.db.Where(
		"client_id = ? AND log_id = ? AND incident_type = ? AND incident_scope = ? AND status = ?",
		clientID,
		logID,
		incidentType,
		models.RecoveryScopeGatewayIntegrity,
		models.IncidentStatusOpen,
	).First(&existing).Error
	if err != nil {
		return nil, err
	}
	return &existing, nil
}

func sourceStatusFromAgentResult(auditLog models.AuditLog, result *agentverifier.VerifyResult) string {
	if result == nil {
		return models.SourceStatusNotComparable
	}
	if strings.EqualFold(strings.TrimSpace(auditLog.Action), "DELETE") {
		if result.SourceFound {
			return models.SourceStatusUnexpectedPresent
		}
		return models.SourceStatusMatched
	}
	if !result.SourceFound {
		return models.SourceStatusMissing
	}
	return models.SourceStatusMismatch
}

func sourceStatusFromAgentStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "matched":
		return models.SourceStatusMatched
	case "mismatch":
		return models.SourceStatusMismatch
	case "unreachable":
		return models.SourceStatusUnreachable
	case "not_configured":
		return models.SourceStatusNotComparable
	default:
		return ""
	}
}

func shouldRecordClientSourceIncident(proofStatus, sourceStatus string) bool {
	return proofStatus != "LEGACY_LIMITED" && isClientSourceIncidentStatus(sourceStatus)
}

func verificationAssessment(proofStatus, sourceStatus, agentStatus, auditTrailStatus string) string {
	switch proofStatus {
	case "FAILED_LOCAL_HASH", "FAILED_ANCHOR_ROOT", "FAILED_PROOF_BATCH_ROOT", "FAILED_MERKLE_PROOF":
		return "OFFCHAIN_INTEGRITY_FAILED"
	case "PENDING", "PENDING_HASH", "PENDING_ANCHOR":
		return "PENDING"
	case "UNAVAILABLE", "UNAVAILABLE_FABRIC":
		return "INCONCLUSIVE_FABRIC_UNAVAILABLE"
	case "UNAVAILABLE_FABRIC_ROOT", "UNAVAILABLE_FABRIC_ANCHOR_INVALID":
		return "INCONCLUSIVE_FABRIC_ANCHOR_INVALID"
	case "UNAVAILABLE_MERKLE_PROOF":
		return "INCONCLUSIVE_MERKLE_PROOF_MISSING"
	case "UNAVAILABLE_MERKLE_PROOF_LOOKUP":
		return "INCONCLUSIVE_MERKLE_PROOF_LOOKUP_FAILED"
	}
	if proofStatus == "LEGACY_LIMITED" {
		if auditTrailStatus == "mismatch" {
			return "INCONCLUSIVE_LEGACY_PROOF_WITH_SOURCE_EVENT_DIVERGENCE"
		}
		if sourceStatus == models.SourceStatusMatched {
			return "VERIFIED_LEGACY_LIMITED_PROOF"
		}
		if sourceStatus == models.SourceStatusMismatch || sourceStatus == models.SourceStatusMissing || sourceStatus == models.SourceStatusUnexpectedPresent {
			return "INCONCLUSIVE_LEGACY_PROOF_WITH_SOURCE_DIVERGENCE"
		}
		if sourceStatus == models.SourceStatusUnreachable {
			return "INCONCLUSIVE_LEGACY_PROOF_AGENT_UNREACHABLE"
		}
		if sourceStatus == models.SourceStatusNotComparable {
			return "INCONCLUSIVE_LEGACY_PROOF_SOURCE_NOT_COMPARABLE"
		}
		return "INCONCLUSIVE_LEGACY_PROOF"
	}
	if auditTrailStatus == "mismatch" {
		return "SOURCE_EVENT_DIVERGENCE"
	}
	if agentStatus == "skipped_historical" || agentStatus == "skipped_recovery" {
		return "GATEWAY_PROOF_VERIFIED_SOURCE_SKIPPED"
	}
	if sourceStatus == models.SourceStatusMatched {
		return "VERIFIED"
	}
	if sourceStatus == models.SourceStatusMismatch || sourceStatus == models.SourceStatusMissing || sourceStatus == models.SourceStatusUnexpectedPresent {
		return "CLIENT_STATE_DIVERGENCE"
	}
	if sourceStatus == models.SourceStatusUnreachable {
		return "INCONCLUSIVE_AGENT_UNREACHABLE"
	}
	if sourceStatus == models.SourceStatusNotComparable || sourceStatus == "" {
		return "INCONCLUSIVE_SOURCE_NOT_COMPARABLE"
	}
	return "INCONCLUSIVE"
}

// agentFailureStatuses distinguishes an Agent that answered but denied or
// rejected the request from an Agent that could not be reached. A denied
// request is not evidence that the client row mismatches; no comparison ran.
func agentFailureStatuses(err error) (agentStatus, sourceStatus string) {
	var statusErr *agentverifier.HTTPStatusError
	if errors.As(err, &statusErr) && statusErr != nil {
		switch statusErr.StatusCode {
		case http.StatusUnauthorized:
			return "unauthorized", models.SourceStatusNotComparable
		case http.StatusForbidden:
			return "forbidden", models.SourceStatusNotComparable
		default:
			return "http_error", models.SourceStatusNotComparable
		}
	}

	return "unreachable", models.SourceStatusUnreachable
}

// recordClientSourceIncident records a discrepancy between a trusted latest
// client event and the live operational row. It is intentionally separate
// from Gateway/Fabric integrity incidents: the AuditChain evidence can remain
// VALID while the client source is MISMATCH/MISSING/UNEXPECTED_PRESENT.
func (s *auditService) recordClientSourceIncident(auditLog models.AuditLog, sourceStatus, detectedStateHash string, discrepancies []agentverifier.Discrepancy) {
	if _, err := s.ensureClientSourceIncident(auditLog, sourceStatus, detectedStateHash, discrepancies); err != nil {
		log.Printf("[RecoveryIncident] gagal menyimpan client-source client=%s log=%s status=%s: %v", auditLog.ClientID, auditLog.LogID, sourceStatus, err)
	}
}

func isClientSourceIncidentStatus(sourceStatus string) bool {
	switch strings.ToUpper(strings.TrimSpace(sourceStatus)) {
	case models.SourceStatusMismatch, models.SourceStatusMissing, models.SourceStatusUnexpectedPresent:
		return true
	default:
		return false
	}
}

func clientSourceIncidentType(sourceStatus string) string {
	switch strings.ToUpper(strings.TrimSpace(sourceStatus)) {
	case models.SourceStatusMissing:
		return "CLIENT_ROW_MISSING"
	case models.SourceStatusUnexpectedPresent:
		return "CLIENT_ROW_UNEXPECTED_PRESENT"
	default:
		return "CLIENT_STATE_MISMATCH"
	}
}

func (s *auditService) ensureClientSourceIncident(auditLog models.AuditLog, sourceStatus, detectedStateHash string, discrepancies []agentverifier.Discrepancy) (*models.TamperIncident, error) {
	if !isClientSourceIncidentStatus(sourceStatus) {
		return nil, nil
	}
	if s.db == nil {
		return nil, fmt.Errorf("%w: database unavailable", errIncidentPersistence)
	}
	if strings.TrimSpace(auditLog.ClientID) == "" || strings.TrimSpace(auditLog.Resource) == "" || strings.TrimSpace(auditLog.LogID) == "" {
		return nil, fmt.Errorf("%w: client, resource, or log is empty", errIncidentPersistence)
	}
	incidentType := clientSourceIncidentType(sourceStatus)
	// Do not persist client row values in the incident table. The hash and the
	// field names are enough to correlate the source-state problem; raw values
	// may contain credentials, identifiers, or other client-sensitive data.
	redactedFields := make([]map[string]interface{}, 0, len(discrepancies))
	for _, discrepancy := range discrepancies {
		redactedFields = append(redactedFields, map[string]interface{}{
			"field":   discrepancy.Field,
			"changed": true,
		})
	}
	redacted, err := json.Marshal(redactedFields)
	if err != nil {
		redacted = []byte("[]")
	}
	var existing models.TamperIncident
	lookupErr := s.db.Where(
		"client_id = ? AND resource = ? AND incident_type = ? AND incident_scope = ? AND status = ?",
		auditLog.ClientID, auditLog.Resource, incidentType, models.RecoveryScopeClientSource, models.IncidentStatusOpen,
	).First(&existing).Error
	now := time.Now().UTC()
	if lookupErr == nil {
		if err := s.db.Model(&existing).Updates(map[string]interface{}{
			"log_id":              auditLog.LogID,
			"expected_hash":       auditLog.HashValue,
			"detected_hash":       detectedStateHash,
			"detected_state_hash": detectedStateHash,
			"source_status":       sourceStatus,
			"reference_log_id":    auditLog.LogID,
			"discrepancy_summary": string(redacted),
			"last_confirmed_at":   now,
		}).Error; err != nil {
			return nil, fmt.Errorf("%w: update active client-source incident: %v", errIncidentPersistence, err)
		}
		existing.LogID = auditLog.LogID
		existing.ExpectedHash = auditLog.HashValue
		existing.DetectedHash = detectedStateHash
		existing.DetectedStateHash = detectedStateHash
		existing.SourceStatus = sourceStatus
		existing.ReferenceLogID = auditLog.LogID
		existing.DiscrepancySummary = string(redacted)
		existing.LastConfirmedAt = &now
		return &existing, nil
	}
	if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w: lookup active client-source incident: %v", errIncidentPersistence, lookupErr)
	}
	incident := &models.TamperIncident{
		ID: uuid.NewString(), ClientID: auditLog.ClientID, LogID: auditLog.LogID,
		Resource: auditLog.Resource, IncidentType: incidentType,
		ExpectedHash: auditLog.HashValue, DetectedHash: detectedStateHash,
		Status: models.IncidentStatusOpen, IncidentScope: models.RecoveryScopeClientSource,
		SourceStatus: sourceStatus, ReferenceLogID: auditLog.LogID,
		DetectedStateHash: detectedStateHash, DiscrepancySummary: string(redacted),
		DetectedAt: now, LastConfirmedAt: &now,
	}
	if err := s.db.Create(incident).Error; err != nil {
		// Concurrent checks for the same resource may race on the resource-level
		// active index. Return the winning row when it exists; otherwise surface
		// the persistence failure to the synchronous verification endpoint.
		var existing models.TamperIncident
		lookupErr := s.db.Where(
			"client_id = ? AND resource = ? AND incident_type = ? AND incident_scope = ? AND status = ?",
			auditLog.ClientID, auditLog.Resource, incidentType, models.RecoveryScopeClientSource, models.IncidentStatusOpen,
		).First(&existing).Error
		if lookupErr == nil {
			return &existing, nil
		}
		return nil, fmt.Errorf("%w: insert active client-source incident: %v", errIncidentPersistence, err)
	}
	return incident, nil
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
func (s *auditService) GetRecentLogsPaginated(clientID string, page, pageSize int, integrityStatus, sortOrder, sourceTable, dbEngine string, fromTime, toTime *time.Time) (*RecentLogsResult, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 200 {
		pageSize = 200
	}

	validFilter := map[string]bool{"valid": true, "tampered": true, "unreachable": true, "pending": true, "not_checked": true}
	clientDBEngine, err := s.repo.GetClientDBEngine(clientID)
	if err != nil {
		return nil, err
	}
	normalizedClientDBEngine := normalizeDBEngine(clientDBEngine)
	normalizedFilterDBEngine := normalizeDBEngine(dbEngine)

	if strings.TrimSpace(dbEngine) != "" && normalizedFilterDBEngine == "" {
		return &RecentLogsResult{
			Data: []RecentLogItem{},
			Pagination: PaginationMeta{
				Page:       page,
				PageSize:   pageSize,
				TotalItems: 0,
				TotalPages: 1,
			},
		}, nil
	}

	if normalizedFilterDBEngine != "" && normalizedClientDBEngine != normalizedFilterDBEngine {
		return &RecentLogsResult{
			Data: []RecentLogItem{},
			Pagination: PaginationMeta{
				Page:       page,
				PageSize:   pageSize,
				TotalItems: 0,
				TotalPages: 1,
			},
		}, nil
	}

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
		logs, total, err := s.repo.GetRecentLogsPage(clientID, page, pageSize, sortOrder, sourceTable, fromTime, toTime)
		if err != nil {
			return nil, err
		}

		items := make([]RecentLogItem, 0, len(logs))
		for _, l := range logs {
			integrityStatus := strings.ToLower(strings.TrimSpace(l.IntegrityStatus))
			if integrityStatus == "" {
				integrityStatus = "not_checked"
			}
			items = append(items, RecentLogItem{
				AuditLog:        l,
				IntegrityStatus: integrityStatus,
				DBEngine:        normalizedClientDBEngine,
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

func (s *auditService) GetResourceInventory(clientID string) (interface{}, error) {
	return s.repo.GetClientTables(clientID)
}

// VerifyResourceHistory menjalankan verifikasi Layer 2 (re-hash) + Layer 4
// (Merkle proof reconstruction vs Fabric) untuk SETIAP log milik resource
// ini, dan Layer 3 (Agent, live client data) HANYA untuk event client terbaru.
// Event RECOVERY tetap latest di timeline tetapi tidak menggantikan event
// client yang menjadi pembanding kondisi live. Hasilnya diagregasi menjadi
// satu chain_status.
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
	latestClientIndex := latestClientEventIndex(logs)
	for i, auditLog := range logs {
		item := s.classifyResourceLog(auditLog, i == lastIndex, i == latestClientIndex)
		if strings.EqualFold(item.IntegrityStatus, "tampered") {
			incident, persistErr := s.ensureResourceTamperIncident(auditLog)
			if persistErr != nil {
				return nil, &IncidentPersistenceError{
					LogID: auditLog.LogID, IncidentType: incidentTypeForResourceTamper(auditLog), Cause: persistErr,
				}
			}
			item.IncidentID = incident.ID
			item.IncidentScope = models.RecoveryScopeGatewayIntegrity
		}
		if shouldRecordClientSourceIncident(item.ProofStatus, item.SourceStatus) {
			incident, persistErr := s.ensureClientSourceIncident(auditLog, item.SourceStatus, item.SourceStateHash, item.AgentDiscrepancies)
			if persistErr != nil {
				return nil, &IncidentPersistenceError{
					LogID: auditLog.LogID, IncidentType: clientSourceIncidentType(item.SourceStatus),
					IncidentScope: models.RecoveryScopeClientSource, Cause: persistErr,
				}
			}
			if item.IncidentID == "" {
				item.IncidentID = incident.ID
				item.IncidentScope = incident.IncidentScope
			}
		}
		result.Logs = append(result.Logs, item)

		// baseIntegrityFailed dicek terpisah dari AgentStatus supaya
		// "log_integrity_failed" hanya dilaporkan untuk kegagalan Layer 2/4
		// yang sesungguhnya. Ketidaktersediaan Agent adalah status operasional
		// client dan tidak mengubah integritas Gateway/Fabric.
		baseIntegrityFailed := item.IntegrityStatus == "tampered"
		if baseIntegrityFailed {
			chainIssues = append(chainIssues, fmt.Sprintf("log_integrity_failed:%s", auditLog.LogID))
		}
		if item.IsLatestClientEvent && item.AgentStatus == "mismatch" {
			chainIssues = append(chainIssues, fmt.Sprintf("client_mismatch:%s", auditLog.LogID))
		}

		// Agregasi chain hanya memakai status Gateway/Fabric. AgentStatus
		// sengaja tidak ikut menentukan chain_status.
		switch item.ChainStatus {
		case "tampered":
			hasTampered = true
		case "unreachable":
			hasUnreachable = true
		case "pending":
			hasPending = true
		}
	}
	if err := s.enrichRecoveryStatuses(clientID, result.Logs); err != nil {
		return nil, err
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

func (s *auditService) GetLatestClientLogByResource(resource, clientID string) (*models.AuditLog, error) {
	return s.repo.GetLatestClientLogByResource(resource, clientID)
}

func (s *auditService) GetLogsByTimeRange(from, to time.Time, clientID string) ([]models.AuditLog, error) {
	return s.repo.GetLogsByTimeRange(from, to, clientID)
}

func (s *auditService) GetTableResources(tableName, clientID string) ([]ResourceLogVerification, error) {
	logs, err := s.repo.GetTableResources(tableName, clientID)
	if err != nil {
		return nil, err
	}

	results := make([]ResourceLogVerification, 0, len(logs))
	for _, auditLog := range logs {
		item := s.classifyResourceLog(auditLog, false, false)
		if strings.EqualFold(item.IntegrityStatus, "tampered") {
			incident, persistErr := s.ensureResourceTamperIncident(auditLog)
			if persistErr != nil {
				return nil, &IncidentPersistenceError{
					LogID: auditLog.LogID, IncidentType: incidentTypeForResourceTamper(auditLog), Cause: persistErr,
				}
			}
			item.IncidentID = incident.ID
			item.IncidentScope = incident.IncidentScope
		}
		results = append(results, item)
	}
	if err := s.enrichRecoveryStatuses(clientID, results); err != nil {
		return nil, err
	}

	return results, nil
}

func incidentTypeForResourceTamper(auditLog models.AuditLog) string {
	logCopy := auditLog
	canonicalizeLog(&logCopy)
	if hasher.GenerateLogHash(&logCopy) != auditLog.HashValue {
		return "METADATA_HASH_MISMATCH"
	}
	return "MERKLE_ROOT_MISMATCH"
}

func (s *auditService) ensureResourceTamperIncident(auditLog models.AuditLog) (*models.TamperIncident, error) {
	logCopy := auditLog
	canonicalizeLog(&logCopy)
	return s.ensureTamperIncident(auditLog, incidentTypeForResourceTamper(auditLog), hasher.GenerateLogHash(&logCopy))
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

// proofsForAnchorRoot narrows legacy hash-only proof lookups to the batch root
// recorded by Fabric whenever proof rows carry a root. MerkleProof has no
// tenant column, so this uses the existing root field without a schema change.
// If no row identifies its root, retain the legacy rows and let the caller
// classify the evidence as legacy-limited or unavailable.
func proofsForAnchorRoot(proofs []models.MerkleProof, anchorRoot string) []models.MerkleProof {
	anchorRoot = strings.TrimSpace(anchorRoot)
	if anchorRoot == "" || len(proofs) == 0 {
		return proofs
	}

	var matching []models.MerkleProof
	for _, proof := range proofs {
		if strings.TrimSpace(proof.MerkleRoot) == anchorRoot {
			matching = append(matching, proof)
		}
	}
	if len(matching) > 0 {
		return matching
	}
	return proofs
}

// classifyResourceLog menjalankan Layer 2+4 (rehash + Merkle vs Fabric) untuk
// SEMUA log, tapi Layer 3 (Agent, live data klien) HANYA untuk event client
// terbaru. RECOVERY adalah event internal Gateway: tetap dapat menjadi latest
// secara kronologis, tetapi tidak mengambil posisi latest client event.
func (s *auditService) classifyResourceLog(auditLog models.AuditLog, isLatest, isLatestClientEvent bool) ResourceLogVerification {
	baseStatus, proofStatus := s.classifyIntegrityDetailed(auditLog)
	integrityStatus, chainStatus := resourceGatewayStatuses(baseStatus, "skipped_historical")

	item := ResourceLogVerification{
		LogID:               auditLog.LogID,
		Resource:            auditLog.Resource,
		Action:              auditLog.Action,
		LastAction:          auditLog.Action,
		Actor:               auditLog.Actor,
		Timestamp:           formatPgTimestamp(auditLog.Timestamp),
		LastUpdatedAt:       formatPgTimestamp(auditLog.Timestamp),
		HashValue:           auditLog.HashValue,
		IntegrityStatus:     integrityStatus,
		ChainStatus:         chainStatus,
		ProofStatus:         proofStatus,
		Assessment:          verificationAssessment(proofStatus, "", "skipped_historical", ""),
		RecoveryStatus:      recoveryDisplayNotRecovered,
		IsLatest:            isLatest,
		IsLatestClientEvent: isLatestClientEvent,
		AgentStatus:         "skipped_historical",
	}

	if isRecoveryAction(auditLog.Action) {
		item.AgentStatus = "skipped_recovery"
		item.Assessment = verificationAssessment(proofStatus, "", item.AgentStatus, "")
		if requestID := recoveryRequestIDFromAuthorizationContext(auditLog.AuthorizationContext); requestID != "" {
			item.RecoveryStatus = recoveryDisplayRecovered
			item.RecoveryRequestID = requestID
		}
		return item
	}
	if !shouldVerifyResourceWithAgent(auditLog, isLatestClientEvent) {
		return item
	}

	item.AgentStatus = "not_configured"

	logCopy := auditLog
	agentResult, err := s.agent.VerifyAgainstAgent(&logCopy)
	if err != nil {
		item.AgentStatus, item.SourceStatus = agentFailureStatuses(err)
	} else if agentResult != nil && agentResult.AgentUsed {
		item.StateComparison = agentResult.StateComparison
		item.ComparisonBasis = agentResult.ComparisonBasis
		item.OffchainStateHash = agentResult.OffchainStateHash
		item.ClientStateHash = agentResult.ClientStateHash
		item.SourceCheckedAt = agentResult.SourceCheckedAt
		item.SourceStateHash = agentResult.ClientStateHash
		item.AuditTrailStatus = agentResult.AuditTrailStatus
		if !agentResult.StateComparable {
			item.AgentStatus = "not_comparable"
			item.SourceStatus = models.SourceStatusNotComparable
		} else if agentResult.IsMatch {
			item.AgentStatus = "matched"
			item.SourceStatus = models.SourceStatusMatched
		} else {
			item.AgentStatus = "mismatch"
			item.SourceStatus = sourceStatusFromAgentResult(auditLog, agentResult)
			item.AgentDiscrepancies = agentResult.Discrepancies
			if baseStatus == "valid" && shouldRecordClientSourceIncident(proofStatus, item.SourceStatus) {
				s.recordClientSourceIncident(auditLog, item.SourceStatus, agentResult.ClientStateHash, agentResult.Discrepancies)
			}
		}
	} else {
		item.SourceStatus = models.SourceStatusNotComparable
	}

	// AgentStatus hanya menjelaskan verifikasi Layer 3 terhadap database
	// operasional client. Jangan pernah menurunkan status integritas/chain
	// Gateway ketika Agent sedang offline; validasi Gateway/Fabric tetap sah.
	item.IntegrityStatus, item.ChainStatus = resourceGatewayStatuses(baseStatus, item.AgentStatus)
	item.Assessment = verificationAssessment(proofStatus, item.SourceStatus, item.AgentStatus, item.AuditTrailStatus)

	return item
}

const (
	recoveryDisplayNotRecovered = "not_recovered"
	recoveryDisplayPending      = "pending"
	recoveryDisplayRecovered    = "recovered"
	recoveryDisplayFailed       = "failed"
)

// resourceGatewayStatuses memetakan hasil verifikasi PostgreSQL Gateway dan
// Fabric ke dua field kompatibilitas API. AgentStatus sengaja tidak menjadi
// input fungsi ini: Agent offline adalah kondisi client, bukan bukti bahwa
// snapshot atau anchor Gateway rusak. Parameter agentStatus sengaja diterima
// untuk menegaskan kontrak bahwa nilainya tidak pernah mengubah hasil.
func resourceGatewayStatuses(baseStatus, _ string) (integrityStatus, chainStatus string) {
	switch baseStatus {
	case "tampered":
		return "tampered", "tampered"
	case "unreachable":
		return "unreachable", "unreachable"
	case "pending":
		return "pending", "pending"
	default:
		return "valid", "valid"
	}
}

func recoveryDisplayStatus(status string) string {
	switch status {
	case models.RecoveryStatusSucceeded:
		return recoveryDisplayRecovered
	case models.RecoveryStatusPendingExecution,
		models.RecoveryStatusPendingApproval,
		models.RecoveryStatusApproved,
		models.RecoveryStatusExecuting:
		return recoveryDisplayPending
	case models.RecoveryStatusRejected,
		models.RecoveryStatusFailedVerification,
		models.RecoveryStatusFailedExecution:
		return recoveryDisplayFailed
	default:
		return recoveryDisplayNotRecovered
	}
}

// shouldVerifyResourceWithAgent hanya mengizinkan Layer 3 untuk event
// operasional client terbaru. Event RECOVERY dibuat oleh Gateway sendiri,
// sehingga tidak boleh dibandingkan dengan Agent client.
func shouldVerifyResourceWithAgent(auditLog models.AuditLog, isLatestClientEvent bool) bool {
	return isLatestClientEvent && !isRecoveryAction(auditLog.Action)
}

func isRecoveryAction(action string) bool {
	return strings.EqualFold(strings.TrimSpace(action), "RECOVERY")
}

// latestClientEventIndex receives logs ordered by timestamp ascending and
// returns the newest client-originated event. Gateway RECOVERY events do not
// replace the client event used for live source verification.
func latestClientEventIndex(logs []models.AuditLog) int {
	for i := len(logs) - 1; i >= 0; i-- {
		if !isRecoveryAction(logs[i].Action) {
			return i
		}
	}
	return -1
}

func recoveryRequestIDFromAuthorizationContext(authorizationContext string) string {
	const prefix = "recovery_request:"
	if !strings.HasPrefix(strings.TrimSpace(authorizationContext), prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(authorizationContext), prefix))
}

// enrichRecoveryStatuses mengambil workflow recovery terbaru untuk setiap log
// dalam satu query. Dengan begitu endpoint riwayat tidak melakukan N+1 query
// dan frontend dapat menampilkan badge RECOVERED tanpa mencampurnya dengan
// status konektivitas Agent.
func (s *auditService) enrichRecoveryStatuses(clientID string, items []ResourceLogVerification) error {
	if len(items) == 0 {
		return nil
	}

	logIDs := make([]string, 0, len(items))
	seenLogIDs := make(map[string]struct{}, len(items))
	for i := range items {
		items[i].RecoveryStatus = recoveryDisplayNotRecovered
		if strings.EqualFold(strings.TrimSpace(items[i].Action), "RECOVERY") && items[i].RecoveryRequestID != "" {
			items[i].RecoveryStatus = recoveryDisplayRecovered
		}
		if items[i].LogID == "" {
			continue
		}
		if _, exists := seenLogIDs[items[i].LogID]; exists {
			continue
		}
		seenLogIDs[items[i].LogID] = struct{}{}
		logIDs = append(logIDs, items[i].LogID)
	}
	if s.db == nil || len(logIDs) == 0 {
		return nil
	}

	var requests []models.RecoveryRequest
	if err := s.db.Where("client_id = ? AND target_log_id IN ?", clientID, logIDs).
		Order("requested_at DESC").Find(&requests).Error; err != nil {
		return err
	}

	itemByLogID := make(map[string]int, len(items))
	for i := range items {
		if _, exists := itemByLogID[items[i].LogID]; !exists {
			itemByLogID[items[i].LogID] = i
		}
	}
	latestRequestByLogID := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		index, exists := itemByLogID[request.TargetLogID]
		if !exists {
			continue
		}
		if _, alreadySet := latestRequestByLogID[request.TargetLogID]; alreadySet {
			continue
		}
		latestRequestByLogID[request.TargetLogID] = struct{}{}
		items[index].RecoveryStatus = recoveryDisplayStatus(request.Status)
		items[index].RecoveryRequestID = request.ID
		items[index].RecoveryExecutedAt = request.ExecutedAt
	}
	return nil
}

type AgentAuditReportRequest struct {
	RunID              string                `json:"run_id"`
	From               time.Time             `json:"from"`
	To                 time.Time             `json:"to"`
	TotalLogs          int                   `json:"total_logs"`
	AuditedLogs        int                   `json:"audited_logs"`
	HistoricalSkipped  int                   `json:"historical_skipped"`
	DataValid          int                   `json:"data_valid"`
	DataTampered       int                   `json:"data_tampered"`
	DataNotFound       int                   `json:"data_not_found"`
	DataError          int                   `json:"data_error"`
	BlockchainValid    int                   `json:"blockchain_valid"`
	BlockchainTampered int                   `json:"blockchain_tampered"`
	BlockchainPending  int                   `json:"blockchain_pending"`
	BlockchainSkipped  int                   `json:"blockchain_skipped"`
	BlockchainError    int                   `json:"blockchain_error"`
	AutoRecoveredCount int                   `json:"auto_recovered_count"`
	Duration           string                `json:"duration"`
	Results            []AgentAuditLogResult `json:"results"`
}

type AgentAuditDiscrepancy struct {
	Field    string      `json:"field"`
	Expected interface{} `json:"expected"`
	Actual   interface{} `json:"actual"`
}

type AgentAuditLogResult struct {
	LogID            string                  `json:"log_id"`
	Resource         string                  `json:"resource"`
	Action           string                  `json:"action"`
	DataStatus       string                  `json:"data_status"`       // VALID | TAMPERED | NOT_FOUND | ERROR
	BlockchainStatus string                  `json:"blockchain_status"` // VALID | TAMPERED | PENDING | SKIPPED | ERROR
	ClientHash       string                  `json:"client_hash,omitempty"`
	OffchainHash     string                  `json:"offchain_hash,omitempty"`
	MerkleRoot       string                  `json:"merkle_root,omitempty"`
	BlockchainTxID   string                  `json:"blockchain_tx_id,omitempty"`
	Discrepancies    []AgentAuditDiscrepancy `json:"discrepancies,omitempty"`
	AutoRecovered    bool                    `json:"auto_recovered,omitempty"`
	RecoveryError    string                  `json:"recovery_error,omitempty"`
	Error            string                  `json:"error,omitempty"`
}


func (s *auditService) ProcessAgentAuditReport(clientID string, req AgentAuditReportRequest) error {
	if s.db == nil {
		return errors.New("database not available")
	}
	if clientID == "" {
		return errors.New("client_id cannot be empty")
	}

	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		runID = uuid.New().String()
	}
	var existingRun models.VerificationRun
	if err := s.db.Where("id = ? AND client_id = ?", runID, clientID).First(&existingRun).Error; err == nil {
		return nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("check duplicate Agent audit run: %w", err)
	}

	// Agent results are evidence for a recheck, not sufficient authorization to
	// create a recovery incident. Re-run Gateway/Fabric/Agent verification only
	// for rows the Agent reports as anomalous; its existing verifier records
	// client-source and Gateway-integrity findings in separate scopes.
	verificationErrors := make(map[string]error)
	for _, item := range req.Results {
		chainStatus := strings.ToUpper(strings.TrimSpace(item.BlockchainStatus))
		if strings.TrimSpace(item.LogID) == "" || chainStatus != "TAMPERED" {
			continue
		}
		if _, verifyErr := s.VerifyLogIntegrity(item.LogID, clientID); verifyErr != nil {
			verificationErrors[item.LogID] = verifyErr
			log.Printf("[AgentAuditReport] Gateway recheck inconclusive client=%s log=%s: %v", clientID, item.LogID, verifyErr)
		}
	}

	now := time.Now().UTC()
	transactionErr := s.db.Transaction(func(tx *gorm.DB) error {
		var duplicate models.VerificationRun
		if err := tx.Where("id = ? AND client_id = ?", runID, clientID).First(&duplicate).Error; err == nil {
			return nil
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("check duplicate Agent audit run: %w", err)
		}

		var stat models.ClientDashboardStats
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("client_id = ?", clientID).First(&stat).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				stat = models.ClientDashboardStats{
					ClientID: clientID,
				}
				if err := tx.Create(&stat).Error; err != nil {
					return fmt.Errorf("create client stats: %w", err)
				}
			} else {
				return fmt.Errorf("find client stats: %w", err)
			}
		}

		var tableResults map[string]rangeTableSummary
		if err := json.Unmarshal([]byte(stat.TableVerifyResults), &tableResults); err != nil || tableResults == nil {
			tableResults = make(map[string]rangeTableSummary)
		}

		totalValid := int64(0)
		totalTampered := int64(0)
		totalPending := int64(0)

		if len(req.Results) > 0 {
			for _, item := range req.Results {
				sourceStatus := ""
				if strings.TrimSpace(item.LogID) != "" {
					status := models.IntegrityStatusUnreachable
					dataStatus := strings.ToUpper(strings.TrimSpace(item.DataStatus))
					chainStatus := strings.ToUpper(strings.TrimSpace(item.BlockchainStatus))
					needsRecheck := dataStatus == "TAMPERED" || dataStatus == "NOT_FOUND" || chainStatus == "TAMPERED"
					if needsRecheck {
						var current models.AuditLog
						if err := tx.Where("log_id = ? AND client_id = ?", item.LogID, clientID).First(&current).Error; err == nil {
							status = strings.ToUpper(strings.TrimSpace(current.IntegrityStatus))
							if status == "" || status == models.IntegrityStatusNotChecked {
								if verificationErrors[item.LogID] != nil {
									status = models.IntegrityStatusUnreachable
								} else {
									status = models.IntegrityStatusUnreachable
								}
							}
							sourceStatus = strings.ToUpper(strings.TrimSpace(current.SourceIntegrityStatus))
							if sourceStatus == "" {
								sourceStatus = models.IntegrityStatusNotChecked
							}
						}
					} else if dataStatus == "VALID" && (chainStatus == "VALID" || chainStatus == "SKIPPED") {
						status = models.IntegrityStatusValid
						sourceStatus = models.IntegrityStatusValid
					} else if chainStatus == "PENDING" || dataStatus == "PENDING" {
						status = models.IntegrityStatusPending
					} else if dataStatus == "ERROR" || chainStatus == "ERROR" {
						status = models.IntegrityStatusUnreachable
					}
					updates := map[string]interface{}{
						"integrity_status": status, "integrity_checked_at": now,
						"integrity_error": "", "integrity_source": models.IntegritySourceAgentAudit,
						"integrity_run_id": runID,
					}
					if sourceStatus != "" {
						updates["source_integrity_status"] = sourceStatus
						updates["source_integrity_checked_at"] = now
					}
					if verificationErrors[item.LogID] != nil {
						updates["integrity_error"] = "Gateway recheck did not complete"
					}
					if err := tx.Model(&models.AuditLog{}).Where("log_id = ? AND client_id = ?", item.LogID, clientID).Updates(updates).Error; err != nil {
						return fmt.Errorf("update Agent audit log status: %w", err)
					}
				}

				// Track Client Data Tampering (comparing actual client DB hash vs offchain log hash)
				if strings.EqualFold(item.DataStatus, "TAMPERED") {
					sourceStatus = models.IntegrityStatusTampered
					resParts := strings.SplitN(strings.TrimSpace(item.Resource), ":", 2)
					recTable := "UNKNOWN"
					recID := ""
					if len(resParts) == 2 {
						recTable = strings.TrimSpace(resParts[0])
						recID = strings.TrimSpace(resParts[1])
					} else if len(resParts) == 1 {
						recTable = strings.TrimSpace(resParts[0])
					}
					discJSON, _ := json.Marshal(item.Discrepancies)

					var existingTamper models.ClientDataTamperRecord
					findErr := tx.Where("client_id = ? AND resource = ? AND status = ?", clientID, item.Resource, models.ClientTamperStatusOpen).
						First(&existingTamper).Error
					if findErr == nil {
						upd := map[string]interface{}{
							"run_id":            runID,
							"log_id":            item.LogID,
							"expected_hash":     item.OffchainHash,
							"actual_hash":       item.ClientHash,
							"discrepancies":     string(discJSON),
							"blockchain_tx_id":  item.BlockchainTxID,
							"last_confirmed_at": &now,
							"recovery_error":    item.RecoveryError,
						}
						if item.AutoRecovered {
							upd["status"] = models.ClientTamperStatusRecovered
							upd["auto_recovered"] = true
							upd["resolved_at"] = &now
							upd["resolved_by"] = "agent_auto_recovery"
						}
						_ = tx.Model(&existingTamper).Updates(upd).Error
					} else if errors.Is(findErr, gorm.ErrRecordNotFound) {
						newRec := models.ClientDataTamperRecord{
							ID:              uuid.New().String(),
							ClientID:        clientID,
							RunID:           runID,
							Resource:        item.Resource,
							TableName:       recTable,
							RecordID:        recID,
							LogID:           item.LogID,
							Action:          item.Action,
							ExpectedHash:    item.OffchainHash,
							ActualHash:      item.ClientHash,
							Discrepancies:   string(discJSON),
							BlockchainTxID:  item.BlockchainTxID,
							RecoveryError:   item.RecoveryError,
							DetectedAt:      now,
							LastConfirmedAt: &now,
						}
						if item.AutoRecovered {
							newRec.Status = models.ClientTamperStatusRecovered
							newRec.AutoRecovered = true
							newRec.ResolvedAt = &now
							newRec.ResolvedBy = "agent_auto_recovery"
						} else {
							newRec.Status = models.ClientTamperStatusOpen
						}
						_ = tx.Create(&newRec).Error
					}
				} else if strings.EqualFold(item.DataStatus, "VALID") {
					sourceStatus = models.IntegrityStatusValid
					_ = tx.Model(&models.ClientDataTamperRecord{}).
						Where("client_id = ? AND resource = ? AND status = ?", clientID, item.Resource, models.ClientTamperStatusOpen).
						Updates(map[string]interface{}{
							"status":            models.ClientTamperStatusRecovered,
							"resolved_at":       &now,
							"resolved_by":       "agent_audit_verified",
							"last_confirmed_at": &now,
						}).Error
				}

				tableName := strings.TrimSpace(strings.SplitN(item.Resource, ":", 2)[0])
				if tableName == "" {
					tableName = "UNKNOWN"
				}

				table := tableResults[tableName]
				table.Total++

				isDataValid := strings.EqualFold(item.DataStatus, "VALID")
				isChainValid := strings.EqualFold(item.BlockchainStatus, "VALID") || strings.EqualFold(item.BlockchainStatus, "SKIPPED")
				isTampered := strings.EqualFold(item.DataStatus, "TAMPERED") || strings.EqualFold(item.DataStatus, "NOT_FOUND") || strings.EqualFold(item.BlockchainStatus, "TAMPERED")
				isPending := strings.EqualFold(item.BlockchainStatus, "PENDING")

				if isDataValid && isChainValid {
					totalValid++
					table.Valid++
				} else if isTampered {
					totalTampered++
					table.Invalid++
				} else if isPending {
					totalPending++
					table.Pending++
				} else {
					table.Unavailable++
				}

				tableResults[tableName] = table
			}
		} else {
			totalValid = int64(req.DataValid)
			totalTampered = int64(req.DataTampered + req.BlockchainTampered)
			totalPending = int64(req.BlockchainPending)
		}

		newJSON, _ := json.Marshal(tableResults)
		auditedCount := int64(req.AuditedLogs)
		if auditedCount == 0 && (totalValid > 0 || totalTampered > 0 || totalPending > 0) {
			auditedCount = totalValid + totalTampered + totalPending
		}

		newTotalVerifications := stat.TotalVerifications + auditedCount
		newTotalValid := stat.TotalValid + totalValid
		newTotalTampered := stat.TotalTampered + totalTampered
		newIntegrityScore := float64(0)
		if newTotalVerifications > 0 {
			newIntegrityScore = float64(newTotalValid) / float64(newTotalVerifications) * 100
		}

		fromTime := req.From
		toTime := req.To
		if fromTime.IsZero() {
			fromTime = now.Add(-24 * time.Hour)
		}
		if toTime.IsZero() {
			toTime = now
		}

		vRun := models.VerificationRun{
			ID: runID, ClientID: clientID, RequestedBy: "agent",
			FromTime: fromTime, ToTime: toTime, Status: models.VerificationRunCompleted,
			BatchSize: int(auditedCount), TotalItems: int64(req.TotalLogs), ProcessedItems: auditedCount,
			TotalValid: totalValid, TotalInvalid: totalTampered, TotalPending: totalPending,
			AlreadyVerified: int64(req.HistoricalSkipped), VerifiedNow: auditedCount,
			StartedAt: &fromTime, CompletedAt: &now,
		}
		if err := tx.Create(&vRun).Error; err != nil {
			return fmt.Errorf("create Agent verification run: %w", err)
		}

		updates := map[string]interface{}{
			"last_verified_at":            now,
			"last_verified_table":         "MULTIPLE (Agent Audit)",
			"total_verifications":         newTotalVerifications,
			"total_rows_verified":         gorm.Expr("total_rows_verified + ?", auditedCount),
			"total_valid":                 newTotalValid,
			"total_tampered":              newTotalTampered,
			"total_verify_pending":        stat.TotalVerifyPending + totalPending,
			"integrity_score":             newIntegrityScore,
			"table_verify_results":        string(newJSON),
			"last_integrity_check_at":     now,
			"last_integrity_check_source": "agent",
			"last_integrity_check_run_id": runID,
			"last_integrity_check_logs":   auditedCount,
		}
		if req.AutoRecoveredCount > 0 {
			updates["total_recoveries"] = gorm.Expr("total_recoveries + ?", req.AutoRecoveredCount)
		}
		if totalTampered > 0 {
			updates["total_tamper_incidents"] = gorm.Expr("total_tamper_incidents + ?", totalTampered)
			unrecovered := totalTampered - int64(req.AutoRecoveredCount)
			if unrecovered > 0 {
				updates["open_tamper_incidents"] = gorm.Expr("open_tamper_incidents + ?", unrecovered)
			}
		}

		if err := tx.Model(&stat).Updates(updates).Error; err != nil {
			return fmt.Errorf("update client stats: %w", err)
		}

		return nil
	})
	if transactionErr != nil {
		// The asynchronous callback and the synchronous Portal request can race.
		// A winner commits the unique run and all counters in one transaction;
		// a duplicate loser is an idempotent success.
		var committed models.VerificationRun
		if err := s.db.Where("id = ? AND client_id = ?", runID, clientID).First(&committed).Error; err == nil {
			return nil
		}
		return transactionErr
	}
	return nil
}
