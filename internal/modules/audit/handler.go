package audit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/middleware"
	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Handler struct {
	Service          Service
	VerificationJobs *VerificationJobService
	AgentAuditRunner interface {
		RunAudit(context.Context, string, time.Time, time.Time) (*agentverifier.AuditRunReport, error)
	}
}

const (
	defaultAuditLogPageSize = 10
	maxAuditLogPageSize     = 100
)

func NewHandler(service Service) *Handler {
	return &Handler{Service: service}
}

func NewHandlerWithVerificationJobs(service Service, jobs *VerificationJobService) *Handler {
	return &Handler{Service: service, VerificationJobs: jobs}
}

func (h *Handler) SetAgentAuditRunner(runner interface {
	RunAudit(context.Context, string, time.Time, time.Time) (*agentverifier.AuditRunReport, error)
}) {
	h.AgentAuditRunner = runner
}

type agentAuditRunInput struct {
	From string `json:"from" binding:"required"`
	To   string `json:"to" binding:"required"`
}

// RunAgentAudit runs the tenant's configured Agent audit and returns only its
// aggregate summary. Per-log rows and client payloads remain server-side.
// @Summary Run Agent integrity audit
// @Description Runs a tenant-scoped Agent audit for a range of at most one hour and up to 100 Gateway logs.
// @Tags Dashboard
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body agentAuditRunInput true "Audit time range"
// @Success 200 {object} map[string]interface{}
// @Failure 422 {object} map[string]interface{} "Range exceeds the synchronous 100-log limit"
// @Router /dashboard/agent/audit-run [post]
func (h *Handler) RunAgentAudit(c *gin.Context) {
	clientIDValue, exists := c.Get("client_id")
	clientID, ok := clientIDValue.(string)
	if !exists || !ok || strings.TrimSpace(clientID) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas client tidak ditemukan pada token."})
		return
	}
	if h.AgentAuditRunner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Agent audit belum dikonfigurasi."})
		return
	}
	var input agentAuditRunInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Body wajib memuat from dan to dalam format RFC3339."})
		return
	}
	from, err := parseTimeRobust(strings.TrimSpace(input.From))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format from tidak valid. Gunakan RFC3339."})
		return
	}
	to, err := parseTimeRobust(strings.TrimSpace(input.To))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format to tidak valid. Gunakan RFC3339."})
		return
	}
	if !to.After(from) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "to harus setelah from."})
		return
	}
	if to.Sub(from) > time.Hour {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Rentang Agent audit maksimal satu jam.", "code": "audit_range_too_large"})
		return
	}
	if from.After(time.Now().Add(time.Minute)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Rentang audit tidak boleh berada di masa depan."})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 70*time.Second)
	defer cancel()
	report, err := h.AgentAuditRunner.RunAudit(ctx, clientID, from.UTC(), to.UTC())
	if err != nil {
		var limitErr *agentverifier.AuditRangeLimitError
		if errors.As(err, &limitErr) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"error": "Rentang memuat terlalu banyak log untuk audit sinkron. Persempit rentang waktunya.",
				"code":  "audit_log_limit_exceeded", "estimated_logs": limitErr.EstimatedLogs, "sync_limit": limitErr.Limit,
			})
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": "Agent audit gagal dijalankan.", "code": "agent_audit_failed"})
		return
	}

	request := AgentAuditReportRequest{
		RunID: report.RunID, From: report.From, To: report.To,
		TotalLogs: report.TotalLogs, AuditedLogs: report.AuditedLogs, HistoricalSkipped: report.HistoricalSkipped,
		DataValid: report.DataValid, DataTampered: report.DataTampered, DataNotFound: report.DataNotFound, DataError: report.DataError,
		BlockchainValid: report.BlockchainValid, BlockchainTampered: report.BlockchainTampered,
		BlockchainPending: report.BlockchainPending, BlockchainSkipped: report.BlockchainSkipped,
		BlockchainError: report.BlockchainError, AutoRecoveredCount: report.AutoRecoveredCount, Duration: report.Duration,
		Results: make([]AgentAuditLogResult, 0, len(report.Results)),
	}
	for _, item := range report.Results {
		request.Results = append(request.Results, AgentAuditLogResult{
			LogID: item.LogID, Resource: item.Resource, Action: item.Action,
			DataStatus: item.DataStatus, BlockchainStatus: item.BlockchainStatus,
			AutoRecovered: item.AutoRecovered,
		})
	}
	if err := h.Service.ProcessAgentAuditReport(clientID, request); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Hasil Agent audit gagal disimpan di Gateway.", "code": "agent_audit_persist_failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"run_id": report.RunID, "from": report.From, "to": report.To,
		"total_logs": report.TotalLogs, "audited_logs": report.AuditedLogs,
		"historical_skipped": report.HistoricalSkipped, "data_valid": report.DataValid,
		"data_tampered": report.DataTampered, "data_not_found": report.DataNotFound,
		"data_error": report.DataError, "blockchain_valid": report.BlockchainValid,
		"blockchain_tampered": report.BlockchainTampered, "blockchain_pending": report.BlockchainPending,
		"blockchain_error": report.BlockchainError, "duration": report.Duration,
	}})
}

func normalizeAuditLogPageSize(raw string) int {
	if raw == "" {
		return defaultAuditLogPageSize
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return defaultAuditLogPageSize
	}
	if parsed > maxAuditLogPageSize {
		return maxAuditLogPageSize
	}
	return parsed
}

func (h *Handler) getClientID(c *gin.Context) (string, bool) {
	clientIDVal, exists := c.Get("client_id")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas client tidak ditemukan pada token."})
		return "", false
	}
	clientID, ok := clientIDVal.(string)
	if !ok || clientID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas client pada token tidak valid."})
		return "", false
	}

	// Jika user adalah admin, izinkan override client_id via query param
	roleVal, hasRole := c.Get("role")
	if hasRole {
		roleStr, okRole := roleVal.(string)
		if okRole && strings.ToLower(roleStr) == "admin" {
			if queryClientID := c.Query("client_id"); queryClientID != "" {
				return queryClientID, true
			}
		}
	}

	return clientID, true
}

type ErrorResponse struct {
	Error string `json:"error" example:"Pesan kesalahan atau validasi"`
}

type VerifyLogData struct {
	LogID                   string      `json:"log_id"`
	IsValid                 bool        `json:"is_valid"`
	Message                 string      `json:"message"`
	VerificationAssessment  string      `json:"verification_assessment,omitempty"`
	ProofStatus             string      `json:"proof_status,omitempty"`
	IncidentID              string      `json:"incident_id,omitempty"`
	IncidentScope           string      `json:"incident_scope,omitempty"`
	IncidentType            string      `json:"incident_type,omitempty"`
	IncidentStatus          string      `json:"incident_status,omitempty"`
	ExpectedHash            string      `json:"expected_hash,omitempty"`
	ActualHash              string      `json:"actual_hash,omitempty"`
	DBRoot                  string      `json:"merkle_root,omitempty"`
	ChainRoot               string      `json:"blockchain_tx_id,omitempty"`
	AgentStatus             string      `json:"agent_status,omitempty"`
	SourceStatus            string      `json:"source_status,omitempty"`
	StateComparison         string      `json:"state_comparison,omitempty"`
	ComparisonBasis         string      `json:"comparison_basis,omitempty"`
	OffchainStateHash       string      `json:"offchain_state_hash,omitempty"`
	ClientStateHash         string      `json:"client_state_hash,omitempty"`
	SourceCheckedAt         *time.Time  `json:"source_checked_at,omitempty"`
	AuditTrailStatus        string      `json:"audit_trail_status,omitempty"`
	AgentDiscrepancies      interface{} `json:"agent_discrepancies,omitempty"`
	AuditTrailDiscrepancies interface{} `json:"audit_trail_discrepancies,omitempty"`
}

type IncidentPersistenceErrorResponse struct {
	Code               string                 `json:"code" example:"INCIDENT_PERSISTENCE_FAILED"`
	Error              string                 `json:"error"`
	LogID              string                 `json:"log_id,omitempty"`
	IncidentScope      string                 `json:"incident_scope,omitempty"`
	IncidentType       string                 `json:"incident_type,omitempty"`
	VerificationStatus string                 `json:"verification_status,omitempty"`
	Status             string                 `json:"status,omitempty"`
	Layer              string                 `json:"layer,omitempty"`
	Data               map[string]interface{} `json:"data,omitempty"`
}

type VerifyLogResponse struct {
	Status  string        `json:"status" example:"success"`
	Code    string        `json:"code,omitempty" example:"INTEGRITY_TAMPERED"`
	Layer   string        `json:"layer,omitempty" example:"4_blockchain"`
	Data    VerifyLogData `json:"data,omitempty"`
	LogID   string        `json:"log_id,omitempty"`
	Message string        `json:"message,omitempty"`
}

type RecentLogsResponse struct {
	Data       []interface{} `json:"data"` // Array of logs
	Pagination struct {
		Page       int `json:"page"`
		PageSize   int `json:"page_size"`
		TotalRows  int `json:"total_rows"`
		TotalPages int `json:"total_pages"`
	} `json:"pagination"`
	Note string `json:"note,omitempty"`
}

type VerifyRangeEstimateResponse struct {
	EstimatedItems int64 `json:"estimated_items"`
	SyncLimit      int   `json:"sync_limit"`
	CanVerifySync  bool  `json:"can_verify_sync"`
}

type ClientRangeVerificationResponse struct {
	Range   RangeInfo    `json:"range"`
	Summary RangeSummary `json:"summary"`
}

type CreateVerificationRunRequest struct {
	From      string `json:"from" binding:"required" example:"2026-09-29T00:00:00Z"`
	To        string `json:"to" binding:"required" example:"2026-09-30T23:59:59Z"`
	BatchSize int    `json:"batch_size,omitempty" example:"100"`
}

type VerificationRunResponseEnvelope struct {
	Message string                  `json:"message,omitempty"`
	Data    VerificationRunResponse `json:"data"`
}

type LatestVerificationRunResponse struct {
	Data *VerificationRunResponse `json:"data"`
}

func incidentPersistenceErrorPayload(err error) gin.H {
	payload := gin.H{
		"code":  "INCIDENT_PERSISTENCE_FAILED",
		"error": "The gateway could not save the detected recovery incident. Retry after the database issue is resolved.",
	}
	var persistenceErr *IncidentPersistenceError
	if errors.As(err, &persistenceErr) {
		if persistenceErr.LogID != "" {
			payload["log_id"] = persistenceErr.LogID
		}
		if persistenceErr.IncidentType != "" {
			payload["incident_type"] = persistenceErr.IncidentType
		}
		if persistenceErr.IncidentScope != "" {
			payload["incident_scope"] = persistenceErr.IncidentScope
		}
	}
	return payload
}

func verificationLayer(status string) string {
	switch status {
	case "failed_local":
		return "2_local_hash"
	case "failed_source":
		return "3_agent_source"
	case "failed_onchain":
		return "4_blockchain"
	default:
		return "integrity"
	}
}

// @Summary Verify a specific log
// @Description Memverifikasi integritas satu log tertentu (Lapis 2, 3, dan 4).
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param log_id path string true "ID Log"
// @Success 200 {object} VerifyLogResponse "Verifikasi sukses dan log valid"
// @Success 202 {object} VerifyLogResponse "Verifikasi pending/dalam proses"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Log tidak ditemukan"
// @Failure 409 {object} VerifyLogResponse "Verifikasi gagal/tampered pada suatu layer"
// @Failure 500 {object} IncidentPersistenceErrorResponse "Kesalahan sistem atau penyimpanan incident saat verifikasi"
// @Router /dashboard/verify/{log_id} [get]
func (h *Handler) VerifyLog(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	// Parameter sekarang adalah log_id, bukan hash
	logID := c.Param("log_id")

	result, err := h.Service.VerifyLogIntegrity(logID, clientID)
	if err != nil {
		if errors.Is(err, errIncidentPersistence) {
			payload := incidentPersistenceErrorPayload(err)
			if result != nil {
				payload["verification_status"] = result.Status
				if strings.HasPrefix(result.Status, "failed_") {
					payload["status"] = "failed"
					payload["layer"] = verificationLayer(result.Status)
				}
				payload["data"] = gin.H{
					"log_id":                  result.LogID,
					"is_valid":                result.IsValid,
					"message":                 result.Message,
					"verification_assessment": result.Assessment,
					"proof_status":            result.ProofStatus,
					"source_status":           result.SourceStatus,
					"expected_hash":           result.ExpectedHash,
					"actual_hash":             result.ActualHash,
					"db_root":                 result.DBRoot,
					"chain_root":              result.ChainRoot,
				}
			}
			c.JSON(http.StatusInternalServerError, payload)
			return
		}
		switch err.Error() {
		case "log_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Log tidak ditemukan."})
		case "agent_error":
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "Gagal menghubungi Agent klien untuk verifikasi Lapis 3.",
				"hint":  "Periksa konektivitas Agent via GET /api/dashboard/agent/ping",
			})
		case "fabric_error":
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal terhubung ke Blockchain Fabric."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Kesalahan sistem saat verifikasi."})
		}
		return
	}

	switch result.Status {
	case "failed_local":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "code": "INTEGRITY_TAMPERED", "layer": "2_local_hash",
			"data": gin.H{
				"is_valid": result.IsValid, "message": result.Message,
				"verification_assessment": result.Assessment,
				"proof_status":            result.ProofStatus,
				"log_id":                  result.LogID,
				"incident_id":             result.IncidentID,
				"incident_scope":          result.IncidentScope,
				"incident_type":           result.IncidentType,
				"incident_status":         result.IncidentStatus,
				"expected_hash":           result.ExpectedHash, "actual_hash": result.ActualHash,
			},
		})
	case "failed_source":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "code": "CLIENT_SOURCE_MISMATCH", "layer": "3_agent_source",
			"data": gin.H{
				"is_valid":                result.IsValid,
				"verification_assessment": result.Assessment,
				"proof_status":            result.ProofStatus,
				"log_id":                  result.LogID,
				"message":                 result.Message,
				"incident_id":             result.IncidentID,
				"incident_scope":          result.IncidentScope,
				"incident_type":           result.IncidentType,
				"incident_status":         result.IncidentStatus,
			},
		})
	case "pending":
		c.JSON(http.StatusAccepted, gin.H{
			"status":                  "pending",
			"verification_assessment": result.Assessment,
			"proof_status":            result.ProofStatus,
			"log_id":                  result.LogID,
			"message":                 result.Message,
		})
	case "inconclusive":
		c.JSON(http.StatusAccepted, gin.H{
			"status":                  "inconclusive",
			"verification_assessment": result.Assessment,
			"proof_status":            result.ProofStatus,
			"log_id":                  result.LogID,
			"message":                 result.Message,
		})
	case "failed_onchain":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "code": "INTEGRITY_TAMPERED", "layer": "4_blockchain",
			"data": gin.H{
				"is_valid": result.IsValid, "message": result.Message,
				"verification_assessment": result.Assessment,
				"proof_status":            result.ProofStatus,
				"log_id":                  result.LogID,
				"incident_id":             result.IncidentID,
				"incident_scope":          result.IncidentScope,
				"incident_type":           result.IncidentType,
				"incident_status":         result.IncidentStatus,
				"db_root":                 result.DBRoot, "chain_root": result.ChainRoot,
			},
		})
	case "success":
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"data": gin.H{
				"log_id":                    result.LogID,
				"hash_value":                result.ExpectedHash,
				"merkle_root":               result.DBRoot,
				"blockchain_tx_id":          result.TxID,
				"is_valid":                  result.IsValid,
				"verification_assessment":   result.Assessment,
				"proof_status":              result.ProofStatus,
				"message":                   result.Message,
				"agent_status":              result.AgentStatus,
				"agent_discrepancies":       result.AgentDiscrepancies,
				"source_status":             result.SourceStatus,
				"state_comparison":          result.StateComparison,
				"comparison_basis":          result.ComparisonBasis,
				"offchain_state_hash":       result.OffchainStateHash,
				"client_state_hash":         result.ClientStateHash,
				"source_checked_at":         result.SourceCheckedAt,
				"audit_trail_status":        result.AuditTrailStatus,
				"audit_trail_discrepancies": result.AuditTrailDiffs,
				"incident_id":               result.IncidentID,
				"incident_scope":            result.IncidentScope,
				"incident_type":             result.IncidentType,
				"incident_status":           result.IncidentStatus,
			},
		})
	}
}

func (h *Handler) GetFabricRecord(c *gin.Context) {
	data, err := h.Service.GetFabricRecord(c.Param("anchor_id"))
	if err != nil {
		switch err.Error() {
		case "fabric_bypass":
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Fabric Gateway terputus"})
		case "fabric_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Data tidak ditemukan di Ledger Fabric"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses data dari Fabric"})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"source": "Hyperledger Fabric World State", "data": data})
}

type VerifyDataRequest struct {
	Resource string                  `json:"resource" binding:"required"`
	Data     *map[string]interface{} `json:"data"`
}

func (h *Handler) VerifyData(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	var req VerifyDataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid."})
		return
	}
	result, err := h.Service.VerifyDataIntegrity(req.Resource, clientID, req.Data)
	if err != nil {
		switch err.Error() {
		case "log_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Tidak ada rekam jejak audit untuk resource ini."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi integritas data."})
		}
		return
	}
	if result.IsValid {
		c.JSON(http.StatusOK, result)
	} else {
		c.JSON(http.StatusConflict, result)
	}
}

// GetRecentLogs sekarang mendukung pagination sesungguhnya via query params
// ?page=&page_size= (default: page=1, page_size=10, maksimum 100), serta
// filter opsional ?integrity_status=valid|tampered|unreachable.
//
// Response contract baru: {"data": [...], "pagination": {...}, "note"?: "..."}
// menggantikan array polos yang dipakai versi lama (limit hardcoded 500).
// Frontend (src/App.js) perlu disesuaikan untuk membaca res.data.data alih-alih
// res.data langsung — lihat catatan terpisah, belum diterapkan di sesi ini.
// @Summary Get recent audit logs
// @Description Mengambil daftar log terbaru dengan dukungan paginasi dan filter.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param page query int false "Nomor Halaman (default: 1)"
// @Param page_size query int false "Ukuran Halaman (default: 10, maksimum: 100)"
// @Param integrity_status query string false "Filter Status Integritas (valid, tampered, unreachable)"
// @Param sort_order query string false "Urutan (asc, desc)"
// @Param source_table query string false "Filter Tabel Sumber"
// @Param db_engine query string false "Filter Database Engine"
// @Param from query string false "Waktu Mulai (RFC3339)"
// @Param to query string false "Waktu Selesai (RFC3339)"
// @Success 200 {object} RecentLogsResponse "Daftar log beserta data paginasi"
// @Failure 400 {object} ErrorResponse "Parameter tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil log terbaru"
// @Router /dashboard/logs [get]
func (h *Handler) GetRecentLogs(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	page := 1
	if p := c.Query("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	pageSize := normalizeAuditLogPageSize(c.Query("page_size"))

	integrityStatus := strings.TrimSpace(c.Query("integrity_status"))
	sortOrder := strings.ToLower(strings.TrimSpace(c.Query("sort_order")))
	if sortOrder != "asc" && sortOrder != "desc" {
		sortOrder = "desc"
	}
	sourceTable := strings.TrimSpace(c.Query("source_table"))
	dbEngine := strings.TrimSpace(c.Query("db_engine"))
	fromStr := strings.TrimSpace(c.Query("from"))
	toStr := strings.TrimSpace(c.Query("to"))

	var fromTime, toTime *time.Time
	if fromStr != "" {
		if t, err := parseTimeRobust(fromStr); err == nil {
			fromTime = &t
		}
	}
	if toStr != "" {
		if t, err := parseTimeRobust(toStr); err == nil {
			toTime = &t
		}
	}

	result, err := h.Service.GetRecentLogsPaginated(clientID, page, pageSize, integrityStatus, sortOrder, sourceTable, dbEngine, fromTime, toTime)
	if err != nil {
		if err.Error() == "invalid_integrity_status" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Parameter integrity_status tidak valid. Gunakan salah satu: valid, tampered, unreachable.",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil log terbaru"})
		return
	}
	c.JSON(http.StatusOK, result)
}

// @Summary Get resource inventory
// @Description Mengambil inventaris/daftar resource unik (tabel/entity) yang terekam.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Success 200 {array} models.ClientTable "Daftar resource inventory"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal memuat daftar data"
// @Router /dashboard/inventory [get]
func (h *Handler) GetResourceInventory(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	inventory, err := h.Service.GetResourceInventory(clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memuat daftar data"})
		return
	}
	c.JSON(http.StatusOK, inventory)
}

func (h *Handler) VerifyResourceHistory(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	resourceParam := c.Param("resource")

	// Deteksi jika param adalah nama tabel (tidak mengandung ':')
	if !strings.Contains(resourceParam, ":") {
		// Ambil semua resource (baris terbaru) di dalam tabel ini
		resources, err := h.Service.GetTableResources(resourceParam, clientID)
		if err != nil {
			if errors.Is(err, errIncidentPersistence) {
				c.JSON(http.StatusInternalServerError, incidentPersistenceErrorPayload(err))
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil data baris tabel."})
			return
		}

		// Agregasi status tabel
		hasTampered := false
		hasUnreachable := false
		hasPending := false
		for _, res := range resources {
			switch res.ChainStatus {
			case "tampered":
				hasTampered = true
			case "unreachable":
				hasUnreachable = true
			case "pending":
				hasPending = true
			}
		}

		chainStatus := "valid"
		switch {
		case hasTampered:
			chainStatus = "tampered"
		case hasUnreachable:
			chainStatus = "unreachable"
		case hasPending:
			chainStatus = "pending"
		}

		result := &ResourceChainResult{
			Resource:    resourceParam,
			ChainStatus: chainStatus,
			TotalLogs:   len(resources),
			Logs:        resources,
		}

		// Gunakan HTTP 200 karena ini adalah tampilan daftar, kecuali jika semuanya conflict
		c.JSON(http.StatusOK, result)
		return
	}

	result, err := h.Service.VerifyResourceHistory(resourceParam, clientID)
	if err != nil {
		if errors.Is(err, errIncidentPersistence) {
			c.JSON(http.StatusInternalServerError, incidentPersistenceErrorPayload(err))
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "Riwayat resource tidak ditemukan."})
		return
	}

	switch result.ChainStatus {
	case "tampered":
		c.JSON(http.StatusConflict, result)
	case "pending":
		c.JSON(http.StatusAccepted, result)
	default: // valid, unreachable
		c.JSON(http.StatusOK, result)
	}
}

func (h *Handler) GetLogsByResource(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	logs, err := h.Service.GetLogsByResource(c.Param("resource"), clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil log resource"})
		return
	}
	c.JSON(http.StatusOK, logs)
}

func parseTimeRobust(timeStr string) (time.Time, error) {
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return time.Time{}, fmt.Errorf("empty time string")
	}

	if len(timeStr) > 10 && timeStr[10] == ' ' {
		timeStr = timeStr[:10] + "T" + timeStr[11:]
	}

	// Double space/plus replacement
	normalized := strings.ReplaceAll(timeStr, " ", "+")

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05.999999999Z07",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, l := range layouts {
		if t, err := time.Parse(l, normalized); err == nil {
			return t, nil
		}
		if t, err := time.Parse(l, timeStr); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse time: %s", timeStr)
}

func parseVerifyRangeBounds(c *gin.Context) (time.Time, time.Time, bool) {
	fromStr := c.Query("from")
	toStr := c.Query("to")

	if fromStr == "" || toStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Parameter 'from' dan 'to' wajib diisi (format: RFC3339)"})
		return time.Time{}, time.Time{}, false
	}

	from, err := parseTimeRobust(fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'from' tidak valid. Gunakan format seperti: 2026-06-26T10:00:00Z atau 2026-06-29 10:26:32.54+07"})
		return time.Time{}, time.Time{}, false
	}

	to, err := parseTimeRobust(toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'to' tidak valid. Gunakan format seperti: 2026-06-26T10:05:00Z atau 2026-06-29 10:26:32.54+07"})
		return time.Time{}, time.Time{}, false
	}

	if to.Before(from) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'to' tidak boleh lebih awal dari 'from'"})
		return time.Time{}, time.Time{}, false
	}

	return from, to, true
}

// @Summary Estimate verification range
// @Description Menghitung jumlah audit log dalam rentang waktu sebelum verifikasi sinkron dijalankan.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param from query string true "Waktu mulai (RFC3339)"
// @Param to query string true "Waktu selesai (RFC3339)"
// @Success 200 {object} VerifyRangeEstimateResponse "Estimasi ukuran range"
// @Failure 400 {object} ErrorResponse "Parameter rentang tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal menghitung estimasi range"
// @Router /dashboard/verify-range/estimate [get]
func (h *Handler) EstimateLogRange(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	from, to, ok := parseVerifyRangeBounds(c)
	if !ok {
		return
	}

	estimatedItems, err := h.Service.EstimateLogRange(from, to, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghitung estimasi log pada range"})
		return
	}

	c.JSON(http.StatusOK, VerifyRangeEstimateResponse{
		EstimatedItems: estimatedItems,
		SyncLimit:      MaxSynchronousVerifyRange,
		CanVerifySync:  estimatedItems <= MaxSynchronousVerifyRange,
	})
}

// @Summary Verify all logs in a time range
// @Description Memverifikasi seluruh audit log dalam rentang waktu secara sinkron.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param from query string true "Waktu mulai (RFC3339)"
// @Param to query string true "Waktu selesai (RFC3339)"
// @Success 200 {object} RangeVerificationResult "Ringkasan dan hasil tiap log"
// @Failure 400 {object} ErrorResponse "Parameter rentang tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 422 {object} ErrorResponse "Range terlalu besar untuk verifikasi sinkron"
// @Failure 500 {object} ErrorResponse "Gagal memverifikasi range log"
// @Router /dashboard/verify-range/internal [get]
func (h *Handler) VerifyInternalLogRange(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	from, to, ok := parseVerifyRangeBounds(c)
	if !ok {
		return
	}

	result, err := h.Service.VerifyInternalLogRange(from, to, clientID, middleware.GetRequestID(c))
	if err != nil {
		var tooLarge *VerifyRangeTooLargeError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"code":            "VERIFY_RANGE_TOO_LARGE",
				"error":           "Range terlalu besar untuk verifikasi sinkron. Persempit date range atau tunggu fitur background job.",
				"estimated_items": tooLarge.EstimatedItems,
				"sync_limit":      tooLarge.Limit,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi range log"})
		return
	}

	c.JSON(http.StatusOK, result)
}

// @Summary Verify all logs in a time range (legacy route)
// @Description Compatibility alias for the gateway dashboard. New clients should use /dashboard/verify-range/internal.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param from query string true "Waktu mulai (RFC3339)"
// @Param to query string true "Waktu selesai (RFC3339)"
// @Success 200 {object} RangeVerificationResult "Ringkasan dan hasil tiap log"
// @Failure 400 {object} ErrorResponse "Parameter rentang tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 422 {object} ErrorResponse "Range terlalu besar untuk verifikasi sinkron"
// @Failure 500 {object} ErrorResponse "Gagal memverifikasi range log"
// @Router /dashboard/verify-range [get]
func (h *Handler) VerifyLegacyLogRange(c *gin.Context) {
	h.VerifyInternalLogRange(c)
}

// @Summary Verify client logs in a time range
// @Description Memverifikasi seluruh audit log dalam rentang waktu dengan kontrak yang sama seperti gateway dashboard. Client Portal hanya menerima summary tanpa daftar row log.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param from query string true "Waktu mulai (RFC3339)"
// @Param to query string true "Waktu selesai (RFC3339)"
// @Success 200 {object} ClientRangeVerificationResponse "Summary verifikasi log client"
// @Failure 400 {object} ErrorResponse "Parameter rentang tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 422 {object} ErrorResponse "Range terlalu besar untuk verifikasi sinkron"
// @Failure 500 {object} ErrorResponse "Gagal memverifikasi client range log"
// @Router /dashboard/verify-range/client [get]
func (h *Handler) VerifyClientLogRange(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	from, to, ok := parseVerifyRangeBounds(c)
	if !ok {
		return
	}

	result, err := h.Service.VerifyClientLogRange(from, to, clientID, middleware.GetRequestID(c))
	if err != nil {
		var tooLarge *VerifyRangeTooLargeError
		if errors.As(err, &tooLarge) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{
				"code":            "VERIFY_RANGE_TOO_LARGE",
				"error":           "Range terlalu besar untuk verifikasi sinkron. Persempit date range atau tunggu fitur background job.",
				"estimated_items": tooLarge.EstimatedItems,
				"sync_limit":      tooLarge.Limit,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi client range log"})
		return
	}

	c.JSON(http.StatusOK, ClientRangeVerificationResponse{
		Range:   result.Range,
		Summary: result.Summary,
	})
}

// @Summary Queue a background verification run
// @Description Membuat job verifikasi range yang diproses backend secara bertahap. Ukuran batch membatasi pekerjaan per iterasi, bukan total log dalam range.
// @Tags Audit
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body CreateVerificationRunRequest true "Rentang verifikasi dan ukuran batch"
// @Param client_id query string false "Client ID; hanya admin yang boleh memilih tenant"
// @Success 202 {object} VerificationRunResponseEnvelope "Job berhasil diantrikan"
// @Failure 400 {object} ErrorResponse "Rentang tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 503 {object} ErrorResponse "Background verifier belum tersedia"
// @Router /dashboard/verification-runs [post]
func (h *Handler) CreateVerificationRun(c *gin.Context) {
	if h.VerificationJobs == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Background verifier belum tersedia"})
		return
	}

	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	var req CreateVerificationRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Body wajib memuat from dan to."})
		return
	}

	from, err := parseTimeRobust(strings.TrimSpace(req.From))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'from' tidak valid. Gunakan RFC3339."})
		return
	}
	to, err := parseTimeRobust(strings.TrimSpace(req.To))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'to' tidak valid. Gunakan RFC3339."})
		return
	}
	if to.Before(from) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'to' tidak boleh lebih awal dari 'from'."})
		return
	}

	requestedBy, _ := c.Get("user_id")
	requestedByValue, _ := requestedBy.(string)
	run, err := h.VerificationJobs.Enqueue(from.UTC(), to.UTC(), clientID, requestedByValue, req.BatchSize)
	if err != nil {
		if errors.Is(err, ErrManualVerificationCooldown) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": err.Error(), "retry_after_seconds": 60})
			return
		}
		if errors.Is(err, ErrActiveVerificationRun) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengantrikan verification run."})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message": "Verification run berhasil diantrikan.",
		"data":    ToVerificationRunResponse(run),
	})
}

// @Summary Get a background verification run
// @Description Mengambil progress dan summary verification run yang hanya boleh diakses oleh tenant pada token.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param id path string true "Verification run ID"
// @Param client_id query string false "Client ID; hanya admin yang boleh memilih tenant"
// @Success 200 {object} VerificationRunResponseEnvelope "Progress verification run"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Verification run tidak ditemukan"
// @Router /dashboard/verification-runs/{id} [get]
func (h *Handler) GetVerificationRun(c *gin.Context) {
	if h.VerificationJobs == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Background verifier belum tersedia"})
		return
	}

	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	run, err := h.VerificationJobs.GetRun(c.Param("id"), clientID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Verification run tidak ditemukan."})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil verification run."})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": ToVerificationRunResponse(run)})
}

// @Summary Get the latest background verification run
// @Description Mengambil verification run terakhir untuk tenant yang sedang login. Jika belum ada run, data bernilai null.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Param client_id query string false "Client ID; hanya admin yang boleh memilih tenant"
// @Param run_type query string false "Filter tipe audit (default INTERNAL)" Enums(INTERNAL,EXTERNAL)
// @Success 200 {object} LatestVerificationRunResponse "Verification run terakhir"
// @Failure 400 {object} ErrorResponse "Tipe audit tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Router /dashboard/verification-runs/latest [get]
func (h *Handler) GetLatestVerificationRun(c *gin.Context) {
	if h.VerificationJobs == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Background verifier belum tersedia"})
		return
	}

	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	runType := strings.ToUpper(strings.TrimSpace(c.Query("run_type")))
	if runType != "" && runType != models.VerificationRunTypeInternal && runType != models.VerificationRunTypeExternal {
		c.JSON(http.StatusBadRequest, gin.H{"error": "run_type harus INTERNAL atau EXTERNAL"})
		return
	}
	run, err := h.VerificationJobs.GetLatestRunByType(clientID, runType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil verification run terakhir."})
		return
	}
	if run == nil {
		c.JSON(http.StatusOK, gin.H{"data": nil})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": ToVerificationRunResponse(run)})
}

// --- Agent API Handlers ---

func (h *Handler) GetOffchainDataForAgent(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	resource := c.Param("resource")
	if resource != "" {
		resource = strings.TrimPrefix(resource, "/")
	}
	fromStr := c.Query("from")
	toStr := c.Query("to")

	var logs []models.AuditLog
	var err error

	if fromStr != "" && toStr != "" {
		from, errFrom := parseTimeRobust(fromStr)
		to, errTo := parseTimeRobust(toStr)
		if errFrom != nil || errTo != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Format from/to tidak valid. Gunakan format RFC3339."})
			return
		}
		logs, err = h.Service.GetLogsByTimeRange(from, to, clientID)
	} else if resource != "" {
		latestLog, errLatest := h.Service.GetLatestClientLogByResource(resource, clientID)
		if errLatest != nil {
			err = errLatest
		} else if latestLog != nil {
			logs = []models.AuditLog{*latestLog}
		}
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Harus menyertakan :resource di path atau parameter from & to"})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil data offchain"})
		return
	}

	c.JSON(http.StatusOK, logs)
}

type VerifyMerkleRootRequest struct {
	LeafHash       string `json:"leaf_hash"`
	MerkleRoot     string `json:"merkle_root"`
	BlockchainTxID string `json:"blockchain_tx_id" binding:"required"`
}

// VerifyMerkleRootForAgent godoc
// @Summary Verifikasi Merkle root & proof terhadap ledger Blockchain
// @Description Endpoint khusus Agent untuk merekonstruksi Merkle root dari leaf_hash data client dengan Merkle proof offchain lalu membandingkannya dengan Merkle root di blockchain.
// @Tags Agent
// @Accept json
// @Produce json
// @Param request body VerifyMerkleRootRequest true "Leaf hash / Merkle root dan Blockchain TX ID"
// @Success 200 {object} MerkleVerificationResult
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /agent/verify-merkle-root [post]
func (h *Handler) VerifyMerkleRootForAgent(c *gin.Context) {
	var req VerifyMerkleRootRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid: " + err.Error()})
		return
	}

	result, err := h.Service.VerifyMerkleProofAgainstBlockchain(req.LeafHash, req.MerkleRoot, req.BlockchainTxID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi dengan blockchain: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// SubmitAgentAuditReport receives audit reports generated by auditchain-agent
// and updates the persistent client dashboard stats and verification run history.
func (h *Handler) SubmitAgentAuditReport(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	var req AgentAuditReportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format laporan audit tidak valid: " + err.Error()})
		return
	}

	if err := h.Service.ProcessAgentAuditReport(clientID, req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses laporan audit: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "ok",
		"message": "Laporan audit berhasil diproses",
		"run_id":  req.RunID,
	})
}
