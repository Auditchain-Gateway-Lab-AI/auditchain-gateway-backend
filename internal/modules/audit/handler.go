package audit

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Service Service
}

func NewHandler(service Service) *Handler {
	return &Handler{Service: service}
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

type DashboardStatsResponse struct {
	TotalLogs       int    `json:"total_logs" example:"1500"`
	ValidLogs       int    `json:"valid_logs" example:"1450"`
	TamperedLogs    int    `json:"tampered_logs" example:"3"`
	UnreachableLogs int    `json:"unreachable_logs" example:"10"`
	PendingLogs     int    `json:"pending_logs" example:"37"`
	TotalResources  int    `json:"total_resources" example:"45"`
	IntegrityScore  string `json:"integrity_score" example:"99.79"`
}

type VerifyLogData struct {
	LogID              string      `json:"log_id"`
	IsValid            bool        `json:"is_valid"`
	Message            string      `json:"message"`
	ExpectedHash       string      `json:"expected_hash,omitempty"`
	ActualHash         string      `json:"actual_hash,omitempty"`
	DBRoot             string      `json:"merkle_root,omitempty"`
	ChainRoot          string      `json:"blockchain_tx_id,omitempty"`
	AgentStatus        string      `json:"agent_status,omitempty"`
	AgentDiscrepancies interface{} `json:"agent_discrepancies,omitempty"`
}

type VerifyLogResponse struct {
	Status  string        `json:"status" example:"success"`
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

type ResourceInventoryItem struct {
	Resource    string `json:"resource" example:"orders"`
	TotalLogs   int    `json:"total_logs" example:"500"`
	LatestLogID string `json:"latest_log_id" example:"log-abc"`
}

// @Summary Get dashboard statistics
// @Description Mengambil statistik ringkasan dashboard seperti jumlah log dan status integritas.
// @Tags Audit
// @Produce json
// @Security BearerAuth
// @Success 200 {object} DashboardStatsResponse "Statistik dashboard"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil statistik"
// @Router /dashboard/stats [get]
func (h *Handler) GetStats(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	stats, err := h.Service.GetDashboardStats(clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil statistik"})
		return
	}
	c.JSON(http.StatusOK, stats)
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
// @Failure 500 {object} ErrorResponse "Kesalahan sistem saat verifikasi"
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
			"status": "failed", "layer": "2_local_hash",
			"data": gin.H{
				"is_valid": result.IsValid, "message": result.Message,
				"log_id":        result.LogID,
				"expected_hash": result.ExpectedHash, "actual_hash": result.ActualHash,
			},
		})
	case "failed_source":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "layer": "3_agent_source",
			"data": gin.H{
				"is_valid": result.IsValid,
				"log_id":   result.LogID,
				"message":  result.Message,
			},
		})
	case "pending":
		c.JSON(http.StatusAccepted, gin.H{
			"status":  "pending",
			"log_id":  result.LogID,
			"message": result.Message,
		})
	case "failed_onchain":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "layer": "4_blockchain",
			"data": gin.H{
				"is_valid": result.IsValid, "message": result.Message,
				"log_id":  result.LogID,
				"db_root": result.DBRoot, "chain_root": result.ChainRoot,
			},
		})
	case "success":
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"data": gin.H{
				"log_id":              result.LogID,
				"hash_value":          result.ExpectedHash,
				"merkle_root":         result.DBRoot,
				"blockchain_tx_id":    result.TxID,
				"is_valid":            result.IsValid,
				"message":             result.Message,
				"agent_status":        result.AgentStatus,
				"agent_discrepancies": result.AgentDiscrepancies,
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
// ?page=&page_size= (default: page=1, page_size=10, maksimum 200), serta
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
// @Param page_size query int false "Ukuran Halaman (default: 10)"
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

	pageSize := 10
	if ps := c.Query("page_size"); ps != "" {
		if parsed, err := strconv.Atoi(ps); err == nil && parsed > 0 {
			pageSize = parsed
		}
	}

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
// @Success 200 {array} ResourceInventoryItem "Daftar resource inventory"
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

func (h *Handler) VerifyLogRange(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	fromStr := c.Query("from")
	toStr := c.Query("to")

	if fromStr == "" || toStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Parameter 'from' dan 'to' wajib diisi (format: RFC3339)"})
		return
	}

	from, err := parseTimeRobust(fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'from' tidak valid. Gunakan format seperti: 2026-06-26T10:00:00Z atau 2026-06-29 10:26:32.54+07"})
		return
	}

	to, err := parseTimeRobust(toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format 'to' tidak valid. Gunakan format seperti: 2026-06-26T10:05:00Z atau 2026-06-29 10:26:32.54+07"})
		return
	}

	if to.Before(from) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "'to' tidak boleh lebih awal dari 'from'"})
		return
	}

	result, err := h.Service.VerifyLogRange(from, to, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi range log"})
		return
	}

	c.JSON(http.StatusOK, result)
}
