package recovery

import (
	"net/http"
	"strings"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{Service: service}
}

type ErrorResponse struct {
	Error string `json:"error" example:"Pesan kesalahan"`
}

type IncidentListResponse struct {
	Data []models.TamperIncident `json:"data"`
}

type IncidentDetailResponse struct {
	Data models.TamperIncident `json:"data"`
}

type RequestListResponse struct {
	Data []models.RecoveryRequest `json:"data"`
}

type RequestDetailResponse struct {
	Data models.RecoveryRequest `json:"data"`
}

func (h *Handler) clientID(c *gin.Context) (string, bool) {
	value, exists := c.Get("client_id")
	clientID, ok := value.(string)
	if !ok {
		clientID = ""
	}
	clientID = strings.TrimSpace(clientID)

	if !exists || clientID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas client pada token tidak valid."})
		return "", false
	}
	return clientID, true
}

func (h *Handler) userID(c *gin.Context) string {
	value, _ := c.Get("user_id")
	userID, _ := value.(string)
	return userID
}

// @Summary List tamper incidents
// @Description Mengambil daftar semua insiden tamper berdasarkan client_id (opsional difilter dengan status).
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param status query string false "Filter Status Insiden (OPEN, RESOLVED)"
// @Success 200 {object} IncidentListResponse "Daftar Insiden Tamper"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar tamper incident"
// @Router /recovery/incidents [get]
func (h *Handler) ListIncidents(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	incidents, err := h.Service.ListIncidents(c.Request.Context(), clientID, IncidentFilter{Status: c.Query("status")})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar tamper incident."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": incidents})
}

// @Summary Get tamper incident detail
// @Description Mengambil detail sebuah insiden tamper berdasarkan ID.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} IncidentDetailResponse "Detail Insiden Tamper"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Tamper incident tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil detail tamper incident"
// @Router /recovery/incidents/{id} [get]
func (h *Handler) GetIncident(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	incident, err := h.Service.GetIncident(c.Request.Context(), clientID, c.Param("id"))
	if serviceErrorCode(err, "incident_not_found") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Tamper incident tidak ditemukan."})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil detail tamper incident."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": incident})
}

// @Summary List candidate snapshots for recovery
// @Description Mengambil daftar snapshot valid yang tersedia di S3 untuk di-recovery pada insiden tertentu.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} map[string]interface{} "Daftar kandidat snapshot"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Insiden tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar kandidat"
// @Router /recovery/incidents/{id}/candidates [get]
func (h *Handler) ListCandidates(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	candidates, err := h.Service.ListCandidates(c.Request.Context(), clientID, c.Param("id"))
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": candidates})
}

// @Summary Preflight check for recovery
// @Description Melakukan pengecekan preflight pada kandidat snapshot untuk memastikan tidak ada konflik sebelum recovery.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} map[string]interface{} "Hasil preflight check"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 409 {object} ErrorResponse "Konflik atau kondisi tidak valid untuk preflight"
// @Failure 500 {object} ErrorResponse "Gagal melakukan preflight"
// @Router /recovery/incidents/{id}/preflight [post]
func (h *Handler) Preflight(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	result, err := h.Service.Preflight(c.Request.Context(), clientID, c.Param("id"))
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// @Summary List snapshot versions by resource
// @Description Mengambil histori versi snapshot dari sebuah resource (table).
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param resource path string true "Resource/Table Name"
// @Success 200 {object} map[string]interface{} "Histori versi snapshot"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil histori snapshot recovery"
// @Router /recovery/snapshots/{resource}/versions [get]
func (h *Handler) ListVersions(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	versions, err := h.Service.ListVersions(c.Request.Context(), clientID, c.Param("resource"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil histori snapshot recovery."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": versions})
}

// @Summary List recovery requests
// @Description Mengambil daftar permintaan (request) recovery yang diajukan.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param status query string false "Filter Status Request"
// @Success 200 {object} RequestListResponse "Daftar recovery request"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar recovery request"
// @Router /recovery/requests [get]
func (h *Handler) ListRequests(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	requests, err := h.Service.ListRequests(c.Request.Context(), clientID, c.Query("status"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar recovery request."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": requests})
}

// @Summary Get recovery request detail
// @Description Mengambil detail sebuah permintaan (request) recovery berdasarkan ID.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Request ID"
// @Success 200 {object} RequestDetailResponse "Detail recovery request"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Recovery request tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil detail recovery request"
// @Router /recovery/requests/{id} [get]
func (h *Handler) GetRequest(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	request, err := h.Service.GetRequest(c.Request.Context(), clientID, c.Param("id"))
	if serviceErrorCode(err, "request_not_found") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Recovery request tidak ditemukan."})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil detail recovery request."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": request})
}

// @Summary Create recovery request
// @Description Membuat permintaan baru untuk melakukan recovery data ke snapshot tertentu.
// @Tags Recovery
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body CreateRequestInput true "Data Request Recovery"
// @Success 201 {object} RequestDetailResponse "Recovery request berhasil dibuat"
// @Failure 400 {object} ErrorResponse "Format request tidak valid"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 409 {object} ErrorResponse "Konflik atau state tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal membuat recovery request"
// @Router /recovery/requests [post]
func (h *Handler) CreateRequest(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	var input CreateRequestInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "incident_id, selected_log_id, reason, dan idempotency_key wajib diisi."})
		return
	}
	request, err := h.Service.CreateRequest(c.Request.Context(), clientID, h.userID(c), input)
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"data": request})
}

// @Summary Approve recovery request
// @Description Menyetujui sebuah permintaan recovery (untuk backward compatibility).
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Request ID"
// @Success 200 {object} RequestDetailResponse "Request berhasil disetujui"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Request tidak ditemukan"
// @Failure 409 {object} ErrorResponse "State request tidak sesuai"
// @Router /recovery/requests/{id}/approve [post]
func (h *Handler) Approve(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	request, err := h.Service.Approve(c.Request.Context(), clientID, c.Param("id"), h.userID(c))
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": request})
}

// @Summary Reject recovery request
// @Description Menolak sebuah permintaan recovery.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Request ID"
// @Success 200 {object} RequestDetailResponse "Request berhasil ditolak"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Request tidak ditemukan"
// @Failure 409 {object} ErrorResponse "State request tidak sesuai"
// @Router /recovery/requests/{id}/reject [post]
func (h *Handler) Reject(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	request, err := h.Service.Reject(c.Request.Context(), clientID, c.Param("id"), h.userID(c))
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": request})
}

// @Summary Execute recovery request
// @Description Mengeksekusi permintaan recovery yang telah disetujui atau siap dieksekusi, memulihkan data dari S3.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Request ID"
// @Success 200 {object} RequestDetailResponse "Eksekusi recovery dimulai/berhasil"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Request tidak ditemukan"
// @Failure 409 {object} ErrorResponse "State request tidak sesuai"
// @Failure 500 {object} ErrorResponse "Gagal mengeksekusi recovery"
// @Router /recovery/requests/{id}/execute [post]
func (h *Handler) Execute(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	request, err := h.Service.Execute(c.Request.Context(), clientID, c.Param("id"), h.userID(c))
	if err != nil {
		h.respondServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": request})
}

func (h *Handler) respondServiceError(c *gin.Context, err error) {
	switch {
	case serviceErrorCode(err, "incident_not_found"), serviceErrorCode(err, "request_not_found"), serviceErrorCode(err, "snapshot_not_found"):
		c.JSON(http.StatusNotFound, gin.H{"error": "Data recovery tidak ditemukan."})
	case serviceErrorCode(err, "invalid_request"), serviceErrorCode(err, "invalid_request_state"), serviceErrorCode(err, "incident_closed"), serviceErrorCode(err, "legacy_recovery_out_of_scope"), serviceErrorCode(err, "snapshot_belum_verified"), serviceErrorCode(err, "snapshot_reference_missing"), serviceErrorCode(err, "snapshot_checksum_missing"), serviceErrorCode(err, "snapshot_plaintext_hash_missing"), serviceErrorCode(err, "cross_log_recovery_not_allowed"), serviceErrorCode(err, "anchor_missing"), serviceErrorCode(err, "merkle_root_missing"), serviceErrorCode(err, "recovery_not_required"):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case serviceErrorCode(err, "recovery_storage_unavailable"), serviceErrorCode(err, "fabric_unavailable"):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
	}
}

func serviceErrorCode(err error, code string) bool {
	if err == nil {
		return false
	}
	message := err.Error()
	return message == code || strings.HasPrefix(message, code+":")
}
