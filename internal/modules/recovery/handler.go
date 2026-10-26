package recovery

import (
	"net/http"
	"strconv"
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
	Data        []models.TamperIncident `json:"data"`
	Page        int                     `json:"page"`
	PageSize    int                     `json:"page_size"`
	TotalItems  int64                   `json:"total_items"`
	TotalPages  int                     `json:"total_pages"`
	OpenCount   int64                   `json:"open_count"`
	ActiveCount int64                   `json:"active_count"`
}

type CandidateListResponse struct {
	Data []CandidateView `json:"data"`
}

type VersionListResponse struct {
	Data []VersionView `json:"data"`
}

type IncidentDetailResponse struct {
	Data IncidentDetailView `json:"data"`
}

type RequestListResponse struct {
	Data []models.RecoveryRequest `json:"data"`
}

type RequestDetailResponse struct {
	Data models.RecoveryRequest `json:"data"`
}

type EventDetailResponse struct {
	Data RecoveryEventView `json:"data"`
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

func (h *Handler) clientOperator(c *gin.Context) bool {
	roleName, _ := c.Get("role")
	role, _ := roleName.(string)
	role = strings.TrimSpace(role)
	normalizedRole := strings.ToLower(role)
	if normalizedRole == "admin" || normalizedRole == "administrator" || strings.Contains(normalizedRole, "admin") || normalizedRole == "superuser" || normalizedRole == "platform_operator" {
		c.JSON(http.StatusForbidden, gin.H{"error": "Recovery hanya dapat dijalankan oleh user client; admin platform tidak dapat mengeksekusi recovery."})
		return false
	}
	if strings.TrimSpace(h.userID(c)) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas user pada token tidak valid."})
		return false
	}
	return true
}

// @Summary List recovery incidents
// @Description Mengambil incident recovery milik client pada JWT. Kirim page/page_size untuk daftar berpaginasi; permintaan lama tanpa parameter itu tetap mengembalikan seluruh daftar.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param status query string false "Status incident, misalnya OPEN atau RESOLVED"
// @Param recovery_status query string false "Filter status operasi recovery, misalnya ACTIVE_RECOVERY"
// @Param scope query string false "Scope incident, CLIENT_SOURCE atau GATEWAY_INTEGRITY"
// @Param search query string false "Pencarian ID log, resource, atau jenis incident"
// @Param sort query string false "Urutan: newest, oldest, atau resource"
// @Param page query int false "Nomor halaman" default(1)
// @Param page_size query int false "Jumlah incident per halaman, maksimum 100" default(50)
// @Success 200 {object} IncidentListResponse "Daftar incident"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar incident"
// @Router /dashboard/recovery/incidents [get]
func (h *Handler) ListIncidents(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	paginationRequested := c.Query("page") != "" || c.Query("page_size") != ""
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "50"))
	incidents, err := h.Service.ListIncidents(c.Request.Context(), clientID, IncidentFilter{
		Status: c.Query("status"), RecoveryStatus: c.Query("recovery_status"),
		Scope: c.Query("scope"), Search: c.Query("search"), Sort: c.Query("sort"),
		Page: page, PageSize: pageSize, Unpaged: !paginationRequested,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar tamper incident."})
		return
	}
	if paginationRequested {
		c.JSON(http.StatusOK, incidents)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": incidents.Data})
}

// @Summary Get tamper incident detail
// @Description Mengambil detail sebuah incident recovery berdasarkan ID.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} IncidentDetailResponse "Detail Insiden Tamper"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Tamper incident tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil detail tamper incident"
// @Router /dashboard/recovery/incidents/{id} [get]
func (h *Handler) GetIncident(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	incident, err := h.Service.GetIncidentDetail(c.Request.Context(), clientID, c.Param("id"))
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

// @Summary List trusted client recovery references
// @Description Mengambil kandidat referensi untuk pemulihan data client. Incident GATEWAY_INTEGRITY tetap read-only untuk investigasi.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} CandidateListResponse "Daftar kandidat recovery"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Insiden tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar kandidat"
// @Router /dashboard/recovery/incidents/{id}/candidates [get]
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
// @Description Memeriksa referensi PostgreSQL/Fabric dan state live sebelum recovery tanpa melakukan write.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Incident ID"
// @Success 200 {object} PreflightResult "Hasil preflight check"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 409 {object} ErrorResponse "Konflik atau kondisi tidak valid untuk preflight"
// @Failure 500 {object} ErrorResponse "Gagal melakukan preflight"
// @Router /dashboard/recovery/incidents/{id}/preflight [post]
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
// @Description Mengambil histori event client ter-anchor dari sebuah resource (table).
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param resource path string true "Resource/Table Name"
// @Success 200 {object} VersionListResponse "Histori event client ter-anchor"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil histori recovery"
// @Router /dashboard/recovery/resources/{resource}/versions [get]
func (h *Handler) ListVersions(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	versions, err := h.Service.ListVersions(c.Request.Context(), clientID, c.Param("resource"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil histori recovery."})
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
// @Router /dashboard/recovery/requests [get]
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

// @Summary List recovery events
// @Description Mengambil event recovery milik client, diurutkan dari yang terbaru.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param result_status query string false "Filter hasil recovery"
// @Param resource query string false "Filter resource"
// @Param page query int false "Nomor halaman" default(1)
// @Param page_size query int false "Jumlah event per halaman, maksimum 100" default(20)
// @Param include_legacy query bool false "Sertakan event legacy" default(true)
// @Success 200 {object} RecoveryEventPage "Halaman event recovery"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 500 {object} ErrorResponse "Gagal mengambil daftar recovery event"
// @Router /dashboard/recovery/events [get]
func (h *Handler) ListEvents(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	includeLegacy := !strings.EqualFold(c.DefaultQuery("include_legacy", "true"), "false")
	result, err := h.Service.ListEvents(c.Request.Context(), clientID, c.Query("result_status"), c.Query("resource"), page, pageSize, includeLegacy)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar recovery event."})
		return
	}
	c.JSON(http.StatusOK, result)
}

// @Summary Get recovery event
// @Description Mengambil detail event recovery milik client pada JWT.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Recovery event ID"
// @Success 200 {object} EventDetailResponse "Detail event recovery"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Recovery event tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil recovery event"
// @Router /dashboard/recovery/events/{id} [get]
func (h *Handler) GetEvent(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	event, err := h.Service.GetEvent(c.Request.Context(), clientID, c.Param("id"))
	if serviceErrorCode(err, "recovery_event_not_found") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Recovery event tidak ditemukan."})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil recovery event."})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": event})
}

// @Summary Verify recovery event evidence
// @Description Memeriksa hash, Merkle proof, anchor Fabric, dan referensi recovery event.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Recovery event ID"
// @Success 200 {object} map[string]interface{} "Status verifikasi event"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Recovery event tidak ditemukan"
// @Failure 409 {object} map[string]interface{} "Evidence belum valid atau belum terverifikasi"
// @Router /dashboard/recovery/events/{id}/verify [get]
func (h *Handler) VerifyEvent(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	result, err := h.Service.VerifyEvent(c.Request.Context(), clientID, c.Param("id"))
	if serviceErrorCode(err, "recovery_event_not_found") {
		c.JSON(http.StatusNotFound, gin.H{"error": "Recovery event tidak ditemukan."})
		return
	}
	if err != nil {
		c.JSON(http.StatusConflict, result)
		return
	}
	c.JSON(http.StatusOK, result)
}

// @Summary Get recovery request
// @Description Mengambil status terbaru request recovery milik client pada JWT.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Recovery request ID"
// @Success 200 {object} RequestDetailResponse "Detail request recovery"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Recovery request tidak ditemukan"
// @Failure 500 {object} ErrorResponse "Gagal mengambil detail request"
// @Router /dashboard/recovery/requests/{id} [get]
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
// @Description Membekukan intent pemulihan row database client setelah referensi terverifikasi dan preflight lulus.
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
// @Router /dashboard/recovery/requests [post]
func (h *Handler) CreateRequest(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	if !h.clientOperator(c) {
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
// @Description Mengeksekusi request pemulihan row database client melalui Agent. Metadata audit Gateway tidak dapat ditulis melalui Client Portal.
// @Tags Recovery
// @Produce json
// @Security BearerAuth
// @Param id path string true "Request ID"
// @Success 200 {object} RequestDetailResponse "Eksekusi recovery dimulai/berhasil"
// @Failure 401 {object} ErrorResponse "Identitas client tidak valid"
// @Failure 404 {object} ErrorResponse "Request tidak ditemukan"
// @Failure 409 {object} ErrorResponse "State request tidak sesuai"
// @Failure 500 {object} ErrorResponse "Gagal mengeksekusi recovery"
// @Router /dashboard/recovery/requests/{id}/execute [post]
func (h *Handler) Execute(c *gin.Context) {
	clientID, ok := h.clientID(c)
	if !ok {
		return
	}
	if !h.clientOperator(c) {
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
	case serviceErrorCode(err, "incident_not_found"), serviceErrorCode(err, "request_not_found"), serviceErrorCode(err, "snapshot_not_found"), serviceErrorCode(err, "recovery_event_not_found"):
		c.JSON(http.StatusNotFound, gin.H{"error": "Data recovery tidak ditemukan."})
	case serviceErrorCode(err, "client_state_not_comparable"), serviceErrorCode(err, "invalid_request"), serviceErrorCode(err, "invalid_request_state"), serviceErrorCode(err, "incident_closed"), serviceErrorCode(err, "legacy_recovery_out_of_scope"), serviceErrorCode(err, "agent_direct_recovery_required"), serviceErrorCode(err, "gateway_integrity_read_only"), serviceErrorCode(err, "gateway_integrity_incident_required"), serviceErrorCode(err, "gateway_metadata_recovery_unsupported"), serviceErrorCode(err, "audit_trail_reference_missing"), serviceErrorCode(err, "audit_trail_record_not_found"), serviceErrorCode(err, "audit_trail_reference_mismatch"), serviceErrorCode(err, "audit_trail_resource_mismatch"), serviceErrorCode(err, "audit_trail_operation_mismatch"), serviceErrorCode(err, "snapshot_belum_verified"), serviceErrorCode(err, "snapshot_reference_missing"), serviceErrorCode(err, "snapshot_checksum_missing"), serviceErrorCode(err, "snapshot_plaintext_hash_missing"), serviceErrorCode(err, "cross_log_recovery_not_allowed"), serviceErrorCode(err, "anchor_missing"), serviceErrorCode(err, "reference_anchor_missing"), serviceErrorCode(err, "merkle_root_missing"), serviceErrorCode(err, "recovery_not_required"), serviceErrorCode(err, "idempotency_conflict"), serviceErrorCode(err, "recovery_active_conflict"), serviceErrorCode(err, "client_source_incident_required"), serviceErrorCode(err, "reference_resource_mismatch"), serviceErrorCode(err, "client_source_unreachable"), serviceErrorCode(err, "reference_changed"), serviceErrorCode(err, "target_changed"), serviceErrorCode(err, "source_state_changed"), serviceErrorCode(err, "execution_timeout_unknown"), serviceErrorCode(err, "cdc_start_time_missing"), serviceErrorCode(err, "request_execution_state_changed"), serviceErrorCode(err, "reference_not_latest_client_event"), serviceErrorCode(err, "reference_latest_client_event_missing"), serviceErrorCode(err, "reference_local_hash_mismatch"), serviceErrorCode(err, "reference_merkle_proof_mismatch"), serviceErrorCode(err, "reference_fabric_root_mismatch"), serviceErrorCode(err, "reference_metadata_invalid"), serviceErrorCode(err, "operation_not_allowed"), serviceErrorCode(err, "recovery_operation_not_allowed"), serviceErrorCode(err, "payload_incomplete"), serviceErrorCode(err, "payload_invalid"), serviceErrorCode(err, "column_not_allowed"), serviceErrorCode(err, "column_not_updateable"), serviceErrorCode(err, "column_not_insertable"), serviceErrorCode(err, "table_not_allowed"), serviceErrorCode(err, "recovery_disabled"):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	case serviceErrorCode(err, "recovery_storage_unavailable"), serviceErrorCode(err, "fabric_unavailable"), serviceErrorCode(err, "fabric_anchor_unreachable"), serviceErrorCode(err, "reference_proof_read_failed"), serviceErrorCode(err, "agent_recovery_client_unavailable"), serviceErrorCode(err, "agent_config_unavailable"), serviceErrorCode(err, "agent_audit_trail_unavailable"), serviceErrorCode(err, "agent_not_configured"), serviceErrorCode(err, "agent_recovery_token_missing"), serviceErrorCode(err, "agent_recovery_unreachable"), serviceErrorCode(err, "agent_recovery_capability_unavailable"), serviceErrorCode(err, "agent_recovery_capability_invalid"), serviceErrorCode(err, "invalid_recovery_token"):
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
