package recovery

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{Service: service}
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
	if strings.EqualFold(role, "admin") || strings.EqualFold(role, "administrator") {
		c.JSON(http.StatusForbidden, gin.H{"error": "Recovery hanya dapat dijalankan oleh user client; admin platform tidak dapat mengeksekusi recovery."})
		return false
	}
	if strings.TrimSpace(h.userID(c)) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas user pada token tidak valid."})
		return false
	}
	return true
}

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
