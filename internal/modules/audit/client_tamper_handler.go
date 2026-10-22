package audit

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// ListClientTamperRecords godoc
// @Summary Daftar Insiden Tampered Client Data
// @Description Mengambil daftar anomali/perbedaan antara hash data aktual database klien vs hash log terbaru offchain.
// @Tags Client Tampering
// @Produce json
// @Security BearerAuth
// @Param status query string false "Filter status (OPEN, RECOVERED, DISMISSED, ALL)"
// @Param table_name query string false "Filter nama tabel"
// @Param resource query string false "Filter nama resource (TABLE:ID)"
// @Param search query string false "Pencarian kata kunci"
// @Param page query int false "Nomor halaman (default 1)"
// @Param page_size query int false "Ukuran halaman (default 10)"
// @Success 200 {object} ClientTamperListResponse
// @Failure 401 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /dashboard/client-tampering [get]
func (h *Handler) ListClientTamperRecords(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	var query ClientTamperQuery
	if err := c.ShouldBindQuery(&query); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Parameter query tidak valid: " + err.Error()})
		return
	}

	resp, err := h.Service.ListClientTamperRecords(clientID, query)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar tamper data klien: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetClientTamperRecord godoc
// @Summary Detail Insiden Tampered Client Data
// @Description Mengambil detail spesifik satu insiden manipulasi data klien beserta ringkasan diskrepansi per kolom.
// @Tags Client Tampering
// @Produce json
// @Security BearerAuth
// @Param id path string true "ID Rekaman Tamper"
// @Success 200 {object} models.ClientDataTamperRecord
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /dashboard/client-tampering/{id} [get]
func (h *Handler) GetClientTamperRecord(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID wajib diisi"})
		return
	}

	record, err := h.Service.GetClientTamperRecord(clientID, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || err.Error() == "record_not_found" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Data insiden tamper tidak ditemukan"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil data tamper: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, record)
}

// RecoverClientTamperRecord godoc
// @Summary Pemulihan (Recovery) Data Klien yang Tampered
// @Description Mengirimkan instruksi pemulihan ke Agent klien menggunakan nilai metadata bersih dari log offchain terverifikasi.
// @Tags Client Tampering
// @Produce json
// @Security BearerAuth
// @Param id path string true "ID Rekaman Tamper"
// @Success 200 {object} ClientTamperRecoverResponse
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /dashboard/client-tampering/{id}/recover [post]
func (h *Handler) RecoverClientTamperRecord(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID wajib diisi"})
		return
	}

	recoveredBy := "dashboard_user"
	if username, exists := c.Get("username"); exists {
		if uStr, ok := username.(string); ok && uStr != "" {
			recoveredBy = uStr
		}
	}

	resp, err := h.Service.RecoverClientTamperRecord(c.Request.Context(), clientID, id, recoveredBy)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || err.Error() == "record_not_found" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Data insiden tamper tidak ditemukan"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memulihkan data klien: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// DismissClientTamperRecord godoc
// @Summary Abaikan (Dismiss) Insiden Tampered Data Klien
// @Description Menandai insiden tamper sebagai diabaikan/dismissed dengan catatan penjelasan.
// @Tags Client Tampering
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param id path string true "ID Rekaman Tamper"
// @Param body body DismissClientTamperInput false "Catatan alasan pengabaian"
// @Success 200 {object} models.ClientDataTamperRecord
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Failure 500 {object} map[string]string
// @Router /dashboard/client-tampering/{id}/dismiss [post]
func (h *Handler) DismissClientTamperRecord(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID wajib diisi"})
		return
	}

	var input DismissClientTamperInput
	_ = c.ShouldBindJSON(&input)

	dismissedBy := "dashboard_user"
	if username, exists := c.Get("username"); exists {
		if uStr, ok := username.(string); ok && uStr != "" {
			dismissedBy = uStr
		}
	}

	record, err := h.Service.DismissClientTamperRecord(clientID, id, dismissedBy, input.Notes)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) || err.Error() == "record_not_found" {
			c.JSON(http.StatusNotFound, gin.H{"error": "Data insiden tamper tidak ditemukan"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengabaikan insiden: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, record)
}
