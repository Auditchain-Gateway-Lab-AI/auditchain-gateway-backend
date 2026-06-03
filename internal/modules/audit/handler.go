package audit

import (
	"net/http"

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

	return clientID, true
}

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

// VerifyLog mengecek integritas log dari database hingga ke catatan Blockchain.
// (Komentar Swagger biarkan utuh seperti milik Anda)
func (h *Handler) VerifyLog(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	requestedHash := c.Param("hash")

	result, err := h.Service.VerifyLogIntegrity(requestedHash, clientID)

	// Tangani error sistem
	if err != nil {
		switch err.Error() {
		case "log_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Log tidak ditemukan di dalam sistem."})
		case "fabric_error":
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal terhubung ke jaringan Blockchain Fabric."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Terjadi kesalahan sistem saat memverifikasi data."})
		}
		return
	}

	// Tangani hasil logika bisnis (Business Logic)
	switch result.Status {
	case "failed_local":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed",
			"data": gin.H{
				"is_valid":      result.IsValid,
				"message":       result.Message,
				"expected_hash": result.ExpectedHash,
				"actual_hash":   result.ActualHash,
			},
		})
	case "pending":
		c.JSON(http.StatusAccepted, gin.H{
			"status":  "pending",
			"message": result.Message,
		})
	case "failed_onchain":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed",
			"data": gin.H{
				"is_valid":   result.IsValid,
				"message":    result.Message,
				"db_root":    result.DBRoot,
				"chain_root": result.ChainRoot,
			},
		})
	case "success":
		c.JSON(http.StatusOK, gin.H{
			"status": "success",
			"data": gin.H{
				"log_id":           result.LogID,
				"hash_value":       result.ExpectedHash,
				"merkle_root":      result.DBRoot,
				"blockchain_tx_id": result.TxID,
				"is_valid":         result.IsValid,
				"message":          result.Message,
			},
		})
	}
}

func (h *Handler) GetFabricRecord(c *gin.Context) {
	anchorID := c.Param("anchor_id")

	data, err := h.Service.GetFabricRecord(anchorID)
	if err != nil {
		switch err.Error() {
		case "fabric_bypass":
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Koneksi ke jaringan Hyperledger Fabric sedang terputus (Bypass Mode)"})
		case "fabric_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Data tidak ditemukan di dalam Ledger Fabric"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses data dari Fabric"})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"source": "Hyperledger Fabric World State",
		"data":   data,
	})
}

type VerifyDataRequest struct {
	Resource string                  `json:"resource" binding:"required" example:"tabel_cuti_pegawai:id:123"`
	Data     *map[string]interface{} `json:"data"`
}

// VerifyData menerima payload data aktual dari klien untuk dicek integritasnya
func (h *Handler) VerifyData(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	var req VerifyDataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid. Pastikan atribut 'resource' diisi."})
		return
	}

	// Memanggil service dengan mengirimkan pointer req.Data
	result, err := h.Service.VerifyDataIntegrity(req.Resource, clientID, req.Data)
	if err != nil {
		switch err.Error() {
		case "log_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Tidak ada rekam jejak audit untuk resource tersebut di sistem."})
		case "no_data_hash_in_log":
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "Log terakhir untuk resource ini tidak memiliki atribut data_hash."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memverifikasi integritas data."})
		}
		return
	}

	// Mengembalikan respons sesuai status IsValid
	if result.IsValid {
		c.JSON(http.StatusOK, result)
	} else {
		// Gunakan 409 Conflict jika data terbukti dimanipulasi
		c.JSON(http.StatusConflict, result)
	}
}

func (h *Handler) GetRecentLogs(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	logs, err := h.Service.GetRecentLogs(500, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar log terbaru"})
		return
	}
	c.JSON(http.StatusOK, logs)
}

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

// Tambahkan di bagian bawah file handler.go
func (h *Handler) VerifyResourceHistory(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}

	resource := c.Param("resource")

	result, err := h.Service.VerifyResourceHistory(resource, clientID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Riwayat resource tidak ditemukan."})
		return
	}

	if result.IsValid {
		if result.Status == "pending" {
			c.JSON(http.StatusAccepted, result)
		} else {
			c.JSON(http.StatusOK, result)
		}
	} else {
		c.JSON(http.StatusConflict, result)
	}
}

func (h *Handler) GetLogsByResource(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	resource := c.Param("resource")
	logs, err := h.Service.GetLogsByResource(resource, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil log resource"})
		return
	}
	c.JSON(http.StatusOK, logs)
}
