package audit

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Service Service
}

type RecentLogResponse struct {
	LogID          string  `json:"log_id"`
	ClientID       string  `json:"client_id"`
	Actor          string  `json:"actor"`
	Action         string  `json:"action"`
	SourceTable    string  `json:"source_table"`
	SourceSystem   string  `json:"source_system"`
	Timestamp      string  `json:"timestamp"`
	HashValue      string  `json:"hash_value"`
	PreviousHash   string  `json:"previous_hash"`
	MerkleRoot     string  `json:"merkle_root"`
	BlockchainTxID *string `json:"blockchain_tx_id"`
	Status         string  `json:"status"`
	Metadata       string  `json:"metadata"`
}

func NewHandler(service Service) *Handler {
	return &Handler{Service: service}
}

// parseSourceTable mengambil nama tabel saja dari resource "tabel:id".
// Tidak mengubah data di DB — resource tetap tersimpan format aslinya.
func parseSourceTable(resource string) string {
	if idx := strings.Index(resource, ":"); idx != -1 {
		return resource[:idx]
	}
	return resource
}

func (h *Handler) GetRecentLogs(c *gin.Context) {
	clientID, ok := h.getClientID(c)
	if !ok {
		return
	}
	logs, err := h.Service.GetRecentLogs(500, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil log terbaru"})
		return
	}

	response := make([]RecentLogResponse, 0, len(logs))
	for _, l := range logs {
		response = append(response, RecentLogResponse{
			LogID:          l.LogID,
			ClientID:       l.ClientID,
			Actor:          l.Actor,
			Action:         l.Action,
			SourceTable:    parseSourceTable(l.Resource),
			SourceSystem:   l.SourceSystem,
			Timestamp:      formatPgTimestamp(l.Timestamp), // full precision, sudah ada di service.go
			HashValue:      l.HashValue,
			MerkleRoot:     l.MerkleRoot,
			BlockchainTxID: l.BlockchainTxID,
			Status:         l.Status,
			Metadata:       l.Metadata,
		})
	}
	c.JSON(http.StatusOK, response)
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
	case "failed_kafka":
		c.JSON(http.StatusConflict, gin.H{
			"status": "failed", "layer": "3_kafka",
			"data": gin.H{
				"is_valid":     result.IsValid,
				"log_id":       result.LogID,
				"message":      result.Message,
				"kafka_hash":   result.KafkaHash,
				"kafka_topic":  result.KafkaTopic,
				"kafka_offset": result.KafkaOffset,
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
	result, err := h.Service.VerifyResourceHistory(c.Param("resource"), clientID)
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
	logs, err := h.Service.GetLogsByResource(c.Param("resource"), clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil log resource"})
		return
	}
	c.JSON(http.StatusOK, logs)
}

func parseTimeRobust(timeStr string) (time.Time, error) {
	// 1. Pastikan string cukup panjang
	if len(timeStr) > 10 {
		// Jika karakter ke-11 adalah spasi (pemisah tanggal & jam), ganti jadi 'T'
		if timeStr[10] == ' ' {
			timeStr = timeStr[:10] + "T" + timeStr[11:]
		}
	}

	// 2. Jebakan HTTP: URL mengubah '+' menjadi spasi.
	// Jika masih ada spasi yang tersisa di belakang (misal " 07"), kita kembalikan jadi '+'
	timeStr = strings.ReplaceAll(timeStr, " ", "+")

	// 3. Coba parse dengan standar ketat RFC3339
	t, err := time.Parse(time.RFC3339, timeStr)
	if err == nil {
		return t, nil
	}

	// 4. Jika gagal, gunakan custom layout untuk toleransi zona waktu +07 (tanpa menit)
	customLayout := "2006-01-02T15:04:05.999999999Z07"
	return time.Parse(customLayout, timeStr)
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

	// Gunakan helper function yang sudah dimodifikasi
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
