package ingestion

import (
	"fmt"
	"log"
	"net/http"

	"go-blockchain-api/internal/engine/normalizer"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Handler struct {
	Service *Service
	DB      *gorm.DB
}

type ErrorResponse struct {
	Error string `json:"error" example:"Format JSON tidak valid, harus berupa Array Objek"`
}

type IngestionResponse struct {
	Message       string `json:"message" example:"Proses bulk ingestion selesai"`
	TotalReceived int    `json:"total_received" example:"100"`
	TotalSuccess  int    `json:"total_success" example:"98"`
	TotalFailed   int    `json:"total_failed" example:"2"`
}

// @Summary Receive batch logs
// @Description Menerima array of log dinamis dari sistem klien (Agent atau Kafka) untuk diproses dan diverifikasi ke blockchain.
// @Tags Ingestion
// @Accept json
// @Produce json
// @Security BearerAuth
// @Param request body []map[string]interface{} true "Array of Log Data"
// @Success 202 {object} IngestionResponse "Log diterima dan sedang diproses"
// @Failure 400 {object} ErrorResponse "Format JSON tidak valid atau array kosong"
// @Failure 500 {object} ErrorResponse "Identitas klien tidak ditemukan atau gagal memproses konfigurasi"
// @Router /ingestion [post]
func (h *Handler) ReceiveLog(c *gin.Context) {
	clientIDVal, exists := c.Get("client_id")
	if !exists {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Identitas klien tidak ditemukan oleh sistem"})
		return
	}
	clientID := clientIDVal.(string)

	var dynamicPayloads []map[string]interface{}
	if err := c.ShouldBindJSON(&dynamicPayloads); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format JSON tidak valid, harus berupa Array Objek (Bulk)"})
		return
	}

	if len(dynamicPayloads) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Array log kosong"})
		return
	}

	var mapping normalizer.ClientFieldMapping
	err := h.DB.Table("clients").
		Select("actor_field, fallback_actor_field, action_field, resource_field").
		Where("id = ?", clientID).
		Scan(&mapping).Error

	if err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil konfigurasi pemetaan klien"})
		return
	}

	var successCount, errorCount int

	for _, payload := range dynamicPayloads {
		input, err := normalizer.MapDynamicPayload(payload, &mapping)
		if err != nil {
			log.Printf("❌ [Ingestion] Gagal mapping payload client=%s: %v", clientID, err)
			errorCount++
			continue
		}

		input.ClientID = clientID

		if sourceSys, ok := payload["source_system"].(string); ok && sourceSys != "" {
			input.SourceSystem = sourceSys
		} else {
			input.SourceSystem = "Unknown-Agent"
		}

		if _, err = h.Service.ProcessLog(input); err != nil {
			log.Printf("❌ [Ingestion] Gagal proses log client=%s resource=%s: %v",
				clientID, input.Resource, err)
			errorCount++
			continue
		}

		successCount++
	}

	// Log setiap batch yang masuk agar mudah di-trace di backend
	log.Printf("📥 [Ingestion] client=%s diterima=%d sukses=%d gagal=%d",
		clientID, len(dynamicPayloads), successCount, errorCount)

	c.JSON(http.StatusAccepted, gin.H{
		"message":        "Proses bulk ingestion selesai",
		"total_received": len(dynamicPayloads),
		"total_success":  successCount,
		"total_failed":   errorCount,
	})
}

// suppress unused import warning
var _ = fmt.Sprintf
