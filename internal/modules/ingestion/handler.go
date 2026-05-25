package ingestion

import (
	"fmt"
	"net/http"

	"go-blockchain-api/internal/engine/normalizer"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Handler struct {
	Service *Service
	DB      *gorm.DB
}

// ReceiveLog menerima kumpulan log mentah dari sistem eksternal
// @Summary Bulk Ingestion Log Audit
// @Description Menerima raw log audit dalam bentuk Array dan memasukkannya ke antrean Redis secara asinkron.
// @Tags Ingestion
// @Accept json
// @Produce json
// @Security ApiKeyAuth
// @Param request body array true "Array Payload Raw Log Dinamis dari Klien"
// @Success 202 {object} map[string]interface{} "Log diterima"
// @Router /v1/logs [post]
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
		Select("actor_field, action_field, resource_field").
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
			fmt.Printf("❌ [ERROR MAPPING]: %v\n", err)
			errorCount++
			continue
		}

		input.ClientID = clientID

		if sourceSys, ok := payload["source_system"].(string); ok && sourceSys != "" {
			input.SourceSystem = sourceSys
		} else {
			input.SourceSystem = "SatuPeta_Agent_Auto"
		}

		if _, err = h.Service.ProcessLog(input); err != nil {
			fmt.Printf("❌ [ERROR SERVICE]: %v\n", err)
			errorCount++
			continue
		}

		successCount++
	}

	c.JSON(http.StatusAccepted, gin.H{
		"message":        "Proses bulk ingestion selesai",
		"total_received": len(dynamicPayloads),
		"total_success":  successCount,
		"total_failed":   errorCount,
	})
}
