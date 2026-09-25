package clientverify

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

type VerifyTableRequest struct {
	TableName string `json:"table_name" binding:"required"`
}

func (h *Handler) VerifyTable(c *gin.Context) {
	// Dapatkan client_id dari API Key middleware atau JWT middleware
	// Asumsi middleware auth sudah men-set "client_id" ke dalam gin context
	clientIDVal, exists := c.Get("client_id")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas klien tidak ditemukan pada token"})
		return
	}
	clientID, ok := clientIDVal.(string)
	if !ok || clientID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Identitas klien tidak valid"})
		return
	}

	var req VerifyTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid, pastikan 'table_name' disertakan"})
		return
	}

	response, err := h.Service.VerifyTable(clientID, req.TableName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, response)
}
