package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"go-blockchain-api/internal/models"
)

// APIKeyAuth melindungi rute Machine-to-Machine (M2M) via API Key
func APIKeyAuth(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		clientKey := c.GetHeader("x-api-key")
		if clientKey == "" {
			clientKey = c.GetHeader("api-key")
		}

		// Panjang minimum: prefix 10 + underscore + minimal 32 char hash hex
		if clientKey == "" || len(clientKey) < 43 {
			c.JSON(http.StatusUnauthorized, gin.H{
				"status":  "error",
				"message": "Akses Ditolak: API Key tidak valid atau tidak ditemukan di Header",
			})
			c.Abort()
			return
		}

		prefix := clientKey[:10]
		hashBytes := sha256.Sum256([]byte(clientKey))
		hashedKey := hex.EncodeToString(hashBytes[:])

		var client models.Client
		err := db.Where(
			"api_key_prefix = ? AND api_key_hash = ? AND status = ?",
			prefix, hashedKey, "active",
		).First(&client).Error

		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{
				"status":  "error",
				"message": "Akses Ditolak: API Key tidak dikenali atau akun klien tidak aktif",
			})
			c.Abort()
			return
		}

		c.Set("client_id", client.ID)
		c.Next()
	}
}
