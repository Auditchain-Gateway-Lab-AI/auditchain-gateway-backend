package clientverify

import (
	"go-blockchain-api/internal/middleware"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler, db *gorm.DB) {
	// Endpoint ini digunakan oleh klien (dashboard klien) sehingga
	// menggunakan autentikasi yang berlaku untuk klien (API Key atau JWT klien).
	// Di sini kita gunakan APIKeyAuth dan JWTAuth sebagai opsi jika klien
	// menggunakan keduanya.
	clientAPI := routerGroup.Group("/client")
	
	// Gunakan middleware auth. Pastikan middleware menset client_id
	clientAPI.Use(middleware.APIKeyAuth(db)) 
	{
		clientAPI.POST("/verify-table", h.VerifyTable)
	}
}
