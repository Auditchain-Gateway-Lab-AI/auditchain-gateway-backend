package audit

import (
	"go-blockchain-api/internal/middleware"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	dashAPI := routerGroup.Group("/dashboard")
	dashAPI.Use(middleware.JWTAuth())
	{
		dashAPI.GET("/stats", h.GetStats)
		dashAPI.GET("/logs", h.GetRecentLogs)
		dashAPI.GET("/logs/by-resource/:resource", h.GetLogsByResource)
		dashAPI.GET("/verify/:hash", h.VerifyLog)
		dashAPI.GET("/fabric/:anchor_id", h.GetFabricRecord)
		dashAPI.POST("/verify-data", h.VerifyData)
		dashAPI.GET("/inventory", h.GetResourceInventory)
		dashAPI.GET("/verify-resource/:resource", h.VerifyResourceHistory)
	}
}
