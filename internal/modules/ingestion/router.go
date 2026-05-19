package ingestion

import (
	"go-blockchain-api/internal/middleware"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	logsRoutes := routerGroup.Group("/logs")
	logsRoutes.Use(middleware.APIKeyAuth(h.DB))
	logsRoutes.Use(middleware.RateLimiter(50)) // default 50 req/s, akan di-override per klien nanti
	{
		logsRoutes.POST("/", h.ReceiveLog)
	}
}
