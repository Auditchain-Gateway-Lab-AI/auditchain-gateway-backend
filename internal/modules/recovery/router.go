package recovery

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"

	"go-blockchain-api/internal/middleware"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("RECOVERY_ENABLED")), "true") {
		return
	}

	readRoutes := routerGroup.Group("/dashboard/recovery", middleware.JWTAuth())
	{
		readRoutes.GET("/incidents", h.ListIncidents)
		readRoutes.GET("/incidents/:id", h.GetIncident)
		readRoutes.GET("/incidents/:id/candidates", h.ListCandidates)
		readRoutes.POST("/incidents/:id/preflight", h.Preflight)
		readRoutes.GET("/resources/:resource/versions", h.ListVersions)
		readRoutes.GET("/requests", h.ListRequests)
		readRoutes.GET("/requests/:id", h.GetRequest)
		readRoutes.POST("/requests", h.CreateRequest)
	}

	adminRoutes := routerGroup.Group("/dashboard/recovery", middleware.AdminAuth())
	{
		adminRoutes.POST("/requests/:id/approve", h.Approve)
		adminRoutes.POST("/requests/:id/reject", h.Reject)
		adminRoutes.POST("/requests/:id/execute", h.Execute)
	}
}
