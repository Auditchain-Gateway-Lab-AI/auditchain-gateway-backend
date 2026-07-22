package client

import (
	"go-blockchain-api/internal/middleware"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	adminRoutes := routerGroup.Group("/admin", middleware.AdminAuth())
	{
		adminRoutes.POST("/clients", h.CreateClient)
		adminRoutes.GET("/clients", h.ListClients)
		adminRoutes.POST("/kafka-config", h.CreateKafkaConfig)
		adminRoutes.GET("/kafka-configs", h.ListKafkaConfigs)
		adminRoutes.PATCH("/kafka-config/:id/toggle", h.ToggleKafkaConfig)
		adminRoutes.GET("/summary", h.GetDashboardSummary)
		adminRoutes.DELETE("/kafka-config/:id", h.DeleteKafkaConfig)
		adminRoutes.PATCH("/clients/:id/toggle", h.ToggleClientStatus)
		adminRoutes.DELETE("/clients/:id", h.DeleteClient)
		adminRoutes.GET("/clients/:id/users", h.GetClientUsers)
		adminRoutes.POST("/clients/:id/users", h.CreateClientUser)
		adminRoutes.DELETE("/users/:id", h.DeleteClientUser)

		// Agent Config Routes
		adminRoutes.POST("/clients/:id/agent-config", h.CreateAgentConfig)
		adminRoutes.GET("/clients/:id/agent-config", h.GetAgentConfig)
		adminRoutes.DELETE("/clients/:id/agent-config", h.DeleteAgentConfig)
		adminRoutes.GET("/clients/:id/agent-ping", h.PingAgentConfig)
	}
}

