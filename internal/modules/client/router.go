package client

import "github.com/gin-gonic/gin"

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	adminRoutes := routerGroup.Group("/admin")
	{
		adminRoutes.POST("/clients", h.CreateClient)
		adminRoutes.GET("/clients", h.ListClients)
		adminRoutes.POST("/kafka-config", h.CreateKafkaConfig)
		adminRoutes.GET("/kafka-configs", h.ListKafkaConfigs)
		adminRoutes.PATCH("/kafka-config/:id/toggle", h.ToggleKafkaConfig)
		adminRoutes.GET("/summary", h.GetDashboardSummary)
		adminRoutes.DELETE("/kafka-config/:id", h.DeleteKafkaConfig)
	}
}
