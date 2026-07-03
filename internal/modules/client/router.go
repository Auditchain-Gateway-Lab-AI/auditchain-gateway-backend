package client

import "github.com/gin-gonic/gin"

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler) {
	adminRoutes := routerGroup.Group("/admin")
	{
		adminRoutes.POST("/clients", h.CreateClient)
		adminRoutes.POST("/kafka-config", h.CreateKafkaConfig)
		adminRoutes.PATCH("/kafka-config/:id/toggle", h.ToggleKafkaConfig)
	}
}
