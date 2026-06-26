package api

import (
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/modules/audit"
	"go-blockchain-api/internal/modules/auth"
	"go-blockchain-api/internal/modules/client"

	_ "go-blockchain-api/docs"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

func SetupRouter(
	auditHandler *audit.Handler,
	authHandler *auth.Handler,
	clientHandler *client.Handler,
	agentHandler *agentverifier.Handler,
) *gin.Engine {
	router := gin.Default()

	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	router.Use(cors.New(corsConfig))

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	apiGroup := router.Group("/api")
	auth.RegisterRoutes(apiGroup, authHandler)
	client.RegisterRoutes(apiGroup, clientHandler)
	audit.RegisterRoutes(apiGroup, auditHandler)

	agentverifier.RegisterRoutes(apiGroup.Group("/dashboard"), agentHandler)

	return router
}
