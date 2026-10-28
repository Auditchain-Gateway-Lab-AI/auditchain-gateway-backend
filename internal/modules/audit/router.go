package audit

import (
	"go-blockchain-api/internal/middleware"
	"gorm.io/gorm"

	"github.com/gin-gonic/gin"
)

func RegisterRoutes(routerGroup *gin.RouterGroup, h *Handler, db ...*gorm.DB) {
	var gormDB *gorm.DB
	if len(db) > 0 {
		gormDB = db[0]
	}
	agentAPI := routerGroup.Group("/agent")
	if gormDB != nil {
		agentAPI.Use(middleware.AgentAuth(gormDB))
	} else {
		agentAPI.Use(middleware.JWTAuth())
	}
	{
		agentAPI.GET("/offchain-data", h.GetOffchainDataForAgent)
		agentAPI.GET("/offchain-data/:resource", h.GetOffchainDataForAgent)
		agentAPI.POST("/verify-merkle-root", h.VerifyMerkleRootForAgent)
		agentAPI.POST("/audit-report", h.SubmitAgentAuditReport)
	}

	dashAPI := routerGroup.Group("/dashboard")
	dashAPI.Use(middleware.JWTAuth())
	{
		// dashAPI.GET("/stats", h.GetStats) (Moved to client module)
		dashAPI.GET("/logs", h.GetRecentLogs)
		dashAPI.GET("/logs/by-resource/:resource", h.GetLogsByResource)
		dashAPI.GET("/verify/:log_id", h.VerifyLog)
		dashAPI.GET("/fabric/:anchor_id", h.GetFabricRecord)
		dashAPI.POST("/verify-data", h.VerifyData)
		dashAPI.GET("/inventory", h.GetResourceInventory)
		dashAPI.GET("/verify-resource/:resource", h.VerifyResourceHistory)
		dashAPI.GET("/verify-range/estimate", h.EstimateLogRange)
		dashAPI.GET("/verify-range/internal", h.VerifyInternalLogRange)
		dashAPI.GET("/verify-range/client", h.VerifyClientLogRange)
		dashAPI.POST("/verification-runs", h.CreateVerificationRun)
		dashAPI.POST("/agent/audit-run", h.RunAgentAudit)
		dashAPI.GET("/verification-runs/latest", h.GetLatestVerificationRun)
		dashAPI.GET("/verification-runs/:id", h.GetVerificationRun)
		// Compatibility route for older gateway-dashboard bundles. New clients
		// should call /verify-range/internal explicitly.
		dashAPI.GET("/verify-range", h.VerifyLegacyLogRange)

		// Client Data Tampering Endpoints (actual client DB hash vs latest offchain log hash)
		dashAPI.GET("/client-tampering", h.ListClientTamperRecords)
		dashAPI.GET("/client-tampering/:id", h.GetClientTamperRecord)
		dashAPI.POST("/client-tampering/:id/recover", h.RecoverClientTamperRecord)
		dashAPI.POST("/client-tampering/:id/dismiss", h.DismissClientTamperRecord)

		// Client Recovery Events (immutable execution history of client DB recoveries)
		dashAPI.GET("/client-recovery/events", h.ListClientRecoveryEvents)
	}
}
