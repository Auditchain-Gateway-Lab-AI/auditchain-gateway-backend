// @title AuditChain Gateway API
// @version 1.0
// @description API Enterprise untuk sistem audit log berbasis Blockchain dan Merkle Tree.
// @termsOfService http://swagger.io/terms/
// @contact.name API Support
// @contact.email support@auditchain.local
// @securityDefinitions.apikey BearerAuth
// @in header
// @name Authorization
// @description Masukkan token dengan format: Bearer {token}
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name api-key
// @host localhost:8080
// @BasePath /api
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"gorm.io/gorm"

	"go-blockchain-api/internal/api"
	"go-blockchain-api/internal/blockchain"
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/config"
	"go-blockchain-api/internal/engine/aggregator"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/modules/audit"
	"go-blockchain-api/internal/modules/auth"
	"go-blockchain-api/internal/modules/client"
	"go-blockchain-api/internal/modules/ingestion"

	"github.com/redis/go-redis/v9"
)

func startPipelineWorker(ctx context.Context, db *gorm.DB, fabricSvc *blockchain.FabricService, redisClient *redis.Client) {
	hashEngine := &hasher.Engine{DB: db}
	aggEngine := &aggregator.Engine{DB: db}

	go func() {
		log.Println("⚙️ Background Pipeline Worker mulai berjalan...")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("⚙️ Pipeline Worker berhenti.")
				return
			case <-ticker.C:
				if err := hashEngine.ProcessPendingLogs(); err != nil {
					log.Printf("❌ [Hasher] Error: %v\n", err)
				}
				if err := aggEngine.ProcessBatch(10); err != nil {
					log.Printf("❌ [Aggregator] Error: %v\n", err)
				}
				if fabricSvc != nil {
					if err := fabricSvc.AnchorPendingRoots(); err != nil {
						log.Printf("❌ [Anchoring] Error: %v\n", err)
					}
				}
			}
		}
	}()

	if redisClient == nil {
		return
	}

	go func() {
		log.Println("📥 Redis Queue Worker mulai berjalan...")
		for {
			result, err := redisClient.BLPop(ctx, 2*time.Second, "audit_log_queue").Result()
			if err != nil {
				if err == redis.Nil {
					select {
					case <-ctx.Done():
						return
					default:
						continue
					}
				}
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️ Error membaca dari Redis: %v\n", err)
				time.Sleep(1 * time.Second)
				continue
			}
			if len(result) < 2 {
				continue
			}
			var logData models.AuditLog
			if err := json.Unmarshal([]byte(result[1]), &logData); err != nil {
				log.Printf("⚠️ Gagal parse log dari Redis: %v\n", err)
				continue
			}
			if err := db.Create(&logData).Error; err != nil {
				log.Printf("⚠️ Gagal simpan log %s: %v\n", logData.HashValue, err)
			}
		}
	}()
}

func main() {
	godotenv.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db := config.ConnectDB()
	redisClient := config.ConnectRedis()

	fabricSvc, err := blockchain.InitFabricGateway(db)
	if err != nil {
		log.Printf("⚠️ Gagal terhubung ke Fabric: %v\n", err)
	} else {
		defer fabricSvc.Close()
	}

	startPipelineWorker(ctx, db, fabricSvc, redisClient)

	// === Modul Audit + Lapis 3 ===
	agentVerifySvc := agentverifier.NewService(db)
	auditRepo := audit.NewAuditRepository(db)
	auditService := audit.NewService(auditRepo, fabricSvc, agentVerifySvc)
	auditHandler := audit.NewHandler(auditService)

	// === Modul Auth ===
	authRepo := auth.NewRepository(db)
	authService := auth.NewService(authRepo)
	authHandler := &auth.Handler{Service: authService}

	// === Modul Ingestion ===
	ingestionRepo := ingestion.NewRepository(redisClient)
	ingestionService := ingestion.NewService(ingestionRepo)
	ingestionHandler := &ingestion.Handler{Service: ingestionService, DB: db}

	// === Modul Client ===
	clientRepo := client.NewRepository(db)
	clientService := client.NewService(clientRepo)
	clientHandler := client.NewHandler(clientService)

	// === Handler konfigurasi Agent ===
	agentHandler := agentverifier.NewHandler(db)

	router := api.SetupRouter(ingestionHandler, auditHandler, authHandler, clientHandler, agentHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{Addr: "0.0.0.0:" + port, Handler: router}

	go func() {
		log.Printf("🚀 AuditChain Gateway berjalan di port %s...\n", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v\n", err)
		}
	}()

	<-ctx.Done()
	log.Println("🛑 Sinyal shutdown diterima...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
	log.Println("✅ Server berhenti dengan bersih.")
}
