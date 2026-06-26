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
// @host localhost:8080
// @BasePath /api
package main

import (
	"context"
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
	"go-blockchain-api/internal/engine/kafkaconsumer"
	"go-blockchain-api/internal/modules/audit"
	"go-blockchain-api/internal/modules/auth"
	"go-blockchain-api/internal/modules/client"
)

func startPipelineWorker(ctx context.Context, db *gorm.DB, fabricSvc *blockchain.FabricService) {
	hashEngine := &hasher.Engine{DB: db}
	aggEngine := &aggregator.Engine{DB: db}
	kafkaEngine := &kafkaconsumer.Engine{DB: db}

	// Ticker pipeline: hasher + aggregator (batch=10) + anchoring setiap 10 detik
	go func() {
		log.Println("⚙️  Pipeline Worker mulai berjalan...")
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("⚙️  Pipeline Worker berhenti.")
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

	// Kafka consumer: satu goroutine per klien aktif
	go func() {
		if err := kafkaEngine.StartConsumers(ctx); err != nil {
			log.Printf("⚠️  [KafkaConsumer] Error start: %v\n", err)
		}
	}()
}

func main() {
	godotenv.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db := config.ConnectDB()

	fabricSvc, err := blockchain.InitFabricGateway(db)
	if err != nil {
		log.Printf("⚠️  Gagal terhubung ke Fabric: %v\n", err)
	} else {
		defer fabricSvc.Close()
	}

	startPipelineWorker(ctx, db, fabricSvc)

	auditRepo := audit.NewAuditRepository(db)
	auditService := audit.NewService(auditRepo, fabricSvc, db)
	auditHandler := audit.NewHandler(auditService)

	authRepo := auth.NewRepository(db)
	authService := auth.NewService(authRepo)
	authHandler := &auth.Handler{Service: authService}

	clientRepo := client.NewRepository(db)
	clientService := client.NewService(clientRepo)
	clientHandler := &client.Handler{
		Service: clientService,
		DB:      db,
	}

	agentHandler := agentverifier.NewHandler(db)

	router := api.SetupRouter(auditHandler, authHandler, clientHandler, agentHandler)

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
