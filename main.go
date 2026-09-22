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
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	"go-blockchain-api/internal/engine/snapshotworker"
	"go-blockchain-api/internal/engine/tamperscanner"
	"go-blockchain-api/internal/modules/audit"
	"go-blockchain-api/internal/modules/auth"
	"go-blockchain-api/internal/modules/client"
	"go-blockchain-api/internal/modules/recovery"
	"go-blockchain-api/internal/modules/report"
	"go-blockchain-api/internal/storage/snapshotstore"
)

func snapshotWriterEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("SNAPSHOT_WRITER_ENABLED")), "true")
}

func snapshotRequiredForAnchor() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("SNAPSHOT_REQUIRED_FOR_ANCHOR")), "true")
}

func recoveryEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("RECOVERY_ENABLED")), "true")
}

func tamperScannerEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("TAMPER_SCANNER_ENABLED")), "true")
}

func validateRecoveryScopeFlags(recovery, snapshotRequired bool, cutoff *time.Time) error {
	if (recovery || snapshotRequired) && cutoff == nil {
		return fmt.Errorf("RECOVERY_CUTOFF_AT wajib diisi saat recovery atau anchoring gate aktif")
	}
	if recovery && !snapshotRequired {
		return fmt.Errorf("RECOVERY_ENABLED=true membutuhkan SNAPSHOT_REQUIRED_FOR_ANCHOR=true")
	}
	return nil
}

func buildSnapshotRuntime() (snapshotstore.OutboxBuilder, snapshotstore.SnapshotStore, *snapshotstore.Cipher, error) {
	if !snapshotWriterEnabled() {
		return nil, nil, nil, nil
	}

	minioCfg, err := config.LoadMinIOConfig()
	if err != nil {
		return nil, nil, nil, err
	}
	keyID := strings.TrimSpace(os.Getenv("SNAPSHOT_ENCRYPTION_ACTIVE_KEY_ID"))
	encodedKey := strings.TrimSpace(os.Getenv("SNAPSHOT_ENCRYPTION_KEY"))
	cipher, err := snapshotstore.NewCipherFromEncodedKey(keyID, encodedKey)
	if err != nil {
		return nil, nil, nil, err
	}
	store, err := snapshotstore.NewMinIOStore(minioCfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return snapshotstore.AuditSnapshotOutboxBuilder{Cipher: cipher}, store, cipher, nil
}

func loadSnapshotWorkerConfig() (snapshotworker.Config, error) {
	parsePositiveInt := func(name string, fallback int) (int, error) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return fallback, nil
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return 0, fmt.Errorf("%s harus berupa bilangan bulat positif", name)
		}
		return value, nil
	}

	maxAttempts, err := parsePositiveInt("SNAPSHOT_MAX_ATTEMPTS", 10)
	if err != nil {
		return snapshotworker.Config{}, err
	}
	concurrency, err := parsePositiveInt("SNAPSHOT_WORKER_CONCURRENCY", 1)
	if err != nil {
		return snapshotworker.Config{}, err
	}
	retrySeconds, err := parsePositiveInt("SNAPSHOT_RETRY_BASE_SECONDS", 5)
	if err != nil {
		return snapshotworker.Config{}, err
	}
	pollSeconds, err := parsePositiveInt("SNAPSHOT_POLL_INTERVAL_SECONDS", 2)
	if err != nil {
		return snapshotworker.Config{}, err
	}

	return snapshotworker.Config{
		Environment:  os.Getenv("APP_ENV"),
		MaxAttempts:  maxAttempts,
		Concurrency:  concurrency,
		RetryBase:    time.Duration(retrySeconds) * time.Second,
		PollInterval: time.Duration(pollSeconds) * time.Second,
	}, nil
}

func loadTamperScannerConfig(recoveryCutoff *time.Time) (tamperscanner.Config, error) {
	parsePositiveInt := func(name string, fallback int) (int, error) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return fallback, nil
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return 0, fmt.Errorf("%s harus berupa bilangan bulat positif", name)
		}
		return value, nil
	}

	intervalSeconds, err := parsePositiveInt("TAMPER_SCAN_INTERVAL_SECONDS", 300)
	if err != nil {
		return tamperscanner.Config{}, err
	}
	batchSize, err := parsePositiveInt("TAMPER_SCAN_BATCH_SIZE", 100)
	if err != nil {
		return tamperscanner.Config{}, err
	}
	concurrency, err := parsePositiveInt("TAMPER_SCAN_CONCURRENCY", 2)
	if err != nil {
		return tamperscanner.Config{}, err
	}
	return tamperscanner.Config{
		Enabled:        tamperScannerEnabled(),
		Interval:       time.Duration(intervalSeconds) * time.Second,
		BatchSize:      batchSize,
		Concurrency:    concurrency,
		RecoveryCutoff: recoveryCutoff,
	}, nil
}

func startPipelineWorker(ctx context.Context, db *gorm.DB, fabricSvc *blockchain.FabricService, snapshotBuilder snapshotstore.OutboxBuilder, recoveryCutoff *time.Time, recoveryService *recovery.Service) {
	hashEngine := &hasher.Engine{DB: db}
	aggEngine := &aggregator.Engine{DB: db, RecoveryCutoff: recoveryCutoff}
	kafkaEngine := &kafkaconsumer.Engine{DB: db, SnapshotBuilder: snapshotBuilder}

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
				if err := aggEngine.ProcessRecoveryBatch(10); err != nil {
					log.Printf("❌ [RecoveryAggregator] Error: %v\n", err)
				}
				if fabricSvc != nil {
					if err := fabricSvc.AnchorPendingRoots(); err != nil {
						log.Printf("❌ [Anchoring] Error: %v\n", err)
					}
					if err := fabricSvc.AnchorPendingRecoveryRoots(); err != nil {
						log.Printf("❌ [RecoveryAnchoring] Error: %v\n", err)
					}
					if recoveryService != nil {
						if err := recoveryService.VerifyReadyEvents(ctx, 100); err != nil {
							log.Printf("⚠️ [RecoveryVerification] Event auto-verification selesai dengan status non-valid: %v\n", err)
						}
					}
				}
			}
		}
	}()

	// Kafka consumer: satu goroutine per klien aktif
	go func() {
		if err := kafkaEngine.Reconcile(ctx); err != nil {
			log.Printf("⚠️  [KafkaConsumer] Reconcile awal gagal: %v\n", err)
		}
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := kafkaEngine.Reconcile(ctx); err != nil {
					log.Printf("⚠️  [KafkaConsumer] Reconcile gagal: %v\n", err)
				}
			}
		}
	}()
}

func main() {
	godotenv.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db := config.ConnectDB()
	recoveryCutoff, cutoffErr := config.LoadRecoveryCutoff()
	if cutoffErr != nil {
		log.Fatalf("❌ Konfigurasi recovery scope tidak valid: %v", cutoffErr)
	}
	if scopeErr := validateRecoveryScopeFlags(recoveryEnabled(), snapshotRequiredForAnchor(), recoveryCutoff); scopeErr != nil {
		log.Fatalf("❌ Konfigurasi recovery scope tidak valid: %v", scopeErr)
	}

	snapshotBuilder, snapshotStore, snapshotCipher, err := buildSnapshotRuntime()
	if err != nil {
		log.Fatalf("❌ Konfigurasi snapshot writer tidak valid: %v", err)
	}
	if snapshotRequiredForAnchor() && snapshotBuilder == nil {
		log.Fatal("❌ SNAPSHOT_REQUIRED_FOR_ANCHOR=true membutuhkan SNAPSHOT_WRITER_ENABLED=true dan konfigurasi MinIO yang valid")
	}
	if snapshotStore != nil {
		snapshotWorkerConfig, configErr := loadSnapshotWorkerConfig()
		if configErr != nil {
			log.Fatalf("❌ Konfigurasi snapshot worker tidak valid: %v", configErr)
		}
		worker, workerErr := snapshotworker.New(db, snapshotStore, snapshotWorkerConfig)
		if workerErr != nil {
			log.Fatalf("❌ Snapshot worker tidak dapat dibuat: %v", workerErr)
		}
		go worker.Run(ctx)
	}

	fabricSvc, err := blockchain.InitFabricGateway(db)
	if err != nil {
		log.Printf("⚠️  Gagal terhubung ke Fabric: %v\n", err)
	} else {
		defer fabricSvc.Close()
	}
	if snapshotRequiredForAnchor() && fabricSvc == nil {
		log.Fatal("❌ SNAPSHOT_REQUIRED_FOR_ANCHOR=true membutuhkan koneksi Fabric yang valid")
	}
	if recoveryEnabled() && (snapshotStore == nil || snapshotCipher == nil || snapshotBuilder == nil || fabricSvc == nil) {
		log.Fatal("❌ RECOVERY_ENABLED=true membutuhkan MinIO, encryption key, snapshot builder, dan Fabric yang valid")
	}
	if tamperScannerEnabled() && fabricSvc == nil {
		log.Fatal("❌ TAMPER_SCANNER_ENABLED=true membutuhkan koneksi Fabric yang valid")
	}

	var recoverySnapshotBuilder snapshotstore.RecoveryEventOutboxBuilder
	if snapshotCipher != nil {
		recoverySnapshotBuilder = snapshotstore.RecoveryEventSnapshotOutboxBuilder{Cipher: snapshotCipher}
	}
	recoveryService := recovery.NewService(db, snapshotStore, snapshotCipher, fabricSvc, recoverySnapshotBuilder)
	recoveryService.SetRecoveryCutoff(recoveryCutoff)

	startPipelineWorker(ctx, db, fabricSvc, snapshotBuilder, recoveryCutoff, recoveryService)

	auditRepo := audit.NewAuditRepository(db)
	auditService := audit.NewService(auditRepo, fabricSvc, db)
	auditHandler := audit.NewHandler(auditService)
	if tamperScannerEnabled() {
		scannerConfig, configErr := loadTamperScannerConfig(recoveryCutoff)
		if configErr != nil {
			log.Fatalf("❌ Konfigurasi tamper scanner tidak valid: %v", configErr)
		}
		scanner, scannerErr := tamperscanner.New(db, auditService, scannerConfig)
		if scannerErr != nil {
			log.Fatalf("❌ Tamper scanner tidak dapat dibuat: %v", scannerErr)
		}
		go scanner.Run(ctx)
		log.Printf("🔍 Tamper scanner aktif; interval=%s batch=%d concurrency=%d", scannerConfig.Interval, scannerConfig.BatchSize, scannerConfig.Concurrency)
	}

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

	reportService := report.NewService(auditService)
	reportHandler := report.NewHandler(reportService)
	recoveryHandler := recovery.NewHandler(recoveryService)

	router := api.SetupRouter(auditHandler, authHandler, clientHandler, agentHandler, reportHandler, recoveryHandler)
	api.RegisterHealthRoutes(router, db)

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
