package config

import (
	"go-blockchain-api/internal/models"
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func ConnectDB() *gorm.DB {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN environment variable is not set")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Gagal koneksi ke database: %v", err)
	}

	err = db.AutoMigrate(
		&models.Client{},
		&models.User{},
		&models.AuditLog{},
		&models.MerkleMetadata{},
		&models.MerkleProof{},
		&models.AgentConfig{},
		&models.ClientKafkaConfig{},
		&models.KafkaOffset{},
		&models.ClientTable{},
		&models.ClientUser{},
		&models.SnapshotOutbox{},
		&models.TamperIncident{},
		&models.RecoveryRequest{},
	)
	if err != nil {
		log.Fatalf("Gagal migrasi database: %v", err)
	}

	// Satu log boleh memiliki banyak incident historis (misalnya setelah
	// recovery dan kemudian tamper lagi), tetapi hanya boleh ada satu incident
	// yang masih aktif untuk kombinasi client/log/jenis yang sama. GORM tidak
	// dapat mengekspresikan partial unique index ini melalui tag model, dan
	// index lama yang mencakup kolom status akan gagal saat incident baru
	// ditutup menjadi RESOLVED setelah incident RESOLVED sebelumnya sudah ada.
	if err := ensureTamperIncidentActiveIndex(db); err != nil {
		log.Fatalf("Gagal menyiapkan index incident tamper aktif: %v", err)
	}
	// New recovery requests are self-service: a client user may execute after
	// the preflight checks, without a platform-admin approval transition.
	// Keep the database default aligned with the model for any future insert
	// that does not explicitly set Status. Existing legacy rows are preserved.
	if err := db.Exec("ALTER TABLE recovery_requests ALTER COLUMN status SET DEFAULT 'PENDING_EXECUTION'").Error; err != nil {
		log.Fatalf("Gagal menyiapkan default status recovery request: %v", err)
	}

	log.Println("✅ Database terhubung dan schema telah di-migrate.")
	if err := db.Exec("ALTER TABLE users ALTER COLUMN client_id DROP NOT NULL").Error; err != nil {
		log.Printf("Gagal mengubah kolom users.client_id menjadi nullable: %v", err)
	}

	return db
}

func ensureTamperIncidentActiveIndex(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DROP INDEX IF EXISTS idx_tamper_active").Error; err != nil {
			return err
		}
		return tx.Exec(`
			CREATE UNIQUE INDEX idx_tamper_active
			ON tamper_incidents (client_id, log_id, incident_type)
			WHERE status IN ('OPEN', 'UNDER_REVIEW', 'RECOVERING')
		`).Error
	})
}
