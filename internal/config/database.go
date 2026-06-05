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
		&models.AuditLog{}, // tambah kolom source_record_id via AutoMigrate
		&models.MerkleMetadata{},
		&models.MerkleProof{},
		&models.AgentConfig{}, // tabel baru untuk Lapis 3
	)
	if err != nil {
		log.Fatalf("Gagal migrasi database: %v", err)
	}

	log.Println("✅ Database terhubung dan schema telah di-migrate.")
	return db
}
