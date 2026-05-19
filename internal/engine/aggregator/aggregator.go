package aggregator

import (
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
	"log"

	"gorm.io/gorm"
)

type Engine struct {
	DB *gorm.DB
}

// ProcessBatch mengelompokkan log transaksi yang sudah di-hash dan membuat Merkle Root
func (a *Engine) ProcessBatch(batchSize int) error {
	var logs []models.AuditLog

	// 1. Ambil log yang siap diagregasi (maksimal sejumlah batchSize)
	if err := a.DB.Where("status = ?", "HASHED").Order("timestamp asc").Limit(batchSize).Find(&logs).Error; err != nil {
		return err
	}

	if len(logs) == 0 {
		return nil // Tidak ada data untuk diproses
	}

	var hashes []string
	for _, l := range logs {
		hashes = append(hashes, l.HashValue)
	}

	// 2. Bangun Merkle Tree dan dapatkan Root beserta Proof-nya
	merkleResult := crypto.BuildMerkleTree(hashes)
	if merkleResult == nil {
		return nil
	}

	// 3. Gunakan Database Transaction agar semua proses update aman dan atomik
	err := a.DB.Transaction(func(tx *gorm.DB) error {

		// A. Simpan Merkle Metadata (Aktivitas 8) [cite: 240-242]
		merkleMeta := models.MerkleMetadata{
			MerkleRoot: merkleResult.Root,
			BatchSize:  len(logs),
		}
		if err := tx.Create(&merkleMeta).Error; err != nil {
			return err
		}

		// B. Update Audit Logs dan Simpan Merkle Proofs
		for _, logItem := range logs {
			// Update status log menjadi siap dikirim ke blockchain
			logItem.MerkleRoot = merkleResult.Root
			logItem.Status = "AGGREGATED"
			if err := tx.Save(&logItem).Error; err != nil {
				return err
			}

			// Ambil dan simpan bukti sibling (Merkle Proof) ke database
			proofs := merkleResult.Proofs[logItem.HashValue]
			for _, p := range proofs {
				mp := models.MerkleProof{
					TransactionHash: logItem.HashValue,
					SiblingHash:     p.SiblingHash,
					TreeLevel:       p.TreeLevel,
					MerkleRoot:      merkleResult.Root,
				}
				if err := tx.Create(&mp).Error; err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err != nil {
		log.Printf("[Aggregator] ❌ Gagal menyimpan Merkle Batch: %v", err)
		return err
	}

	log.Printf("[Aggregator] ✅ Batch %d transaksi sukses diagregasi. Merkle Root: %s", len(logs), merkleResult.Root)
	return nil
}
