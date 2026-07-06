package hasher

import (
	"fmt"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
	"log"

	"gorm.io/gorm"
)

type Engine struct {
	DB *gorm.DB
}

// GenerateLogHash menghasilkan hash deterministik dari AuditLog.
//
// TESTING: parameter prevHash DIHAPUS — local chain (previous hash linking)
// tidak lagi dipakai karena Merkle Tree sudah dihapus dari alur. Setiap log
// kini murni berdiri sendiri secara kriptografis.
//
// PERINGATAN: format string ini HARUS IDENTIK dengan generateLogHash di
// kafkaconsumer/consumer.go dan kafkaconsumer/verifier.go. Perubahan
// apapun pada format akan memecahkan semua verifikasi hash yang sudah ada.
func GenerateLogHash(auditLog *models.AuditLog) string {
	authCtx := auditLog.AuthorizationContext
	if authCtx == "null" || authCtx == "<nil>" || authCtx == "" {
		authCtx = ""
	}

	contextString := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s|%s",
		auditLog.LogID,
		auditLog.Actor,
		auditLog.Action,
		auditLog.Resource,
		auditLog.Timestamp.UnixMicro(),
		auditLog.SourceSystem,
		authCtx,
		auditLog.Metadata,
	)

	return crypto.GenerateSHA3_256(contextString)
}

// ProcessPendingLogs mencari log berstatus RECEIVED dan memprosesnya menjadi HASHED.
// Dipakai untuk log yang masuk via ingestion manual (bukan Kafka).
func (h *Engine) ProcessPendingLogs() error {
	var pendingLogs []models.AuditLog

	if err := h.DB.Where("status = ?", "RECEIVED").Order("timestamp asc").Find(&pendingLogs).Error; err != nil {
		return err
	}

	if len(pendingLogs) == 0 {
		return nil
	}

	for _, auditLog := range pendingLogs {
		hashValue := GenerateLogHash(&auditLog)

		auditLog.HashValue = hashValue
		auditLog.Status = "HASHED"

		if err := h.DB.Save(&auditLog).Error; err != nil {
			log.Printf("[Hasher] Gagal menyimpan hash untuk log %s: %v", auditLog.LogID, err)
			continue
		}

		log.Printf("[Hasher] ✅ Log %s berhasil di-hash: %s", auditLog.LogID, hashValue)
	}

	return nil
}
