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

func GenerateLogHash(auditLog *models.AuditLog, prevHash string) string {
	// Serialisasi Data Secara Deterministik
	contextString := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s|%s|%s",
		auditLog.LogID,
		auditLog.Actor,
		auditLog.Action,
		auditLog.Resource,
		auditLog.Timestamp.UnixMicro(),
		auditLog.SourceSystem,
		auditLog.AuthorizationContext,
		prevHash,
		auditLog.Metadata,
	)

	return crypto.GenerateSHA3_256(contextString)
}

// ProcessPendingLogs mencari log berstatus RECEIVED dan memprosesnya menjadi HASHED
func (h *Engine) ProcessPendingLogs() error {
	var pendingLogs []models.AuditLog

	if err := h.DB.Where("status = ?", "RECEIVED").Order("timestamp asc").Find(&pendingLogs).Error; err != nil {
		return err
	}

	if len(pendingLogs) == 0 {
		return nil
	}

	for _, auditLog := range pendingLogs {
		var lastLog models.AuditLog
		var prevHash string

		result := h.DB.Where("status IN ?", []string{"HASHED", "ANCHORED"}).Order("timestamp desc").First(&lastLog)
		if result.Error == nil {
			prevHash = lastLog.HashValue
		} else {
			prevHash = "GENESIS_00000000000000000000000000000000000000000000000000000000"
		}

		hashValue := GenerateLogHash(&auditLog, prevHash)

		auditLog.HashValue = hashValue
		auditLog.PreviousHash = prevHash
		auditLog.Status = "HASHED"

		if err := h.DB.Save(&auditLog).Error; err != nil {
			log.Printf("[Hasher] Gagal menyimpan hash untuk log %s: %v", auditLog.LogID, err)
			continue
		}

		log.Printf("[Hasher] ✅ Log %s berhasil di-hash: %s", auditLog.LogID, hashValue)
	}

	return nil
}
