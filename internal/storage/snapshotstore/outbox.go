package snapshotstore

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"go-blockchain-api/internal/models"
)

const SnapshotEventType = "STORE_AUDIT_SNAPSHOT"

type OutboxBuilder interface {
	Build(log models.AuditLog) (*models.SnapshotOutbox, error)
}

type AuditSnapshotOutboxBuilder struct {
	Cipher *Cipher
	Now    func() time.Time
}

func (b AuditSnapshotOutboxBuilder) Build(log models.AuditLog) (*models.SnapshotOutbox, error) {
	if b.Cipher == nil {
		return nil, fmt.Errorf("snapshot cipher belum dikonfigurasi")
	}
	now := time.Now
	if b.Now != nil {
		now = b.Now
	}

	plaintext, err := NewAuditSnapshot(log, now())
	if err != nil {
		return nil, err
	}
	encrypted, err := b.Cipher.Encrypt(plaintext)
	if err != nil {
		return nil, fmt.Errorf("enkripsi snapshot log %s gagal: %w", log.LogID, err)
	}

	return &models.SnapshotOutbox{
		ID:           uuid.NewString(),
		LogID:        log.LogID,
		ClientID:     log.ClientID,
		EventType:    SnapshotEventType,
		Payload:      encrypted,
		PayloadHash:  log.HashValue,
		Status:       models.OutboxStatusPending,
		AttemptCount: 0,
		CreatedAt:    now().UTC(),
	}, nil
}
