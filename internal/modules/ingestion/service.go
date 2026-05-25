package ingestion

import (
	"context"
	"encoding/json"
	"fmt"

	"go-blockchain-api/internal/engine/normalizer"
	"go-blockchain-api/internal/models"
)

type Service struct {
	Repo QueueRepository
}

func NewService(repo QueueRepository) *Service {
	return &Service{Repo: repo}
}

// ProcessLog menormalisasi data dan memasukkannya ke antrean Redis
func (s *Service) ProcessLog(input normalizer.RawLogInput) (*models.AuditLog, error) {
	standardLog, err := normalizer.Normalize(input)
	if err != nil {
		return nil, fmt.Errorf("gagal menormalisasi log: %v", err)
	}

	standardLog.ClientID = input.ClientID
	standardLog.HashValue = "PENDING-" + standardLog.LogID

	logJSON, _ := json.Marshal(standardLog)
	ctx := context.Background()

	if err := s.Repo.PushToQueue(ctx, "audit_log_queue", logJSON); err != nil {
		return nil, fmt.Errorf("gagal memasukkan log ke antrean Redis: %v", err)
	}

	return standardLog, nil
}
