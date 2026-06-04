package client

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"go-blockchain-api/internal/models"
)

type Service interface {
	RegisterClient(req CreateClientRequest) (*models.Client, string, error)
}

type clientService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &clientService{repo: repo}
}

func (s *clientService) RegisterClient(req CreateClientRequest) (*models.Client, string, error) {
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, "", errors.New("gagal generate API key yang aman")
	}

	rawAPIKey := "ak_live_" + hex.EncodeToString(randomBytes)

	hash := sha256.Sum256([]byte(rawAPIKey))
	apiKeyHash := hex.EncodeToString(hash[:])
	apiKeyPrefix := rawAPIKey[:16]

	tier := req.SubscriptionTier
	if tier == "" {
		tier = "basic"
	}
	rateLimit := req.RateLimitPerSec
	if rateLimit == 0 {
		rateLimit = 50
	}
	status := req.Status
	if status == "" {
		status = "active"
	}

	newClient := &models.Client{
		CompanyName:        req.CompanyName,
		APIKeyHash:         apiKeyHash,
		APIKeyPrefix:       apiKeyPrefix,
		SubscriptionTier:   tier,
		RateLimitPerSec:    rateLimit,
		Status:             status,
		ActorField:         req.ActorField,
		FallbackActorField: req.FallbackActorField,
		ActionField:        req.ActionField,
		ResourceField:      req.ResourceField,
	}

	if err := s.repo.CreateClient(newClient); err != nil {
		return nil, "", errors.New("gagal mendaftarkan klien ke database")
	}

	return newClient, rawAPIKey, nil
}
