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
	// Generate 32 random bytes → 64 hex chars
	randomBytes := make([]byte, 32)
	if _, err := rand.Read(randomBytes); err != nil {
		return nil, "", errors.New("gagal generate API key yang aman")
	}

	// Format: ak_live_ (8 char) + 64 hex char = 72 char total
	rawAPIKey := "ak_live_" + hex.EncodeToString(randomBytes)

	// Hash untuk disimpan di DB
	hash := sha256.Sum256([]byte(rawAPIKey))
	apiKeyHash := hex.EncodeToString(hash[:])

	// Prefix 16 char (8 prefix tetap + 8 char random pertama)
	// Jauh lebih kecil kemungkinan collision dibanding 10 char sebelumnya
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
		CompanyName:      req.CompanyName,
		APIKeyHash:       apiKeyHash,
		APIKeyPrefix:     apiKeyPrefix,
		SubscriptionTier: tier,
		RateLimitPerSec:  rateLimit,
		Status:           status,
		ActorField:       req.ActorField,
		ActionField:      req.ActionField,
		ResourceField:    req.ResourceField,
	}

	if err := s.repo.CreateClient(newClient); err != nil {
		return nil, "", errors.New("gagal mendaftarkan klien ke database")
	}

	return newClient, rawAPIKey, nil
}
