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
	GetClients() ([]models.Client, error)
	GetKafkaConfigs() ([]KafkaConfigWithClient, error)
	GetDashboardSummary() (DashboardSummary, error)
}

type KafkaConfigWithClient struct {
	models.ClientKafkaConfig
	CompanyName string `json:"company_name"`
}

type DashboardSummary struct {
	TotalClients  int64 `json:"total_clients"`
	ActiveStreams int64 `json:"active_streams"`
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

	status := req.Status
	if status == "" {
		status = "active"
	}

	newClient := &models.Client{
		CompanyName:        req.CompanyName,
		APIKeyHash:         apiKeyHash,
		APIKeyPrefix:       apiKeyPrefix,
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

func (s *clientService) GetClients() ([]models.Client, error) {
	return s.repo.GetClients()
}

func (s *clientService) GetKafkaConfigs() ([]KafkaConfigWithClient, error) {
	configs, err := s.repo.GetKafkaConfigs()
	if err != nil {
		return nil, err
	}

	clients, err := s.repo.GetClients()
	if err != nil {
		return nil, err
	}

	clientMap := make(map[string]string)
	for _, c := range clients {
		clientMap[c.ID] = c.CompanyName
	}

	var result []KafkaConfigWithClient
	for _, cfg := range configs {
		companyName := clientMap[cfg.ClientID]
		if companyName == "" {
			companyName = "Unknown Client"
		}
		result = append(result, KafkaConfigWithClient{
			ClientKafkaConfig: cfg,
			CompanyName:       companyName,
		})
	}

	return result, nil
}

func (s *clientService) GetDashboardSummary() (DashboardSummary, error) {
	clients, err := s.repo.GetClients()
	if err != nil {
		return DashboardSummary{}, err
	}

	configs, err := s.repo.GetKafkaConfigs()
	if err != nil {
		return DashboardSummary{}, err
	}

	var activeStreams int64
	for _, cfg := range configs {
		if cfg.IsActive {
			activeStreams++
		}
	}

	return DashboardSummary{
		TotalClients:  int64(len(clients)),
		ActiveStreams: activeStreams,
	}, nil
}
