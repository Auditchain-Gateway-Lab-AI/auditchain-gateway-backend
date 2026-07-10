package client

import (
	"go-blockchain-api/internal/models"

	"gorm.io/gorm"
)

type Repository interface {
	CreateClient(client *models.Client) error
	GetClients() ([]models.Client, error)
	GetKafkaConfigs() ([]models.ClientKafkaConfig, error)
}

type clientRepository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) Repository {
	return &clientRepository{db: db}
}

func (r *clientRepository) CreateClient(client *models.Client) error {
	return r.db.Create(client).Error
}

func (r *clientRepository) GetClients() ([]models.Client, error) {
	var clients []models.Client
	err := r.db.Order("created_at ASC").Find(&clients).Error
	return clients, err
}

func (r *clientRepository) GetKafkaConfigs() ([]models.ClientKafkaConfig, error) {
	var configs []models.ClientKafkaConfig
	err := r.db.Order("created_at ASC").Find(&configs).Error
	return configs, err
}
