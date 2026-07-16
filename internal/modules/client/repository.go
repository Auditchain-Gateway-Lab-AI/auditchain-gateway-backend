package client

import (
	"go-blockchain-api/internal/models"

	"gorm.io/gorm"
)

type Repository interface {
	CreateClient(client *models.Client) error
	GetClients() ([]models.Client, error)
	GetKafkaConfigs() ([]models.ClientKafkaConfig, error)
	UpdateClientStatus(id string, status string) error
	DeleteClient(id string) error
	GetClientByID(id string) (*models.Client, error)
	GetUsersByClientID(clientID string) ([]models.User, error)
	CreateUser(user *models.User) error
	DeleteUserByID(userID string) error
	CheckUsernameExists(username string) (bool, error)
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

func (r *clientRepository) UpdateClientStatus(id string, status string) error {
	return r.db.Model(&models.Client{}).Where("id = ?", id).Update("status", status).Error
}

func (r *clientRepository) DeleteClient(id string) error {
	return r.db.Delete(&models.Client{}, "id = ?", id).Error
}

func (r *clientRepository) GetClientByID(id string) (*models.Client, error) {
	var client models.Client
	err := r.db.First(&client, "id = ?", id).Error
	return &client, err
}

func (r *clientRepository) GetUsersByClientID(clientID string) ([]models.User, error) {
	var users []models.User
	err := r.db.Where("client_id = ?", clientID).Order("created_at DESC").Find(&users).Error
	return users, err
}

func (r *clientRepository) CreateUser(user *models.User) error {
	return r.db.Create(user).Error
}

func (r *clientRepository) DeleteUserByID(userID string) error {
	return r.db.Delete(&models.User{}, "id = ?", userID).Error
}

func (r *clientRepository) CheckUsernameExists(username string) (bool, error) {
	var count int64
	err := r.db.Model(&models.User{}).Where("username = ?", username).Count(&count).Error
	return count > 0, err
}
