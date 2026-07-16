package auth

import (
	"errors"
	"os"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type Service interface {
	Register(clientID, username, password string) (*models.User, *models.Client, error)
	Login(username, password string) (string, error)
}

type authService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &authService{repo: repo}
}

func (s *authService) Register(clientID, username, password string) (*models.User, *models.Client, error) {
	client, err := s.repo.CheckClient(clientID)
	if err != nil {
		return nil, nil, errors.New("client_not_found")
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, nil, errors.New("hash_error")
	}

	newUser := &models.User{
		ClientID: clientID,
		Username: username,
		Password: string(hashedPassword),
		Role:     "Auditor",
	}

	if err := s.repo.CreateUser(newUser); err != nil {
		return nil, nil, errors.New("username_used")
	}

	return newUser, client, nil
}

func (s *authService) Login(username, password string) (string, error) {
	user, err := s.repo.FindByUsername(username)
	if err != nil {
		return "", errors.New("invalid_credentials")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password)); err != nil {
		return "", errors.New("invalid_credentials")
	}

	// Periksa status klien untuk user non-Admin
	if user.Role != "Admin" {
		client, err := s.repo.CheckClient(user.ClientID)
		if err != nil || client.Status != "active" {
			return "", errors.New("client_inactive")
		}
	}

	claims := jwt.MapClaims{
		"user_id":   user.ID,
		"client_id": user.ClientID,
		"username":  user.Username,
		"role":      user.Role,
		"exp":       time.Now().Add(time.Hour * 2).Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	secret := os.Getenv("JWT_SECRET")

	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", errors.New("token_error")
	}

	return tokenString, nil
}
