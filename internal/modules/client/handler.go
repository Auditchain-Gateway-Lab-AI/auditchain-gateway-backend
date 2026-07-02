package client

import (
	"errors"
	"go-blockchain-api/internal/models"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Handler struct {
	Service Service
	DB      *gorm.DB
}

func NewHandler(service Service) *Handler {
	return &Handler{Service: service}
}

type CreateClientRequest struct {
	CompanyName        string `json:"company_name" binding:"required" example:"PT Karya Bangsa"`
	SubscriptionTier   string `json:"subscription_tier" example:"enterprise"`
	RateLimitPerSec    int    `json:"rate_limit_per_sec" example:"100"`
	Status             string `json:"status" example:"active"`
	ActorField         string `json:"actor_field" example:"app_user"`
	FallbackActorField string `json:"fallback_actor_field" example:"db_user"`
	ActionField        string `json:"action_field" example:"operasi"`
	ResourceField      string `json:"resource_field" example:"tabel"`
}

type CreateKafkaConfigRequest struct {
	ClientID     string `json:"client_id" binding:"required"`
	TopicPrefix  string `json:"topic_prefix" binding:"required"`
	KafkaBrokers string `json:"kafka_brokers" binding:"required"`
	SourceSystem string `json:"source_system" binding:"required"`
	ActorField   string `json:"actor_field"`
	PKField      string `json:"pk_field"`
}

func (h *Handler) CreateKafkaConfig(c *gin.Context) {
	var req CreateKafkaConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	pkField := req.PKField
	if pkField == "" {
		pkField = "ID"
	}
	actorField := req.ActorField
	if actorField == "" {
		actorField = "__user_name"
	}

	cfg := &models.ClientKafkaConfig{
		ClientID:     req.ClientID,
		TopicPrefix:  req.TopicPrefix,
		KafkaBrokers: req.KafkaBrokers,
		SourceSystem: req.SourceSystem,
		ActorField:   actorField,
		PKField:      pkField,
		IsActive:     true,
	}

	if err := h.DB.Create(cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal simpan kafka config"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":       "Kafka config berhasil didaftarkan",
		"id":            cfg.ID,
		"topic_prefix":  cfg.TopicPrefix,
		"kafka_brokers": cfg.KafkaBrokers,
	})
}

func (h *Handler) ToggleKafkaConfig(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID konfigurasi tidak boleh kosong"})
		return
	}

	var cfg models.ClientKafkaConfig
	if err := h.DB.First(&cfg, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Konfigurasi Kafka tidak ditemukan"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mencari konfigurasi Kafka"})
		}
		return
	}

	// Toggle status
	cfg.IsActive = !cfg.IsActive

	if err := h.DB.Save(&cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memperbarui status konfigurasi Kafka"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":       "Status konfigurasi Kafka berhasil diperbarui",
		"id":            cfg.ID,
		"client_id":     cfg.ClientID,
		"kafka_brokers": cfg.KafkaBrokers,
		"is_active":     cfg.IsActive,
	})
}


func (h *Handler) CreateClient(c *gin.Context) {
	var req CreateClientRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format tidak valid atau company_name belum diisi"})
		return
	}

	clientData, rawAPIKey, err := h.Service.RegisterClient(req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":   "Klien / Perusahaan SaaS berhasil didaftarkan",
		"client_id": clientData.ID,
		"api_key":   rawAPIKey,
		"field_mapping": gin.H{
			"actor_field":          clientData.ActorField,
			"fallback_actor_field": clientData.FallbackActorField,
			"action_field":         clientData.ActionField,
			"resource_field":       clientData.ResourceField,
		},
	})
}
