package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"

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
	Status             string `json:"status" example:"active"`
	ActorField         string `json:"actor_field" example:"app_user"`
	FallbackActorField string `json:"fallback_actor_field" example:"db_user"`
	ActionField        string `json:"action_field" example:"operasi"`
	ResourceField      string `json:"resource_field" example:"tabel"`
}

type CreateClientUserRequest struct {
	Username string `json:"username" binding:"required,min=4"`
	Password string `json:"password" binding:"required,min=6"`
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

func (h *Handler) DeleteKafkaConfig(c *gin.Context) {
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

	// Soft delete via GORM (mengisi deleted_at), BUKAN raw SQL DELETE —
	// supaya riwayat config tetap tertelusuri, dan supaya query Reconcile
	// (yang otomatis exclude baris ber-deleted_at berkat model punya
	// gorm.DeletedAt) langsung mengecualikan baris ini di siklus berikutnya.
	if err := h.DB.Delete(&cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghapus konfigurasi Kafka"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "Konfigurasi Kafka dihapus. Consumer terkait akan berhenti otomatis dalam ≤15 detik (siklus reconcile).",
		"id":        cfg.ID,
		"client_id": cfg.ClientID,
	})
}

func (h *Handler) ListClients(c *gin.Context) {
	clients, err := h.Service.GetClients()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar klien"})
		return
	}
	c.JSON(http.StatusOK, clients)
}

func (h *Handler) ListKafkaConfigs(c *gin.Context) {
	configs, err := h.Service.GetKafkaConfigs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar konfigurasi Kafka"})
		return
	}
	c.JSON(http.StatusOK, configs)
}

func (h *Handler) GetDashboardSummary(c *gin.Context) {
	summary, err := h.Service.GetDashboardSummary()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil ringkasan dashboard"})
		return
	}
	c.JSON(http.StatusOK, summary)
}

func (h *Handler) ToggleClientStatus(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	client, err := h.Service.ToggleClientStatus(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal merubah status klien"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Status klien berhasil diperbarui",
		"id":      client.ID,
		"status":  client.Status,
	})
}

func (h *Handler) DeleteClient(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	if err := h.Service.DeleteClient(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghapus klien"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Klien berhasil dihapus dari sistem",
		"id":      id,
	})
}

func (h *Handler) GetClientUsers(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	users, err := h.Service.GetUsersByClient(clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar pengguna klien"})
		return
	}

	c.JSON(http.StatusOK, users)
}

func (h *Handler) CreateClientUser(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var req CreateClientUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.Service.AddUserToClient(clientID, req.Username, req.Password)
	if err != nil {
		if err.Error() == "username_already_exists" {
			c.JSON(http.StatusConflict, gin.H{"error": "Username is already taken"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mendaftarkan pengguna klien"})
		}
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Pengguna klien berhasil ditambahkan",
		"user": map[string]interface{}{
			"id":        user.ID,
			"client_id": user.ClientID,
			"username":  user.Username,
			"role":      user.Role,
		},
	})
}

func (h *Handler) DeleteClientUser(c *gin.Context) {
	userID := c.Param("id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID pengguna tidak boleh kosong"})
		return
	}

	if err := h.Service.RemoveUser(userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghapus pengguna klien"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Pengguna klien berhasil dihapus",
		"id":      userID,
	})
}

type CreateAgentConfigRequest struct {
	AgentURL       string `json:"agent_url" binding:"required" example:"http://192.168.11.50:9090"`
	VerifyToken    string `json:"verify_token" binding:"required" example:"token-rahasia-acak-panjang"`
	TimeoutSeconds int    `json:"timeout_seconds" example:"5"`
}

func (h *Handler) CreateAgentConfig(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var req CreateAgentConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format tidak valid: " + err.Error()})
		return
	}

	timeout := req.TimeoutSeconds
	if timeout <= 0 {
		timeout = 5
	}

	var existing models.AgentConfig
	err := h.DB.Where("client_id = ?", clientID).First(&existing).Error
	if err == nil {
		h.DB.Model(&existing).Updates(map[string]interface{}{
			"agent_url":       req.AgentURL,
			"verify_token":    req.VerifyToken,
			"timeout_seconds": timeout,
			"is_active":       true,
		})
		c.JSON(http.StatusOK, gin.H{
			"message":   "Konfigurasi Agent berhasil diperbarui",
			"agent_url": req.AgentURL,
		})
		return
	}

	cfg := models.AgentConfig{
		ClientID:       clientID,
		AgentURL:       req.AgentURL,
		VerifyToken:    req.VerifyToken,
		TimeoutSeconds: timeout,
		IsActive:       true,
	}
	if err := h.DB.Create(&cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menyimpan konfigurasi Agent"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":   "Agent berhasil didaftarkan untuk klien.",
		"agent_url": req.AgentURL,
	})
}

func (h *Handler) GetAgentConfig(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var cfg models.AgentConfig
	if err := h.DB.Where("client_id = ?", clientID).First(&cfg).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Agent belum terdaftar untuk klien ini"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"agent_url":       cfg.AgentURL,
		"timeout_seconds": cfg.TimeoutSeconds,
		"is_active":       cfg.IsActive,
		"updated_at":      cfg.UpdatedAt,
	})
}

func (h *Handler) DeleteAgentConfig(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	if err := h.DB.Where("client_id = ?", clientID).Delete(&models.AgentConfig{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal menghapus konfigurasi Agent"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Konfigurasi Agent klien dihapus."})
}

func (h *Handler) PingAgentConfig(c *gin.Context) {
	clientID := c.Param("id")
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var cfg models.AgentConfig
	if err := h.DB.Where("client_id = ? AND is_active = true", clientID).First(&cfg).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Agent belum terdaftar atau tidak aktif"})
		return
	}

	client := &http.Client{Timeout: 3e9}
	resp, err := client.Get(cfg.AgentURL + "/connectors")
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"reachable": false,
			"agent_url": cfg.AgentURL,
			"error":     err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	c.JSON(http.StatusOK, gin.H{
		"reachable":   true,
		"agent_url":   cfg.AgentURL,
		"http_status": resp.StatusCode,
	})
}

type UpdateClientRequest struct {
	CompanyName        string `json:"company_name"`
	Status             string `json:"status"`
	ActorField         string `json:"actor_field"`
	FallbackActorField string `json:"fallback_actor_field"`
	ActionField        string `json:"action_field"`
	ResourceField      string `json:"resource_field"`
	TopicPrefix        string `json:"topic_prefix"`
}

func (h *Handler) UpdateClient(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var req UpdateClientRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid: " + err.Error()})
		return
	}

	var client models.Client
	if err := h.DB.First(&client, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Klien tidak ditemukan"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mencari data klien"})
		}
		return
	}

	if req.CompanyName != "" {
		client.CompanyName = req.CompanyName
	}
	if req.ActorField != "" {
		client.ActorField = req.ActorField
	}
	client.FallbackActorField = req.FallbackActorField
	if req.ActionField != "" {
		client.ActionField = req.ActionField
	}
	if req.ResourceField != "" {
		client.ResourceField = req.ResourceField
	}
	if req.Status != "" {
		client.Status = req.Status
	}

	if err := h.DB.Save(&client).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal meng-update data klien"})
		return
	}

	if req.TopicPrefix != "" {
		h.DB.Model(&models.ClientKafkaConfig{}).Where("client_id = ?", client.ID).Update("topic_prefix", req.TopicPrefix)
	}

	// Jika status di-update menjadi 'active' (Approved), aktifkan juga AgentConfig & ClientKafkaConfig
	if client.Status == "active" {
		h.DB.Model(&models.AgentConfig{}).Where("client_id = ?", client.ID).Update("is_active", true)
		h.DB.Model(&models.ClientKafkaConfig{}).Where("client_id = ?", client.ID).Update("is_active", true)
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Data klien berhasil diperbarui",
		"client":  client,
	})
}

func (h *Handler) GetClientDetail(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID klien tidak boleh kosong"})
		return
	}

	var client models.Client
	if err := h.DB.First(&client, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Klien tidak ditemukan"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil data klien"})
		}
		return
	}

	var agentCfg models.AgentConfig
	_ = h.DB.Where("client_id = ?", id).First(&agentCfg).Error

	var kafkaCfg models.ClientKafkaConfig
	_ = h.DB.Where("client_id = ?", id).First(&kafkaCfg).Error

	c.JSON(http.StatusOK, gin.H{
		"client":       client,
		"agent_config": agentCfg,
		"kafka_config": kafkaCfg,
	})
}

type AgentTelemetryRequest struct {
	APIKeyPrefix    string `json:"api_key_prefix" binding:"required"`
	KafkaBrokers    string `json:"kafka_brokers" binding:"required"`
	AgentServerURL  string `json:"agent_server_url" binding:"required"`
	Hostname        string `json:"hostname"`
	TailscaleIP     string `json:"tailscale_ip"`
	Status          string `json:"status"`
	DBEngine        string `json:"db_engine"`
	DBName          string `json:"db_name"`
	DBTables        string `json:"db_tables"`
	ConnectorStatus string `json:"connector_status"`
}

func (h *Handler) ProcessTelemetry(c *gin.Context) {
	var req AgentTelemetryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	searchPrefix := req.APIKeyPrefix
	if len(searchPrefix) > 16 {
		searchPrefix = searchPrefix[:16]
	}

	var client models.Client
	err := h.DB.Where("api_key_prefix = ?", searchPrefix).First(&client).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			companyName := "Auto Registered (" + req.Hostname + ")"
			if req.DBName != "" {
				companyName = "Auto Registered (" + req.Hostname + " - " + req.DBName + ")"
			}
			client = models.Client{
				CompanyName:  companyName,
				APIKeyPrefix: req.APIKeyPrefix,
				Status:       "active",
			}
			if err := h.DB.Create(&client).Error; err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mendaftarkan klien baru"})
				return
			}
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
	}

	var agentCfg models.AgentConfig
	if err := h.DB.Where("client_id = ?", client.ID).First(&agentCfg).Error; err != nil {
		agentCfg = models.AgentConfig{
			ClientID:        client.ID,
			AgentURL:        req.AgentServerURL,
			TailscaleIP:     req.TailscaleIP,
			Hostname:        req.Hostname,
			DBEngine:        req.DBEngine,
			DBName:          req.DBName,
			DBTables:        req.DBTables,
			ConnectorStatus: req.ConnectorStatus,
			IsActive:        true,
		}
		h.DB.Create(&agentCfg)
	} else {
		h.DB.Model(&agentCfg).Updates(map[string]interface{}{
			"agent_url":        req.AgentServerURL,
			"tailscale_ip":     req.TailscaleIP,
			"hostname":         req.Hostname,
			"db_engine":        req.DBEngine,
			"db_name":          req.DBName,
			"db_tables":        req.DBTables,
			"connector_status": req.ConnectorStatus,
			"is_active":        true,
		})
	}

	sourceSys := req.Hostname
	if req.DBEngine != "" && req.DBName != "" {
		sourceSys = req.DBEngine + " - " + req.DBName + " (" + req.Hostname + ")"
	}

	topicPrefix := "draft." + client.ID
	if req.DBName != "" {
		topicPrefix = req.Hostname + "_" + req.DBName + "."
	}

	var kafkaCfg models.ClientKafkaConfig
	if err := h.DB.Where("client_id = ?", client.ID).First(&kafkaCfg).Error; err != nil {
		kafkaCfg = models.ClientKafkaConfig{
			ClientID:     client.ID,
			KafkaBrokers: req.KafkaBrokers,
			TopicPrefix:  topicPrefix,
			SourceSystem: sourceSys,
			DBEngine:     req.DBEngine,
			IsActive:     true,
		}
		h.DB.Create(&kafkaCfg)
	} else {
		h.DB.Model(&kafkaCfg).Updates(models.ClientKafkaConfig{
			KafkaBrokers: req.KafkaBrokers,
			SourceSystem: sourceSys,
			TopicPrefix:  topicPrefix,
			DBEngine:     req.DBEngine,
			IsActive:     true,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"message":          "Telemetri berhasil diterima dan klien otomatis diaktifkan.",
		"client_id":        client.ID,
		"status":           "active",
		"kafka_brokers":    req.KafkaBrokers,
		"agent_server_url": req.AgentServerURL,
	})
}

// GenerateTailscaleKey Requests a new ephemeral Auth Key from Tailscale OAuth API
func (h *Handler) GenerateTailscaleKey(c *gin.Context) {
	var req struct {
		APIKeyPrefix string `json:"api_key_prefix" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload"})
		return
	}

	searchPrefix := req.APIKeyPrefix
	if len(searchPrefix) > 16 {
		searchPrefix = searchPrefix[:16]
	}

	var client models.Client
	if err := h.DB.Where("api_key_prefix = ?", searchPrefix).First(&client).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Client not found or invalid API Key"})
		return
	}

	apiToken := os.Getenv("TAILSCALE_API_TOKEN")
	tailnet := os.Getenv("TAILSCALE_TAILNET")

	if apiToken == "" || tailnet == "" {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Tailscale API Token not configured on Gateway"})
		return
	}

	// 1. Generate new Auth Key directly using API Token
	keyPayload := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"devices": map[string]interface{}{
				"create": map[string]interface{}{
					"reusable":      false,
					"ephemeral":     false,
					"preauthorized": true,
				},
			},
		},
		"expirySeconds": 3600,
	}

	keyPayloadBytes, _ := json.Marshal(keyPayload)
	reqHttp, _ := http.NewRequest("POST", fmt.Sprintf("https://api.tailscale.com/api/v2/tailnet/%s/keys", tailnet), bytes.NewBuffer(keyPayloadBytes))
	reqHttp.Header.Set("Authorization", "Bearer "+apiToken)
	reqHttp.Header.Set("Content-Type", "application/json")

	keyResp, err := http.DefaultClient.Do(reqHttp)
	if err != nil || keyResp.StatusCode != 200 {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate Tailscale Auth Key"})
		return
	}
	defer keyResp.Body.Close()

	var keyData struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(keyResp.Body).Decode(&keyData); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to decode Tailscale key response"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"auth_key": keyData.Key,
	})
}

func (h *Handler) ServeInstallScript(c *gin.Context) {
	possiblePaths := []string{
		"./scripts/install.sh",
		"./auditchain-gateway-backend/scripts/install.sh",
		"../scripts/install.sh",
		"scripts/install.sh",
		"/etc/auditchain/install.sh",
	}

	for _, p := range possiblePaths {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			c.Header("Content-Type", "text/x-shellscript")
			c.File(p)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "File script install.sh tidak ditemukan di server Gateway"})
}

