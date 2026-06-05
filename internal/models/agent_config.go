package models

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// AgentConfig menyimpan URL dan token Agent milik masing-masing klien.
// Gateway menggunakan ini untuk memanggil GET /verify/:source_record_id
// ke Agent saat verifikasi Lapis 3.
type AgentConfig struct {
	ID       string `gorm:"primaryKey;type:varchar(36)" json:"id"`
	ClientID string `gorm:"type:varchar(36);not null;uniqueIndex" json:"client_id"`

	// URL Agent yang dapat dihubungi dari jaringan Gateway.
	// Contoh: http://192.168.11.50:9090
	AgentURL string `gorm:"type:varchar(255);not null" json:"agent_url"`

	// Bearer token untuk autentikasi — harus cocok dengan AGENT_VERIFY_TOKEN di Agent.
	VerifyToken string `gorm:"type:varchar(255);not null" json:"-"`

	// Timeout dalam detik untuk request ke Agent (default: 5)
	TimeoutSeconds int `gorm:"default:5" json:"timeout_seconds"`

	IsActive  bool           `gorm:"default:true" json:"is_active"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`
}

func (a *AgentConfig) BeforeCreate(tx *gorm.DB) (err error) {
	if a.ID == "" {
		a.ID = uuid.New().String()
	}
	return
}
