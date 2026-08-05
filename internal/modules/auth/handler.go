package auth

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Service Service
}

type AuthRequest struct {
	Username string `json:"username" binding:"required" example:"auditor_senior"`
	Password string `json:"password" binding:"required,min=6" example:"rahasia1234"`
}

type RegisterRequest struct {
	ClientID string `json:"client_id" binding:"required" example:"a1b2c3d4-e5f6-7890-1234-56789abcdef0"`
	Username string `json:"username" binding:"required" example:"auditor_senior"`
	Password string `json:"password" binding:"required,min=6" example:"rahasia1234"`
}

type ProfileUpdateRequest struct {
	FullName        string `json:"full_name"`
	Username        string `json:"username" binding:"required,min=4"`
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

func (h *Handler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format tidak valid atau client_id belum diisi."})
		return
	}

	user, client, err := h.Service.Register(req.ClientID, req.Username, req.Password)
	if err != nil {
		switch err.Error() {
		case "client_not_found":
			c.JSON(http.StatusNotFound, gin.H{"error": "Perusahaan (Client ID) tidak terdaftar di sistem"})
		case "username_used":
			c.JSON(http.StatusConflict, gin.H{"error": "Username sudah digunakan"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memproses pendaftaran"})
		}
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message": "Pengguna berhasil didaftarkan ke perusahaan " + client.CompanyName,
		"user": map[string]interface{}{
			"id":        user.ID,
			"client_id": user.ClientID,
			"username":  user.Username,
			"role":      user.Role,
		},
	})
}

func (h *Handler) Login(c *gin.Context) {
	var req AuthRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Format request tidak valid"})
		return
	}

	token, err := h.Service.Login(req.Username, req.Password)
	if err != nil {
		switch err.Error() {
		case "invalid_credentials":
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Username atau Password salah!"})
		case "client_inactive":
			c.JSON(http.StatusForbidden, gin.H{"error": "Account is disabled because the associated client is inactive."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mencetak token keamanan"})
		}
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Login berhasil",
		"token":   token,
	})
}

func (h *Handler) GetProfile(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Token tidak memiliki identitas user yang valid."})
		return
	}

	user, client, err := h.Service.GetProfile(userID.(string))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Profil user tidak ditemukan."})
		return
	}

	companyName := ""
	if client != nil {
		companyName = client.CompanyName
	}

	c.JSON(http.StatusOK, gin.H{
		"id":           user.ID,
		"full_name":    user.FullName,
		"username":     user.Username,
		"role":         user.Role,
		"client_id":    user.ClientID,
		"company_name": companyName,
		"created_at":   user.CreatedAt,
		"updated_at":   user.UpdatedAt,
	})
}

func (h *Handler) UpdateProfile(c *gin.Context) {
	userID, ok := c.Get("user_id")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Token tidak memiliki identitas user yang valid."})
		return
	}

	var req ProfileUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Nama pengguna minimal 4 karakter."})
		return
	}

	user, client, token, err := h.Service.UpdateProfile(
		userID.(string),
		req.FullName,
		req.Username,
		req.CurrentPassword,
		req.NewPassword,
	)
	if err != nil {
		switch err.Error() {
		case "username_used":
			c.JSON(http.StatusConflict, gin.H{"error": "Username sudah digunakan."})
		case "invalid_current_password":
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Password saat ini tidak sesuai."})
		case "password_too_short":
			c.JSON(http.StatusBadRequest, gin.H{"error": "Password baru minimal 6 karakter."})
		case "username_too_short", "username_required":
			c.JSON(http.StatusBadRequest, gin.H{"error": "Username minimal 4 karakter."})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal memperbarui profil."})
		}
		return
	}

	companyName := ""
	if client != nil {
		companyName = client.CompanyName
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Profil berhasil diperbarui.",
		"token":   token,
		"user": gin.H{
			"id":           user.ID,
			"full_name":    user.FullName,
			"username":     user.Username,
			"role":         user.Role,
			"client_id":    user.ClientID,
			"company_name": companyName,
		},
	})
}
