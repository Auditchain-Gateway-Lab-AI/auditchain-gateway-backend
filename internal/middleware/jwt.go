package middleware

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// JWTAuth melindungi endpoint Dashboard — hanya user login yang boleh masuk
func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Akses ditolak. Token tidak ditemukan."})
			c.Abort()
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Format token salah. Gunakan format: Bearer <token>"})
			c.Abort()
			return
		}

		tokenString := parts[1]
		secret := os.Getenv("JWT_SECRET")

		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, http.ErrAbortHandler
			}
			return []byte(secret), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Token tidak valid atau sudah kedaluwarsa. Silakan login kembali.",
			})
			c.Abort()
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Klaim token tidak valid."})
			c.Abort()
			return
		}

		clientIDVal, ok := claims["client_id"]
		clientID, okString := clientIDVal.(string)
		if !ok || !okString || clientID == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Token tidak memiliki identitas client yang valid."})
			c.Abort()
			return
		}

		c.Set("client_id", clientID)
		c.Next()
	}
}

// AdminAuth melindungi endpoint admin — hanya internal system yang boleh akses
// Gunakan ADMIN_SECRET di .env, inject via header X-Admin-Secret
func AdminAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		secret := os.Getenv("ADMIN_SECRET")
		if secret == "" {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Konfigurasi admin belum diset."})
			c.Abort()
			return
		}

		provided := c.GetHeader("X-Admin-Secret")
		if provided == "" || provided != secret {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Akses ditolak. Kredensial admin tidak valid."})
			c.Abort()
			return
		}

		c.Next()
	}
}
