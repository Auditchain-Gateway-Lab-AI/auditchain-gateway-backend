package client

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
)

type monitoringStatisticsResponse struct {
	FetchedAt               time.Time               `json:"fetched_at"`
	Summary                 monitoringSummary       `json:"summary"`
	Metrics                 monitoringMetrics       `json:"metrics"`
	Trend                   *DashboardTrendResponse `json:"trend"`
	Attention               []monitoringClient      `json:"attention"`
	PendingThresholdMinutes int                     `json:"pending_threshold_minutes"`
}

// GetMonitoringStatistics reads current audit rows for every registered client.
// range affects only the activity trend, never the current totals or conditions.
func (h *Handler) GetMonitoringStatistics(c *gin.Context) {
	rangeName := strings.ToUpper(strings.TrimSpace(c.DefaultQuery("range", "7D")))
	if rangeName != "24H" && rangeName != "7D" && rangeName != "30D" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "range harus 24H, 7D, atau 30D"})
		return
	}
	if h.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database tidak tersedia"})
		return
	}
	db := h.DB.WithContext(c.Request.Context())
	var clients []models.Client
	if err := db.Find(&clients).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar client"})
		return
	}
	now := time.Now().UTC()
	threshold := monitoringPendingThresholdMinutes()
	ids := make([]string, 0, len(clients))
	for _, client := range clients {
		ids = append(ids, client.ID)
	}
	metrics, err := loadMonitoringMetrics(db, ids, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil metrik monitoring"})
		return
	}
	integrations, err := loadMonitoringIntegrations(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil konfigurasi integrasi client"})
		return
	}
	items := make([]monitoringClient, 0, len(clients))
	for _, client := range clients {
		items = append(items, buildMonitoringClient(client, metrics[client.ID], integrations[client.ID], threshold, now))
	}
	result := summarizeMonitoringStatistics(items)
	window, _ := parseDashboardTrendRange(rangeName)
	result.Trend, err = loadDashboardTrendForClients(db, ids, window, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil tren monitoring"})
		return
	}
	result.FetchedAt = now
	result.PendingThresholdMinutes = threshold
	c.JSON(http.StatusOK, result)
}

func summarizeMonitoringStatistics(clients []monitoringClient) monitoringStatisticsResponse {
	result := monitoringStatisticsResponse{Attention: make([]monitoringClient, 0, 5)}
	result.Summary.Total = int64(len(clients))
	for _, client := range clients {
		switch client.Condition {
		case "critical":
			result.Summary.Critical++
		case "attention":
			result.Summary.Attention++
		case "unknown":
			result.Summary.Unknown++
		default:
			result.Summary.Normal++
		}
		if client.Condition != "normal" {
			result.Attention = append(result.Attention, client)
		}
		m := client.Metrics
		r := &result.Metrics
		r.Total += m.Total
		r.Anchored += m.Anchored
		r.Pending += m.Pending
		r.Logs24H += m.Logs24H
		r.Valid += m.Valid
		r.Tampered += m.Tampered
		r.VerifyPending += m.VerifyPending
		r.Unavailable += m.Unavailable
		r.NotChecked += m.NotChecked
		if m.CheckedAt != nil && (r.CheckedAt == nil || m.CheckedAt.After(*r.CheckedAt)) {
			r.CheckedAt = m.CheckedAt
		}
		if m.LastLogAt != nil && (r.LastLogAt == nil || m.LastLogAt.After(*r.LastLogAt)) {
			r.LastLogAt = m.LastLogAt
		}
		if m.OldestPendingAt != nil && (r.OldestPendingAt == nil || m.OldestPendingAt.Before(*r.OldestPendingAt)) {
			r.OldestPendingAt = m.OldestPendingAt
		}
	}
	if result.Metrics.Total > 0 {
		percentage := float64(result.Metrics.Anchored) / float64(result.Metrics.Total) * 100
		result.Metrics.AnchorPercentage = &percentage
	}
	sort.Slice(result.Attention, func(i, j int) bool {
		return monitoringClientLess(result.Attention[i], result.Attention[j], "condition")
	})
	if len(result.Attention) > 5 {
		result.Attention = result.Attention[:5]
	}
	return result
}
