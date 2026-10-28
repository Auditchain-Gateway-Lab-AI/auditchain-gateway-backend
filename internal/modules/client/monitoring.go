package client

import (
	"errors"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const monitoringPageSize = 20

const monitoringUnavailableStatusSQL = `(
	UPPER(TRIM(COALESCE(integrity_status, ''))) IN ('UNREACHABLE', 'UNAVAILABLE', 'AGENT_ERROR', 'FABRIC_ERROR', 'TIMEOUT', 'FAILED')
	OR UPPER(TRIM(COALESCE(integrity_status, ''))) LIKE '%ERROR%'
)`

type monitoringMetrics struct {
	Total            int64      `json:"total" gorm:"column:total"`
	Anchored         int64      `json:"anchored" gorm:"column:anchored"`
	Pending          int64      `json:"pending" gorm:"column:pending"`
	Logs24H          int64      `json:"logs_24h" gorm:"column:logs_24h"`
	Valid            int64      `json:"valid" gorm:"column:valid"`
	Tampered         int64      `json:"tampered" gorm:"column:tampered"`
	VerifyPending    int64      `json:"verify_pending" gorm:"column:verify_pending"`
	Unavailable      int64      `json:"unavailable" gorm:"column:unavailable"`
	NotChecked       int64      `json:"not_checked" gorm:"column:not_checked"`
	CheckedAt        *time.Time `json:"checked_at" gorm:"column:checked_at"`
	LastLogAt        *time.Time `json:"last_log_at" gorm:"column:last_log_at"`
	OldestPendingAt  *time.Time `json:"oldest_pending_at" gorm:"column:oldest_pending_at"`
	AnchorPercentage *float64   `json:"anchor_percentage" gorm:"-"`
}

type monitoringReason struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Section string `json:"section"`
}

type monitoringClient struct {
	ID                    string             `json:"id"`
	CompanyName           string             `json:"company_name"`
	AdministrativeStatus  string             `json:"administrative_status"`
	Condition             string             `json:"condition"`
	Reasons               []monitoringReason `json:"reasons"`
	Metrics               monitoringMetrics  `json:"metrics"`
	AgentConfigured       bool               `json:"agent_configured"`
	AgentActive           bool               `json:"agent_active"`
	KafkaConfigured       bool               `json:"kafka_configured"`
	KafkaActive           bool               `json:"kafka_active"`
	ConnectorStatus       string             `json:"connector_status"`
	IntegrationReportedAt *time.Time         `json:"integration_reported_at"`
}

type monitoringSummary struct {
	Total     int64 `json:"total"`
	Critical  int64 `json:"critical"`
	Attention int64 `json:"attention"`
	Unknown   int64 `json:"unknown"`
	Normal    int64 `json:"normal"`
}

type monitoringOverviewResponse struct {
	FetchedAt               time.Time          `json:"fetched_at"`
	Summary                 monitoringSummary  `json:"summary"`
	Attention               []monitoringClient `json:"attention"`
	Clients                 []monitoringClient `json:"clients"`
	Total                   int64              `json:"total"`
	Page                    int                `json:"page"`
	PageSize                int                `json:"page_size"`
	Pages                   int                `json:"pages"`
	PendingThresholdMinutes int                `json:"pending_threshold_minutes"`
}

type monitoringAggregateRow struct {
	ClientID        string     `gorm:"column:client_id"`
	Total           int64      `gorm:"column:total"`
	Anchored        int64      `gorm:"column:anchored"`
	Pending         int64      `gorm:"column:pending"`
	Logs24H         int64      `gorm:"column:logs_24h"`
	Valid           int64      `gorm:"column:valid"`
	Tampered        int64      `gorm:"column:tampered"`
	VerifyPending   int64      `gorm:"column:verify_pending"`
	Unavailable     int64      `gorm:"column:unavailable"`
	NotChecked      int64      `gorm:"column:not_checked"`
	CheckedAt       *time.Time `gorm:"column:checked_at"`
	LastLogAt       *time.Time `gorm:"column:last_log_at"`
	OldestPendingAt *time.Time `gorm:"column:oldest_pending_at"`
}

type monitoringTable struct {
	TableName     string     `json:"table_name" gorm:"column:table_name"`
	Total         int64      `json:"total" gorm:"column:total"`
	Valid         int64      `json:"valid" gorm:"column:valid"`
	Tampered      int64      `json:"tampered" gorm:"column:tampered"`
	VerifyPending int64      `json:"verify_pending" gorm:"column:verify_pending"`
	Unavailable   int64      `json:"unavailable" gorm:"column:unavailable"`
	NotChecked    int64      `json:"not_checked" gorm:"column:not_checked"`
	CheckedAt     *time.Time `json:"checked_at" gorm:"column:checked_at"`
}

type monitoringIntegration struct {
	Agent    models.AgentConfig
	HasAgent bool
	Kafka    models.ClientKafkaConfig
	HasKafka bool
}

// GetMonitoringOverview returns global client conditions and a filtered page
// of client metrics. Every metric is read from current audit rows; a failed
// database query is returned as an error instead of being presented as zero.
func (h *Handler) GetMonitoringOverview(c *gin.Context) {
	if h.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database tidak tersedia"})
		return
	}

	condition := strings.ToLower(strings.TrimSpace(c.Query("condition")))
	if condition != "" && condition != "critical" && condition != "attention" && condition != "unknown" && condition != "normal" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "condition harus critical, attention, unknown, atau normal"})
		return
	}
	status := strings.ToLower(strings.TrimSpace(c.Query("status")))
	if status != "" && status != "active" && status != "inactive" && status != "pending_setup" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status harus active, inactive, atau pending_setup"})
		return
	}
	sortBy := strings.ToLower(strings.TrimSpace(c.DefaultQuery("sort", "condition")))
	if sortBy != "condition" && sortBy != "name" && sortBy != "logs_desc" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sort harus condition, name, atau logs_desc"})
		return
	}
	page := 1
	if rawPage := strings.TrimSpace(c.Query("page")); rawPage != "" {
		parsedPage, err := strconv.Atoi(rawPage)
		if err != nil || parsedPage < 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "page harus bilangan bulat positif"})
			return
		}
		page = parsedPage
	}
	query := strings.ToLower(strings.TrimSpace(c.Query("q")))

	db := h.DB.WithContext(c.Request.Context())
	var clients []models.Client
	if err := db.Order("created_at ASC, id ASC").Find(&clients).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil daftar client"})
		return
	}

	now := time.Now().UTC()
	threshold := monitoringPendingThresholdMinutes()
	clientIDs := make([]string, 0, len(clients))
	for _, client := range clients {
		clientIDs = append(clientIDs, client.ID)
	}
	metricsByClient, err := loadMonitoringMetrics(db, clientIDs, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil metrik monitoring"})
		return
	}
	integrations, err := loadMonitoringIntegrations(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil konfigurasi integrasi client"})
		return
	}

	allClients := make([]monitoringClient, 0, len(clients))
	summary := monitoringSummary{Total: int64(len(clients))}
	for _, client := range clients {
		integration := integrations[client.ID]
		item := buildMonitoringClient(client, metricsByClient[client.ID], integration, threshold, now)
		allClients = append(allClients, item)
		switch item.Condition {
		case "critical":
			summary.Critical++
		case "attention":
			summary.Attention++
		case "unknown":
			summary.Unknown++
		default:
			summary.Normal++
		}
	}

	attention := make([]monitoringClient, 0, 5)
	for _, item := range allClients {
		if item.Condition != "normal" {
			attention = append(attention, item)
		}
	}
	sort.Slice(attention, func(i, j int) bool { return monitoringClientLess(attention[i], attention[j], "condition") })
	if len(attention) > 5 {
		attention = attention[:5]
	}

	filtered := make([]monitoringClient, 0, len(allClients))
	for _, item := range allClients {
		if condition != "" && item.Condition != condition {
			continue
		}
		if status != "" && strings.ToLower(item.AdministrativeStatus) != status {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(item.CompanyName), query) {
			continue
		}
		filtered = append(filtered, item)
	}
	sort.Slice(filtered, func(i, j int) bool { return monitoringClientLess(filtered[i], filtered[j], sortBy) })

	total := int64(len(filtered))
	pages := int((total + int64(monitoringPageSize) - 1) / int64(monitoringPageSize))
	if pages < 1 {
		pages = 1
	}
	if page > pages {
		page = pages
	}
	start := (page - 1) * monitoringPageSize
	end := start + monitoringPageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	pageClients := filtered[start:end]

	c.JSON(http.StatusOK, monitoringOverviewResponse{
		FetchedAt:               now,
		Summary:                 summary,
		Attention:               attention,
		Clients:                 pageClients,
		Total:                   total,
		Page:                    page,
		PageSize:                monitoringPageSize,
		Pages:                   pages,
		PendingThresholdMinutes: threshold,
	})
}

// GetClientMonitoring returns one client's condition, current metrics, trend,
// table inventory and integration readiness. It performs read-only queries.
func (h *Handler) GetClientMonitoring(c *gin.Context) {
	if h.DB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database tidak tersedia"})
		return
	}

	clientID := strings.TrimSpace(c.Param("id"))
	if clientID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ID client tidak boleh kosong"})
		return
	}
	rangeName := strings.ToUpper(strings.TrimSpace(c.DefaultQuery("range", "7D")))
	if rangeName != "24H" && rangeName != "7D" && rangeName != "30D" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "range harus 24H, 7D, atau 30D"})
		return
	}

	db := h.DB.WithContext(c.Request.Context())
	var client models.Client
	if err := db.First(&client, "id = ?", clientID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Client tidak ditemukan"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil data client"})
		}
		return
	}

	now := time.Now().UTC()
	threshold := monitoringPendingThresholdMinutes()
	metricsByClient, err := loadMonitoringMetrics(db, []string{clientID}, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil metrik monitoring"})
		return
	}
	integration, err := loadMonitoringIntegration(db, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil konfigurasi integrasi client"})
		return
	}
	window, _ := parseDashboardTrendRange(rangeName)
	trend, err := loadDashboardTrend(db, clientID, window, now)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil tren monitoring"})
		return
	}
	tables, err := loadMonitoringTables(db, clientID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengambil ringkasan tabel monitoring"})
		return
	}

	item := buildMonitoringClient(client, metricsByClient[clientID], integration, threshold, now)
	c.JSON(http.StatusOK, gin.H{
		"fetched_at":                now,
		"client":                    item,
		"trend":                     trend,
		"tables":                    tables,
		"pending_threshold_minutes": threshold,
	})
}

func loadMonitoringMetrics(db *gorm.DB, clientIDs []string, now time.Time) (map[string]monitoringMetrics, error) {
	result := make(map[string]monitoringMetrics, len(clientIDs))
	if len(clientIDs) == 0 {
		return result, nil
	}

	var rows []monitoringAggregateRow
	err := db.Raw(`
		SELECT
			client_id,
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(status, ''))) = 'ANCHORED' AND NULLIF(BTRIM(blockchain_tx_id), '') IS NOT NULL) AS anchored,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(status, ''))) IN ('RECEIVED', 'HASHED', 'AGGREGATED')) AS pending,
			COUNT(*) FILTER (WHERE timestamp >= ? AND timestamp < ?) AS logs_24h,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'VALID') AS valid,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'TAMPERED') AS tampered,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'PENDING') AS verify_pending,
			COUNT(*) FILTER (WHERE `+monitoringUnavailableStatusSQL+`) AS unavailable,
			COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) IN ('', 'NOT_CHECKED')) AS not_checked,
			MAX(GREATEST(integrity_checked_at, source_integrity_checked_at)) AS checked_at,
			MAX(timestamp) AS last_log_at,
			MIN(COALESCE(db_timestamp, timestamp)) FILTER (
				WHERE UPPER(TRIM(COALESCE(status, ''))) IN ('RECEIVED', 'HASHED', 'AGGREGATED')
			) AS oldest_pending_at
		FROM audit_logs
		WHERE client_id IN ?
		  AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
		GROUP BY client_id
	`, now.Add(-24*time.Hour), now, clientIDs).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		metrics := monitoringMetrics{
			Total:           row.Total,
			Anchored:        row.Anchored,
			Pending:         row.Pending,
			Logs24H:         row.Logs24H,
			Valid:           row.Valid,
			Tampered:        row.Tampered,
			VerifyPending:   row.VerifyPending,
			Unavailable:     row.Unavailable,
			NotChecked:      row.NotChecked,
			CheckedAt:       row.CheckedAt,
			LastLogAt:       row.LastLogAt,
			OldestPendingAt: row.OldestPendingAt,
		}
		if metrics.Total > 0 {
			percentage := float64(metrics.Anchored) / float64(metrics.Total) * 100
			metrics.AnchorPercentage = &percentage
		}
		result[row.ClientID] = metrics
	}
	return result, nil
}

func loadMonitoringIntegrations(db *gorm.DB) (map[string]monitoringIntegration, error) {
	var agents []models.AgentConfig
	if err := db.Find(&agents).Error; err != nil {
		return nil, err
	}
	var kafkaConfigs []models.ClientKafkaConfig
	if err := db.Find(&kafkaConfigs).Error; err != nil {
		return nil, err
	}
	result := make(map[string]monitoringIntegration, len(agents)+len(kafkaConfigs))
	for _, config := range agents {
		item := result[config.ClientID]
		item.Agent = config
		item.HasAgent = true
		result[config.ClientID] = item
	}
	for _, config := range kafkaConfigs {
		item := result[config.ClientID]
		item.Kafka = config
		item.HasKafka = true
		result[config.ClientID] = item
	}
	return result, nil
}

func loadMonitoringIntegration(db *gorm.DB, clientID string) (monitoringIntegration, error) {
	var result monitoringIntegration
	err := db.Where("client_id = ?", clientID).First(&result.Agent).Error
	if err == nil {
		result.HasAgent = true
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return result, err
	}
	err = db.Where("client_id = ?", clientID).First(&result.Kafka).Error
	if err == nil {
		result.HasKafka = true
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return result, err
	}
	return result, nil
}

func buildMonitoringClient(client models.Client, metrics monitoringMetrics, integration monitoringIntegration, threshold int, now time.Time) monitoringClient {
	reasons := make([]monitoringReason, 0, 5)
	condition := "normal"
	if metrics.Tampered > 0 {
		condition = "critical"
		reasons = append(reasons, monitoringReason{Code: "TAMPERED", Message: "Ditemukan log dengan integrity TAMPERED", Section: "integrity"})
	}
	if metrics.Unavailable > 0 {
		if condition == "normal" {
			condition = "attention"
		}
		reasons = append(reasons, monitoringReason{Code: "VERIFICATION_UNAVAILABLE", Message: "Hasil pemeriksaan integrity tidak tersedia atau gagal", Section: "integrity"})
	}
	if integration.HasAgent && connectorReportedFailure(integration.Agent.ConnectorStatus) {
		if condition == "normal" {
			condition = "attention"
		}
		reasons = append(reasons, monitoringReason{Code: "CONNECTOR_ERROR", Message: "Laporan connector terakhir menunjukkan gagal atau error", Section: "integration"})
	}
	if metrics.OldestPendingAt != nil && now.Sub(*metrics.OldestPendingAt) > time.Duration(threshold)*time.Minute {
		if condition == "normal" {
			condition = "attention"
		}
		reasons = append(reasons, monitoringReason{Code: "PENDING_ANCHORING", Message: "Anchoring tertunda lebih dari batas waktu yang ditentukan", Section: "anchoring"})
	}
	if metrics.Total == 0 {
		if condition == "normal" {
			condition = "unknown"
		}
		reasons = append(reasons, monitoringReason{Code: "NO_LOGS", Message: "Belum ada audit log untuk client ini", Section: "activity"})
	}
	if metrics.CheckedAt == nil {
		if condition == "normal" {
			condition = "unknown"
		}
		reasons = append(reasons, monitoringReason{Code: "NOT_CHECKED", Message: "Belum ada pemeriksaan integrity", Section: "integrity"})
	}

	item := monitoringClient{
		ID:                   client.ID,
		CompanyName:          client.CompanyName,
		AdministrativeStatus: client.Status,
		Condition:            condition,
		Reasons:              reasons,
		Metrics:              metrics,
		AgentConfigured:      integration.HasAgent,
		AgentActive:          integration.HasAgent && integration.Agent.IsActive,
		KafkaConfigured:      integration.HasKafka,
		KafkaActive:          integration.HasKafka && integration.Kafka.IsActive,
	}
	if integration.HasAgent {
		item.ConnectorStatus = integration.Agent.ConnectorStatus
		if !integration.Agent.UpdatedAt.IsZero() {
			reportedAt := integration.Agent.UpdatedAt
			item.IntegrationReportedAt = &reportedAt
		}
	}
	return item
}

func connectorReportedFailure(status string) bool {
	status = strings.ToLower(strings.TrimSpace(status))
	return strings.Contains(status, "fail") || strings.Contains(status, "error")
}

func monitoringPendingThresholdMinutes() int {
	const maxDurationMinutes = int64(1<<63-1) / int64(time.Minute)
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MONITORING_PENDING_THRESHOLD_MINUTES")))
	if err != nil || value <= 0 || int64(value) > maxDurationMinutes {
		return 15
	}
	return value
}

func monitoringConditionRank(condition string) int {
	switch condition {
	case "critical":
		return 0
	case "attention":
		return 1
	case "unknown":
		return 2
	default:
		return 3
	}
}

func monitoringClientLess(left, right monitoringClient, sortBy string) bool {
	if sortBy == "condition" {
		leftRank, rightRank := monitoringConditionRank(left.Condition), monitoringConditionRank(right.Condition)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
	}
	if sortBy == "logs_desc" && left.Metrics.Logs24H != right.Metrics.Logs24H {
		return left.Metrics.Logs24H > right.Metrics.Logs24H
	}
	leftName, rightName := strings.ToLower(left.CompanyName), strings.ToLower(right.CompanyName)
	if leftName != rightName {
		return leftName < rightName
	}
	return left.ID < right.ID
}

func loadMonitoringTables(db *gorm.DB, clientID string) ([]monitoringTable, error) {
	tables := make([]monitoringTable, 0)
	query := `
		WITH table_names AS (
			SELECT NULLIF(BTRIM(table_name), '') AS table_name
			FROM client_tables
			WHERE client_id = ? AND NULLIF(BTRIM(table_name), '') IS NOT NULL
			UNION
			SELECT COALESCE(NULLIF(BTRIM(SPLIT_PART(resource, ':', 1)), ''), 'unknown') AS table_name
			FROM audit_logs
			WHERE client_id = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
		), log_metrics AS (
			SELECT
				COALESCE(NULLIF(BTRIM(SPLIT_PART(resource, ':', 1)), ''), 'unknown') AS table_name,
				COUNT(*) AS total,
				COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'VALID') AS valid,
				COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'TAMPERED') AS tampered,
				COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) = 'PENDING') AS verify_pending,
				COUNT(*) FILTER (WHERE ` + monitoringUnavailableStatusSQL + `) AS unavailable,
				COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status, ''))) IN ('', 'NOT_CHECKED')) AS not_checked,
				MAX(GREATEST(integrity_checked_at, source_integrity_checked_at)) AS checked_at
			FROM audit_logs
			WHERE client_id = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
			GROUP BY 1
		)
		SELECT
			table_names.table_name,
			COALESCE(log_metrics.total, 0) AS total,
			COALESCE(log_metrics.valid, 0) AS valid,
			COALESCE(log_metrics.tampered, 0) AS tampered,
			COALESCE(log_metrics.verify_pending, 0) AS verify_pending,
			COALESCE(log_metrics.unavailable, 0) AS unavailable,
			COALESCE(log_metrics.not_checked, 0) AS not_checked,
			log_metrics.checked_at
		FROM table_names
		LEFT JOIN log_metrics ON log_metrics.table_name = table_names.table_name
		ORDER BY LOWER(table_names.table_name), table_names.table_name
	`
	err := db.Raw(query, clientID, clientID, clientID).Scan(&tables).Error
	return tables, err
}
