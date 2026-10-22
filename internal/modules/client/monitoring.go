package client

import (
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

type MonitoringCounts struct {
	Total            int64      `json:"total"`
	Anchored         int64      `json:"anchored"`
	Pending          int64      `json:"pending"`
	Logs24H          int64      `json:"logs_24h" gorm:"column:logs_24h"`
	Valid            int64      `json:"valid"`
	Tampered         int64      `json:"tampered"`
	VerifyPending    int64      `json:"verify_pending"`
	Unavailable      int64      `json:"unavailable"`
	NotChecked       int64      `json:"not_checked"`
	LastLogAt        *time.Time `json:"last_log_at"`
	CheckedAt        *time.Time `json:"checked_at"`
	OldestPendingAt  *time.Time `json:"oldest_pending_at"`
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
	Metrics               *MonitoringCounts  `json:"metrics"`
	AgentConfigured       bool               `json:"agent_configured"`
	KafkaConfigured       bool               `json:"kafka_configured"`
	AgentActive           *bool              `json:"agent_active"`
	KafkaActive           *bool              `json:"kafka_active"`
	ConnectorStatus       *string            `json:"connector_status"`
	IntegrationReportedAt *time.Time         `json:"integration_reported_at"`
}

// Use the same current integrity buckets as the tenant dashboard, independent
// of the historical verification cache. Unknown future statuses stay unchecked.
const monitoringAggregate = `COUNT(*) AS total,
 COUNT(*) FILTER (WHERE status = 'ANCHORED') AS anchored,
 COUNT(*) FILTER (WHERE status IN ('RECEIVED','HASHED','AGGREGATED')) AS pending,
 COUNT(*) FILTER (WHERE timestamp >= ? AND timestamp < ?) AS logs_24h,
 COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status,''))) = 'VALID') AS valid,
 COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status,''))) = 'TAMPERED') AS tampered,
 COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status,''))) = 'PENDING') AS verify_pending,
 COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status,''))) IN ('UNREACHABLE','UNAVAILABLE','AGENT_ERROR','FABRIC_ERROR','ERROR')) AS unavailable,
 COUNT(*) FILTER (WHERE UPPER(TRIM(COALESCE(integrity_status,''))) NOT IN ('VALID','TAMPERED','PENDING','UNREACHABLE','UNAVAILABLE','AGENT_ERROR','FABRIC_ERROR','ERROR')) AS not_checked,
 MAX(timestamp) AS last_log_at, MAX(integrity_checked_at) AS checked_at,
 MIN(COALESCE(db_timestamp,timestamp)) FILTER (WHERE status IN ('RECEIVED','HASHED','AGGREGATED')) AS oldest_pending_at`

func monitoringThreshold() time.Duration {
	minutes, err := strconv.Atoi(os.Getenv("MONITORING_PENDING_THRESHOLD_MINUTES"))
	if err != nil || minutes <= 0 {
		minutes = 15
	}
	return time.Duration(minutes) * time.Minute
}

func classifyMonitoring(row *monitoringClient, now time.Time, threshold time.Duration) {
	row.Reasons = []monitoringReason{}
	add := func(code, message, section string) {
		row.Reasons = append(row.Reasons, monitoringReason{code, message, section})
	}
	critical, attention, unknown := false, false, false
	m := row.Metrics
	if m == nil {
		unknown = true
		add("DATA_UNAVAILABLE", "Data monitoring belum tersedia", "integrity")
	} else {
		if m.Tampered > 0 {
			critical = true
			add("TAMPERED", "Ditemukan log dengan integrity TAMPERED", "integrity")
		}
		if m.Unavailable > 0 {
			attention = true
			add("VERIFICATION_UNAVAILABLE", "Ada hasil verifikasi unavailable/error", "integrity")
		}
		if m.OldestPendingAt != nil && now.Sub(*m.OldestPendingAt) > threshold {
			attention = true
			add("ANCHORING_DELAY", "Pending anchoring melewati batas waktu", "anchoring")
		}
		if m.Total == 0 {
			unknown = true
			add("NO_LOGS", "Belum ada audit log", "activity")
		} else if m.CheckedAt == nil {
			unknown = true
			add("NEVER_CHECKED", "Belum pernah diperiksa", "integrity")
		}
		if m.Total > 0 {
			percentage := float64(m.Anchored) * 100 / float64(m.Total)
			m.AnchorPercentage = &percentage
		}
	}
	if row.ConnectorStatus != nil {
		status := strings.ToLower(*row.ConnectorStatus)
		if strings.Contains(status, "fail") || strings.Contains(status, "error") {
			attention = true
			add("CONNECTOR_FAILED", "Connector terakhir melaporkan kegagalan", "integration")
		}
	}
	row.Condition = "normal"
	if unknown {
		row.Condition = "unknown"
	}
	if attention {
		row.Condition = "attention"
	}
	if critical {
		row.Condition = "critical"
	}
}

// The optional ID narrows every aggregate for detail requests. Overview uses a
// fixed number of grouped queries rather than one query per client.
func loadMonitoring(db *gorm.DB, id string, now time.Time) ([]monitoringClient, error) {
	var clients []models.Client
	query := db.Model(&models.Client{})
	if id != "" {
		query = query.Where("id = ?", id)
	}
	if err := query.Find(&clients).Error; err != nil {
		return nil, err
	}
	rows := make([]monitoringClient, 0, len(clients))
	if len(clients) == 0 {
		return rows, nil
	}
	ids := make([]string, 0, len(clients))
	for _, c := range clients {
		ids = append(ids, c.ID)
	}
	var aggregates []struct {
		ClientID         string
		MonitoringCounts `gorm:"embedded"`
	}
	if err := db.Table("audit_logs").Select("client_id, "+monitoringAggregate, now.Add(-24*time.Hour), now).
		Where("client_id IN ? AND COALESCE(UPPER(TRIM(action)),'') <> 'RECOVERY'", ids).Group("client_id").Scan(&aggregates).Error; err != nil {
		return nil, err
	}
	counts := map[string]MonitoringCounts{}
	for _, a := range aggregates {
		counts[a.ClientID] = a.MonitoringCounts
	}
	var agents []models.AgentConfig
	if err := db.Where("client_id IN ?", ids).Find(&agents).Error; err != nil {
		return nil, err
	}
	var kafka []models.ClientKafkaConfig
	if err := db.Where("client_id IN ?", ids).Find(&kafka).Error; err != nil {
		return nil, err
	}
	agentMap := map[string]models.AgentConfig{}
	for _, a := range agents {
		agentMap[a.ClientID] = a
	}
	kafkaMap := map[string]models.ClientKafkaConfig{}
	for _, k := range kafka {
		kafkaMap[k.ClientID] = k
	}
	for _, c := range clients {
		metrics := counts[c.ID]
		row := monitoringClient{ID: c.ID, CompanyName: c.CompanyName, AdministrativeStatus: c.Status, Metrics: &metrics}
		if a, ok := agentMap[c.ID]; ok {
			row.AgentConfigured = true
			row.AgentActive = &a.IsActive
			row.IntegrationReportedAt = &a.UpdatedAt
			if a.ConnectorStatus != "" {
				row.ConnectorStatus = &a.ConnectorStatus
			}
		}
		if k, ok := kafkaMap[c.ID]; ok {
			row.KafkaConfigured = true
			row.KafkaActive = &k.IsActive
		}
		classifyMonitoring(&row, now, monitoringThreshold())
		rows = append(rows, row)
	}
	return rows, nil
}

func monitoringRank(condition string) int {
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

func sortMonitoring(rows []monitoringClient, order string) {
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if order == "condition" && a.Condition != b.Condition {
			return monitoringRank(a.Condition) < monitoringRank(b.Condition)
		}
		if order == "logs_desc" && a.Metrics.Logs24H != b.Metrics.Logs24H {
			return a.Metrics.Logs24H > b.Metrics.Logs24H
		}
		if strings.EqualFold(a.CompanyName, b.CompanyName) {
			return a.ID < b.ID
		}
		return strings.ToLower(a.CompanyName) < strings.ToLower(b.CompanyName)
	})
}

func (h *Handler) GetMonitoringOverview(c *gin.Context) {
	condition, status, order := c.Query("condition"), c.Query("status"), c.DefaultQuery("sort", "condition")
	if condition != "" && condition != "critical" && condition != "attention" && condition != "unknown" && condition != "normal" {
		c.JSON(400, gin.H{"error": "Kondisi tidak valid"})
		return
	}
	if order != "condition" && order != "name" && order != "logs_desc" {
		c.JSON(400, gin.H{"error": "Sorting tidak valid"})
		return
	}
	page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
	if err != nil || page < 1 {
		c.JSON(400, gin.H{"error": "Halaman tidak valid"})
		return
	}
	now := time.Now().UTC()
	rows, err := loadMonitoring(h.DB.WithContext(c.Request.Context()), "", now)
	if err != nil {
		c.JSON(500, gin.H{"error": "Gagal mengambil monitoring client"})
		return
	}
	summary := map[string]int{"total": len(rows), "critical": 0, "attention": 0, "unknown": 0, "normal": 0}
	for _, r := range rows {
		summary[r.Condition]++
	}
	sortMonitoring(rows, "condition")
	attention := []monitoringClient{}
	for _, r := range rows {
		if (r.Condition == "critical" || r.Condition == "attention") && len(attention) < 5 {
			attention = append(attention, r)
		}
	}
	filtered := []monitoringClient{}
	search := strings.ToLower(strings.TrimSpace(c.Query("q")))
	for _, r := range rows {
		if (condition == "" || r.Condition == condition) && (status == "" || r.AdministrativeStatus == status) && strings.Contains(strings.ToLower(r.CompanyName), search) {
			filtered = append(filtered, r)
		}
	}
	sortMonitoring(filtered, order)
	total := len(filtered)
	pages := (total + 19) / 20
	if pages < 1 {
		pages = 1
	}
	if page > pages {
		page = pages
	}
	start := (page - 1) * 20
	end := start + 20
	if end > total {
		end = total
	}
	c.JSON(200, gin.H{"fetched_at": now, "summary": summary, "attention": attention, "clients": filtered[start:end], "total": total, "page": page, "page_size": 20, "pages": pages, "pending_threshold_minutes": int(monitoringThreshold().Minutes())})
}

type monitoringTable struct {
	TableName        string `json:"table_name"`
	MonitoringCounts `gorm:"embedded"`
}

func (h *Handler) GetClientMonitoring(c *gin.Context) {
	rangeName := strings.ToUpper(c.DefaultQuery("range", "7D"))
	window, ok := parseDashboardTrendRange(rangeName)
	if !ok || rangeName == "8H" {
		c.JSON(400, gin.H{"error": "Range harus 24H, 7D, atau 30D"})
		return
	}
	now := time.Now().UTC()
	db := h.DB.WithContext(c.Request.Context())
	rows, err := loadMonitoring(db, c.Param("id"), now)
	if err != nil {
		c.JSON(500, gin.H{"error": "Gagal mengambil dashboard client"})
		return
	}
	if len(rows) == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Client tidak ditemukan"})
		return
	}
	trend, err := loadDashboardTrend(db, rows[0].ID, window, now)
	if err != nil {
		c.JSON(500, gin.H{"error": "Gagal mengambil tren client"})
		return
	}
	tables := []monitoringTable{}
	// Match inventory's source table metadata, falling back to the resource name.
	tableExpr := `COALESCE(NULLIF(SPLIT_PART(resource, ':', 1),''), 'Unknown')`
	if err := db.Table("audit_logs").Select(tableExpr+" AS table_name, "+monitoringAggregate, now.Add(-24*time.Hour), now).
		Where("client_id = ? AND COALESCE(UPPER(TRIM(action)),'') <> 'RECOVERY'", rows[0].ID).Group(tableExpr).Order("table_name").Scan(&tables).Error; err != nil {
		c.JSON(500, gin.H{"error": "Gagal mengambil ringkasan tabel"})
		return
	}
	// Include configured tables even when no log has arrived yet.
	var inventory []models.ClientTable
	if err := db.Where("client_id = ?", rows[0].ID).Find(&inventory).Error; err != nil {
		c.JSON(500, gin.H{"error": "Gagal mengambil inventory"})
		return
	}
	seen := map[string]bool{}
	for _, table := range tables {
		seen[table.TableName] = true
	}
	for _, table := range inventory {
		if !seen[table.TableName] {
			tables = append(tables, monitoringTable{TableName: table.TableName})
			seen[table.TableName] = true
		}
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].TableName < tables[j].TableName })
	c.JSON(200, gin.H{"fetched_at": now, "client": rows[0], "trend": trend, "tables": tables, "pending_threshold_minutes": int(monitoringThreshold().Minutes())})
}
