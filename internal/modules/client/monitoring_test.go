package client

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestMonitoringConditions(t *testing.T) {
	now := time.Now().UTC()
	exact := now.Add(-15 * time.Minute)
	delayed := exact.Add(-time.Second)
	failed := "failed_connection"
	tests := []struct {
		name      string
		metrics   *MonitoringCounts
		connector *string
		want      string
		reasons   int
	}{
		{"missing", nil, nil, "unknown", 1},
		{"empty", &MonitoringCounts{}, nil, "unknown", 1},
		{"unchecked", &MonitoringCounts{Total: 3, NotChecked: 3}, nil, "unknown", 1},
		{"normal", &MonitoringCounts{Total: 3, Valid: 3, CheckedAt: &now}, nil, "normal", 0},
		{"exact threshold", &MonitoringCounts{Total: 3, CheckedAt: &now, OldestPendingAt: &exact}, nil, "normal", 0},
		{"delayed", &MonitoringCounts{Total: 3, CheckedAt: &now, OldestPendingAt: &delayed}, nil, "attention", 1},
		{"unavailable", &MonitoringCounts{Total: 3, Unavailable: 1, CheckedAt: &now}, nil, "attention", 1},
		{"all reasons", &MonitoringCounts{Total: 3, Tampered: 1, Unavailable: 1, OldestPendingAt: &delayed}, &failed, "critical", 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row := monitoringClient{Metrics: test.metrics, ConnectorStatus: test.connector}
			classifyMonitoring(&row, now, 15*time.Minute)
			if row.Condition != test.want || len(row.Reasons) != test.reasons {
				t.Fatalf("got %s, %v", row.Condition, row.Reasons)
			}
			if row.Metrics != nil && row.Metrics.Total == 0 && row.Metrics.AnchorPercentage != nil {
				t.Fatal("empty coverage must be null")
			}
		})
	}
}

func TestMonitoringThreshold(t *testing.T) {
	for value, want := range map[string]time.Duration{"": 15 * time.Minute, "invalid": 15 * time.Minute, "-1": 15 * time.Minute, "30": 30 * time.Minute} {
		t.Setenv("MONITORING_PENDING_THRESHOLD_MINUTES", value)
		if got := monitoringThreshold(); got != want {
			t.Fatalf("%q: %s", value, got)
		}
	}
}

func TestMonitoringSort(t *testing.T) {
	rows := []monitoringClient{
		{ID: "b", CompanyName: "Beta", Condition: "critical", Metrics: &MonitoringCounts{Logs24H: 2}},
		{ID: "a", CompanyName: "Alpha", Condition: "normal", Metrics: &MonitoringCounts{Logs24H: 5}},
		{ID: "c", CompanyName: "Alpha", Condition: "critical", Metrics: &MonitoringCounts{Logs24H: 2}},
	}
	for order, expected := range map[string]string{"condition": "cba", "name": "acb", "logs_desc": "acb"} {
		sortMonitoring(rows, order)
		got := ""
		for _, row := range rows {
			got += row.ID
		}
		if got != expected {
			t.Fatalf("%s: %s, want %s", order, got, expected)
		}
	}
}

func TestMonitoringRoutesRejectNonAdmins(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("JWT_SECRET", "monitoring-test-secret")
	router := gin.New()
	RegisterRoutes(router.Group("/api"), &Handler{})
	for _, path := range []string{"/api/admin/monitoring/overview", "/api/admin/clients/a/monitoring"} {
		for _, role := range []string{"", "auditor"} {
			request := httptest.NewRequest("GET", path, nil)
			want := 401
			if role != "" {
				token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"role": role, "client_id": "a", "exp": time.Now().Add(time.Hour).Unix()})
				signed, err := token.SignedString([]byte("monitoring-test-secret"))
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Authorization", "Bearer "+signed)
				want = 403
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != want {
				t.Fatalf("%s %s: %d", path, role, response.Code)
			}
		}
	}
}

// Run against an isolated PostgreSQL instance using MONITORING_TEST_DATABASE_URL.
// All fixtures are connection-local TEMP tables inside a rolled-back transaction.
func TestMonitoringPostgres(t *testing.T) {
	dsn := os.Getenv("MONITORING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MONITORING_TEST_DATABASE_URL is not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	statements := []string{
		`CREATE TEMP TABLE clients (id text, company_name text, status text, deleted_at timestamptz)`,
		`CREATE TEMP TABLE audit_logs (client_id text, resource text, action text, timestamp timestamptz, db_timestamp timestamptz, status text, integrity_status text, integrity_checked_at timestamptz, source_integrity_status text)`,
		`CREATE TEMP TABLE agent_configs (client_id text, is_active boolean, connector_status text, updated_at timestamptz, deleted_at timestamptz)`,
		`CREATE TEMP TABLE client_kafka_configs (client_id text, is_active boolean, deleted_at timestamptz)`,
		`CREATE TEMP TABLE client_tables (client_id text, table_name text)`,
		`INSERT INTO clients VALUES ('a','Alpha','active',NULL),('b','Beta','inactive',NULL),('c','Empty','active',NULL),('deleted','Deleted','active',now())`,
		`INSERT INTO audit_logs VALUES ('a','patients:1','INSERT',now()-interval '1 hour',now()-interval '1 hour','ANCHORED','TAMPERED',now(),'NOT_CHECKED'),('a','patients:2','UPDATE',now()-interval '2 hour',now()-interval '20 minute','RECEIVED','VALID',now(),'NOT_CHECKED'),('a','patients:3','RECOVERY',now(),now(),'ANCHORED','TAMPERED',now(),'NOT_CHECKED'),('b','private:1','INSERT',now(),now(),'ANCHORED','VALID',now(),'VALID')`,
		`INSERT INTO agent_configs VALUES ('a',true,'failed_connector',now(),NULL)`,
		`INSERT INTO client_kafka_configs VALUES ('a',true,NULL)`,
		`INSERT INTO client_tables VALUES ('a','patients'),('a','empty_table'),('b','private')`,
	}
	for _, statement := range statements {
		if err := tx.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("MONITORING_PENDING_THRESHOLD_MINUTES", "15")
	rows, err := loadMonitoring(tx, "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("clients = %d", len(rows))
	}
	sortMonitoring(rows, "condition")
	a := rows[0]
	if a.ID != "a" || a.Condition != "critical" || a.Metrics.Total != 2 || a.Metrics.Logs24H != 2 || a.Metrics.Tampered != 1 || *a.Metrics.AnchorPercentage != 50 {
		t.Fatalf("unexpected Alpha: %+v, metrics %+v", a, a.Metrics)
	}
	if rows[1].ID != "c" || rows[2].ID != "b" {
		t.Fatalf("priority: %+v", rows)
	}
	gin.SetMode(gin.TestMode)
	handler := &Handler{DB: tx}
	call := func(path, id string, fn gin.HandlerFunc) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(response)
		ctx.Request = httptest.NewRequest("GET", path, nil)
		ctx.Params = gin.Params{{Key: "id", Value: id}}
		fn(ctx)
		return response
	}
	detail := call("/?range=7D", "a", handler.GetClientMonitoring)
	if detail.Code != 200 {
		t.Fatalf("detail %d: %s", detail.Code, detail.Body.String())
	}
	var body struct {
		Client monitoringClient       `json:"client"`
		Tables []monitoringTable      `json:"tables"`
		Trend  DashboardTrendResponse `json:"trend"`
	}
	if err := json.Unmarshal(detail.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Client.Metrics.Total != a.Metrics.Total || len(body.Tables) != 2 || body.Tables[0].TableName != "empty_table" || body.Tables[1].TableName != "patients" || body.Tables[1].Total != 2 {
		t.Fatalf("detail mismatch: %s", detail.Body.String())
	}
	total := int64(0)
	for _, p := range body.Trend.Points {
		total += p.TotalLogs
	}
	if total != 2 {
		t.Fatalf("trend total = %d", total)
	}
	overview := call("/?q=Beta&condition=normal&page=99", "", handler.GetMonitoringOverview)
	var list struct {
		Summary map[string]int     `json:"summary"`
		Clients []monitoringClient `json:"clients"`
		Page    int                `json:"page"`
		Total   int                `json:"total"`
	}
	if err := json.Unmarshal(overview.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if list.Summary["total"] != 3 || list.Total != 1 || list.Page != 1 || len(list.Clients) != 1 || list.Clients[0].ID != "b" {
		t.Fatalf("overview mismatch: %s", overview.Body.String())
	}
	if response := call("/", "missing", handler.GetClientMonitoring); response.Code != 404 {
		t.Fatalf("missing: %d", response.Code)
	}
	if response := call("/?range=90D", "a", handler.GetClientMonitoring); response.Code != 400 {
		t.Fatalf("invalid range: %d", response.Code)
	}
	// More than one page; deleting the cache never affects monitoring counts.
	if err := tx.Exec(`INSERT INTO clients SELECT 'client-'||n, 'Client '||n, 'active', NULL FROM generate_series(1,23) n`).Error; err != nil {
		t.Fatal(err)
	}
	response := call("/?page=2", "", handler.GetMonitoringOverview)
	if err := json.Unmarshal(response.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if list.Page != 2 || list.Total != 26 || len(list.Clients) != 6 {
		t.Fatalf("pagination: %s", response.Body.String())
	}
}
