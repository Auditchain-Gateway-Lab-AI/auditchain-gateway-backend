package client

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"go-blockchain-api/internal/models"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestMonitoringStatisticsAggregation(t *testing.T) {
	now := time.Now().UTC()
	older := now.Add(-time.Hour)
	clients := make([]monitoringClient, 25)
	for i := range clients {
		clients[i] = buildMonitoringClient(models.Client{ID: fmt.Sprint(i), CompanyName: fmt.Sprintf("Client %02d", i)}, monitoringMetrics{}, monitoringIntegration{}, 15, now)
	}
	clients[0] = buildMonitoringClient(models.Client{ID: "a", CompanyName: "Alpha", Status: "inactive"}, monitoringMetrics{Total: 100, Anchored: 90, Valid: 98, Tampered: 2, Logs24H: 3, CheckedAt: &older, LastLogAt: &older}, monitoringIntegration{}, 15, now)
	clients[24] = buildMonitoringClient(models.Client{ID: "z", CompanyName: "Zulu", Status: "pending_setup"}, monitoringMetrics{Total: 10, Pending: 10, NotChecked: 10, Logs24H: 10, OldestPendingAt: &older, LastLogAt: &now}, monitoringIntegration{}, 15, now)
	got := summarizeMonitoringStatistics(clients)
	if len(got.Clients) != 25 || got.Clients[0].ID != "a" || got.Clients[1].ID != "z" || clients[24].ID != "z" {
		t.Fatalf("comparison must include all clients, sort by volume, and preserve input order: %+v", got.Clients)
	}
	if got.Summary.Total != 25 || got.Summary.Critical != 1 || got.Summary.Attention != 1 || got.Summary.Unknown != 23 {
		t.Fatalf("unexpected conditions: %+v", got.Summary)
	}
	if got.Metrics.Total != 110 || got.Metrics.Anchored != 90 || got.Metrics.Pending != 10 || got.Metrics.Tampered != 2 || got.Metrics.Logs24H != 13 {
		t.Fatalf("unexpected metrics: %+v", got.Metrics)
	}
	if got.Metrics.AnchorPercentage == nil || math.Abs(*got.Metrics.AnchorPercentage-90.0/110*100) > 0.00001 {
		t.Fatalf("coverage must be weighted by log count: %+v", got.Metrics.AnchorPercentage)
	}
	if len(got.Attention) != 5 || got.Attention[0].ID != "a" || got.Attention[1].ID != "z" || got.Attention[2].CompanyName != "Client 01" {
		t.Fatalf("incorrect priority ordering: %+v", got.Attention)
	}
	if !got.Metrics.LastLogAt.Equal(now) || !got.Metrics.CheckedAt.Equal(older) || !got.Metrics.OldestPendingAt.Equal(older) {
		t.Fatal("incorrect aggregate observation timestamps")
	}
	empty := summarizeMonitoringStatistics(nil)
	if empty.Metrics.Total != 0 || empty.Metrics.AnchorPercentage != nil || empty.Attention == nil || len(empty.Attention) != 0 || empty.Clients == nil || len(empty.Clients) != 0 {
		t.Fatalf("incorrect empty state: %+v", empty)
	}
}

func TestMonitoringStatisticsValidationAndDatabaseFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		query  string
		status int
	}{{"?range=8H", 400}, {"?range=bad", 400}, {"?range=", 400}, {"", 500}, {"?range=24H", 500}, {"?range=7D", 500}, {"?range=30D", 500}} {
		t.Run(test.query, func(t *testing.T) {
			res := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(res)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/monitoring/statistics"+test.query, nil)
			(&Handler{}).GetMonitoringStatistics(ctx)
			if res.Code != test.status {
				t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["metrics"] != nil {
				t.Fatal("database failure fabricated metrics")
			}
		})
	}
}

func TestMonitoringStatisticsAdminRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("JWT_SECRET", "statistics-test-secret")
	router := gin.New()
	RegisterRoutes(router.Group("/api"), &Handler{})
	for _, test := range []struct {
		role   string
		status int
	}{{"", 401}, {"auditor", 403}, {"admin", 500}} {
		req := httptest.NewRequest(http.MethodGet, "/api/admin/monitoring/statistics", nil)
		if test.role != "" {
			token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"role": test.role, "exp": time.Now().Add(time.Hour).Unix()})
			signed, err := token.SignedString([]byte("statistics-test-secret"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+signed)
		}
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		if res.Code != test.status {
			t.Fatalf("role=%s status=%d body=%s", test.role, res.Code, res.Body.String())
		}
	}
}

// Uses session-local TEMP tables and rolls back all fixtures, including on failure.
func TestMonitoringStatisticsPostgres(t *testing.T) {
	dsn := os.Getenv("MONITORING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MONITORING_TEST_DATABASE_URL to run isolated PostgreSQL coverage")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`CREATE TEMP TABLE clients (id text PRIMARY KEY, company_name text, status text, deleted_at timestamptz)`,
		`CREATE TEMP TABLE agent_configs (id text, client_id text, connector_status text, is_active bool, updated_at timestamptz, deleted_at timestamptz)`,
		`CREATE TEMP TABLE client_kafka_configs (id text, client_id text, is_active bool, deleted_at timestamptz)`,
		`CREATE TEMP TABLE audit_logs (client_id text, status text, blockchain_tx_id text, integrity_status text, source_integrity_status text, integrity_checked_at timestamptz, source_integrity_checked_at timestamptz, timestamp timestamptz, db_timestamp timestamptz, action text)`,
		`INSERT INTO clients SELECT 'client-' || n, 'Client ' || n, CASE WHEN n=1 THEN 'inactive' WHEN n=25 THEN 'pending_setup' ELSE 'active' END, NULL FROM generate_series(1,25) n`,
		`INSERT INTO clients VALUES ('deleted', 'Deleted', 'active', now())`,
		`INSERT INTO audit_logs SELECT 'client-1', 'ANCHORED', 'tx', 'VALID', '', now(), NULL, now()-interval '1 hour', now()-interval '1 hour', 'INSERT' FROM generate_series(1,90)`,
		`INSERT INTO audit_logs SELECT 'client-25', 'RECEIVED', '', 'TAMPERED', '', now(), NULL, now()-interval '1 hour', now()-interval '1 hour', 'UPDATE' FROM generate_series(1,10)`,
		`INSERT INTO audit_logs VALUES ('client-1','ANCHORED','tx','VALID','',now(),NULL,now()-interval '1 hour',now(),'RECOVERY'), ('orphan','ANCHORED','tx','VALID','',now(),NULL,now()-interval '1 hour',now(),'INSERT'), ('deleted','ANCHORED','tx','VALID','',now(),NULL,now()-interval '1 hour',now(),'INSERT')`,
	} {
		if err := tx.Exec(statement).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, rangeName := range []string{"24H", "7D", "30D"} {
		res := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(res)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/monitoring/statistics?range="+rangeName, nil)
		(&Handler{DB: tx}).GetMonitoringStatistics(ctx)
		if res.Code != 200 {
			t.Fatalf("range=%s status=%d body=%s", rangeName, res.Code, res.Body.String())
		}
		var got monitoringStatisticsResponse
		if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Summary.Total != 25 || got.Summary.Critical != 1 || got.Summary.Normal != 1 || got.Summary.Unknown != 23 || got.Metrics.Total != 100 || got.Metrics.Anchored != 90 || got.Metrics.Pending != 10 || got.Metrics.Tampered != 10 || got.Metrics.Logs24H != 100 || *got.Metrics.AnchorPercentage != 90 {
			t.Fatalf("incorrect database aggregate: %+v %+v", got.Summary, got.Metrics)
		}
		if len(got.Clients) != 25 || got.Clients[0].ID != "client-1" || got.Clients[0].Metrics.Total != 90 || got.Clients[0].AdministrativeStatus != "inactive" || got.Clients[1].ID != "client-25" || got.Clients[1].Metrics.Tampered != 10 || got.Clients[1].AdministrativeStatus != "pending_setup" || got.Clients[2].Metrics.Total != 0 {
			t.Fatalf("comparison lost per-client identities, metrics or empty clients: %+v", got.Clients)
		}
		window, _ := parseDashboardTrendRange(rangeName)
		var total int64
		for _, point := range got.Trend.Points {
			total += point.TotalLogs
		}
		if len(got.Trend.Points) != window.bucketQty || total != 100 || got.Trend.Points[0].TotalLogs != 0 {
			t.Fatalf("incorrect trend: %+v", got.Trend)
		}
		single, err := loadDashboardTrend(tx, "client-1", window, got.FetchedAt)
		if err != nil {
			t.Fatal(err)
		}
		var singleTotal int64
		for _, point := range single.Points {
			singleTotal += point.TotalLogs
		}
		if singleTotal != 90 {
			t.Fatalf("tenant trend leaked another client's logs: %d", singleTotal)
		}
	}
	if err := tx.Exec(`DELETE FROM clients`).Error; err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(res)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/monitoring/statistics", nil)
	(&Handler{DB: tx}).GetMonitoringStatistics(ctx)
	var empty monitoringStatisticsResponse
	if err := json.Unmarshal(res.Body.Bytes(), &empty); err != nil {
		t.Fatal(err)
	}
	if res.Code != 200 || empty.Summary.Total != 0 || empty.Metrics.Total != 0 || empty.Metrics.AnchorPercentage != nil || len(empty.Attention) != 0 || len(empty.Clients) != 0 || len(empty.Trend.Points) != 7 {
		t.Fatalf("incorrect empty response: %s", res.Body.String())
	}
	if err := tx.Exec(`DROP TABLE audit_logs`).Error; err != nil {
		t.Fatal(err)
	}
	if err := tx.Exec(`INSERT INTO clients VALUES ('client-error','Broken','active',NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	failed := httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(failed)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/monitoring/statistics", nil)
	(&Handler{DB: tx}).GetMonitoringStatistics(ctx)
	if failed.Code != 500 {
		t.Fatalf("query failure must return 500: %s", failed.Body.String())
	}
}
