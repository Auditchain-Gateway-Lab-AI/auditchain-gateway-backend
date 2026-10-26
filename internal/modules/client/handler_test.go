package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestPingAgentConfigUsesHealthEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("request path = %q, want /health", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &http.Client{}
	statusCode, reachable, err := pingAgentHealth(client, server.URL)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	if statusCode != http.StatusOK || !reachable {
		t.Fatalf("health result = status %d, reachable %t; want 200/true", statusCode, reachable)
	}
}

func TestPingAgentConfigTreatsNon2xxAsUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Fatalf("request path = %q, want /health", r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := &http.Client{}
	statusCode, reachable, err := pingAgentHealth(client, server.URL)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	if statusCode != http.StatusNotFound || reachable {
		t.Fatalf("health result = status %d, reachable %t; want 404/false", statusCode, reachable)
	}
}

func TestGetClientStatsRejectsUnsupportedTrendRange(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/dashboard/stats?trend_range=90D", nil)
	ctx.Set("client_id", "tenant-1")

	(&Handler{}).GetClientStats(ctx)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error"] != "trend_range harus 8H, 24H, 7D, atau 30D" {
		t.Fatalf("error = %q, want unsupported trend range message", body["error"])
	}
}

func TestGetIntegrityHistoryRejectsUnsupportedRangeAndDimension(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		url  string
	}{
		{name: "range", url: "/dashboard/integrity-history?range=90D&dimension=proof"},
		{name: "dimension", url: "/dashboard/integrity-history?range=8H&dimension=all"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, test.url, nil)
			ctx.Set("client_id", "tenant-1")

			(&Handler{}).GetIntegrityHistory(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestIntegrityHistoryRequiresAuthenticatedTenant(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/dashboard/integrity-history?range=8H", nil)

	(&Handler{}).GetIntegrityHistory(ctx)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
	}
}

func TestIntegrityMonitoringRoutesAreRegisteredAndProtected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	RegisterRoutes(router.Group("/api"), &Handler{})
	for _, path := range []string{
		"/api/dashboard/integrity-history?range=8H&dimension=proof",
		"/api/dashboard/worker-status",
		"/api/dashboard/stats/external",
		"/api/admin/stats/internal",
		"/api/admin/stats/external",
		"/api/admin/clients/tenant-a/stats/internal",
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("%s: got %d, want 401 for registered protected route", path, response.Code)
		}
	}
}

func TestGetAdminExternalAuditOverview_NilDB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/stats/external", nil)

	(&Handler{DB: nil}).GetAdminExternalAuditOverview(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error"] != "Database tidak tersedia" {
		t.Fatalf("error = %q, want 'Database tidak tersedia'", body["error"])
	}
}

func TestGetAdminInternalAuditOverview_NilDB(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/admin/stats/internal", nil)

	(&Handler{DB: nil}).GetAdminInternalAuditOverview(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error"] != "Database tidak tersedia" {
		t.Fatalf("error = %q, want 'Database tidak tersedia'", body["error"])
	}
}
