package recovery

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func recoveryTestContext(path string) *gin.Context {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, path, nil)
	return ctx
}

func TestClientIDAdminCanOverrideTenant(t *testing.T) {
	h := &Handler{}
	ctx := recoveryTestContext("/api/dashboard/recovery/incidents?client_id=morbis-client")
	ctx.Set("client_id", "admin-default-client")
	ctx.Set("role", "admin")

	clientID, ok := h.clientID(ctx)
	if !ok {
		t.Fatal("expected admin tenant override to be accepted")
	}
	if clientID != "morbis-client" {
		t.Fatalf("client_id = %q, want %q", clientID, "morbis-client")
	}
}

func TestClientIDUserCannotOverrideTenant(t *testing.T) {
	h := &Handler{}
	ctx := recoveryTestContext("/api/dashboard/recovery/incidents?client_id=other-client")
	ctx.Set("client_id", "user-client")
	ctx.Set("role", "Auditor")

	clientID, ok := h.clientID(ctx)
	if !ok {
		t.Fatal("expected user token client_id to be accepted")
	}
	if clientID != "user-client" {
		t.Fatalf("client_id = %q, want token client_id %q", clientID, "user-client")
	}
}

func TestClientIDRejectsMissingIdentity(t *testing.T) {
	h := &Handler{}
	ctx := recoveryTestContext("/api/dashboard/recovery/incidents")

	if _, ok := h.clientID(ctx); ok {
		t.Fatal("expected missing client identity to be rejected")
	}
	if ctx.Writer.Status() != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", ctx.Writer.Status(), http.StatusUnauthorized)
	}
}
