package agentverifier

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-blockchain-api/internal/models"
)

func TestFetchResourceFromAgentReturnsTypedHTTPStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/verify/AGAMA/923" {
			t.Errorf("request path = %q, want %q", r.URL.Path, "/verify/AGAMA/923")
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-verify-token" {
			t.Errorf("Authorization = %q, want bearer verify token", got)
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	service := &Service{}
	config := &models.AgentConfig{
		AgentURL:       server.URL,
		VerifyToken:    "test-verify-token",
		TimeoutSeconds: 2,
	}
	_, err := service.fetchResourceFromAgentContext(context.Background(), config, "AGAMA", "923")
	if err == nil {
		t.Fatal("expected a forbidden response error")
	}

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error type = %T (%v), want *HTTPStatusError", err, err)
	}
	if statusErr.StatusCode != http.StatusForbidden {
		t.Fatalf("status code = %d, want %d", statusErr.StatusCode, http.StatusForbidden)
	}
}
