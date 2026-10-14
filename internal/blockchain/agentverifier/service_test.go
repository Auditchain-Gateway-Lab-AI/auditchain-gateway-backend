package agentverifier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestCompareCurrentResourceStateUsesCanonicalEventProjection(t *testing.T) {
	log := &models.AuditLog{
		ClientID: "client-1",
		Resource: "AGAMA:923",
		Action:   "UPDATE",
		Metadata: `{"ID":923,"NAMA":"Indosian","JUMLAH":1.0}`,
	}
	record := &ResourceRecord{
		Found: true,
		Table: "agama",
		ID:    "923",
		Data: map[string]interface{}{
			"id":     json.Number("923"),
			"nama":   "Indosian",
			"jumlah": json.Number("1e0"),
			"extra":  "ignored because it is outside this event projection",
		},
	}

	result := compareCurrentResourceState(log, record)
	if !result.Comparable || !result.Match {
		t.Fatalf("comparison = %+v, want comparable match", result)
	}
	if result.ClientHash == "" || result.ClientHash != result.OffchainHash {
		t.Fatalf("state hashes = (%q, %q), want same non-empty hash", result.ClientHash, result.OffchainHash)
	}
	if result.Basis != "latest_event_field_projection:v2" {
		t.Fatalf("comparison basis = %q", result.Basis)
	}
}

func TestCompareCurrentResourceStateNormalizesAgentStringIdentity(t *testing.T) {
	log := &models.AuditLog{
		ClientID: "client-1",
		Resource: "AGAMA:923",
		Action:   "UPDATE",
		Metadata: `{"ID":923,"NAMA":"Indosian"}`,
	}
	record := &ResourceRecord{
		Found: true,
		Table: "AGAMA",
		ID:    "923",
		Data: map[string]interface{}{
			"ID":   "923", // Oracle Agent returns the primary key as a string.
			"NAMA": "Indosian",
		},
	}

	result := compareCurrentResourceState(log, record)
	if !result.Comparable || !result.Match {
		t.Fatalf("comparison = %+v, want comparable match for numeric/string ID encodings", result)
	}
	if len(result.Discrepancies) != 0 {
		t.Fatalf("discrepancies = %+v, want none", result.Discrepancies)
	}
	if result.ClientHash == "" || result.ClientHash != result.OffchainHash {
		t.Fatalf("state hashes = (%q, %q), want same non-empty hash", result.ClientHash, result.OffchainHash)
	}
	if result.Basis != "latest_event_field_projection:v2" {
		t.Fatalf("comparison basis = %q", result.Basis)
	}
}

func TestCompareCurrentResourceStateDoesNotNormalizeWrongIdentity(t *testing.T) {
	log := &models.AuditLog{
		ClientID: "client-1",
		Resource: "AGAMA:923",
		Action:   "UPDATE",
		Metadata: `{"ID":923,"NAMA":"Indosian"}`,
	}
	record := &ResourceRecord{
		Found: true,
		Table: "AGAMA",
		ID:    "923",
		Data: map[string]interface{}{
			"ID":   "924",
			"NAMA": "Indosian",
		},
	}

	result := compareCurrentResourceState(log, record)
	if !result.Comparable || result.Match {
		t.Fatalf("comparison = %+v, want comparable mismatch for a different identity", result)
	}
	foundIdentityDiscrepancy := false
	for _, discrepancy := range result.Discrepancies {
		if discrepancy.Field == "id" {
			foundIdentityDiscrepancy = true
			break
		}
	}
	if !foundIdentityDiscrepancy {
		t.Fatalf("discrepancies = %+v, want an id discrepancy", result.Discrepancies)
	}
}

func TestCompareCurrentResourceStateDistinguishesMismatchAndNotComparable(t *testing.T) {
	log := &models.AuditLog{
		ClientID: "client-1", Resource: "AGAMA:923", Action: "UPDATE",
		Metadata: `{"data_baru":{"ID":923,"NAMA":"Indosian"}}`,
	}
	t.Run("different value is a comparable mismatch", func(t *testing.T) {
		result := compareCurrentResourceState(log, &ResourceRecord{
			Found: true,
			Data:  map[string]interface{}{"id": json.Number("923"), "nama": "Indonesia"},
		})
		if !result.Comparable || result.Match || result.ClientHash == result.OffchainHash {
			t.Fatalf("comparison = %+v, want comparable mismatch with distinct hashes", result)
		}
	})
	t.Run("missing projected field is not comparable", func(t *testing.T) {
		result := compareCurrentResourceState(log, &ResourceRecord{
			Found: true,
			Data:  map[string]interface{}{"id": json.Number("923")},
		})
		if result.Comparable || result.Match {
			t.Fatalf("comparison = %+v, want not comparable", result)
		}
	})
}

func TestCompareCurrentResourceStateDeleteSemantics(t *testing.T) {
	log := &models.AuditLog{ClientID: "client-1", Resource: "AGAMA:923", Action: "DELETE"}
	missing := compareCurrentResourceState(log, &ResourceRecord{Found: false})
	if !missing.Comparable || !missing.Match || missing.ClientHash != missing.OffchainHash {
		t.Fatalf("missing row comparison = %+v, want matching deleted state", missing)
	}
	if missing.Basis != "latest_event_row_absence:v1" {
		t.Fatalf("delete comparison basis = %q", missing.Basis)
	}
	present := compareCurrentResourceState(log, &ResourceRecord{
		Found: true,
		Data:  map[string]interface{}{"id": json.Number("923")},
	})
	if !present.Comparable || present.Match || present.ClientHash == present.OffchainHash {
		t.Fatalf("present row comparison = %+v, want mismatch", present)
	}
}

func TestFetchResourceFromAgentPreservesLargeNumbers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"found":true,"table":"T","id":"1","data":{"ID":9007199254740993}}`))
	}))
	defer server.Close()

	record, err := (&Service{}).fetchResourceFromAgentContext(context.Background(), &models.AgentConfig{
		AgentURL: server.URL, TimeoutSeconds: 2,
	}, "T", "1")
	if err != nil {
		t.Fatalf("fetchResourceFromAgentContext() error = %v", err)
	}
	if got, ok := record.Data["ID"].(json.Number); !ok || got.String() != "9007199254740993" {
		t.Fatalf("ID = %#v, want exact json.Number", record.Data["ID"])
	}
}

func TestVerifyViaResourceReturnsAgentCheckedAt(t *testing.T) {
	checkedAt := time.Date(2026, 10, 14, 3, 4, 5, 123000000, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/verify/AGAMA/923" {
			t.Errorf("request path = %q, want %q", r.URL.Path, "/verify/AGAMA/923")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"found":true,"table":"AGAMA","id":"923","data":{"ID":923,"NAMA":"Indosian"},"checked_at":%q}`, checkedAt.Format(time.RFC3339Nano))
	}))
	defer server.Close()

	result, err := (&Service{}).verifyViaResource(&models.AgentConfig{
		AgentURL: server.URL, TimeoutSeconds: 2,
	}, &models.AuditLog{
		ClientID: "client-1",
		Resource: "AGAMA:923",
		Action:   "UPDATE",
		Metadata: `{"ID":923,"NAMA":"Indosian"}`,
	})
	if err != nil {
		t.Fatalf("verifyViaResource() error = %v", err)
	}
	if result.SourceCheckedAt == nil || !result.SourceCheckedAt.Equal(checkedAt) {
		t.Fatalf("SourceCheckedAt = %v, want %v", result.SourceCheckedAt, checkedAt)
	}
}

func TestCompareAuditTrailNormalizesResourceTableAndCanonicalMetadata(t *testing.T) {
	actor := "morbis1"
	log := &models.AuditLog{
		Resource: "RUANGAN:81",
		Action:   "update",
		Actor:    actor,
		Metadata: `{"data_lama":{"NAMA":"Ruang Lama"},"data_baru":{"NAMA":"Ruang Baru","JUMLAH":1.0}}`,
	}
	event := &AuditTrailRecord{
		Found:    true,
		Tabel:    "ruangan",
		Operasi:  "UPDATE",
		AppUser:  &actor,
		DataLama: map[string]interface{}{"nama": "Ruang Lama"},
		DataBaru: map[string]interface{}{"nama": "Ruang Baru", "jumlah": json.Number("1")},
	}

	diffs := (&Service{}).compareFields(log, event)
	if len(diffs) != 0 {
		t.Fatalf("compareFields() = %+v, want equivalent source event", diffs)
	}
}
