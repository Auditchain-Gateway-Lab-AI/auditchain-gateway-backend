package recovery

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/models"
)

func TestDirectDesiredStateDeleteIsEmptyRegardlessOfHistoricalMetadata(t *testing.T) {
	logRow := &models.AuditLog{Metadata: `{"id":7,"nama":"old"}`}
	state, raw, err := directDesiredState(logRow, models.RecoveryOperationDelete)
	if err != nil {
		t.Fatalf("directDesiredState() error = %v", err)
	}
	if len(state) != 0 || string(raw) != `{}` {
		t.Fatalf("delete desired state = %#v/%s, want empty object", state, raw)
	}
}

func TestDirectDesiredStateRejectsRedactionMarkers(t *testing.T) {
	for _, metadata := range []string{`{"ID":7,"NAMA":"[REDACTED]"}`, `{"ID":7,"NAMA":"***REDACTED***"}`, `{"ID":7,"NAMA":{"value":"[REDACTED]"}}`} {
		_, _, err := directDesiredState(&models.AuditLog{Metadata: metadata}, models.RecoveryOperationUpsert)
		if err == nil || err.Error() != "reference_payload_redacted" {
			t.Fatalf("metadata %s: error = %v", metadata, err)
		}
	}
}

func TestValidateDirectCDCResultRequiresMatchingVerifiedEvidence(t *testing.T) {
	row := trustedReferenceLog()
	row.LogID = "cdc-result"
	row.HashValue = cryptoHashLog(&row)
	row.MerkleRoot = row.HashValue
	state := map[string]interface{}{"ID": float64(81), "NAMA": "ruang"}
	hash, err := agentverifier.HashResourceState(row.ClientID, row.Resource, state)
	if err != nil {
		t.Fatal(err)
	}
	event := &models.RecoveryEvent{TargetLogID: "original", ResultAuditLogID: row.LogID, ClientID: row.ClientID, Resource: row.Resource, Operation: models.RecoveryOperationUpsert, ClientAfterHash: hash}
	fabric := trustedReferenceFabric{root: row.HashValue}
	if err := validateDirectCDCResult(event, &row, nil, fabric); err != nil {
		t.Fatalf("valid CDC result rejected: %v", err)
	}
	tampered := row
	tampered.Metadata = `{"ID":81,"NAMA":"changed"}`
	if err := validateDirectCDCResult(event, &tampered, nil, fabric); err == nil {
		t.Fatal("tampered CDC payload accepted")
	}
	wrong := *event
	wrong.ResultAuditLogID = "another"
	if err := validateDirectCDCResult(&wrong, &row, nil, fabric); err == nil {
		t.Fatal("wrong CDC log accepted")
	}
	if err := validateDirectCDCResult(event, &row, nil, trustedReferenceFabric{root: "wrong"}); err == nil {
		t.Fatal("wrong Fabric anchor accepted")
	}
}

func TestDirectChangedStateFieldsNormalizesKeysAndNumbers(t *testing.T) {
	client := &agentverifier.ResourceRecord{Found: true, Data: map[string]interface{}{"ID": int64(7), "NAMA": "Before"}}
	desired := map[string]interface{}{"id": json.Number("7.0"), "nama": "After"}
	got := directChangedStateFields(client, desired)
	if len(got) != 1 || got[0] != "nama" {
		t.Fatalf("changed fields = %#v, want [nama]", got)
	}
}

func TestClientRecoveryScopeKeepsGatewayIntegrityReadOnly(t *testing.T) {
	if err := validateClientRecoveryScope(models.RecoveryScopeClientSource); err != nil {
		t.Fatalf("client source scope rejected: %v", err)
	}
	if err := validateClientRecoveryScope(" client_source "); err != nil {
		t.Fatalf("normalized client source scope rejected: %v", err)
	}
	if err := validateClientRecoveryScope(models.RecoveryScopeGatewayIntegrity); err == nil || err.Error() != "gateway_integrity_read_only" {
		t.Fatalf("gateway integrity scope error = %v, want gateway_integrity_read_only", err)
	}
	if err := validateClientRecoveryScope("UNKNOWN_SCOPE"); err == nil || err.Error() != "client_source_incident_required" {
		t.Fatalf("unknown scope error = %v, want client_source_incident_required", err)
	}
}

func TestDirectCDCMatchesCanonicalizesMetadata(t *testing.T) {
	request := &models.RecoveryRequest{
		ClientID: "client-a", Resource: "RUANGAN:7", Operation: models.RecoveryOperationUpsert,
		DesiredStateHash: "",
	}
	state := map[string]interface{}{"id": 7, "nama": "A"}
	hash, err := agentverifier.HashResourceState(request.ClientID, request.Resource, state)
	if err != nil {
		t.Fatalf("HashResourceState() error = %v", err)
	}
	request.DesiredStateHash = hash
	logRow := &models.AuditLog{Action: "UPDATE", Metadata: `{"NAMA":"A","ID":7}`}
	if !directCDCMatches(request, logRow) {
		t.Fatal("CDC metadata should match canonical desired state")
	}
}

func TestSelectDirectCDCMatchRequiresOneUnambiguousEvent(t *testing.T) {
	request := &models.RecoveryRequest{
		ClientID: "client-a", Resource: "RUANGAN:7", Operation: models.RecoveryOperationUpsert,
	}
	hash, err := agentverifier.HashResourceState(request.ClientID, request.Resource, map[string]interface{}{"id": 7, "nama": "A"})
	if err != nil {
		t.Fatalf("HashResourceState() error = %v", err)
	}
	request.DesiredStateHash = hash
	matching := []models.AuditLog{
		{LogID: "event-1", Action: "UPDATE", Metadata: `{"NAMA":"A","ID":7}`},
		{LogID: "event-2", Action: "UPDATE", Metadata: `{"ID":7,"NAMA":"A"}`},
	}
	if candidate, ambiguous := selectDirectCDCMatch(request, matching[:1]); ambiguous || candidate == nil || candidate.LogID != "event-1" {
		t.Fatalf("single match = (%v, %v), want event-1 and unambiguous", candidate, ambiguous)
	}
	if candidate, ambiguous := selectDirectCDCMatch(request, matching); !ambiguous || candidate != nil {
		t.Fatalf("duplicate matches = (%v, %v), want ambiguous with no selected event", candidate, ambiguous)
	}

	request.Operation = models.RecoveryOperationDelete
	request.DesiredStateHash = ""
	deletes := []models.AuditLog{{LogID: "delete-1", Action: "DELETE"}, {LogID: "delete-2", Action: "DELETE"}}
	if candidate, ambiguous := selectDirectCDCMatch(request, deletes); !ambiguous || candidate != nil {
		t.Fatalf("duplicate delete events = (%v, %v), want ambiguous with no selected event", candidate, ambiguous)
	}
}

func TestDirectCDCWindowStartsAtExecutionStartWithLegacyFallback(t *testing.T) {
	startedAt := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
	appliedAt := startedAt.Add(3 * time.Second)
	request := &models.RecoveryRequest{ExecutionStartedAt: &startedAt, AgentAppliedAt: &appliedAt}
	if got := directCDCWindowStart(request); got == nil || !got.Equal(startedAt) {
		t.Fatalf("CDC scan start = %v, want execution start %v", got, startedAt)
	}
	request.ExecutionStartedAt = nil
	if got := directCDCWindowStart(request); got == nil || !got.Equal(appliedAt) {
		t.Fatalf("legacy CDC scan start = %v, want applied time %v", got, appliedAt)
	}
}

func TestDirectFailureStatusesSeparateVerificationAndExecution(t *testing.T) {
	requestStatus, eventStatus := directFailureStatuses("source_state_changed")
	if requestStatus != models.RecoveryStatusFailedVerification || eventStatus != models.RecoveryResultVerification {
		t.Fatalf("source_state_changed statuses = %q/%q, want verification", requestStatus, eventStatus)
	}
	requestStatus, eventStatus = directFailureStatuses("readback_mismatch")
	if requestStatus != models.RecoveryStatusFailedExecution || eventStatus != models.RecoveryResultExecution {
		t.Fatalf("readback_mismatch statuses = %q/%q, want execution", requestStatus, eventStatus)
	}
}

func TestRecoveryEvidenceRedactsSensitiveMetadata(t *testing.T) {
	got := redactedRecoveryMetadata([]byte(`{"id":7,"password":"secret","nested":{"access_token":"bearer"}}`))
	want := `{"id":7,"nested":{"access_token":"***REDACTED***"},"password":"***REDACTED***"}`
	if got != want {
		t.Fatalf("redacted recovery metadata = %s, want %s", got, want)
	}
}

func TestRedactedMetadataPreviewUnwrapsJSONBStringAndMasksSecrets(t *testing.T) {
	value, err := redactedMetadataPreview(`"{\"ID\":1019,\"NAMA\":\"tampered\",\"password\":\"secret\"}"`)
	if err != nil {
		t.Fatalf("redactedMetadataPreview() error = %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	want := `{"ID":1019,"NAMA":"tampered","password":"***REDACTED***"}`
	if string(encoded) != want {
		t.Fatalf("redacted metadata preview = %s, want %s", encoded, want)
	}
}

func TestRecoveryRequestDoesNotSerializeDesiredState(t *testing.T) {
	encoded, err := json.Marshal(models.RecoveryRequest{ID: "request-1", DesiredState: `{"password":"secret"}`})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "desired_state") {
		t.Fatalf("request JSON leaked desired state: %s", encoded)
	}
}
