package audit

import (
	"errors"
	"fmt"
	"testing"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/models"
)

type verifyLogRepositoryStub struct {
	AuditRepository
	log models.AuditLog
}

func (r *verifyLogRepositoryStub) GetLogByID(logID, clientID string) (*models.AuditLog, error) {
	if logID != r.log.LogID || clientID != r.log.ClientID {
		return nil, errors.New("unexpected tenant-scoped log lookup")
	}
	logCopy := r.log
	return &logCopy, nil
}

func (r *verifyLogRepositoryStub) GetLogsByResource(string, string) ([]models.AuditLog, error) {
	return []models.AuditLog{r.log}, nil
}

func TestManualVerifyReportsTamperIncidentPersistenceFailure(t *testing.T) {
	repo := &verifyLogRepositoryStub{log: models.AuditLog{
		LogID:     "1791354247037786430",
		ClientID:  "client-1",
		Resource:  "RUANGAN:620",
		Action:    "UPDATE",
		Metadata:  `{"id":620,"nama":"tampered"}`,
		HashValue: "trusted-hash",
	}}
	service := &auditService{repo: repo}

	result, err := service.VerifyLogIntegrity(repo.log.LogID, repo.log.ClientID)
	if !errors.Is(err, errIncidentPersistence) {
		t.Fatalf("VerifyLogIntegrity error = %v, want tamper incident persistence error", err)
	}
	if result == nil || result.Status != "failed_local" {
		t.Fatalf("VerifyLogIntegrity result = %+v, want local tamper result", result)
	}
	if result.IncidentType != "METADATA_HASH_MISMATCH" {
		t.Fatalf("incident type = %q, want METADATA_HASH_MISMATCH", result.IncidentType)
	}
}

func TestResourceVerifyReportsTamperIncidentPersistenceFailure(t *testing.T) {
	repo := &verifyLogRepositoryStub{log: models.AuditLog{
		LogID:     "1791354247037786430",
		ClientID:  "client-1",
		Resource:  "RUANGAN:620",
		Action:    "RECOVERY",
		Metadata:  `{"id":620,"nama":"tampered"}`,
		HashValue: "trusted-hash",
	}}
	service := &auditService{repo: repo}

	_, err := service.VerifyResourceHistory(repo.log.Resource, repo.log.ClientID)
	if !errors.Is(err, errIncidentPersistence) {
		t.Fatalf("VerifyResourceHistory error = %v, want incident persistence error", err)
	}
}

func TestResourceGatewayStatusesDoNotUseAgentReachability(t *testing.T) {
	tests := []struct {
		name          string
		baseStatus    string
		agentStatus   string
		wantIntegrity string
		wantChain     string
	}{
		{
			name:          "valid gateway remains valid when agent unreachable",
			baseStatus:    "valid",
			agentStatus:   "unreachable",
			wantIntegrity: "valid",
			wantChain:     "valid",
		},
		{
			name:          "valid gateway remains valid when agent mismatches",
			baseStatus:    "valid",
			agentStatus:   "mismatch",
			wantIntegrity: "valid",
			wantChain:     "valid",
		},
		{
			name:          "tampered gateway remains tampered",
			baseStatus:    "tampered",
			agentStatus:   "unreachable",
			wantIntegrity: "tampered",
			wantChain:     "tampered",
		},
		{
			name:          "fabric unreachable remains unreachable",
			baseStatus:    "unreachable",
			agentStatus:   "matched",
			wantIntegrity: "unreachable",
			wantChain:     "unreachable",
		},
		{
			name:          "unanchored gateway remains pending",
			baseStatus:    "pending",
			agentStatus:   "unreachable",
			wantIntegrity: "pending",
			wantChain:     "pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIntegrity, gotChain := resourceGatewayStatuses(tt.baseStatus, tt.agentStatus)
			if gotIntegrity != tt.wantIntegrity || gotChain != tt.wantChain {
				t.Fatalf("resourceGatewayStatuses(%q) = (%q, %q), want (%q, %q)",
					tt.baseStatus, gotIntegrity, gotChain, tt.wantIntegrity, tt.wantChain)
			}
		})
	}
}

func TestAgentFailureStatusesDistinguishHTTPDenialFromUnreachable(t *testing.T) {
	tests := []struct {
		name             string
		err              error
		wantAgentStatus  string
		wantSourceStatus string
	}{
		{
			name:             "forbidden means reached but access denied",
			err:              fmt.Errorf("wrapped Agent error: %w", &agentverifier.HTTPStatusError{StatusCode: 403}),
			wantAgentStatus:  "forbidden",
			wantSourceStatus: models.SourceStatusNotComparable,
		},
		{
			name:             "unauthorized means verification token rejected",
			err:              &agentverifier.HTTPStatusError{StatusCode: 401},
			wantAgentStatus:  "unauthorized",
			wantSourceStatus: models.SourceStatusNotComparable,
		},
		{
			name:             "other HTTP failure means Agent responded",
			err:              &agentverifier.HTTPStatusError{StatusCode: 500},
			wantAgentStatus:  "http_error",
			wantSourceStatus: models.SourceStatusNotComparable,
		},
		{
			name:             "transport failure means unreachable",
			err:              errors.New("dial tcp: i/o timeout"),
			wantAgentStatus:  "unreachable",
			wantSourceStatus: models.SourceStatusUnreachable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAgentStatus, gotSourceStatus := agentFailureStatuses(tt.err)
			if gotAgentStatus != tt.wantAgentStatus || gotSourceStatus != tt.wantSourceStatus {
				t.Fatalf("agentFailureStatuses() = (%q, %q), want (%q, %q)",
					gotAgentStatus, gotSourceStatus, tt.wantAgentStatus, tt.wantSourceStatus)
			}
		})
	}
}

func TestVerificationAssessmentSeparatesProofAndSourceResults(t *testing.T) {
	tests := []struct {
		name             string
		proofStatus      string
		sourceStatus     string
		agentStatus      string
		auditTrailStatus string
		want             string
	}{
		{name: "complete proof and live state match", proofStatus: "VERIFIED", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", want: "VERIFIED"},
		{name: "anchored event differs from live client row", proofStatus: "VERIFIED", sourceStatus: models.SourceStatusMismatch, agentStatus: "mismatch", want: "CLIENT_STATE_DIVERGENCE"},
		{name: "agent offline does not invalidate Fabric proof", proofStatus: "VERIFIED", sourceStatus: models.SourceStatusUnreachable, agentStatus: "unreachable", want: "INCONCLUSIVE_AGENT_UNREACHABLE"},
		{name: "state fields are not comparable", proofStatus: "VERIFIED", sourceStatus: models.SourceStatusNotComparable, agentStatus: "not_comparable", want: "INCONCLUSIVE_SOURCE_NOT_COMPARABLE"},
		{name: "historical event skips live lookup", proofStatus: "VERIFIED", agentStatus: "skipped_historical", want: "GATEWAY_PROOF_VERIFIED_SOURCE_SKIPPED"},
		{name: "bad local proof is not attributed to source", proofStatus: "FAILED_MERKLE_PROOF", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", want: "OFFCHAIN_INTEGRITY_FAILED"},
		{name: "legacy proof cannot establish client divergence", proofStatus: "LEGACY_LIMITED", sourceStatus: models.SourceStatusMismatch, agentStatus: "mismatch", want: "INCONCLUSIVE_LEGACY_PROOF_WITH_SOURCE_DIVERGENCE"},
		{name: "legacy proof remains explicitly limited when source is skipped", proofStatus: "LEGACY_LIMITED", agentStatus: "skipped_historical", want: "INCONCLUSIVE_LEGACY_PROOF"},
		{name: "legacy proof is not full verification even when projected source matches", proofStatus: "LEGACY_LIMITED", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", want: "VERIFIED_LEGACY_LIMITED_PROOF"},
		{name: "missing Fabric proof is inconclusive", proofStatus: "UNAVAILABLE_MERKLE_PROOF", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", want: "INCONCLUSIVE_MERKLE_PROOF_MISSING"},
		{name: "proof batch root mismatch is offchain failure", proofStatus: "FAILED_PROOF_BATCH_ROOT", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", want: "OFFCHAIN_INTEGRITY_FAILED"},
		{name: "Fabric outage in history is inconclusive", proofStatus: "UNAVAILABLE_FABRIC", agentStatus: "unreachable", want: "INCONCLUSIVE_FABRIC_UNAVAILABLE"},
		{name: "source audit event separately differs", proofStatus: "VERIFIED", sourceStatus: models.SourceStatusMatched, agentStatus: "matched", auditTrailStatus: "mismatch", want: "SOURCE_EVENT_DIVERGENCE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := verificationAssessment(tt.proofStatus, tt.sourceStatus, tt.agentStatus, tt.auditTrailStatus)
			if got != tt.want {
				t.Fatalf("verificationAssessment() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestShouldRecordClientSourceIncidentRequiresFullyVerifiedProof(t *testing.T) {
	tests := []struct {
		name        string
		proofStatus string
		source      string
		want        bool
	}{
		{name: "complete proof and divergence", proofStatus: "VERIFIED", source: models.SourceStatusMismatch, want: true},
		{name: "legacy proof does not attribute divergence", proofStatus: "LEGACY_LIMITED", source: models.SourceStatusMismatch, want: false},
		{name: "failed proof does not create a client incident", proofStatus: "FAILED_MERKLE_PROOF", source: models.SourceStatusMismatch, want: false},
		{name: "pending proof does not create a client incident", proofStatus: "PENDING", source: models.SourceStatusMissing, want: false},
		{name: "unavailable Fabric does not create a client incident", proofStatus: "UNAVAILABLE_FABRIC", source: models.SourceStatusMismatch, want: false},
		{name: "matched source has no incident", proofStatus: "VERIFIED", source: models.SourceStatusMatched, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRecordClientSourceIncident(tt.proofStatus, tt.source); got != tt.want {
				t.Fatalf("shouldRecordClientSourceIncident(%q, %q) = %v, want %v", tt.proofStatus, tt.source, got, tt.want)
			}
		})
	}
}

func TestProofsForAnchorRootSelectsOnlyProofsForFabricBatch(t *testing.T) {
	proofs := []models.MerkleProof{
		{TransactionHash: "leaf", MerkleRoot: "root-a", TreeLevel: 0},
		{TransactionHash: "leaf", MerkleRoot: "root-b", TreeLevel: 0},
		{TransactionHash: "leaf", MerkleRoot: "root-b", TreeLevel: 1},
	}

	selected := proofsForAnchorRoot(proofs, "root-b")
	if len(selected) != 2 {
		t.Fatalf("selected proof count = %d, want 2", len(selected))
	}
	for _, proof := range selected {
		if proof.MerkleRoot != "root-b" {
			t.Fatalf("selected proof root = %q, want root-b", proof.MerkleRoot)
		}
	}
}

func TestRecoveryDisplayStatus(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   string
	}{
		{name: "no request", status: "", want: recoveryDisplayNotRecovered},
		{name: "client executable", status: models.RecoveryStatusPendingExecution, want: recoveryDisplayPending},
		{name: "legacy approval", status: models.RecoveryStatusApproved, want: recoveryDisplayPending},
		{name: "executing", status: models.RecoveryStatusExecuting, want: recoveryDisplayPending},
		{name: "succeeded", status: models.RecoveryStatusSucceeded, want: recoveryDisplayRecovered},
		{name: "verification failed", status: models.RecoveryStatusFailedVerification, want: recoveryDisplayFailed},
		{name: "execution failed", status: models.RecoveryStatusFailedExecution, want: recoveryDisplayFailed},
		{name: "rejected", status: models.RecoveryStatusRejected, want: recoveryDisplayFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := recoveryDisplayStatus(tt.status); got != tt.want {
				t.Fatalf("recoveryDisplayStatus(%q) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestShouldVerifyResourceWithAgent(t *testing.T) {
	tests := []struct {
		name     string
		action   string
		isLatest bool
		want     bool
	}{
		{name: "latest client event", action: "UPDATE", isLatest: true, want: true},
		{name: "historical client event", action: "UPDATE", isLatest: false, want: false},
		{name: "latest recovery event", action: "RECOVERY", isLatest: true, want: false},
		{name: "latest recovery event ignores casing and spaces", action: " recovery ", isLatest: true, want: false},
		{name: "historical recovery event", action: "RECOVERY", isLatest: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := models.AuditLog{Action: tt.action}
			if got := shouldVerifyResourceWithAgent(log, tt.isLatest); got != tt.want {
				t.Fatalf("shouldVerifyResourceWithAgent(%q, %t) = %t, want %t", tt.action, tt.isLatest, got, tt.want)
			}
		})
	}
}

func TestLatestClientEventIndexSkipsRecoveryEvents(t *testing.T) {
	tests := []struct {
		name string
		logs []models.AuditLog
		want int
	}{
		{
			name: "update remains latest client event after recovery",
			logs: []models.AuditLog{
				{Action: "INSERT"},
				{Action: "UPDATE"},
				{Action: "RECOVERY"},
			},
			want: 1,
		},
		{
			name: "multiple recovery events do not hide delete",
			logs: []models.AuditLog{
				{Action: "DELETE"},
				{Action: "RECOVERY"},
				{Action: " recovery "},
			},
			want: 0,
		},
		{
			name: "new client event after recovery becomes latest",
			logs: []models.AuditLog{
				{Action: "UPDATE"},
				{Action: "RECOVERY"},
				{Action: "UPDATE"},
			},
			want: 2,
		},
		{
			name: "only recovery events have no client event",
			logs: []models.AuditLog{
				{Action: "RECOVERY"},
			},
			want: -1,
		},
		{
			name: "empty history has no client event",
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := latestClientEventIndex(tt.logs); got != tt.want {
				t.Fatalf("latestClientEventIndex() = %d, want %d", got, tt.want)
			}
		})
	}
}
