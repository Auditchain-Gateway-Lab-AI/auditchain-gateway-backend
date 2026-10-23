package audit

import (
	"errors"
	"testing"
	"time"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
)

type sourceRecoveryFixture struct {
	w                                       sourceRecoveryWorkflow
	row                                     models.AuditLog
	latest                                  *models.AuditLog
	now                                     time.Time
	firstSeen                               time.Time
	observedHash, observedReference         string
	proof                                   *VerificationResult
	read                                    *agentverifier.VerifyResult
	proofErr, readErr, createErr            error
	proofCalls, readCalls, created, cleared int
	afterProof, afterRead                   func()
}

func newSourceRecoveryFixture() *sourceRecoveryFixture {
	anchor := "anchor-1"
	f := &sourceRecoveryFixture{
		now: time.Date(2026, 10, 23, 5, 0, 0, 0, time.UTC),
		row: models.AuditLog{LogID: "log-1", ClientID: "morbis", Resource: "RUANGAN:1046", Action: "UPDATE",
			Metadata: `{"id":1046,"nama":"Melati"}`, Status: "ANCHORED", BlockchainTxID: &anchor},
		proof: &VerificationResult{ProofStatus: "VERIFIED"},
		read:  &agentverifier.VerifyResult{AgentUsed: true, StateComparable: true, SourceFound: true, ClientStateHash: "mawar-hash"},
	}
	f.row.HashValue = hasher.GenerateLogHash(&f.row)
	f.row.MerkleRoot = f.row.HashValue
	latest := f.row
	f.latest = &latest
	f.w = sourceRecoveryWorkflow{
		now: func() time.Time { return f.now },
		latest: func(resource, clientID string) (*models.AuditLog, error) {
			if resource != f.row.Resource || clientID != f.row.ClientID {
				return nil, errors.New("unscoped source lookup")
			}
			return f.latest, nil
		},
		observe: func(row models.AuditLog, hash string, now time.Time) (time.Time, error) {
			reference := sourceRecoveryReference(row)
			if f.firstSeen.IsZero() || hash != f.observedHash || reference != f.observedReference {
				f.firstSeen, f.observedHash, f.observedReference = now, hash, reference
			}
			return f.firstSeen, nil
		},
		clear: func(models.AuditLog) error { f.firstSeen = time.Time{}; f.cleared++; return nil },
		proof: func(models.AuditLog) (*VerificationResult, error) {
			f.proofCalls++
			if f.afterProof != nil {
				f.afterProof()
			}
			return f.proof, f.proofErr
		},
		read: func(*models.AuditLog) (*agentverifier.VerifyResult, error) {
			f.readCalls++
			if f.afterRead != nil {
				f.afterRead()
			}
			return f.read, f.readErr
		},
		create: func(row models.AuditLog, result *VerificationResult) (*models.TamperIncident, error) {
			if f.createErr != nil {
				return nil, f.createErr
			}
			f.created++
			return &models.TamperIncident{ID: "incident-1", ClientID: row.ClientID, LogID: row.LogID,
				IncidentScope: models.RecoveryScopeClientSource, IncidentType: clientSourceIncidentType(result.SourceStatus), Status: models.IncidentStatusOpen}, nil
		},
	}
	return f
}

func mismatchResult() *VerificationResult {
	return &VerificationResult{LogID: "log-1", ProofStatus: "NOT_CHECKED", SourceStatus: models.SourceStatusMismatch, SourceStateHash: "mawar-hash"}
}

func (f *sourceRecoveryFixture) observeThenWait(t *testing.T) {
	t.Helper()
	if err := f.w.confirm(f.row, mismatchResult()); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(sourceMismatchConfirmationDelay + time.Second)
}

func TestSourceRecoveryWaitsForAnotherSweepAndRechecksProofAndClient(t *testing.T) {
	f := newSourceRecoveryFixture()
	r := mismatchResult()
	if err := f.w.confirm(f.row, r); err != nil {
		t.Fatal(err)
	}
	if f.proofCalls != 0 || f.readCalls != 0 || f.created != 0 || r.Assessment != "CLIENT_STATE_DIVERGENCE_AWAITING_CONFIRMATION" {
		t.Fatalf("first observation must wait: result=%+v proof=%d read=%d created=%d", r, f.proofCalls, f.readCalls, f.created)
	}
	first := f.firstSeen
	f.now = f.now.Add(30 * time.Second)
	if err := f.w.confirm(f.row, mismatchResult()); err != nil {
		t.Fatal(err)
	}
	if !f.firstSeen.Equal(first) || f.created != 0 {
		t.Fatal("short sweeps must preserve the original confirmation time")
	}
	f.now = f.now.Add(31 * time.Second)
	r = mismatchResult()
	if err := f.w.confirm(f.row, r); err != nil {
		t.Fatal(err)
	}
	if f.proofCalls != 1 || f.readCalls != 1 || f.created != 1 || r.IncidentID != "incident-1" || r.ProofStatus != "VERIFIED" || r.ClientStateHash != "mawar-hash" {
		t.Fatalf("confirmed mismatch missing incident: %+v", r)
	}
}

func TestSourceRecoveryRejectsIncompleteUntrustedAndUnavailableProofs(t *testing.T) {
	for _, status := range []string{"NOT_CHECKED", "LEGACY_LIMITED", "PENDING_ANCHOR", "FAILED_LOCAL_HASH", "FAILED_ANCHOR_ROOT", "FAILED_MERKLE_PROOF", "UNAVAILABLE_FABRIC", "UNAVAILABLE_MERKLE_PROOF"} {
		t.Run(status, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.observeThenWait(t)
			f.proof = &VerificationResult{ProofStatus: status, Assessment: "REFERENCE_BLOCKED"}
			r := mismatchResult()
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			if f.created != 0 || f.readCalls != 0 || r.ProofStatus != status || r.Assessment != "REFERENCE_BLOCKED" {
				t.Fatalf("unsafe reference passed: %+v", r)
			}
		})
	}
}

func TestSourceRecoveryRejectsNewerPendingEventBeforeAndDuringConfirmation(t *testing.T) {
	for _, stage := range []string{"before", "during proof", "during read"} {
		t.Run(stage, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.observeThenWait(t)
			change := func() { newer := f.row; newer.LogID = "log-2"; newer.Status = "HASHED"; f.latest = &newer }
			switch stage {
			case "before":
				change()
			case "during proof":
				f.afterProof = change
			case "during read":
				f.afterRead = change
			}
			r := mismatchResult()
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			if f.created != 0 || r.Assessment != "SOURCE_REFERENCE_CHANGED" || r.SourceStatus != models.SourceStatusNotComparable || f.cleared != 1 {
				t.Fatalf("new event must block old recovery: %+v", r)
			}
		})
	}
}

func TestSourceRecoveryCancelsWhenLiveStateChangesOrBecomesUnknown(t *testing.T) {
	for _, state := range []string{"matched", "different mismatch", "not comparable", "agent missing"} {
		t.Run(state, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.observeThenWait(t)
			switch state {
			case "matched":
				f.read.IsMatch = true
				f.read.ClientStateHash = "melati-hash"
			case "different mismatch":
				f.read.ClientStateHash = "anggrek-hash"
			case "not comparable":
				f.read.StateComparable = false
			case "agent missing":
				f.read = nil
			}
			r := mismatchResult()
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			if f.created != 0 || f.cleared != 1 || !f.firstSeen.IsZero() {
				t.Fatalf("changed state must cancel: %+v", r)
			}
			if state == "matched" && r.SourceStatus != models.SourceStatusMatched {
				t.Fatalf("fresh match lost: %+v", r)
			}
		})
	}
}

func TestSourceRecoveryChangedObservationRestartsGracePeriod(t *testing.T) {
	f := newSourceRecoveryFixture()
	f.observeThenWait(t)
	r := mismatchResult()
	r.SourceStateHash = "new-state-hash"
	if err := f.w.confirm(f.row, r); err != nil {
		t.Fatal(err)
	}
	if !f.firstSeen.Equal(f.now) || f.proofCalls != 0 || f.created != 0 {
		t.Fatal("new mismatch must start its own confirmation period")
	}
}

func TestSourceRecoveryClearsObservationWhenInitialReadIsHealthyOrUnknown(t *testing.T) {
	for _, status := range []string{models.SourceStatusMatched, models.SourceStatusNotComparable, models.SourceStatusUnreachable} {
		t.Run(status, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.observeThenWait(t)
			r := &VerificationResult{SourceStatus: status}
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			if f.cleared != 1 || !f.firstSeen.IsZero() || f.proofCalls != 0 || f.created != 0 {
				t.Fatal("healthy/unknown initial result must cancel previous mismatch")
			}
		})
	}
}

type sourceRecoveryRepositoryStub struct {
	AuditRepository
	row models.AuditLog
}

func (r sourceRecoveryRepositoryStub) GetLogByID(string, string) (*models.AuditLog, error) {
	row := r.row
	return &row, nil
}

func (r sourceRecoveryRepositoryStub) GetLatestClientLogByResource(string, string) (*models.AuditLog, error) {
	row := r.row
	return &row, nil
}

func TestSourceSweepWithoutAgentRemainsInconclusive(t *testing.T) {
	f := newSourceRecoveryFixture()
	service := &auditService{repo: sourceRecoveryRepositoryStub{row: f.row}}
	r, err := service.VerifyClientSourceIntegrity(f.row.LogID, f.row.ClientID, "attempt-1")
	if err != nil || r.SourceStatus != models.SourceStatusNotComparable || r.IncidentID != "" {
		t.Fatalf("unconfigured Agent must not create incident: result=%+v err=%v", r, err)
	}
}

func TestSourceRecoveryAgentAndPersistenceErrorsAreReturned(t *testing.T) {
	for _, stage := range []string{"proof", "read", "create"} {
		t.Run(stage, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.observeThenWait(t)
			want := errors.New("test failure")
			switch stage {
			case "proof":
				f.proofErr = want
			case "read":
				f.readErr = want
			case "create":
				f.createErr = want
			}
			r := mismatchResult()
			if err := f.w.confirm(f.row, r); !errors.Is(err, want) {
				t.Fatalf("error=%v, want %v", err, want)
			}
			if f.created != 0 || r.IncidentID != "" {
				t.Fatalf("failed confirmation created incident: %+v", r)
			}
			if stage == "read" && r.SourceStatus != models.SourceStatusUnreachable {
				t.Fatalf("read failure not reflected: %+v", r)
			}
		})
	}
}

func TestSourceRecoveryHandlesMissingAndUnexpectedRows(t *testing.T) {
	for _, action := range []string{"UPDATE", "DELETE"} {
		t.Run(action, func(t *testing.T) {
			f := newSourceRecoveryFixture()
			f.row.Action = action
			latest := f.row
			f.latest = &latest
			f.read.SourceFound = action == "DELETE"
			status := sourceStatusFromAgentResult(f.row, f.read)
			r := mismatchResult()
			r.SourceStatus = status
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			f.now = f.now.Add(sourceMismatchConfirmationDelay + time.Second)
			r = mismatchResult()
			r.SourceStatus = status
			if err := f.w.confirm(f.row, r); err != nil {
				t.Fatal(err)
			}
			if r.IncidentType != clientSourceIncidentType(status) || f.created != 1 {
				t.Fatalf("row existence incident missing: %+v", r)
			}
		})
	}
}

func TestSourceRecoveryReferenceIncludesPayloadAndAnchorButNotOperationalCache(t *testing.T) {
	f := newSourceRecoveryFixture()
	before := sourceRecoveryReference(f.row)
	row := f.row
	row.IntegrityStatus = "VALID"
	row.SourceMismatchStateHash = "operational"
	if sourceRecoveryReference(row) != before {
		t.Fatal("cache must not change source reference identity")
	}
	row.Metadata = `{"id":1046,"nama":"changed"}`
	if sourceRecoveryReference(row) == before {
		t.Fatal("payload change must invalidate confirmation")
	}
	row = f.row
	anchor := "new-anchor"
	row.BlockchainTxID = &anchor
	if sourceRecoveryReference(row) == before {
		t.Fatal("anchor change must invalidate confirmation")
	}
}
