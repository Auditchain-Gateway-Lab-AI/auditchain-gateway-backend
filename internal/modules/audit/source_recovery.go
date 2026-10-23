package audit

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go-blockchain-api/internal/auditreference"
	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const sourceMismatchConfirmationDelay = time.Minute

// No sleeps or process-local timers: the first observation survives restarts in
// PostgreSQL. The next sweep confirms the same state against the same reference.
// Function dependencies keep the ordering and race checks independently testable.
type sourceRecoveryWorkflow struct {
	now     func() time.Time
	latest  func(string, string) (*models.AuditLog, error)
	observe func(models.AuditLog, string, time.Time) (time.Time, error)
	clear   func(models.AuditLog) error
	proof   func(models.AuditLog) (*VerificationResult, error)
	read    func(*models.AuditLog) (*agentverifier.VerifyResult, error)
	create  func(models.AuditLog, *VerificationResult) (*models.TamperIncident, error)
}

func sourceRecoveryReference(row models.AuditLog) string {
	anchor := ""
	if row.BlockchainTxID != nil {
		anchor = *row.BlockchainTxID
	}
	return crypto.GenerateSHA3_256(strings.Join([]string{
		row.ClientID, row.LogID, hasher.GenerateLogHash(&row), row.HashValue,
		row.MerkleRoot, anchor, row.Status,
	}, "|"))
}

func (w sourceRecoveryWorkflow) confirm(row models.AuditLog, result *VerificationResult) error {
	if !isClientSourceIncidentStatus(result.SourceStatus) || result.SourceStateHash == "" {
		return w.clear(row)
	}
	// Include pending newer events: never recover from an older anchored event
	// merely because its replacement is still moving through the CDC pipeline.
	current, err := w.latest(row.Resource, row.ClientID)
	if err != nil {
		return err
	}
	if current == nil || sourceRecoveryReference(*current) != sourceRecoveryReference(row) || row.Status != "ANCHORED" {
		result.Assessment = "SOURCE_REFERENCE_CHANGED"
		result.SourceStatus = models.SourceStatusNotComparable
		return w.clear(row)
	}
	now := w.now().UTC()
	firstSeen, err := w.observe(row, result.SourceStateHash, now)
	if err != nil {
		return err
	}
	if firstSeen.IsZero() || now.Before(firstSeen.Add(sourceMismatchConfirmationDelay)) {
		result.Assessment = "CLIENT_STATE_DIVERGENCE_AWAITING_CONFIRMATION"
		result.Message = "Perbedaan data menunggu pemeriksaan ulang setelah jeda konfirmasi."
		return nil
	}

	proof, err := w.proof(row)
	if err != nil {
		return err
	}
	if proof == nil {
		return errors.New("source recovery proof result missing")
	}
	result.ProofStatus = proof.ProofStatus
	if !shouldRecordClientSourceIncident(proof.ProofStatus, result.SourceStatus) {
		result.Assessment = proof.Assessment
		result.Message = proof.Message
		return nil
	}

	// Proof validation can take time. Read the live row again afterwards; a
	// transient mismatch, lost Agent connection or changed value must not create
	// a recovery incident based on the earlier observation.
	confirmed, err := w.read(&row)
	if err != nil {
		applySourceComparison(result, &row, nil)
		result.AgentStatus, result.SourceStatus = agentFailureStatuses(err)
		if clearErr := w.clear(row); clearErr != nil {
			return errors.Join(err, clearErr)
		}
		return err
	}
	oldStatus, oldHash := result.SourceStatus, result.SourceStateHash
	applySourceComparison(result, &row, confirmed)
	if result.SourceStatus != oldStatus || result.SourceStateHash == "" || result.SourceStateHash != oldHash {
		result.Assessment = "SOURCE_STATE_CHANGED_DURING_CONFIRMATION"
		return w.clear(row)
	}
	current, err = w.latest(row.Resource, row.ClientID)
	if err != nil {
		return err
	}
	if current == nil || sourceRecoveryReference(*current) != sourceRecoveryReference(row) {
		result.SourceStatus = models.SourceStatusNotComparable
		result.Assessment = "SOURCE_REFERENCE_CHANGED"
		return w.clear(row)
	}
	incident, err := w.create(row, result)
	if err != nil {
		return err
	}
	if incident == nil {
		return errors.New("source recovery incident result missing")
	}
	result.IncidentID, result.IncidentScope = incident.ID, incident.IncidentScope
	result.IncidentType, result.IncidentStatus = incident.IncidentType, incident.Status
	result.Assessment = "CLIENT_STATE_DIVERGENCE_CONFIRMED"
	result.Message = "Perbedaan dikonfirmasi ulang terhadap referensi terbaru dengan bukti Fabric yang valid."
	return nil
}

func applySourceComparison(result *VerificationResult, row *models.AuditLog, observed *agentverifier.VerifyResult) {
	// Clear fields from an earlier read before applying an inconclusive response.
	result.SourceStateHash, result.ClientStateHash, result.OffchainStateHash = "", "", ""
	result.AgentDiscrepancies, result.AuditTrailDiffs = nil, nil
	result.SourceCheckedAt = nil
	result.StateComparison, result.ComparisonBasis, result.AuditTrailStatus = "", "", ""
	if observed == nil || !observed.AgentUsed {
		result.SourceStatus, result.AgentStatus = models.SourceStatusNotComparable, "not_configured"
		return
	}
	result.SourceStateHash, result.ClientStateHash = observed.ClientStateHash, observed.ClientStateHash
	result.OffchainStateHash, result.SourceCheckedAt = observed.OffchainStateHash, observed.SourceCheckedAt
	result.StateComparison, result.ComparisonBasis = observed.StateComparison, observed.ComparisonBasis
	result.AuditTrailStatus, result.AuditTrailDiffs = observed.AuditTrailStatus, observed.AuditTrailDiffs
	result.AgentDiscrepancies = observed.Discrepancies
	switch {
	case !observed.StateComparable:
		result.SourceStatus, result.AgentStatus = models.SourceStatusNotComparable, "not_comparable"
	case observed.IsMatch:
		result.SourceStatus, result.AgentStatus = models.SourceStatusMatched, "matched"
	default:
		result.SourceStatus, result.AgentStatus = sourceStatusFromAgentResult(*row, observed), "mismatch"
	}
}

func (s *auditService) sourceRecoveryWorkflow(attemptID string) sourceRecoveryWorkflow {
	return sourceRecoveryWorkflow{
		now:     time.Now,
		latest:  s.repo.GetLatestClientLogByResource,
		observe: s.observeSourceMismatch,
		clear:   s.clearSourceMismatch,
		read:    s.agent.VerifyAgainstAgent,
		proof: func(row models.AuditLog) (*VerificationResult, error) {
			proof, err := s.verifyLogIntegrity(row.LogID, row.ClientID, "source-confirmation-"+attemptID, true)
			if err != nil || proof == nil {
				return proof, err
			}
			historyResult := ""
			if proof.ProofStatus == "LEGACY_LIMITED" {
				historyResult = "LIMITED"
			}
			err = s.persistIntegrityStatusForAttempt(row.LogID, row.ClientID,
				integrityStatusFromVerificationResult(proof), "SOURCE_SWEEP", "", nil, attemptID, historyResult)
			return proof, err
		},
		create: s.createConfirmedSourceIncident,
	}
}

func (s *auditService) observeSourceMismatch(row models.AuditLog, stateHash string, now time.Time) (time.Time, error) {
	if s.db == nil {
		return time.Time{}, errors.New("source confirmation database unavailable")
	}
	var firstSeen time.Time
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var stored models.AuditLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("client_id = ? AND log_id = ?", row.ClientID, row.LogID).First(&stored).Error; err != nil {
			return err
		}
		reference := sourceRecoveryReference(row)
		if sourceRecoveryReference(stored) != reference {
			return errors.New("source reference changed while recording mismatch")
		}
		if stored.SourceMismatchFirstSeenAt != nil && stored.SourceMismatchStateHash == stateHash && stored.SourceMismatchReference == reference {
			firstSeen = stored.SourceMismatchFirstSeenAt.UTC()
			return nil
		}
		firstSeen = now
		return tx.Model(&stored).Updates(map[string]interface{}{
			"source_mismatch_first_seen_at": now, "source_mismatch_state_hash": stateHash,
			"source_mismatch_reference": reference,
		}).Error
	})
	return firstSeen, err
}

func (s *auditService) clearSourceMismatch(row models.AuditLog) error {
	if s.db == nil {
		return nil
	}
	return s.db.Model(&models.AuditLog{}).Where("client_id = ? AND log_id = ?", row.ClientID, row.LogID).
		Updates(map[string]interface{}{
			"source_mismatch_first_seen_at": nil, "source_mismatch_state_hash": "", "source_mismatch_reference": "",
		}).Error
}

func (s *auditService) createConfirmedSourceIncident(row models.AuditLog, result *VerificationResult) (*models.TamperIncident, error) {
	if s.db == nil {
		return nil, errors.New("source recovery database unavailable")
	}
	var incident *models.TamperIncident
	err := s.db.Transaction(func(tx *gorm.DB) error {
		var stored models.AuditLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("client_id = ? AND log_id = ?", row.ClientID, row.LogID).First(&stored).Error; err != nil {
			return err
		}
		latest, err := auditreference.LatestClientEvent(tx, row.ClientID, row.Resource)
		if err != nil {
			return err
		}
		reference := sourceRecoveryReference(row)
		if sourceRecoveryReference(*latest) != reference || sourceRecoveryReference(stored) != reference ||
			stored.SourceMismatchReference != reference || stored.SourceMismatchStateHash != result.SourceStateHash ||
			stored.SourceMismatchFirstSeenAt == nil || time.Since(*stored.SourceMismatchFirstSeenAt) < sourceMismatchConfirmationDelay {
			return errors.New("source confirmation changed before incident creation")
		}
		txService := &auditService{db: tx}
		incident, err = txService.ensureClientSourceIncident(stored, result.SourceStatus, result.SourceStateHash, result.AgentDiscrepancies)
		if err != nil {
			return fmt.Errorf("create confirmed source recovery incident: %w", err)
		}
		return nil
	})
	return incident, err
}
