package recovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/redaction"
)

const gatewayAuditRecoverySource = "CLIENT_AUDIT_TRAIL"

type gatewayAuditRecoveryData struct {
	Incident    models.TamperIncident
	Log         models.AuditLog
	TrustedLog  models.AuditLog
	Reference   *TrustedReference
	MetadataRaw []byte
	CurrentHash string
}

// loadGatewayAuditRecoveryData obtains the original event image from the
// client's Agent, then accepts it only if it recreates the existing anchored
// AuditChain leaf and proof. PostgreSQL's current metadata is never used as the
// recovery source.
func (s *Service) loadGatewayAuditRecoveryData(ctx context.Context, clientID, incidentID string) (*gatewayAuditRecoveryData, error) {
	if s.agent == nil {
		return nil, errors.New("agent_recovery_client_unavailable")
	}
	var incident models.TamperIncident
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", incidentID, clientID).First(&incident).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("incident_not_found")
		}
		return nil, err
	}
	if !strings.EqualFold(strings.TrimSpace(incident.IncidentScope), models.RecoveryScopeGatewayIntegrity) {
		return nil, errors.New("gateway_integrity_incident_required")
	}
	if incident.Status == models.IncidentStatusResolved {
		return nil, errors.New("incident_closed")
	}
	if !strings.EqualFold(strings.TrimSpace(incident.IncidentType), "METADATA_HASH_MISMATCH") {
		return nil, errors.New("gateway_metadata_recovery_unsupported")
	}

	var logRow models.AuditLog
	if err := s.db.WithContext(ctx).Where("log_id = ? AND client_id = ?", incident.LogID, clientID).First(&logRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("target_log_not_found")
		}
		return nil, err
	}
	if strings.TrimSpace(logRow.SourceRecordID) == "" {
		return nil, errors.New("audit_trail_reference_missing")
	}
	if logRow.BlockchainTxID == nil || strings.TrimSpace(*logRow.BlockchainTxID) == "" || strings.TrimSpace(logRow.MerkleRoot) == "" {
		return nil, errors.New("reference_anchor_missing")
	}

	trail, err := s.agent.ReadAuditTrail(ctx, clientID, logRow.SourceRecordID)
	if err != nil {
		return nil, err
	}
	if trail == nil || !trail.Found {
		return nil, errors.New("audit_trail_record_not_found")
	}
	trustedLog, metadata, err := trustedGatewayLogFromAuditTrail(logRow, trail)
	if err != nil {
		return nil, err
	}

	var proofs []models.MerkleProof
	if err := s.db.WithContext(ctx).
		Where("transaction_hash = ? AND merkle_root = ?", logRow.HashValue, logRow.MerkleRoot).
		Order("tree_level ASC").Find(&proofs).Error; err != nil {
		return nil, fmt.Errorf("reference_proof_read_failed: %w", err)
	}
	reference, err := ValidateTrustedReference(&trustedLog, proofs, s.fabric)
	if err != nil {
		return nil, err
	}
	currentHash := hasher.GenerateLogHash(&logRow)
	if strings.EqualFold(currentHash, reference.LeafHash) {
		return nil, errors.New("recovery_not_required")
	}
	return &gatewayAuditRecoveryData{
		Incident: incident, Log: logRow, TrustedLog: trustedLog,
		Reference: reference, MetadataRaw: metadata, CurrentHash: currentHash,
	}, nil
}

func trustedGatewayLogFromAuditTrail(logRow models.AuditLog, trail *agentverifier.AuditTrailRecord) (models.AuditLog, []byte, error) {
	if trail == nil || !trail.Found {
		return models.AuditLog{}, nil, errors.New("audit_trail_record_not_found")
	}
	if strings.TrimSpace(trail.ID) != "" && strings.TrimSpace(trail.ID) != strings.TrimSpace(logRow.SourceRecordID) {
		return models.AuditLog{}, nil, errors.New("audit_trail_reference_mismatch")
	}
	if !sameAuditTrailResource(logRow.Resource, trail.Tabel) {
		return models.AuditLog{}, nil, errors.New("audit_trail_resource_mismatch")
	}
	if !strings.EqualFold(strings.TrimSpace(logRow.Action), strings.TrimSpace(trail.Operasi)) {
		return models.AuditLog{}, nil, errors.New("audit_trail_operation_mismatch")
	}
	metadata, err := trail.MetadataJSON()
	if err != nil {
		return models.AuditLog{}, nil, err
	}
	trustedLog := logRow
	trustedLog.Metadata = string(metadata)
	if actual := hasher.GenerateLogHash(&trustedLog); !strings.EqualFold(actual, logRow.HashValue) {
		return models.AuditLog{}, nil, errors.New("reference_local_hash_mismatch")
	}
	return trustedLog, metadata, nil
}

func sameAuditTrailResource(resource, tableName string) bool {
	normalize := func(value string) string {
		value = strings.TrimSpace(value)
		if strings.Contains(value, ":") {
			value = strings.SplitN(value, ":", 2)[0]
		}
		if strings.Contains(value, ".") {
			parts := strings.Split(value, ".")
			value = parts[len(parts)-1]
		}
		return strings.ToUpper(strings.TrimSpace(value))
	}
	return normalize(resource) != "" && normalize(resource) == normalize(tableName)
}

func (s *Service) listGatewayAuditTrailCandidate(ctx context.Context, clientID, incidentID string) ([]CandidateView, error) {
	var incident models.TamperIncident
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", incidentID, clientID).First(&incident).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("incident_not_found")
		}
		return nil, err
	}
	data, err := s.loadGatewayAuditRecoveryData(ctx, clientID, incidentID)
	if err != nil {
		return []CandidateView{{
			LogID: incident.LogID, Resource: incident.Resource, Eligible: false,
			RecoverySource: gatewayAuditRecoverySource, Reason: failureCode(err),
		}}, nil
	}
	return []CandidateView{{
		LogID: data.Log.LogID, Resource: data.Log.Resource, RecoverySource: gatewayAuditRecoverySource,
		ReferenceLogHash: data.Reference.LeafHash, ReferenceMerkleRoot: data.Reference.MerkleRoot,
		ReferenceAnchorID: data.Reference.AnchorID, Eligible: true,
	}}, nil
}

func (s *Service) preflightGatewayAuditTrail(ctx context.Context, clientID, incidentID string) (*PreflightResult, error) {
	data, err := s.loadGatewayAuditRecoveryData(ctx, clientID, incidentID)
	if err != nil {
		return nil, err
	}
	metadata, err := redaction.Value(data.MetadataRaw)
	if err != nil {
		return nil, fmt.Errorf("reference_metadata_invalid: %w", err)
	}
	return &PreflightResult{
		Status: "VALID", Recoverable: true, LogID: data.Log.LogID,
		RecoverySource: gatewayAuditRecoverySource, CurrentHash: data.CurrentHash,
		CurrentIntegrity: models.IntegrityStatusTampered, SnapshotHash: data.Reference.LeafHash,
		MerkleRoot: data.Reference.MerkleRoot, AnchorID: data.Reference.AnchorID,
		ReferenceLogHash: data.Reference.LeafHash, ReferenceMerkleRoot: data.Reference.MerkleRoot,
		ReferenceAnchorID: data.Reference.AnchorID, FabricRoot: data.Reference.FabricRoot,
		TrustedReferencePreview: &SnapshotPreview{
			Actor: data.TrustedLog.Actor, Action: data.TrustedLog.Action,
			Resource: data.TrustedLog.Resource, Timestamp: data.TrustedLog.Timestamp,
			DBTimestamp: data.TrustedLog.DBTimestamp, SourceSystem: data.TrustedLog.SourceSystem,
			AuthorizationContext: data.TrustedLog.AuthorizationContext,
			SourceRecordID:       data.TrustedLog.SourceRecordID, Metadata: metadata,
		},
	}, nil
}

func (s *Service) createGatewayAuditTrailRequest(ctx context.Context, clientID, userID string, input CreateRequestInput) (*models.RecoveryRequest, error) {
	data, err := s.loadGatewayAuditRecoveryData(ctx, clientID, input.IncidentID)
	if err != nil {
		return nil, err
	}
	if input.SelectedLogID != data.Log.LogID {
		return nil, errors.New("cross_log_recovery_not_allowed")
	}
	var active models.RecoveryRequest
	if err := s.db.WithContext(ctx).
		Where("client_id = ? AND resource = ? AND status IN ?", clientID, data.Log.Resource, []string{
			models.RecoveryStatusPendingExecution, models.RecoveryStatusPendingApproval,
			models.RecoveryStatusApproved, models.RecoveryStatusExecuting,
		}).First(&active).Error; err == nil {
		return nil, errors.New("recovery_active_conflict")
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	request := &models.RecoveryRequest{
		ID: uuid.NewString(), ClientID: clientID, IncidentID: data.Incident.ID,
		TargetLogID: data.Log.LogID, SelectedLogID: data.Log.LogID,
		Resource: data.Log.Resource, Operation: "RESTORE_AUDIT_LOG",
		ReferenceLogHash: data.Reference.LeafHash, ReferenceMerkleRoot: data.Reference.MerkleRoot,
		ReferenceAnchorID: data.Reference.AnchorID, AnchorID: data.Reference.AnchorID,
		ExpectedMerkleRoot: data.Reference.MerkleRoot, RequestedBy: userID,
		Reason: strings.TrimSpace(input.Reason), Status: models.RecoveryStatusPendingExecution,
		IdempotencyKey: strings.TrimSpace(input.IdempotencyKey), BeforeHash: data.CurrentHash,
	}
	if err := s.db.WithContext(ctx).Create(request).Error; err != nil {
		if strings.Contains(err.Error(), "idx_recovery_active_resource") {
			return nil, errors.New("recovery_active_conflict")
		}
		return nil, err
	}
	return request, nil
}

func (s *Service) executeGatewayAuditTrail(ctx context.Context, clientID, requestID, executorID string) (*models.RecoveryRequest, error) {
	var request models.RecoveryRequest
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND client_id = ?", requestID, clientID).First(&request).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("request_not_found")
			}
			return err
		}
		if request.Status != models.RecoveryStatusPendingExecution {
			return errors.New("invalid_request_state")
		}
		return tx.Model(&request).Updates(map[string]interface{}{
			"status": models.RecoveryStatusExecuting, "executed_by": executorID,
			"execution_started_at": time.Now().UTC(),
		}).Error
	}); err != nil {
		return nil, err
	}

	data, err := s.loadGatewayAuditRecoveryData(ctx, clientID, request.IncidentID)
	if err != nil {
		return s.failGatewayAuditRequest(ctx, &request, executorID, err)
	}
	if request.TargetLogID != data.Log.LogID || request.SelectedLogID != data.Log.LogID ||
		request.ReferenceLogHash != data.Reference.LeafHash ||
		request.ReferenceMerkleRoot != data.Reference.MerkleRoot ||
		request.ReferenceAnchorID != data.Reference.AnchorID {
		return s.failGatewayAuditRequest(ctx, &request, executorID, errors.New("reference_changed"))
	}
	if request.BeforeHash != data.CurrentHash {
		return s.failGatewayAuditRequest(ctx, &request, executorID, errors.New("target_changed"))
	}

	now := time.Now().UTC()
	var event models.RecoveryEvent
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var locked models.RecoveryRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND client_id = ?", request.ID, clientID).First(&locked).Error; err != nil {
			return err
		}
		if locked.Status != models.RecoveryStatusExecuting {
			return errors.New("request_execution_state_changed")
		}
		var target models.AuditLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("log_id = ? AND client_id = ?", data.Log.LogID, clientID).First(&target).Error; err != nil {
			return err
		}
		if hasher.GenerateLogHash(&target) != request.BeforeHash {
			return errors.New("target_changed")
		}
		if err := tx.Model(&target).Updates(map[string]interface{}{
			"metadata":             data.TrustedLog.Metadata,
			"integrity_status":     models.IntegrityStatusValid,
			"integrity_checked_at": now,
			"integrity_error":      "",
			"integrity_source":     models.IntegritySourceRecovery,
			"integrity_run_id":     "",
			"status":               "ANCHORED",
		}).Error; err != nil {
			return fmt.Errorf("restore_audit_log_failed: %w", err)
		}
		var restored models.AuditLog
		if err := tx.Where("log_id = ? AND client_id = ?", target.LogID, clientID).First(&restored).Error; err != nil {
			return fmt.Errorf("restore_audit_log_readback_failed: %w", err)
		}
		if actual := hasher.GenerateLogHash(&restored); !strings.EqualFold(actual, data.Reference.LeafHash) {
			return errors.New("post_recovery_hash_mismatch")
		}

		targetTimestamp := restored.Timestamp
		event = models.RecoveryEvent{
			ID: uuid.NewString(), ClientID: clientID, RequestID: request.ID,
			IncidentID: request.IncidentID, TargetLogID: restored.LogID, SelectedLogID: restored.LogID,
			EventType: models.RecoveryEventTypeExecution, ResultStatus: models.RecoveryResultSucceeded,
			Operation: "RESTORE_AUDIT_LOG", Resource: restored.Resource,
			TargetActor: restored.Actor, TargetAction: restored.Action, TargetTimestamp: &targetTimestamp,
			SourceSystem: restored.SourceSystem, TargetSourceSystem: restored.SourceSystem,
			TargetAuthorization: restored.AuthorizationContext, TargetSourceRecordID: restored.SourceRecordID,
			RecoveredMetadata: redactedRecoveryMetadata(data.MetadataRaw),
			ExecutorSystem:    "AuditChain Gateway", ExecutedBy: executorID, Reason: request.Reason,
			BeforeHash: request.BeforeHash, AfterHash: data.Reference.LeafHash,
			ReferenceLogHash: data.Reference.LeafHash, ReferenceMerkleRoot: data.Reference.MerkleRoot,
			ReferenceAnchorID: data.Reference.AnchorID, ReadbackStatus: models.SourceStatusMatched,
			CDCStatus: models.CDCStatusNotRequired, ExecutedAt: now,
			PipelineStatus: models.RecoveryPipelineHashed,
			SnapshotStatus: models.SnapshotStatusLegacyMissing, IntegrityStatus: models.IntegrityStatusNotChecked,
		}
		event.EventHash = hasher.GenerateRecoveryEventHash(&event)
		if err := tx.Create(&event).Error; err != nil {
			return fmt.Errorf("recovery_event_create_failed: %w", err)
		}
		if err := tx.Model(&locked).Updates(map[string]interface{}{
			"status": models.RecoveryStatusSucceeded, "executed_by": executorID,
			"executed_at": now, "after_hash": data.Reference.LeafHash,
			"recovery_event_id": event.ID,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.ClientDashboardStats{}).Where("client_id = ?", clientID).Updates(map[string]interface{}{
			"last_integrity_check_at":     now,
			"last_integrity_check_source": models.IntegritySourceRecovery,
			"last_integrity_check_run_id": "",
			"last_integrity_check_logs":   1,
		}).Error; err != nil {
			return err
		}
		incidentUpdate := tx.Model(&models.TamperIncident{}).
			Where("id = ? AND client_id = ? AND status = ?", request.IncidentID, clientID, models.IncidentStatusOpen).
			Updates(map[string]interface{}{"status": models.IncidentStatusResolved, "resolved_at": now})
		if incidentUpdate.Error != nil {
			return incidentUpdate.Error
		}
		if incidentUpdate.RowsAffected != 1 {
			return errors.New("incident_closed")
		}
		return nil
	})
	if err != nil {
		return s.failGatewayAuditRequest(ctx, &request, executorID, err)
	}
	_ = s.db.WithContext(ctx).First(&request, "id = ? AND client_id = ?", request.ID, clientID).Error
	return &request, nil
}

func (s *Service) failGatewayAuditRequest(ctx context.Context, request *models.RecoveryRequest, executorID string, failure error) (*models.RecoveryRequest, error) {
	if request == nil {
		return nil, failure
	}
	message := sanitizeFailure(failure)
	code := failureCode(failure)
	_ = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		updates := map[string]interface{}{
			"status":         models.RecoveryStatusFailedVerification,
			"failure_reason": message, "executed_by": executorID,
		}
		if strings.TrimSpace(request.RecoveryEventID) == "" {
			var target models.AuditLog
			_ = tx.Where("log_id = ? AND client_id = ?", request.TargetLogID, request.ClientID).First(&target).Error
			now := time.Now().UTC()
			event := models.RecoveryEvent{
				ID: uuid.NewString(), ClientID: request.ClientID, RequestID: request.ID,
				IncidentID: request.IncidentID, TargetLogID: request.TargetLogID, SelectedLogID: request.SelectedLogID,
				EventType: models.RecoveryEventTypeExecution, ResultStatus: models.RecoveryResultVerification,
				Operation: "RESTORE_AUDIT_LOG", Resource: request.Resource,
				TargetActor: target.Actor, TargetAction: target.Action, SourceSystem: target.SourceSystem,
				TargetSourceSystem: target.SourceSystem, TargetAuthorization: target.AuthorizationContext,
				TargetSourceRecordID: target.SourceRecordID, ExecutorSystem: "AuditChain Gateway",
				ExecutedBy: executorID, Reason: request.Reason, BeforeHash: request.BeforeHash,
				ReferenceLogHash: request.ReferenceLogHash, ReferenceMerkleRoot: request.ReferenceMerkleRoot,
				ReferenceAnchorID: request.ReferenceAnchorID, FailureCode: code,
				FailureReason: message, ExecutedAt: now, PipelineStatus: models.RecoveryPipelineHashed,
				SnapshotStatus: models.SnapshotStatusLegacyMissing, IntegrityStatus: models.IntegrityStatusNotChecked,
				ReadbackStatus: models.SourceStatusNotComparable, CDCStatus: models.CDCStatusNotRequired,
			}
			if !target.Timestamp.IsZero() {
				timestamp := target.Timestamp
				event.TargetTimestamp = &timestamp
			}
			event.EventHash = hasher.GenerateRecoveryEventHash(&event)
			if err := tx.Create(&event).Error; err == nil {
				updates["recovery_event_id"] = event.ID
			}
		}
		return tx.Model(&models.RecoveryRequest{}).Where("id = ? AND client_id = ?", request.ID, request.ClientID).Updates(updates).Error
	})
	_ = s.db.WithContext(ctx).First(request, "id = ? AND client_id = ?", request.ID, request.ClientID).Error
	return request, fmt.Errorf("%s", code)
}
