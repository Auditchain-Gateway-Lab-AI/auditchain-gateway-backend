package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/storage/snapshotstore"
	"go-blockchain-api/pkg/crypto"
)

type FabricReader interface {
	GetAnchorFromLedger(anchorID string) (string, error)
}

type Service struct {
	db              *gorm.DB
	store           snapshotstore.SnapshotStore
	cipher          *snapshotstore.Cipher
	fabric          FabricReader
	snapshotBuilder snapshotstore.OutboxBuilder
}

type IncidentFilter struct {
	Status string
}

type VersionView struct {
	LogID              string     `json:"log_id"`
	Action             string     `json:"action"`
	Actor              string     `json:"actor"`
	Resource           string     `json:"resource"`
	Timestamp          time.Time  `json:"timestamp"`
	HashValue          string     `json:"hash_value"`
	MerkleRoot         string     `json:"merkle_root"`
	Status             string     `json:"status"`
	SnapshotStatus     string     `json:"snapshot_status"`
	SnapshotObjectKey  string     `json:"snapshot_object_key"`
	SnapshotVersionID  string     `json:"snapshot_version_id"`
	SnapshotVerifiedAt *time.Time `json:"snapshot_verified_at,omitempty"`
}

type CreateRequestInput struct {
	IncidentID     string `json:"incident_id" binding:"required"`
	SelectedLogID  string `json:"selected_log_id" binding:"required"`
	Reason         string `json:"reason" binding:"required,min=5"`
	IdempotencyKey string `json:"idempotency_key" binding:"required"`
}

func NewService(db *gorm.DB, store snapshotstore.SnapshotStore, cipher *snapshotstore.Cipher, fabric FabricReader, builders ...snapshotstore.OutboxBuilder) *Service {
	var builder snapshotstore.OutboxBuilder
	if len(builders) > 0 {
		builder = builders[0]
	}
	return &Service{db: db, store: store, cipher: cipher, fabric: fabric, snapshotBuilder: builder}
}

func (s *Service) ListIncidents(ctx context.Context, clientID string, filter IncidentFilter) ([]models.TamperIncident, error) {
	query := s.db.WithContext(ctx).Where("client_id = ?", clientID).Order("detected_at DESC")
	if strings.TrimSpace(filter.Status) != "" {
		query = query.Where("status = ?", strings.ToUpper(strings.TrimSpace(filter.Status)))
	}
	var incidents []models.TamperIncident
	if err := query.Find(&incidents).Error; err != nil {
		return nil, err
	}
	return incidents, nil
}

func (s *Service) GetIncident(ctx context.Context, clientID, incidentID string) (*models.TamperIncident, error) {
	var incident models.TamperIncident
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", incidentID, clientID).First(&incident).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("incident_not_found")
		}
		return nil, err
	}
	return &incident, nil
}

func (s *Service) ListRequests(ctx context.Context, clientID, status string) ([]models.RecoveryRequest, error) {
	query := s.db.WithContext(ctx).Where("client_id = ?", clientID).Order("requested_at DESC")
	if strings.TrimSpace(status) != "" {
		query = query.Where("status = ?", strings.ToUpper(strings.TrimSpace(status)))
	}
	var requests []models.RecoveryRequest
	if err := query.Find(&requests).Error; err != nil {
		return nil, err
	}
	return requests, nil
}

func (s *Service) GetRequest(ctx context.Context, clientID, requestID string) (*models.RecoveryRequest, error) {
	var request models.RecoveryRequest
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", requestID, clientID).First(&request).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("request_not_found")
		}
		return nil, err
	}
	return &request, nil
}

func (s *Service) ListVersions(ctx context.Context, clientID, resource string) ([]VersionView, error) {
	var logs []models.AuditLog
	if err := s.db.WithContext(ctx).
		Where("client_id = ? AND resource = ? AND snapshot_status = ?", clientID, resource, models.SnapshotStatusVerified).
		Order("timestamp ASC").
		Find(&logs).Error; err != nil {
		return nil, err
	}
	versions := make([]VersionView, 0, len(logs))
	for _, log := range logs {
		versions = append(versions, VersionView{
			LogID:              log.LogID,
			Action:             log.Action,
			Actor:              log.Actor,
			Resource:           log.Resource,
			Timestamp:          log.Timestamp,
			HashValue:          log.HashValue,
			MerkleRoot:         log.MerkleRoot,
			Status:             log.Status,
			SnapshotStatus:     log.SnapshotStatus,
			SnapshotObjectKey:  log.SnapshotObjectKey,
			SnapshotVersionID:  log.SnapshotVersionID,
			SnapshotVerifiedAt: log.SnapshotVerifiedAt,
		})
	}
	return versions, nil
}

func (s *Service) CreateRequest(ctx context.Context, clientID, userID string, input CreateRequestInput) (*models.RecoveryRequest, error) {
	if strings.TrimSpace(input.Reason) == "" || strings.TrimSpace(input.IdempotencyKey) == "" {
		return nil, errors.New("invalid_request")
	}
	var existing models.RecoveryRequest
	if err := s.db.WithContext(ctx).Where("idempotency_key = ? AND client_id = ?", input.IdempotencyKey, clientID).First(&existing).Error; err == nil {
		return &existing, nil
	}

	var incident models.TamperIncident
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", input.IncidentID, clientID).First(&incident).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("incident_not_found")
		}
		return nil, err
	}
	if incident.Status == models.IncidentStatusResolved || incident.Status == models.IncidentStatusDismissed {
		return nil, errors.New("incident_closed")
	}

	var selected models.AuditLog
	if err := s.db.WithContext(ctx).Where(
		"log_id = ? AND client_id = ? AND resource = ? AND snapshot_status = ?",
		input.SelectedLogID, clientID, incident.Resource, models.SnapshotStatusVerified,
	).First(&selected).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("snapshot_not_found")
		}
		return nil, err
	}
	if selected.SnapshotObjectKey == "" || selected.SnapshotVersionID == "" {
		return nil, errors.New("snapshot_reference_missing")
	}
	if selected.LogID != incident.LogID {
		return nil, errors.New("cross_log_recovery_not_allowed")
	}

	request := &models.RecoveryRequest{
		ID:                uuid.NewString(),
		ClientID:          clientID,
		IncidentID:        incident.ID,
		TargetLogID:       incident.LogID,
		SelectedLogID:     selected.LogID,
		SnapshotObjectKey: selected.SnapshotObjectKey,
		SnapshotVersionID: selected.SnapshotVersionID,
		RequestedBy:       userID,
		Reason:            strings.TrimSpace(input.Reason),
		Status:            models.RecoveryStatusPendingApproval,
		IdempotencyKey:    strings.TrimSpace(input.IdempotencyKey),
		BeforeHash:        selected.HashValue,
	}
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(request).Error; err != nil {
			return err
		}
		return tx.Model(&models.TamperIncident{}).Where("id = ? AND status = ?", incident.ID, models.IncidentStatusOpen).
			Update("status", models.IncidentStatusUnderReview).Error
	}); err != nil {
		return nil, err
	}
	return request, nil
}

func (s *Service) Approve(ctx context.Context, clientID, requestID, approverID string) (*models.RecoveryRequest, error) {
	return s.transitionRequest(ctx, clientID, requestID, approverID, models.RecoveryStatusApproved, models.RecoveryStatusPendingApproval)
}

func (s *Service) Reject(ctx context.Context, clientID, requestID, approverID string) (*models.RecoveryRequest, error) {
	return s.transitionRequest(ctx, clientID, requestID, approverID, models.RecoveryStatusRejected, models.RecoveryStatusPendingApproval)
}

func (s *Service) transitionRequest(ctx context.Context, clientID, requestID, actorID, next, expected string) (*models.RecoveryRequest, error) {
	var request models.RecoveryRequest
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND client_id = ?", requestID, clientID).First(&request).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("request_not_found")
			}
			return err
		}
		if request.Status != expected {
			return errors.New("invalid_request_state")
		}
		now := time.Now().UTC()
		updates := map[string]interface{}{"status": next}
		if next == models.RecoveryStatusApproved {
			updates["approved_by"] = actorID
			updates["approved_at"] = now
		} else {
			updates["failure_reason"] = "ditolak oleh approver"
		}
		if err := tx.Model(&request).Updates(updates).Error; err != nil {
			return err
		}
		if next == models.RecoveryStatusRejected {
			if err := tx.Model(&models.TamperIncident{}).Where("id = ? AND status = ?", request.IncidentID, models.IncidentStatusUnderReview).Update("status", models.IncidentStatusOpen).Error; err != nil {
				return err
			}
		}
		request.Status = next
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &request, nil
}

func (s *Service) Execute(ctx context.Context, clientID, requestID, executorID string) (*models.RecoveryRequest, error) {
	if s.store == nil || s.cipher == nil || s.snapshotBuilder == nil {
		return nil, errors.New("recovery_storage_unavailable")
	}
	var request models.RecoveryRequest
	if err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ? AND client_id = ?", requestID, clientID).First(&request).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errors.New("request_not_found")
			}
			return err
		}
		if request.Status != models.RecoveryStatusApproved {
			return errors.New("invalid_request_state")
		}
		if err := tx.Model(&request).Update("status", models.RecoveryStatusExecuting).Error; err != nil {
			return err
		}
		return tx.Model(&models.TamperIncident{}).Where("id = ?", request.IncidentID).Update("status", models.IncidentStatusRecovering).Error
	}); err != nil {
		return nil, err
	}

	if err := s.executeSnapshot(ctx, &request, executorID); err != nil {
		_ = s.db.WithContext(ctx).Model(&request).Updates(map[string]interface{}{
			"status":         models.RecoveryStatusFailedVerification,
			"failure_reason": sanitizeFailure(err),
			"executed_by":    executorID,
		})
		_ = s.db.WithContext(ctx).Model(&models.TamperIncident{}).Where("id = ? AND status = ?", request.IncidentID, models.IncidentStatusRecovering).Update("status", models.IncidentStatusUnderReview).Error
		request.Status = models.RecoveryStatusFailedVerification
		request.FailureReason = sanitizeFailure(err)
		return &request, err
	}
	request.Status = models.RecoveryStatusSucceeded
	return &request, nil
}

func (s *Service) executeSnapshot(ctx context.Context, request *models.RecoveryRequest, executorID string) error {
	payload, info, err := s.store.GetVersion(ctx, request.SnapshotObjectKey, request.SnapshotVersionID)
	if err != nil {
		return fmt.Errorf("snapshot_read_failed: %w", err)
	}
	if info.VersionID != request.SnapshotVersionID {
		return errors.New("snapshot_version_mismatch")
	}
	plaintext, err := s.cipher.Decrypt(payload)
	if err != nil {
		return fmt.Errorf("snapshot_decrypt_failed: %w", err)
	}
	snapshot, err := snapshotstore.ParseAuditSnapshot(plaintext)
	if err != nil {
		return fmt.Errorf("snapshot_invalid: %w", err)
	}
	if snapshot.LogID != request.TargetLogID || snapshot.ClientID != request.ClientID {
		return errors.New("cross_log_recovery_not_allowed")
	}

	reconstructed := models.AuditLog{
		LogID:                snapshot.LogID,
		ClientID:             snapshot.ClientID,
		Actor:                snapshot.Actor,
		Action:               snapshot.Action,
		Resource:             snapshot.Resource,
		Timestamp:            snapshot.Timestamp,
		DBTimestamp:          snapshot.DBTimestamp,
		SourceSystem:         snapshot.SourceSystem,
		AuthorizationContext: snapshot.AuthorizationContext,
		Metadata:             string(snapshot.Metadata),
		SourceRecordID:       snapshot.SourceRecordID,
	}
	if hasher.GenerateLogHash(&reconstructed) != snapshot.HashValue {
		return errors.New("snapshot_hash_mismatch")
	}
	anchorRoot, err := s.verifyAnchor(ctx, snapshot.HashValue, request.TargetLogID, request.ClientID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var target models.AuditLog
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("log_id = ? AND client_id = ?", request.TargetLogID, request.ClientID).First(&target).Error; err != nil {
			return errors.New("target_log_not_found")
		}
		tamperedPayload, err := json.Marshal(target)
		if err != nil {
			return fmt.Errorf("serialisasi nilai tampered gagal: %w", err)
		}
		tamperedEvidence, err := s.cipher.Encrypt(tamperedPayload)
		if err != nil {
			return fmt.Errorf("enkripsi bukti tampered gagal: %w", err)
		}
		updates := map[string]interface{}{
			"actor":                 reconstructed.Actor,
			"action":                reconstructed.Action,
			"resource":              reconstructed.Resource,
			"timestamp":             reconstructed.Timestamp,
			"db_timestamp":          reconstructed.DBTimestamp,
			"source_system":         reconstructed.SourceSystem,
			"authorization_context": reconstructed.AuthorizationContext,
			"source_record_id":      reconstructed.SourceRecordID,
			"metadata":              reconstructed.Metadata,
			"hash_value":            snapshot.HashValue,
			"merkle_root":           anchorRoot,
			"snapshot_status":       models.SnapshotStatusVerified,
			"snapshot_last_error":   "",
			"status":                "ANCHORED",
		}
		if err := tx.Model(&target).Updates(updates).Error; err != nil {
			return fmt.Errorf("restore audit log gagal: %w", err)
		}
		var restored models.AuditLog
		if err := tx.Where("log_id = ? AND client_id = ?", request.TargetLogID, request.ClientID).First(&restored).Error; err != nil {
			return fmt.Errorf("post-recovery readback gagal: %w", err)
		}
		if actualHash := hasher.GenerateLogHash(&restored); actualHash != snapshot.HashValue {
			return errors.New("post_recovery_hash_mismatch")
		}
		if err := tx.Model(&models.TamperIncident{}).Where("id = ?", request.IncidentID).Updates(map[string]interface{}{
			"tampered_payload": tamperedEvidence,
			"status":           models.IncidentStatusResolved,
			"resolved_at":      now,
		}).Error; err != nil {
			return err
		}

		recoveryLog := models.AuditLog{
			LogID:                uuid.NewString(),
			ClientID:             request.ClientID,
			Actor:                executorID,
			Action:               "RECOVERY",
			Resource:             restored.Resource,
			Timestamp:            now,
			SourceSystem:         "AuditChain Gateway",
			AuthorizationContext: "recovery_request:" + request.ID,
			Metadata:             fmt.Sprintf(`{"event":"RECOVERY","request_id":%q,"incident_id":%q,"target_log_id":%q,"selected_log_id":%q,"before_hash":%q,"after_hash":%q}`, request.ID, request.IncidentID, request.TargetLogID, request.SelectedLogID, request.BeforeHash, snapshot.HashValue),
			Status:               "HASHED",
			SnapshotStatus:       models.SnapshotStatusPending,
		}
		recoveryLog.HashValue = hasher.GenerateLogHash(&recoveryLog)
		if err := tx.Create(&recoveryLog).Error; err != nil {
			return fmt.Errorf("audit event recovery gagal: %w", err)
		}
		if s.snapshotBuilder != nil {
			recoveryOutbox, err := s.snapshotBuilder.Build(recoveryLog)
			if err != nil {
				return fmt.Errorf("snapshot audit event recovery gagal: %w", err)
			}
			if err := tx.Create(recoveryOutbox).Error; err != nil {
				return fmt.Errorf("snapshot outbox event recovery gagal: %w", err)
			}
		}
		if err := tx.Model(&models.RecoveryRequest{}).Where("id = ?", request.ID).Updates(map[string]interface{}{
			"status":      models.RecoveryStatusSucceeded,
			"after_hash":  snapshot.HashValue,
			"executed_at": now,
			"executed_by": executorID,
		}).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *Service) verifyAnchor(ctx context.Context, hash, logID, clientID string) (string, error) {
	var logRow models.AuditLog
	if err := s.db.WithContext(ctx).Where("log_id = ? AND client_id = ?", logID, clientID).First(&logRow).Error; err != nil {
		return "", errors.New("target_log_not_found")
	}
	if logRow.BlockchainTxID == nil || *logRow.BlockchainTxID == "" {
		return "", errors.New("anchor_missing")
	}
	var proofs []models.MerkleProof
	if err := s.db.WithContext(ctx).Where("transaction_hash = ?", hash).Order("tree_level ASC").Find(&proofs).Error; err != nil {
		return "", err
	}
	proofData := make([]crypto.MerkleProofData, 0, len(proofs))
	for _, proof := range proofs {
		proofData = append(proofData, crypto.MerkleProofData{SiblingHash: proof.SiblingHash, IsLeft: proof.IsLeft, TreeLevel: proof.TreeLevel})
	}
	if s.fabric == nil {
		return "", errors.New("fabric_unavailable")
	}
	raw, err := s.fabric.GetAnchorFromLedger(*logRow.BlockchainTxID)
	if err != nil {
		return "", fmt.Errorf("fabric_read_failed: %w", err)
	}
	var anchor struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(raw), &anchor); err != nil {
		return "", fmt.Errorf("fabric_anchor_invalid: %w", err)
	}
	reconstructedRoot := crypto.ReconstructMerkleRoot(hash, proofData)
	if reconstructedRoot != anchor.MerkleRoot {
		return "", errors.New("merkle_proof_invalid")
	}
	return anchor.MerkleRoot, nil
}

func sanitizeFailure(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 1000 {
		message = message[:1000]
	}
	return message
}
