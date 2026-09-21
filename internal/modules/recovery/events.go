package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"

	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"
	"go-blockchain-api/internal/storage/snapshotstore"
	"go-blockchain-api/pkg/crypto"
)

type RecoveryEventView struct {
	models.RecoveryEvent
	// SourceSystem is the client/source label expected by the dashboard. The
	// persisted model calls it TargetSourceSystem to distinguish it from the
	// component that performed the recovery write.
	SourceSystem string `json:"source_system"`
	StorageKind  string `json:"storage_kind"`
	Legacy       bool   `json:"legacy"`
}

type RecoveryEventPage struct {
	Data       []RecoveryEventView `json:"data"`
	Page       int                 `json:"page"`
	PageSize   int                 `json:"page_size"`
	TotalItems int64               `json:"total_items"`
	TotalPages int                 `json:"total_pages"`
}

func (s *Service) ListEvents(ctx context.Context, clientID, resultStatus, resource string, page, pageSize int, includeLegacy bool) (*RecoveryEventPage, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}
	var events []models.RecoveryEvent
	query := s.db.WithContext(ctx).Where("client_id = ?", clientID).Order("executed_at DESC")
	if strings.TrimSpace(resultStatus) != "" {
		query = query.Where("result_status = ?", strings.ToUpper(strings.TrimSpace(resultStatus)))
	}
	if strings.TrimSpace(resource) != "" {
		query = query.Where("resource = ?", resource)
	}
	if err := query.Find(&events).Error; err != nil {
		return nil, err
	}
	views := make([]RecoveryEventView, 0, len(events))
	for _, event := range events {
		views = append(views, RecoveryEventView{RecoveryEvent: event, SourceSystem: recoverySourceSystem(event), StorageKind: "RECOVERY_EVENT"})
	}
	if includeLegacy {
		legacy, err := s.legacyEvents(ctx, clientID, resource, resultStatus)
		if err != nil {
			return nil, err
		}
		views = append(views, legacy...)
	}
	sort.SliceStable(views, func(i, j int) bool {
		return views[i].ExecutedAt.After(views[j].ExecutedAt)
	})
	total := int64(len(views))
	offset := (page - 1) * pageSize
	if offset > len(views) {
		offset = len(views)
	}
	end := offset + pageSize
	if end > len(views) {
		end = len(views)
	}
	pages := int((total + int64(pageSize) - 1) / int64(pageSize))
	return &RecoveryEventPage{Data: views[offset:end], Page: page, PageSize: pageSize, TotalItems: total, TotalPages: pages}, nil
}

func (s *Service) GetEvent(ctx context.Context, clientID, eventID string) (*RecoveryEventView, error) {
	var event models.RecoveryEvent
	if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", eventID, clientID).First(&event).Error; err == nil {
		return &RecoveryEventView{RecoveryEvent: event, SourceSystem: recoverySourceSystem(event), StorageKind: "RECOVERY_EVENT"}, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	legacy, err := s.legacyEvents(ctx, clientID, "", "")
	if err != nil {
		return nil, err
	}
	for i := range legacy {
		if legacy[i].ID == eventID {
			return &legacy[i], nil
		}
	}
	return nil, errors.New("recovery_event_not_found")
}

func (s *Service) legacyEvents(ctx context.Context, clientID, resource, resultStatus string) ([]RecoveryEventView, error) {
	var logs []models.AuditLog
	query := s.db.WithContext(ctx).Where("client_id = ? AND COALESCE(UPPER(TRIM(action)), '') = 'RECOVERY'", clientID)
	if strings.TrimSpace(resource) != "" {
		query = query.Where("resource = ?", resource)
	}
	if strings.TrimSpace(resultStatus) != "" && !strings.EqualFold(strings.TrimSpace(resultStatus), models.RecoveryResultSucceeded) {
		return []RecoveryEventView{}, nil
	}
	if err := query.Order("timestamp DESC").Find(&logs).Error; err != nil {
		return nil, err
	}
	views := make([]RecoveryEventView, 0, len(logs))
	for _, logRow := range logs {
		view := RecoveryEventView{
			StorageKind:  "LEGACY_AUDIT_LOG",
			Legacy:       true,
			SourceSystem: logRow.SourceSystem,
			RecoveryEvent: models.RecoveryEvent{
				ID:                    logRow.LogID,
				ClientID:              logRow.ClientID,
				EventType:             models.RecoveryEventTypeExecution,
				ResultStatus:          models.RecoveryResultSucceeded,
				Resource:              logRow.Resource,
				TargetActor:           logRow.Actor,
				SourceSystem:          logRow.SourceSystem,
				TargetSourceSystem:    logRow.SourceSystem,
				RecoveredMetadata:     logRow.Metadata,
				EventHash:             logRow.HashValue,
				ExecutedAt:            logRow.Timestamp,
				PipelineStatus:        logRow.Status,
				MerkleRoot:            logRow.MerkleRoot,
				BlockchainTxID:        logRow.BlockchainTxID,
				SnapshotStatus:        logRow.SnapshotStatus,
				SnapshotObjectKey:     logRow.SnapshotObjectKey,
				SnapshotVersionID:     logRow.SnapshotVersionID,
				SnapshotChecksum:      logRow.SnapshotChecksum,
				SnapshotPlaintextHash: logRow.SnapshotPlaintextHash,
				SnapshotVerifiedAt:    logRow.SnapshotVerifiedAt,
				IntegrityStatus:       logRow.IntegrityStatus,
			},
		}
		requestID := recoveryRequestIDFromContext(logRow.AuthorizationContext)
		if requestID != "" {
			view.RequestID = requestID
			var request models.RecoveryRequest
			if err := s.db.WithContext(ctx).Where("id = ? AND client_id = ?", requestID, clientID).First(&request).Error; err == nil {
				view.IncidentID = request.IncidentID
				view.TargetLogID = request.TargetLogID
				view.SelectedLogID = request.SelectedLogID
				view.ExecutedBy = request.ExecutedBy
				view.BeforeHash = request.BeforeHash
				view.AfterHash = request.AfterHash
				view.SourceSnapshotObjectKey = request.SnapshotObjectKey
				view.SourceSnapshotVersionID = request.SnapshotVersionID
				view.SourceSnapshotChecksum = request.SnapshotChecksum
				view.SourceSnapshotPlainHash = request.SnapshotPlaintextHash
				view.SourceAnchorID = request.AnchorID
				view.SourceExpectedMerkleRoot = request.ExpectedMerkleRoot
			}
		}
		view.ExecutorSystem = "AuditChain Gateway"
		views = append(views, view)
	}
	return views, nil
}

func recoveryRequestIDFromContext(value string) string {
	const prefix = "recovery_request:"
	if strings.HasPrefix(strings.TrimSpace(value), prefix) {
		return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(value), prefix))
	}
	return ""
}

func recoverySourceSystem(event models.RecoveryEvent) string {
	if strings.TrimSpace(event.SourceSystem) != "" {
		return event.SourceSystem
	}
	return event.TargetSourceSystem
}

func (s *Service) VerifyEvent(ctx context.Context, clientID, eventID string) (map[string]interface{}, error) {
	view, err := s.GetEvent(ctx, clientID, eventID)
	if err != nil {
		return nil, err
	}
	if view.Legacy {
		return map[string]interface{}{
			"status": "VALID", "event_id": eventID, "storage_kind": view.StorageKind,
			"pipeline_status": view.PipelineStatus, "snapshot_status": view.SnapshotStatus,
			"integrity_status": view.IntegrityStatus,
		}, nil
	}
	event := view.RecoveryEvent
	if hasher.GenerateRecoveryEventHash(&event) != event.EventHash {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, "recovery event hash mismatch", false)
	}
	if event.SnapshotStatus != models.SnapshotStatusVerified || event.SnapshotObjectKey == "" || event.SnapshotVersionID == "" {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusPending, "recovery event snapshot belum terverifikasi", false)
	}
	if event.PipelineStatus != models.RecoveryPipelineAnchored || strings.TrimSpace(event.MerkleRoot) == "" {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusPending, "recovery event belum anchored", false)
	}
	if s.store == nil || s.cipher == nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusUnreachable, "recovery event storage unavailable", false)
	}
	payload, info, err := s.store.GetVersion(ctx, event.SnapshotObjectKey, event.SnapshotVersionID)
	if err != nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusUnreachable, err.Error(), false)
	}
	if err := snapshotstore.ValidateObjectInfo(info, event.SnapshotObjectKey, event.SnapshotVersionID, event.SnapshotChecksum); err != nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, err.Error(), false)
	}
	plaintext, err := s.cipher.Decrypt(payload)
	if err != nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, err.Error(), false)
	}
	snapshot, err := snapshotstore.ParseRecoveryEventSnapshot(plaintext)
	if err != nil || snapshot.EventID != event.ID || snapshot.ClientID != event.ClientID || snapshot.EventHash != event.EventHash {
		if err == nil {
			err = errors.New("recovery event snapshot identity mismatch")
		}
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, err.Error(), false)
	}
	snapshotEvent := recoveryEventFromSnapshot(snapshot)
	if hasher.GenerateRecoveryEventHash(&snapshotEvent) != snapshot.EventHash {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, "recovery event snapshot hash mismatch", false)
	}
	var proofs []models.MerkleProof
	if err := s.db.WithContext(ctx).Where("transaction_hash = ? AND merkle_root = ?", event.EventHash, event.MerkleRoot).Order("tree_level ASC").Find(&proofs).Error; err != nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusUnreachable, err.Error(), false)
	}
	proofData := make([]crypto.MerkleProofData, 0, len(proofs))
	for _, proof := range proofs {
		proofData = append(proofData, crypto.MerkleProofData{SiblingHash: proof.SiblingHash, IsLeft: proof.IsLeft, TreeLevel: proof.TreeLevel})
	}
	if crypto.ReconstructMerkleRoot(event.EventHash, proofData) != event.MerkleRoot {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, "recovery event merkle proof mismatch", false)
	}
	if s.fabric == nil || event.BlockchainTxID == nil || strings.TrimSpace(*event.BlockchainTxID) == "" {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusUnreachable, "recovery event anchor unavailable", false)
	}
	raw, err := s.fabric.GetAnchorFromLedger(*event.BlockchainTxID)
	if err != nil {
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusUnreachable, err.Error(), false)
	}
	var anchor struct {
		MerkleRoot string `json:"merkle_root"`
	}
	if err := json.Unmarshal([]byte(raw), &anchor); err != nil || anchor.MerkleRoot != event.MerkleRoot {
		if err == nil {
			err = errors.New("recovery event merkle root mismatch")
		}
		return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusTampered, err.Error(), false)
	}
	return s.updateEventIntegrity(ctx, &event, models.IntegrityStatusValid, "", true)
}

func recoveryEventFromSnapshot(snapshot snapshotstore.RecoveryEventSnapshot) models.RecoveryEvent {
	return models.RecoveryEvent{
		ID: snapshot.EventID, ClientID: snapshot.ClientID, RequestID: snapshot.RequestID,
		IncidentID: snapshot.IncidentID, TargetLogID: snapshot.TargetLogID, SelectedLogID: snapshot.SelectedLogID,
		EventType: snapshot.EventType, ResultStatus: snapshot.ResultStatus, Resource: snapshot.Resource,
		TargetActor: snapshot.TargetActor, TargetAction: snapshot.TargetAction, TargetTimestamp: snapshot.TargetTimestamp,
		SourceSystem:       snapshot.SourceSystem,
		TargetSourceSystem: snapshot.TargetSourceSystem, TargetAuthorization: snapshot.TargetAuthorization,
		TargetSourceRecordID: snapshot.TargetSourceRecordID, RecoveredMetadata: string(snapshot.RecoveredMetadata),
		ExecutorSystem: snapshot.ExecutorSystem, ExecutedBy: snapshot.ExecutedBy, Reason: snapshot.Reason,
		BeforeHash: snapshot.BeforeHash, AfterHash: snapshot.AfterHash,
		SourceSnapshotObjectKey: snapshot.SourceSnapshotObjectKey, SourceSnapshotVersionID: snapshot.SourceSnapshotVersionID,
		SourceSnapshotChecksum: snapshot.SourceSnapshotChecksum, SourceSnapshotPlainHash: snapshot.SourceSnapshotPlainHash,
		SourceAnchorID: snapshot.SourceAnchorID, SourceExpectedMerkleRoot: snapshot.SourceExpectedMerkleRoot,
		FailureCode: snapshot.FailureCode, FailureReason: snapshot.FailureReason,
		EventHash: snapshot.EventHash, ExecutedAt: snapshot.ExecutedAt,
	}
}

func (s *Service) updateEventIntegrity(ctx context.Context, event *models.RecoveryEvent, status, message string, valid bool) (map[string]interface{}, error) {
	now := time.Now().UTC()
	updates := map[string]interface{}{"integrity_status": status, "integrity_checked_at": now, "integrity_error": message}
	if err := s.db.WithContext(ctx).Model(&models.RecoveryEvent{}).Where("id = ? AND client_id = ?", event.ID, event.ClientID).Updates(updates).Error; err != nil {
		return nil, err
	}
	result := map[string]interface{}{"status": status, "event_id": event.ID, "storage_kind": "RECOVERY_EVENT", "pipeline_status": event.PipelineStatus, "snapshot_status": event.SnapshotStatus, "integrity_status": status}
	if !valid {
		return result, fmt.Errorf("recovery_event_%s", strings.ToLower(status))
	}
	return result, nil
}
