package hasher

import (
	"encoding/json"
	"strconv"
	"strings"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"
)

const RecoveryEventSchemaVersion = 1

// GenerateRecoveryEventHash hashes only immutable recovery semantics. Pipeline
// fields (snapshot references, Merkle root, Fabric tx id and integrity cache)
// are intentionally excluded because they are populated asynchronously.
func GenerateRecoveryEventHash(event *models.RecoveryEvent) string {
	if event == nil {
		return ""
	}
	metadata := canonicalRecoveryJSON(event.RecoveredMetadata)
	targetTimestamp := int64(0)
	if event.TargetTimestamp != nil {
		targetTimestamp = event.TargetTimestamp.UnixMicro()
	}
	sourceSystem := event.SourceSystem
	if strings.TrimSpace(sourceSystem) == "" {
		sourceSystem = event.TargetSourceSystem
	}

	canonical := strings.Join([]string{
		strconv.Itoa(RecoveryEventSchemaVersion),
		event.ID,
		event.ClientID,
		event.RequestID,
		event.IncidentID,
		event.TargetLogID,
		event.SelectedLogID,
		event.EventType,
		event.ResultStatus,
		event.Resource,
		event.TargetActor,
		event.TargetAction,
		strconv.FormatInt(targetTimestamp, 10),
		sourceSystem,
		event.TargetAuthorization,
		event.TargetSourceRecordID,
		metadata,
		event.ExecutorSystem,
		event.ExecutedBy,
		event.Reason,
		event.BeforeHash,
		event.AfterHash,
		event.SourceSnapshotObjectKey,
		event.SourceSnapshotVersionID,
		event.SourceSnapshotChecksum,
		event.SourceSnapshotPlainHash,
		event.SourceAnchorID,
		event.SourceExpectedMerkleRoot,
		event.FailureCode,
		event.FailureReason,
		strconv.FormatInt(event.ExecutedAt.UnixMicro(), 10),
	}, "|")
	return crypto.GenerateSHA3_256(canonical)
}

func canonicalRecoveryJSON(raw string) string {
	if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "null" {
		return "null"
	}
	var value interface{}
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return string(normalized)
}
