package kafkaconsumer

import (
	"context"
	"encoding/json"
	"fmt"

	"go-blockchain-api/internal/models"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

type KafkaVerifier struct {
	DB *gorm.DB
}

type KafkaVerifyResult struct {
	IsValid    bool   `json:"is_valid"`
	Message    string `json:"message"`
	KafkaHash  string `json:"kafka_hash"`
	StoredHash string `json:"stored_hash"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
}

// VerifyAgainstKafka mengambil message original dari Kafka,
// normalisasi ulang, hash, bandingkan dengan hash di PostgreSQL
func (v *KafkaVerifier) VerifyAgainstKafka(auditLog *models.AuditLog) (*KafkaVerifyResult, error) {
	// Ambil offset yang tersimpan
	var kafkaOffset models.KafkaOffset
	if err := v.DB.Where("log_id = ?", auditLog.LogID).First(&kafkaOffset).Error; err != nil {
		return &KafkaVerifyResult{
			IsValid: true,
			Message: "offset Kafka tidak tersimpan — log mungkin bukan dari Kafka consumer",
		}, nil
	}

	// Ambil ClientKafkaConfig untuk tahu broker
	var kafkaCfg models.ClientKafkaConfig
	if err := v.DB.Where("client_id = ? AND is_active = true", auditLog.ClientID).
		First(&kafkaCfg).Error; err != nil {
		return nil, fmt.Errorf("gagal load kafka config: %w", err)
	}

	// Baca message dari Kafka berdasarkan offset
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaCfg.KafkaBrokers},
		Topic:     kafkaOffset.Topic,
		Partition: int(kafkaOffset.Partition),
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()

	// Seek ke offset yang tersimpan
	if err := r.SetOffset(kafkaOffset.Offset); err != nil {
		return nil, fmt.Errorf("gagal set offset: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10e9) // 10 detik
	defer cancel()

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		return &KafkaVerifyResult{
			IsValid:   false,
			Message:   fmt.Sprintf("🚨 Gagal membaca message dari Kafka offset=%d: %v", kafkaOffset.Offset, err),
			Topic:     kafkaOffset.Topic,
			Partition: kafkaOffset.Partition,
			Offset:    kafkaOffset.Offset,
		}, nil
	}

	// Parse message
	var rawMap map[string]interface{}
	if err := json.Unmarshal(msg.Value, &rawMap); err != nil {
		return nil, fmt.Errorf("gagal parse message Kafka: %w", err)
	}

	var payload DebeziumOracleMessage
	if innerPayload, exists := rawMap["payload"]; exists {
		if innerMap, ok := innerPayload.(map[string]interface{}); ok {
			payload = innerMap
		}
	}
	if payload == nil {
		payload = rawMap
	}

	// Normalisasi ulang — sama persis dengan saat pertama diproses
	op, _ := payload["__op"].(string)
	if op == "" {
		op, _ = payload["op"].(string)
	}
	tableName, _ := payload["__table"].(string)
	userName, _ := payload["__user_name"].(string)
	tsMs, _ := payload["__ts_ms"].(float64)

	action := opToAction(op)
	resourceID := findPrimaryKey(payload, kafkaCfg.PKField)
	resource := fmt.Sprintf("%s:%s", tableName, resourceID)

	actor := userName
	if actor == "" {
		actor = "simrs-system"
	}

	var timestamp = auditLog.Timestamp // gunakan timestamp yang sama
	_ = tsMs

	metadata := extractMetadata(payload)
	metaBytes, _ := json.Marshal(metadata)

	// Rekonstruksi AuditLog untuk hashing — tanpa PreviousHash
	reconstructed := &models.AuditLog{
		LogID:                auditLog.LogID,
		Actor:                actor,
		Action:               action,
		Resource:             resource,
		Timestamp:            timestamp,
		SourceSystem:         auditLog.SourceSystem,
		AuthorizationContext: auditLog.AuthorizationContext,
		Metadata:             string(metaBytes),
	}

	kafkaHash := generateLogHash(reconstructed)

	if kafkaHash != auditLog.HashValue {
		return &KafkaVerifyResult{
			IsValid:    false,
			Message:    "🚨 MISMATCH: Hash dari Kafka berbeda dengan hash di database — data mungkin dimanipulasi setelah masuk ke Gateway",
			KafkaHash:  kafkaHash,
			StoredHash: auditLog.HashValue,
			Topic:      kafkaOffset.Topic,
			Partition:  kafkaOffset.Partition,
			Offset:     kafkaOffset.Offset,
		}, nil
	}

	return &KafkaVerifyResult{
		IsValid:    true,
		Message:    "✅ Kafka verified: data konsisten antara Kafka dan database Gateway",
		KafkaHash:  kafkaHash,
		StoredHash: auditLog.HashValue,
		Topic:      kafkaOffset.Topic,
		Partition:  kafkaOffset.Partition,
		Offset:     kafkaOffset.Offset,
	}, nil
}
