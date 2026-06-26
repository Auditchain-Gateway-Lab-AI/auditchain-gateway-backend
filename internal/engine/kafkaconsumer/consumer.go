package kafkaconsumer

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/crypto"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// DebeziumOracleMessage adalah struktur message dari Kafka
// setelah unwrap ExtractNewRecordState
type DebeziumOracleMessage map[string]interface{}

type Engine struct {
	DB *gorm.DB
}

// StartConsumers memulai consumer untuk semua klien yang punya ClientKafkaConfig aktif
func (e *Engine) StartConsumers(ctx context.Context) error {
	var configs []models.ClientKafkaConfig
	if err := e.DB.Where("is_active = true").Find(&configs).Error; err != nil {
		return fmt.Errorf("gagal load kafka configs: %w", err)
	}

	if len(configs) == 0 {
		log.Println("⚠️  [KafkaConsumer] Tidak ada klien dengan Kafka config aktif")
		return nil
	}

	for _, cfg := range configs {
		go e.startClientConsumer(ctx, cfg)
	}

	return nil
}

// startClientConsumer consume topic untuk satu klien
func (e *Engine) startClientConsumer(ctx context.Context, cfg models.ClientKafkaConfig) {
	log.Printf("🎧 [KafkaConsumer] Memulai consumer klien=%s prefix=%s", cfg.ClientID, cfg.TopicPrefix)

	for {
		select {
		case <-ctx.Done():
			log.Printf("🛑 [KafkaConsumer] Consumer klien=%s berhenti", cfg.ClientID)
			return
		default:
			if err := e.discoverAndConsume(ctx, cfg); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("⚠️  [KafkaConsumer] Error klien=%s: %v, retry 5s...", cfg.ClientID, err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

// discoverAndConsume discover topic lalu consume
func (e *Engine) discoverAndConsume(ctx context.Context, cfg models.ClientKafkaConfig) error {
	conn, err := kafka.Dial("tcp", cfg.KafkaBrokers)
	if err != nil {
		return fmt.Errorf("gagal konek Kafka: %w", err)
	}
	partitions, err := conn.ReadPartitions()
	conn.Close()
	if err != nil {
		return fmt.Errorf("gagal baca partisi: %w", err)
	}

	topicSet := make(map[string]struct{})
	for _, p := range partitions {
		if strings.HasPrefix(p.Topic, cfg.TopicPrefix) &&
			!strings.Contains(p.Topic, "schema_history") &&
			!strings.Contains(p.Topic, "heartbeat") {
			topicSet[p.Topic] = struct{}{}
		}
	}

	if len(topicSet) == 0 {
		log.Printf("⚠️  [KafkaConsumer] Belum ada topic prefix=%s", cfg.TopicPrefix)
		time.Sleep(10 * time.Second)
		return nil
	}

	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}

	log.Printf("📋 [KafkaConsumer] klien=%s ditemukan %d topic: %v", cfg.ClientID, len(topics), topics)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.KafkaBrokers},
		GroupID:        fmt.Sprintf("auditchain-gateway-%s", cfg.ClientID),
		GroupTopics:    topics,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})
	defer reader.Close()

	log.Printf("✅ [KafkaConsumer] klien=%s siap consume %d topic", cfg.ClientID, len(topics))

	// ─── KUNCI FIX: re-discovery setiap 60 detik ───
	rediscoverTicker := time.NewTicker(60 * time.Second)
	defer rediscoverTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-rediscoverTicker.C:
			// Cek apakah ada topic baru
			newTopics := e.discoverTopics(cfg)
			if len(newTopics) != len(topics) {
				log.Printf("🔄 [KafkaConsumer] klien=%s topic baru terdeteksi (%d→%d), restart consumer...",
					cfg.ClientID, len(topics), len(newTopics))
				// Return nil → startClientConsumer akan loop ulang → discover ulang
				return nil
			}

		default:
			fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			msg, err := reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// Timeout biasa — lanjut loop untuk cek ticker
				continue
			}

			if err := e.processMessage(msg, cfg); err != nil {
				log.Printf("⚠️  [KafkaConsumer] Gagal proses message topic=%s offset=%d: %v",
					msg.Topic, msg.Offset, err)
			}

			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("⚠️  [KafkaConsumer] Gagal commit offset: %v", err)
			}
		}
	}
}

// discoverTopics — helper untuk cek daftar topic terkini
func (e *Engine) discoverTopics(cfg models.ClientKafkaConfig) []string {
	conn, err := kafka.Dial("tcp", cfg.KafkaBrokers)
	if err != nil {
		return nil
	}
	partitions, err := conn.ReadPartitions()
	conn.Close()
	if err != nil {
		return nil
	}

	topicSet := make(map[string]struct{})
	for _, p := range partitions {
		if strings.HasPrefix(p.Topic, cfg.TopicPrefix) &&
			!strings.Contains(p.Topic, "schema_history") &&
			!strings.Contains(p.Topic, "heartbeat") {
			topicSet[p.Topic] = struct{}{}
		}
	}

	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}
	return topics
}

// processMessage memproses satu message Kafka menjadi AuditLog
func (e *Engine) processMessage(msg kafka.Message, cfg models.ClientKafkaConfig) error {
	if len(msg.Value) == 0 {
		return nil
	}

	var payload DebeziumOracleMessage
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return fmt.Errorf("gagal parse JSON: %w", err)
	}

	// Ambil metadata dari field yang di-inject oleh unwrap transform
	op, _ := payload["__op"].(string)
	if op == "" {
		op, _ = payload["op"].(string)
	}
	if op == "r" {
		return nil // Skip snapshot read
	}

	tableName, _ := payload["__table"].(string)
	if tableName == "" {
		tableName, _ = payload["__table"].(string)
	}

	userName, _ := payload["__user_name"].(string)
	tsMs, _ := payload["__ts_ms"].(float64)

	if tableName == "" {
		return nil
	}

	// Tentukan action
	action := opToAction(op)

	// Cari primary key
	pkField := cfg.PKField
	if pkField == "" {
		pkField = "ID"
	}
	resourceID := findPrimaryKey(payload, pkField)
	resource := fmt.Sprintf("%s:%s", tableName, resourceID)

	// Actor
	actor := userName
	if actor == "" {
		actor = "simrs-system"
	}

	// Timestamp
	var timestamp time.Time
	if tsMs > 0 {
		timestamp = time.UnixMilli(int64(tsMs))
	} else {
		timestamp = time.Now()
	}

	// Metadata — semua field non-sistem
	metadata := extractMetadata(payload)
	metaBytes, _ := json.Marshal(metadata)

	var metaMap interface{}
	json.Unmarshal(metaBytes, &metaMap)
	canonicalMetaBytes, _ := json.Marshal(metaMap)
	canonicalMeta := string(canonicalMetaBytes)

	// Cek duplikasi — skip jika log dengan resource+timestamp sudah ada
	var existing models.AuditLog
	if err := e.DB.Where(
		"resource = ? AND timestamp = ? AND client_id = ?",
		resource, timestamp, cfg.ClientID,
	).First(&existing).Error; err == nil {
		return nil // sudah ada
	}

	// Hitung previous hash
	var lastLog models.AuditLog
	var prevHash string
	if err := e.DB.Where("client_id = ? AND status IN ?",
		cfg.ClientID, []string{"HASHED", "ANCHORED"},
	).Order("timestamp desc").First(&lastLog).Error; err == nil {
		prevHash = lastLog.HashValue
	} else {
		prevHash = "GENESIS_00000000000000000000000000000000000000000000000000000000"
	}

	// Buat AuditLog
	auditLog := &models.AuditLog{
		LogID:        generateLogID(),
		ClientID:     cfg.ClientID,
		Actor:        actor,
		Action:       action,
		Resource:     resource,
		Timestamp:    timestamp,
		SourceSystem: cfg.SourceSystem,
		Metadata:     canonicalMeta,
		Status:       "RECEIVED",
		PreviousHash: prevHash,
	}

	// Hitung hash
	auditLog.HashValue = generateLogHash(auditLog, prevHash)
	auditLog.Status = "HASHED"

	// Simpan ke DB
	if err := e.DB.Create(auditLog).Error; err != nil {
		return fmt.Errorf("gagal simpan audit log: %w", err)
	}

	// Simpan Kafka offset untuk verifikasi Lapis 3
	kafkaOffset := &models.KafkaOffset{
		LogID:     auditLog.LogID,
		Topic:     msg.Topic,
		Partition: int32(msg.Partition),
		Offset:    msg.Offset,
	}
	if err := e.DB.Create(kafkaOffset).Error; err != nil {
		log.Printf("⚠️  [KafkaConsumer] Gagal simpan offset untuk log %s: %v", auditLog.LogID, err)
	}

	log.Printf("✅ [KafkaConsumer] Tersimpan → action=%-8s resource=%s client=%s",
		action, resource, cfg.ClientID)

	return nil
}

func opToAction(op string) string {
	switch op {
	case "c":
		return "INSERT"
	case "u":
		return "UPDATE"
	case "d":
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// findPrimaryKey — tambah handling untuk Debezium Oracle numeric/bytes type
func findPrimaryKey(payload map[string]interface{}, pkField string) string {
	candidates := []string{pkField, "ID", "id", "ogc_fid", "_id", "fid"}

	for _, key := range candidates {
		val, ok := payload[key]
		if !ok || val == nil {
			continue
		}
		return extractScalarValue(val)
	}

	if rowID, ok := payload["__row_id"].(string); ok && rowID != "" {
		return rowID
	}
	return "unknown"
}

// extractScalarValue menangani tipe Debezium Oracle:
// - string/number biasa → langsung
// - {"scale": N, "value": "<base64>"} → decode base64 → integer
// - {"value": "<base64>"} → decode base64 → bytes/string
func extractScalarValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		// Jika bilangan bulat, hapus desimal
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case map[string]interface{}:
		// Format Debezium untuk Oracle NUMBER/DECIMAL/RAW
		encoded, hasValue := v["value"].(string)
		if !hasValue || encoded == "" {
			return fmt.Sprintf("%v", v)
		}

		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			// Bukan base64 valid — kembalikan apa adanya
			return encoded
		}

		scale, hasScale := v["scale"].(float64)
		if hasScale && scale == 0 && len(decoded) <= 8 {
			// Oracle NUMBER dengan scale=0 → integer
			// Decode big-endian bytes ke int64
			var result int64
			for _, b := range decoded {
				result = result*256 + int64(b)
			}
			return fmt.Sprintf("%d", result)
		}

		// Fallback: coba sebagai string
		s := strings.TrimRight(string(decoded), "\x00")
		if s != "" && isPrintable(s) {
			return s
		}
		// Last resort: hex
		return hex.EncodeToString(decoded)

	default:
		return fmt.Sprintf("%v", v)
	}
}

func isPrintable(s string) bool {
	for _, r := range s {
		if r < 32 || r > 126 {
			return false
		}
	}
	return true
}

func extractMetadata(payload map[string]interface{}) map[string]interface{} {
	skip := map[string]bool{
		"__op": true, "__table": true, "__db": true, "__schema": true,
		"__ts_ms": true, "__deleted": true, "__user_name": true,
		"__scn": true, "__tx_id": true, "__row_id": true,
		"op": true, "table": true, "db": true, "schema": true,
		"ts_ms": true, "deleted": true, "user_name": true,
	}

	meta := make(map[string]interface{})
	for k, v := range payload {
		if skip[k] {
			continue
		}
		// Normalisasi semua nilai agar tidak ada map[...] di metadata
		meta[k] = normalizeFieldValue(v)
	}
	return meta
}

// normalizeFieldValue flatten nilai Debezium Oracle menjadi scalar
func normalizeFieldValue(val interface{}) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		// Debezium wrapped value — ekstrak ke scalar
		if _, hasValue := v["value"]; hasValue {
			return extractScalarValue(v)
		}
		// Object biasa — rekursi
		result := make(map[string]interface{})
		for k, inner := range v {
			result[k] = normalizeFieldValue(inner)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = normalizeFieldValue(item)
		}
		return result
	default:
		return val
	}
}

func generateLogID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func generateLogHash(auditLog *models.AuditLog, prevHash string) string {
	authCtx := auditLog.AuthorizationContext
	if authCtx == "null" || authCtx == "<nil>" {
		authCtx = ""
	}

	contextString := fmt.Sprintf("%s|%s|%s|%s|%d|%s|%s|%s|%s",
		auditLog.LogID,
		auditLog.Actor,
		auditLog.Action,
		auditLog.Resource,
		auditLog.Timestamp.UnixMicro(),
		auditLog.SourceSystem,
		authCtx,
		prevHash,
		auditLog.Metadata,
	)
	return crypto.GenerateSHA3_256(contextString)
}
