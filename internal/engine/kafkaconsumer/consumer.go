package kafkaconsumer

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"go-blockchain-api/internal/engine/hasher"
	"go-blockchain-api/internal/models"

	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

// DebeziumOracleMessage adalah struktur message dari Kafka
// setelah unwrap ExtractNewRecordState
type DebeziumOracleMessage map[string]interface{}

type clientMapping struct {
	ActorField         string
	FallbackActorField string
	ActionField        string
	ResourceField      string
}

type Engine struct {
	DB *gorm.DB

	sourceSystemCache sync.Map
	mappingCache      sync.Map
	actorCache        sync.Map // key: "clientID:actorID" → resolved name

	mu        sync.Mutex
	consumers map[string]*runningConsumer // key: ClientID
}

func (e *Engine) resolveClientMapping(clientID string) clientMapping {
	if cached, ok := e.mappingCache.Load(clientID); ok {
		return cached.(clientMapping)
	}

	var m clientMapping
	if err := e.DB.Table("clients").
		Select("actor_field, fallback_actor_field, action_field, resource_field").
		Where("id = ?", clientID).
		Scan(&m).Error; err == nil {
		e.mappingCache.Store(clientID, m)
	}
	return m
}

type runningConsumer struct {
	cancel      context.CancelFunc
	fingerprint string
}

func (e *Engine) resolveSourceSystem(cfg models.ClientKafkaConfig) string {
	if cached, ok := e.sourceSystemCache.Load(cfg.ClientID); ok {
		return cached.(string)
	}

	resolved := cfg.SourceSystem
	var companyName string
	if err := e.DB.Table("clients").
		Select("company_name").
		Where("id = ?", cfg.ClientID).
		Scan(&companyName).Error; err == nil && companyName != "" {
		resolved = companyName
	}

	e.sourceSystemCache.Store(cfg.ClientID, resolved)
	return resolved
}

func configFingerprint(cfg models.ClientKafkaConfig) string {
	return cfg.TopicPrefix + "|" + cfg.KafkaBrokers + "|" + cfg.ActorField + "|" + cfg.PKField
}

// StartConsumers memulai consumer untuk semua klien yang punya ClientKafkaConfig aktif
func (e *Engine) Reconcile(ctx context.Context) error {
	var configs []models.ClientKafkaConfig
	if err := e.DB.Where("is_active = true").Find(&configs).Error; err != nil {
		return fmt.Errorf("gagal load kafka configs: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.consumers == nil {
		e.consumers = make(map[string]*runningConsumer)
	}

	seen := make(map[string]bool, len(configs))

	for _, cfg := range configs {
		seen[cfg.ClientID] = true
		fp := configFingerprint(cfg)

		if existing, running := e.consumers[cfg.ClientID]; running {
			// Hapus cache mapping & source system agar perubahan di admin panel segera terbaca
			e.mappingCache.Delete(cfg.ClientID)
			e.sourceSystemCache.Delete(cfg.ClientID)

			if existing.fingerprint == fp {
				continue // tidak ada perubahan, biarkan goroutine yang sudah jalan
			}
			log.Printf("🔄 [KafkaConsumer] Config klien=%s berubah, restart consumer...", cfg.ClientID)
			existing.cancel()
		}

		consumerCtx, cancel := context.WithCancel(ctx)
		e.consumers[cfg.ClientID] = &runningConsumer{cancel: cancel, fingerprint: fp}
		go e.startClientConsumer(consumerCtx, cfg)
	}

	// Stop consumer untuk klien yang config-nya sudah dihapus/dinonaktifkan
	// sejak siklus reconcile sebelumnya — inilah yang tadinya hilang.
	for clientID, rc := range e.consumers {
		if !seen[clientID] {
			log.Printf("🛑 [KafkaConsumer] Config klien=%s sudah tidak aktif, menghentikan consumer...", clientID)
			rc.cancel()
			delete(e.consumers, clientID)
		}
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
	dialer := getDialer(cfg)
	conn, err := dialer.DialContext(ctx, "tcp", cfg.KafkaBrokers)
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
		// Debug: tampilkan semua topic yang tersedia di broker agar mudah diagnosa mismatch prefix
		allTopics := make([]string, 0)
		for _, p := range partitions {
			found := false
			for _, at := range allTopics {
				if at == p.Topic {
					found = true
					break
				}
			}
			if !found {
				allTopics = append(allTopics, p.Topic)
			}
		}
		log.Printf("⚠️  [KafkaConsumer] Belum ada topic prefix=%s (tersedia di Kafka: %v)", cfg.TopicPrefix, allTopics)
		time.Sleep(30 * time.Second)
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
		StartOffset:    kafka.FirstOffset,
		Dialer:         dialer,
	})
	defer reader.Close()

	log.Printf("✅ [KafkaConsumer] klien=%s siap consume %d topic", cfg.ClientID, len(topics))

	// Re-discovery setiap 60 detik untuk mendeteksi topic baru
	rediscoverTicker := time.NewTicker(60 * time.Second)
	defer rediscoverTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-rediscoverTicker.C:
			newTopics := e.discoverTopics(cfg)
			if len(newTopics) != len(topics) {
				log.Printf("🔄 [KafkaConsumer] klien=%s topic baru terdeteksi (%d→%d), restart consumer...",
					cfg.ClientID, len(topics), len(newTopics))
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
				if err != context.DeadlineExceeded && err != context.Canceled {
					log.Printf("⚠️  [KafkaConsumer] Gagal fetch message klien=%s: %v", cfg.ClientID, err)
					time.Sleep(1 * time.Second)
				}
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

// discoverTopics helper untuk cek daftar topic terkini
func (e *Engine) discoverTopics(cfg models.ClientKafkaConfig) []string {
	dialer := getDialer(cfg)
	conn, err := dialer.Dial("tcp", cfg.KafkaBrokers)
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

	var rawMap map[string]interface{}
	if err := json.Unmarshal(msg.Value, &rawMap); err != nil {
		log.Printf("⚠️  [KafkaConsumer] Gagal decode JSON: %v", err)
		return nil
	}

	// DEBUG: Tampilkan payload raw jika dibutuhkan
	log.Printf("🔍 [KafkaConsumer DEBUG] Topik: %s | Menerima payload: %v", msg.Topic, string(msg.Value))

	var payload DebeziumOracleMessage
	if innerPayload, exists := rawMap["payload"]; exists {
		if innerMap, ok := innerPayload.(map[string]interface{}); ok {
			payload = innerMap
		}
	}
	if payload == nil {
		payload = rawMap
	}

	op, _ := payload["__op"].(string)
	if op == "" {
		op, _ = payload["op"].(string)
	}

	tableName, _ := payload["__table"].(string)
	if tableName == "" {
		tableName, _ = payload["__collection"].(string)
	}
	if tableName == "" {
		tableName, _ = payload["table"].(string)
	}
	if tableName == "" {
		tableName, _ = payload["collection"].(string)
	}

	if tableName == "" {
		// Fallback parse dari topic name: "client_topic_prefix.schema.table"
		parts := strings.Split(msg.Topic, ".")
		if len(parts) > 0 {
			tableName = parts[len(parts)-1]
		}
	}
	if tableName == "" {
		return nil
	}

	var agentCfg models.AgentConfig
	e.DB.Where("client_id = ?", cfg.ClientID).First(&agentCfg)

	// Proses CDC tabel user: jika admin sudah konfigurasi UserTableName, gunakan itu.
	// Jika belum, auto-detect berdasarkan nama tabel (user, account, member, dll)
	isUserTable := false
	if agentCfg.UserTableName != "" {
		// agentCfg.UserTableName bisa berupa "public.users" atau "dbo.users"
		// sedangkan tableName dari Debezium mungkin cuma "users".
		parts := strings.Split(agentCfg.UserTableName, ".")
		baseTarget := parts[len(parts)-1]
		
		isUserTable = strings.EqualFold(tableName, baseTarget) || 
			strings.Contains(agentCfg.UserTableName, tableName) || 
			strings.Contains(tableName, agentCfg.UserTableName)
	} else {
		lowerTable := strings.ToLower(tableName)
		for _, keyword := range []string{"user", "account", "akun", "pengguna", "member", "employee", "karyawan"} {
			if strings.Contains(lowerTable, keyword) {
				isUserTable = true
				break
			}
		}
	}

	// Untuk tabel user, kita WAJIB memproses operasi "r" (snapshot read)
	// agar memori identitas Gateway terisi dengan user yang sudah ada.
	log.Printf("🔎 [DEBUG] tableName=%s, agentCfg.UserTableName=%s, isUserTable=%v", tableName, agentCfg.UserTableName, isUserTable)
	if isUserTable {
		e.processClientUserCDC(payload, cfg, tableName, agentCfg)
	}

	// Untuk tabel non-user, skip operasi "r" (jangan catat snapshot awal sebagai audit log)
	if op == "r" {
		return nil
	}

	userName, _ := payload["__user_name"].(string)
	tsMs, _ := payload["__ts_ms"].(float64)

	mapping := e.resolveClientMapping(cfg.ClientID)

	action := opToAction(op)

	pkField := cfg.PKField
	if pkField == "" {
		pkField = "ID"
	}
	resourceID := findPrimaryKey(payload, pkField)
	resource := fmt.Sprintf("%s:%s", tableName, resourceID)

	actor := userName
	actorFound := false

	if mapping.ActorField != "" {
		if customActor, ok := findFieldInsensitive(payload, mapping.ActorField); ok && customActor != nil {
			actor = extractScalarValue(customActor)
			actorFound = true
		}
	}

	if !actorFound && mapping.FallbackActorField != "" {
		if fallbackActor, ok := findFieldInsensitive(payload, mapping.FallbackActorField); ok && fallbackActor != nil {
			actor = extractScalarValue(fallbackActor)
			actorFound = true
		}
	}

	// Auto-detect common actor columns jika admin tidak mensettingnya
	if !actorFound {
		commonFields := []string{"updated_by", "deleted_by", "modified_by", "created_by", "actor", "user_id", "username", "author", "userid", "updatedby", "createdby"}
		for _, field := range commonFields {
			if autoActor, ok := findFieldInsensitive(payload, field); ok && autoActor != nil {
				actor = extractScalarValue(autoActor)
				actorFound = true
				break
			}
		}
	}

	if !actorFound && actor == "" {
		actor = "Unknown"
	}

	// Resolusi CUID/UUID → Nama Manusia
	// Jika actor terlihat seperti ID acak (Prisma CUID, UUID, dll),
	// coba cari nama aslinya di tabel client_users
	log.Printf("🔎 [DEBUG] Actor SEBELUM resolve: '%s', looksLikeID=%v, table=%s", actor, looksLikeGeneratedID(actor), tableName)
	if actor != "" && actor != "Unknown" && looksLikeGeneratedID(actor) {
		if resolved := e.resolveActorName(cfg.ClientID, actor); resolved != "" {
			log.Printf("✅ [DEBUG] Actor RESOLVED: '%s' → '%s'", actor, resolved)
			actor = resolved
		} else {
			log.Printf("❌ [DEBUG] Actor TIDAK BISA di-resolve: '%s' — client_users kosong atau tidak ditemukan", actor)
		}
	}

	var timestamp time.Time
	if tsMs > 0 {
		timestamp = time.UnixMilli(int64(tsMs))
	} else {
		timestamp = time.Now()
	}

	// Ekstrak dan canonicalize metadata — satu kali marshal, tidak double
	metadata := extractMetadata(payload)
	metaBytes, _ := json.Marshal(metadata)
	canonicalMeta := string(metaBytes) // ← tidak perlu unmarshal+marshal ulang

	// Cek duplikasi
	var existing models.AuditLog
	if err := e.DB.Where(
		"resource = ? AND timestamp = ? AND client_id = ?",
		resource, timestamp, cfg.ClientID,
	).First(&existing).Error; err == nil {
		return nil
	}

	// source_system = Client.CompanyName (diadopsi dari branch testing),
	// fallback ke cfg.SourceSystem jika company_name klien belum diisi.
	sourceSystem := e.resolveSourceSystem(cfg)

	auditLog := &models.AuditLog{
		LogID:        generateLogID(),
		ClientID:     cfg.ClientID,
		Actor:        actor,
		Action:       action,
		Resource:     resource,
		Timestamp:    timestamp,
		SourceSystem: sourceSystem,
		Metadata:     canonicalMeta,
		// AuthorizationContext sengaja dibiarkan "" untuk log dari Kafka
		// — konsisten dengan normalisasi di generateLogHash
		AuthorizationContext: "",
		Status:               "RECEIVED",
	}

	// Hash menggunakan fungsi shared agar canonicalization konsisten
	auditLog.HashValue = hasher.GenerateLogHash(auditLog)
	auditLog.Status = "HASHED"

	if err := e.DB.Create(auditLog).Error; err != nil {
		return fmt.Errorf("gagal simpan audit log: %w", err)
	}

	// Update counter cache di client_tables
	tableNameOnly := tableName
	if tableNameOnly == "" {
		if strings.Contains(resource, ":") {
			tableNameOnly = strings.Split(resource, ":")[0]
		} else {
			tableNameOnly = resource
		}
	}
	upsertQuery := `
		INSERT INTO client_tables (client_id, table_name, row_count, last_action, last_actor, last_updated_at, created_at)
		VALUES (?, ?, CASE WHEN ? = 'INSERT' THEN 1 ELSE 0 END, ?, ?, ?, NOW())
		ON CONFLICT (client_id, table_name) DO UPDATE SET
			row_count = CASE 
				WHEN EXCLUDED.last_action = 'INSERT' THEN client_tables.row_count + 1
				WHEN EXCLUDED.last_action = 'DELETE' THEN GREATEST(client_tables.row_count - 1, 0)
				ELSE client_tables.row_count 
			END,
			last_action = EXCLUDED.last_action,
			last_actor = EXCLUDED.last_actor,
			last_updated_at = EXCLUDED.last_updated_at
	`
	_ = e.DB.Exec(upsertQuery, cfg.ClientID, tableNameOnly, action, action, actor, timestamp)

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

	log.Printf("✅ [KafkaConsumer] Tersimpan → engine=%-10s action=%-8s resource=%s client=%s source_system=%s",
		cfg.DBEngine, action, resource, cfg.ClientID, sourceSystem)

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

func findPrimaryKey(payload map[string]interface{}, pkField string) string {
	candidates := []string{pkField, "ID", "id", "ogc_fid", "_id", "fid"}

	for _, key := range candidates {
		val, ok := findFieldInsensitive(payload, key)
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

func findFieldInsensitive(payload map[string]interface{}, field string) (interface{}, bool) {
	if field == "" {
		return nil, false
	}

	if val, ok := payload[field]; ok {
		return val, true
	}

	lowerField := strings.ToLower(field)
	for k, v := range payload {
		if strings.ToLower(k) == lowerField {
			return v, true
		}
	}

	if after, ok := payload["after"].(map[string]interface{}); ok {
		if val, ok := after[field]; ok {
			return val, true
		}
		for k, v := range after {
			if strings.ToLower(k) == lowerField {
				return v, true
			}
		}
	}

	if before, ok := payload["before"].(map[string]interface{}); ok {
		if val, ok := before[field]; ok {
			return val, true
		}
		for k, v := range before {
			if strings.ToLower(k) == lowerField {
				return v, true
			}
		}
	}

	return nil, false
}

func extractScalarValue(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%g", v)
	case map[string]interface{}:
		encoded, hasValue := v["value"].(string)
		if !hasValue || encoded == "" {
			return fmt.Sprintf("%v", v)
		}

		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return encoded
		}

		scale, hasScale := v["scale"].(float64)
		if hasScale && scale == 0 && len(decoded) <= 8 {
			var result int64
			for _, b := range decoded {
				result = result*256 + int64(b)
			}
			return fmt.Sprintf("%d", result)
		}

		s := strings.TrimRight(string(decoded), "\x00")
		if s != "" && isPrintable(s) {
			return s
		}
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

func cleanPayload(p DebeziumOracleMessage) DebeziumOracleMessage {
	res := make(DebeziumOracleMessage)
	for k, v := range p {
		if k == "__op" || k == "__table" || k == "__db" || k == "__schema" || k == "__ts_ms" || k == "__deleted" || k == "__user_name" || k == "__collection" {
			continue
		}
		if k == "op" || k == "table" || k == "db" || k == "schema" || k == "ts_ms" || k == "deleted" || k == "user_name" || k == "collection" {
			continue
		}
		
		// Redact sensitive fields
		lowerK := strings.ToLower(k)
		if strings.Contains(lowerK, "password") || strings.Contains(lowerK, "token") || strings.Contains(lowerK, "secret") || lowerK == "pin" || lowerK == "pass" {
			res[k] = "[REDACTED]"
			continue
		}

		res[k] = v
	}
	return res
}

func extractMetadata(payload map[string]interface{}) map[string]interface{} {
	skip := map[string]bool{
		"__op": true, "__table": true, "__db": true, "__schema": true,
		"__ts_ms": true, "__deleted": true, "__user_name": true,
		"__scn": true, "__tx_id": true, "__row_id": true,
		"op": true, "table": true, "db": true, "schema": true,
		"ts_ms": true, "deleted": true, "user_name": true,
		"__collection": true, "collection": true,
	}

	meta := make(map[string]interface{})
	for k, v := range payload {
		lowerK := strings.ToLower(k)
		if skip[lowerK] || skip[k] {
			continue
		}

		// Redact sensitive fields
		if strings.Contains(lowerK, "password") || strings.Contains(lowerK, "token") || strings.Contains(lowerK, "secret") || lowerK == "pin" || lowerK == "pass" {
			meta[lowerK] = "[REDACTED]"
			continue
		}

		meta[lowerK] = normalizeFieldValue(v)
	}
	return meta
}

func normalizeFieldValue(val interface{}) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		if _, hasValue := v["value"]; hasValue {
			return extractScalarValue(v)
		}
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

// generateLogHash — format string HARUS identik dengan hasher.GenerateLogHash
// Normalisasi AuthorizationContext: "null"/"<nil>"/"" → selalu ""
type mapResolver struct {
	overrides map[string]string
}

func (r *mapResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if ip, ok := r.overrides[host]; ok {
		return []string{ip}, nil
	}
	return net.DefaultResolver.LookupHost(ctx, host)
}

func getDialer(cfg models.ClientKafkaConfig) *kafka.Dialer {
	overrides := make(map[string]string)
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	for _, broker := range brokers {
		host, _, err := net.SplitHostPort(broker)
		if err == nil && host != "" {
			overrides["localhost"] = host
			overrides["127.0.0.1"] = host
		}
	}

	return &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		Resolver: &mapResolver{
			overrides: overrides,
		},
	}
}

func (e *Engine) processClientUserCDC(payload DebeziumOracleMessage, cfg models.ClientKafkaConfig, tableName string, agentCfg models.AgentConfig) {
	// Tentukan kolom identifier user — dari config admin, atau auto-detect
	userCol := agentCfg.UserColumnName
	if userCol == "" {
		// Auto-detect: cari kolom yang kemungkinan besar berisi identifier user
		candidates := []string{"username", "email", "login", "name", "nama", "user_name"}
		for _, c := range candidates {
			if raw, ok := findFieldInsensitive(payload, c); ok && raw != nil {
				userCol = c
				break
			}
		}
	}

	// Ambil user ID (primary key) — ini yang akan dipakai untuk mencocokkan actor
	userID := ""
	idCandidates := []string{"id", "user_id", "userid", "uid"}
	for _, idc := range idCandidates {
		if idRaw, ok := findFieldInsensitive(payload, idc); ok && idRaw != nil {
			userID = extractScalarValue(idRaw)
			if userID != "" {
				break
			}
		}
	}

	// Ambil username/identifier dari kolom yang terdeteksi
	username := ""
	if userCol != "" {
		if usernameRaw, ok := findFieldInsensitive(payload, userCol); ok && usernameRaw != nil {
			username = extractScalarValue(usernameRaw)
		}
	}

	// Jika tidak ada ID maupun username, skip
	if userID == "" && username == "" {
		return
	}

	// Gunakan userID sebagai key utama (karena ini yang muncul di kolom actor tabel lain)
	// Fallback ke username jika tidak ada ID
	lookupKey := userID
	if lookupKey == "" {
		lookupKey = username
	}

	// Coba cari email dan fullname
	email := ""
	if emailRaw, ok := findFieldInsensitive(payload, "email"); ok {
		email = extractScalarValue(emailRaw)
	}
	
	fullName := ""
	for _, nameField := range []string{"name", "nama", "full_name", "fullname", "display_name"} {
		if nameRaw, ok := findFieldInsensitive(payload, nameField); ok {
			fullName = extractScalarValue(nameRaw)
			if fullName != "" {
				break
			}
		}
	}

	// Gabungkan first_name + last_name jika kolom 'name' tunggal tidak ada
	if fullName == "" {
		firstName := ""
		lastName := ""
		if fnRaw, ok := findFieldInsensitive(payload, "first_name"); ok && fnRaw != nil {
			firstName = extractScalarValue(fnRaw)
		}
		if lnRaw, ok := findFieldInsensitive(payload, "last_name"); ok && lnRaw != nil {
			lastName = extractScalarValue(lnRaw)
		}
		combined := strings.TrimSpace(firstName + " " + lastName)
		if combined != "" {
			fullName = combined
		}
	}

	// Jika fullName kosong tapi username ada dan bukan UUID/CUID, pakai username sebagai nama
	if fullName == "" && username != "" && !looksLikeGeneratedID(username) {
		fullName = username
	}

	rawJSON, _ := json.Marshal(payload)

	var clientUser models.ClientUser
	err := e.DB.Where("client_id = ? AND username = ?", cfg.ClientID, lookupKey).First(&clientUser).Error
	if err != nil {
		// Insert
		newUser := models.ClientUser{
			ClientID:    cfg.ClientID,
			Username:    lookupKey, // Simpan ID (UUID/CUID) agar bisa di-resolve nanti
			Email:       email,
			FullName:    fullName,
			SourceTable: tableName,
			RawData:     string(rawJSON),
			LastSeenAt:  time.Now(),
		}
		e.DB.Create(&newUser)
		log.Printf("👤 [KafkaConsumer] User baru disimpan: id=%s name=%s email=%s", lookupKey, fullName, email)
	} else {
		// Update
		updates := map[string]interface{}{
			"last_seen_at": time.Now(),
			"raw_data":     string(rawJSON),
		}
		if email != "" {
			updates["email"] = email
		}
		if fullName != "" {
			updates["full_name"] = fullName
		}
		e.DB.Model(&clientUser).Updates(updates)
	}

	// Invalidate actor cache agar resolusi langsung pakai data terbaru
	e.actorCache.Delete(cfg.ClientID + ":" + lookupKey)

	// Backfill: update audit_logs yang masih menyimpan UUID sebagai actor
	// Prioritas: Email > FullName > Username
	resolvedName := email
	if resolvedName == "" {
		resolvedName = fullName
	}
	if resolvedName == "" {
		resolvedName = username
	}
	if resolvedName != "" && lookupKey != "" && looksLikeGeneratedID(lookupKey) {
		result := e.DB.Model(&models.AuditLog{}).Where(
			"client_id = ? AND actor = ?", cfg.ClientID, lookupKey,
		).Update("actor", resolvedName)
		if result.RowsAffected > 0 {
			log.Printf("🔄 [KafkaConsumer] Backfill: %d audit log(s) actor '%s' → '%s'", result.RowsAffected, lookupKey, resolvedName)
		}
	}
}

// looksLikeGeneratedID mendeteksi apakah string terlihat seperti CUID, UUID, atau ID acak lainnya
// sehingga perlu di-resolve ke nama manusia.
var (
	cuidPattern = regexp.MustCompile(`^c[a-z0-9]{20,30}$`)                                                          // Prisma CUID: cmsrsfohw000a6jjxh4jgty6d
	uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)             // UUID v4
	hexIDPattern = regexp.MustCompile(`^[0-9a-f]{24,}$`)                                                            // MongoDB ObjectId dll
)

func looksLikeGeneratedID(val string) bool {
	if len(val) < 16 {
		return false // Terlalu pendek, kemungkinan bukan generated ID
	}
	lower := strings.ToLower(val)
	return cuidPattern.MatchString(lower) || uuidPattern.MatchString(lower) || hexIDPattern.MatchString(lower)
}

// resolveActorName menerjemahkan ID acak (CUID/UUID) menjadi nama manusia
// dengan mencari di tabel client_users. Hasilnya di-cache agar tidak query DB berulang.
func (e *Engine) resolveActorName(clientID, actorID string) string {
	cacheKey := clientID + ":" + actorID

	// Cek cache dulu
	if cached, ok := e.actorCache.Load(cacheKey); ok {
		return cached.(string)
	}

	// Cari di client_users: cocokkan actorID dengan username (yang biasanya berisi UUID/CUID)
	var user models.ClientUser
	err := e.DB.Where("client_id = ? AND username = ?", clientID, actorID).First(&user).Error
	if err != nil {
		// Fallback: coba cari di raw_data (mungkin ID disimpan dengan key berbeda)
		log.Printf("🔎 [DEBUG] resolveActorName: tidak ditemukan di username='%s', coba cari di raw_data...", actorID)
		err2 := e.DB.Where("client_id = ? AND raw_data LIKE ?", clientID, "%"+actorID+"%").First(&user).Error
		if err2 != nil {
			log.Printf("❌ [DEBUG] resolveActorName: GAGAL TOTAL untuk actorID='%s'. Tidak ada data di client_users.", actorID)
			// Hitung total rows di client_users untuk debugging
			var count int64
			e.DB.Model(&models.ClientUser{}).Where("client_id = ?", clientID).Count(&count)
			log.Printf("📊 [DEBUG] Total client_users untuk client '%s': %d", clientID, count)
			return ""
		}
		log.Printf("✅ [DEBUG] resolveActorName: DITEMUKAN via raw_data fallback untuk actorID='%s'", actorID)
	}

	// Prioritas: Email > FullName > Username (tetap CUID)
	resolved := ""
	if user.Email != "" {
		resolved = user.Email
	} else if user.FullName != "" {
		resolved = user.FullName
	}

	e.actorCache.Store(cacheKey, resolved)

	if resolved != "" {
		log.Printf("🔍 [KafkaConsumer] Resolved actor: %s → %s", actorID, resolved)
	}
	return resolved
}
