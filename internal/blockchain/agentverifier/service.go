// Package agentverifier mengimplementasikan verifikasi Lapis 3:
// Gateway memanggil Agent yang sudah berjalan di sisi klien untuk
// mengambil data aktual dari audit_trail, lalu membandingkannya
// dengan isi AuditLog yang tersimpan di DB middleware.
package agentverifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go-blockchain-api/internal/models"
	"go-blockchain-api/pkg/canonicalstate"

	"gorm.io/gorm"
)

// AuditTrailRecord adalah respons dari endpoint GET /verify/<table>/<id> di Agent.
// Field-field ini mencerminkan kolom tabel audit_trail di DB klien.
type AuditTrailRecord struct {
	Found    bool                   `json:"found"`
	ID       string                 `json:"id"`
	Tabel    string                 `json:"tabel"`
	Operasi  string                 `json:"operasi"`
	DBUser   string                 `json:"db_user"`
	AppUser  *string                `json:"app_user"`
	DataLama map[string]interface{} `json:"data_lama"`
	DataBaru map[string]interface{} `json:"data_baru"`
	Waktu    time.Time              `json:"waktu"`
}

type AuditTrailLookupResult struct {
	Records   []AuditTrailRecord `json:"records"`
	Truncated bool               `json:"truncated"`
}

// Discrepancy mencatat satu perbedaan antara audit log di middleware vs data dari Agent
type Discrepancy struct {
	Field   string `json:"field"`
	InLog   string `json:"in_log"`
	InAgent string `json:"in_agent"`
}

// VerifyResult adalah hasil verifikasi Lapis 3
type VerifyResult struct {
	IsMatch        bool
	SourceFound    bool
	AgentUsed      bool
	Discrepancies  []Discrepancy
	AgentRecord    *AuditTrailRecord
	ClientMetadata string
	// ClientStateHash is the canonical hash of the live operational row (or
	// the canonical empty object when the row is missing). It is deliberately
	// separate from the AuditLog leaf hash so source-state incidents can be
	// correlated without pretending the client row is itself a Merkle leaf.
	ClientStateHash   string
	OffchainStateHash string
	SourceCheckedAt   *time.Time
	StateComparable   bool
	StateComparison   string
	ComparisonBasis   string
	AuditTrailStatus  string
	AuditTrailDiffs   []Discrepancy
}

// Service mengelola request verifikasi ke Agent klien
type Service struct {
	db *gorm.DB
}

// ResourceRecord adalah response dari endpoint /verify/<table>/<id> di Agent.
type ResourceRecord struct {
	Found     bool                   `json:"found"`
	Table     string                 `json:"table"`
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	CheckedAt time.Time              `json:"checked_at"`
}

// HTTPStatusError reports an HTTP response received from the Agent. It is
// distinct from transport errors: a 403 means the Agent was reached but
// refused the verification request, not that the Agent is unreachable.
type HTTPStatusError struct {
	StatusCode int
	Message    string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "Agent mengembalikan status HTTP yang tidak valid"
	}
	if e.Message != "" {
		return e.Message
	}
	switch e.StatusCode {
	case http.StatusUnauthorized:
		return "token verifikasi Agent tidak valid (401)"
	case http.StatusForbidden:
		return "akses verifikasi Agent ditolak (403)"
	default:
		return fmt.Sprintf("Agent mengembalikan status %d", e.StatusCode)
	}
}

func NewService(db *gorm.DB) *Service {
	return &Service{db: db}
}

// VerifyAgainstAgent membandingkan row terkini dari Agent dengan state sesudah
// event klien terakhir di AuditLog. Jika SourceRecordID tersedia, event
// audit_trail juga diperiksa sebagai bukti terpisah; event historis itu tidak
// menggantikan pembacaan row operasional.
func (s *Service) VerifyAgainstAgent(auditLog *models.AuditLog) (*VerifyResult, error) {
	cfg, err := s.loadAgentConfig(auditLog.ClientID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
		}
		return nil, fmt.Errorf("gagal memuat konfigurasi Agent: %w", err)
	}

	if !cfg.IsActive {
		return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
	}

	if cfg.AgentURL == "" {
		return &VerifyResult{IsMatch: true, AgentUsed: false}, nil
	}

	// Jika AgentURL mengarah ke port 8083 (Debezium Native CDC), kita tahu ini bukan Custom Agent Lapis 3.
	// Debezium tidak memiliki endpoint /verify, sehingga kita skip verifikasi agent untuk klien ini.
	if strings.Contains(cfg.AgentURL, ":8083") {
		return &VerifyResult{IsMatch: true, AgentUsed: false}, nil
	}

	// Format resource TABLE:ID memungkinkan Agent membaca row operasional saat
	// ini, yang dibutuhkan untuk alur verifikasi state terbaru.
	if auditLog.Resource != "" && strings.Contains(auditLog.Resource, ":") {
		result, err := s.verifyViaResource(cfg, auditLog)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(auditLog.SourceRecordID) != "" {
			trailResult, trailErr := s.verifyViaAuditTrail(cfg, auditLog)
			if trailErr != nil {
				result.AuditTrailStatus = "unavailable"
			} else if trailResult != nil && trailResult.AgentUsed {
				if trailResult.IsMatch {
					result.AuditTrailStatus = "matched"
				} else {
					result.AuditTrailStatus = "mismatch"
					result.AuditTrailDiffs = trailResult.Discrepancies
				}
			}
		}
		return result, nil
	}

	// Event audit sumber saja tidak membuktikan kondisi row saat ini.
	if auditLog.SourceRecordID != "" {
		return s.verifyViaAuditTrail(cfg, auditLog)
	}

	// Tidak ada yang bisa diverifikasi — lewati
	return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
}

// verifyViaAuditTrail reads the immutable source event via /verify-audit/<id>.
func (s *Service) verifyViaAuditTrail(cfg *models.AgentConfig, auditLog *models.AuditLog) (*VerifyResult, error) {
	recordID := strings.TrimSpace(auditLog.SourceRecordID)
	if recordID == "" {
		return nil, fmt.Errorf("source_record_id untuk verifikasi Agent kosong")
	}

	agentRec, err := s.fetchFromAgent(cfg, recordID)
	if err != nil {
		return nil, fmt.Errorf("gagal menghubungi Agent: %w", err)
	}

	if !agentRec.Found {
		return &VerifyResult{
			IsMatch:          false,
			SourceFound:      false,
			AgentUsed:        true,
			StateComparable:  false,
			StateComparison:  "not_comparable",
			ComparisonBasis:  "audit_trail_event_only",
			AuditTrailStatus: "missing",
			Discrepancies: []Discrepancy{{
				Field:   "existence",
				InLog:   fmt.Sprintf("audit_trail.id=%s", auditLog.SourceRecordID),
				InAgent: "(baris tidak ditemukan di audit_trail klien)",
			}},
		}, nil
	}

	agentMeta := map[string]interface{}{}
	if agentRec.DataLama != nil {
		agentMeta["data_lama"] = agentRec.DataLama
	}
	if agentRec.DataBaru != nil {
		agentMeta["data_baru"] = agentRec.DataBaru
	}
	clientMetadata := marshalToJSON(agentMeta)

	discrepancies := s.compareFields(auditLog, agentRec)
	auditTrailStatus := "matched"
	if len(discrepancies) > 0 {
		auditTrailStatus = "mismatch"
	}
	return &VerifyResult{
		IsMatch:          len(discrepancies) == 0,
		SourceFound:      true,
		AgentUsed:        true,
		Discrepancies:    discrepancies,
		AgentRecord:      agentRec,
		ClientMetadata:   clientMetadata,
		StateComparable:  false,
		StateComparison:  "not_comparable",
		ComparisonBasis:  "audit_trail_event_only",
		AuditTrailStatus: auditTrailStatus,
	}, nil
}

// verifyViaResource menangani verifikasi untuk log yang membawa Resource
// berformat "table:id" — ini mencakup SIMRS Morbis maupun Satu Peta, karena
// keduanya mengisi field Resource dengan format yang sama saat masuk lewat
// Kafka consumer (lihat kafkaconsumer/consumer.go dan consumer/consumer.go).
// Endpoint Agent yang dipanggil: GET /verify/<table>/<id>.
func (s *Service) verifyViaResource(cfg *models.AgentConfig, auditLog *models.AuditLog) (*VerifyResult, error) {
	// Parse resource: "nama_tabel:id"
	parts := strings.SplitN(auditLog.Resource, ":", 2)
	if len(parts) != 2 {
		return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
	}
	tableName := parts[0]
	resourceID := parts[1]

	// Panggil Agent: GET /verify/<table>/<id>
	resourceRec, err := s.fetchResourceFromAgent(cfg, tableName, resourceID)
	if err != nil {
		return nil, fmt.Errorf("gagal menghubungi Agent untuk resource: %w", err)
	}
	if resourceRec.Table != "" && !strings.EqualFold(resourceRec.Table, tableName) {
		return nil, fmt.Errorf("respons Agent memiliki tabel yang tidak sesuai dengan resource yang diminta")
	}
	if resourceRec.ID != "" && resourceRec.ID != resourceID {
		return nil, fmt.Errorf("respons Agent memiliki ID yang tidak sesuai dengan resource yang diminta")
	}
	stateResult := compareCurrentResourceState(auditLog, resourceRec)

	// Jika action adalah DELETE, baris memang tidak boleh ada lagi
	if strings.EqualFold(strings.TrimSpace(auditLog.Action), "DELETE") {
		if !resourceRec.Found {
			return &VerifyResult{
				IsMatch:           stateResult.Match,
				SourceFound:       false,
				AgentUsed:         true,
				SourceCheckedAt:   checkedAtUTC(resourceRec.CheckedAt),
				ClientStateHash:   stateResult.ClientHash,
				OffchainStateHash: stateResult.OffchainHash,
				StateComparable:   stateResult.Comparable,
				StateComparison:   stateComparisonLabel(stateResult),
				ComparisonBasis:   stateResult.Basis,
			}, nil
		}
		// Baris masih ada padahal sudah di-DELETE — anomali
		return &VerifyResult{
			IsMatch:           false,
			SourceFound:       true,
			AgentUsed:         true,
			SourceCheckedAt:   checkedAtUTC(resourceRec.CheckedAt),
			ClientStateHash:   stateResult.ClientHash,
			OffchainStateHash: stateResult.OffchainHash,
			StateComparable:   true,
			StateComparison:   "mismatch",
			ComparisonBasis:   stateResult.Basis,
			Discrepancies: []Discrepancy{{
				Field:   "existence",
				InLog:   "DELETE — baris seharusnya tidak ada",
				InAgent: fmt.Sprintf("baris masih ditemukan di tabel %s id=%s", tableName, resourceID),
			}},
		}, nil
	}

	// Untuk INSERT/UPDATE — baris harus ada
	if !resourceRec.Found {
		return &VerifyResult{
			IsMatch:           false,
			SourceFound:       false,
			AgentUsed:         true,
			SourceCheckedAt:   checkedAtUTC(resourceRec.CheckedAt),
			ClientStateHash:   stateResult.ClientHash,
			OffchainStateHash: stateResult.OffchainHash,
			StateComparable:   true,
			StateComparison:   "mismatch",
			ComparisonBasis:   stateResult.Basis,
			Discrepancies: []Discrepancy{{
				Field:   "existence",
				InLog:   fmt.Sprintf("%s — baris seharusnya ada", auditLog.Action),
				InAgent: fmt.Sprintf("baris tidak ditemukan di tabel %s id=%s", tableName, resourceID),
			}},
		}, nil
	}

	clientMetadata := marshalToJSON(resourceRec.Data)
	return &VerifyResult{
		IsMatch:           stateResult.Match,
		SourceFound:       true,
		AgentUsed:         true,
		SourceCheckedAt:   checkedAtUTC(resourceRec.CheckedAt),
		Discrepancies:     stateResult.Discrepancies,
		ClientMetadata:    clientMetadata,
		ClientStateHash:   stateResult.ClientHash,
		OffchainStateHash: stateResult.OffchainHash,
		StateComparable:   stateResult.Comparable,
		StateComparison:   stateComparisonLabel(stateResult),
		ComparisonBasis:   stateResult.Basis,
	}, nil
}

func checkedAtUTC(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	checkedAt := value.UTC()
	return &checkedAt
}

// fetchResourceFromAgent memanggil GET <agent_url>/verify/<table>/<id>
func (s *Service) fetchResourceFromAgent(cfg *models.AgentConfig, tableName, resourceID string) (*ResourceRecord, error) {
	return s.fetchResourceFromAgentContext(context.Background(), cfg, tableName, resourceID)
}

func (s *Service) fetchResourceFromAgentContext(ctx context.Context, cfg *models.AgentConfig, tableName, resourceID string) (*ResourceRecord, error) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	url := agentVerifyURL(cfg.AgentURL, tableName, resourceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	if cfg.VerifyToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.VerifyToken)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request ke Agent gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca response Agent: %w", err)
	}

	var rec ResourceRecord
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&rec); err != nil {
		return nil, fmt.Errorf("gagal parse response Agent: %w", err)
	}

	return &rec, nil
}

// CompareResourceData membandingkan metadata di log dengan data aktual dari Agent
func (s *Service) CompareResourceData(auditLog *models.AuditLog, rec *ResourceRecord) []Discrepancy {
	var diffs []Discrepancy

	if auditLog.Metadata == "" || auditLog.Metadata == "{}" {
		return diffs
	}

	// Parse metadata log — ambil bagian data_baru saja
	var logMeta map[string]interface{}
	if err := json.Unmarshal([]byte(auditLog.Metadata), &logMeta); err != nil {
		return diffs
	}

	// Bandingkan field per field
	skipFields := map[string]bool{
		"ogc_fid": true, "id": true, "_id": true,
		"fid": true, "gid": true, "objectid": true,
	}

	for key, logVal := range logMeta {
		if skipFields[strings.ToLower(key)] {
			continue
		}
		agentVal, exists := rec.Data[key]
		if !exists {
			continue
		}

		strLogVal := formatValueToString(logVal)
		strAgentVal := formatValueToString(agentVal)

		strLogVal, strAgentVal = tryNormalizeTimeMatch(strLogVal, strAgentVal)

		if strLogVal != strAgentVal {
			diffs = append(diffs, Discrepancy{
				Field:   key,
				InLog:   strLogVal,
				InAgent: strAgentVal,
			})
		}
	}

	return diffs
}

// loadAgentConfig mengambil konfigurasi Agent dari DB middleware
func (s *Service) loadAgentConfig(clientID string) (*models.AgentConfig, error) {
	var cfg models.AgentConfig
	err := s.db.
		Where("client_id = ? AND is_active = true AND deleted_at IS NULL", clientID).
		First(&cfg).Error
	return &cfg, err
}

// fetchFromAgent reads one source audit event from the client Agent.
func (s *Service) fetchFromAgent(cfg *models.AgentConfig, sourceRecordID string) (*AuditTrailRecord, error) {
	return fetchAuditTrailFromAgent(context.Background(), cfg, sourceRecordID)
}

func fetchAuditTrailFromAgent(ctx context.Context, cfg *models.AgentConfig, sourceRecordID string) (*AuditTrailRecord, error) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	endpoint := agentAuditTrailURL(cfg.AgentURL, sourceRecordID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	if cfg.VerifyToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.VerifyToken)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request ke Agent gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("audit_trail_record_not_found")
	}
	if resp.StatusCode != http.StatusOK {
		message := fmt.Sprintf("agent_audit_trail_unavailable: Agent mengembalikan status %d", resp.StatusCode)
		if resp.StatusCode == http.StatusUnauthorized {
			message = "token verifikasi Agent tidak valid (401) — periksa AGENT_VERIFY_TOKEN"
		}
		return nil, &HTTPStatusError{StatusCode: resp.StatusCode, Message: message}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca response Agent: %w", err)
	}

	var rec AuditTrailRecord
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&rec); err != nil {
		return nil, fmt.Errorf("gagal parse response Agent: %w", err)
	}

	return &rec, nil
}

// ReadAuditTrail fetches one immutable source event from the tenant's Agent.
// It is used for both Layer 3 verification and no-MinIO Gateway log recovery.
func (s *Service) ReadAuditTrail(ctx context.Context, clientID, sourceRecordID string) (*AuditTrailRecord, error) {
	if strings.TrimSpace(sourceRecordID) == "" {
		return nil, fmt.Errorf("audit_trail_reference_missing")
	}
	cfg, err := s.loadAgentConfig(clientID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("agent_not_configured")
		}
		return nil, fmt.Errorf("agent_config_unavailable: %w", err)
	}
	if !cfg.IsActive || strings.TrimSpace(cfg.AgentURL) == "" || strings.Contains(cfg.AgentURL, ":8083") {
		return nil, fmt.Errorf("agent_not_configured")
	}
	return fetchAuditTrailFromAgent(ctx, cfg, sourceRecordID)
}

// FindAuditTrailCandidates asks the Agent for a bounded set of nearby audit
// events. A candidate is not trusted until recovery matches it to the stored
// AuditChain leaf, Merkle proof, and Fabric root.
func (s *Service) FindAuditTrailCandidates(ctx context.Context, clientID, tableName, operation, primaryKey, recordID string, at time.Time) (*AuditTrailLookupResult, error) {
	if strings.TrimSpace(tableName) == "" || strings.TrimSpace(primaryKey) == "" || strings.TrimSpace(recordID) == "" || at.IsZero() {
		return nil, fmt.Errorf("audit_trail_lookup_invalid_request")
	}
	cfg, err := s.loadAgentConfig(clientID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("agent_not_configured")
		}
		return nil, fmt.Errorf("agent_config_unavailable: %w", err)
	}
	if !cfg.IsActive || strings.TrimSpace(cfg.AgentURL) == "" || strings.Contains(cfg.AgentURL, ":8083") {
		return nil, fmt.Errorf("agent_not_configured")
	}
	return fetchAuditTrailCandidatesFromAgent(ctx, cfg, tableName, operation, primaryKey, recordID, at)
}

func fetchAuditTrailCandidatesFromAgent(ctx context.Context, cfg *models.AgentConfig, tableName, operation, primaryKey, recordID string, at time.Time) (*AuditTrailLookupResult, error) {
	if cfg == nil || strings.TrimSpace(cfg.AgentURL) == "" {
		return nil, fmt.Errorf("agent_not_configured")
	}
	values := url.Values{}
	values.Set("table", strings.TrimSpace(tableName))
	values.Set("operation", strings.ToUpper(strings.TrimSpace(operation)))
	values.Set("primary_key", strings.TrimSpace(primaryKey))
	values.Set("record_id", strings.TrimSpace(recordID))
	values.Set("at", at.UTC().Format(time.RFC3339Nano))
	values.Set("window_seconds", "300")
	values.Set("limit", "50")
	baseURL := strings.TrimRight(cfg.AgentURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	requestURL := baseURL + "/verify-audit-lookup?" + values.Encode()
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("audit_trail_lookup_request_invalid: %w", err)
	}
	if cfg.VerifyToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.VerifyToken)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("agent_audit_trail_lookup_unreachable: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, fmt.Errorf("agent_audit_trail_lookup_read_failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var agentError struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(body, &agentError)
		switch strings.ToLower(strings.TrimSpace(agentError.Code)) {
		case "database_unreachable":
			return nil, fmt.Errorf("agent_audit_trail_lookup_database_unavailable")
		case "audit_trail_lookup_failed":
			return nil, fmt.Errorf("agent_audit_trail_lookup_failed")
		case "audit_trail_table_not_found":
			return nil, fmt.Errorf("agent_audit_trail_table_not_found")
		default:
			return nil, fmt.Errorf("agent_audit_trail_lookup_http_%d", resp.StatusCode)
		}
	}
	var result AuditTrailLookupResult
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("agent_audit_trail_lookup_response_invalid: %w", err)
	}
	return &result, nil
}

func agentAuditTrailURL(agentURL, sourceRecordID string) string {
	baseURL := strings.TrimRight(agentURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	return baseURL + "/verify-audit/" + url.PathEscape(strings.TrimSpace(sourceRecordID))
}

// MetadataJSON produces the exact metadata envelope used by the Gateway CDC
// contract. Nil images are omitted, matching historical Layer 3 comparison.
func (r *AuditTrailRecord) MetadataJSON() ([]byte, error) {
	if r == nil || !r.Found {
		return nil, fmt.Errorf("audit_trail_record_not_found")
	}
	metadata := make(map[string]interface{}, 2)
	if r.DataLama != nil {
		metadata["data_lama"] = r.DataLama
	}
	if r.DataBaru != nil {
		metadata["data_baru"] = r.DataBaru
	}
	return json.Marshal(metadata)
}

func agentVerifyURL(agentURL, tableName, resourceID string) string {
	baseURL := strings.TrimRight(agentURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	return fmt.Sprintf("%s/verify/%s/%s", baseURL, tableName, resourceID)
}

// compareFields membandingkan metadata event dengan audit_trail di Agent.
// Mapping dilakukan sesuai ClientFieldMapping:
//   - audit_trail.tabel    ↔ AuditLog.Resource
//   - audit_trail.operasi  ↔ AuditLog.Action
//   - audit_trail.app_user (fallback db_user) ↔ AuditLog.Actor
//   - gabungan data_lama+data_baru ↔ AuditLog.Metadata
func (s *Service) compareFields(auditLog *models.AuditLog, agent *AuditTrailRecord) []Discrepancy {
	var diffs []Discrepancy

	// Resource dapat berbentuk TABLE:ID, sedangkan audit_trail.tabel hanya nama tabel.
	logTable := strings.TrimSpace(strings.SplitN(auditLog.Resource, ":", 2)[0])
	agentTable := strings.TrimSpace(strings.SplitN(agent.Tabel, ":", 2)[0])
	if !strings.EqualFold(logTable, agentTable) {
		diffs = append(diffs, Discrepancy{
			Field:   "table/resource",
			InLog:   logTable,
			InAgent: agent.Tabel,
		})
	}

	// Action ↔ operasi
	if !strings.EqualFold(strings.TrimSpace(auditLog.Action), strings.TrimSpace(agent.Operasi)) {
		diffs = append(diffs, Discrepancy{
			Field:   "action/operasi",
			InLog:   auditLog.Action,
			InAgent: agent.Operasi,
		})
	}

	// Actor ↔ app_user (fallback ke db_user) — sesuai ClientFieldMapping klien
	sourceActor := agent.DBUser
	if agent.AppUser != nil && *agent.AppUser != "" {
		sourceActor = *agent.AppUser
	}
	if auditLog.Actor != sourceActor {
		diffs = append(diffs, Discrepancy{
			Field:   "actor/app_user",
			InLog:   auditLog.Actor,
			InAgent: sourceActor,
		})
	}

	// Metadata ↔ gabungan data_lama + data_baru
	// Agent selalu mengirim metadata sebagai {"data_lama": {...}, "data_baru": {...}}
	if auditLog.Metadata != "" {
		agentMeta := map[string]interface{}{}
		if agent.DataLama != nil {
			agentMeta["data_lama"] = agent.DataLama
		}
		if agent.DataBaru != nil {
			agentMeta["data_baru"] = agent.DataBaru
		}

		logMetaNorm := normalizeJSON(auditLog.Metadata)
		agentMetaNorm := normalizeJSON(marshalToJSON(agentMeta))

		if logMetaNorm != agentMetaNorm {
			diffs = append(diffs, Discrepancy{
				Field:   "metadata",
				InLog:   logMetaNorm,
				InAgent: agentMetaNorm,
			})
		}
	}

	return diffs
}

type resourceStateComparison struct {
	Match         bool
	Comparable    bool
	ClientHash    string
	OffchainHash  string
	Basis         string
	Discrepancies []Discrepancy
}

// compareCurrentResourceState hashes the same projected state on both sides.
// The state hash is deliberately distinct from the audit-log leaf hash.
func compareCurrentResourceState(auditLog *models.AuditLog, record *ResourceRecord) resourceStateComparison {
	result := resourceStateComparison{Basis: "latest_event_field_projection:v1"}
	if auditLog == nil || record == nil {
		result.Discrepancies = []Discrepancy{{Field: "comparison", InLog: "event state", InAgent: "missing response"}}
		return result
	}

	expected, err := offchainResourceState(auditLog)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "offchain_state", InLog: "valid state object", InAgent: "not comparable"}}
		return result
	}
	expected = removeRedactedStateFields(expected)
	if !strings.EqualFold(strings.TrimSpace(auditLog.Action), "DELETE") && len(expected) == 0 {
		result.Discrepancies = []Discrepancy{{Field: "offchain_state", InLog: "non-empty event state", InAgent: "empty or redacted projection"}}
		return result
	}
	result.OffchainHash, err = HashResourceState(auditLog.ClientID, auditLog.Resource, expected)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "offchain_state", InLog: "canonical state", InAgent: "invalid state"}}
		return result
	}

	if strings.EqualFold(strings.TrimSpace(auditLog.Action), "DELETE") {
		result.Basis = "latest_event_row_absence:v1"
		if !record.Found {
			result.ClientHash, _ = HashResourceState(auditLog.ClientID, auditLog.Resource, map[string]interface{}{})
			result.Comparable = true
			result.Match = true
			return result
		}
		result.ClientHash, _ = HashResourceState(auditLog.ClientID, auditLog.Resource, record.Data)
		result.Comparable = true
		result.Discrepancies = []Discrepancy{{
			Field: "existence", InLog: "DELETE — row tidak ada", InAgent: "row masih ada",
		}}
		return result
	}

	if !record.Found {
		result.ClientHash, _ = HashResourceState(auditLog.ClientID, auditLog.Resource, map[string]interface{}{})
		result.Comparable = true
		result.Discrepancies = []Discrepancy{{Field: "existence", InLog: "row seharusnya ada", InAgent: "row tidak ditemukan"}}
		return result
	}
	if record.Data == nil {
		result.Discrepancies = []Discrepancy{{Field: "client_state", InLog: "state object", InAgent: "data kosong"}}
		return result
	}

	actualRaw, err := json.Marshal(record.Data)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "client_state", InLog: "canonical state", InAgent: "invalid JSON"}}
		return result
	}
	expectedRaw, err := json.Marshal(expected)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "offchain_state", InLog: "canonical state", InAgent: "invalid JSON"}}
		return result
	}
	expectedCanonical, _, err := canonicalstate.CanonicalizeObject(expectedRaw)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "offchain_state", InLog: "canonical state", InAgent: "invalid canonical state"}}
		return result
	}
	actualCanonical, _, err := canonicalstate.CanonicalizeObject(actualRaw)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "client_state", InLog: "canonical state", InAgent: "invalid canonical state"}}
		return result
	}

	projected := make(map[string]interface{}, len(expectedCanonical))
	for key, expectedValue := range expectedCanonical {
		actualValue, exists := actualCanonical[key]
		if !exists {
			result.Discrepancies = append(result.Discrepancies, Discrepancy{
				Field: key, InLog: "field present in latest event", InAgent: "field not returned by Agent",
			})
			continue
		}
		projected[key] = actualValue
		expectedJSON, _ := json.Marshal(expectedValue)
		actualJSON, _ := json.Marshal(actualValue)
		if !bytes.Equal(expectedJSON, actualJSON) {
			result.Discrepancies = append(result.Discrepancies, Discrepancy{
				Field: key, InLog: string(expectedJSON), InAgent: string(actualJSON),
			})
		}
	}
	if len(projected) != len(expectedCanonical) {
		return result
	}
	result.ClientHash, err = HashResourceState(auditLog.ClientID, auditLog.Resource, projected)
	if err != nil {
		result.Discrepancies = []Discrepancy{{Field: "client_state", InLog: "canonical state", InAgent: "hash failed"}}
		return result
	}
	result.Comparable = true
	result.Match = len(result.Discrepancies) == 0 && result.ClientHash == result.OffchainHash
	return result
}

func stateComparisonLabel(comparison resourceStateComparison) string {
	if !comparison.Comparable {
		return "not_comparable"
	}
	if comparison.Match {
		return "matched"
	}
	return "mismatch"
}

func offchainResourceState(auditLog *models.AuditLog) (map[string]interface{}, error) {
	if strings.EqualFold(strings.TrimSpace(auditLog.Action), "DELETE") {
		return map[string]interface{}{}, nil
	}
	decoder := json.NewDecoder(strings.NewReader(auditLog.Metadata))
	decoder.UseNumber()
	var state map[string]interface{}
	if err := decoder.Decode(&state); err != nil || state == nil {
		return nil, fmt.Errorf("metadata event tidak dapat dibaca sebagai state object")
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("metadata event memiliki konten tambahan")
	}
	for _, wrapper := range []string{"data_baru", "after"} {
		for key, value := range state {
			if strings.EqualFold(key, wrapper) {
				wrapped, ok := value.(map[string]interface{})
				if !ok || wrapped == nil {
					return nil, fmt.Errorf("metadata %s bukan state object", wrapper)
				}
				return wrapped, nil
			}
		}
	}
	return state, nil
}

func removeRedactedStateFields(state map[string]interface{}) map[string]interface{} {
	filtered := make(map[string]interface{}, len(state))
	for key, value := range state {
		if redacted, ok := value.(string); ok && strings.EqualFold(strings.TrimSpace(redacted), "[REDACTED]") {
			continue
		}
		filtered[key] = value
	}
	return filtered
}

func formatValueToString(val interface{}) string {
	if val == nil {
		return "null"
	}

	switch v := val.(type) {
	case float64:
		if v == math.Trunc(v) {
			return fmt.Sprintf("%.0f", v)
		}
		return fmt.Sprintf("%g", v)
	case json.Number:
		return v.String()
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func tryNormalizeTimeMatch(strLog, strAgent string) (string, string) {
	if strLog == strAgent {
		return strLog, strAgent
	}

	var timeLog, timeAgent time.Time
	var errLog, errAgent error

	// Coba parse Agent time sebagai ISO-8601 (RFC3339)
	timeAgent, errAgent = time.Parse(time.RFC3339, strAgent)
	if errAgent != nil {
		return strLog, strAgent
	}

	// Parse Log time
	timeLog, errLog = time.Parse(time.RFC3339, strLog)
	if errLog != nil {
		// Coba parse sebagai angka epoch (dari Debezium)
		epochFloat, err := strconv.ParseFloat(strLog, 64)
		if err == nil {
			if epochFloat > 1e14 {
				// Microseconds
				timeLog = time.Unix(0, int64(epochFloat*1000)).UTC()
			} else if epochFloat > 1e11 {
				// Milliseconds
				timeLog = time.UnixMilli(int64(epochFloat)).UTC()
			} else {
				// Seconds
				timeLog = time.Unix(int64(epochFloat), 0).UTC()
			}
		} else {
			return strLog, strAgent
		}
	}

	// Bandingkan selisih waktu
	diff := timeLog.Sub(timeAgent)
	if diff < 0 {
		diff = -diff
	}

	// Jika selisih max 1 detik (toleransi presisi/pembulatan), anggap cocok
	if diff <= time.Second {
		return timeAgent.Format(time.RFC3339), strAgent
	}

	return strLog, strAgent
}

func normalizeJSON(raw string) string {
	_, canonical, err := canonicalstate.CanonicalizeObject([]byte(raw))
	if err != nil {
		return raw
	}
	return string(canonical)
}

func marshalToJSON(m map[string]interface{}) string {
	b, _ := json.Marshal(m)
	return string(b)
}

// FetchTableData memuat konfigurasi lalu memanggil FetchTableFromAgent
func (s *Service) FetchTableData(clientID, tableName string) ([]ResourceRecord, error) {
	cfg, err := s.loadAgentConfig(clientID)
	if err != nil {
		return nil, fmt.Errorf("gagal memuat konfigurasi Agent: %w", err)
	}
	if !cfg.IsActive {
		return nil, fmt.Errorf("agent tidak aktif")
	}
	return s.FetchTableFromAgent(cfg, tableName)
}

// FetchTableFromAgent memanggil GET <agent_url>/table/<table>
func (s *Service) FetchTableFromAgent(cfg *models.AgentConfig, tableName string) ([]ResourceRecord, error) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 15 * time.Second
	}

	url := agentFetchTableURL(cfg.AgentURL, tableName)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	if cfg.VerifyToken != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.VerifyToken)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request ke Agent gagal: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("token verifikasi Agent tidak valid (401)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Agent mengembalikan status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca response Agent: %w", err)
	}

	var records []ResourceRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("gagal parse response Agent: %w", err)
	}

	return records, nil
}

func agentFetchTableURL(agentURL, tableName string) string {
	baseURL := strings.TrimRight(agentURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "http://" + baseURL
	}
	return fmt.Sprintf("%s/table/%s", baseURL, tableName)
}
