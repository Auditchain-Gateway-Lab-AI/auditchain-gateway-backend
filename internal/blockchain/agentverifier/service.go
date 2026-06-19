// Package agentverifier mengimplementasikan verifikasi Lapis 3:
// Gateway memanggil Agent yang sudah berjalan di sisi klien untuk
// mengambil data aktual dari audit_trail, lalu membandingkannya
// dengan isi AuditLog yang tersimpan di DB middleware.
package agentverifier

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go-blockchain-api/internal/models"

	"gorm.io/gorm"

	"strings"
)

// AuditTrailRecord adalah respons dari endpoint GET /verify/:id di Agent.
// Field-field ini mencerminkan kolom tabel audit_trail di DB klien.
type AuditTrailRecord struct {
	Found    bool                   `json:"found"`
	ID       int                    `json:"id"`
	Tabel    string                 `json:"tabel"`
	Operasi  string                 `json:"operasi"`
	DBUser   string                 `json:"db_user"`
	AppUser  *string                `json:"app_user"`
	DataLama map[string]interface{} `json:"data_lama"`
	DataBaru map[string]interface{} `json:"data_baru"`
	Waktu    time.Time              `json:"waktu"`
}

// Discrepancy mencatat satu perbedaan antara audit log di middleware vs data dari Agent
type Discrepancy struct {
	Field   string `json:"field"`
	InLog   string `json:"in_log"`
	InAgent string `json:"in_agent"`
}

// VerifyResult adalah hasil verifikasi Lapis 3
type VerifyResult struct {
	IsMatch       bool
	SourceFound   bool
	AgentUsed     bool
	Discrepancies []Discrepancy
	AgentRecord   *AuditTrailRecord
}

// Service mengelola request verifikasi ke Agent klien
type Service struct {
	db *gorm.DB
}

// ResourceRecord adalah response dari endpoint /verify-resource di Agent
type ResourceRecord struct {
	Found     bool                   `json:"found"`
	Table     string                 `json:"table"`
	ID        string                 `json:"id"`
	Data      map[string]interface{} `json:"data"`
	CheckedAt time.Time              `json:"checked_at"`
}

func NewService(db *gorm.DB) *Service {
	return &Service{db: db}
}

// VerifyAgainstAgent adalah entry point Lapis 3.
//
// Alur:
//  1. Cek apakah log punya SourceRecordID (audit_trail_id dari Agent).
//     Jika kosong → log bukan dari Agent, lapis ini dilewati.
//  2. Ambil AgentConfig klien dari DB.
//     Jika belum dikonfigurasi → lapis ini dilewati.
//  3. Panggil GET <agent_url>/verify/<source_record_id>.
//  4. Bandingkan field kunci: tabel↔resource, operasi↔action, app_user/db_user↔actor,
//     serta metadata (data_lama+data_baru) ↔ metadata di log.
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

	// Mode 1: SIMRS — verifikasi via audit_trail_id (source_record_id)
	if auditLog.SourceRecordID != "" {
		return s.verifyViaAuditTrail(cfg, auditLog)
	}

	// Mode 2: Satu Peta — verifikasi via resource (format: tabel:id)
	if auditLog.Resource != "" && strings.Contains(auditLog.Resource, ":") {
		return s.verifyViaResource(cfg, auditLog)
	}

	// Tidak ada yang bisa diverifikasi — lewati
	return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
}

// verifyViaAuditTrail — mode SIMRS, query ke /verify/<audit_trail_id>
func (s *Service) verifyViaAuditTrail(cfg *models.AgentConfig, auditLog *models.AuditLog) (*VerifyResult, error) {
	agentRec, err := s.fetchFromAgent(cfg, auditLog.SourceRecordID)
	if err != nil {
		return nil, fmt.Errorf("gagal menghubungi Agent: %w", err)
	}

	if !agentRec.Found {
		return &VerifyResult{
			IsMatch:     false,
			SourceFound: false,
			AgentUsed:   true,
			Discrepancies: []Discrepancy{{
				Field:   "existence",
				InLog:   fmt.Sprintf("audit_trail.id=%s", auditLog.SourceRecordID),
				InAgent: "(baris tidak ditemukan di audit_trail klien)",
			}},
		}, nil
	}

	discrepancies := s.compareFields(auditLog, agentRec)
	return &VerifyResult{
		IsMatch:       len(discrepancies) == 0,
		SourceFound:   true,
		AgentUsed:     true,
		Discrepancies: discrepancies,
		AgentRecord:   agentRec,
	}, nil
}

// verifyViaResource — mode Satu Peta, query ke /verify-resource/<table>/<id>
func (s *Service) verifyViaResource(cfg *models.AgentConfig, auditLog *models.AuditLog) (*VerifyResult, error) {
	// Parse resource: "nama_tabel:id"
	parts := strings.SplitN(auditLog.Resource, ":", 2)
	if len(parts) != 2 {
		return &VerifyResult{IsMatch: true, SourceFound: false, AgentUsed: false}, nil
	}
	tableName := parts[0]
	resourceID := parts[1]

	// Panggil Agent: GET /verify-resource/<table>/<id>
	resourceRec, err := s.fetchResourceFromAgent(cfg, tableName, resourceID)
	if err != nil {
		return nil, fmt.Errorf("gagal menghubungi Agent untuk resource: %w", err)
	}

	// Jika action adalah DELETE, baris memang tidak boleh ada lagi
	if auditLog.Action == "DELETE" {
		if !resourceRec.Found {
			return &VerifyResult{
				IsMatch:     true,
				SourceFound: false,
				AgentUsed:   true,
			}, nil
		}
		// Baris masih ada padahal sudah di-DELETE — anomali
		return &VerifyResult{
			IsMatch:     false,
			SourceFound: true,
			AgentUsed:   true,
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
			IsMatch:     false,
			SourceFound: false,
			AgentUsed:   true,
			Discrepancies: []Discrepancy{{
				Field:   "existence",
				InLog:   fmt.Sprintf("%s — baris seharusnya ada", auditLog.Action),
				InAgent: fmt.Sprintf("baris tidak ditemukan di tabel %s id=%s", tableName, resourceID),
			}},
		}, nil
	}

	// Bandingkan metadata log dengan data aktual dari Agent
	discrepancies := s.compareResourceData(auditLog, resourceRec)
	return &VerifyResult{
		IsMatch:       len(discrepancies) == 0,
		SourceFound:   true,
		AgentUsed:     true,
		Discrepancies: discrepancies,
	}, nil
}

// fetchResourceFromAgent memanggil GET <agent_url>/verify-resource/<table>/<id>
func (s *Service) fetchResourceFromAgent(cfg *models.AgentConfig, tableName, resourceID string) (*ResourceRecord, error) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	url := fmt.Sprintf("%s/verify-resource/%s/%s", cfg.AgentURL, tableName, resourceID)
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

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca response Agent: %w", err)
	}

	var rec ResourceRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("gagal parse response Agent: %w", err)
	}

	return &rec, nil
}

// compareResourceData membandingkan metadata di log dengan data aktual dari Agent
func (s *Service) compareResourceData(auditLog *models.AuditLog, rec *ResourceRecord) []Discrepancy {
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
		if skipFields[key] {
			continue
		}
		agentVal, exists := rec.Data[key]
		if !exists {
			continue
		}
		if fmt.Sprintf("%v", logVal) != fmt.Sprintf("%v", agentVal) {
			diffs = append(diffs, Discrepancy{
				Field:   key,
				InLog:   fmt.Sprintf("%v", logVal),
				InAgent: fmt.Sprintf("%v", agentVal),
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

// fetchFromAgent memanggil GET <agent_url>/verify/<source_record_id>
func (s *Service) fetchFromAgent(cfg *models.AgentConfig, sourceRecordID string) (*AuditTrailRecord, error) {
	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	url := fmt.Sprintf("%s/verify/%s", cfg.AgentURL, sourceRecordID)
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
		return nil, fmt.Errorf("token verifikasi Agent tidak valid (401) — periksa AGENT_VERIFY_TOKEN")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Agent mengembalikan status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("gagal membaca response Agent: %w", err)
	}

	var rec AuditTrailRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return nil, fmt.Errorf("gagal parse response Agent: %w", err)
	}

	return &rec, nil
}

// compareFields membandingkan field-field penting antara AuditLog di middleware
// dengan data aktual dari Agent. Mapping dilakukan sesuai ClientFieldMapping:
//   - audit_trail.tabel    ↔ AuditLog.Resource
//   - audit_trail.operasi  ↔ AuditLog.Action
//   - audit_trail.app_user (fallback db_user) ↔ AuditLog.Actor
//   - gabungan data_lama+data_baru ↔ AuditLog.Metadata
func (s *Service) compareFields(auditLog *models.AuditLog, agent *AuditTrailRecord) []Discrepancy {
	var diffs []Discrepancy

	// Resource ↔ tabel
	if auditLog.Resource != agent.Tabel {
		diffs = append(diffs, Discrepancy{
			Field:   "resource/tabel",
			InLog:   auditLog.Resource,
			InAgent: agent.Tabel,
		})
	}

	// Action ↔ operasi
	if auditLog.Action != agent.Operasi {
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
		agentMetaNorm := marshalToJSON(agentMeta)

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

func normalizeJSON(raw string) string {
	var m interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return raw
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func marshalToJSON(m map[string]interface{}) string {
	b, _ := json.Marshal(m)
	return string(b)
}
