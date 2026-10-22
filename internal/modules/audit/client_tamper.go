package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

type ClientTamperQuery struct {
	Status    string `form:"status"`     // OPEN | RECOVERED | DISMISSED | ALL
	TableName string `form:"table_name"` // e.g. SATUPETA_POLYGON
	Resource  string `form:"resource"`
	Search    string `form:"search"`
	Page      int    `form:"page,default=1"`
	PageSize  int    `form:"page_size,default=10"`
}

type ClientTamperSummaryCounts struct {
	OpenCount      int64 `json:"open_count"`
	RecoveredCount int64 `json:"recovered_count"`
	DismissedCount int64 `json:"dismissed_count"`
	TotalCount     int64 `json:"total_count"`
}

type ClientTamperListResponse struct {
	Records    []models.ClientDataTamperRecord `json:"records"`
	Total      int64                           `json:"total"`
	Page       int                             `json:"page"`
	PageSize   int                             `json:"page_size"`
	TotalPages int                             `json:"total_pages"`
	Summary    ClientTamperSummaryCounts       `json:"summary"`
}

type ClientTamperRecoverResponse struct {
	Record  models.ClientDataTamperRecord        `json:"record"`
	Result  *agentverifier.ClientRecoverResponse `json:"result,omitempty"`
	Message string                               `json:"message"`
}

type DismissClientTamperInput struct {
	Notes string `json:"notes"`
}

func (s *auditService) ListClientTamperRecords(clientID string, query ClientTamperQuery) (*ClientTamperListResponse, error) {
	if s.db == nil {
		return nil, errors.New("database not available")
	}

	db := s.db.Model(&models.ClientDataTamperRecord{}).Where("client_id = ?", clientID)

	status := strings.ToUpper(strings.TrimSpace(query.Status))
	if status != "" && status != "ALL" {
		db = db.Where("status = ?", status)
	}
	if query.TableName != "" {
		db = db.Where("UPPER(table_name) = ?", strings.ToUpper(strings.TrimSpace(query.TableName)))
	}
	if query.Resource != "" {
		db = db.Where("resource = ?", strings.TrimSpace(query.Resource))
	}
	if query.Search != "" {
		search := "%" + strings.TrimSpace(query.Search) + "%"
		db = db.Where("resource ILIKE ? OR record_id ILIKE ? OR table_name ILIKE ?", search, search, search)
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, fmt.Errorf("count client tamper records: %w", err)
	}

	page := query.Page
	if page < 1 {
		page = 1
	}
	pageSize := query.PageSize
	if pageSize < 1 || pageSize > 100 {
		pageSize = 10
	}
	offset := (page - 1) * pageSize

	var records []models.ClientDataTamperRecord
	if err := db.Order("detected_at DESC").Limit(pageSize).Offset(offset).Find(&records).Error; err != nil {
		return nil, fmt.Errorf("list client tamper records: %w", err)
	}

	var summary ClientTamperSummaryCounts
	_ = s.db.Model(&models.ClientDataTamperRecord{}).Where("client_id = ? AND status = ?", clientID, models.ClientTamperStatusOpen).Count(&summary.OpenCount)
	_ = s.db.Model(&models.ClientDataTamperRecord{}).Where("client_id = ? AND status = ?", clientID, models.ClientTamperStatusRecovered).Count(&summary.RecoveredCount)
	_ = s.db.Model(&models.ClientDataTamperRecord{}).Where("client_id = ? AND status = ?", clientID, models.ClientTamperStatusDismissed).Count(&summary.DismissedCount)
	summary.TotalCount = summary.OpenCount + summary.RecoveredCount + summary.DismissedCount

	totalPages := int((total + int64(pageSize) - 1) / int64(pageSize))

	return &ClientTamperListResponse{
		Records:    records,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
		Summary:    summary,
	}, nil
}

func (s *auditService) GetClientTamperRecord(clientID, id string) (*models.ClientDataTamperRecord, error) {
	if s.db == nil {
		return nil, errors.New("database not available")
	}

	var record models.ClientDataTamperRecord
	if err := s.db.Where("id = ? AND client_id = ?", id, clientID).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("record_not_found")
		}
		return nil, fmt.Errorf("get client tamper record: %w", err)
	}
	return &record, nil
}

func (s *auditService) RecoverClientTamperRecord(ctx context.Context, clientID, id, recoveredBy string) (*ClientTamperRecoverResponse, error) {
	if s.db == nil {
		return nil, errors.New("database not available")
	}

	var record models.ClientDataTamperRecord
	if err := s.db.Where("id = ? AND client_id = ?", id, clientID).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("record_not_found")
		}
		return nil, fmt.Errorf("find client tamper record: %w", err)
	}

	if record.Status != models.ClientTamperStatusOpen {
		return nil, fmt.Errorf("record status is %s, only OPEN records can be recovered", record.Status)
	}

	var logRow models.AuditLog
	if err := s.db.Where("log_id = ? AND client_id = ?", record.LogID, clientID).First(&logRow).Error; err != nil {
		if err := s.db.Where("client_id = ? AND resource = ?", clientID, record.Resource).
			Order("timestamp DESC").First(&logRow).Error; err != nil {
			return nil, fmt.Errorf("clean audit log not found: %w", err)
		}
	}

	if strings.TrimSpace(logRow.Metadata) == "" {
		return nil, errors.New("audit log metadata is empty, cannot recover")
	}

	var cleanData map[string]interface{}
	if err := json.Unmarshal([]byte(logRow.Metadata), &cleanData); err != nil {
		return nil, fmt.Errorf("unmarshal clean metadata: %w", err)
	}

	if s.agent == nil {
		return nil, errors.New("agent verifier service not available")
	}

	recResult, err := s.agent.DirectRecoverClientRow(ctx, clientID, record.Resource, record.TableName, record.RecordID, cleanData)
	now := time.Now().UTC()
	if err != nil {
		_ = s.db.Model(&record).Updates(map[string]interface{}{
			"recovery_error": err.Error(),
		}).Error
		return nil, fmt.Errorf("agent recovery failed: %w", err)
	}

	updates := map[string]interface{}{
		"status":         models.ClientTamperStatusRecovered,
		"resolved_at":    &now,
		"resolved_by":    recoveredBy,
		"recovery_error": "",
	}
	if err := s.db.Model(&record).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("update record status: %w", err)
	}

	_ = s.db.Model(&models.AuditLog{}).Where("log_id = ? AND client_id = ?", logRow.LogID, clientID).
		Updates(map[string]interface{}{
			"source_integrity_status":     models.IntegrityStatusValid,
			"source_integrity_checked_at": &now,
		}).Error

	record.Status = models.ClientTamperStatusRecovered
	record.ResolvedAt = &now
	record.ResolvedBy = recoveredBy
	record.RecoveryError = ""

	return &ClientTamperRecoverResponse{
		Record:  record,
		Result:  recResult,
		Message: "Data berhasil dipulihkan pada database klien",
	}, nil
}

func (s *auditService) DismissClientTamperRecord(clientID, id, dismissedBy, notes string) (*models.ClientDataTamperRecord, error) {
	if s.db == nil {
		return nil, errors.New("database not available")
	}

	var record models.ClientDataTamperRecord
	if err := s.db.Where("id = ? AND client_id = ?", id, clientID).First(&record).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("record_not_found")
		}
		return nil, fmt.Errorf("find client tamper record: %w", err)
	}

	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":           models.ClientTamperStatusDismissed,
		"resolved_at":      &now,
		"resolved_by":      dismissedBy,
		"resolution_notes": notes,
	}
	if err := s.db.Model(&record).Updates(updates).Error; err != nil {
		return nil, fmt.Errorf("dismiss client tamper record: %w", err)
	}

	record.Status = models.ClientTamperStatusDismissed
	record.ResolvedAt = &now
	record.ResolvedBy = dismissedBy
	record.ResolutionNotes = notes

	return &record, nil
}
