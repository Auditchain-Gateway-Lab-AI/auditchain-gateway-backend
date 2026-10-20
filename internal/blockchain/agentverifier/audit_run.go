package agentverifier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

const MaxSynchronousAgentAuditLogs = 100

type AuditRunLogResult struct {
	LogID            string `json:"log_id"`
	Resource         string `json:"resource"`
	Action           string `json:"action"`
	DataStatus       string `json:"data_status"`
	BlockchainStatus string `json:"blockchain_status"`
	AutoRecovered    bool   `json:"auto_recovered,omitempty"`
	Error            string `json:"error,omitempty"`
}

type AuditRunReport struct {
	RunID              string              `json:"run_id"`
	From               time.Time           `json:"from"`
	To                 time.Time           `json:"to"`
	TotalLogs          int                 `json:"total_logs"`
	AuditedLogs        int                 `json:"audited_logs"`
	HistoricalSkipped  int                 `json:"historical_skipped"`
	DataValid          int                 `json:"data_valid"`
	DataTampered       int                 `json:"data_tampered"`
	DataNotFound       int                 `json:"data_not_found"`
	DataError          int                 `json:"data_error"`
	BlockchainValid    int                 `json:"blockchain_valid"`
	BlockchainTampered int                 `json:"blockchain_tampered"`
	BlockchainPending  int                 `json:"blockchain_pending"`
	BlockchainSkipped  int                 `json:"blockchain_skipped"`
	BlockchainError    int                 `json:"blockchain_error"`
	AutoRecoveredCount int                 `json:"auto_recovered_count"`
	Duration           string              `json:"duration"`
	Results            []AuditRunLogResult `json:"results"`
}

type AuditRangeLimitError struct {
	EstimatedLogs int64
	Limit         int64
}

func (e *AuditRangeLimitError) Error() string {
	return fmt.Sprintf("audit range contains %d logs; synchronous limit is %d", e.EstimatedLogs, e.Limit)
}

// RunAudit asks the tenant's configured Agent to audit a bounded range. Tenant
// identity and both Agent tokens come only from Gateway-side trusted config.
func (s *Service) RunAudit(ctx context.Context, clientID string, from, to time.Time) (*AuditRunReport, error) {
	if s.db == nil {
		return nil, errors.New("database unavailable")
	}
	if strings.TrimSpace(clientID) == "" {
		return nil, errors.New("client identity is required")
	}
	cfg, err := s.loadAgentConfig(clientID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("agent_not_configured")
		}
		return nil, fmt.Errorf("agent_config_load_failed: %w", err)
	}
	if !cfg.IsActive || strings.TrimSpace(cfg.AgentURL) == "" {
		return nil, errors.New("agent_not_configured")
	}
	if strings.TrimSpace(cfg.VerifyToken) == "" {
		return nil, errors.New("agent_verify_token_missing")
	}

	var count int64
	if err := s.db.WithContext(ctx).Model(&models.AuditLog{}).
		Where("client_id = ? AND timestamp BETWEEN ? AND ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'", clientID, from, to).
		Count(&count).Error; err != nil {
		return nil, fmt.Errorf("count tenant audit logs: %w", err)
	}
	if count > MaxSynchronousAgentAuditLogs {
		return nil, &AuditRangeLimitError{EstimatedLogs: count, Limit: MaxSynchronousAgentAuditLogs}
	}

	timeout := time.Duration(cfg.TimeoutSeconds) * time.Second
	if timeout < 70*time.Second {
		timeout = 70 * time.Second
	}
	return postAgentAudit(ctx, cfg, from, to, timeout)
}

func postAgentAudit(ctx context.Context, cfg *models.AgentConfig, from, to time.Time, timeout time.Duration) (*AuditRunReport, error) {
	if cfg == nil || strings.TrimSpace(cfg.AgentURL) == "" || strings.TrimSpace(cfg.VerifyToken) == "" {
		return nil, errors.New("agent_not_configured")
	}
	body, err := json.Marshal(struct {
		From        string `json:"from"`
		To          string `json:"to"`
		AutoRecover bool   `json:"auto_recover"`
	}{From: from.UTC().Format(time.RFC3339Nano), To: to.UTC().Format(time.RFC3339Nano), AutoRecover: false})
	if err != nil {
		return nil, fmt.Errorf("encode Agent audit request: %w", err)
	}
	requestURL := strings.TrimRight(cfg.AgentURL, "/") + "/audit/run"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build Agent audit request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+cfg.VerifyToken)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	if timeout <= 0 {
		timeout = 70 * time.Second
	}
	response, err := (&http.Client{Timeout: timeout}).Do(request)
	if err != nil {
		return nil, fmt.Errorf("agent_audit_unreachable: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("agent_audit_response_read_failed: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("agent_audit_http_%d", response.StatusCode)
	}
	var report AuditRunReport
	if err := json.Unmarshal(responseBody, &report); err != nil {
		return nil, fmt.Errorf("agent_audit_response_invalid: %w", err)
	}
	if strings.TrimSpace(report.RunID) == "" {
		return nil, errors.New("agent_audit_run_id_missing")
	}
	if report.TotalLogs > MaxSynchronousAgentAuditLogs || len(report.Results) > MaxSynchronousAgentAuditLogs {
		return nil, &AuditRangeLimitError{EstimatedLogs: int64(max(report.TotalLogs, len(report.Results))), Limit: MaxSynchronousAgentAuditLogs}
	}
	return &report, nil
}
