package report

import (
	"bytes"
	"encoding/csv"
	"time"

	"go-blockchain-api/internal/modules/audit"
)

type Service interface {
	GenerateCSVReport(clientID string, fromTime, toTime time.Time) ([]byte, error)
}

type reportService struct {
	auditSvc audit.Service
}

func NewService(auditSvc audit.Service) Service {
	return &reportService{
		auditSvc: auditSvc,
	}
}

func (s *reportService) GenerateCSVReport(clientID string, fromTime, toTime time.Time) ([]byte, error) {
	// Fetch all logs for the given client and time range
	// We'll use a large page size to fetch all or paginate through them.
	// For simplicity in Phase 1, we assume pulling up to 10000 records is fine.
	
	result, err := s.auditSvc.GetRecentLogsPaginated(clientID, 1, 10000, "", "asc", "", "", &fromTime, &toTime)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	// Add UTF-8 BOM
	buf.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(&buf)
	
	// Write Header
	header := []string{"Timestamp", "Actor", "Action", "Resource", "Source Table", "Hash Value", "Status", "Blockchain TX ID"}
	if err := writer.Write(header); err != nil {
		return nil, err
	}

	// Write Data
	for _, item := range result.Data {
		txID := ""
		if item.BlockchainTxID != nil {
			txID = *item.BlockchainTxID
		}
		
		record := []string{
			item.Timestamp.Format(time.RFC3339),
			item.Actor,
			item.Action,
			item.Resource,
			item.SourceSystem, // or SourceTable if you map it
			item.HashValue,
			item.IntegrityStatus,
			txID,
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
