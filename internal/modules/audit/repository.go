package audit

import (
	"go-blockchain-api/internal/models"

	"gorm.io/gorm"
)

type AuditRepository interface {
	CreateLog(log *models.AuditLog) error
	GetLogByHash(hash, clientID string) (*models.AuditLog, error)
	GetProofsByHash(hash string) ([]models.MerkleProof, error)
	GetDashboardStats(clientID string) (map[string]int64, error)
	GetLatestLogByResource(resource, clientID string) (*models.AuditLog, error)
	GetRecentLogs(limit int, clientID string) ([]models.AuditLog, error)
	GetResourceInventory(clientID string) ([]models.AuditLog, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
}

type auditRepoImpl struct {
	db *gorm.DB
}

func NewAuditRepository(db *gorm.DB) AuditRepository {
	return &auditRepoImpl{db: db}
}

func (r *auditRepoImpl) CreateLog(log *models.AuditLog) error {
	return r.db.Create(log).Error
}

func (r *auditRepoImpl) GetLogByHash(hash, clientID string) (*models.AuditLog, error) {
	var log models.AuditLog
	err := r.db.Where("hash_value = ? AND client_id = ?", hash, clientID).First(&log).Error
	return &log, err
}

func (r *auditRepoImpl) GetProofsByHash(hash string) ([]models.MerkleProof, error) {
	var proofs []models.MerkleProof
	err := r.db.Where("transaction_hash = ?", hash).Order("tree_level asc").Find(&proofs).Error
	return proofs, err
}

// GetDashboardStats mengambil total, anchored, dan pending dalam satu query
// menggunakan conditional aggregation — jauh lebih cepat dari tiga COUNT terpisah.
func (r *auditRepoImpl) GetDashboardStats(clientID string) (map[string]int64, error) {
	var result struct {
		Total    int64
		Anchored int64
		Pending  int64
	}

	err := r.db.Raw(`
		SELECT
			COUNT(*)                                                        AS total,
			COUNT(*) FILTER (WHERE status = 'ANCHORED')                    AS anchored,
			COUNT(*) FILTER (WHERE status IN ('RECEIVED','HASHED','AGGREGATED')) AS pending
		FROM audit_logs
		WHERE client_id = ?
	`, clientID).Scan(&result).Error

	if err != nil {
		return nil, err
	}

	return map[string]int64{
		"total_logs":    result.Total,
		"anchored_logs": result.Anchored,
		"pending_logs":  result.Pending,
	}, nil
}

func (r *auditRepoImpl) GetLatestLogByResource(resource, clientID string) (*models.AuditLog, error) {
	var log models.AuditLog
	err := r.db.Where("resource = ? AND client_id = ?", resource, clientID).
		Order("timestamp desc").First(&log).Error
	return &log, err
}

func (r *auditRepoImpl) GetRecentLogs(limit int, clientID string) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	err := r.db.Where("client_id = ?", clientID).
		Order("timestamp desc").Limit(limit).Find(&logs).Error
	return logs, err
}

func (r *auditRepoImpl) GetResourceInventory(clientID string) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	err := r.db.Raw(
		"SELECT DISTINCT ON (resource) * FROM audit_logs WHERE client_id = ? ORDER BY resource, timestamp DESC",
		clientID,
	).Scan(&logs).Error
	return logs, err
}

func (r *auditRepoImpl) GetLogsByResource(resource, clientID string) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	err := r.db.Where("resource = ? AND client_id = ?", resource, clientID).
		Order("timestamp asc").Find(&logs).Error
	return logs, err
}
