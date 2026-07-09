package audit

import (
	"go-blockchain-api/internal/models"

	"time"

	"gorm.io/gorm"
)

type AuditRepository interface {
	CreateLog(log *models.AuditLog) error
	GetLogByHash(hash, clientID string) (*models.AuditLog, error)
	GetLogByID(logID, clientID string) (*models.AuditLog, error)
	GetProofsByHash(hash string) ([]models.MerkleProof, error)
	GetDashboardStats(clientID string) (map[string]int64, error)
	GetLatestLogByResource(resource, clientID string) (*models.AuditLog, error)

	// GetRecentLogsPage mengambil satu halaman log terbaru untuk klien,
	// dengan opsi filter berdasarkan status penyimpanan mentah (mis. status
	// GORM: RECEIVED/HASHED/AGGREGATED/ANCHORED). Filter integrity_status
	// (valid/tampered/unreachable) TIDAK bisa dilakukan di level SQL karena
	// hasilnya baru diketahui setelah re-hash + cek Fabric di service layer,
	// jadi repository hanya bertanggung jawab atas pagination status ANCHORED
	// vs non-ANCHORED. Service layer yang melakukan post-filter in-memory.
	GetRecentLogsPage(clientID string, page, pageSize int) ([]models.AuditLog, int64, error)
	CountAnchoredLogs(clientID string) (int64, error)
	GetAnchoredLogsPage(clientID string, page, pageSize int) ([]models.AuditLog, error)

	GetResourceInventory(clientID string) ([]models.AuditLog, error)
	GetLogsByResource(resource, clientID string) ([]models.AuditLog, error)
	GetLogsByTimeRange(from, to time.Time, clientID string) ([]models.AuditLog, error)
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

func (r *auditRepoImpl) GetLogByID(logID, clientID string) (*models.AuditLog, error) {
	var log models.AuditLog
	err := r.db.Where("log_id = ? AND client_id = ?", logID, clientID).First(&log).Error
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

// GetRecentLogsPage mengembalikan satu halaman log terbaru (tanpa filter
// integrity_status) beserta total count untuk keperluan pagination di
// dashboard.
func (r *auditRepoImpl) GetRecentLogsPage(clientID string, page, pageSize int) ([]models.AuditLog, int64, error) {
	var logs []models.AuditLog
	var total int64

	if err := r.db.Model(&models.AuditLog{}).Where("client_id = ?", clientID).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	err := r.db.Where("client_id = ?", clientID).
		Order("timestamp desc").
		Limit(pageSize).
		Offset(offset).
		Find(&logs).Error

	return logs, total, err
}

// CountAnchoredLogs menghitung total log berstatus ANCHORED untuk klien.
// Dipakai sebagai basis perhitungan pagination APPROXIMATE saat filter
// integrity_status aktif (lihat catatan di service.go).
func (r *auditRepoImpl) CountAnchoredLogs(clientID string) (int64, error) {
	var total int64
	err := r.db.Model(&models.AuditLog{}).
		Where("client_id = ? AND status = ?", clientID, "ANCHORED").
		Count(&total).Error
	return total, err
}

// GetAnchoredLogsPage mengambil satu halaman log berstatus ANCHORED saja.
// Dipakai saat integrity_status filter aktif, karena hanya log ANCHORED
// yang bisa diverifikasi penuh sampai Layer 4 (valid/tampered/unreachable);
// log yang masih pending tidak relevan untuk filter ini.
func (r *auditRepoImpl) GetAnchoredLogsPage(clientID string, page, pageSize int) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	offset := (page - 1) * pageSize
	err := r.db.Where("client_id = ? AND status = ?", clientID, "ANCHORED").
		Order("timestamp desc").
		Limit(pageSize).
		Offset(offset).
		Find(&logs).Error
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

func (r *auditRepoImpl) GetLogsByTimeRange(from, to time.Time, clientID string) ([]models.AuditLog, error) {
	var logs []models.AuditLog
	err := r.db.Where("client_id = ? AND timestamp BETWEEN ? AND ?", clientID, from, to).
		Order("timestamp asc").Find(&logs).Error
	return logs, err
}
