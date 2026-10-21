package auditreference

import (
	"strings"

	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

// LatestClientEvent deliberately includes pending events: callers must never
// silently fall back to an older anchored state when the newest event is pending.
func LatestClientEvent(db *gorm.DB, clientID, resource string) (*models.AuditLog, error) {
	var row models.AuditLog
	err := db.Where("client_id = ? AND resource = ? AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'", clientID, resource).
		Order("timestamp DESC, db_timestamp DESC NULLS LAST, log_id DESC").First(&row).Error
	return &row, err
}

// LatestClientEventIndex applies the same ordering to a resource history.
func LatestClientEventIndex(logs []models.AuditLog) int {
	latest := -1
	for i := range logs {
		if strings.EqualFold(strings.TrimSpace(logs[i].Action), "RECOVERY") {
			continue
		}
		if latest < 0 || newer(logs[i], logs[latest]) {
			latest = i
		}
	}
	return latest
}

func newer(a, b models.AuditLog) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.After(b.Timestamp)
	}
	if a.DBTimestamp == nil || b.DBTimestamp == nil {
		if (a.DBTimestamp == nil) != (b.DBTimestamp == nil) {
			return a.DBTimestamp != nil
		}
	} else if !a.DBTimestamp.Equal(*b.DBTimestamp) {
		return a.DBTimestamp.After(*b.DBTimestamp)
	}
	return a.LogID >= b.LogID
}
