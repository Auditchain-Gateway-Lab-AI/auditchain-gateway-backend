package auditreference

import (
	"go-blockchain-api/internal/models"
	"gorm.io/gorm"
)

// SupersedeSourceIncidents retires obsolete references, without claiming the
// client row was repaired or is healthy. Pending newer events count as well.
// Active recovery requests keep their incident until their own execution ends.
func SupersedeSourceIncidents(db *gorm.DB, clientID string) error {
	return db.Exec(`WITH latest AS (
		SELECT DISTINCT ON (client_id, resource) client_id, resource, log_id
		FROM audit_logs
		WHERE resource <> '' AND COALESCE(UPPER(TRIM(action)), '') <> 'RECOVERY'
		  AND (? = '' OR client_id = ?)
		ORDER BY client_id, resource, timestamp DESC, db_timestamp DESC NULLS LAST, log_id DESC
	)
	UPDATE tamper_incidents AS i SET status = ?, resolved_at = CURRENT_TIMESTAMP
	FROM latest AS l
	WHERE i.client_id = l.client_id AND i.resource = l.resource
	  AND i.incident_scope = ? AND i.status = ?
	  AND COALESCE(NULLIF(i.reference_log_id, ''), i.log_id) <> l.log_id
	  AND NOT EXISTS (SELECT 1 FROM recovery_requests r
		WHERE r.client_id = i.client_id AND r.incident_id = i.id AND r.status IN (?, ?, ?))`,
		clientID, clientID, models.IncidentStatusSuperseded, models.RecoveryScopeClientSource, models.IncidentStatusOpen,
		models.RecoveryStatusPendingExecution, models.RecoveryStatusExecuting, models.RecoveryStatusAppliedAwaitingCDC).Error
}
