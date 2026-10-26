package audit

import (
	"os"
	"testing"

	"go-blockchain-api/internal/engine/hasher"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestResolveMatchedSourceIncidentsPostgres(t *testing.T) {
	dsn := os.Getenv("MONITORING_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MONITORING_TEST_DATABASE_URL is not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx := db.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	for _, q := range []string{
		`CREATE TEMP TABLE audit_logs (client_id text,resource text,log_id text,action text,timestamp timestamptz,db_timestamp timestamptz,status text,metadata text,hash_value text,merkle_root text,blockchain_tx_id text)`,
		`CREATE TEMP TABLE tamper_incidents (id text,client_id text,resource text,log_id text,reference_log_id text,incident_scope text,status text,source_status text,resolved_at timestamptz)`,
		`CREATE TEMP TABLE recovery_requests (client_id text,incident_id text,status text)`,
	} {
		if err := tx.Exec(q).Error; err != nil {
			t.Fatal(err)
		}
	}
	f := newSourceRecoveryFixture()
	row := f.row
	row.Timestamp = f.now
	row.HashValue = hasher.GenerateLogHash(&row)
	row.MerkleRoot = row.HashValue
	if err := tx.Exec(`INSERT INTO audit_logs (client_id,resource,log_id,action,timestamp,status,metadata,hash_value,merkle_root,blockchain_tx_id) VALUES (?,?,?,?,?,?,?,?,?,?)`, row.ClientID, row.Resource, row.LogID, row.Action, row.Timestamp, row.Status, row.Metadata, row.HashValue, row.MerkleRoot, *row.BlockchainTxID).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"current", "active", "gateway", "other-client", "older-reference"} {
		tenant, scope, reference := row.ClientID, "CLIENT_SOURCE", row.LogID
		if id == "gateway" {
			scope = "GATEWAY_INTEGRITY"
		}
		if id == "other-client" {
			tenant = "other"
		}
		if id == "older-reference" {
			reference = "old"
		}
		if err := tx.Exec(`INSERT INTO tamper_incidents (id,client_id,resource,log_id,reference_log_id,incident_scope,status,source_status) VALUES (?,?,?,?,?,?,'OPEN','MISMATCH')`, id, tenant, row.Resource, reference, reference, scope).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Exec(`INSERT INTO recovery_requests VALUES (?,'active','EXECUTING')`, row.ClientID).Error; err != nil {
		t.Fatal(err)
	}
	s := &auditService{db: tx}
	if open, err := s.hasOpenSourceIncident(row); err != nil || !open {
		t.Fatalf("expected active source incident: %v", err)
	}
	if err := s.resolveMatchedSourceIncidents(row); err != nil {
		t.Fatal(err)
	}
	var rows []struct{ ID, Status, SourceStatus string }
	if err := tx.Table("tamper_incidents").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.ID == "current" {
			if r.Status != "RESOLVED" || r.SourceStatus != "MATCHED" {
				t.Fatalf("match not settled: %+v", r)
			}
		} else if r.Status != "OPEN" {
			t.Fatalf("unrelated/active incident closed: %+v", r)
		}
	}
	changed := row
	changed.Metadata = `{"id":1046,"nama":"Changed"}`
	if err := s.resolveMatchedSourceIncidents(changed); err == nil {
		t.Fatal("changed reference must block closure")
	}
}
