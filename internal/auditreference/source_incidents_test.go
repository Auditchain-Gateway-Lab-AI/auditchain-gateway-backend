package auditreference

import (
	"os"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestSupersedeSourceIncidentsPostgres(t *testing.T) {
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
	for _, sql := range []string{
		`CREATE TEMP TABLE audit_logs (client_id text,resource text,log_id text,action text,timestamp timestamptz,db_timestamp timestamptz)`,
		`CREATE TEMP TABLE tamper_incidents (id text,client_id text,resource text,log_id text,reference_log_id text,incident_scope text,status text,resolved_at timestamptz)`,
		`CREATE TEMP TABLE recovery_requests (client_id text,incident_id text,status text)`,
		`INSERT INTO audit_logs VALUES
		('a','ROOM:1','old','UPDATE','2026-10-26T01:00Z',NULL),
		('a','ROOM:1','new','DELETE','2026-10-26T02:00Z',NULL),
		('a','ROOM:2','pending-new','UPDATE','2026-10-26T02:00Z',NULL),
		('a','ROOM:3','old','INSERT','2026-10-26T01:00Z',NULL),
		('a','ROOM:3','internal','RECOVERY','2026-10-26T02:00Z',NULL),
		('b','ROOM:1','new','DELETE','2026-10-26T02:00Z',NULL)`,
		`INSERT INTO tamper_incidents VALUES
		('stale','a','ROOM:1','old','old','CLIENT_SOURCE','OPEN',NULL),
		('latest','a','ROOM:1','new','new','CLIENT_SOURCE','OPEN',NULL),
		('pending','a','ROOM:2','old','','CLIENT_SOURCE','OPEN',NULL),
		('internal','a','ROOM:3','old','old','CLIENT_SOURCE','OPEN',NULL),
		('gateway','a','ROOM:1','old','old','GATEWAY_INTEGRITY','OPEN',NULL),
		('active','a','ROOM:1','old','old','CLIENT_SOURCE','OPEN',NULL),
		('other-tenant','b','ROOM:1','old','old','CLIENT_SOURCE','OPEN',NULL)`,
		`INSERT INTO recovery_requests VALUES ('a','active','APPLIED_AWAITING_CDC')`,
	} {
		if err := tx.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := SupersedeSourceIncidents(tx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	var rows []struct {
		ID     string
		Status string
	}
	if err := tx.Table("tamper_incidents").Select("id,status").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		want := "OPEN"
		if row.ID == "stale" || row.ID == "pending" {
			want = "SUPERSEDED"
		}
		if row.Status != want {
			t.Fatalf("%s status=%s want %s", row.ID, row.Status, want)
		}
	}
}
