package recovery

import (
	"encoding/json"
	"reflect"
	"testing"

	"go-blockchain-api/internal/blockchain/agentverifier"
	"go-blockchain-api/internal/models"
)

func TestRecoveryComparisonUsesSourceVerificationSemantics(t *testing.T) {
	logRow := models.AuditLog{ClientID: "client-1", Resource: "RUANGAN:1467", Action: "INSERT",
		Metadata: `{"aktif":"1","created_at":1792732127000000,"id":1467,"id_unit":"4348","nama":"Melati","updated_at":null}`}
	baseline := map[string]interface{}{"AKTIF": "1", "CREATED_AT": "2026-10-23T05:08:47Z", "ID": "1467", "ID_UNIT": "4348", "NAMA": "Melati", "UPDATED_AT": nil, "EXTRA": "outside projection"}
	tests := []struct {
		name   string
		mutate func(map[string]interface{})
		found  bool
		action string
		status string
		fields []string
	}{
		{name: "equivalent Oracle and CDC formats", found: true, status: models.SourceStatusMatched, fields: []string{}},
		{name: "real value change", found: true, mutate: func(m map[string]interface{}) { m["NAMA"] = "Mawar" }, status: models.SourceStatusMismatch, fields: []string{"nama"}},
		{name: "real timestamp change", found: true, mutate: func(m map[string]interface{}) { m["CREATED_AT"] = "2026-10-23T05:08:48Z" }, status: models.SourceStatusMismatch, fields: []string{"created_at"}},
		{name: "wrong identity", found: true, mutate: func(m map[string]interface{}) { m["ID"] = "1468" }, status: models.SourceStatusMismatch, fields: []string{"id"}},
		{name: "ordinary value types stay strict", found: true, mutate: func(m map[string]interface{}) { m["ID_UNIT"] = json.Number("4348") }, status: models.SourceStatusMismatch, fields: []string{"id_unit"}},
		{name: "missing projected field is inconclusive", found: true, mutate: func(m map[string]interface{}) { delete(m, "NAMA") }, status: models.SourceStatusNotComparable, fields: []string{"nama"}},
		{name: "normalized key collision is inconclusive", found: true, mutate: func(m map[string]interface{}) { m["nama"] = "Mawar" }, status: models.SourceStatusNotComparable, fields: []string{"client_state"}},
		{name: "missing row is a real incident", status: models.SourceStatusMissing, fields: []string{"existence"}},
		{name: "deleted row remains absent", action: "DELETE", status: models.SourceStatusMatched, fields: []string{}},
		{name: "deleted row unexpectedly present", action: "DELETE", found: true, status: models.SourceStatusUnexpectedPresent, fields: []string{"existence"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := logRow
			if tc.action != "" {
				row.Action = tc.action
			}
			actual := make(map[string]interface{}, len(baseline))
			for key, value := range baseline {
				actual[key] = value
			}
			if tc.mutate != nil {
				tc.mutate(actual)
			}
			record := &agentverifier.ResourceRecord{Found: tc.found, Table: "RUANGAN", ID: "1467", Data: actual}
			operation, err := directOperation(row.Action)
			if err != nil {
				t.Fatal(err)
			}
			comparison := agentverifier.CompareResourceState(&row, record)
			if got := directComparisonStatus(operation, record, comparison); got != tc.status {
				t.Fatalf("status = %s, want %s", got, tc.status)
			}
			if got := comparisonChangedFields(comparison); !reflect.DeepEqual(got, tc.fields) {
				t.Fatalf("changed fields = %#v, want %#v", got, tc.fields)
			}
			data := &directRecoveryData{Log: row, Client: record, Operation: operation}
			if got := (&Service{}).directSourceMatches(data); got != (tc.status == models.SourceStatusMatched) {
				t.Fatalf("recovery match = %v for %s", got, tc.status)
			}
		})
	}
	// Semantic comparison must not rewrite the payload or raw CAS hash.
	before, _ := agentverifier.HashResourceState(logRow.ClientID, logRow.Resource, baseline)
	agentverifier.CompareResourceState(&logRow, &agentverifier.ResourceRecord{Found: true, Data: baseline})
	after, _ := agentverifier.HashResourceState(logRow.ClientID, logRow.Resource, baseline)
	if before != after || baseline["ID"] != "1467" {
		t.Fatal("comparison mutated Agent execution state")
	}
}

func TestPreflightSerializesMissingSourceAndEmptyComparison(t *testing.T) {
	fields := []string{}
	raw, err := json.Marshal(PreflightResult{SourceFound: false, ChangedFields: &fields})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	if found, ok := payload["source_found"]; !ok || found != false {
		t.Fatal("missing source must be explicit")
	}
	if fields, ok := payload["changed_fields"].([]interface{}); !ok || len(fields) != 0 {
		t.Fatal("empty fresh comparison must serialize as []")
	}
}
