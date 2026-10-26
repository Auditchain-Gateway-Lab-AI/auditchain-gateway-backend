package agentverifier

import (
	"encoding/json"
	"testing"

	"go-blockchain-api/internal/models"
)

// Values captured from a live RUANGAN UPDATE after ALL-column logging was enabled.
func TestResourceUpdatePreservesOracleMicrosecondPrecision(t *testing.T) {
	row := &models.AuditLog{ClientID: "morbis", Resource: "RUANGAN:1762", Action: "UPDATE",
		Metadata: `{"ID":1762,"CREATED_AT":1793012400000000,"USER_INPUT":"1","UPDATED_AT":1793011907090164}`}
	live := &ResourceRecord{Found: true, Data: map[string]interface{}{
		"ID": "1762", "CREATED_AT": "2026-10-26T11:00:00Z", "USER_INPUT": "1", "UPDATED_AT": "2026-10-26T10:51:47.090164Z",
	}}
	if diffs := (&Service{}).CompareResourceData(row, live); len(diffs) != 0 {
		t.Fatalf("false legacy source mismatch: %+v", diffs)
	}
	if result := compareCurrentResourceState(row, live); !result.Comparable || !result.Match {
		t.Fatalf("false canonical source mismatch: %+v", result)
	}
	// A real microsecond change must not disappear under a time tolerance.
	live.Data["UPDATED_AT"] = "2026-10-26T10:51:47.090165Z"
	if diffs := (&Service{}).CompareResourceData(row, live); len(diffs) != 1 || diffs[0].Field != "UPDATED_AT" {
		t.Fatalf("real timestamp change hidden: %+v", diffs)
	}
	if result := compareCurrentResourceState(row, live); result.Match {
		t.Fatal("canonical comparison hid a real microsecond change")
	}
}

func TestResourceUpdateKeepsExplicitNullDifferentFromValue(t *testing.T) {
	row := &models.AuditLog{ClientID: "morbis", Resource: "RUANGAN:1762", Action: "UPDATE", Metadata: `{"ID":1762,"USER_INPUT":null,"UPDATED_AT":1793011907090164}`}
	live := &ResourceRecord{Found: true, Data: map[string]interface{}{"ID": "1762", "USER_INPUT": nil, "UPDATED_AT": "2026-10-26T10:51:47.090164Z"}}
	if diffs := (&Service{}).CompareResourceData(row, live); len(diffs) != 0 {
		t.Fatalf("intentional NULL update should match: %+v", diffs)
	}
	live.Data["USER_INPUT"] = json.Number("1")
	if diffs := (&Service{}).CompareResourceData(row, live); len(diffs) != 1 {
		t.Fatalf("NULL-versus-value must remain a difference: %+v", diffs)
	}
}
