package agentverifier

import (
	"testing"

	"go-blockchain-api/internal/models"
)

func TestClientUpdateComparisonIsIndependentOfEarlierMismatch(t *testing.T) {
	row := &models.AuditLog{
		ClientID: "client-1", Resource: "RUANGAN:1761", Action: "UPDATE",
		Metadata: `{"ID":1761,"NAMA":"Baru","USER_INPUT":null}`,
	}
	live := &ResourceRecord{Found: true, Data: map[string]interface{}{
		"ID": "1761", "NAMA": "Baru", "USER_INPUT": "1",
	}}
	first := compareCurrentResourceState(row, live)
	if !first.Comparable || first.Match {
		t.Fatal("an actual null-versus-value difference must be reported")
	}
	// A later client event has its own values. An earlier mismatch is not
	// inherited by the next UPDATE, whether for this resource or another one.
	for _, resource := range []string{"RUANGAN:1761", "RUANGAN:1762"} {
		row.Resource = resource
		id := "1761"
		if resource == "RUANGAN:1762" {
			id = "1762"
		}
		row.Metadata = `{"ID":"` + id + `","NAMA":"Baru","USER_INPUT":"1"}`
		live.Data["ID"] = id
		result := compareCurrentResourceState(row, live)
		if !result.Comparable || !result.Match || result.ClientHash != result.OffchainHash {
			t.Fatalf("normal update %s inherited a previous mismatch: %+v", resource, result)
		}
	}
}
