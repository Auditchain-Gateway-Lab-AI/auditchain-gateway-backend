package audit

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go-blockchain-api/internal/models"
)

func TestVerificationRunResponsePreservesAuditDomain(t *testing.T) {
	tests := []struct {
		name, runType, requestedBy, want string
	}{
		{"internal job", models.VerificationRunTypeInternal, "user-1", models.VerificationRunTypeInternal},
		{"external Agent report", models.VerificationRunTypeExternal, "agent", models.VerificationRunTypeExternal},
		{"legacy internal job", "", "user-1", models.VerificationRunTypeInternal},
		{"legacy Agent report", "", "agent", models.VerificationRunTypeExternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run := &models.VerificationRun{ID: "run-1", RunType: test.runType, RequestedBy: test.requestedBy}
			if got := ToVerificationRunResponse(run).RunType; got != test.want {
				t.Fatalf("run_type = %q, want %q", got, test.want)
			}
		})
	}
}

func TestVerificationRunLeaseRemainsPrivateAlongsideRunType(t *testing.T) {
	expires := time.Now().Add(time.Minute)
	raw, err := json.Marshal(models.VerificationRun{
		RunType: models.VerificationRunTypeInternal, LeaseOwner: "private-worker",
		LeaseExpiresAt: &expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "private-worker") || strings.Contains(string(raw), "lease_") {
		t.Fatalf("worker lease leaked in API JSON: %s", raw)
	}
	var response map[string]interface{}
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if response["run_type"] != models.VerificationRunTypeInternal {
		t.Fatalf("mentor run_type contract missing: %s", raw)
	}
}
