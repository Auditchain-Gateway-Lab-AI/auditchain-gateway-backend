package main

import (
	"testing"
	"time"
)

func TestValidateRecoveryScopeFlags(t *testing.T) {
	cutoff := time.Date(2026, 9, 18, 14, 34, 33, 0, time.UTC)
	tests := []struct {
		name             string
		recovery         bool
		snapshotRequired bool
		cutoff           *time.Time
		wantErr          bool
	}{
		{name: "disabled local", wantErr: false},
		{name: "recovery requires cutoff", recovery: true, snapshotRequired: true, wantErr: true},
		{name: "recovery requires gate", recovery: true, cutoff: &cutoff, wantErr: true},
		{name: "gate requires cutoff", snapshotRequired: true, wantErr: true},
		{name: "production scope valid", recovery: true, snapshotRequired: true, cutoff: &cutoff, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateRecoveryScopeFlags(tt.recovery, tt.snapshotRequired, tt.cutoff); (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
