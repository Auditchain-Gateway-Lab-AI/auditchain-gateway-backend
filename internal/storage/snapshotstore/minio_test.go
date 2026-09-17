package snapshotstore

import (
	"fmt"
	"testing"

	"github.com/minio/minio-go/v7"
)

func TestIsNotFoundUnwrapsMinIOError(t *testing.T) {
	err := fmt.Errorf("stat failed: %w", minio.ErrorResponse{Code: "NoSuchVersion", StatusCode: 404})
	if !IsNotFound(err) {
		t.Fatalf("expected wrapped MinIO not-found error to be detected")
	}
}

func TestIsNotFoundRejectsOtherErrors(t *testing.T) {
	err := fmt.Errorf("stat failed: %w", minio.ErrorResponse{Code: "AccessDenied", StatusCode: 403})
	if IsNotFound(err) {
		t.Fatalf("did not expect access denied to be classified as not found")
	}
}
