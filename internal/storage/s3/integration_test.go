//go:build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/storage"
)

func TestStoreRoundTripAgainstMinIO(t *testing.T) {
	endpoint := envOr("DUCKMESH_TEST_S3_ENDPOINT", "")
	if endpoint == "" {
		t.Skip("DUCKMESH_TEST_S3_ENDPOINT is not set")
	}

	cfg := Config{
		Endpoint:         endpoint,
		Region:           envOr("DUCKMESH_TEST_S3_REGION", "us-east-1"),
		Bucket:           envOr("DUCKMESH_TEST_S3_BUCKET", "duckmesh-it"),
		AccessKeyID:      envOr("DUCKMESH_TEST_S3_ACCESS_KEY", "minio"),
		SecretAccessKey:  envOr("DUCKMESH_TEST_S3_SECRET_KEY", "miniostorage"),
		UseSSL:           false,
		Prefix:           "integration-tests",
		AutoCreateBucket: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	key := "tenant-1/events/roundtrip.parquet"
	payload := []byte("duckmesh-integration")

	if _, err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload)), storage.PutOptions{ContentType: "application/octet-stream"}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	stat, err := store.Stat(ctx, key)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if stat.Size != int64(len(payload)) {
		t.Fatalf("Stat().Size = %d, want %d", stat.Size, len(payload))
	}

	reader, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	readPayload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader.Close() error = %v", err)
	}
	if !bytes.Equal(readPayload, payload) {
		t.Fatalf("Get() payload = %q, want %q", string(readPayload), string(payload))
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Stat(ctx, key); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("Stat() after delete error = %v, want ErrObjectNotFound", err)
	}
}

func envOr(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
