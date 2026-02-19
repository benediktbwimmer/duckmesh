package s3

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/storage"
)

func TestPutUsesPrefixAndNormalizedKey(t *testing.T) {
	fake := &fakeClient{}
	store, err := NewWithClient("bucket-a", "duckmesh/prod", fake)
	if err != nil {
		t.Fatalf("NewWithClient() error = %v", err)
	}

	_, err = store.Put(context.Background(), "/tenant-1/path/file.parquet", bytes.NewBufferString("abc"), 3, storage.PutOptions{ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if fake.lastPutBucket != "bucket-a" {
		t.Fatalf("bucket = %q", fake.lastPutBucket)
	}
	if fake.lastPutKey != "duckmesh/prod/tenant-1/path/file.parquet" {
		t.Fatalf("key = %q", fake.lastPutKey)
	}
}

func TestPutRejectsPathTraversal(t *testing.T) {
	fake := &fakeClient{}
	store, err := NewWithClient("bucket-a", "", fake)
	if err != nil {
		t.Fatalf("NewWithClient() error = %v", err)
	}
	_, err = store.Put(context.Background(), "../secrets.txt", bytes.NewBufferString("x"), 1, storage.PutOptions{})
	if err == nil {
		t.Fatal("expected path traversal validation error")
	}
}

func TestEnsureBucketCreatesWhenMissing(t *testing.T) {
	fake := &fakeClient{bucketExists: false}
	store, err := NewWithClient("bucket-a", "", fake)
	if err != nil {
		t.Fatalf("NewWithClient() error = %v", err)
	}

	if err := store.ensureBucket(context.Background(), "us-east-1"); err != nil {
		t.Fatalf("ensureBucket() error = %v", err)
	}
	if !fake.createBucketCalled {
		t.Fatal("expected CreateBucket to be called")
	}
}

func TestDeleteIgnoresMissingObject(t *testing.T) {
	fake := &fakeClient{deleteErr: storage.ErrObjectNotFound}
	store, err := NewWithClient("bucket-a", "", fake)
	if err != nil {
		t.Fatalf("NewWithClient() error = %v", err)
	}
	if err := store.Delete(context.Background(), "missing/file.parquet"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
}

func TestParseEndpoint(t *testing.T) {
	endpoint, secure, err := parseEndpoint("https://minio.example.com", false)
	if err != nil {
		t.Fatalf("parseEndpoint() error = %v", err)
	}
	if endpoint != "minio.example.com" || !secure {
		t.Fatalf("endpoint/secure = %q/%v", endpoint, secure)
	}
}

type fakeClient struct {
	lastPutBucket      string
	lastPutKey         string
	bucketExists       bool
	createBucketCalled bool
	deleteErr          error
}

func (f *fakeClient) Put(_ context.Context, bucket, key string, reader io.Reader, size int64, _ string) (storage.ObjectInfo, error) {
	f.lastPutBucket = bucket
	f.lastPutKey = key
	_, _ = io.Copy(io.Discard, reader)
	return storage.ObjectInfo{Key: key, Size: size, ETag: "etag-1"}, nil
}

func (f *fakeClient) Get(_ context.Context, _, key string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(key)), nil
}

func (f *fakeClient) Stat(_ context.Context, _, key string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{Key: key, Size: 10, LastModified: time.Now().UTC()}, nil
}

func (f *fakeClient) Delete(_ context.Context, _, _ string) error {
	return f.deleteErr
}

func (f *fakeClient) BucketExists(_ context.Context, _ string) (bool, error) {
	return f.bucketExists, nil
}

func (f *fakeClient) CreateBucket(_ context.Context, _, _ string) error {
	f.createBucketCalled = true
	return nil
}
