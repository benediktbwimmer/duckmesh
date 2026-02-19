package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/duckmesh/duckmesh/internal/storage"
)

type Config struct {
	Endpoint         string
	Region           string
	Bucket           string
	AccessKeyID      string
	SecretAccessKey  string
	UseSSL           bool
	Prefix           string
	AutoCreateBucket bool
}

type client interface {
	Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) (storage.ObjectInfo, error)
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	Stat(ctx context.Context, bucket, key string) (storage.ObjectInfo, error)
	Delete(ctx context.Context, bucket, key string) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
	CreateBucket(ctx context.Context, bucket, region string) error
}

type Store struct {
	client client
	bucket string
	prefix string
}

func New(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, fmt.Errorf("s3 endpoint is required")
	}
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}

	mc, err := newMinioClient(cfg)
	if err != nil {
		return nil, err
	}
	store := &Store{
		client: mc,
		bucket: strings.TrimSpace(cfg.Bucket),
		prefix: cleanPrefix(cfg.Prefix),
	}
	if cfg.AutoCreateBucket {
		if err := store.ensureBucket(ctx, strings.TrimSpace(cfg.Region)); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func NewWithClient(bucket, prefix string, c client) (*Store, error) {
	if c == nil {
		return nil, fmt.Errorf("client is required")
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	return &Store{client: c, bucket: strings.TrimSpace(bucket), prefix: cleanPrefix(prefix)}, nil
}

func (s *Store) Put(ctx context.Context, key string, body io.Reader, size int64, opts storage.PutOptions) (storage.ObjectInfo, error) {
	normalized, err := s.normalizeKey(key)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	info, err := s.client.Put(ctx, s.bucket, normalized, body, size, opts.ContentType)
	if err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("put object %q: %w", normalized, err)
	}
	return info, nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	normalized, err := s.normalizeKey(key)
	if err != nil {
		return nil, err
	}
	reader, err := s.client.Get(ctx, s.bucket, normalized)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("get object %q: %w", normalized, err)
	}
	return reader, nil
}

func (s *Store) Stat(ctx context.Context, key string) (storage.ObjectInfo, error) {
	normalized, err := s.normalizeKey(key)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	info, err := s.client.Stat(ctx, s.bucket, normalized)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.ObjectInfo{}, storage.ErrObjectNotFound
		}
		return storage.ObjectInfo{}, fmt.Errorf("stat object %q: %w", normalized, err)
	}
	return info, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	normalized, err := s.normalizeKey(key)
	if err != nil {
		return err
	}
	if err := s.client.Delete(ctx, s.bucket, normalized); err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil
		}
		return fmt.Errorf("delete object %q: %w", normalized, err)
	}
	return nil
}

func (s *Store) ensureBucket(ctx context.Context, region string) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("check bucket %q: %w", s.bucket, err)
	}
	if exists {
		return nil
	}
	if err := s.client.CreateBucket(ctx, s.bucket, region); err != nil {
		return fmt.Errorf("create bucket %q: %w", s.bucket, err)
	}
	return nil
}

func (s *Store) normalizeKey(key string) (string, error) {
	key = strings.TrimSpace(strings.TrimPrefix(key, "/"))
	if key == "" {
		return "", fmt.Errorf("object key is required")
	}
	cleaned := path.Clean(key)
	if cleaned == "." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
		return "", fmt.Errorf("invalid object key: %q", key)
	}
	if s.prefix == "" {
		return cleaned, nil
	}
	return path.Join(s.prefix, cleaned), nil
}

func cleanPrefix(prefix string) string {
	prefix = strings.TrimSpace(strings.TrimPrefix(prefix, "/"))
	if prefix == "" {
		return ""
	}
	prefix = path.Clean(prefix)
	if prefix == "." {
		return ""
	}
	return prefix
}

func newMinioClient(cfg Config) (*minioClient, error) {
	endpoint, secure, err := parseEndpoint(cfg.Endpoint, cfg.UseSSL)
	if err != nil {
		return nil, err
	}
	clientImpl, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure: secure,
		Region: strings.TrimSpace(cfg.Region),
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	return &minioClient{client: clientImpl}, nil
}

func parseEndpoint(raw string, useSSL bool) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, fmt.Errorf("endpoint is required")
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", false, fmt.Errorf("parse endpoint URL: %w", err)
		}
		if parsed.Host == "" {
			return "", false, fmt.Errorf("endpoint host is required")
		}
		if parsed.Scheme == "https" {
			return parsed.Host, true, nil
		}
		return parsed.Host, useSSL, nil
	}
	return raw, useSSL, nil
}

type minioClient struct {
	client *minio.Client
}

func (m *minioClient) Put(ctx context.Context, bucket, key string, reader io.Reader, size int64, contentType string) (storage.ObjectInfo, error) {
	uploadInfo, err := m.client.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		return storage.ObjectInfo{}, mapMinioErr(err)
	}
	return storage.ObjectInfo{Key: uploadInfo.Key, Size: uploadInfo.Size, ETag: uploadInfo.ETag}, nil
}

func (m *minioClient) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	obj, err := m.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapMinioErr(err)
	}
	if _, err := obj.Stat(); err != nil {
		_ = obj.Close()
		return nil, mapMinioErr(err)
	}
	return obj, nil
}

func (m *minioClient) Stat(ctx context.Context, bucket, key string) (storage.ObjectInfo, error) {
	obj, err := m.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return storage.ObjectInfo{}, mapMinioErr(err)
	}
	return storage.ObjectInfo{Key: obj.Key, Size: obj.Size, ETag: obj.ETag, LastModified: obj.LastModified}, nil
}

func (m *minioClient) Delete(ctx context.Context, bucket, key string) error {
	if err := m.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{}); err != nil {
		return mapMinioErr(err)
	}
	return nil
}

func (m *minioClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	exists, err := m.client.BucketExists(ctx, bucket)
	if err != nil {
		return false, mapMinioErr(err)
	}
	return exists, nil
}

func (m *minioClient) CreateBucket(ctx context.Context, bucket, region string) error {
	if err := m.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region}); err != nil {
		return mapMinioErr(err)
	}
	return nil
}

func mapMinioErr(err error) error {
	if err == nil {
		return nil
	}
	var response minio.ErrorResponse
	if errors.As(err, &response) {
		switch response.Code {
		case "NoSuchKey", "NoSuchBucket", "NotFound":
			return storage.ErrObjectNotFound
		}
	}
	return err
}
