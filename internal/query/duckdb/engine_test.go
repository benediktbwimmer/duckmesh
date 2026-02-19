package duckdb

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/duckmesh/duckmesh/internal/query"
	"github.com/duckmesh/duckmesh/internal/storage"
)

type row struct {
	ID    int64  `parquet:"id"`
	Value string `parquet:"value"`
}

func TestExecuteReadsParquetThroughObjectStore(t *testing.T) {
	parquetBytes, err := buildParquet([]row{{ID: 1, Value: "a"}, {ID: 2, Value: "b"}})
	if err != nil {
		t.Fatalf("buildParquet() error = %v", err)
	}

	store := &memoryStore{objects: map[string][]byte{"tenant/events/file1.parquet": parquetBytes}}
	engine := NewEngine(store)

	result, err := engine.Execute(context.Background(), query.Request{
		SQL: "SELECT COUNT(*) AS c FROM events",
		Files: []query.TableFile{{
			TableName:     "events",
			ObjectPath:    "tenant/events/file1.parquet",
			FileSizeBytes: int64(len(parquetBytes)),
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Fatalf("count = %#v", result.Rows[0][0])
	}
	if result.ScannedFiles != 1 {
		t.Fatalf("ScannedFiles = %d", result.ScannedFiles)
	}
}

func TestExecuteSupportsTrailingSemicolonWithRowLimit(t *testing.T) {
	parquetBytes, err := buildParquet([]row{{ID: 1, Value: "a"}, {ID: 2, Value: "b"}})
	if err != nil {
		t.Fatalf("buildParquet() error = %v", err)
	}

	store := &memoryStore{objects: map[string][]byte{"tenant/events/file1.parquet": parquetBytes}}
	engine := NewEngine(store)

	result, err := engine.Execute(context.Background(), query.Request{
		SQL:      "SELECT COUNT(*) AS c FROM events;",
		RowLimit: 2000,
		Files: []query.TableFile{{
			TableName:     "events",
			ObjectPath:    "tenant/events/file1.parquet",
			FileSizeBytes: int64(len(parquetBytes)),
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("rows = %d", len(result.Rows))
	}
	if result.Rows[0][0] != int64(2) {
		t.Fatalf("count = %#v", result.Rows[0][0])
	}
}

func buildParquet(rows []row) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[row](buf)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type memoryStore struct {
	objects map[string][]byte
}

func (m *memoryStore) Put(context.Context, string, io.Reader, int64, storage.PutOptions) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, nil
}

func (m *memoryStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.objects[key])), nil
}

func (m *memoryStore) Stat(context.Context, string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, nil
}

func (m *memoryStore) Delete(context.Context, string) error {
	return nil
}

var _ = time.Second
