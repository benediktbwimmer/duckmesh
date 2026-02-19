package coordinator

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/duckmesh/duckmesh/internal/bus"
)

func TestEncodeEventsToParquet(t *testing.T) {
	events := []bus.Envelope{
		{
			EventID:         "1",
			TenantID:        "tenant-a",
			TableID:         "10",
			IdempotencyKey:  "idem-1",
			Op:              "insert",
			PayloadJSON:     []byte(`{"a":1}`),
			EventTimeUnixMs: time.Date(2026, time.February, 19, 10, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			EventID:         "2",
			TenantID:        "tenant-a",
			TableID:         "10",
			IdempotencyKey:  "idem-2",
			Op:              "insert",
			PayloadJSON:     []byte(`{"a":2}`),
			EventTimeUnixMs: time.Date(2026, time.February, 19, 11, 0, 0, 0, time.UTC).UnixMilli(),
		},
	}

	result, err := EncodeEventsToParquet(events)
	if err != nil {
		t.Fatalf("EncodeEventsToParquet() error = %v", err)
	}
	if result.RecordCount != 2 {
		t.Fatalf("RecordCount = %d", result.RecordCount)
	}
	if len(result.Data) == 0 {
		t.Fatal("expected non-empty parquet payload")
	}

	reader := parquet.NewGenericReader[parquetEvent](bytes.NewReader(result.Data))
	defer func() { _ = reader.Close() }()
	rows := make([]parquetEvent, 2)
	count, err := reader.Read(rows)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("reader.Read() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("read rows = %d", count)
	}
	if rows[0].EventID != 1 || rows[1].EventID != 2 {
		t.Fatalf("unexpected event ids: %+v", rows)
	}
}
