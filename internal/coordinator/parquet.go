package coordinator

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/duckmesh/duckmesh/internal/bus"
)

type ParquetEncodeResult struct {
	Data         []byte
	RecordCount  int64
	MinEventTime *time.Time
	MaxEventTime *time.Time
}

type parquetEvent struct {
	EventID         int64  `parquet:"event_id"`
	TenantID        string `parquet:"tenant_id"`
	TableID         int64  `parquet:"table_id"`
	IdempotencyKey  string `parquet:"idempotency_key"`
	Op              string `parquet:"op"`
	PayloadJSON     string `parquet:"payload_json"`
	EventTimeUnixMs int64  `parquet:"event_time_unix_ms"`
}

func EncodeEventsToParquet(events []bus.Envelope) (ParquetEncodeResult, error) {
	if len(events) == 0 {
		return ParquetEncodeResult{}, fmt.Errorf("events are required")
	}

	rows := make([]parquetEvent, 0, len(events))
	var minTime *time.Time
	var maxTime *time.Time

	for _, event := range events {
		eventID, err := strconv.ParseInt(event.EventID, 10, 64)
		if err != nil {
			return ParquetEncodeResult{}, fmt.Errorf("invalid event id %q: %w", event.EventID, err)
		}
		tableID, err := strconv.ParseInt(event.TableID, 10, 64)
		if err != nil {
			return ParquetEncodeResult{}, fmt.Errorf("invalid table id %q: %w", event.TableID, err)
		}

		rows = append(rows, parquetEvent{
			EventID:         eventID,
			TenantID:        event.TenantID,
			TableID:         tableID,
			IdempotencyKey:  event.IdempotencyKey,
			Op:              event.Op,
			PayloadJSON:     string(event.PayloadJSON),
			EventTimeUnixMs: event.EventTimeUnixMs,
		})

		if event.EventTimeUnixMs > 0 {
			eventTime := time.UnixMilli(event.EventTimeUnixMs).UTC()
			if minTime == nil || eventTime.Before(*minTime) {
				copy := eventTime
				minTime = &copy
			}
			if maxTime == nil || eventTime.After(*maxTime) {
				copy := eventTime
				maxTime = &copy
			}
		}
	}

	buf := bytes.NewBuffer(nil)
	writer := parquet.NewGenericWriter[parquetEvent](buf)
	if _, err := writer.Write(rows); err != nil {
		return ParquetEncodeResult{}, fmt.Errorf("write parquet rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		return ParquetEncodeResult{}, fmt.Errorf("close parquet writer: %w", err)
	}

	return ParquetEncodeResult{
		Data:         buf.Bytes(),
		RecordCount:  int64(len(rows)),
		MinEventTime: minTime,
		MaxEventTime: maxTime,
	}, nil
}
