package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/duckmesh/duckmesh/internal/bus"
)

type IngestBus struct {
	db    *sql.DB
	clock func() time.Time
}

func NewIngestBus(db *sql.DB) *IngestBus {
	return &IngestBus{db: db, clock: time.Now}
}

func (b *IngestBus) Publish(ctx context.Context, events []bus.Envelope) ([]bus.PublishResult, error) {
	if len(events) == 0 {
		return []bus.PublishResult{}, nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	query := `
INSERT INTO ingest_event (tenant_id, table_id, idempotency_key, op, payload_json, event_time, state)
VALUES ($1, $2, $3, $4::duckmesh_ingest_op, $5::jsonb, $6, 'accepted')
ON CONFLICT (tenant_id, table_id, idempotency_key)
DO UPDATE SET idempotency_key = ingest_event.idempotency_key
RETURNING event_id, (xmax = 0) AS inserted`

	results := make([]bus.PublishResult, 0, len(events))
	for _, event := range events {
		tableID, err := strconv.ParseInt(event.TableID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid table id %q: %w", event.TableID, err)
		}

		var eventTime *time.Time
		if event.EventTimeUnixMs > 0 {
			timeValue := time.UnixMilli(event.EventTimeUnixMs).UTC()
			eventTime = &timeValue
		}

		payload := event.PayloadJSON
		if len(payload) == 0 {
			payload = []byte("{}")
		}

		var eventID int64
		var inserted bool
		if err := tx.QueryRowContext(ctx, query,
			event.TenantID,
			tableID,
			event.IdempotencyKey,
			event.Op,
			string(payload),
			eventTime,
		).Scan(&eventID, &inserted); err != nil {
			return nil, fmt.Errorf("publish event %q: %w", event.IdempotencyKey, err)
		}

		results = append(results, bus.PublishResult{
			EventID:         strconv.FormatInt(eventID, 10),
			VisibilityToken: eventID,
			Inserted:        inserted,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit publish tx: %w", err)
	}
	return results, nil
}

func (b *IngestBus) ClaimBatch(ctx context.Context, consumerID string, limit int, leaseSeconds int) (bus.Batch, error) {
	if limit <= 0 {
		limit = 100
	}
	if leaseSeconds <= 0 {
		leaseSeconds = 30
	}

	tx, err := b.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return bus.Batch{}, fmt.Errorf("begin claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	selectionQuery := `
SELECT event_id, tenant_id, table_id, idempotency_key, op::text, payload_json, event_time
FROM ingest_event
WHERE state = 'accepted' AND (lease_until IS NULL OR lease_until <= NOW())
ORDER BY event_id ASC
FOR UPDATE SKIP LOCKED
LIMIT $1`

	rows, err := tx.QueryContext(ctx, selectionQuery, limit)
	if err != nil {
		return bus.Batch{}, fmt.Errorf("select claim candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type selectedEvent struct {
		eventID        int64
		tenantID       string
		tableID        int64
		idempotencyKey string
		op             string
		payloadJSON    []byte
		eventTime      *time.Time
	}

	selected := make([]selectedEvent, 0, limit)
	for rows.Next() {
		var event selectedEvent
		if err := rows.Scan(
			&event.eventID,
			&event.tenantID,
			&event.tableID,
			&event.idempotencyKey,
			&event.op,
			&event.payloadJSON,
			&event.eventTime,
		); err != nil {
			return bus.Batch{}, fmt.Errorf("scan claim candidate: %w", err)
		}
		selected = append(selected, event)
	}
	if err := rows.Err(); err != nil {
		return bus.Batch{}, fmt.Errorf("iterate claim candidates: %w", err)
	}

	if len(selected) == 0 {
		if err := tx.Commit(); err != nil {
			return bus.Batch{}, fmt.Errorf("commit empty claim tx: %w", err)
		}
		return bus.Batch{}, nil
	}

	leaseUntil := b.clock().UTC().Add(time.Duration(leaseSeconds) * time.Second)
	createBatchQuery := `
INSERT INTO ingest_claim_batch (consumer_id, claimed_at, lease_until, state)
VALUES ($1, NOW(), $2, 'claimed')
RETURNING batch_id, claimed_at`

	var batchID int64
	var claimedAt time.Time
	if err := tx.QueryRowContext(ctx, createBatchQuery, consumerID, leaseUntil).Scan(&batchID, &claimedAt); err != nil {
		return bus.Batch{}, fmt.Errorf("create claim batch: %w", err)
	}

	updateEventQuery := `
UPDATE ingest_event
SET state = 'claimed', lease_owner = $1, lease_until = $2
WHERE event_id = $3`
	claimItemQuery := `
INSERT INTO ingest_claim_item (batch_id, event_id)
VALUES ($1, $2)`

	batch := bus.Batch{
		BatchID:     strconv.FormatInt(batchID, 10),
		ConsumerID:  consumerID,
		LeaseUntil:  leaseUntil.UnixMilli(),
		EventIDs:    make([]string, 0, len(selected)),
		Envelopes:   make([]bus.Envelope, 0, len(selected)),
		ClaimedUnix: claimedAt.UnixMilli(),
	}

	for _, event := range selected {
		if _, err := tx.ExecContext(ctx, updateEventQuery, consumerID, leaseUntil, event.eventID); err != nil {
			return bus.Batch{}, fmt.Errorf("update claimed event %d: %w", event.eventID, err)
		}
		if _, err := tx.ExecContext(ctx, claimItemQuery, batchID, event.eventID); err != nil {
			return bus.Batch{}, fmt.Errorf("insert claim item %d: %w", event.eventID, err)
		}

		eventID := strconv.FormatInt(event.eventID, 10)
		batch.EventIDs = append(batch.EventIDs, eventID)
		if event.eventID > batch.Visibility {
			batch.Visibility = event.eventID
		}

		eventTimeUnixMs := int64(0)
		if event.eventTime != nil {
			eventTimeUnixMs = event.eventTime.UTC().UnixMilli()
		}
		batch.Envelopes = append(batch.Envelopes, bus.Envelope{
			EventID:         eventID,
			TenantID:        event.tenantID,
			TableID:         strconv.FormatInt(event.tableID, 10),
			IdempotencyKey:  event.idempotencyKey,
			Op:              event.op,
			PayloadJSON:     event.payloadJSON,
			EventTimeUnixMs: eventTimeUnixMs,
		})
	}

	if err := tx.Commit(); err != nil {
		return bus.Batch{}, fmt.Errorf("commit claim tx: %w", err)
	}
	return batch, nil
}

func (b *IngestBus) Ack(ctx context.Context, batchID string, eventIDs []string) error {
	batchIDInt, err := parseInt64(batchID, "batch id")
	if err != nil {
		return err
	}
	parsedEventIDs, err := parseEventIDs(eventIDs)
	if err != nil {
		return err
	}
	if len(parsedEventIDs) == 0 {
		return nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ack tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ackQuery := `
UPDATE ingest_event AS e
SET state = 'committed', lease_owner = NULL, lease_until = NULL
FROM ingest_claim_item AS i
WHERE i.batch_id = $1 AND i.event_id = $2 AND e.event_id = i.event_id`

	for _, eventID := range parsedEventIDs {
		if _, err := tx.ExecContext(ctx, ackQuery, batchIDInt, eventID); err != nil {
			return fmt.Errorf("ack event %d: %w", eventID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE ingest_claim_batch
SET state = CASE
    WHEN EXISTS (
        SELECT 1
        FROM ingest_claim_item AS i
        JOIN ingest_event AS e ON e.event_id = i.event_id
        WHERE i.batch_id = $1 AND e.state = 'claimed'
    ) THEN 'claimed'::duckmesh_ingest_state
    ELSE 'committed'::duckmesh_ingest_state
END
WHERE batch_id = $1`, batchIDInt); err != nil {
		return fmt.Errorf("update claim batch state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ack tx: %w", err)
	}
	return nil
}

func (b *IngestBus) Nack(ctx context.Context, batchID string, eventIDs []string, _ string) error {
	batchIDInt, err := parseInt64(batchID, "batch id")
	if err != nil {
		return err
	}
	parsedEventIDs, err := parseEventIDs(eventIDs)
	if err != nil {
		return err
	}
	if len(parsedEventIDs) == 0 {
		return nil
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin nack tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	nackQuery := `
UPDATE ingest_event AS e
SET state = 'failed', lease_owner = NULL, lease_until = NULL
FROM ingest_claim_item AS i
WHERE i.batch_id = $1 AND i.event_id = $2 AND e.event_id = i.event_id`

	for _, eventID := range parsedEventIDs {
		if _, err := tx.ExecContext(ctx, nackQuery, batchIDInt, eventID); err != nil {
			return fmt.Errorf("nack event %d: %w", eventID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `UPDATE ingest_claim_batch SET state = 'failed' WHERE batch_id = $1`, batchIDInt); err != nil {
		return fmt.Errorf("update claim batch state to failed: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit nack tx: %w", err)
	}
	return nil
}

func (b *IngestBus) ExtendLease(ctx context.Context, batchID string, leaseSeconds int) error {
	batchIDInt, err := parseInt64(batchID, "batch id")
	if err != nil {
		return err
	}
	if leaseSeconds <= 0 {
		leaseSeconds = 30
	}
	leaseUntil := b.clock().UTC().Add(time.Duration(leaseSeconds) * time.Second)

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin extend lease tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	batchResult, err := tx.ExecContext(ctx, `
UPDATE ingest_claim_batch
SET lease_until = $2
WHERE batch_id = $1 AND state = 'claimed'`, batchIDInt, leaseUntil)
	if err != nil {
		return fmt.Errorf("update claim batch lease: %w", err)
	}
	rowsAffected, err := batchResult.RowsAffected()
	if err != nil {
		return fmt.Errorf("read batch rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("claim batch %d not found or not claimable", batchIDInt)
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE ingest_event AS e
SET lease_until = $2
FROM ingest_claim_item AS i
WHERE i.batch_id = $1 AND e.event_id = i.event_id AND e.state = 'claimed'`, batchIDInt, leaseUntil); err != nil {
		return fmt.Errorf("update event lease: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit extend lease tx: %w", err)
	}
	return nil
}

func (b *IngestBus) RequeueExpired(ctx context.Context) (int, error) {
	requeueQuery := `
WITH moved AS (
    UPDATE ingest_event
    SET state = 'accepted', lease_owner = NULL, lease_until = NULL
    WHERE state = 'claimed' AND lease_until IS NOT NULL AND lease_until < NOW()
    RETURNING event_id
)
SELECT COUNT(*) FROM moved`

	var count int
	if err := b.db.QueryRowContext(ctx, requeueQuery).Scan(&count); err != nil {
		return 0, fmt.Errorf("requeue expired events: %w", err)
	}

	_, _ = b.db.ExecContext(ctx, `
UPDATE ingest_claim_batch
SET state = 'failed'
WHERE state = 'claimed' AND lease_until < NOW()`)

	return count, nil
}

func parseEventIDs(eventIDs []string) ([]int64, error) {
	parsed := make([]int64, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		value, err := parseInt64(eventID, "event id")
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, value)
	}
	return parsed, nil
}

func parseInt64(value string, field string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, value, err)
	}
	return parsed, nil
}

var _ bus.IngestBus = (*IngestBus)(nil)
