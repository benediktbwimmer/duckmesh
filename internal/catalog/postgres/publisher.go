package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

type PublishBatchInput struct {
	SnapshotID         int64
	TenantID           string
	TableID            int64
	BatchID            string
	EventIDs           []string
	CreatedBy          string
	MaxVisibilityToken int64
	DataFilePath       string
	RecordCount        int64
	FileSizeBytes      int64
	MinEventTime       *time.Time
	MaxEventTime       *time.Time
	StatsJSON          []byte
}

type PublishBatchResult struct {
	SnapshotID int64
	FileID     int64
}

type PublishCompactionInput struct {
	SnapshotID         int64
	TenantID           string
	TableID            int64
	CreatedBy          string
	MaxVisibilityToken int64
	DataFilePath       string
	RecordCount        int64
	FileSizeBytes      int64
	StatsJSON          []byte
	RemovedFileIDs     []int64
}

type PublishCompactionResult struct {
	SnapshotID   int64
	NewFileID    int64
	RemovedCount int
}

func (r *Repository) AllocateSnapshotID(ctx context.Context) (int64, error) {
	var snapshotID int64
	if err := r.db.QueryRowContext(ctx, `SELECT nextval(pg_get_serial_sequence('snapshot', 'snapshot_id'))`).Scan(&snapshotID); err != nil {
		return 0, fmt.Errorf("allocate snapshot id: %w", err)
	}
	return snapshotID, nil
}

func (r *Repository) PublishBatch(ctx context.Context, in PublishBatchInput) (PublishBatchResult, error) {
	if in.SnapshotID <= 0 {
		return PublishBatchResult{}, fmt.Errorf("snapshot id is required")
	}
	if in.TableID <= 0 {
		return PublishBatchResult{}, fmt.Errorf("table id is required")
	}
	if len(in.EventIDs) == 0 {
		return PublishBatchResult{}, fmt.Errorf("at least one event id is required")
	}
	if in.CreatedBy == "" {
		in.CreatedBy = "duckmesh-coordinator"
	}
	if len(in.StatsJSON) == 0 {
		in.StatsJSON = []byte("{}")
	}

	eventIDs, err := parseInt64Slice(in.EventIDs, "event id")
	if err != nil {
		return PublishBatchResult{}, err
	}
	batchID, err := strconv.ParseInt(in.BatchID, 10, 64)
	if err != nil {
		return PublishBatchResult{}, fmt.Errorf("invalid batch id %q: %w", in.BatchID, err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return PublishBatchResult{}, fmt.Errorf("begin publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var parentSnapshotID *int64
	if err := tx.QueryRowContext(ctx, `
SELECT snapshot_id
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT 1`, in.TenantID).Scan(&parentSnapshotID); err != nil && err != sql.ErrNoRows {
		return PublishBatchResult{}, fmt.Errorf("select parent snapshot: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot (snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id)
OVERRIDING SYSTEM VALUE
VALUES ($1, $2, $3, $4, $5)`, in.SnapshotID, in.TenantID, in.CreatedBy, in.MaxVisibilityToken, parentSnapshotID); err != nil {
		return PublishBatchResult{}, fmt.Errorf("insert snapshot: %w", err)
	}

	var fileID int64
	if err := tx.QueryRowContext(ctx, `
INSERT INTO data_file (tenant_id, table_id, path, format, record_count, file_size_bytes, min_event_time, max_event_time, stats_json)
VALUES ($1, $2, $3, 'parquet', $4, $5, $6, $7, $8::jsonb)
RETURNING file_id`, in.TenantID, in.TableID, in.DataFilePath, in.RecordCount, in.FileSizeBytes, in.MinEventTime, in.MaxEventTime, string(in.StatsJSON)).Scan(&fileID); err != nil {
		return PublishBatchResult{}, fmt.Errorf("insert data file: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_table_watermark (snapshot_id, table_id, max_visibility_token)
VALUES ($1, $2, $3)`, in.SnapshotID, in.TableID, in.MaxVisibilityToken); err != nil {
		return PublishBatchResult{}, fmt.Errorf("insert snapshot table watermark: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_file (snapshot_id, table_id, file_id, change_type)
VALUES ($1, $2, $3, 'add')`, in.SnapshotID, in.TableID, fileID); err != nil {
		return PublishBatchResult{}, fmt.Errorf("insert snapshot file: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE ingest_event AS e
SET state = 'committed', lease_owner = NULL, lease_until = NULL
FROM ingest_claim_item AS i
WHERE i.batch_id = $1 AND i.event_id = e.event_id AND e.event_id = ANY($2::bigint[])`, batchID, eventIDs); err != nil {
		return PublishBatchResult{}, fmt.Errorf("mark events committed: %w", err)
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
WHERE batch_id = $1`, batchID); err != nil {
		return PublishBatchResult{}, fmt.Errorf("update claim batch state: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return PublishBatchResult{}, fmt.Errorf("commit publish tx: %w", err)
	}

	return PublishBatchResult{SnapshotID: in.SnapshotID, FileID: fileID}, nil
}

func parseInt64Slice(values []string, field string) ([]int64, error) {
	parsed := make([]int64, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s %q: %w", field, value, err)
		}
		parsed = append(parsed, id)
	}
	return parsed, nil
}

func (r *Repository) PublishCompaction(ctx context.Context, in PublishCompactionInput) (PublishCompactionResult, error) {
	if in.SnapshotID <= 0 {
		return PublishCompactionResult{}, fmt.Errorf("snapshot id is required")
	}
	if in.TableID <= 0 {
		return PublishCompactionResult{}, fmt.Errorf("table id is required")
	}
	if in.CreatedBy == "" {
		in.CreatedBy = "duckmesh-compactor"
	}
	if len(in.StatsJSON) == 0 {
		in.StatsJSON = []byte("{}")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return PublishCompactionResult{}, fmt.Errorf("begin compaction publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var parentSnapshotID *int64
	if err := tx.QueryRowContext(ctx, `
SELECT snapshot_id
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT 1`, in.TenantID).Scan(&parentSnapshotID); err != nil && err != sql.ErrNoRows {
		return PublishCompactionResult{}, fmt.Errorf("select parent snapshot: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot (snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id)
OVERRIDING SYSTEM VALUE
VALUES ($1, $2, $3, $4, $5)`, in.SnapshotID, in.TenantID, in.CreatedBy, in.MaxVisibilityToken, parentSnapshotID); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("insert compaction snapshot: %w", err)
	}

	var newFileID int64
	if err := tx.QueryRowContext(ctx, `
INSERT INTO data_file (tenant_id, table_id, path, format, record_count, file_size_bytes, stats_json)
VALUES ($1, $2, $3, 'parquet', $4, $5, $6::jsonb)
RETURNING file_id`, in.TenantID, in.TableID, in.DataFilePath, in.RecordCount, in.FileSizeBytes, string(in.StatsJSON)).Scan(&newFileID); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("insert compacted data file: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_table_watermark (snapshot_id, table_id, max_visibility_token)
VALUES ($1, $2, $3)`, in.SnapshotID, in.TableID, in.MaxVisibilityToken); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("insert compaction watermark: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_file (snapshot_id, table_id, file_id, change_type)
VALUES ($1, $2, $3, 'add')`, in.SnapshotID, in.TableID, newFileID); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("insert compaction add file entry: %w", err)
	}

	for _, removedFileID := range in.RemovedFileIDs {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO snapshot_file (snapshot_id, table_id, file_id, change_type)
VALUES ($1, $2, $3, 'remove')`, in.SnapshotID, in.TableID, removedFileID); err != nil {
			return PublishCompactionResult{}, fmt.Errorf("insert compaction remove entry for file %d: %w", removedFileID, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO compaction_run (tenant_id, table_id, status, details_json, completed_at)
VALUES ($1, $2, 'completed', $3::jsonb, NOW())`, in.TenantID, in.TableID, string(in.StatsJSON)); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("insert compaction_run: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return PublishCompactionResult{}, fmt.Errorf("commit compaction publish tx: %w", err)
	}

	return PublishCompactionResult{
		SnapshotID:   in.SnapshotID,
		NewFileID:    newFileID,
		RemovedCount: len(in.RemovedFileIDs),
	}, nil
}
