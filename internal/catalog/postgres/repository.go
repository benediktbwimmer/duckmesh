package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
)

type dbTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) HealthCheck(ctx context.Context) error {
	if err := r.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping catalog db: %w", err)
	}
	return nil
}

func (r *Repository) CreateTenant(ctx context.Context, in catalog.CreateTenantInput) (catalog.Tenant, error) {
	status := in.Status
	if status == "" {
		status = "active"
	}

	query := `
INSERT INTO tenant (tenant_id, name, status)
VALUES ($1, $2, $3)
RETURNING created_at`
	var createdAt time.Time
	if err := r.db.QueryRowContext(ctx, query, in.TenantID, in.Name, status).Scan(&createdAt); err != nil {
		return catalog.Tenant{}, fmt.Errorf("create tenant: %w", err)
	}
	return catalog.Tenant{
		TenantID:  in.TenantID,
		Name:      in.Name,
		Status:    status,
		CreatedAt: createdAt,
	}, nil
}

func (r *Repository) GetTenant(ctx context.Context, tenantID string) (catalog.Tenant, error) {
	query := `
SELECT tenant_id, name, status, created_at
FROM tenant
WHERE tenant_id = $1`

	var tenant catalog.Tenant
	if err := r.db.QueryRowContext(ctx, query, tenantID).Scan(
		&tenant.TenantID,
		&tenant.Name,
		&tenant.Status,
		&tenant.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return catalog.Tenant{}, catalog.ErrNotFound
		}
		return catalog.Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return tenant, nil
}

func (r *Repository) ListTenants(ctx context.Context) ([]catalog.Tenant, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT tenant_id, name, status, created_at
FROM tenant
WHERE status = 'active'
ORDER BY tenant_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tenants := make([]catalog.Tenant, 0)
	for rows.Next() {
		var tenant catalog.Tenant
		if err := rows.Scan(&tenant.TenantID, &tenant.Name, &tenant.Status, &tenant.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan tenant row: %w", err)
		}
		tenants = append(tenants, tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant rows: %w", err)
	}
	return tenants, nil
}

func (r *Repository) CreateAPIKey(ctx context.Context, in catalog.CreateAPIKeyInput) (catalog.APIKey, error) {
	query := `
INSERT INTO api_key (key_id, tenant_id, key_hash, role)
VALUES ($1, $2, $3, $4)
RETURNING created_at, revoked_at`

	var key catalog.APIKey
	key.KeyID = in.KeyID
	key.TenantID = in.TenantID
	key.KeyHash = in.KeyHash
	key.Role = in.Role

	if err := r.db.QueryRowContext(ctx, query, in.KeyID, in.TenantID, in.KeyHash, in.Role).Scan(&key.CreatedAt, &key.RevokedAt); err != nil {
		return catalog.APIKey{}, fmt.Errorf("create api key: %w", err)
	}
	return key, nil
}

func (r *Repository) CreateTable(ctx context.Context, in catalog.CreateTableInput) (catalog.TableDef, error) {
	pkCols := in.PrimaryKeyCols
	if len(pkCols) == 0 {
		pkCols = []byte("[]")
	}
	partitionSpec := in.PartitionSpec
	if len(partitionSpec) == 0 {
		partitionSpec = []byte("{}")
	}
	schemaVersion := in.SchemaVersion
	if schemaVersion <= 0 {
		schemaVersion = 1
	}

	query := `
INSERT INTO table_def (tenant_id, table_name, primary_key_cols, partition_spec, schema_version)
VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
RETURNING table_id, created_at`

	var table catalog.TableDef
	table.TenantID = in.TenantID
	table.TableName = in.TableName
	table.PrimaryKeyCols = pkCols
	table.PartitionSpec = partitionSpec
	table.SchemaVersion = schemaVersion

	if err := r.db.QueryRowContext(ctx, query, in.TenantID, in.TableName, string(pkCols), string(partitionSpec), schemaVersion).Scan(&table.TableID, &table.CreatedAt); err != nil {
		return catalog.TableDef{}, fmt.Errorf("create table: %w", err)
	}
	return table, nil
}

func (r *Repository) GetTableByName(ctx context.Context, tenantID, tableName string) (catalog.TableDef, error) {
	query := `
SELECT table_id, tenant_id, table_name, primary_key_cols, partition_spec, schema_version, created_at
FROM table_def
WHERE tenant_id = $1 AND table_name = $2`

	var table catalog.TableDef
	if err := r.db.QueryRowContext(ctx, query, tenantID, tableName).Scan(
		&table.TableID,
		&table.TenantID,
		&table.TableName,
		&table.PrimaryKeyCols,
		&table.PartitionSpec,
		&table.SchemaVersion,
		&table.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return catalog.TableDef{}, catalog.ErrNotFound
		}
		return catalog.TableDef{}, fmt.Errorf("get table by name: %w", err)
	}
	return table, nil
}

func (r *Repository) ListTables(ctx context.Context, tenantID string) ([]catalog.TableDef, error) {
	query := `
SELECT table_id, tenant_id, table_name, primary_key_cols, partition_spec, schema_version, created_at
FROM table_def
WHERE tenant_id = $1
ORDER BY table_name ASC`

	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tables := make([]catalog.TableDef, 0)
	for rows.Next() {
		var table catalog.TableDef
		if err := rows.Scan(
			&table.TableID,
			&table.TenantID,
			&table.TableName,
			&table.PrimaryKeyCols,
			&table.PartitionSpec,
			&table.SchemaVersion,
			&table.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table rows: %w", err)
	}
	return tables, nil
}

func (r *Repository) DeleteTableByName(ctx context.Context, tenantID, tableName string) (bool, error) {
	result, err := r.db.ExecContext(ctx, `
DELETE FROM table_def
WHERE tenant_id = $1 AND table_name = $2`, tenantID, tableName)
	if err != nil {
		return false, fmt.Errorf("delete table by name: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete table by name rows affected: %w", err)
	}
	return rows > 0, nil
}

func (r *Repository) SetTableSchemaVersion(ctx context.Context, tableID int64, schemaVersion int) error {
	result, err := r.db.ExecContext(ctx, `
UPDATE table_def
SET schema_version = $2
WHERE table_id = $1`, tableID, schemaVersion)
	if err != nil {
		return fmt.Errorf("set table schema version: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set table schema version rows affected: %w", err)
	}
	if rows == 0 {
		return catalog.ErrNotFound
	}
	return nil
}

func (r *Repository) UpsertTableSchemaVersion(ctx context.Context, in catalog.UpsertTableSchemaVersionInput) (catalog.TableSchemaVersion, error) {
	schemaJSON := in.SchemaJSON
	if len(schemaJSON) == 0 {
		schemaJSON = []byte("{}")
	}
	compatibility := in.CompatibilityMode
	if compatibility == "" {
		compatibility = "backward"
	}

	query := `
INSERT INTO table_schema_version (table_id, schema_version, schema_json, compatibility_mode)
VALUES ($1, $2, $3::jsonb, $4)
ON CONFLICT (table_id, schema_version)
DO UPDATE SET
    schema_json = EXCLUDED.schema_json,
    compatibility_mode = EXCLUDED.compatibility_mode
RETURNING created_at`

	var createdAt time.Time
	if err := r.db.QueryRowContext(ctx, query, in.TableID, in.SchemaVersion, string(schemaJSON), compatibility).Scan(&createdAt); err != nil {
		return catalog.TableSchemaVersion{}, fmt.Errorf("upsert table schema version: %w", err)
	}
	return catalog.TableSchemaVersion{
		TableID:           in.TableID,
		SchemaVersion:     in.SchemaVersion,
		SchemaJSON:        schemaJSON,
		CompatibilityMode: compatibility,
		CreatedAt:         createdAt,
	}, nil
}

func (r *Repository) InsertIngestEvent(ctx context.Context, in catalog.InsertIngestEventInput) (catalog.InsertIngestEventResult, error) {
	payload := in.PayloadJSON
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	query := `
INSERT INTO ingest_event (tenant_id, table_id, idempotency_key, op, payload_json, event_time, state)
VALUES ($1, $2, $3, $4::duckmesh_ingest_op, $5::jsonb, $6, 'accepted')
ON CONFLICT (tenant_id, table_id, idempotency_key)
DO UPDATE SET idempotency_key = ingest_event.idempotency_key
RETURNING event_id, (xmax = 0) AS inserted, ingested_at`

	var result catalog.InsertIngestEventResult
	if err := r.db.QueryRowContext(ctx, query, in.TenantID, in.TableID, in.IdempotencyKey, in.Op, string(payload), in.EventTime).Scan(
		&result.EventID,
		&result.Inserted,
		&result.IngestedAt,
	); err != nil {
		return catalog.InsertIngestEventResult{}, fmt.Errorf("insert ingest event: %w", err)
	}
	return result, nil
}

func (r *Repository) CreateSnapshot(ctx context.Context, in catalog.CreateSnapshotInput) (catalog.Snapshot, error) {
	query := `
INSERT INTO snapshot (tenant_id, created_by, max_visibility_token, parent_snapshot_id)
VALUES ($1, $2, $3, $4)
RETURNING snapshot_id, created_at`

	var snapshot catalog.Snapshot
	snapshot.TenantID = in.TenantID
	snapshot.CreatedBy = in.CreatedBy
	snapshot.MaxVisibilityToken = in.MaxVisibilityToken
	snapshot.ParentSnapshotID = in.ParentSnapshotID
	if err := r.db.QueryRowContext(ctx, query, in.TenantID, in.CreatedBy, in.MaxVisibilityToken, in.ParentSnapshotID).Scan(
		&snapshot.SnapshotID,
		&snapshot.CreatedAt,
	); err != nil {
		return catalog.Snapshot{}, fmt.Errorf("create snapshot: %w", err)
	}
	return snapshot, nil
}

func (r *Repository) ListSnapshots(ctx context.Context, tenantID string, limit int) ([]catalog.Snapshot, error) {
	query := `
SELECT snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id, created_at
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC`

	var rows *sql.Rows
	var err error
	if limit > 0 {
		rows, err = r.db.QueryContext(ctx, query+`
LIMIT $2`, tenantID, limit)
	} else {
		rows, err = r.db.QueryContext(ctx, query, tenantID)
	}
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	snapshots := make([]catalog.Snapshot, 0)
	for rows.Next() {
		var snapshot catalog.Snapshot
		if err := rows.Scan(
			&snapshot.SnapshotID,
			&snapshot.TenantID,
			&snapshot.CreatedBy,
			&snapshot.MaxVisibilityToken,
			&snapshot.ParentSnapshotID,
			&snapshot.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan snapshot row: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot rows: %w", err)
	}
	return snapshots, nil
}

func (r *Repository) GetLatestSnapshot(ctx context.Context, tenantID string) (catalog.Snapshot, error) {
	query := `
SELECT snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id, created_at
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT 1`
	return scanSnapshot(r.db.QueryRowContext(ctx, query, tenantID))
}

func (r *Repository) GetSnapshotByID(ctx context.Context, tenantID string, snapshotID int64) (catalog.Snapshot, error) {
	query := `
SELECT snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id, created_at
FROM snapshot
WHERE tenant_id = $1 AND snapshot_id = $2`
	return scanSnapshot(r.db.QueryRowContext(ctx, query, tenantID, snapshotID))
}

func (r *Repository) GetSnapshotByTime(ctx context.Context, tenantID string, at time.Time) (catalog.Snapshot, error) {
	query := `
SELECT snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id, created_at
FROM snapshot
WHERE tenant_id = $1 AND created_at <= $2
ORDER BY created_at DESC
LIMIT 1`
	return scanSnapshot(r.db.QueryRowContext(ctx, query, tenantID, at.UTC()))
}

func (r *Repository) ListSnapshotFiles(ctx context.Context, tenantID string, snapshotID int64) ([]catalog.SnapshotFileEntry, error) {
	query := `
SELECT sf.table_id, td.table_name, sf.file_id, df.path, df.file_size_bytes, df.record_count
FROM snapshot_file AS sf
JOIN table_def AS td ON td.table_id = sf.table_id
JOIN data_file AS df ON df.file_id = sf.file_id
WHERE sf.change_type = 'add'
  AND td.tenant_id = $1
  AND sf.snapshot_id <= $2
  AND NOT EXISTS (
      SELECT 1
      FROM snapshot_file AS sf_remove
      WHERE sf_remove.table_id = sf.table_id
        AND sf_remove.file_id = sf.file_id
        AND sf_remove.change_type = 'remove'
        AND sf_remove.snapshot_id <= $2
  )
ORDER BY td.table_name ASC, sf.file_id ASC`

	rows, err := r.db.QueryContext(ctx, query, tenantID, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("list snapshot files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	files := make([]catalog.SnapshotFileEntry, 0)
	for rows.Next() {
		var file catalog.SnapshotFileEntry
		if err := rows.Scan(
			&file.TableID,
			&file.TableName,
			&file.FileID,
			&file.Path,
			&file.FileSizeBytes,
			&file.RecordCount,
		); err != nil {
			return nil, fmt.Errorf("scan snapshot file row: %w", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot files: %w", err)
	}
	return files, nil
}

func (r *Repository) ListSnapshotFilesForTable(ctx context.Context, tenantID string, snapshotID, tableID int64) ([]catalog.SnapshotFileEntry, error) {
	query := `
SELECT sf.table_id, td.table_name, sf.file_id, df.path, df.file_size_bytes, df.record_count
FROM snapshot_file AS sf
JOIN table_def AS td ON td.table_id = sf.table_id
JOIN data_file AS df ON df.file_id = sf.file_id
WHERE sf.change_type = 'add'
  AND td.tenant_id = $1
  AND sf.table_id = $2
  AND sf.snapshot_id <= $3
  AND NOT EXISTS (
      SELECT 1
      FROM snapshot_file AS sf_remove
      WHERE sf_remove.table_id = sf.table_id
        AND sf_remove.file_id = sf.file_id
        AND sf_remove.change_type = 'remove'
        AND sf_remove.snapshot_id <= $3
  )
ORDER BY sf.file_id ASC`

	rows, err := r.db.QueryContext(ctx, query, tenantID, tableID, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("list snapshot files for table: %w", err)
	}
	defer func() { _ = rows.Close() }()

	files := make([]catalog.SnapshotFileEntry, 0)
	for rows.Next() {
		var file catalog.SnapshotFileEntry
		if err := rows.Scan(&file.TableID, &file.TableName, &file.FileID, &file.Path, &file.FileSizeBytes, &file.RecordCount); err != nil {
			return nil, fmt.Errorf("scan snapshot table file row: %w", err)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot table file rows: %w", err)
	}
	return files, nil
}

type GCFileCandidate struct {
	FileID int64
	Path   string
}

type RecordGCRunInput struct {
	TenantID    string
	Status      string
	DetailsJSON []byte
}

func (r *Repository) ListGCFileCandidates(ctx context.Context, tenantID string, keepSnapshots int, olderThan time.Time) ([]GCFileCandidate, error) {
	if keepSnapshots < 1 {
		keepSnapshots = 1
	}
	offset := keepSnapshots - 1

	var minKeepSnapshotID int64
	if err := r.db.QueryRowContext(ctx, `
SELECT snapshot_id
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
OFFSET $2
LIMIT 1`, tenantID, offset).Scan(&minKeepSnapshotID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("resolve minimum keep snapshot: %w", err)
	}

	rows, err := r.db.QueryContext(ctx, `
WITH latest_change AS (
    SELECT sf.file_id, sf.snapshot_id, sf.change_type,
           ROW_NUMBER() OVER (PARTITION BY sf.file_id ORDER BY sf.snapshot_id DESC) AS rn
    FROM snapshot_file AS sf
    JOIN data_file AS df ON df.file_id = sf.file_id
    WHERE df.tenant_id = $1
)
SELECT df.file_id, df.path
FROM latest_change AS lc
JOIN data_file AS df ON df.file_id = lc.file_id
WHERE lc.rn = 1
  AND lc.change_type = 'remove'
  AND lc.snapshot_id < $2
  AND df.created_at <= $3
ORDER BY df.file_id ASC`, tenantID, minKeepSnapshotID, olderThan.UTC())
	if err != nil {
		return nil, fmt.Errorf("list gc candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	candidates := make([]GCFileCandidate, 0)
	for rows.Next() {
		var candidate GCFileCandidate
		if err := rows.Scan(&candidate.FileID, &candidate.Path); err != nil {
			return nil, fmt.Errorf("scan gc candidate row: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate gc candidate rows: %w", err)
	}
	return candidates, nil
}

func (r *Repository) DeleteDataFileByID(ctx context.Context, fileID int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM data_file WHERE file_id = $1`, fileID); err != nil {
		return fmt.Errorf("delete data file %d: %w", fileID, err)
	}
	return nil
}

func (r *Repository) GetIngestLagStats(ctx context.Context, tenantID string) (catalog.IngestLagStats, error) {
	var stats catalog.IngestLagStats

	if err := r.db.QueryRowContext(ctx, `
SELECT
    COALESCE(SUM(CASE WHEN state = 'accepted' THEN 1 ELSE 0 END), 0) AS accepted_events,
    COALESCE(SUM(CASE WHEN state = 'claimed' THEN 1 ELSE 0 END), 0) AS claimed_events,
    MIN(CASE WHEN state IN ('accepted', 'claimed') THEN ingested_at END) AS oldest_pending_ingested_at,
    COALESCE(MAX(CASE WHEN state IN ('accepted', 'claimed') THEN event_id ELSE NULL END), 0) AS max_pending_token
FROM ingest_event
WHERE tenant_id = $1`, tenantID).Scan(
		&stats.AcceptedEvents,
		&stats.ClaimedEvents,
		&stats.OldestPendingIngestAt,
		&stats.MaxPendingToken,
	); err != nil {
		return catalog.IngestLagStats{}, fmt.Errorf("query ingest lag counters: %w", err)
	}

	var snapshotAt sql.NullTime
	if err := r.db.QueryRowContext(ctx, `
SELECT snapshot_id, max_visibility_token, created_at
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT 1`, tenantID).Scan(&stats.LatestSnapshotID, &stats.LatestVisibilityToken, &snapshotAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return catalog.IngestLagStats{}, fmt.Errorf("query latest snapshot lag values: %w", err)
		}
	} else if snapshotAt.Valid {
		t := snapshotAt.Time
		stats.LatestSnapshotAt = &t
	}

	return stats, nil
}

func (r *Repository) RecordGCRun(ctx context.Context, in RecordGCRunInput) error {
	status := in.Status
	if status == "" {
		status = "completed"
	}
	details := in.DetailsJSON
	if len(details) == 0 {
		details = []byte("{}")
	}
	if _, err := r.db.ExecContext(ctx, `
INSERT INTO gc_run (tenant_id, status, details_json, completed_at)
VALUES ($1, $2, $3::jsonb, NOW())`, in.TenantID, status, string(details)); err != nil {
		return fmt.Errorf("record gc_run: %w", err)
	}
	return nil
}

func (r *Repository) UpsertSnapshotTableWatermark(ctx context.Context, in catalog.UpsertSnapshotTableWatermarkInput) error {
	query := `
INSERT INTO snapshot_table_watermark (snapshot_id, table_id, max_visibility_token)
VALUES ($1, $2, $3)
ON CONFLICT (snapshot_id, table_id)
DO UPDATE SET max_visibility_token = GREATEST(
    snapshot_table_watermark.max_visibility_token,
    EXCLUDED.max_visibility_token
)`
	if _, err := r.db.ExecContext(ctx, query, in.SnapshotID, in.TableID, in.MaxVisibilityToken); err != nil {
		return fmt.Errorf("upsert snapshot watermark: %w", err)
	}
	return nil
}

func (r *Repository) RegisterDataFile(ctx context.Context, in catalog.RegisterDataFileInput) (catalog.DataFile, error) {
	stats := in.StatsJSON
	if len(stats) == 0 {
		stats = []byte("{}")
	}
	format := in.Format
	if format == "" {
		format = "parquet"
	}

	query := `
INSERT INTO data_file (tenant_id, table_id, path, format, record_count, file_size_bytes, min_event_time, max_event_time, stats_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
RETURNING file_id, created_at`

	var file catalog.DataFile
	file.TenantID = in.TenantID
	file.TableID = in.TableID
	file.Path = in.Path
	file.Format = format
	file.RecordCount = in.RecordCount
	file.FileSizeBytes = in.FileSizeBytes
	file.MinEventTime = in.MinEventTime
	file.MaxEventTime = in.MaxEventTime
	file.StatsJSON = stats

	if err := r.db.QueryRowContext(ctx, query,
		in.TenantID,
		in.TableID,
		in.Path,
		format,
		in.RecordCount,
		in.FileSizeBytes,
		in.MinEventTime,
		in.MaxEventTime,
		string(stats),
	).Scan(&file.FileID, &file.CreatedAt); err != nil {
		return catalog.DataFile{}, fmt.Errorf("register data file: %w", err)
	}
	return file, nil
}

func (r *Repository) AddSnapshotFile(ctx context.Context, in catalog.AddSnapshotFileInput) error {
	query := `
INSERT INTO snapshot_file (snapshot_id, table_id, file_id, change_type)
VALUES ($1, $2, $3, $4::duckmesh_change_type)
ON CONFLICT DO NOTHING`

	if _, err := r.db.ExecContext(ctx, query, in.SnapshotID, in.TableID, in.FileID, string(in.ChangeType)); err != nil {
		return fmt.Errorf("add snapshot file: %w", err)
	}
	return nil
}

func (r *Repository) WithTx(ctx context.Context, fn func(tx *TxRepository) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	txRepo := &TxRepository{q: tx}
	if err := fn(txRepo); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

type TxRepository struct {
	q dbTX
}

func (r *TxRepository) UpsertSnapshotTableWatermark(ctx context.Context, in catalog.UpsertSnapshotTableWatermarkInput) error {
	query := `
INSERT INTO snapshot_table_watermark (snapshot_id, table_id, max_visibility_token)
VALUES ($1, $2, $3)
ON CONFLICT (snapshot_id, table_id)
DO UPDATE SET max_visibility_token = GREATEST(
    snapshot_table_watermark.max_visibility_token,
    EXCLUDED.max_visibility_token
)`
	if _, err := r.q.ExecContext(ctx, query, in.SnapshotID, in.TableID, in.MaxVisibilityToken); err != nil {
		return fmt.Errorf("upsert snapshot watermark in tx: %w", err)
	}
	return nil
}

func (r *TxRepository) AddSnapshotFile(ctx context.Context, in catalog.AddSnapshotFileInput) error {
	query := `
INSERT INTO snapshot_file (snapshot_id, table_id, file_id, change_type)
VALUES ($1, $2, $3, $4::duckmesh_change_type)
ON CONFLICT DO NOTHING`

	if _, err := r.q.ExecContext(ctx, query, in.SnapshotID, in.TableID, in.FileID, string(in.ChangeType)); err != nil {
		return fmt.Errorf("add snapshot file in tx: %w", err)
	}
	return nil
}

func scanSnapshot(row *sql.Row) (catalog.Snapshot, error) {
	var snapshot catalog.Snapshot
	if err := row.Scan(
		&snapshot.SnapshotID,
		&snapshot.TenantID,
		&snapshot.CreatedBy,
		&snapshot.MaxVisibilityToken,
		&snapshot.ParentSnapshotID,
		&snapshot.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return catalog.Snapshot{}, catalog.ErrNotFound
		}
		return catalog.Snapshot{}, fmt.Errorf("scan snapshot: %w", err)
	}
	return snapshot, nil
}
