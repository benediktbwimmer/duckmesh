package catalog

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("catalog: not found")

type Repository interface {
	HealthCheck(ctx context.Context) error
	CreateTenant(ctx context.Context, in CreateTenantInput) (Tenant, error)
	GetTenant(ctx context.Context, tenantID string) (Tenant, error)
	CreateAPIKey(ctx context.Context, in CreateAPIKeyInput) (APIKey, error)
	CreateTable(ctx context.Context, in CreateTableInput) (TableDef, error)
	GetTableByName(ctx context.Context, tenantID, tableName string) (TableDef, error)
	ListTables(ctx context.Context, tenantID string) ([]TableDef, error)
	DeleteTableByName(ctx context.Context, tenantID, tableName string) (bool, error)
	SetTableSchemaVersion(ctx context.Context, tableID int64, schemaVersion int) error
	UpsertTableSchemaVersion(ctx context.Context, in UpsertTableSchemaVersionInput) (TableSchemaVersion, error)
	InsertIngestEvent(ctx context.Context, in InsertIngestEventInput) (InsertIngestEventResult, error)
	CreateSnapshot(ctx context.Context, in CreateSnapshotInput) (Snapshot, error)
	ListSnapshots(ctx context.Context, tenantID string, limit int) ([]Snapshot, error)
	GetLatestSnapshot(ctx context.Context, tenantID string) (Snapshot, error)
	GetSnapshotByID(ctx context.Context, tenantID string, snapshotID int64) (Snapshot, error)
	GetSnapshotByTime(ctx context.Context, tenantID string, at time.Time) (Snapshot, error)
	ListSnapshotFiles(ctx context.Context, tenantID string, snapshotID int64) ([]SnapshotFileEntry, error)
	UpsertSnapshotTableWatermark(ctx context.Context, in UpsertSnapshotTableWatermarkInput) error
	RegisterDataFile(ctx context.Context, in RegisterDataFileInput) (DataFile, error)
	AddSnapshotFile(ctx context.Context, in AddSnapshotFileInput) error
}

type Tenant struct {
	TenantID  string
	Name      string
	Status    string
	CreatedAt time.Time
}

type APIKey struct {
	KeyID     string
	TenantID  string
	KeyHash   string
	Role      string
	CreatedAt time.Time
	RevokedAt *time.Time
}

type TableDef struct {
	TableID        int64
	TenantID       string
	TableName      string
	PrimaryKeyCols []byte
	PartitionSpec  []byte
	SchemaVersion  int
	CreatedAt      time.Time
}

type TableSchemaVersion struct {
	TableID           int64
	SchemaVersion     int
	SchemaJSON        []byte
	CompatibilityMode string
	CreatedAt         time.Time
}

type InsertIngestEventResult struct {
	EventID    int64
	Inserted   bool
	IngestedAt time.Time
}

type Snapshot struct {
	SnapshotID         int64
	TenantID           string
	CreatedBy          string
	MaxVisibilityToken int64
	ParentSnapshotID   *int64
	CreatedAt          time.Time
}

type DataFile struct {
	FileID        int64
	TenantID      string
	TableID       int64
	Path          string
	Format        string
	RecordCount   int64
	FileSizeBytes int64
	MinEventTime  *time.Time
	MaxEventTime  *time.Time
	StatsJSON     []byte
	CreatedAt     time.Time
}

type SnapshotFileEntry struct {
	TableID       int64
	TableName     string
	FileID        int64
	Path          string
	FileSizeBytes int64
	RecordCount   int64
}

type SnapshotChangeType string

const (
	SnapshotChangeAdd    SnapshotChangeType = "add"
	SnapshotChangeRemove SnapshotChangeType = "remove"
)

type IngestLagStats struct {
	AcceptedEvents        int64
	ClaimedEvents         int64
	OldestPendingIngestAt *time.Time
	MaxPendingToken       int64
	LatestSnapshotID      *int64
	LatestSnapshotAt      *time.Time
	LatestVisibilityToken int64
}

type CreateTenantInput struct {
	TenantID string
	Name     string
	Status   string
}

type CreateAPIKeyInput struct {
	KeyID    string
	TenantID string
	KeyHash  string
	Role     string
}

type CreateTableInput struct {
	TenantID       string
	TableName      string
	PrimaryKeyCols []byte
	PartitionSpec  []byte
	SchemaVersion  int
}

type UpsertTableSchemaVersionInput struct {
	TableID           int64
	SchemaVersion     int
	SchemaJSON        []byte
	CompatibilityMode string
}

type InsertIngestEventInput struct {
	TenantID       string
	TableID        int64
	IdempotencyKey string
	Op             string
	PayloadJSON    []byte
	EventTime      *time.Time
}

type CreateSnapshotInput struct {
	TenantID           string
	CreatedBy          string
	MaxVisibilityToken int64
	ParentSnapshotID   *int64
}

type UpsertSnapshotTableWatermarkInput struct {
	SnapshotID         int64
	TableID            int64
	MaxVisibilityToken int64
}

type RegisterDataFileInput struct {
	TenantID      string
	TableID       int64
	Path          string
	Format        string
	RecordCount   int64
	FileSizeBytes int64
	MinEventTime  *time.Time
	MaxEventTime  *time.Time
	StatsJSON     []byte
}

type AddSnapshotFileInput struct {
	SnapshotID int64
	TableID    int64
	FileID     int64
	ChangeType SnapshotChangeType
}
