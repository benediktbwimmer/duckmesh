package maintenance

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/storage"
)

func TestRunIntegrityCheckOnceSuccess(t *testing.T) {
	svc := &Service{
		Catalog: &fakeIntegrityCatalog{
			tenants: []catalog.Tenant{{TenantID: "t1", Status: "active"}},
			snapshotsByTenant: map[string][]catalog.Snapshot{
				"t1": {
					{SnapshotID: 2, TenantID: "t1"},
					{SnapshotID: 1, TenantID: "t1"},
				},
			},
			filesBySnapshot: map[string][]catalog.SnapshotFileEntry{
				"t1/2": {
					{FileID: 10, Path: "t1/events/a.parquet", FileSizeBytes: 100},
					{FileID: 11, Path: "t1/events/b.parquet", FileSizeBytes: 200},
				},
				"t1/1": {
					{FileID: 10, Path: "t1/events/a.parquet", FileSizeBytes: 100},
				},
			},
		},
		ObjectStore: &fakeIntegrityObjectStore{
			stats: map[string]storage.ObjectInfo{
				"t1/events/a.parquet": {Key: "t1/events/a.parquet", Size: 100},
				"t1/events/b.parquet": {Key: "t1/events/b.parquet", Size: 200},
			},
		},
		Config: Config{IntegritySnapshotLimit: 10},
	}

	summary, err := svc.RunIntegrityCheckOnce(context.Background(), "t1")
	if err != nil {
		t.Fatalf("RunIntegrityCheckOnce() error = %v", err)
	}
	if summary.TenantsScanned != 1 {
		t.Fatalf("TenantsScanned = %d", summary.TenantsScanned)
	}
	if summary.SnapshotsScanned != 2 {
		t.Fatalf("SnapshotsScanned = %d", summary.SnapshotsScanned)
	}
	if summary.ReferencedFiles != 3 {
		t.Fatalf("ReferencedFiles = %d", summary.ReferencedFiles)
	}
	if summary.UniqueFilesChecked != 2 {
		t.Fatalf("UniqueFilesChecked = %d", summary.UniqueFilesChecked)
	}
	if summary.MissingFiles != 0 || summary.SizeMismatchFiles != 0 || summary.OperationalFailures != 0 {
		t.Fatalf("unexpected summary values: %+v", summary)
	}
}

func TestRunIntegrityCheckOnceDetectsMissingFiles(t *testing.T) {
	svc := &Service{
		Catalog: &fakeIntegrityCatalog{
			tenants: []catalog.Tenant{{TenantID: "t1", Status: "active"}},
			snapshotsByTenant: map[string][]catalog.Snapshot{
				"t1": {{SnapshotID: 9, TenantID: "t1"}},
			},
			filesBySnapshot: map[string][]catalog.SnapshotFileEntry{
				"t1/9": {{FileID: 77, Path: "t1/events/missing.parquet", FileSizeBytes: 123}},
			},
		},
		ObjectStore: &fakeIntegrityObjectStore{
			statErrs: map[string]error{
				"t1/events/missing.parquet": storage.ErrObjectNotFound,
			},
		},
		Config: Config{IntegritySnapshotLimit: 10},
	}

	summary, err := svc.RunIntegrityCheckOnce(context.Background(), "t1")
	if err == nil {
		t.Fatal("expected integrity error")
	}
	if summary.MissingFiles != 1 {
		t.Fatalf("MissingFiles = %d, want 1", summary.MissingFiles)
	}
}

type fakeIntegrityCatalog struct {
	tenants           []catalog.Tenant
	snapshotsByTenant map[string][]catalog.Snapshot
	filesBySnapshot   map[string][]catalog.SnapshotFileEntry
}

func (f *fakeIntegrityCatalog) ListTenants(context.Context) ([]catalog.Tenant, error) {
	return f.tenants, nil
}

func (f *fakeIntegrityCatalog) ListTables(context.Context, string) ([]catalog.TableDef, error) {
	return nil, nil
}

func (f *fakeIntegrityCatalog) ListSnapshots(_ context.Context, tenantID string, limit int) ([]catalog.Snapshot, error) {
	snapshots := f.snapshotsByTenant[tenantID]
	if limit > 0 && len(snapshots) > limit {
		return snapshots[:limit], nil
	}
	return snapshots, nil
}

func (f *fakeIntegrityCatalog) GetLatestSnapshot(context.Context, string) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (f *fakeIntegrityCatalog) ListSnapshotFiles(_ context.Context, tenantID string, snapshotID int64) ([]catalog.SnapshotFileEntry, error) {
	return f.filesBySnapshot[tenantSnapshotKey(tenantID, snapshotID)], nil
}

func (f *fakeIntegrityCatalog) ListSnapshotFilesForTable(context.Context, string, int64, int64) ([]catalog.SnapshotFileEntry, error) {
	return nil, nil
}

func (f *fakeIntegrityCatalog) AllocateSnapshotID(context.Context) (int64, error) {
	return 0, errors.New("not implemented")
}

func (f *fakeIntegrityCatalog) PublishCompaction(context.Context, catalogpostgres.PublishCompactionInput) (catalogpostgres.PublishCompactionResult, error) {
	return catalogpostgres.PublishCompactionResult{}, errors.New("not implemented")
}

func (f *fakeIntegrityCatalog) ListGCFileCandidates(context.Context, string, int, time.Time) ([]catalogpostgres.GCFileCandidate, error) {
	return nil, nil
}

func (f *fakeIntegrityCatalog) DeleteDataFileByID(context.Context, int64) error {
	return nil
}

func (f *fakeIntegrityCatalog) RecordGCRun(context.Context, catalogpostgres.RecordGCRunInput) error {
	return nil
}

type fakeIntegrityObjectStore struct {
	stats    map[string]storage.ObjectInfo
	statErrs map[string]error
}

func (f *fakeIntegrityObjectStore) Put(context.Context, string, io.Reader, int64, storage.PutOptions) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, errors.New("not implemented")
}

func (f *fakeIntegrityObjectStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeIntegrityObjectStore) Stat(_ context.Context, key string) (storage.ObjectInfo, error) {
	if err := f.statErrs[key]; err != nil {
		return storage.ObjectInfo{}, err
	}
	info, ok := f.stats[key]
	if !ok {
		return storage.ObjectInfo{}, storage.ErrObjectNotFound
	}
	return info, nil
}

func (f *fakeIntegrityObjectStore) Delete(context.Context, string) error {
	return nil
}

func tenantSnapshotKey(tenantID string, snapshotID int64) string {
	return tenantID + "/" + strconv.FormatInt(snapshotID, 10)
}
