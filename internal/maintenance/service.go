package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/storage"
)

type Catalog interface {
	ListTenants(ctx context.Context) ([]catalog.Tenant, error)
	ListTables(ctx context.Context, tenantID string) ([]catalog.TableDef, error)
	ListSnapshots(ctx context.Context, tenantID string, limit int) ([]catalog.Snapshot, error)
	GetLatestSnapshot(ctx context.Context, tenantID string) (catalog.Snapshot, error)
	ListSnapshotFiles(ctx context.Context, tenantID string, snapshotID int64) ([]catalog.SnapshotFileEntry, error)
	ListSnapshotFilesForTable(ctx context.Context, tenantID string, snapshotID, tableID int64) ([]catalog.SnapshotFileEntry, error)
	AllocateSnapshotID(ctx context.Context) (int64, error)
	PublishCompaction(ctx context.Context, in catalogpostgres.PublishCompactionInput) (catalogpostgres.PublishCompactionResult, error)
	ListGCFileCandidates(ctx context.Context, tenantID string, keepSnapshots int, olderThan time.Time) ([]catalogpostgres.GCFileCandidate, error)
	DeleteDataFileByID(ctx context.Context, fileID int64) error
	RecordGCRun(ctx context.Context, in catalogpostgres.RecordGCRunInput) error
}

type Config struct {
	CompactionInterval      time.Duration
	CompactionMinInputFiles int
	RetentionInterval       time.Duration
	IntegritySnapshotLimit  int
	KeepSnapshots           int
	GCSafetyAge             time.Duration
	CreatedBy               string
}

type Service struct {
	Catalog     Catalog
	ObjectStore storage.ObjectStore
	Config      Config
	Logger      *slog.Logger
	Clock       func() time.Time
}

type CompactionSummary struct {
	TenantsScanned      int   `json:"tenants_scanned"`
	TablesScanned       int   `json:"tables_scanned"`
	TablesCompacted     int   `json:"tables_compacted"`
	InputFilesCompacted int   `json:"input_files_compacted"`
	BytesRewritten      int64 `json:"bytes_rewritten"`
	SnapshotsPublished  int   `json:"snapshots_published"`
	Failures            int   `json:"failures"`
}

type RetentionSummary struct {
	TenantsScanned int `json:"tenants_scanned"`
	CandidateFiles int `json:"candidate_files"`
	FilesDeleted   int `json:"files_deleted"`
	Failures       int `json:"failures"`
}

type IntegritySummary struct {
	TenantsScanned      int `json:"tenants_scanned"`
	SnapshotsScanned    int `json:"snapshots_scanned"`
	ReferencedFiles     int `json:"referenced_files"`
	UniqueFilesChecked  int `json:"unique_files_checked"`
	MissingFiles        int `json:"missing_files"`
	SizeMismatchFiles   int `json:"size_mismatch_files"`
	OperationalFailures int `json:"operational_failures"`
}

func (s *Service) Run(ctx context.Context) error {
	s.ensureDefaults()

	compactionTicker := time.NewTicker(s.Config.CompactionInterval)
	defer compactionTicker.Stop()
	retentionTicker := time.NewTicker(s.Config.RetentionInterval)
	defer retentionTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-compactionTicker.C:
			summary, err := s.RunCompactionOnce(ctx, "")
			if err != nil {
				if s.Logger != nil {
					s.Logger.ErrorContext(ctx, "compaction cycle failed", slog.Any("error", err), slog.Any("summary", summary))
				}
				continue
			}
			if s.Logger != nil {
				s.Logger.InfoContext(ctx, "compaction cycle completed", slog.Any("summary", summary))
			}
		case <-retentionTicker.C:
			summary, err := s.RunRetentionOnce(ctx, "")
			if err != nil {
				if s.Logger != nil {
					s.Logger.ErrorContext(ctx, "retention cycle failed", slog.Any("error", err), slog.Any("summary", summary))
				}
				continue
			}
			if s.Logger != nil {
				s.Logger.InfoContext(ctx, "retention cycle completed", slog.Any("summary", summary))
			}
		}
	}
}

func (s *Service) RunCompactionOnce(ctx context.Context, tenantID string) (CompactionSummary, error) {
	s.ensureDefaults()
	if s.Catalog == nil {
		return CompactionSummary{}, fmt.Errorf("catalog is required")
	}
	if s.ObjectStore == nil {
		return CompactionSummary{}, fmt.Errorf("object store is required")
	}

	tenants, err := s.listTargetTenants(ctx, tenantID)
	if err != nil {
		return CompactionSummary{}, err
	}

	summary := CompactionSummary{TenantsScanned: len(tenants)}
	failures := make([]string, 0)

	for _, tenant := range tenants {
		tables, err := s.Catalog.ListTables(ctx, tenant.TenantID)
		if err != nil {
			summary.Failures++
			failures = append(failures, fmt.Sprintf("tenant %s list tables: %v", tenant.TenantID, err))
			continue
		}

		snapshot, err := s.Catalog.GetLatestSnapshot(ctx, tenant.TenantID)
		if err != nil {
			if errors.Is(err, catalog.ErrNotFound) {
				continue
			}
			summary.Failures++
			failures = append(failures, fmt.Sprintf("tenant %s latest snapshot: %v", tenant.TenantID, err))
			continue
		}

		for _, table := range tables {
			summary.TablesScanned++
			files, err := s.Catalog.ListSnapshotFilesForTable(ctx, tenant.TenantID, snapshot.SnapshotID, table.TableID)
			if err != nil {
				summary.Failures++
				failures = append(failures, fmt.Sprintf("tenant %s table %s snapshot files: %v", tenant.TenantID, table.TableName, err))
				continue
			}
			if len(files) < s.Config.CompactionMinInputFiles {
				continue
			}

			tableSummary, err := s.compactTable(ctx, tenant.TenantID, table.TableID, table.TableName, snapshot.MaxVisibilityToken, files)
			if err != nil {
				summary.Failures++
				failures = append(failures, fmt.Sprintf("tenant %s table %s compaction: %v", tenant.TenantID, table.TableName, err))
				continue
			}
			summary.TablesCompacted++
			summary.InputFilesCompacted += tableSummary.inputFiles
			summary.BytesRewritten += tableSummary.bytesRewritten
			summary.SnapshotsPublished++
		}
	}

	if len(failures) > 0 {
		if summary.BytesRewritten > 0 {
			compactionBytesRewritten.Add(float64(summary.BytesRewritten))
		}
		compactionRunsTotal.WithLabelValues("failed").Inc()
		return summary, fmt.Errorf("compaction encountered %d failure(s): %s", len(failures), strings.Join(failures, "; "))
	}
	if summary.BytesRewritten > 0 {
		compactionBytesRewritten.Add(float64(summary.BytesRewritten))
	}
	compactionRunsTotal.WithLabelValues("completed").Inc()
	return summary, nil
}

func (s *Service) RunRetentionOnce(ctx context.Context, tenantID string) (RetentionSummary, error) {
	s.ensureDefaults()
	if s.Catalog == nil {
		return RetentionSummary{}, fmt.Errorf("catalog is required")
	}
	if s.ObjectStore == nil {
		return RetentionSummary{}, fmt.Errorf("object store is required")
	}

	tenants, err := s.listTargetTenants(ctx, tenantID)
	if err != nil {
		return RetentionSummary{}, err
	}

	summary := RetentionSummary{TenantsScanned: len(tenants)}
	failures := make([]string, 0)
	cutoff := s.Clock().Add(-s.Config.GCSafetyAge)

	for _, tenant := range tenants {
		candidates, err := s.Catalog.ListGCFileCandidates(ctx, tenant.TenantID, s.Config.KeepSnapshots, cutoff)
		if err != nil {
			summary.Failures++
			failures = append(failures, fmt.Sprintf("tenant %s gc candidates: %v", tenant.TenantID, err))
			_ = s.recordGCRun(ctx, tenant.TenantID, "failed", 0, 0, err)
			continue
		}
		summary.CandidateFiles += len(candidates)

		deleted := 0
		var tenantErr error
		for _, candidate := range candidates {
			if err := s.ObjectStore.Delete(ctx, candidate.Path); err != nil {
				summary.Failures++
				failures = append(failures, fmt.Sprintf("tenant %s delete object %s: %v", tenant.TenantID, candidate.Path, err))
				tenantErr = err
				continue
			}
			if err := s.Catalog.DeleteDataFileByID(ctx, candidate.FileID); err != nil {
				summary.Failures++
				failures = append(failures, fmt.Sprintf("tenant %s delete data_file %d: %v", tenant.TenantID, candidate.FileID, err))
				tenantErr = err
				continue
			}
			deleted++
			summary.FilesDeleted++
		}

		status := "completed"
		if tenantErr != nil {
			status = "failed"
		}
		if err := s.recordGCRun(ctx, tenant.TenantID, status, len(candidates), deleted, tenantErr); err != nil {
			summary.Failures++
			failures = append(failures, fmt.Sprintf("tenant %s record gc_run: %v", tenant.TenantID, err))
		}
	}

	if len(failures) > 0 {
		if summary.FilesDeleted > 0 {
			gcFilesDeletedTotal.Add(float64(summary.FilesDeleted))
		}
		return summary, fmt.Errorf("retention encountered %d failure(s): %s", len(failures), strings.Join(failures, "; "))
	}
	if summary.FilesDeleted > 0 {
		gcFilesDeletedTotal.Add(float64(summary.FilesDeleted))
	}
	return summary, nil
}

func (s *Service) RunIntegrityCheckOnce(ctx context.Context, tenantID string) (IntegritySummary, error) {
	s.ensureDefaults()
	if s.Catalog == nil {
		return IntegritySummary{}, fmt.Errorf("catalog is required")
	}
	if s.ObjectStore == nil {
		return IntegritySummary{}, fmt.Errorf("object store is required")
	}

	tenants, err := s.listTargetTenants(ctx, tenantID)
	if err != nil {
		return IntegritySummary{}, err
	}
	summary := IntegritySummary{TenantsScanned: len(tenants)}
	const maxIssueSamples = 20
	issueSamples := make([]string, 0, maxIssueSamples)
	issueCount := 0
	addIssue := func(message string) {
		issueCount++
		if len(issueSamples) < maxIssueSamples {
			issueSamples = append(issueSamples, message)
		}
	}

	for _, tenant := range tenants {
		snapshots, err := s.Catalog.ListSnapshots(ctx, tenant.TenantID, s.Config.IntegritySnapshotLimit)
		if err != nil {
			summary.OperationalFailures++
			addIssue(fmt.Sprintf("tenant %s list snapshots: %v", tenant.TenantID, err))
			continue
		}
		summary.SnapshotsScanned += len(snapshots)

		checkedPaths := make(map[string]struct{})
		for _, snapshot := range snapshots {
			files, err := s.Catalog.ListSnapshotFiles(ctx, tenant.TenantID, snapshot.SnapshotID)
			if err != nil {
				summary.OperationalFailures++
				addIssue(fmt.Sprintf("tenant %s snapshot %d files: %v", tenant.TenantID, snapshot.SnapshotID, err))
				continue
			}
			summary.ReferencedFiles += len(files)

			for _, file := range files {
				if _, seen := checkedPaths[file.Path]; seen {
					continue
				}
				checkedPaths[file.Path] = struct{}{}
				summary.UniqueFilesChecked++

				info, err := s.ObjectStore.Stat(ctx, file.Path)
				if err != nil {
					if errors.Is(err, storage.ErrObjectNotFound) {
						summary.MissingFiles++
						addIssue(fmt.Sprintf("tenant %s missing file %s (snapshot=%d file_id=%d)", tenant.TenantID, file.Path, snapshot.SnapshotID, file.FileID))
						continue
					}
					summary.OperationalFailures++
					addIssue(fmt.Sprintf("tenant %s stat file %s: %v", tenant.TenantID, file.Path, err))
					continue
				}
				if info.Size != file.FileSizeBytes {
					summary.SizeMismatchFiles++
					addIssue(fmt.Sprintf("tenant %s size mismatch for %s (expected=%d actual=%d)", tenant.TenantID, file.Path, file.FileSizeBytes, info.Size))
				}
			}
		}
	}

	if summary.UniqueFilesChecked > 0 {
		integrityFilesCheckedTotal.Add(float64(summary.UniqueFilesChecked))
	}
	if summary.MissingFiles > 0 {
		integrityMissingFilesTotal.Add(float64(summary.MissingFiles))
	}
	if summary.SizeMismatchFiles > 0 {
		integritySizeMismatchFilesTotal.Add(float64(summary.SizeMismatchFiles))
	}
	if summary.MissingFiles > 0 || summary.SizeMismatchFiles > 0 || summary.OperationalFailures > 0 {
		integrityRunsTotal.WithLabelValues("failed").Inc()
		extra := issueCount - len(issueSamples)
		if extra > 0 {
			return summary, fmt.Errorf("integrity check found %d issue(s): %s; ... plus %d more", issueCount, strings.Join(issueSamples, "; "), extra)
		}
		return summary, fmt.Errorf("integrity check found %d issue(s): %s", issueCount, strings.Join(issueSamples, "; "))
	}
	integrityRunsTotal.WithLabelValues("completed").Inc()
	return summary, nil
}

type compactedTableSummary struct {
	inputFiles     int
	bytesRewritten int64
}

func (s *Service) compactTable(ctx context.Context, tenantID string, tableID int64, tableName string, maxVisibilityToken int64, files []catalog.SnapshotFileEntry) (compactedTableSummary, error) {
	workDir, err := os.MkdirTemp("", "duckmesh-compact-")
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("create compaction temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	inputPaths := make([]string, 0, len(files))
	removedFileIDs := make([]int64, 0, len(files))
	var sourceBytes int64
	var sourceRecords int64

	for i, file := range files {
		reader, err := s.ObjectStore.Get(ctx, file.Path)
		if err != nil {
			return compactedTableSummary{}, fmt.Errorf("download source object %s: %w", file.Path, err)
		}

		localPath := filepath.Join(workDir, fmt.Sprintf("input_%03d.parquet", i))
		if err := writeLocalFile(localPath, reader); err != nil {
			_ = reader.Close()
			return compactedTableSummary{}, fmt.Errorf("write local source file %s: %w", localPath, err)
		}
		if err := reader.Close(); err != nil {
			return compactedTableSummary{}, fmt.Errorf("close source object %s: %w", file.Path, err)
		}
		inputPaths = append(inputPaths, localPath)
		removedFileIDs = append(removedFileIDs, file.FileID)
		sourceBytes += file.FileSizeBytes
		sourceRecords += file.RecordCount
	}

	outputLocalPath := filepath.Join(workDir, "compacted.parquet")
	mergedRecords, err := mergeParquetFiles(ctx, inputPaths, outputLocalPath)
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("merge parquet files: %w", err)
	}
	if mergedRecords != sourceRecords {
		return compactedTableSummary{}, fmt.Errorf("compaction row count mismatch: source=%d merged=%d", sourceRecords, mergedRecords)
	}

	outputStat, err := os.Stat(outputLocalPath)
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("stat compacted file: %w", err)
	}
	outputFile, err := os.Open(outputLocalPath)
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("open compacted file: %w", err)
	}
	defer func() { _ = outputFile.Close() }()

	snapshotID, err := s.Catalog.AllocateSnapshotID(ctx)
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("allocate snapshot id: %w", err)
	}
	objectPath, err := storage.BuildDataFilePath(tenantID, tableName, s.Clock(), snapshotID, 0)
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("build compacted object path: %w", err)
	}

	objectInfo, err := s.ObjectStore.Put(ctx, objectPath, outputFile, outputStat.Size(), storage.PutOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("upload compacted parquet: %w", err)
	}

	statsJSON, err := json.Marshal(map[string]any{
		"source_file_count": len(files),
		"source_records":    sourceRecords,
		"merged_records":    mergedRecords,
		"source_bytes":      sourceBytes,
		"merged_bytes":      objectInfo.Size,
	})
	if err != nil {
		return compactedTableSummary{}, fmt.Errorf("marshal compaction stats: %w", err)
	}

	if _, err := s.Catalog.PublishCompaction(ctx, catalogpostgres.PublishCompactionInput{
		SnapshotID:         snapshotID,
		TenantID:           tenantID,
		TableID:            tableID,
		CreatedBy:          s.Config.CreatedBy,
		MaxVisibilityToken: maxVisibilityToken,
		DataFilePath:       objectPath,
		RecordCount:        mergedRecords,
		FileSizeBytes:      objectInfo.Size,
		StatsJSON:          statsJSON,
		RemovedFileIDs:     removedFileIDs,
	}); err != nil {
		_ = s.ObjectStore.Delete(ctx, objectPath)
		return compactedTableSummary{}, fmt.Errorf("publish compaction snapshot: %w", err)
	}

	return compactedTableSummary{
		inputFiles:     len(files),
		bytesRewritten: sourceBytes,
	}, nil
}

func (s *Service) recordGCRun(ctx context.Context, tenantID, status string, candidateCount, deletedCount int, runErr error) error {
	details := map[string]any{
		"candidate_files": candidateCount,
		"files_deleted":   deletedCount,
	}
	if runErr != nil {
		details["error"] = runErr.Error()
	}
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("marshal gc_run details: %w", err)
	}
	if err := s.Catalog.RecordGCRun(ctx, catalogpostgres.RecordGCRunInput{
		TenantID:    tenantID,
		Status:      status,
		DetailsJSON: detailsJSON,
	}); err != nil {
		return err
	}
	return nil
}

func (s *Service) listTargetTenants(ctx context.Context, tenantID string) ([]catalog.Tenant, error) {
	if strings.TrimSpace(tenantID) != "" {
		return []catalog.Tenant{{TenantID: strings.TrimSpace(tenantID), Status: "active"}}, nil
	}
	tenants, err := s.Catalog.ListTenants(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	return tenants, nil
}

func (s *Service) ensureDefaults() {
	if s.Clock == nil {
		s.Clock = time.Now
	}
	if s.Config.CompactionMinInputFiles <= 1 {
		s.Config.CompactionMinInputFiles = 4
	}
	if s.Config.CompactionInterval <= 0 {
		s.Config.CompactionInterval = 2 * time.Minute
	}
	if s.Config.RetentionInterval <= 0 {
		s.Config.RetentionInterval = 10 * time.Minute
	}
	if s.Config.IntegritySnapshotLimit <= 0 {
		s.Config.IntegritySnapshotLimit = 20
	}
	if s.Config.KeepSnapshots < 1 {
		s.Config.KeepSnapshots = 3
	}
	if s.Config.GCSafetyAge <= 0 {
		s.Config.GCSafetyAge = 30 * time.Minute
	}
	if s.Config.CreatedBy == "" {
		s.Config.CreatedBy = "duckmesh-compactor"
	}
}

func writeLocalFile(path string, reader io.Reader) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	if _, err := io.Copy(file, reader); err != nil {
		return err
	}
	return nil
}
