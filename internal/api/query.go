package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/observability"
	"github.com/duckmesh/duckmesh/internal/query"
)

type queryRequest struct {
	SQL                  string         `json:"sql"`
	Params               map[string]any `json:"params"`
	SnapshotID           *int64         `json:"snapshot_id"`
	SnapshotTime         *time.Time     `json:"snapshot_time"`
	MinVisibilityToken   *int64         `json:"min_visibility_token"`
	ConsistencyTimeoutMs int            `json:"consistency_timeout_ms"`
	RowLimit             int            `json:"row_limit"`
}

type queryResponse struct {
	Columns            []string       `json:"columns"`
	Rows               [][]any        `json:"rows"`
	SnapshotID         int64          `json:"snapshot_id"`
	SnapshotTime       time.Time      `json:"snapshot_time"`
	MaxVisibilityToken int64          `json:"max_visibility_token"`
	Stats              map[string]any `json:"stats"`
}

func handleQuery(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.CatalogRepo == nil || deps.QueryEngine == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "QUERY_NOT_CONFIGURED", "query dependencies are not configured", false, nil)
		return
	}

	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireRole(r, "query_reader"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	var request queryRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "INVALID_JSON", "invalid query request body", false, map[string]any{"details": err.Error()})
		return
	}

	if strings.TrimSpace(request.SQL) == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "SQL_REQUIRED", "sql is required", false, nil)
		return
	}
	if !isAllowedSQL(request.SQL) {
		writeError(r.Context(), w, http.StatusBadRequest, "SQL_NOT_ALLOWED", "only read-only SELECT/WITH queries are allowed", false, nil)
		return
	}
	if len(request.Params) > 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "PARAMS_UNSUPPORTED", "query params are not supported yet", false, nil)
		return
	}
	if request.SnapshotID != nil && request.SnapshotTime != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "SNAPSHOT_SELECTOR_CONFLICT", "specify only one of snapshot_id or snapshot_time", false, nil)
		return
	}

	var snapshot catalog.Snapshot
	switch {
	case request.SnapshotID != nil:
		snapshot, err = deps.CatalogRepo.GetSnapshotByID(r.Context(), tenantID, *request.SnapshotID)
	case request.SnapshotTime != nil:
		snapshot, err = deps.CatalogRepo.GetSnapshotByTime(r.Context(), tenantID, request.SnapshotTime.UTC())
	default:
		snapshot, err = resolveSnapshotWithBarrier(r, deps, tenantID, request.MinVisibilityToken, request.ConsistencyTimeoutMs)
	}
	if err != nil {
		handleSnapshotResolutionError(r, w, err)
		return
	}

	files, err := deps.CatalogRepo.ListSnapshotFiles(r.Context(), tenantID, snapshot.SnapshotID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to load snapshot files", true, map[string]any{"details": err.Error()})
		return
	}
	if len(files) == 0 {
		writeError(r.Context(), w, http.StatusNotFound, "SNAPSHOT_NOT_FOUND", "snapshot has no queryable files", false, map[string]any{"snapshot_id": snapshot.SnapshotID})
		return
	}

	queryFiles := make([]query.TableFile, 0, len(files))
	for _, file := range files {
		queryFiles = append(queryFiles, query.TableFile{
			TableName:     file.TableName,
			ObjectPath:    file.Path,
			FileSizeBytes: file.FileSizeBytes,
		})
	}

	result, err := deps.QueryEngine.Execute(r.Context(), query.Request{
		SQL:      request.SQL,
		RowLimit: request.RowLimit,
		Files:    queryFiles,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "QUERY_EXECUTION_FAILED", "query execution failed", false, map[string]any{"details": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, queryResponse{
		Columns:            result.Columns,
		Rows:               result.Rows,
		SnapshotID:         snapshot.SnapshotID,
		SnapshotTime:       snapshot.CreatedAt,
		MaxVisibilityToken: snapshot.MaxVisibilityToken,
		Stats: map[string]any{
			"duration_ms":   result.Duration.Milliseconds(),
			"scanned_files": result.ScannedFiles,
			"scanned_bytes": result.ScannedBytes,
		},
	})
}

func resolveSnapshotWithBarrier(r *http.Request, deps Dependencies, tenantID string, minToken *int64, timeoutMs int) (catalog.Snapshot, error) {
	if minToken == nil || *minToken <= 0 {
		return deps.CatalogRepo.GetLatestSnapshot(r.Context(), tenantID)
	}

	waitStart := time.Now()
	timeout := 3 * time.Second
	if timeoutMs > 0 {
		timeout = time.Duration(timeoutMs) * time.Millisecond
	}
	deadline := time.Now().Add(timeout)
	pollEvery := 50 * time.Millisecond

	for {
		snapshot, err := deps.CatalogRepo.GetLatestSnapshot(r.Context(), tenantID)
		if err == nil && snapshot.MaxVisibilityToken >= *minToken {
			observability.ObserveWriteToVisibleLatency(time.Since(waitStart))
			return snapshot, nil
		}
		if err != nil && !errors.Is(err, catalog.ErrNotFound) {
			return catalog.Snapshot{}, fmt.Errorf("resolve latest snapshot: %w", err)
		}
		if time.Now().After(deadline) {
			latest := int64(0)
			if err == nil {
				latest = snapshot.MaxVisibilityToken
			}
			observability.IncrementConsistencyTimeout()
			return catalog.Snapshot{}, &consistencyTimeoutError{RequestedToken: *minToken, LatestToken: latest}
		}
		select {
		case <-r.Context().Done():
			return catalog.Snapshot{}, r.Context().Err()
		case <-time.After(pollEvery):
		}
	}
}

func handleSnapshotResolutionError(r *http.Request, w http.ResponseWriter, err error) {
	if errors.Is(err, catalog.ErrNotFound) {
		writeError(r.Context(), w, http.StatusNotFound, "SNAPSHOT_NOT_FOUND", "snapshot was not found", false, nil)
		return
	}
	var timeoutErr *consistencyTimeoutError
	if errors.As(err, &timeoutErr) {
		writeError(r.Context(), w, http.StatusGatewayTimeout, "CONSISTENCY_TIMEOUT", "visibility barrier timed out", true, map[string]any{
			"requested_token": timeoutErr.RequestedToken,
			"latest_token":    timeoutErr.LatestToken,
		})
		return
	}
	writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to resolve snapshot", true, map[string]any{"details": err.Error()})
}

func isAllowedSQL(sqlText string) bool {
	normalized := strings.ToLower(strings.TrimSpace(sqlText))
	if normalized == "" {
		return false
	}
	if strings.HasPrefix(normalized, "select") || strings.HasPrefix(normalized, "with") {
		return true
	}
	return false
}

type consistencyTimeoutError struct {
	RequestedToken int64
	LatestToken    int64
}

func (e *consistencyTimeoutError) Error() string {
	return fmt.Sprintf("consistency timeout waiting for token %d (latest=%d)", e.RequestedToken, e.LatestToken)
}
