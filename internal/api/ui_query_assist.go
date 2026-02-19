package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/nl2sql"
	"github.com/duckmesh/duckmesh/internal/query"
)

type translateRequest struct {
	Prompt string `json:"prompt"`
}

func handleUISchema(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "SCHEMA_NOT_CONFIGURED", "catalog dependency is not configured", false, nil)
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

	tableContexts, snapshot, err := buildTableContexts(r.Context(), deps, tenantID, schemaSampleRows(deps))
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "SCHEMA_FETCH_FAILED", "failed to load schema context", true, map[string]any{"details": err.Error()})
		return
	}

	response := map[string]any{
		"tenant_id": tenantID,
		"tables":    tableContexts,
	}
	if snapshot != nil {
		response["snapshot_id"] = snapshot.SnapshotID
		response["snapshot_time"] = snapshot.CreatedAt
		response["max_visibility_token"] = snapshot.MaxVisibilityToken
	}
	writeJSON(w, http.StatusOK, response)
}

func handleTranslateQuery(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.QueryTranslator == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TRANSLATE_NOT_CONFIGURED", "query translation is not configured", false, nil)
		return
	}
	if deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "SCHEMA_NOT_CONFIGURED", "catalog dependency is not configured", false, nil)
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

	var req translateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "INVALID_JSON", "invalid translation request body", false, map[string]any{"details": err.Error()})
		return
	}
	if strings.TrimSpace(req.Prompt) == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "PROMPT_REQUIRED", "prompt is required", false, nil)
		return
	}

	tableContexts, _, err := buildTableContexts(r.Context(), deps, tenantID, schemaSampleRows(deps))
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "SCHEMA_FETCH_FAILED", "failed to load schema context", true, map[string]any{"details": err.Error()})
		return
	}

	result, err := deps.QueryTranslator.Translate(r.Context(), nl2sql.Request{
		TenantID:        tenantID,
		NaturalLanguage: req.Prompt,
		Tables:          tableContexts,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusBadGateway, "TRANSLATE_FAILED", "failed to translate query", true, map[string]any{"details": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"sql":      result.SQL,
		"provider": result.Provider,
		"model":    result.Model,
	})
}

func buildTableContexts(ctx context.Context, deps Dependencies, tenantID string, sampleRows int) ([]nl2sql.TableContext, *catalog.Snapshot, error) {
	tables, err := deps.CatalogRepo.ListTables(ctx, tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("list tables: %w", err)
	}

	contexts := make([]nl2sql.TableContext, 0, len(tables))
	for _, table := range tables {
		contexts = append(contexts, nl2sql.TableContext{TableName: table.TableName})
	}
	if deps.QueryEngine == nil {
		return contexts, nil, nil
	}

	snapshot, err := deps.CatalogRepo.GetLatestSnapshot(ctx, tenantID)
	if err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			return contexts, nil, nil
		}
		return nil, nil, fmt.Errorf("get latest snapshot: %w", err)
	}

	files, err := deps.CatalogRepo.ListSnapshotFiles(ctx, tenantID, snapshot.SnapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("list snapshot files: %w", err)
	}
	byTable := map[string][]query.TableFile{}
	for _, file := range files {
		byTable[file.TableName] = append(byTable[file.TableName], query.TableFile{
			TableName:     file.TableName,
			ObjectPath:    file.Path,
			FileSizeBytes: file.FileSizeBytes,
		})
	}

	for i := range contexts {
		filesForTable := byTable[contexts[i].TableName]
		if len(filesForTable) == 0 {
			continue
		}
		result, err := deps.QueryEngine.Execute(ctx, query.Request{
			SQL:      "SELECT * FROM " + quoteIdent(contexts[i].TableName) + " LIMIT " + strconv.Itoa(sampleRows),
			Files:    filesForTable,
			RowLimit: sampleRows,
		})
		if err != nil {
			continue
		}
		contexts[i].Columns = append(contexts[i].Columns, result.Columns...)
		contexts[i].SampleRows = append(contexts[i].SampleRows, result.Rows...)
	}

	return contexts, &snapshot, nil
}

func schemaSampleRows(deps Dependencies) int {
	if deps.UISchemaSamples > 0 {
		return deps.UISchemaSamples
	}
	return 5
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
