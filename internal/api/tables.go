package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/catalog"
)

type tableCreateRequest struct {
	TableName         string         `json:"table_name"`
	PrimaryKeyCols    []string       `json:"primary_key_cols"`
	PartitionSpec     map[string]any `json:"partition_spec"`
	SchemaJSON        map[string]any `json:"schema_json"`
	CompatibilityMode string         `json:"compatibility_mode"`
}

type tablePatchRequest struct {
	SchemaJSON        map[string]any `json:"schema_json"`
	CompatibilityMode string         `json:"compatibility_mode"`
}

type tableAdminCatalog interface {
	CreateTable(ctx context.Context, in catalog.CreateTableInput) (catalog.TableDef, error)
	UpsertTableSchemaVersion(ctx context.Context, in catalog.UpsertTableSchemaVersionInput) (catalog.TableSchemaVersion, error)
	SetTableSchemaVersion(ctx context.Context, tableID int64, schemaVersion int) error
	DeleteTableByName(ctx context.Context, tenantID, tableName string) (bool, error)
}

func handleListTables(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TABLES_NOT_CONFIGURED", "catalog dependency is not configured", false, nil)
		return
	}
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireAnyRole(r, "query_reader", "table_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}
	tables, err := deps.CatalogRepo.ListTables(r.Context(), tenantID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to list tables", true, map[string]any{"details": err.Error()})
		return
	}
	items := make([]map[string]any, 0, len(tables))
	for _, table := range tables {
		items = append(items, map[string]any{
			"table_id":         table.TableID,
			"table_name":       table.TableName,
			"schema_version":   table.SchemaVersion,
			"primary_key_cols": json.RawMessage(table.PrimaryKeyCols),
			"partition_spec":   json.RawMessage(table.PartitionSpec),
			"created_at":       table.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"tables":    items,
	})
}

func handleCreateTable(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	adminRepo, ok := deps.CatalogRepo.(tableAdminCatalog)
	if !ok || deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TABLES_NOT_CONFIGURED", "table admin operations are not configured", false, nil)
		return
	}
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireAnyRole(r, "table_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	var req tableCreateRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "INVALID_JSON", "invalid create table request body", false, map[string]any{"details": err.Error()})
		return
	}
	if strings.TrimSpace(req.TableName) == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "TABLE_NAME_REQUIRED", "table_name is required", false, nil)
		return
	}

	pkJSON, _ := json.Marshal(req.PrimaryKeyCols)
	partitionJSON, _ := json.Marshal(req.PartitionSpec)
	table, err := adminRepo.CreateTable(r.Context(), catalog.CreateTableInput{
		TenantID:       tenantID,
		TableName:      strings.TrimSpace(req.TableName),
		PrimaryKeyCols: pkJSON,
		PartitionSpec:  partitionJSON,
		SchemaVersion:  1,
	})
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to create table", true, map[string]any{"details": err.Error()})
		return
	}

	if len(req.SchemaJSON) > 0 {
		schemaJSON, _ := json.Marshal(req.SchemaJSON)
		compatibility := strings.TrimSpace(req.CompatibilityMode)
		if compatibility == "" {
			compatibility = "backward"
		}
		if _, err := adminRepo.UpsertTableSchemaVersion(r.Context(), catalog.UpsertTableSchemaVersionInput{
			TableID:           table.TableID,
			SchemaVersion:     1,
			SchemaJSON:        schemaJSON,
			CompatibilityMode: compatibility,
		}); err != nil {
			writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to create table schema version", true, map[string]any{"details": err.Error()})
			return
		}
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"table_id":         table.TableID,
		"tenant_id":        tenantID,
		"table_name":       table.TableName,
		"schema_version":   table.SchemaVersion,
		"primary_key_cols": json.RawMessage(table.PrimaryKeyCols),
		"partition_spec":   json.RawMessage(table.PartitionSpec),
		"created_at":       table.CreatedAt,
	})
}

func handleGetTable(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TABLES_NOT_CONFIGURED", "catalog dependency is not configured", false, nil)
		return
	}
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireAnyRole(r, "query_reader", "table_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}
	tableName := strings.TrimSpace(r.PathValue("table"))
	if tableName == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "TABLE_REQUIRED", "table path parameter is required", false, nil)
		return
	}
	table, err := deps.CatalogRepo.GetTableByName(r.Context(), tenantID, tableName)
	if err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "TABLE_NOT_FOUND", "table was not found", false, nil)
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to get table", true, map[string]any{"details": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table_id":         table.TableID,
		"tenant_id":        tenantID,
		"table_name":       table.TableName,
		"schema_version":   table.SchemaVersion,
		"primary_key_cols": json.RawMessage(table.PrimaryKeyCols),
		"partition_spec":   json.RawMessage(table.PartitionSpec),
		"created_at":       table.CreatedAt,
	})
}

func handlePatchTable(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	adminRepo, ok := deps.CatalogRepo.(tableAdminCatalog)
	if !ok || deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TABLES_NOT_CONFIGURED", "table admin operations are not configured", false, nil)
		return
	}
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireAnyRole(r, "table_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}
	tableName := strings.TrimSpace(r.PathValue("table"))
	if tableName == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "TABLE_REQUIRED", "table path parameter is required", false, nil)
		return
	}

	var req tablePatchRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "INVALID_JSON", "invalid patch table request body", false, map[string]any{"details": err.Error()})
		return
	}
	if len(req.SchemaJSON) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "SCHEMA_REQUIRED", "schema_json is required", false, nil)
		return
	}

	table, err := deps.CatalogRepo.GetTableByName(r.Context(), tenantID, tableName)
	if err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "TABLE_NOT_FOUND", "table was not found", false, nil)
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to get table", true, map[string]any{"details": err.Error()})
		return
	}
	newVersion := table.SchemaVersion + 1
	schemaJSON, _ := json.Marshal(req.SchemaJSON)
	compatibility := strings.TrimSpace(req.CompatibilityMode)
	if compatibility == "" {
		compatibility = "backward"
	}

	if _, err := adminRepo.UpsertTableSchemaVersion(r.Context(), catalog.UpsertTableSchemaVersionInput{
		TableID:           table.TableID,
		SchemaVersion:     newVersion,
		SchemaJSON:        schemaJSON,
		CompatibilityMode: compatibility,
	}); err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to upsert schema version", true, map[string]any{"details": err.Error()})
		return
	}
	if err := adminRepo.SetTableSchemaVersion(r.Context(), table.TableID, newVersion); err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to set table schema version", true, map[string]any{"details": err.Error()})
		return
	}

	updated, err := deps.CatalogRepo.GetTableByName(r.Context(), tenantID, tableName)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to fetch updated table", true, map[string]any{"details": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table_id":         updated.TableID,
		"tenant_id":        tenantID,
		"table_name":       updated.TableName,
		"schema_version":   updated.SchemaVersion,
		"primary_key_cols": json.RawMessage(updated.PrimaryKeyCols),
		"partition_spec":   json.RawMessage(updated.PartitionSpec),
		"created_at":       updated.CreatedAt,
	})
}

func handleDeleteTable(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	adminRepo, ok := deps.CatalogRepo.(tableAdminCatalog)
	if !ok || deps.CatalogRepo == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "TABLES_NOT_CONFIGURED", "table admin operations are not configured", false, nil)
		return
	}
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireAnyRole(r, "table_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}
	tableName := strings.TrimSpace(r.PathValue("table"))
	if tableName == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "TABLE_REQUIRED", "table path parameter is required", false, nil)
		return
	}

	deleted, err := adminRepo.DeleteTableByName(r.Context(), tenantID, tableName)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to delete table", true, map[string]any{"details": err.Error()})
		return
	}
	if !deleted {
		writeError(r.Context(), w, http.StatusNotFound, "TABLE_NOT_FOUND", "table was not found", false, nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "deleted", "table_name": tableName})
}

func requireAnyRole(r *http.Request, roles ...string) error {
	identity, ok := auth.IdentityFromContext(r.Context())
	if !ok {
		return nil
	}
	for _, role := range roles {
		if identity.HasRole(role) {
			return nil
		}
	}
	return fmt.Errorf("missing required role, expected one of %q", strings.Join(roles, ","))
}
