package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/duckmesh/duckmesh/internal/query"
	"github.com/duckmesh/duckmesh/internal/storage"
)

type Engine struct {
	Store storage.ObjectStore
}

func NewEngine(store storage.ObjectStore) *Engine {
	return &Engine{Store: store}
}

func (e *Engine) Execute(ctx context.Context, request query.Request) (query.Result, error) {
	if strings.TrimSpace(request.SQL) == "" {
		return query.Result{}, fmt.Errorf("sql is required")
	}
	if len(request.Files) == 0 {
		return query.Result{}, fmt.Errorf("no files available for snapshot")
	}
	if e.Store == nil {
		return query.Result{}, fmt.Errorf("object store is required")
	}

	start := time.Now()
	workDir, err := os.MkdirTemp("", "duckmesh-query-")
	if err != nil {
		return query.Result{}, fmt.Errorf("create query temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	groupedPaths := map[string][]string{}
	var scannedBytes int64

	for index, file := range request.Files {
		reader, err := e.Store.Get(ctx, file.ObjectPath)
		if err != nil {
			return query.Result{}, fmt.Errorf("get object %q: %w", file.ObjectPath, err)
		}

		localPath := filepath.Join(workDir, fmt.Sprintf("%s_%d.parquet", sanitizeFileComponent(file.TableName), index))
		if err := writeFile(localPath, reader); err != nil {
			_ = reader.Close()
			return query.Result{}, fmt.Errorf("write local parquet file %q: %w", localPath, err)
		}
		if err := reader.Close(); err != nil {
			return query.Result{}, fmt.Errorf("close object %q: %w", file.ObjectPath, err)
		}

		groupedPaths[file.TableName] = append(groupedPaths[file.TableName], localPath)
		scannedBytes += file.FileSizeBytes
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return query.Result{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	for tableName, localPaths := range groupedPaths {
		viewSQL := fmt.Sprintf(`CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet(%s)`, quoteIdent(tableName), quoteStringArray(localPaths))
		if _, err := db.ExecContext(ctx, viewSQL); err != nil {
			return query.Result{}, fmt.Errorf("create view for table %q: %w", tableName, err)
		}
	}

	sqlText := stripTrailingSemicolons(request.SQL)
	if sqlText == "" {
		return query.Result{}, fmt.Errorf("sql is required")
	}
	if request.RowLimit > 0 {
		sqlText = fmt.Sprintf("SELECT * FROM (%s) AS q LIMIT %d", sqlText, request.RowLimit)
	}

	rows, err := db.QueryContext(ctx, sqlText)
	if err != nil {
		return query.Result{}, fmt.Errorf("execute query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return query.Result{}, fmt.Errorf("query columns: %w", err)
	}

	resultRows := make([][]any, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		scanTargets := make([]any, len(columns))
		for i := range values {
			scanTargets[i] = &values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return query.Result{}, fmt.Errorf("scan row: %w", err)
		}
		resultRows = append(resultRows, normalizeValues(values))
	}
	if err := rows.Err(); err != nil {
		return query.Result{}, fmt.Errorf("iterate rows: %w", err)
	}

	return query.Result{
		Columns:      columns,
		Rows:         resultRows,
		ScannedFiles: len(request.Files),
		ScannedBytes: scannedBytes,
		Duration:     time.Since(start),
	}, nil
}

func normalizeValues(values []any) []any {
	normalized := make([]any, len(values))
	for i, value := range values {
		switch typed := value.(type) {
		case []byte:
			normalized[i] = string(typed)
		default:
			normalized[i] = typed
		}
	}
	return normalized
}

func quoteIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteStringArray(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, `'`+strings.ReplaceAll(value, `'`, `''`)+`'`)
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

func sanitizeFileComponent(value string) string {
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "..", "_")
	if value == "" {
		return "table"
	}
	return value
}

func stripTrailingSemicolons(sqlText string) string {
	trimmed := strings.TrimSpace(sqlText)
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}
