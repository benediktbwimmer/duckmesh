package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func mergeParquetFiles(ctx context.Context, inputPaths []string, outputPath string) (int64, error) {
	if len(inputPaths) == 0 {
		return 0, fmt.Errorf("at least one input path is required")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return 0, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	var recordCount int64
	countSQL := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet(%s)`, quoteStringArray(inputPaths))
	if err := db.QueryRowContext(ctx, countSQL).Scan(&recordCount); err != nil {
		return 0, fmt.Errorf("count merged rows: %w", err)
	}

	copySQL := fmt.Sprintf(
		`COPY (SELECT * FROM read_parquet(%s)) TO %s (FORMAT PARQUET, COMPRESSION ZSTD)`,
		quoteStringArray(inputPaths),
		quoteString(outputPath),
	)
	if _, err := db.ExecContext(ctx, copySQL); err != nil {
		return 0, fmt.Errorf("write compacted parquet: %w", err)
	}
	return recordCount, nil
}

func quoteStringArray(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteString(value))
	}
	return "[" + strings.Join(quoted, ",") + "]"
}

func quoteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
