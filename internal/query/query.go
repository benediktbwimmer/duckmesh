package query

import (
	"context"
	"time"
)

type TableFile struct {
	TableName     string
	ObjectPath    string
	FileSizeBytes int64
}

type Request struct {
	SQL      string
	RowLimit int
	Files    []TableFile
}

type Result struct {
	Columns      []string
	Rows         [][]any
	ScannedFiles int
	ScannedBytes int64
	Duration     time.Duration
}

type Engine interface {
	Execute(ctx context.Context, request Request) (Result, error)
}
