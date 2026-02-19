package nl2sql

import "context"

type TableContext struct {
	TableName  string   `json:"table_name"`
	Columns    []string `json:"columns"`
	SampleRows [][]any  `json:"sample_rows"`
}

type Request struct {
	TenantID        string         `json:"tenant_id"`
	NaturalLanguage string         `json:"natural_language"`
	Tables          []TableContext `json:"tables"`
}

type Result struct {
	SQL      string `json:"sql"`
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

type Translator interface {
	Translate(ctx context.Context, req Request) (Result, error)
}
