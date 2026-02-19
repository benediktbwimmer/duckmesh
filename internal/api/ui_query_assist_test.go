package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/nl2sql"
)

func TestUISchemaEndpointReturnsTables(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		CatalogRepo: &fakeQueryCatalogRepo{
			table: catalog.TableDef{TableName: "events"},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/ui/schema", nil)
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	tables, ok := body["tables"].([]any)
	if !ok || len(tables) != 1 {
		t.Fatalf("tables = %#v", body["tables"])
	}
}

func TestTranslateEndpointReturnsSQL(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{
		"DUCKMESH_AUTH_REQUIRED": "true",
	}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	validator, err := auth.NewStaticAPIKeyValidator("k1:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup failed: %v", err)
	}

	translator := &fakeTranslator{
		result: nl2sql.Result{SQL: "SELECT 1", Provider: "fake", Model: "fake-model"},
	}
	h := NewHandler(cfg, Dependencies{
		AuthMiddleware:  auth.Middleware(nil, validator),
		CatalogRepo:     &fakeQueryCatalogRepo{table: catalog.TableDef{TableName: "events"}},
		QueryTranslator: translator,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/query/translate", strings.NewReader(`{"prompt":"show one row"}`))
	req.Header.Set("X-API-Key", "k1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if len(translator.requests) != 1 {
		t.Fatalf("translator requests = %d", len(translator.requests))
	}
}

type fakeTranslator struct {
	requests []nl2sql.Request
	result   nl2sql.Result
	err      error
}

func (f *fakeTranslator) Translate(_ context.Context, req nl2sql.Request) (nl2sql.Result, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return nl2sql.Result{}, f.err
	}
	return f.result, nil
}
