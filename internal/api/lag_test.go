package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
)

func TestLagEndpointReturnsStats(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{
		"DUCKMESH_AUTH_REQUIRED": "true",
	}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	validator, err := auth.NewStaticAPIKeyValidator("ops:t1:ops_admin")
	if err != nil {
		t.Fatalf("validator setup failed: %v", err)
	}
	now := time.Now().UTC().Add(-10 * time.Second)
	snapshotID := int64(9)
	repo := &fakeLagCatalogRepo{
		fakeQueryCatalogRepo: fakeQueryCatalogRepo{},
		stats: catalog.IngestLagStats{
			AcceptedEvents:        3,
			ClaimedEvents:         2,
			OldestPendingIngestAt: &now,
			MaxPendingToken:       120,
			LatestSnapshotID:      &snapshotID,
			LatestVisibilityToken: 114,
		},
	}

	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		CatalogRepo:    repo,
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/lag", nil)
	req.Header.Set("X-API-Key", "ops")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if body["pending_events"] != float64(5) {
		t.Fatalf("pending_events = %v", body["pending_events"])
	}
	if body["token_lag"] != float64(6) {
		t.Fatalf("token_lag = %v", body["token_lag"])
	}
}

func TestLagEndpointRequiresOpsRole(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{
		"DUCKMESH_AUTH_REQUIRED": "true",
	}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	validator, err := auth.NewStaticAPIKeyValidator("reader:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup failed: %v", err)
	}
	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		CatalogRepo:    &fakeLagCatalogRepo{},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/lag", strings.NewReader(``))
	req.Header.Set("X-API-Key", "reader")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

type fakeLagCatalogRepo struct {
	fakeQueryCatalogRepo
	stats catalog.IngestLagStats
	err   error
}

func (f *fakeLagCatalogRepo) GetIngestLagStats(context.Context, string) (catalog.IngestLagStats, error) {
	if f.err != nil {
		return catalog.IngestLagStats{}, f.err
	}
	return f.stats, nil
}
