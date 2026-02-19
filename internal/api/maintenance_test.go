package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/maintenance"
)

func TestCompactionRunEndpointRequiresOpsRole(t *testing.T) {
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
		Maintenance:    &fakeMaintenanceRunner{},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/compaction/run", nil)
	req.Header.Set("X-API-Key", "reader")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestCompactionRunEndpointReturnsSummary(t *testing.T) {
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
	runner := &fakeMaintenanceRunner{
		compactionSummary: maintenance.CompactionSummary{
			TenantsScanned:      1,
			TablesScanned:       2,
			TablesCompacted:     1,
			InputFilesCompacted: 4,
			BytesRewritten:      1234,
			SnapshotsPublished:  1,
		},
	}

	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		Maintenance:    runner,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/compaction/run", nil)
	req.Header.Set("X-API-Key", "ops")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if len(runner.compactionTenants) != 1 || runner.compactionTenants[0] != "t1" {
		t.Fatalf("compaction tenant filters = %#v", runner.compactionTenants)
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if body["status"] != "completed" {
		t.Fatalf("status field = %v", body["status"])
	}
}

func TestRetentionRunEndpointNotConfigured(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{})
	req := httptest.NewRequest(http.MethodPost, "/v1/retention/run", strings.NewReader(`{}`))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestIntegrityRunEndpointReturnsSummary(t *testing.T) {
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
	runner := &fakeMaintenanceRunner{
		integritySummary: maintenance.IntegritySummary{
			TenantsScanned:      1,
			SnapshotsScanned:    3,
			ReferencedFiles:     12,
			UniqueFilesChecked:  5,
			MissingFiles:        0,
			SizeMismatchFiles:   0,
			OperationalFailures: 0,
		},
	}

	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		Maintenance:    runner,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/integrity/run", nil)
	req.Header.Set("X-API-Key", "ops")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if len(runner.integrityTenants) != 1 || runner.integrityTenants[0] != "t1" {
		t.Fatalf("integrity tenant filters = %#v", runner.integrityTenants)
	}
}

type fakeMaintenanceRunner struct {
	compactionSummary maintenance.CompactionSummary
	compactionErr     error
	retentionSummary  maintenance.RetentionSummary
	retentionErr      error
	integritySummary  maintenance.IntegritySummary
	integrityErr      error
	compactionTenants []string
	retentionTenants  []string
	integrityTenants  []string
}

func (f *fakeMaintenanceRunner) RunCompactionOnce(_ context.Context, tenantID string) (maintenance.CompactionSummary, error) {
	f.compactionTenants = append(f.compactionTenants, tenantID)
	if f.compactionErr != nil {
		return maintenance.CompactionSummary{}, f.compactionErr
	}
	return f.compactionSummary, nil
}

func (f *fakeMaintenanceRunner) RunRetentionOnce(_ context.Context, tenantID string) (maintenance.RetentionSummary, error) {
	f.retentionTenants = append(f.retentionTenants, tenantID)
	if f.retentionErr != nil {
		return maintenance.RetentionSummary{}, f.retentionErr
	}
	return f.retentionSummary, nil
}

func (f *fakeMaintenanceRunner) RunIntegrityCheckOnce(_ context.Context, tenantID string) (maintenance.IntegritySummary, error) {
	f.integrityTenants = append(f.integrityTenants, tenantID)
	if f.integrityErr != nil {
		return maintenance.IntegritySummary{}, f.integrityErr
	}
	return f.integritySummary, nil
}
