package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/config"
)

func TestHealthEndpoint(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/health", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestReadyEndpointReturns503WhenDependencyFails(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		Readiness: func(rctx context.Context) error {
			return errors.New("dependency down")
		},
	})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/ready", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rr.Code)
	}
}

func TestProtectedRouteRequiresAuth(t *testing.T) {
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

	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		CatalogRepo:    newInMemoryTableCatalog(),
	})

	unauthResp := httptest.NewRecorder()
	h.ServeHTTP(unauthResp, httptest.NewRequest(http.MethodGet, "/v1/tables", nil))
	if unauthResp.Code != http.StatusUnauthorized {
		t.Fatalf("unauth status = %d", unauthResp.Code)
	}

	authReq := httptest.NewRequest(http.MethodGet, "/v1/tables", nil)
	authReq.Header.Set("X-API-Key", "k1")
	authResp := httptest.NewRecorder()
	h.ServeHTTP(authResp, authReq)
	if authResp.Code != http.StatusOK {
		t.Fatalf("auth status = %d", authResp.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(authResp.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if body["tenant_id"] != "t1" {
		t.Fatalf("tenant_id = %v", body["tenant_id"])
	}
}

func TestCombineReadinessChecksStopsOnFirstFailure(t *testing.T) {
	order := make([]int, 0, 3)
	combined := CombineReadinessChecks(
		func(_ context.Context) error {
			order = append(order, 1)
			return nil
		},
		func(_ context.Context) error {
			order = append(order, 2)
			return errors.New("boom")
		},
		func(_ context.Context) error {
			order = append(order, 3)
			return nil
		},
	)

	err := combined(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("execution order = %#v", order)
	}
}

func TestUIHandlerServesNonAPIRoutes(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		UI: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "<html>ok</html>")
		}),
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

func mapLookup(values map[string]string) config.LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
