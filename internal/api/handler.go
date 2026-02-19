package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/duckmesh/duckmesh/internal/bus"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/maintenance"
	"github.com/duckmesh/duckmesh/internal/nl2sql"
	"github.com/duckmesh/duckmesh/internal/observability"
	"github.com/duckmesh/duckmesh/internal/query"
)

type ReadinessCheck func(ctx context.Context) error

type CatalogTableLookup interface {
	GetTableByName(ctx context.Context, tenantID, tableName string) (catalog.TableDef, error)
	ListTables(ctx context.Context, tenantID string) ([]catalog.TableDef, error)
	GetLatestSnapshot(ctx context.Context, tenantID string) (catalog.Snapshot, error)
	GetSnapshotByID(ctx context.Context, tenantID string, snapshotID int64) (catalog.Snapshot, error)
	GetSnapshotByTime(ctx context.Context, tenantID string, at time.Time) (catalog.Snapshot, error)
	ListSnapshotFiles(ctx context.Context, tenantID string, snapshotID int64) ([]catalog.SnapshotFileEntry, error)
}

type Dependencies struct {
	Logger           *slog.Logger
	Readiness        ReadinessCheck
	AuthMiddleware   func(http.Handler) http.Handler
	DependencyTimout time.Duration
	CatalogRepo      CatalogTableLookup
	IngestBus        bus.IngestBus
	QueryEngine      query.Engine
	Maintenance      MaintenanceRunner
	QueryTranslator  nl2sql.Translator
	UISchemaSamples  int
	UI               http.Handler
}

type MaintenanceRunner interface {
	RunCompactionOnce(ctx context.Context, tenantID string) (maintenance.CompactionSummary, error)
	RunRetentionOnce(ctx context.Context, tenantID string) (maintenance.RetentionSummary, error)
	RunIntegrityCheckOnce(ctx context.Context, tenantID string) (maintenance.IntegritySummary, error)
}

func NewHandler(cfg config.Config, deps Dependencies) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": cfg.Service.Name})
	})

	mux.HandleFunc("GET /v1/ready", func(w http.ResponseWriter, r *http.Request) {
		if deps.Readiness == nil {
			writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
			return
		}
		timeout := deps.DependencyTimout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := deps.Readiness(ctx); err != nil {
			writeError(r.Context(), w, http.StatusServiceUnavailable, "NOT_READY", err.Error(), true, nil)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
	})

	mux.Handle("GET /v1/metrics", promhttp.Handler())

	protected := http.NewServeMux()
	protected.HandleFunc("GET /v1/tables", func(w http.ResponseWriter, r *http.Request) {
		handleListTables(deps, w, r)
	})
	protected.HandleFunc("POST /v1/tables", func(w http.ResponseWriter, r *http.Request) {
		handleCreateTable(deps, w, r)
	})
	protected.HandleFunc("GET /v1/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		handleGetTable(deps, w, r)
	})
	protected.HandleFunc("PATCH /v1/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		handlePatchTable(deps, w, r)
	})
	protected.HandleFunc("DELETE /v1/tables/{table}", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteTable(deps, w, r)
	})

	protected.HandleFunc("POST /v1/ingest/{table}", func(w http.ResponseWriter, r *http.Request) {
		handleIngest(deps, w, r)
	})
	protected.HandleFunc("POST /v1/query", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(deps, w, r)
	})
	protected.HandleFunc("GET /v1/ui/schema", func(w http.ResponseWriter, r *http.Request) {
		handleUISchema(deps, w, r)
	})
	protected.HandleFunc("POST /v1/query/translate", func(w http.ResponseWriter, r *http.Request) {
		handleTranslateQuery(deps, w, r)
	})
	protected.HandleFunc("GET /v1/lag", func(w http.ResponseWriter, r *http.Request) {
		handleLag(deps, w, r)
	})
	protected.HandleFunc("POST /v1/compaction/run", func(w http.ResponseWriter, r *http.Request) {
		handleCompactionRun(deps, w, r)
	})
	protected.HandleFunc("POST /v1/retention/run", func(w http.ResponseWriter, r *http.Request) {
		handleRetentionRun(deps, w, r)
	})
	protected.HandleFunc("POST /v1/integrity/run", func(w http.ResponseWriter, r *http.Request) {
		handleIntegrityRun(deps, w, r)
	})

	var protectedHandler http.Handler = protected
	if cfg.Auth.Required {
		if deps.AuthMiddleware == nil {
			if deps.Logger != nil {
				deps.Logger.Error("auth required but auth middleware missing")
			}
			protectedHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeError(r.Context(), w, http.StatusInternalServerError, "AUTH_MIDDLEWARE_MISSING", "auth middleware is required by configuration", false, nil)
			})
		} else {
			protectedHandler = deps.AuthMiddleware(protectedHandler)
		}
	}
	mux.Handle("GET /v1/tables", protectedHandler)
	mux.Handle("POST /v1/tables", protectedHandler)
	mux.Handle("GET /v1/tables/{table}", protectedHandler)
	mux.Handle("PATCH /v1/tables/{table}", protectedHandler)
	mux.Handle("DELETE /v1/tables/{table}", protectedHandler)
	mux.Handle("POST /v1/ingest/{table}", protectedHandler)
	mux.Handle("POST /v1/query", protectedHandler)
	mux.Handle("GET /v1/ui/schema", protectedHandler)
	mux.Handle("POST /v1/query/translate", protectedHandler)
	mux.Handle("GET /v1/lag", protectedHandler)
	mux.Handle("POST /v1/compaction/run", protectedHandler)
	mux.Handle("POST /v1/retention/run", protectedHandler)
	mux.Handle("POST /v1/integrity/run", protectedHandler)
	if deps.UI != nil {
		mux.Handle("GET /{path...}", deps.UI)
	}

	middlewares := []func(http.Handler) http.Handler{
		observability.TraceMiddleware,
		observability.MetricsMiddleware,
	}
	if deps.Logger != nil {
		middlewares = append(middlewares, observability.LoggingMiddleware(deps.Logger))
	}
	return chain(mux, middlewares...)
}

func CheckCatalogDSN(cfg config.Config) ReadinessCheck {
	return func(_ context.Context) error {
		if cfg.Catalog.DSN == "" {
			return errors.New("catalog dsn is not configured")
		}
		return nil
	}
}

func CheckObjectStoreConfig(cfg config.Config) ReadinessCheck {
	return func(_ context.Context) error {
		if cfg.ObjectStore.Endpoint == "" {
			return errors.New("object store endpoint is not configured")
		}
		if cfg.ObjectStore.Bucket == "" {
			return errors.New("object store bucket is not configured")
		}
		return nil
	}
}

func CombineReadinessChecks(checks ...ReadinessCheck) ReadinessCheck {
	filtered := make([]ReadinessCheck, 0, len(checks))
	for _, check := range checks {
		if check != nil {
			filtered = append(filtered, check)
		}
	}
	return func(ctx context.Context) error {
		for _, check := range filtered {
			if err := check(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

func chain(base http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	wrapped := base
	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}
	return wrapped
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(ctx context.Context, w http.ResponseWriter, status int, code, message string, retryable bool, extra map[string]any) {
	writeJSON(w, status, map[string]any{
		"error_code": code,
		"message":    message,
		"retryable":  retryable,
		"context":    extra,
		"trace_id":   observability.TraceIDFromContext(ctx),
	})
}
