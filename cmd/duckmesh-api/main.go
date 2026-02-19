package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/duckmesh/duckmesh/internal/api"
	"github.com/duckmesh/duckmesh/internal/api/uistatic"
	"github.com/duckmesh/duckmesh/internal/auth"
	buspostgres "github.com/duckmesh/duckmesh/internal/bus/postgres"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/maintenance"
	"github.com/duckmesh/duckmesh/internal/nl2sql"
	"github.com/duckmesh/duckmesh/internal/observability"
	duckdbengine "github.com/duckmesh/duckmesh/internal/query/duckdb"
	s3store "github.com/duckmesh/duckmesh/internal/storage/s3"
)

func main() {
	cfg, err := config.LoadFromEnv("duckmesh-api")
	if err != nil {
		slog.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	logger := observability.NewLogger(cfg, os.Stdout)
	catalogDB, err := catalogpostgres.Open(context.Background(), catalogpostgres.DBConfig{
		DSN:             cfg.Catalog.DSN,
		MaxOpenConns:    cfg.Catalog.MaxOpenConns,
		MaxIdleConns:    cfg.Catalog.MaxIdleConns,
		ConnMaxIdleTime: cfg.Catalog.ConnMaxIdleTime,
		ConnMaxLifetime: cfg.Catalog.ConnMaxLifetime,
	})
	if err != nil {
		logger.Error("failed to open catalog db", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = catalogDB.Close() }()

	catalogRepo := catalogpostgres.NewRepository(catalogDB)
	ingestBus := buspostgres.NewIngestBus(catalogDB)
	objectStore, err := s3store.New(context.Background(), s3store.Config{
		Endpoint:         cfg.ObjectStore.Endpoint,
		Region:           cfg.ObjectStore.Region,
		Bucket:           cfg.ObjectStore.Bucket,
		AccessKeyID:      cfg.ObjectStore.AccessKeyID,
		SecretAccessKey:  cfg.ObjectStore.SecretAccessKey,
		UseSSL:           cfg.ObjectStore.UseSSL,
		Prefix:           cfg.ObjectStore.Prefix,
		AutoCreateBucket: cfg.ObjectStore.AutoCreateBucket,
	})
	if err != nil {
		logger.Error("failed to initialize object store", slog.Any("error", err))
		os.Exit(1)
	}
	queryEngine := duckdbengine.NewEngine(objectStore)
	maintenanceService := &maintenance.Service{
		Catalog:     catalogRepo,
		ObjectStore: objectStore,
		Config: maintenance.Config{
			CompactionInterval:      cfg.Maintenance.CompactionInterval,
			CompactionMinInputFiles: cfg.Maintenance.CompactionMinInputFiles,
			RetentionInterval:       cfg.Maintenance.RetentionInterval,
			IntegritySnapshotLimit:  cfg.Maintenance.IntegritySnapshotLimit,
			KeepSnapshots:           cfg.Maintenance.KeepSnapshots,
			GCSafetyAge:             cfg.Maintenance.GCSafetyAge,
			CreatedBy:               cfg.Maintenance.CreatedBy,
		},
		Logger: logger,
	}

	var translator nl2sql.Translator
	if cfg.AI.TranslateEnabled {
		translator, err = nl2sql.NewOpenAITranslator(nl2sql.OpenAIConfig{
			BaseURL:     cfg.AI.BaseURL,
			APIKey:      cfg.AI.APIKey,
			Model:       cfg.AI.Model,
			Temperature: cfg.AI.Temperature,
			Timeout:     cfg.AI.Timeout,
		})
		if err != nil {
			logger.Error("failed to initialize query translator", slog.Any("error", err))
			os.Exit(1)
		}
	}

	deps := api.Dependencies{
		Logger:          logger,
		CatalogRepo:     catalogRepo,
		IngestBus:       ingestBus,
		QueryEngine:     queryEngine,
		Maintenance:     maintenanceService,
		QueryTranslator: translator,
		UISchemaSamples: cfg.UI.SchemaSampleRows,
		UI:              uistatic.Handler(),
		Readiness: api.CombineReadinessChecks(
			catalogRepo.HealthCheck,
			api.CheckObjectStoreConfig(cfg),
		),
		DependencyTimout: time.Second,
	}
	if cfg.Auth.Required {
		validator, err := auth.NewStaticAPIKeyValidator(cfg.Auth.StaticKeys)
		if err != nil {
			logger.Error("failed to parse static auth keys", slog.Any("error", err))
			os.Exit(1)
		}
		deps.AuthMiddleware = auth.Middleware(logger, validator)
	}

	handler := api.NewHandler(cfg, deps)
	server := &http.Server{
		Addr:         cfg.HTTP.Address,
		Handler:      handler,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("starting api server", slog.String("addr", cfg.HTTP.Address))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api server failed", slog.Any("error", err))
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Info("shutting down api server")
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
		_ = server.Close()
		os.Exit(1)
	}
}
