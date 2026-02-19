package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	buspostgres "github.com/duckmesh/duckmesh/internal/bus/postgres"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/coordinator"
	"github.com/duckmesh/duckmesh/internal/observability"
	s3store "github.com/duckmesh/duckmesh/internal/storage/s3"
)

func main() {
	cfg, err := config.LoadFromEnv("duckmesh-coordinator")
	if err != nil {
		slog.Error("failed to load config", slog.Any("error", err))
		os.Exit(1)
	}

	logger := observability.NewLogger(cfg, os.Stdout)

	db, err := catalogpostgres.Open(context.Background(), catalogpostgres.DBConfig{
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
	defer func() { _ = db.Close() }()

	store, err := s3store.New(context.Background(), s3store.Config{
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

	svc := &coordinator.Service{
		Bus:         buspostgres.NewIngestBus(db),
		Publisher:   catalogpostgres.NewRepository(db),
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   cfg.Coordinator.ConsumerID,
			ClaimLimit:   cfg.Coordinator.ClaimLimit,
			LeaseSeconds: cfg.Coordinator.LeaseSeconds,
			PollInterval: cfg.Coordinator.PollInterval,
			CreatedBy:    cfg.Coordinator.CreatedBy,
		},
		Logger: logger,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("coordinator worker started")
	if err := svc.Run(ctx); err != nil {
		logger.Error("coordinator worker failed", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("coordinator worker stopped")
}
