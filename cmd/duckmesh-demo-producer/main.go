package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/duckmesh/duckmesh/internal/demo/producer"
)

func main() {
	cfg, err := producer.LoadConfigFromEnv(os.LookupEnv)
	if err != nil {
		slog.Error("failed to load demo producer config", slog.Any("error", err))
		os.Exit(1)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	service, err := producer.NewService(cfg, logger, nil)
	if err != nil {
		logger.Error("failed to initialize demo producer", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info(
		"demo producer started",
		slog.String("api_url", cfg.APIBaseURL),
		slog.String("tenant_id", cfg.TenantID),
		slog.String("table", cfg.TableName),
		slog.Int("batch_size", cfg.BatchSize),
		slog.Duration("interval", cfg.Interval),
		slog.Bool("create_table", cfg.CreateTable),
	)

	err = service.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("demo producer stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("demo producer stopped")
}
