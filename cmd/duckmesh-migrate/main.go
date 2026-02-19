package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/migrations"
)

func main() {
	direction := flag.String("direction", "up", "migration direction: up|down")
	steps := flag.Int("steps", 0, "number of migration steps; 0 means all for up, 1 for down")
	flag.Parse()

	cfg, err := config.LoadFromEnv("duckmesh-migrate")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	if cfg.Catalog.DSN == "" {
		fmt.Fprintln(os.Stderr, "DUCKMESH_CATALOG_DSN is required")
		os.Exit(1)
	}

	db, err := sql.Open("pgx", cfg.Catalog.DSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "database open error: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "database ping error: %v\n", err)
		os.Exit(1)
	}

	runner := migrations.NewRunner()
	switch *direction {
	case "up":
		applied, err := runner.Up(ctx, db, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "migration up failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("applied %d migration(s)\n", applied)
	case "down":
		applied, err := runner.Down(ctx, db, *steps)
		if err != nil {
			fmt.Fprintf(os.Stderr, "migration down failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("rolled back %d migration(s)\n", applied)
	default:
		fmt.Fprintf(os.Stderr, "invalid direction: %s\n", *direction)
		os.Exit(1)
	}
}
