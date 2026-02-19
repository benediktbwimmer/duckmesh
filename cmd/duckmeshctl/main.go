package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/duckmesh/duckmesh/internal/cli/duckmeshctl"
)

func main() {
	timeout := parseDurationWithDefault(strings.TrimSpace(os.Getenv("DUCKMESH_CLI_TIMEOUT")), 10*time.Second)
	options := duckmeshctl.Options{
		BaseURL:  envOr("DUCKMESH_API_URL", "http://localhost:8080"),
		APIKey:   strings.TrimSpace(os.Getenv("DUCKMESH_API_KEY")),
		TenantID: strings.TrimSpace(os.Getenv("DUCKMESH_TENANT_ID")),
		Timeout:  timeout,
		Stdout:   os.Stdout,
		Stderr:   os.Stderr,
	}

	code := duckmeshctl.Run(context.Background(), os.Args[1:], options)
	os.Exit(code)
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseDurationWithDefault(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid DUCKMESH_CLI_TIMEOUT %q; using %s\n", raw, fallback)
		return fallback
	}
	return parsed
}
