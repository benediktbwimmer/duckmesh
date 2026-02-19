package producer

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigFromEnvDefaults(t *testing.T) {
	cfg, err := LoadConfigFromEnv(mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}
	if cfg.APIBaseURL != "http://localhost:8080" {
		t.Fatalf("APIBaseURL = %q", cfg.APIBaseURL)
	}
	if cfg.TableName != "events" {
		t.Fatalf("TableName = %q", cfg.TableName)
	}
	if cfg.BatchSize <= 0 {
		t.Fatalf("BatchSize = %d", cfg.BatchSize)
	}
	if cfg.Interval <= 0 {
		t.Fatalf("Interval = %s", cfg.Interval)
	}
}

func TestLoadConfigFromEnvOverrides(t *testing.T) {
	cfg, err := LoadConfigFromEnv(mapLookup(map[string]string{
		"DUCKMESH_DEMO_API_URL":               "http://demo.local:18080",
		"DUCKMESH_DEMO_TENANT_ID":             "tenant-a",
		"DUCKMESH_DEMO_TABLE":                 "orders",
		"DUCKMESH_DEMO_PRODUCER_ID":           "seed-a",
		"DUCKMESH_DEMO_BATCH_SIZE":            "99",
		"DUCKMESH_DEMO_INTERVAL":              "1500ms",
		"DUCKMESH_DEMO_HTTP_TIMEOUT":          "30s",
		"DUCKMESH_DEMO_CREATE_TABLE":          "false",
		"DUCKMESH_DEMO_WAIT_FOR_VISIBILITY":   "true",
		"DUCKMESH_DEMO_VISIBILITY_TIMEOUT_MS": "7000",
		"DUCKMESH_DEMO_USER_CARDINALITY":      "333",
		"DUCKMESH_DEMO_SEED":                  "12345",
		"DUCKMESH_DEMO_API_KEY":               "abc",
	}))
	if err != nil {
		t.Fatalf("LoadConfigFromEnv() error = %v", err)
	}
	if cfg.APIBaseURL != "http://demo.local:18080" {
		t.Fatalf("APIBaseURL = %q", cfg.APIBaseURL)
	}
	if cfg.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %q", cfg.TenantID)
	}
	if cfg.TableName != "orders" {
		t.Fatalf("TableName = %q", cfg.TableName)
	}
	if cfg.BatchSize != 99 {
		t.Fatalf("BatchSize = %d", cfg.BatchSize)
	}
	if cfg.Interval != 1500*time.Millisecond {
		t.Fatalf("Interval = %s", cfg.Interval)
	}
	if cfg.HTTPTimeout != 30*time.Second {
		t.Fatalf("HTTPTimeout = %s", cfg.HTTPTimeout)
	}
	if cfg.CreateTable {
		t.Fatal("CreateTable = true, want false")
	}
	if !cfg.WaitForVisibility {
		t.Fatal("WaitForVisibility = false, want true")
	}
	if cfg.VisibilityTimeoutMS != 7000 {
		t.Fatalf("VisibilityTimeoutMS = %d", cfg.VisibilityTimeoutMS)
	}
	if cfg.UserCardinality != 333 {
		t.Fatalf("UserCardinality = %d", cfg.UserCardinality)
	}
	if cfg.Seed != 12345 {
		t.Fatalf("Seed = %d", cfg.Seed)
	}
	if cfg.APIKey != "abc" {
		t.Fatalf("APIKey = %q", cfg.APIKey)
	}
}

func TestLoadConfigFromEnvRejectsInvalidBatchSize(t *testing.T) {
	_, err := LoadConfigFromEnv(mapLookup(map[string]string{
		"DUCKMESH_DEMO_BATCH_SIZE": "0",
	}))
	if err == nil || !strings.Contains(err.Error(), "DUCKMESH_DEMO_BATCH_SIZE") {
		t.Fatalf("error = %v, want batch size validation error", err)
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		v, ok := values[key]
		return v, ok
	}
}
