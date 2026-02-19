package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadDefaultsForDevProfile(t *testing.T) {
	lookup := mapLookup(map[string]string{})
	cfg, err := Load("duckmesh-api", lookup)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Profile != ProfileDev {
		t.Fatalf("Profile = %q, want %q", cfg.Profile, ProfileDev)
	}
	if cfg.HTTP.Address != ":8080" {
		t.Fatalf("HTTP.Address = %q", cfg.HTTP.Address)
	}
	if cfg.Observability.LogLevel != slog.LevelDebug {
		t.Fatalf("LogLevel = %v", cfg.Observability.LogLevel)
	}
	if cfg.Auth.Required {
		t.Fatal("Auth.Required should default to false in dev")
	}
	if cfg.ObjectStore.Endpoint != "localhost:9000" {
		t.Fatalf("ObjectStore.Endpoint = %q", cfg.ObjectStore.Endpoint)
	}
	if cfg.Catalog.MaxOpenConns != 20 {
		t.Fatalf("Catalog.MaxOpenConns = %d", cfg.Catalog.MaxOpenConns)
	}
	if cfg.Coordinator.ClaimLimit != 500 {
		t.Fatalf("Coordinator.ClaimLimit = %d", cfg.Coordinator.ClaimLimit)
	}
	if cfg.Maintenance.CompactionMinInputFiles != 4 {
		t.Fatalf("Maintenance.CompactionMinInputFiles = %d", cfg.Maintenance.CompactionMinInputFiles)
	}
	if cfg.Maintenance.KeepSnapshots != 3 {
		t.Fatalf("Maintenance.KeepSnapshots = %d", cfg.Maintenance.KeepSnapshots)
	}
	if cfg.Maintenance.IntegritySnapshotLimit != 20 {
		t.Fatalf("Maintenance.IntegritySnapshotLimit = %d", cfg.Maintenance.IntegritySnapshotLimit)
	}
	if cfg.UI.SchemaSampleRows != 5 {
		t.Fatalf("UI.SchemaSampleRows = %d", cfg.UI.SchemaSampleRows)
	}
	if cfg.AI.TranslateEnabled {
		t.Fatal("AI.TranslateEnabled should default to false")
	}
	if cfg.AI.Model != "gpt-5" {
		t.Fatalf("AI.Model = %q", cfg.AI.Model)
	}
}

func TestLoadProdProfileDefaults(t *testing.T) {
	lookup := mapLookup(map[string]string{"DUCKMESH_PROFILE": "prod"})
	cfg, err := Load("duckmesh-api", lookup)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Profile != ProfileProd {
		t.Fatalf("Profile = %q, want %q", cfg.Profile, ProfileProd)
	}
	if !cfg.Auth.Required {
		t.Fatal("Auth.Required should default to true in prod")
	}
	if cfg.Observability.LogLevel != slog.LevelInfo {
		t.Fatalf("LogLevel = %v", cfg.Observability.LogLevel)
	}
	if !cfg.ObjectStore.UseSSL {
		t.Fatal("ObjectStore.UseSSL should default to true in prod")
	}
	if cfg.ObjectStore.AutoCreateBucket {
		t.Fatal("ObjectStore.AutoCreateBucket should default to false in prod")
	}
}

func TestLoadWithEnvOverrides(t *testing.T) {
	lookup := mapLookup(map[string]string{
		"DUCKMESH_PROFILE":                                "test",
		"DUCKMESH_HTTP_ADDR":                              ":9999",
		"DUCKMESH_HTTP_READ_TIMEOUT":                      "2s",
		"DUCKMESH_LOG_LEVEL":                              "error",
		"DUCKMESH_AUTH_REQUIRED":                          "true",
		"DUCKMESH_AUTH_STATIC_KEYS":                       "k1:t1:query_reader",
		"DUCKMESH_CATALOG_DSN":                            "postgres://example",
		"DUCKMESH_CATALOG_MAX_OPEN_CONNS":                 "42",
		"DUCKMESH_CATALOG_MAX_IDLE_CONNS":                 "17",
		"DUCKMESH_SERVICE_NAME":                           "duckmesh-custom",
		"DUCKMESH_HTTP_WRITE_TIMEOUT":                     "3s",
		"DUCKMESH_OBJECTSTORE_ENDPOINT":                   "s3.example.com",
		"DUCKMESH_OBJECTSTORE_BUCKET":                     "duckmesh-prod",
		"DUCKMESH_OBJECTSTORE_REGION":                     "us-west-2",
		"DUCKMESH_OBJECTSTORE_ACCESS_KEY":                 "abc",
		"DUCKMESH_OBJECTSTORE_SECRET_KEY":                 "def",
		"DUCKMESH_OBJECTSTORE_USE_SSL":                    "true",
		"DUCKMESH_OBJECTSTORE_PREFIX":                     "tenant-root",
		"DUCKMESH_OBJECTSTORE_AUTO_CREATE_BUCKET":         "false",
		"DUCKMESH_COORDINATOR_CONSUMER_ID":                "worker-1",
		"DUCKMESH_COORDINATOR_CLAIM_LIMIT":                "123",
		"DUCKMESH_COORDINATOR_LEASE_SECONDS":              "45",
		"DUCKMESH_COORDINATOR_POLL_INTERVAL":              "900ms",
		"DUCKMESH_COORDINATOR_CREATED_BY":                 "coordinator-a",
		"DUCKMESH_MAINTENANCE_COMPACTION_INTERVAL":        "11m",
		"DUCKMESH_MAINTENANCE_COMPACTION_MIN_INPUT_FILES": "7",
		"DUCKMESH_MAINTENANCE_RETENTION_INTERVAL":         "37m",
		"DUCKMESH_MAINTENANCE_INTEGRITY_SNAPSHOT_LIMIT":   "13",
		"DUCKMESH_MAINTENANCE_KEEP_SNAPSHOTS":             "9",
		"DUCKMESH_MAINTENANCE_GC_SAFETY_AGE":              "2h",
		"DUCKMESH_MAINTENANCE_CREATED_BY":                 "ops-worker-a",
		"DUCKMESH_UI_SCHEMA_SAMPLE_ROWS":                  "11",
		"DUCKMESH_AI_TRANSLATE_ENABLED":                   "true",
		"DUCKMESH_AI_BASE_URL":                            "https://api.example.com",
		"DUCKMESH_AI_API_KEY":                             "secret-key",
		"DUCKMESH_AI_MODEL":                               "gpt-5.2",
		"DUCKMESH_AI_TEMPERATURE":                         "0.3",
		"DUCKMESH_AI_TIMEOUT":                             "21s",
	})
	cfg, err := Load("duckmesh-api", lookup)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Service.Name != "duckmesh-custom" {
		t.Fatalf("Service.Name = %q", cfg.Service.Name)
	}
	if cfg.HTTP.Address != ":9999" {
		t.Fatalf("HTTP.Address = %q", cfg.HTTP.Address)
	}
	if cfg.HTTP.ReadTimeout != 2*time.Second {
		t.Fatalf("HTTP.ReadTimeout = %s", cfg.HTTP.ReadTimeout)
	}
	if cfg.HTTP.WriteTimeout != 3*time.Second {
		t.Fatalf("HTTP.WriteTimeout = %s", cfg.HTTP.WriteTimeout)
	}
	if cfg.Observability.LogLevel != slog.LevelError {
		t.Fatalf("LogLevel = %v", cfg.Observability.LogLevel)
	}
	if !cfg.Auth.Required {
		t.Fatal("Auth.Required = false, want true")
	}
	if cfg.Auth.StaticKeys != "k1:t1:query_reader" {
		t.Fatalf("StaticKeys = %q", cfg.Auth.StaticKeys)
	}
	if cfg.Catalog.DSN != "postgres://example" {
		t.Fatalf("Catalog.DSN = %q", cfg.Catalog.DSN)
	}
	if cfg.Catalog.MaxOpenConns != 42 {
		t.Fatalf("Catalog.MaxOpenConns = %d", cfg.Catalog.MaxOpenConns)
	}
	if cfg.Catalog.MaxIdleConns != 17 {
		t.Fatalf("Catalog.MaxIdleConns = %d", cfg.Catalog.MaxIdleConns)
	}
	if cfg.ObjectStore.Endpoint != "s3.example.com" {
		t.Fatalf("ObjectStore.Endpoint = %q", cfg.ObjectStore.Endpoint)
	}
	if cfg.ObjectStore.Bucket != "duckmesh-prod" {
		t.Fatalf("ObjectStore.Bucket = %q", cfg.ObjectStore.Bucket)
	}
	if !cfg.ObjectStore.UseSSL {
		t.Fatal("ObjectStore.UseSSL = false, want true")
	}
	if cfg.ObjectStore.AutoCreateBucket {
		t.Fatal("ObjectStore.AutoCreateBucket = true, want false")
	}
	if cfg.Coordinator.ConsumerID != "worker-1" {
		t.Fatalf("Coordinator.ConsumerID = %q", cfg.Coordinator.ConsumerID)
	}
	if cfg.Coordinator.ClaimLimit != 123 {
		t.Fatalf("Coordinator.ClaimLimit = %d", cfg.Coordinator.ClaimLimit)
	}
	if cfg.Coordinator.LeaseSeconds != 45 {
		t.Fatalf("Coordinator.LeaseSeconds = %d", cfg.Coordinator.LeaseSeconds)
	}
	if cfg.Coordinator.PollInterval != 900*time.Millisecond {
		t.Fatalf("Coordinator.PollInterval = %s", cfg.Coordinator.PollInterval)
	}
	if cfg.Coordinator.CreatedBy != "coordinator-a" {
		t.Fatalf("Coordinator.CreatedBy = %q", cfg.Coordinator.CreatedBy)
	}
	if cfg.Maintenance.CompactionInterval != 11*time.Minute {
		t.Fatalf("Maintenance.CompactionInterval = %s", cfg.Maintenance.CompactionInterval)
	}
	if cfg.Maintenance.CompactionMinInputFiles != 7 {
		t.Fatalf("Maintenance.CompactionMinInputFiles = %d", cfg.Maintenance.CompactionMinInputFiles)
	}
	if cfg.Maintenance.RetentionInterval != 37*time.Minute {
		t.Fatalf("Maintenance.RetentionInterval = %s", cfg.Maintenance.RetentionInterval)
	}
	if cfg.Maintenance.IntegritySnapshotLimit != 13 {
		t.Fatalf("Maintenance.IntegritySnapshotLimit = %d", cfg.Maintenance.IntegritySnapshotLimit)
	}
	if cfg.Maintenance.KeepSnapshots != 9 {
		t.Fatalf("Maintenance.KeepSnapshots = %d", cfg.Maintenance.KeepSnapshots)
	}
	if cfg.Maintenance.GCSafetyAge != 2*time.Hour {
		t.Fatalf("Maintenance.GCSafetyAge = %s", cfg.Maintenance.GCSafetyAge)
	}
	if cfg.Maintenance.CreatedBy != "ops-worker-a" {
		t.Fatalf("Maintenance.CreatedBy = %q", cfg.Maintenance.CreatedBy)
	}
	if cfg.UI.SchemaSampleRows != 11 {
		t.Fatalf("UI.SchemaSampleRows = %d", cfg.UI.SchemaSampleRows)
	}
	if !cfg.AI.TranslateEnabled {
		t.Fatal("AI.TranslateEnabled = false, want true")
	}
	if cfg.AI.BaseURL != "https://api.example.com" {
		t.Fatalf("AI.BaseURL = %q", cfg.AI.BaseURL)
	}
	if cfg.AI.APIKey != "secret-key" {
		t.Fatalf("AI.APIKey = %q", cfg.AI.APIKey)
	}
	if cfg.AI.Model != "gpt-5.2" {
		t.Fatalf("AI.Model = %q", cfg.AI.Model)
	}
	if cfg.AI.Temperature != 0.3 {
		t.Fatalf("AI.Temperature = %f", cfg.AI.Temperature)
	}
	if cfg.AI.Timeout != 21*time.Second {
		t.Fatalf("AI.Timeout = %s", cfg.AI.Timeout)
	}
}

func TestLoadErrorsOnInvalidValues(t *testing.T) {
	tests := []map[string]string{
		{"DUCKMESH_PROFILE": "oops"},
		{"DUCKMESH_HTTP_READ_TIMEOUT": "NaN"},
		{"DUCKMESH_CATALOG_MAX_OPEN_CONNS": "oops"},
		{"DUCKMESH_COORDINATOR_CLAIM_LIMIT": "oops"},
		{"DUCKMESH_MAINTENANCE_INTEGRITY_SNAPSHOT_LIMIT": "oops"},
		{"DUCKMESH_MAINTENANCE_KEEP_SNAPSHOTS": "oops"},
		{"DUCKMESH_AI_TEMPERATURE": "bad"},
		{"DUCKMESH_AUTH_REQUIRED": "not-bool"},
		{"DUCKMESH_LOG_LEVEL": "verbose"},
	}
	for _, env := range tests {
		_, err := Load("duckmesh-api", mapLookup(env))
		if err == nil {
			t.Fatalf("Load() expected error for env %#v", env)
		}
	}
}

func mapLookup(values map[string]string) LookupFunc {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
