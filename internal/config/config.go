package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type LookupFunc func(string) (string, bool)

type Profile string

const (
	ProfileDev  Profile = "dev"
	ProfileTest Profile = "test"
	ProfileProd Profile = "prod"
)

type Config struct {
	Profile       Profile
	Service       ServiceConfig
	HTTP          HTTPConfig
	Catalog       CatalogConfig
	ObjectStore   ObjectStoreConfig
	Coordinator   CoordinatorConfig
	Maintenance   MaintenanceConfig
	UI            UIConfig
	AI            AIConfig
	Observability ObservabilityConfig
	Auth          AuthConfig
}

type ServiceConfig struct {
	Name string
}

type HTTPConfig struct {
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type CatalogConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

type ObjectStoreConfig struct {
	Endpoint         string
	Region           string
	Bucket           string
	AccessKeyID      string
	SecretAccessKey  string
	UseSSL           bool
	Prefix           string
	AutoCreateBucket bool
}

type CoordinatorConfig struct {
	ConsumerID   string
	ClaimLimit   int
	LeaseSeconds int
	PollInterval time.Duration
	CreatedBy    string
}

type MaintenanceConfig struct {
	CompactionInterval      time.Duration
	CompactionMinInputFiles int
	RetentionInterval       time.Duration
	IntegritySnapshotLimit  int
	KeepSnapshots           int
	GCSafetyAge             time.Duration
	CreatedBy               string
}

type UIConfig struct {
	SchemaSampleRows int
}

type AIConfig struct {
	TranslateEnabled bool
	BaseURL          string
	APIKey           string
	Model            string
	Temperature      float64
	Timeout          time.Duration
}

type ObservabilityConfig struct {
	LogLevel slog.Level
	LogJSON  bool
}

type AuthConfig struct {
	Required   bool
	StaticKeys string
}

func LoadFromEnv(serviceName string) (Config, error) {
	return Load(serviceName, os.LookupEnv)
}

func Load(serviceName string, lookup LookupFunc) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("lookup function is required")
	}

	profile := ProfileDev
	if raw, ok := lookup("DUCKMESH_PROFILE"); ok {
		profile = Profile(strings.ToLower(strings.TrimSpace(raw)))
	}
	if !isValidProfile(profile) {
		return Config{}, fmt.Errorf("invalid DUCKMESH_PROFILE: %q", profile)
	}

	cfg := defaultsForProfile(profile)
	if serviceName != "" {
		cfg.Service.Name = serviceName
	}

	if err := applyString(lookup, "DUCKMESH_SERVICE_NAME", &cfg.Service.Name); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_HTTP_ADDR", &cfg.HTTP.Address); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_CATALOG_DSN", &cfg.Catalog.DSN); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_CATALOG_MAX_OPEN_CONNS", &cfg.Catalog.MaxOpenConns); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_CATALOG_MAX_IDLE_CONNS", &cfg.Catalog.MaxIdleConns); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_CATALOG_CONN_MAX_IDLE_TIME", &cfg.Catalog.ConnMaxIdleTime); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_CATALOG_CONN_MAX_LIFETIME", &cfg.Catalog.ConnMaxLifetime); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_ENDPOINT", &cfg.ObjectStore.Endpoint); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_REGION", &cfg.ObjectStore.Region); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_BUCKET", &cfg.ObjectStore.Bucket); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_ACCESS_KEY", &cfg.ObjectStore.AccessKeyID); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_SECRET_KEY", &cfg.ObjectStore.SecretAccessKey); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_OBJECTSTORE_USE_SSL", &cfg.ObjectStore.UseSSL); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_OBJECTSTORE_PREFIX", &cfg.ObjectStore.Prefix); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_OBJECTSTORE_AUTO_CREATE_BUCKET", &cfg.ObjectStore.AutoCreateBucket); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_COORDINATOR_CONSUMER_ID", &cfg.Coordinator.ConsumerID); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_COORDINATOR_CLAIM_LIMIT", &cfg.Coordinator.ClaimLimit); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_COORDINATOR_LEASE_SECONDS", &cfg.Coordinator.LeaseSeconds); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_COORDINATOR_POLL_INTERVAL", &cfg.Coordinator.PollInterval); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_COORDINATOR_CREATED_BY", &cfg.Coordinator.CreatedBy); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_MAINTENANCE_COMPACTION_INTERVAL", &cfg.Maintenance.CompactionInterval); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_MAINTENANCE_COMPACTION_MIN_INPUT_FILES", &cfg.Maintenance.CompactionMinInputFiles); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_MAINTENANCE_RETENTION_INTERVAL", &cfg.Maintenance.RetentionInterval); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_MAINTENANCE_INTEGRITY_SNAPSHOT_LIMIT", &cfg.Maintenance.IntegritySnapshotLimit); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_MAINTENANCE_KEEP_SNAPSHOTS", &cfg.Maintenance.KeepSnapshots); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_MAINTENANCE_GC_SAFETY_AGE", &cfg.Maintenance.GCSafetyAge); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_MAINTENANCE_CREATED_BY", &cfg.Maintenance.CreatedBy); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_UI_SCHEMA_SAMPLE_ROWS", &cfg.UI.SchemaSampleRows); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_AI_TRANSLATE_ENABLED", &cfg.AI.TranslateEnabled); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_AI_BASE_URL", &cfg.AI.BaseURL); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_AI_API_KEY", &cfg.AI.APIKey); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_AI_MODEL", &cfg.AI.Model); err != nil {
		return Config{}, err
	}
	if err := applyFloat(lookup, "DUCKMESH_AI_TEMPERATURE", &cfg.AI.Temperature); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_AI_TIMEOUT", &cfg.AI.Timeout); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_LOG_JSON", &cfg.Observability.LogJSON); err != nil {
		return Config{}, err
	}
	if err := applyLogLevel(lookup, "DUCKMESH_LOG_LEVEL", &cfg.Observability.LogLevel); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_AUTH_REQUIRED", &cfg.Auth.Required); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_AUTH_STATIC_KEYS", &cfg.Auth.StaticKeys); err != nil {
		return Config{}, err
	}

	if cfg.Service.Name == "" {
		return Config{}, fmt.Errorf("service name is required")
	}
	if cfg.HTTP.Address == "" {
		return Config{}, fmt.Errorf("http address is required")
	}
	return cfg, nil
}

func defaultsForProfile(profile Profile) Config {
	cfg := Config{
		Profile: profile,
		Service: ServiceConfig{Name: "duckmesh-api"},
		HTTP: HTTPConfig{
			Address:      ":8080",
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		Catalog: CatalogConfig{
			DSN:             "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
			MaxOpenConns:    20,
			MaxIdleConns:    20,
			ConnMaxIdleTime: 5 * time.Minute,
			ConnMaxLifetime: 30 * time.Minute,
		},
		ObjectStore: ObjectStoreConfig{
			Endpoint:         "localhost:9000",
			Region:           "us-east-1",
			Bucket:           "duckmesh",
			AccessKeyID:      "minio",
			SecretAccessKey:  "miniostorage",
			UseSSL:           false,
			Prefix:           "",
			AutoCreateBucket: true,
		},
		Coordinator: CoordinatorConfig{
			ConsumerID:   "duckmesh-coordinator",
			ClaimLimit:   500,
			LeaseSeconds: 30,
			PollInterval: 300 * time.Millisecond,
			CreatedBy:    "duckmesh-coordinator",
		},
		Maintenance: MaintenanceConfig{
			CompactionInterval:      2 * time.Minute,
			CompactionMinInputFiles: 4,
			RetentionInterval:       10 * time.Minute,
			IntegritySnapshotLimit:  20,
			KeepSnapshots:           3,
			GCSafetyAge:             30 * time.Minute,
			CreatedBy:               "duckmesh-compactor",
		},
		UI: UIConfig{
			SchemaSampleRows: 5,
		},
		AI: AIConfig{
			TranslateEnabled: false,
			BaseURL:          "https://api.openai.com",
			Model:            "gpt-5",
			Temperature:      0.1,
			Timeout:          15 * time.Second,
		},
		Observability: ObservabilityConfig{
			LogLevel: slog.LevelDebug,
			LogJSON:  true,
		},
		Auth: AuthConfig{
			Required:   false,
			StaticKeys: "",
		},
	}

	switch profile {
	case ProfileTest:
		cfg.HTTP.Address = ":18080"
		cfg.Observability.LogLevel = slog.LevelWarn
		cfg.Auth.Required = false
	case ProfileProd:
		cfg.Observability.LogLevel = slog.LevelInfo
		cfg.Auth.Required = true
		cfg.ObjectStore.UseSSL = true
		cfg.ObjectStore.AutoCreateBucket = false
	}

	return cfg
}

func isValidProfile(profile Profile) bool {
	switch profile {
	case ProfileDev, ProfileTest, ProfileProd:
		return true
	default:
		return false
	}
}

func applyString(lookup LookupFunc, key string, dst *string) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	*dst = strings.TrimSpace(raw)
	return nil
}

func applyDuration(lookup LookupFunc, key string, dst *time.Duration) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = value
	return nil
}

func applyBool(lookup LookupFunc, key string, dst *bool) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = value
	return nil
}

func applyInt(lookup LookupFunc, key string, dst *int) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = value
	return nil
}

func applyFloat(lookup LookupFunc, key string, dst *float64) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = value
	return nil
}

func applyLogLevel(lookup LookupFunc, key string, dst *slog.Level) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	level := strings.ToLower(strings.TrimSpace(raw))
	switch level {
	case "debug":
		*dst = slog.LevelDebug
	case "info":
		*dst = slog.LevelInfo
	case "warn", "warning":
		*dst = slog.LevelWarn
	case "error":
		*dst = slog.LevelError
	default:
		return fmt.Errorf("invalid %s: %q", key, raw)
	}
	return nil
}
