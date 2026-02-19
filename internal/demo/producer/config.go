package producer

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type LookupFunc func(string) (string, bool)

type Config struct {
	APIBaseURL          string
	APIKey              string
	TenantID            string
	TableName           string
	ProducerID          string
	BatchSize           int
	Interval            time.Duration
	HTTPTimeout         time.Duration
	CreateTable         bool
	WaitForVisibility   bool
	VisibilityTimeoutMS int
	UserCardinality     int
	Seed                int64
}

func DefaultConfig() Config {
	return Config{
		APIBaseURL:          "http://localhost:8080",
		APIKey:              "",
		TenantID:            "tenant-dev",
		TableName:           "events",
		ProducerID:          "demo-producer",
		BatchSize:           25,
		Interval:            time.Second,
		HTTPTimeout:         10 * time.Second,
		CreateTable:         true,
		WaitForVisibility:   false,
		VisibilityTimeoutMS: 3000,
		UserCardinality:     200,
		Seed:                time.Now().UTC().UnixNano(),
	}
}

func LoadConfigFromEnv(lookup LookupFunc) (Config, error) {
	if lookup == nil {
		return Config{}, fmt.Errorf("lookup function is required")
	}

	cfg := DefaultConfig()
	if err := applyString(lookup, "DUCKMESH_DEMO_API_URL", &cfg.APIBaseURL); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_DEMO_API_KEY", &cfg.APIKey); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_DEMO_TENANT_ID", &cfg.TenantID); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_DEMO_TABLE", &cfg.TableName); err != nil {
		return Config{}, err
	}
	if err := applyString(lookup, "DUCKMESH_DEMO_PRODUCER_ID", &cfg.ProducerID); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_DEMO_BATCH_SIZE", &cfg.BatchSize); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_DEMO_INTERVAL", &cfg.Interval); err != nil {
		return Config{}, err
	}
	if err := applyDuration(lookup, "DUCKMESH_DEMO_HTTP_TIMEOUT", &cfg.HTTPTimeout); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_DEMO_CREATE_TABLE", &cfg.CreateTable); err != nil {
		return Config{}, err
	}
	if err := applyBool(lookup, "DUCKMESH_DEMO_WAIT_FOR_VISIBILITY", &cfg.WaitForVisibility); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_DEMO_VISIBILITY_TIMEOUT_MS", &cfg.VisibilityTimeoutMS); err != nil {
		return Config{}, err
	}
	if err := applyInt(lookup, "DUCKMESH_DEMO_USER_CARDINALITY", &cfg.UserCardinality); err != nil {
		return Config{}, err
	}
	if err := applyInt64(lookup, "DUCKMESH_DEMO_SEED", &cfg.Seed); err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_API_URL is required")
	}
	if strings.TrimSpace(cfg.TenantID) == "" {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_TENANT_ID is required")
	}
	if strings.TrimSpace(cfg.TableName) == "" {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_TABLE is required")
	}
	if strings.TrimSpace(cfg.ProducerID) == "" {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_PRODUCER_ID is required")
	}
	if cfg.BatchSize <= 0 {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_BATCH_SIZE must be > 0")
	}
	if cfg.Interval <= 0 {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_INTERVAL must be > 0")
	}
	if cfg.HTTPTimeout <= 0 {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_HTTP_TIMEOUT must be > 0")
	}
	if cfg.VisibilityTimeoutMS <= 0 {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_VISIBILITY_TIMEOUT_MS must be > 0")
	}
	if cfg.UserCardinality <= 0 {
		return Config{}, fmt.Errorf("DUCKMESH_DEMO_USER_CARDINALITY must be > 0")
	}

	cfg.APIBaseURL = strings.TrimRight(strings.TrimSpace(cfg.APIBaseURL), "/")
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.TenantID = strings.TrimSpace(cfg.TenantID)
	cfg.TableName = strings.TrimSpace(cfg.TableName)
	cfg.ProducerID = strings.TrimSpace(cfg.ProducerID)
	return cfg, nil
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
	v, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = v
	return nil
}

func applyBool(lookup LookupFunc, key string, dst *bool) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = v
	return nil
}

func applyInt(lookup LookupFunc, key string, dst *int) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = v
	return nil
}

func applyInt64(lookup LookupFunc, key string, dst *int64) error {
	raw, ok := lookup(key)
	if !ok {
		return nil
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", key, err)
	}
	*dst = v
	return nil
}
