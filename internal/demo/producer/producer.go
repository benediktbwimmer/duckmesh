package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Service struct {
	cfg       Config
	log       *slog.Logger
	http      *http.Client
	generator *Generator
}

type createTableRequest struct {
	TableName      string         `json:"table_name"`
	PrimaryKeyCols []string       `json:"primary_key_cols"`
	PartitionSpec  map[string]any `json:"partition_spec"`
	SchemaJSON     map[string]any `json:"schema_json"`
}

type ingestRequest struct {
	Records             []IngestRecord `json:"records"`
	WaitForVisibility   bool           `json:"wait_for_visibility,omitempty"`
	VisibilityTimeoutMS int            `json:"visibility_timeout_ms,omitempty"`
}

type ingestResponse struct {
	AcceptedCount      int   `json:"accepted_count"`
	DuplicateCount     int   `json:"duplicate_count"`
	MaxVisibilityToken int64 `json:"max_visibility_token"`
}

func NewService(cfg Config, logger *slog.Logger, client *http.Client) (*Service, error) {
	if strings.TrimSpace(cfg.APIBaseURL) == "" {
		return nil, fmt.Errorf("api base url is required")
	}
	if strings.TrimSpace(cfg.TenantID) == "" {
		return nil, fmt.Errorf("tenant id is required")
	}
	if strings.TrimSpace(cfg.TableName) == "" {
		return nil, fmt.Errorf("table name is required")
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if client == nil {
		client = &http.Client{Timeout: cfg.HTTPTimeout}
	}

	return &Service{
		cfg:       cfg,
		log:       logger,
		http:      client,
		generator: NewGenerator(cfg.Seed, cfg.ProducerID, cfg.UserCardinality),
	}, nil
}

func (s *Service) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	tableReady := !s.cfg.CreateTable

	for {
		if !tableReady {
			if err := s.ensureTable(ctx); err != nil {
				s.log.Error("failed to ensure demo table", slog.Any("error", err))
			} else {
				tableReady = true
			}
		} else {
			if err := s.produceOnce(ctx); err != nil {
				s.log.Error("failed to publish demo batch", slog.Any("error", err))
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) ensureTable(ctx context.Context) error {
	getStatus, getBody, err := s.doJSON(ctx, http.MethodGet, "/v1/tables/"+url.PathEscape(s.cfg.TableName), nil, nil)
	if err != nil {
		return fmt.Errorf("check table existence: %w", err)
	}
	switch getStatus {
	case http.StatusOK:
		s.log.Info("demo table already exists", slog.String("table", s.cfg.TableName))
		return nil
	case http.StatusNotFound:
	default:
		return fmt.Errorf("check table existence failed with status %d: %s", getStatus, strings.TrimSpace(string(getBody)))
	}

	req := createTableRequest{
		TableName:      s.cfg.TableName,
		PrimaryKeyCols: []string{"event_id"},
		PartitionSpec:  map[string]any{"date": "day"},
		SchemaJSON: map[string]any{
			"event_id":    "bigint",
			"user_id":     "varchar",
			"session_id":  "varchar",
			"event_type":  "varchar",
			"amount":      "double",
			"currency":    "varchar",
			"country":     "varchar",
			"device":      "varchar",
			"source":      "varchar",
			"occurred_at": "timestamp",
		},
	}
	status, body, err := s.doJSON(ctx, http.MethodPost, "/v1/tables", req, nil)
	if err != nil {
		return fmt.Errorf("create demo table: %w", err)
	}
	if status != http.StatusCreated {
		return fmt.Errorf("create demo table failed with status %d: %s", status, strings.TrimSpace(string(body)))
	}
	s.log.Info("created demo table", slog.String("table", s.cfg.TableName))
	return nil
}

func (s *Service) produceOnce(ctx context.Context) error {
	request := ingestRequest{
		Records: make([]IngestRecord, 0, s.cfg.BatchSize),
	}
	if s.cfg.WaitForVisibility {
		request.WaitForVisibility = true
		request.VisibilityTimeoutMS = s.cfg.VisibilityTimeoutMS
	}

	for i := 0; i < s.cfg.BatchSize; i++ {
		request.Records = append(request.Records, s.generator.NextRecord())
	}

	var response ingestResponse
	status, body, err := s.doJSON(ctx, http.MethodPost, "/v1/ingest/"+url.PathEscape(s.cfg.TableName), request, &response)
	if err != nil {
		return fmt.Errorf("ingest request failed: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("ingest request status %d: %s", status, strings.TrimSpace(string(body)))
	}

	s.log.Info(
		"published demo ingest batch",
		slog.String("tenant_id", s.cfg.TenantID),
		slog.String("table", s.cfg.TableName),
		slog.Int("batch_size", len(request.Records)),
		slog.Int("accepted_count", response.AcceptedCount),
		slog.Int("duplicate_count", response.DuplicateCount),
		slog.Int64("max_visibility_token", response.MaxVisibilityToken),
	)
	return nil
}

func (s *Service) doJSON(ctx context.Context, method, path string, requestBody any, responseBody any) (int, []byte, error) {
	var payload io.Reader
	if requestBody != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request body: %w", err)
		}
		payload = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, s.cfg.APIBaseURL+path, payload)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tenant-ID", s.cfg.TenantID)
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.cfg.APIKey != "" {
		req.Header.Set("X-API-Key", s.cfg.APIKey)
	}

	resp, err := s.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}

	if responseBody != nil && len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, responseBody); err != nil {
			return resp.StatusCode, body, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, body, nil
}
