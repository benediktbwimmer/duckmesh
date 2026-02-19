package nl2sql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type OpenAIConfig struct {
	BaseURL     string
	APIKey      string
	Model       string
	Temperature float64
	Timeout     time.Duration
}

type OpenAITranslator struct {
	baseURL     string
	apiKey      string
	model       string
	temperature float64
	client      *http.Client
}

func NewOpenAITranslator(cfg OpenAIConfig) (*OpenAITranslator, error) {
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("base URL is required")
	}
	if strings.TrimSpace(cfg.APIKey) == "" {
		return nil, fmt.Errorf("api key is required")
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		model = "gpt-5"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &OpenAITranslator{
		baseURL:     strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"),
		apiKey:      strings.TrimSpace(cfg.APIKey),
		model:       model,
		temperature: cfg.Temperature,
		client:      &http.Client{Timeout: timeout},
	}, nil
}

func (t *OpenAITranslator) Translate(ctx context.Context, req Request) (Result, error) {
	promptPayload, err := buildOpenAIPayload(t.model, t.temperature, req)
	if err != nil {
		return Result{}, err
	}
	body, err := json.Marshal(promptPayload)
	if err != nil {
		return Result{}, fmt.Errorf("marshal chat payload: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Result{}, fmt.Errorf("build chat request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+t.apiKey)

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return Result{}, fmt.Errorf("request chat completion: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	rawRespBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, fmt.Errorf("read chat response body: %w", err)
	}
	if resp.StatusCode >= 400 {
		return Result{}, fmt.Errorf("chat completion failed status=%d body=%s", resp.StatusCode, string(rawRespBody))
	}

	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rawRespBody, &parsed); err != nil {
		return Result{}, fmt.Errorf("decode chat completion response: %w", err)
	}
	if len(parsed.Choices) == 0 {
		return Result{}, fmt.Errorf("empty chat completion choices")
	}

	sql := stripMarkdownSQL(parsed.Choices[0].Message.Content)
	if strings.TrimSpace(sql) == "" {
		return Result{}, fmt.Errorf("model returned empty SQL")
	}
	return Result{
		SQL:      sql,
		Provider: "openai-compatible",
		Model:    t.model,
	}, nil
}

func buildOpenAIPayload(model string, temperature float64, req Request) (map[string]any, error) {
	tablesJSON, err := json.Marshal(req.Tables)
	if err != nil {
		return nil, fmt.Errorf("marshal table context: %w", err)
	}
	systemPrompt := "You convert natural language analytics requests into a single DuckDB SQL query. " +
		"DuckDB uses PostgreSQL-like SQL syntax. " +
		"Return ONLY SQL. No markdown, no explanation."
	userPrompt := fmt.Sprintf(
		"Tenant: %s\nSchema and sample context (JSON):\n%s\n\nUser request:\n%s\n\nRules:\n- Use only listed tables.\n- Prefer explicit columns.\n- Add LIMIT 200 unless user asks otherwise.\n- Output a single SQL query only.",
		req.TenantID,
		string(tablesJSON),
		strings.TrimSpace(req.NaturalLanguage),
	)

	return map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
		"temperature": temperature,
	}, nil
}

func stripMarkdownSQL(value string) string {
	trimmed := strings.TrimSpace(value)
	if strings.HasPrefix(trimmed, "```") {
		trimmed = strings.TrimPrefix(trimmed, "```sql")
		trimmed = strings.TrimPrefix(trimmed, "```")
		trimmed = strings.TrimSuffix(trimmed, "```")
		return strings.TrimSpace(trimmed)
	}
	return trimmed
}
