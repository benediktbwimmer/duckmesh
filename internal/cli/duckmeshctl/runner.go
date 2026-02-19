package duckmeshctl

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Options struct {
	BaseURL    string
	APIKey     string
	TenantID   string
	Timeout    time.Duration
	HTTPClient *http.Client
	Stdout     io.Writer
	Stderr     io.Writer
}

func Run(ctx context.Context, args []string, defaults Options) int {
	stdout := defaults.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	stderr := defaults.Stderr
	if stderr == nil {
		stderr = io.Discard
	}

	fs := flag.NewFlagSet("duckmeshctl", flag.ContinueOnError)
	fs.SetOutput(stderr)

	baseURL := fs.String("base-url", firstNonEmpty(defaults.BaseURL, "http://localhost:8080"), "DuckMesh API base URL")
	apiKey := fs.String("api-key", defaults.APIKey, "API key for authenticated requests")
	tenantID := fs.String("tenant-id", defaults.TenantID, "Tenant ID header (used when auth is disabled)")
	timeout := fs.Duration("timeout", durationOr(defaults.Timeout, 10*time.Second), "HTTP timeout (e.g. 10s)")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		writeUsage(stderr)
		return 2
	}

	client := defaults.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: *timeout}
	}

	command := strings.TrimSpace(fs.Arg(0))
	method := ""
	path := ""
	switch command {
	case "health":
		method, path = http.MethodGet, "/v1/health"
	case "ready":
		method, path = http.MethodGet, "/v1/ready"
	case "lag":
		method, path = http.MethodGet, "/v1/lag"
	case "compaction-run":
		method, path = http.MethodPost, "/v1/compaction/run"
	case "retention-run":
		method, path = http.MethodPost, "/v1/retention/run"
	case "integrity-run":
		method, path = http.MethodPost, "/v1/integrity/run"
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", command)
		writeUsage(stderr)
		return 2
	}

	endpoint := strings.TrimRight(*baseURL, "/") + path
	code, responseBody, err := doRequest(ctx, client, method, endpoint, *apiKey, *tenantID)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "request failed: %v\n", err)
		return 1
	}

	if code >= 400 {
		_, _ = fmt.Fprintf(stderr, "http %d: %s\n", code, strings.TrimSpace(string(responseBody)))
		return 1
	}

	if pretty, ok := prettyJSON(responseBody); ok {
		_, _ = fmt.Fprintln(stdout, pretty)
		return 0
	}
	if len(responseBody) > 0 {
		_, _ = fmt.Fprintln(stdout, string(responseBody))
	}
	return 0
}

func doRequest(ctx context.Context, client *http.Client, method, url, apiKey, tenantID string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("X-API-Key", strings.TrimSpace(apiKey))
	}
	if strings.TrimSpace(tenantID) != "" {
		req.Header.Set("X-Tenant-ID", strings.TrimSpace(tenantID))
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func prettyJSON(raw []byte) (string, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", false
	}
	var anyValue any
	if err := json.Unmarshal(raw, &anyValue); err != nil {
		return "", false
	}
	formatted, err := json.MarshalIndent(anyValue, "", "  ")
	if err != nil {
		return "", false
	}
	return string(formatted), true
}

func writeUsage(w io.Writer) {
	_, _ = fmt.Fprintln(w, "usage: duckmeshctl [flags] <command>")
	_, _ = fmt.Fprintln(w, "")
	_, _ = fmt.Fprintln(w, "commands:")
	_, _ = fmt.Fprintln(w, "  health           GET /v1/health")
	_, _ = fmt.Fprintln(w, "  ready            GET /v1/ready")
	_, _ = fmt.Fprintln(w, "  lag              GET /v1/lag")
	_, _ = fmt.Fprintln(w, "  compaction-run   POST /v1/compaction/run")
	_, _ = fmt.Fprintln(w, "  retention-run    POST /v1/retention/run")
	_, _ = fmt.Fprintln(w, "  integrity-run    POST /v1/integrity/run")
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return strings.TrimSpace(a)
	}
	return b
}

func durationOr(v, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}
