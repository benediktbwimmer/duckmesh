package deployments

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestGrafanaDashboardJSONIsValid(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "deployments", "observability", "grafana", "duckmesh_slo_dashboard.json")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dashboard file: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(content, &decoded); err != nil {
		t.Fatalf("dashboard JSON parse error: %v", err)
	}

	title, _ := decoded["title"].(string)
	if strings.TrimSpace(title) == "" {
		t.Fatal("dashboard title is required")
	}
	panels, ok := decoded["panels"].([]any)
	if !ok || len(panels) == 0 {
		t.Fatal("dashboard must include at least one panel")
	}
}

func TestPrometheusRulesContainExpectedAlerts(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "deployments", "observability", "prometheus", "duckmesh_rules.yaml")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read rules file: %v", err)
	}
	text := string(content)

	requiredAlerts := []string{
		"DuckMeshIngestAckLatencyP95High",
		"DuckMeshWriteToVisibleLatencyP95High",
		"DuckMeshConsistencyTimeoutsDetected",
		"DuckMeshIngestLagEventsHigh",
		"DuckMeshVisibilityLagHigh",
		"DuckMeshIntegrityRunFailed",
		"DuckMeshIntegrityMissingFilesDetected",
	}
	for _, alertName := range requiredAlerts {
		if !strings.Contains(text, "alert: "+alertName) {
			t.Fatalf("rules missing alert %q", alertName)
		}
	}

	requiredMetrics := []string{
		"duckmesh:slo_ingest_ack_latency_ms_p95",
		"duckmesh:slo_write_to_visible_latency_ms_p95",
		"duckmesh:slo_consistency_timeouts_15m",
		"duckmesh:slo_ingest_lag_events",
		"duckmesh:slo_visibility_lag_ms",
		"duckmesh:slo_integrity_failures_30m",
		"duckmesh:slo_integrity_missing_files_30m",
	}
	for _, metricName := range requiredMetrics {
		matched, err := regexp.MatchString(regexp.QuoteMeta(metricName), text)
		if err != nil {
			t.Fatalf("regexp error for metric %q: %v", metricName, err)
		}
		if !matched {
			t.Fatalf("rules missing metric reference %q", metricName)
		}
	}
}

func TestPrometheusScrapeExampleContainsMetricsPathAndRules(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "deployments", "observability", "prometheus", "prometheus-scrape.example.yaml")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read scrape example: %v", err)
	}
	text := string(content)

	if !strings.Contains(text, "metrics_path: /v1/metrics") {
		t.Fatal("scrape example missing DuckMesh metrics path")
	}
	if !strings.Contains(text, "duckmesh_rules.yaml") {
		t.Fatal("scrape example missing duckmesh rule file reference")
	}
	if !strings.Contains(text, "duckmesh_recording_rules.yaml") {
		t.Fatal("scrape example missing duckmesh recording rule file reference")
	}
	if !strings.Contains(text, "job_name: duckmesh-api") {
		t.Fatal("scrape example missing duckmesh-api job")
	}
}

func TestPrometheusRecordingRulesContainExpectedRecords(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "deployments", "observability", "prometheus", "duckmesh_recording_rules.yaml")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read recording rules file: %v", err)
	}
	text := string(content)

	requiredRecords := []string{
		"duckmesh:slo_ingest_ack_latency_ms_p95",
		"duckmesh:slo_write_to_visible_latency_ms_p95",
		"duckmesh:slo_consistency_timeouts_15m",
		"duckmesh:slo_ingest_lag_events",
		"duckmesh:slo_visibility_lag_ms",
		"duckmesh:slo_integrity_failures_30m",
		"duckmesh:slo_integrity_missing_files_30m",
		"duckmesh:slo_integrity_failures_24h",
		"duckmesh:slo_integrity_missing_files_24h",
		"duckmesh:slo_http_error_rate_5m",
	}
	for _, recordName := range requiredRecords {
		if !strings.Contains(text, "record: "+recordName) {
			t.Fatalf("recording rules missing record %q", recordName)
		}
	}
}

func TestAlertmanagerExampleContainsSeverityRouting(t *testing.T) {
	root := repoRoot(t)
	path := filepath.Join(root, "deployments", "observability", "alertmanager", "alertmanager.example.yaml")

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read alertmanager example: %v", err)
	}
	text := string(content)

	requiredTokens := []string{
		"receiver: duckmesh-default",
		"severity=\"critical\"",
		"severity=\"warning\"",
		"name: duckmesh-critical",
		"name: duckmesh-warning",
		"inhibit_rules:",
		"group_by: [alertname, service, severity]",
	}
	for _, token := range requiredTokens {
		if !strings.Contains(text, token) {
			t.Fatalf("alertmanager example missing token %q", token)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), ".."))
}
