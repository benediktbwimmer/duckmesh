package api

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestOpenAPIContainsImplementedOperationalPaths(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	openAPIPath := filepath.Join(repoRoot, "api", "openapi.yaml")

	content, err := os.ReadFile(openAPIPath)
	if err != nil {
		t.Fatalf("read openapi file error = %v", err)
	}
	text := string(content)

	requiredPaths := []string{
		"/v1/health:",
		"/v1/ready:",
		"/v1/metrics:",
		"/v1/tables:",
		"/v1/tables/{table}:",
		"/v1/ingest/{table}:",
		"/v1/query:",
		"/v1/ui/schema:",
		"/v1/query/translate:",
		"/v1/lag:",
		"/v1/compaction/run:",
		"/v1/retention/run:",
		"/v1/integrity/run:",
	}
	for _, path := range requiredPaths {
		if !strings.Contains(text, path) {
			t.Fatalf("openapi missing path %s", path)
		}
	}
}
