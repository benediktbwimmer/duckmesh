package scripts

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestStackScriptDryRunUp(t *testing.T) {
	scriptPath := stackScriptPath(t)

	cmd := exec.Command("bash", scriptPath, "up", "--dry-run")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("stack up dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	out := stdout.String()
	expected := []string{
		"[dry-run] docker compose",
		"[dry-run] cd",
		"[dry-run] nohup env",
		"stack is up",
	}
	for _, token := range expected {
		if !strings.Contains(out, token) {
			t.Fatalf("output missing %q\noutput:\n%s", token, out)
		}
	}
}

func TestStackScriptDryRunDown(t *testing.T) {
	scriptPath := stackScriptPath(t)

	cmd := exec.Command("bash", scriptPath, "down", "--dry-run")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("stack down dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	out := stdout.String()
	expected := []string{
		"[dry-run] cd",
		"[dry-run] docker compose",
		"stack is down",
	}
	for _, token := range expected {
		if !strings.Contains(out, token) {
			t.Fatalf("output missing %q\noutput:\n%s", token, out)
		}
	}
}

func TestStackScriptUnknownCommand(t *testing.T) {
	scriptPath := stackScriptPath(t)

	cmd := exec.Command("bash", scriptPath, "not-a-command")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit for unknown command")
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr missing unknown command message:\n%s", stderr.String())
	}
}

func stackScriptPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(thisFile), "stack.sh")
}
