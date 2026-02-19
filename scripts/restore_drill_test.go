package scripts

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRestoreDrillDryRun(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	scriptPath := filepath.Join(filepath.Dir(thisFile), "restore_drill.sh")

	cmd := exec.Command("bash", scriptPath, "--dry-run")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("dry-run failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	out := stdout.String()
	expected := []string{
		"creating catalog backup",
		"creating restore verification database",
		"restoring backup into verification database",
		"comparing key catalog counts source vs restored",
		"verifying migration version metadata parity",
		"running restored catalog consistency checks",
		"skipping API integrity check",
		"restore drill succeeded",
	}
	for _, token := range expected {
		if !strings.Contains(out, token) {
			t.Fatalf("output missing %q\noutput:\n%s", token, out)
		}
	}
}

func TestRestoreDrillUnknownArgument(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	scriptPath := filepath.Join(filepath.Dir(thisFile), "restore_drill.sh")

	cmd := exec.Command("bash", scriptPath, "--not-a-real-flag")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit for unknown flag")
	}
	if !strings.Contains(stderr.String(), "unknown argument") {
		t.Fatalf("stderr missing unknown argument message:\n%s", stderr.String())
	}
}
