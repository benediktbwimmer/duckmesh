package storage

import (
	"testing"
	"time"
)

func TestBuildDataFilePath(t *testing.T) {
	ts := time.Date(2026, time.February, 19, 4, 5, 0, 0, time.FixedZone("x", -5*3600))
	key, err := BuildDataFilePath("tenant-1", "events", ts, 55, 3)
	if err != nil {
		t.Fatalf("BuildDataFilePath() error = %v", err)
	}
	want := "tenant-1/events/date=2026-02-19/hour=09/part-55-00003.parquet"
	if key != want {
		t.Fatalf("BuildDataFilePath() = %q, want %q", key, want)
	}
}

func TestBuildDeleteFilePath(t *testing.T) {
	key, err := BuildDeleteFilePath("tenant-1", "events", 55, 4)
	if err != nil {
		t.Fatalf("BuildDeleteFilePath() error = %v", err)
	}
	want := "tenant-1/events/deletes/delete-55-00004.parquet"
	if key != want {
		t.Fatalf("BuildDeleteFilePath() = %q, want %q", key, want)
	}
}

func TestBuildPathRejectsInvalidComponent(t *testing.T) {
	_, err := BuildDataFilePath("../oops", "events", time.Now(), 1, 1)
	if err == nil {
		t.Fatal("expected invalid component error")
	}
}
