package postgres

import (
	"context"
	"testing"
)

func TestOpenRequiresDSN(t *testing.T) {
	_, err := Open(context.Background(), DBConfig{})
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}
