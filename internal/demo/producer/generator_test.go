package producer

import (
	"reflect"
	"testing"
	"time"
)

func TestGeneratorDeterministicForSeed(t *testing.T) {
	fixedNow := time.Date(2026, 2, 19, 7, 30, 0, 0, time.UTC)

	g1 := NewGenerator(42, "producer-a", 10)
	g2 := NewGenerator(42, "producer-a", 10)
	g1.now = func() time.Time { return fixedNow }
	g2.now = func() time.Time { return fixedNow }

	for i := 0; i < 5; i++ {
		r1 := g1.NextRecord()
		r2 := g2.NextRecord()
		if !reflect.DeepEqual(r1, r2) {
			t.Fatalf("record %d differs: %#v vs %#v", i, r1, r2)
		}
	}
}

func TestGeneratorIdempotencyKeyMonotonic(t *testing.T) {
	g := NewGenerator(99, "producer-b", 5)
	g.now = func() time.Time { return time.Unix(0, 0).UTC() }

	seen := map[string]struct{}{}
	for i := 1; i <= 50; i++ {
		record := g.NextRecord()
		if _, ok := seen[record.IdempotencyKey]; ok {
			t.Fatalf("duplicate idempotency key: %s", record.IdempotencyKey)
		}
		seen[record.IdempotencyKey] = struct{}{}

		eventID, ok := record.Payload["event_id"].(int64)
		if !ok {
			t.Fatalf("event_id type = %T, want int64", record.Payload["event_id"])
		}
		if eventID != int64(i) {
			t.Fatalf("event_id = %d, want %d", eventID, i)
		}
	}
}
