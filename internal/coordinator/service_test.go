package coordinator

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/bus"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/storage"
)

func TestGroupEventsByTenantAndTable(t *testing.T) {
	events := []bus.Envelope{
		{EventID: "1", TenantID: "t1", TableID: "10"},
		{EventID: "2", TenantID: "t1", TableID: "10"},
		{EventID: "3", TenantID: "t1", TableID: "11"},
		{EventID: "4", TenantID: "t2", TableID: "10"},
	}
	groups := groupEventsByTenantAndTable(events)
	if len(groups) != 3 {
		t.Fatalf("len(groups) = %d", len(groups))
	}
	if groups[0].TenantID != "t1" || groups[0].TableID != 10 || len(groups[0].Events) != 2 {
		t.Fatalf("unexpected group[0] = %+v", groups[0])
	}
	if groups[0].MaxVisibilityToken != 2 {
		t.Fatalf("group[0] max token = %d", groups[0].MaxVisibilityToken)
	}
}

func TestProcessOncePublishesAndAcks(t *testing.T) {
	busStub := &stubBus{
		claimBatch: bus.Batch{
			BatchID: "100",
			Envelopes: []bus.Envelope{
				{EventID: "10", TenantID: "tenant", TableID: "20", IdempotencyKey: "k1", Op: "insert", PayloadJSON: []byte(`{"a":1}`), EventTimeUnixMs: time.Now().UTC().UnixMilli()},
			},
		},
	}
	publisher := &stubPublisher{}
	store := &stubStore{}

	svc := &Service{
		Bus:         busStub,
		Publisher:   publisher,
		ObjectStore: store,
		Config: Config{
			ConsumerID:   "worker-test",
			ClaimLimit:   10,
			LeaseSeconds: 10,
			CreatedBy:    "worker-test",
		},
		Clock: func() time.Time {
			return time.Date(2026, time.February, 19, 12, 0, 0, 0, time.UTC)
		},
	}

	if err := svc.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}
	if len(store.putKeys) != 1 {
		t.Fatalf("put calls = %d", len(store.putKeys))
	}
	if len(publisher.inputs) != 1 {
		t.Fatalf("publish calls = %d", len(publisher.inputs))
	}
	if len(busStub.acked) != 1 {
		t.Fatalf("ack calls = %d", len(busStub.acked))
	}
}

type stubBus struct {
	claimBatch bus.Batch
	acked      [][]string
}

func (s *stubBus) Publish(context.Context, []bus.Envelope) ([]bus.PublishResult, error) {
	return nil, nil
}

func (s *stubBus) ClaimBatch(context.Context, string, int, int) (bus.Batch, error) {
	return s.claimBatch, nil
}

func (s *stubBus) Ack(_ context.Context, _ string, eventIDs []string) error {
	s.acked = append(s.acked, append([]string(nil), eventIDs...))
	return nil
}

func (s *stubBus) Nack(context.Context, string, []string, string) error {
	return nil
}

func (s *stubBus) ExtendLease(context.Context, string, int) error {
	return nil
}

func (s *stubBus) RequeueExpired(context.Context) (int, error) {
	return 0, nil
}

type stubPublisher struct {
	nextID int64
	inputs []catalogpostgres.PublishBatchInput
}

func (s *stubPublisher) AllocateSnapshotID(context.Context) (int64, error) {
	if s.nextID == 0 {
		s.nextID = 900
	}
	s.nextID++
	return s.nextID, nil
}

func (s *stubPublisher) PublishBatch(_ context.Context, in catalogpostgres.PublishBatchInput) (catalogpostgres.PublishBatchResult, error) {
	s.inputs = append(s.inputs, in)
	return catalogpostgres.PublishBatchResult{SnapshotID: in.SnapshotID, FileID: 1}, nil
}

type stubStore struct {
	putKeys []string
}

func (s *stubStore) Put(_ context.Context, key string, body io.Reader, size int64, _ storage.PutOptions) (storage.ObjectInfo, error) {
	_, _ = io.Copy(io.Discard, body)
	s.putKeys = append(s.putKeys, key)
	return storage.ObjectInfo{Key: key, Size: size, ETag: "etag"}, nil
}

func (s *stubStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubStore) Stat(context.Context, string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, fmt.Errorf("not implemented")
}

func (s *stubStore) Delete(context.Context, string) error {
	return nil
}
