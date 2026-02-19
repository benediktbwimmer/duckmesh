package bus

import "context"

type EventState string

const (
	StateAccepted  EventState = "accepted"
	StateClaimed   EventState = "claimed"
	StateCommitted EventState = "committed"
	StateFailed    EventState = "failed"
)

type Envelope struct {
	EventID         string
	TenantID        string
	TableID         string
	IdempotencyKey  string
	Op              string
	PayloadJSON     []byte
	EventTimeUnixMs int64
}

type Batch struct {
	BatchID     string
	ConsumerID  string
	LeaseUntil  int64
	Visibility  int64
	EventIDs    []string
	Envelopes   []Envelope
	LeaseToken  string
	Attempt     int
	ClaimedUnix int64
}

type PublishResult struct {
	EventID         string
	VisibilityToken int64
	Inserted        bool
}

type IngestBus interface {
	Publish(ctx context.Context, events []Envelope) ([]PublishResult, error)
	ClaimBatch(ctx context.Context, consumerID string, limit int, leaseSeconds int) (Batch, error)
	Ack(ctx context.Context, batchID string, eventIDs []string) error
	Nack(ctx context.Context, batchID string, eventIDs []string, reason string) error
	ExtendLease(ctx context.Context, batchID string, leaseSeconds int) error
	RequeueExpired(ctx context.Context) (int, error)
}
