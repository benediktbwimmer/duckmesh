package producer

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type IngestRecord struct {
	IdempotencyKey string         `json:"idempotency_key"`
	Op             string         `json:"op"`
	Payload        map[string]any `json:"payload"`
	EventTime      time.Time      `json:"event_time"`
}

type Generator struct {
	rnd             *rand.Rand
	producerID      string
	userCardinality int
	sequence        int64
	now             func() time.Time
}

func NewGenerator(seed int64, producerID string, userCardinality int) *Generator {
	return &Generator{
		rnd:             rand.New(rand.NewSource(seed)),
		producerID:      producerID,
		userCardinality: userCardinality,
		now:             func() time.Time { return time.Now().UTC() },
	}
}

func (g *Generator) NextRecord() IngestRecord {
	g.sequence++
	eventAt := g.now()
	eventType := g.pickEventType()
	amount := g.pickAmount(eventType)
	sequence := g.sequence

	return IngestRecord{
		IdempotencyKey: fmt.Sprintf("%s-%020d", g.producerID, sequence),
		Op:             "insert",
		EventTime:      eventAt,
		Payload: map[string]any{
			"event_id":    sequence,
			"user_id":     fmt.Sprintf("user-%04d", g.rnd.Intn(g.userCardinality)+1),
			"session_id":  fmt.Sprintf("sess-%08x", g.rnd.Uint32()),
			"event_type":  eventType,
			"amount":      amount,
			"currency":    "USD",
			"country":     pickOne(g.rnd, []string{"US", "DE", "GB", "IN", "JP", "BR"}),
			"device":      pickOne(g.rnd, []string{"desktop", "mobile", "tablet"}),
			"source":      "duckmesh-demo-producer",
			"occurred_at": eventAt.Format(time.RFC3339Nano),
		},
	}
}

func (g *Generator) pickEventType() string {
	p := g.rnd.Intn(100)
	switch {
	case p < 55:
		return "page_view"
	case p < 75:
		return "search"
	case p < 88:
		return "add_to_cart"
	case p < 97:
		return "checkout"
	default:
		return "purchase"
	}
}

func (g *Generator) pickAmount(eventType string) float64 {
	switch eventType {
	case "purchase":
		return round2(20 + g.rnd.Float64()*280)
	case "checkout":
		return round2(15 + g.rnd.Float64()*240)
	case "add_to_cart":
		return round2(5 + g.rnd.Float64()*120)
	default:
		return 0
	}
}

func round2(value float64) float64 {
	return math.Round(value*100) / 100
}

func pickOne(r *rand.Rand, values []string) string {
	return values[r.Intn(len(values))]
}
