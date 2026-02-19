package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/duckmesh/duckmesh/internal/bus"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/storage"
)

type Service struct {
	Bus         bus.IngestBus
	Publisher   Publisher
	ObjectStore storage.ObjectStore
	Config      Config
	Logger      *slog.Logger
	Clock       func() time.Time
}

type Publisher interface {
	AllocateSnapshotID(ctx context.Context) (int64, error)
	PublishBatch(ctx context.Context, in catalogpostgres.PublishBatchInput) (catalogpostgres.PublishBatchResult, error)
}

type Config struct {
	ConsumerID   string
	ClaimLimit   int
	LeaseSeconds int
	PollInterval time.Duration
	CreatedBy    string
}

func (s *Service) Run(ctx context.Context) error {
	s.ensureDefaults()

	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()

	for {
		if err := s.ProcessOnce(ctx); err != nil {
			if s.Logger != nil {
				s.Logger.ErrorContext(ctx, "coordinator process cycle failed", slog.Any("error", err))
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) ProcessOnce(ctx context.Context) error {
	s.ensureDefaults()
	batch, err := s.Bus.ClaimBatch(ctx, s.Config.ConsumerID, s.Config.ClaimLimit, s.Config.LeaseSeconds)
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}
	if len(batch.Envelopes) == 0 {
		return nil
	}

	groups := groupEventsByTenantAndTable(batch.Envelopes)
	for _, group := range groups {
		if err := s.processGroup(ctx, batch.BatchID, group); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) ensureDefaults() {
	if s.Clock == nil {
		s.Clock = time.Now
	}
	if s.Config.ClaimLimit <= 0 {
		s.Config.ClaimLimit = 500
	}
	if s.Config.LeaseSeconds <= 0 {
		s.Config.LeaseSeconds = 30
	}
	if s.Config.PollInterval <= 0 {
		s.Config.PollInterval = 300 * time.Millisecond
	}
	if s.Config.ConsumerID == "" {
		s.Config.ConsumerID = "duckmesh-coordinator"
	}
	if s.Config.CreatedBy == "" {
		s.Config.CreatedBy = "duckmesh-coordinator"
	}
}

func (s *Service) processGroup(ctx context.Context, batchID string, group groupedEvents) error {
	snapshotID, err := s.Publisher.AllocateSnapshotID(ctx)
	if err != nil {
		return fmt.Errorf("allocate snapshot id: %w", err)
	}

	encoded, err := EncodeEventsToParquet(group.Events)
	if err != nil {
		return fmt.Errorf("encode events to parquet: %w", err)
	}

	sequence := int(snapshotID % 100000)
	dataFilePath, err := storage.BuildDataFilePath(group.TenantID, "table-"+strconv.FormatInt(group.TableID, 10), s.Clock(), snapshotID, sequence)
	if err != nil {
		return fmt.Errorf("build data file path: %w", err)
	}

	putInfo, err := s.ObjectStore.Put(ctx, dataFilePath, bytesReader(encoded.Data), int64(len(encoded.Data)), storage.PutOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("put parquet object: %w", err)
	}

	statsJSON, err := json.Marshal(map[string]any{
		"event_count": len(group.Events),
	})
	if err != nil {
		return fmt.Errorf("marshal data file stats: %w", err)
	}

	if _, err := s.Publisher.PublishBatch(ctx, catalogpostgres.PublishBatchInput{
		SnapshotID:         snapshotID,
		TenantID:           group.TenantID,
		TableID:            group.TableID,
		BatchID:            batchID,
		EventIDs:           group.EventIDs,
		CreatedBy:          s.Config.CreatedBy,
		MaxVisibilityToken: group.MaxVisibilityToken,
		DataFilePath:       dataFilePath,
		RecordCount:        encoded.RecordCount,
		FileSizeBytes:      putInfo.Size,
		MinEventTime:       encoded.MinEventTime,
		MaxEventTime:       encoded.MaxEventTime,
		StatsJSON:          statsJSON,
	}); err != nil {
		return fmt.Errorf("publish snapshot batch: %w", err)
	}

	if err := s.Bus.Ack(ctx, batchID, group.EventIDs); err != nil {
		return fmt.Errorf("ack claimed events: %w", err)
	}

	if s.Logger != nil {
		s.Logger.InfoContext(ctx, "coordinator published batch",
			slog.String("tenant_id", group.TenantID),
			slog.Int64("table_id", group.TableID),
			slog.Int64("snapshot_id", snapshotID),
			slog.Int64("max_visibility_token", group.MaxVisibilityToken),
			slog.Int("event_count", len(group.Events)),
			slog.String("object_path", dataFilePath),
		)
	}

	return nil
}

type groupedEvents struct {
	TenantID           string
	TableID            int64
	Events             []bus.Envelope
	EventIDs           []string
	MaxVisibilityToken int64
}

func groupEventsByTenantAndTable(events []bus.Envelope) []groupedEvents {
	type key struct {
		tenantID string
		tableID  int64
	}

	lookup := map[key]*groupedEvents{}
	order := make([]key, 0)

	for _, event := range events {
		tableID, err := strconv.ParseInt(event.TableID, 10, 64)
		if err != nil {
			continue
		}
		k := key{tenantID: event.TenantID, tableID: tableID}
		group, ok := lookup[k]
		if !ok {
			group = &groupedEvents{TenantID: event.TenantID, TableID: tableID}
			lookup[k] = group
			order = append(order, k)
		}
		group.Events = append(group.Events, event)
		group.EventIDs = append(group.EventIDs, event.EventID)
		eventID, err := strconv.ParseInt(event.EventID, 10, 64)
		if err == nil && eventID > group.MaxVisibilityToken {
			group.MaxVisibilityToken = eventID
		}
	}

	result := make([]groupedEvents, 0, len(order))
	for _, k := range order {
		result = append(result, *lookup[k])
	}
	return result
}

type staticReader struct {
	data   []byte
	offset int
}

func bytesReader(data []byte) io.Reader {
	return &staticReader{data: data}
}

func (r *staticReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}
