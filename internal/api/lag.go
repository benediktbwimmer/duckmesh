package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/observability"
)

type ingestLagReader interface {
	GetIngestLagStats(ctx context.Context, tenantID string) (catalog.IngestLagStats, error)
}

func handleLag(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireRole(r, "ops_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	lagRepo, ok := deps.CatalogRepo.(ingestLagReader)
	if !ok {
		writeError(r.Context(), w, http.StatusNotImplemented, "LAG_NOT_CONFIGURED", "lag view is not configured", false, nil)
		return
	}

	stats, err := lagRepo.GetIngestLagStats(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(r.Context(), w, http.StatusNotFound, "TENANT_NOT_FOUND", "tenant not found", false, nil)
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to read lag statistics", true, map[string]any{"details": err.Error()})
		return
	}

	pending := stats.AcceptedEvents + stats.ClaimedEvents
	oldestPendingLagMs := int64(0)
	if stats.OldestPendingIngestAt != nil {
		oldestPendingLagMs = time.Since(stats.OldestPendingIngestAt.UTC()).Milliseconds()
		if oldestPendingLagMs < 0 {
			oldestPendingLagMs = 0
		}
	}
	tokenLag := stats.MaxPendingToken - stats.LatestVisibilityToken
	if tokenLag < 0 {
		tokenLag = 0
	}

	observability.SetLagMetrics(pending, oldestPendingLagMs, stats.LatestVisibilityToken)

	writeJSON(w, http.StatusOK, map[string]any{
		"tenant_id":               tenantID,
		"accepted_events":         stats.AcceptedEvents,
		"claimed_events":          stats.ClaimedEvents,
		"pending_events":          pending,
		"oldest_pending_lag_ms":   oldestPendingLagMs,
		"token_lag":               tokenLag,
		"latest_visibility_token": stats.LatestVisibilityToken,
		"latest_snapshot_id":      stats.LatestSnapshotID,
		"latest_snapshot_time":    stats.LatestSnapshotAt,
	})
}
