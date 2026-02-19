package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/bus"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/observability"
)

type ingestRequest struct {
	Records             []ingestRecord `json:"records"`
	WaitForVisibility   bool           `json:"wait_for_visibility"`
	VisibilityTimeoutMs int            `json:"visibility_timeout_ms"`
}

type ingestRecord struct {
	IdempotencyKey string         `json:"idempotency_key"`
	Op             string         `json:"op"`
	Payload        map[string]any `json:"payload"`
	EventTime      *time.Time     `json:"event_time"`
}

type ingestResponse struct {
	AcceptedCount      int    `json:"accepted_count"`
	DuplicateCount     int    `json:"duplicate_count"`
	MaxVisibilityToken int64  `json:"max_visibility_token"`
	VisibleSnapshotID  *int64 `json:"visible_snapshot_id"`
	Status             string `json:"status"`
}

func handleIngest(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.CatalogRepo == nil || deps.IngestBus == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "INGEST_NOT_CONFIGURED", "ingest dependencies are not configured", false, nil)
		return
	}

	tableName := strings.TrimSpace(r.PathValue("table"))
	if tableName == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "TABLE_REQUIRED", "table path parameter is required", false, nil)
		return
	}

	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}

	if err := requireRole(r, "ingest_writer"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	var request ingestRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "INVALID_JSON", "invalid ingest request body", false, map[string]any{"details": err.Error()})
		return
	}
	if len(request.Records) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "RECORDS_REQUIRED", "at least one record is required", false, nil)
		return
	}

	tableDef, err := deps.CatalogRepo.GetTableByName(r.Context(), tenantID, tableName)
	if err != nil {
		if errors.Is(err, catalog.ErrNotFound) {
			writeError(r.Context(), w, http.StatusBadRequest, "TABLE_NOT_FOUND", "table is not registered for tenant", false, map[string]any{"table": tableName})
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "CATALOG_ERROR", "failed to resolve table", true, map[string]any{"details": err.Error()})
		return
	}

	envelopes := make([]bus.Envelope, 0, len(request.Records))
	for i, record := range request.Records {
		if strings.TrimSpace(record.IdempotencyKey) == "" {
			writeError(r.Context(), w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "idempotency_key is required", false, map[string]any{"record_index": i})
			return
		}
		switch record.Op {
		case "insert", "upsert", "delete":
		default:
			writeError(r.Context(), w, http.StatusBadRequest, "INVALID_OP", "op must be insert, upsert, or delete", false, map[string]any{"record_index": i})
			return
		}
		if record.Payload == nil {
			writeError(r.Context(), w, http.StatusBadRequest, "PAYLOAD_REQUIRED", "payload object is required", false, map[string]any{"record_index": i})
			return
		}

		payloadJSON, err := json.Marshal(record.Payload)
		if err != nil {
			writeError(r.Context(), w, http.StatusBadRequest, "INVALID_PAYLOAD", "payload must be valid JSON object", false, map[string]any{"record_index": i, "details": err.Error()})
			return
		}

		eventTimeMs := int64(0)
		if record.EventTime != nil {
			eventTimeMs = record.EventTime.UTC().UnixMilli()
		}
		envelopes = append(envelopes, bus.Envelope{
			TenantID:        tenantID,
			TableID:         strconv.FormatInt(tableDef.TableID, 10),
			IdempotencyKey:  record.IdempotencyKey,
			Op:              record.Op,
			PayloadJSON:     payloadJSON,
			EventTimeUnixMs: eventTimeMs,
		})
	}

	publishStart := time.Now()
	published, err := deps.IngestBus.Publish(r.Context(), envelopes)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "INGEST_PUBLISH_FAILED", "failed to publish ingest events", true, map[string]any{"details": err.Error()})
		return
	}

	response := ingestResponse{}
	for _, result := range published {
		if result.Inserted {
			response.AcceptedCount++
		} else {
			response.DuplicateCount++
		}
		if result.VisibilityToken > response.MaxVisibilityToken {
			response.MaxVisibilityToken = result.VisibilityToken
		}
	}
	response.Status = "accepted"
	if response.DuplicateCount > 0 {
		response.Status = "partial_duplicate"
	}

	if request.WaitForVisibility {
		if response.MaxVisibilityToken > 0 {
			token := response.MaxVisibilityToken
			snapshot, err := resolveSnapshotWithBarrier(r, deps, tenantID, &token, request.VisibilityTimeoutMs)
			if err != nil {
				handleSnapshotResolutionError(r, w, err)
				return
			}
			response.VisibleSnapshotID = &snapshot.SnapshotID
			if response.AcceptedCount > 0 {
				response.Status = "visible"
			}
		}
	}
	observability.ObserveIngestAck(response.AcceptedCount, response.DuplicateCount, time.Since(publishStart))

	writeJSON(w, http.StatusOK, response)
}

func tenantFromRequest(r *http.Request) (string, error) {
	if identity, ok := auth.IdentityFromContext(r.Context()); ok {
		if strings.TrimSpace(identity.TenantID) != "" {
			return identity.TenantID, nil
		}
	}
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
	if tenantID == "" {
		return "", fmt.Errorf("tenant context is required")
	}
	return tenantID, nil
}

func requireRole(r *http.Request, role string) error {
	identity, ok := auth.IdentityFromContext(r.Context())
	if !ok {
		return nil
	}
	if identity.HasRole(role) {
		return nil
	}
	return fmt.Errorf("missing required role %q", role)
}
