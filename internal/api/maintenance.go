package api

import (
	"net/http"
)

func handleCompactionRun(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.Maintenance == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "MAINTENANCE_NOT_CONFIGURED", "maintenance service is not configured", false, nil)
		return
	}

	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireRole(r, "ops_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	summary, err := deps.Maintenance.RunCompactionOnce(r.Context(), tenantID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "COMPACTION_FAILED", "compaction run failed", true, map[string]any{
			"details": err.Error(),
			"summary": summary,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "completed",
		"summary": summary,
	})
}

func handleRetentionRun(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.Maintenance == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "MAINTENANCE_NOT_CONFIGURED", "maintenance service is not configured", false, nil)
		return
	}

	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireRole(r, "ops_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	summary, err := deps.Maintenance.RunRetentionOnce(r.Context(), tenantID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "RETENTION_FAILED", "retention run failed", true, map[string]any{
			"details": err.Error(),
			"summary": summary,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "completed",
		"summary": summary,
	})
}

func handleIntegrityRun(deps Dependencies, w http.ResponseWriter, r *http.Request) {
	if deps.Maintenance == nil {
		writeError(r.Context(), w, http.StatusNotImplemented, "MAINTENANCE_NOT_CONFIGURED", "maintenance service is not configured", false, nil)
		return
	}

	tenantID, err := tenantFromRequest(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusUnauthorized, "TENANT_REQUIRED", err.Error(), false, nil)
		return
	}
	if err := requireRole(r, "ops_admin"); err != nil {
		writeError(r.Context(), w, http.StatusForbidden, "FORBIDDEN", err.Error(), false, nil)
		return
	}

	summary, err := deps.Maintenance.RunIntegrityCheckOnce(r.Context(), tenantID)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "INTEGRITY_CHECK_FAILED", "integrity check failed", true, map[string]any{
			"details": err.Error(),
			"summary": summary,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "completed",
		"summary": summary,
	})
}
