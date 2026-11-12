package api

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/service"
)

var slugRE = regexp.MustCompile(`^[a-z][-a-z0-9]{1,26}[a-z0-9]$`)

type Handlers struct {
	svc *service.Service
}

func NewRouter(svc *service.Service) http.Handler {
	h := &Handlers{svc: svc}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.health)

	mux.HandleFunc("POST /tenants", h.createTenant)
	mux.HandleFunc("GET /tenants", h.listTenants)
	mux.HandleFunc("GET /tenants/{id}", h.getTenant)
	mux.HandleFunc("PATCH /tenants/{id}", h.updateTenant)
	mux.HandleFunc("DELETE /tenants/{id}", h.deleteTenant)

	mux.HandleFunc("GET /tasks", h.listTasks)
	mux.HandleFunc("GET /tasks/{id}", h.getTask)

	return mux
}

func (h *Handlers) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeValidationError(w, "invalid JSON body")
		return
	}
	if !slugRE.MatchString(req.Slug) {
		writeValidationError(w, "slug must match ^[a-z][-a-z0-9]{1,26}[a-z0-9]$ (3-28 chars)")
		return
	}
	if req.Name == "" {
		writeValidationError(w, "name must not be empty")
		return
	}

	tenant, err := h.svc.CreateTenant(r.Context(), req.Slug, req.Name)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toTenantDTO(tenant))
}

func (h *Handlers) listTenants(w http.ResponseWriter, r *http.Request) {
	limit, offset := pagination(r)
	tenants, err := h.svc.Store.ListTenants(r.Context(), limit, offset)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]tenantDTO, len(tenants))
	for i, t := range tenants {
		out[i] = toTenantDTO(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "limit": limit, "offset": offset})
}

func (h *Handlers) getTenant(w http.ResponseWriter, r *http.Request) {
	tenant, err := h.svc.Store.GetTenant(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTenantDTO(tenant))
}

func (h *Handlers) updateTenant(w http.ResponseWriter, r *http.Request) {
	var req updateTenantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeValidationError(w, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeValidationError(w, "name must not be empty")
		return
	}
	if req.Version <= 0 {
		writeValidationError(w, "version is required and must be the tenant's current version")
		return
	}

	tenant, err := h.svc.UpdateTenant(r.Context(), r.PathValue("id"), req.Name, req.Version)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTenantDTO(tenant))
}

func (h *Handlers) deleteTenant(w http.ResponseWriter, r *http.Request) {
	tenant, err := h.svc.DeleteTenant(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toTenantDTO(tenant))
}

func (h *Handlers) listTasks(w http.ResponseWriter, r *http.Request) {
	limit, offset := pagination(r)
	tasks, err := h.svc.Store.ListTasks(r.Context(), r.URL.Query().Get("tenant_id"), limit, offset)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]taskDTO, len(tasks))
	for i, t := range tasks {
		out[i] = toTaskDTO(t)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "limit": limit, "offset": offset})
}

func (h *Handlers) getTask(w http.ResponseWriter, r *http.Request) {
	task, err := h.svc.Store.GetTask(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTaskDTO(task))
}

func pagination(r *http.Request) (limit, offset int) {
	limit, offset = 20, 0
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 && v <= 100 {
		limit = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil && v >= 0 {
		offset = v
	}
	return
}
