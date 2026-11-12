package api

import (
	"time"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/domain"
)

const timeFmt = "2006-01-02T15:04:05.000Z"

func fmtTime(t time.Time) string { return t.UTC().Format(timeFmt) }

type tenantDTO struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int64  `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func toTenantDTO(t domain.Tenant) tenantDTO {
	return tenantDTO{
		ID:        t.ID,
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    string(t.Status),
		Version:   t.Version,
		CreatedAt: fmtTime(t.CreatedAt),
		UpdatedAt: fmtTime(t.UpdatedAt),
	}
}

type taskDTO struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func toTaskDTO(t domain.Task) taskDTO {
	return taskDTO{
		ID:        t.ID,
		Type:      string(t.Type),
		TenantID:  t.TenantID,
		Status:    string(t.Status),
		CreatedAt: fmtTime(t.CreatedAt),
		UpdatedAt: fmtTime(t.UpdatedAt),
	}
}

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type updateTenantRequest struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
