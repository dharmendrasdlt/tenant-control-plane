package domain

// Error is a structured, machine-readable domain error. The Code is part of
// the API contract (see the brief) and must not change once published.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

var (
	ErrTenantNotFound   = &Error{Code: "tenant_not_found", Message: "tenant not found"}
	ErrTenantExists     = &Error{Code: "tenant_already_exists", Message: "a tenant with this slug already exists"}
	ErrUpdateNotAllowed = &Error{Code: "tenant_update_not_allowed", Message: "tenant is not in a state that allows this operation"}
	ErrVersionConflict  = &Error{Code: "tenant_version_conflict", Message: "tenant was modified by another request; refetch and retry"}
	ErrTaskNotFound     = &Error{Code: "task_not_found", Message: "task not found"}
)
