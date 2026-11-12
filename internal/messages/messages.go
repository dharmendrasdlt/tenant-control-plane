// Package messages defines the wire schema shared by the control plane and
// the worker simulator. Topic/envelope shape is deliberately simple: the
// task row itself is the outbound event, and the inbound progress update
// carries just enough to drive the compare-and-swap in internal/store.
package messages

import "time"

const (
	SubjectTaskCreated  = "tasks.created"
	SubjectTaskProgress = "tasks.progress"
)

// TaskCreated is published by the control plane after a tenant-mutating
// write commits. It lets a consumer recover id/type/tenant_id/status
// without any further lookup.
type TaskCreated struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	TenantID  string    `json:"tenant_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskProgress is published by the worker as it drives a task through
// in_progress -> done|failed. UpdateID is a stable per-update identifier
// (kept for log correlation and as the contractual "idempotency key" the
// brief asks for); TaskID is the reference the control plane applies the
// status to. Correctness does not depend on deduplicating by UpdateID: the
// control plane applies updates via a compare-and-swap on the task's current
// status (see domain.AllowedTaskPredecessors), so a redelivered or
// out-of-order update is a no-op by construction, not by lookup.
type TaskProgress struct {
	UpdateID   string    `json:"update_id"`
	TaskID     string    `json:"task_id"`
	Status     string    `json:"status"`
	OccurredAt time.Time `json:"occurred_at"`
}
