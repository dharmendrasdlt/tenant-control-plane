// Package domain holds the Tenant/Task entities and their state machines.
// It has no I/O dependencies so the transition rules can be unit tested in isolation.
package domain

import "time"

type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

func (s TaskStatus) Terminal() bool {
	return s == TaskDone || s == TaskFailed
}

type Tenant struct {
	ID        string
	Slug      string
	Name      string
	Status    TenantStatus
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Task struct {
	ID        string
	Type      TaskType
	TenantID  string
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// CanUpdate reports whether a tenant in the given status may accept a PATCH.
func CanUpdate(s TenantStatus) bool { return s == TenantActive }

// CanDelete reports whether a tenant in the given status may accept a DELETE.
func CanDelete(s TenantStatus) bool { return s == TenantActive || s == TenantFailed }

// AllowedTaskPredecessors returns the task statuses from which `next` may be
// reached. An update whose current status is not in this set is a duplicate
// or an out-of-order/stale delivery and must be dropped, not applied.
func AllowedTaskPredecessors(next TaskStatus) []TaskStatus {
	switch next {
	case TaskInProgress:
		return []TaskStatus{TaskAccepted}
	case TaskDone, TaskFailed:
		return []TaskStatus{TaskAccepted, TaskInProgress}
	default:
		return nil
	}
}

// TenantStatusOnTaskTerminal maps a task's terminal outcome to the tenant
// status it drives. Only called once the task transition itself has been
// accepted (i.e. the task actually moved into a terminal state just now).
func TenantStatusOnTaskTerminal(t TaskType, outcome TaskStatus) TenantStatus {
	if outcome == TaskFailed {
		return TenantFailed
	}
	switch t {
	case TaskDeploy, TaskUpdate:
		return TenantActive
	case TaskDestroy:
		return TenantDestroyed
	default:
		return TenantFailed
	}
}
