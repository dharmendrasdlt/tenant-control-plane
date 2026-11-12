package domain

import (
	"reflect"
	"testing"
)

func TestCanUpdate(t *testing.T) {
	cases := map[TenantStatus]bool{
		TenantActive:       true,
		TenantProvisioning: false,
		TenantUpdating:     false,
		TenantDestroying:   false,
		TenantDestroyed:    false,
		TenantFailed:       false,
	}
	for status, want := range cases {
		if got := CanUpdate(status); got != want {
			t.Errorf("CanUpdate(%s) = %v, want %v", status, got, want)
		}
	}
}

func TestCanDelete(t *testing.T) {
	cases := map[TenantStatus]bool{
		TenantActive:       true,
		TenantFailed:       true,
		TenantProvisioning: false,
		TenantUpdating:     false,
		TenantDestroying:   false,
		TenantDestroyed:    false,
	}
	for status, want := range cases {
		if got := CanDelete(status); got != want {
			t.Errorf("CanDelete(%s) = %v, want %v", status, got, want)
		}
	}
}

func TestAllowedTaskPredecessors(t *testing.T) {
	cases := []struct {
		next TaskStatus
		want []TaskStatus
	}{
		{TaskInProgress, []TaskStatus{TaskAccepted}},
		{TaskDone, []TaskStatus{TaskAccepted, TaskInProgress}},
		{TaskFailed, []TaskStatus{TaskAccepted, TaskInProgress}},
		{TaskAccepted, nil},
	}
	for _, c := range cases {
		got := AllowedTaskPredecessors(c.next)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("AllowedTaskPredecessors(%s) = %v, want %v", c.next, got, c.want)
		}
	}
}

// Regression guard: a stale "in_progress" arriving after "done" must never be
// re-applicable. The predecessor set for in_progress is {accepted} only, so a
// task already in "done" is never in that set.
func TestStaleInProgressAfterDoneIsRejected(t *testing.T) {
	preds := AllowedTaskPredecessors(TaskInProgress)
	for _, p := range preds {
		if p == TaskDone {
			t.Fatalf("done must never be a valid predecessor of in_progress")
		}
	}
}

func TestTenantStatusOnTaskTerminal(t *testing.T) {
	cases := []struct {
		typ     TaskType
		outcome TaskStatus
		want    TenantStatus
	}{
		{TaskDeploy, TaskDone, TenantActive},
		{TaskUpdate, TaskDone, TenantActive},
		{TaskDestroy, TaskDone, TenantDestroyed},
		{TaskDeploy, TaskFailed, TenantFailed},
		{TaskUpdate, TaskFailed, TenantFailed},
		{TaskDestroy, TaskFailed, TenantFailed},
	}
	for _, c := range cases {
		if got := TenantStatusOnTaskTerminal(c.typ, c.outcome); got != c.want {
			t.Errorf("TenantStatusOnTaskTerminal(%s, %s) = %s, want %s", c.typ, c.outcome, got, c.want)
		}
	}
}
