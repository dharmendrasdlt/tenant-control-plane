// Package service orders the two things the brief calls out explicitly:
// commit first, publish second, never the reverse, and never publish on a
// rolled-back write. The store calls below only return once their
// transaction has committed, so sequencing the publish after them in plain
// Go code is sufficient — no outbox table needed at this scope.
//
// Known trade-off (documented in the README): if the publish itself fails
// after a successful commit, we retry a few times with backoff and then log
// and move on rather than build a transactional outbox/relay. That's an
// accepted at-least-once gap for a 2-day scope, not an oversight.
package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/broker"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/domain"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/messages"
	"github.com/dharmendrasdlt/tenant-control-plane/internal/store"
)

type Service struct {
	Store  *store.Store
	Broker *broker.Broker
	Log    *slog.Logger
}

func New(st *store.Store, br *broker.Broker, log *slog.Logger) *Service {
	return &Service{Store: st, Broker: br, Log: log}
}

func (s *Service) CreateTenant(ctx context.Context, slug, name string) (domain.Tenant, error) {
	tenant, task, err := s.Store.CreateTenantWithTask(ctx, slug, name)
	if err != nil {
		return domain.Tenant{}, err
	}
	s.publishTaskCreated(ctx, task)
	return tenant, nil
}

func (s *Service) UpdateTenant(ctx context.Context, id, name string, version int64) (domain.Tenant, error) {
	tenant, task, err := s.Store.UpdateTenantNameGuarded(ctx, id, name, version)
	if err != nil {
		return domain.Tenant{}, err
	}
	s.publishTaskCreated(ctx, task)
	return tenant, nil
}

func (s *Service) DeleteTenant(ctx context.Context, id string) (domain.Tenant, error) {
	tenant, task, err := s.Store.DeleteTenantGuarded(ctx, id)
	if err != nil {
		return domain.Tenant{}, err
	}
	s.publishTaskCreated(ctx, task)
	return tenant, nil
}

func (s *Service) publishTaskCreated(ctx context.Context, task domain.Task) {
	evt := messages.TaskCreated{
		ID:        task.ID,
		Type:      string(task.Type),
		TenantID:  task.TenantID,
		Status:    string(task.Status),
		CreatedAt: task.CreatedAt,
	}
	var err error
	for attempt, backoff := 0, 100*time.Millisecond; attempt < 3; attempt, backoff = attempt+1, backoff*2 {
		if err = s.Broker.PublishJSON(ctx, messages.SubjectTaskCreated, evt); err == nil {
			return
		}
		time.Sleep(backoff)
	}
	s.Log.Error("publish task.created failed after retries; task is durably committed but the worker will not see it until reconciled",
		"task_id", task.ID, "error", err)
}

// ApplyWorkerUpdate is the inbound side: it drives the compare-and-swap in
// the store and reports whether the update actually changed anything
// (false means duplicate/stale — not an error, just a no-op).
func (s *Service) ApplyWorkerUpdate(ctx context.Context, upd messages.TaskProgress) (applied bool, err error) {
	return s.Store.ApplyTaskUpdate(ctx, upd.TaskID, domain.TaskStatus(upd.Status))
}

// NewUpdateID is a convenience for the worker side of the module (kept here
// so both binaries agree on the format).
func NewUpdateID() string { return uuid.NewString() }
