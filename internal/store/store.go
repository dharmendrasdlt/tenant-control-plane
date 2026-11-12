// Package store is the Postgres persistence layer. It is the only place that
// knows SQL; everything else talks to domain.Tenant / domain.Task.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dharmendrasdlt/tenant-control-plane/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// CreateTenantWithTask inserts a new tenant (status=provisioning) and its
// deploy task in a single transaction. A slug collision is translated to
// domain.ErrTenantExists; this is also the tie-breaker for concurrent
// creates with the same slug, since the UNIQUE constraint is enforced by
// Postgres itself, not by an app-level check-then-insert race.
func (s *Store) CreateTenantWithTask(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO tenants (slug, name, status, version)
			VALUES ($1, $2, $3, 1)
			RETURNING id, slug, name, status, version, created_at, updated_at`,
			slug, name, domain.TenantProvisioning)
		if err := scanTenant(row, &tenant); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return domain.ErrTenantExists
			}
			return err
		}

		row = tx.QueryRow(ctx, `
			INSERT INTO tasks (type, tenant_id, status)
			VALUES ($1, $2, $3)
			RETURNING id, type, tenant_id, status, created_at, updated_at`,
			domain.TaskDeploy, tenant.ID, domain.TaskAccepted)
		return scanTask(row, &task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

func (s *Store) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1`, id)
	var t domain.Tenant
	if err := scanTenant(row, &t); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Tenant{}, domain.ErrTenantNotFound
		}
		return domain.Tenant{}, err
	}
	return t, nil
}

func (s *Store) ListTenants(ctx context.Context, limit, offset int) ([]domain.Tenant, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants ORDER BY created_at ASC LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Tenant
	for rows.Next() {
		var t domain.Tenant
		if err := scanTenant(rows, &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTenantNameGuarded renames a tenant, enforcing both the state-machine
// guard (must be active) and optimistic locking (expectedVersion must match).
// It also creates the `update` task. All in one transaction so the task is
// only ever created alongside the version bump that justified it.
func (s *Store) UpdateTenantNameGuarded(ctx context.Context, id, name string, expectedVersion int64) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := txGetTenant(ctx, tx, id)
		if err != nil {
			return err
		}
		if !domain.CanUpdate(cur.Status) {
			return domain.ErrUpdateNotAllowed
		}
		if cur.Version != expectedVersion {
			return domain.ErrVersionConflict
		}

		row := tx.QueryRow(ctx, `
			UPDATE tenants SET name = $1, status = $2, version = version + 1, updated_at = now()
			WHERE id = $3 AND version = $4
			RETURNING id, slug, name, status, version, created_at, updated_at`,
			name, domain.TenantUpdating, id, expectedVersion)
		if err := scanTenant(row, &tenant); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrVersionConflict // lost the race between the read above and this write
			}
			return err
		}

		row = tx.QueryRow(ctx, `
			INSERT INTO tasks (type, tenant_id, status)
			VALUES ($1, $2, $3)
			RETURNING id, type, tenant_id, status, created_at, updated_at`,
			domain.TaskUpdate, tenant.ID, domain.TaskAccepted)
		return scanTask(row, &task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

// DeleteTenantGuarded moves a tenant into `destroying` and creates the
// destroy task, enforcing the state-machine guard (active or failed).
func (s *Store) DeleteTenantGuarded(ctx context.Context, id string) (domain.Tenant, domain.Task, error) {
	var tenant domain.Tenant
	var task domain.Task

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		cur, err := txGetTenant(ctx, tx, id)
		if err != nil {
			return err
		}
		if !domain.CanDelete(cur.Status) {
			return domain.ErrUpdateNotAllowed
		}

		row := tx.QueryRow(ctx, `
			UPDATE tenants SET status = $1, version = version + 1, updated_at = now()
			WHERE id = $2 AND version = $3
			RETURNING id, slug, name, status, version, created_at, updated_at`,
			domain.TenantDestroying, id, cur.Version)
		if err := scanTenant(row, &tenant); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrVersionConflict
			}
			return err
		}

		row = tx.QueryRow(ctx, `
			INSERT INTO tasks (type, tenant_id, status)
			VALUES ($1, $2, $3)
			RETURNING id, type, tenant_id, status, created_at, updated_at`,
			domain.TaskDestroy, tenant.ID, domain.TaskAccepted)
		return scanTask(row, &task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

func (s *Store) GetTask(ctx context.Context, id string) (domain.Task, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, tenant_id, status, created_at, updated_at
		FROM tasks WHERE id = $1`, id)
	var t domain.Task
	if err := scanTask(row, &t); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Task{}, domain.ErrTaskNotFound
		}
		return domain.Task{}, err
	}
	return t, nil
}

func (s *Store) ListTasks(ctx context.Context, tenantID string, limit, offset int) ([]domain.Task, error) {
	var rows pgx.Rows
	var err error
	if tenantID != "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, type, tenant_id, status, created_at, updated_at
			FROM tasks WHERE tenant_id = $1 ORDER BY created_at ASC LIMIT $2 OFFSET $3`,
			tenantID, limit, offset)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, type, tenant_id, status, created_at, updated_at
			FROM tasks ORDER BY created_at ASC LIMIT $1 OFFSET $2`, limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []domain.Task
	for rows.Next() {
		var t domain.Task
		if err := scanTask(rows, &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ApplyTaskUpdate is the inbound-messaging write path. It is a compare-and-
// swap: the UPDATE only matches rows whose current status is a valid
// predecessor of `next`. Zero rows affected means the update is a duplicate
// or stale/out-of-order delivery, which is not an error — the caller should
// ack the message and move on. On success it also folds the terminal outcome
// into the tenant in the same transaction.
func (s *Store) ApplyTaskUpdate(ctx context.Context, taskID string, next domain.TaskStatus) (applied bool, err error) {
	preds := domain.AllowedTaskPredecessors(next)
	if len(preds) == 0 {
		return false, fmt.Errorf("invalid target task status %q", next)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tasks SET status = $1, updated_at = now()
			WHERE id = $2 AND status = ANY($3)
			RETURNING tenant_id, type`,
			next, taskID, predStrings(preds))
		var tenantID string
		var taskType domain.TaskType
		if err := row.Scan(&tenantID, &taskType); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				applied = false
				return nil // duplicate / stale — not an error
			}
			return err
		}
		applied = true

		if !next.Terminal() {
			return nil
		}
		tenantStatus := domain.TenantStatusOnTaskTerminal(taskType, next)
		_, err := tx.Exec(ctx, `
			UPDATE tenants SET status = $1, version = version + 1, updated_at = now()
			WHERE id = $2`, tenantStatus, tenantID)
		return err
	})
	return applied, err
}

func predStrings(ss []domain.TaskStatus) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

type row interface {
	Scan(dest ...any) error
}

func scanTenant(r row, t *domain.Tenant) error {
	return r.Scan(&t.ID, &t.Slug, &t.Name, &t.Status, &t.Version, &t.CreatedAt, &t.UpdatedAt)
}

func scanTask(r row, t *domain.Task) error {
	return r.Scan(&t.ID, &t.Type, &t.TenantID, &t.Status, &t.CreatedAt, &t.UpdatedAt)
}

func txGetTenant(ctx context.Context, tx pgx.Tx, id string) (domain.Tenant, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, slug, name, status, version, created_at, updated_at
		FROM tenants WHERE id = $1`, id)
	var t domain.Tenant
	if err := scanTenant(row, &t); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Tenant{}, domain.ErrTenantNotFound
		}
		return domain.Tenant{}, err
	}
	return t, nil
}
