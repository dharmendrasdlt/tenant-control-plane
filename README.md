# Tenant Provisioning Control Plane

A minimal, correctness-first implementation of the take-home brief: a
control plane that provisions multi-tenant SaaS environments asynchronously,
plus a worker simulator that drives provisioning to completion over a
message broker.

## Quickstart

```bash
git clone <this-repo> && cd tenant-control-plane
make up      # builds & boots postgres, nats, the control plane, and the worker — one command
make check   # fmt check, vet, lint (incl. security scan), vuln scan, unit tests
```

After `make up`, this walks the full lifecycle with no manual steps:

```bash
curl -s -X POST localhost:8080/tenants \
  -H 'Content-Type: application/json' \
  -d '{"slug":"acme","name":"Acme Corp"}' | python3 -m json.tool
# -> {"status": "provisioning", ...}

sleep 1
curl -s localhost:8080/tenants/<id> | python3 -m json.tool
# -> {"status": "active", ...}   (the worker drove accepted -> in_progress -> done)
```

Tear down with `make down`.

## Architecture

```
            HTTP (CRUD tenants, read-only tasks)
   client ────────────────▶ control-plane ──┐
                                 │           │ tasks.created
                                 │           ▼
                             postgres      NATS JetStream
                           (tenants,         │  tasks.progress
                            tasks)           ▼
                                 ▲         worker (simulator)
                                 └─────────────┘
                     control-plane applies progress via CAS
```

- **control-plane** (`cmd/controlplane`): one process running both the HTTP
  API and the inbound JetStream consumer that applies worker progress.
- **worker** (`cmd/worker`): a separate process, no HTTP API, that consumes
  `tasks.created`, sleeps a random delay, and publishes `in_progress` then
  `done`/`failed` onto `tasks.progress`. Behavior is controlled entirely by
  flags/env vars (`--fail-rate`, `--min-delay-ms`, `--max-delay-ms`), so
  negative scenarios are just a matter of restarting it differently.
- **postgres**: tenant/task persistence. `version` column backs optimistic
  locking.
- **NATS JetStream**: the broker, in both directions. Chosen over
  RabbitMQ/Redis Streams because retry-with-backoff and poison-message
  handling ("stop redelivering after N attempts") are consumer *config*
  (`AckWait`, `MaxDeliver`, `BackOff`), not code I have to write and test —
  the single biggest lever for keeping this a 2-day build instead of a
  month-long one.

## Design decisions & trade-offs

**Idempotency without a dedupe table.** Each progress update carries an
`update_id` (the brief's "stable identifier... idempotency key") and a
`task_id`. Rather than storing seen `update_id`s and checking them on every
write, the control plane applies updates via a SQL compare-and-swap:

```sql
UPDATE tasks SET status = $next, updated_at = now()
WHERE id = $task_id AND status = ANY($allowed_predecessors)
```

`allowed_predecessors` is computed per target status (see
`internal/domain.AllowedTaskPredecessors`): only `accepted` can become
`in_progress`; only `accepted`/`in_progress` can become `done`/`failed`.
Zero rows affected means the update is a duplicate or stale/out-of-order
delivery — not an error, just a no-op, acked and dropped. This gets
idempotency *and* order-tolerance from one mechanism instead of two, and
it's correct even with two workers racing on the same task concurrently
(see "crash recovery" below). The trade-off: if a future requirement needed
to *inspect* which updates were dropped and why beyond logs, a dedupe table
would be the next step — not needed at this scope.

**Optimistic locking is enforced by the UPDATE, not by the pre-check.**
`PATCH` reads the tenant, checks the state-machine guard and version inside
the same transaction, then runs
`UPDATE tenants SET ... WHERE id = $1 AND version = $2`. Postgres takes the
row lock *before* re-evaluating the `WHERE` clause, so of two concurrent
transactions matching the same version, only the one that commits first
actually updates a row; the second sees 0 rows affected and gets
`tenant_version_conflict`. The earlier read is there for the state-machine
guard (`tenant_update_not_allowed`), not for concurrency correctness — that
guarantee comes from Postgres, not from application-level locking.

**Publish-after-commit, not a transactional outbox.** The store methods
only return after their transaction has committed; `internal/service`
publishes the resulting task event as the next line of plain Go code, so
publish can never happen before the commit or after a rollback. If the
publish itself fails after a successful commit (broker down), it's retried
a few times with backoff and then logged — the task is durably persisted
but the worker won't see it until a reconciliation sweep exists. A full
transactional outbox/relay would close that gap entirely; it's deliberately
out of scope here (the brief explicitly says "exactly-once doesn't exist,
design for at-least-once" — this is the accepted at-least-once gap, not an
oversight).

**Worker acks only after the terminal outcome is published.** If the worker
crashes mid-delay, JetStream redelivers `tasks.created` after `AckWait`
(5m, matching "provisioning can take minutes") to the same or a restarted
worker, which just re-simulates and re-publishes. Because the control
plane's CAS makes `in_progress`/`done` idempotent, two workers racing on
the same task — one slow, one redelivered — is safe, not a bug.

**At most one open task per tenant** falls out of the state-machine guards,
not a separate lock: `PATCH` only succeeds from `active`, `DELETE` only
from `active`/`failed`, and `active` is only reached once the prior task
resolved. A tenant mid-task is never in a status that accepts another
mutating call.

**Why Go stdlib `net/http`, not a framework.** Go 1.22+'s `ServeMux`
supports method + path-parameter routing (`PATCH /tenants/{id}`) natively.
A framework would add a dependency and a learning curve for the reviewer
with no behavioral benefit at this route count.

## API

All responses are JSON. Errors are structured:

```json
{"error": {"code": "tenant_version_conflict", "message": "..."}}
```

| Code | HTTP | Meaning |
|---|---|---|
| `tenant_not_found` | 404 | no tenant with that id |
| `tenant_already_exists` | 409 | slug already taken (including by a destroyed tenant) |
| `tenant_update_not_allowed` | 409 | tenant's current status doesn't allow this operation |
| `tenant_version_conflict` | 409 | `version` in the request doesn't match the tenant's current version |
| `task_not_found` | 404 | no task with that id |
| `invalid_request` | 400 | malformed body / invalid slug or name |

### Tenants

```
POST   /tenants              {"slug": "acme", "name": "Acme Corp"}   -> 201
GET    /tenants?limit=&offset=                                        -> 200 {"items": [...]}
GET    /tenants/{id}                                                  -> 200
PATCH  /tenants/{id}          {"name": "New Name", "version": 3}     -> 200 | 409
DELETE /tenants/{id}                                                  -> 202
```

### Tasks (read-only)

```
GET /tasks?tenant_id=&limit=&offset=   -> 200 {"items": [...]}
GET /tasks/{id}                        -> 200
```

## Demonstrating the hard parts

```bash
make concurrency-test   # fires two PATCHes at the same version; expect one 200, one 409
make stale-update       # publishes a stale in_progress + duplicate done straight onto NATS;
                         # requires the NATS CLI: brew install nats-io/nats-tools/nats
```

To see a deploy fail instead of succeed, restart the worker with a non-zero
fail rate:

```bash
docker compose run --rm -e FAIL_RATE=1 worker
```

## Testing

```bash
go test ./...                                   # unit tests: FSM transition rules (internal/domain)
make up && make test-integration                 # full stack: lifecycle, concurrency race, stale/duplicate messaging
```

`internal/domain` is pure (no I/O), so its table-driven tests cover every
tenant/task transition in milliseconds. `test/integration` (build tag
`integration`) exercises the real Postgres + NATS + HTTP stack for the
concurrency and messaging behavior that can't be meaningfully unit-tested.

## Quality gates

`make check` runs, in order: `gofmt` (fails on unformatted files), `go vet`,
`golangci-lint` (via `go run`, no local install needed — includes `gosec`
for the security-scan nice-to-have), `govulncheck`, and the unit test suite.

## No secrets

All config is non-secret local defaults (`docker-compose.yml`); nothing in
this repo requires a credential to boot.
