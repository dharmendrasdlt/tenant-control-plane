.PHONY: up down logs test test-integration fmt lint vuln check concurrency-test stale-update

## up: build & boot the whole stack (postgres, nats, control plane, worker) in one command
up:
	docker compose up --build -d
	@echo "control plane: http://localhost:8080  (try: curl http://localhost:8080/healthz)"

down:
	docker compose down -v

logs:
	docker compose logs -f

## test: full suite — unit tests, then (if the stack is up) the integration test
test: fmt-check vet
	go test ./... -race -count=1

## test-integration: exercises the real stack; requires `make up` first
test-integration:
	CP_BASE_URL=$${CP_BASE_URL:-http://localhost:8080} go test ./test/integration/... -race -count=1 -tags=integration -v

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (echo "not gofmt-formatted:"; gofmt -l .; exit 1)

vet:
	go vet ./...

## lint: static analysis + security scan, wired into one command
lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest run

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

## check: everything required by the brief's quality gates, in one command
check: fmt-check vet lint vuln test

## concurrency-test: fires two concurrent PATCHes with the same version at a running stack;
## demonstrates exactly one succeeds and the other gets tenant_version_conflict
concurrency-test:
	./scripts/concurrency_test.sh

## stale-update: publishes an out-of-order progress message directly onto the broker
## and shows the control plane drops it instead of regressing state
stale-update:
	./scripts/stale_update.sh
