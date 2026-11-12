#!/usr/bin/env bash
# Publishes an out-of-order / duplicate progress update directly onto the
# broker, bypassing the worker entirely, and shows the control plane drops
# it instead of regressing tenant/task state. Requires `make up` first and
# the NATS CLI (https://github.com/nats-io/natscli): `brew install nats-io/nats-tools/nats`.
set -euo pipefail

BASE_URL="${CP_BASE_URL:-http://localhost:8080}"
NATS_URL="${NATS_URL:-nats://localhost:4222}"
SLUG="stale-$(date +%s)-$RANDOM"

echo "creating tenant $SLUG ..."
TENANT=$(curl -sf -X POST "$BASE_URL/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$SLUG\",\"name\":\"Stale Update Test\"}")
ID=$(echo "$TENANT" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')

echo "waiting for tenant to become active (deploy task to go terminal) ..."
for i in $(seq 1 75); do
  STATUS=$(curl -sf "$BASE_URL/tenants/$ID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')
  [ "$STATUS" = "active" ] && break
  sleep 0.2
done
[ "$STATUS" = "active" ] || { echo "tenant never became active" >&2; exit 1; }

TASK_ID=$(curl -sf "$BASE_URL/tasks?tenant_id=$ID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["items"][0]["id"])')
echo "task $TASK_ID is already terminal (done). publishing a stale in_progress and a duplicate done ..."

nats --server "$NATS_URL" pub tasks.progress \
  "{\"update_id\":\"manual-stale-1\",\"task_id\":\"$TASK_ID\",\"status\":\"in_progress\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\"}"
nats --server "$NATS_URL" pub tasks.progress \
  "{\"update_id\":\"manual-dup-1\",\"task_id\":\"$TASK_ID\",\"status\":\"done\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\"}"

sleep 1
FINAL=$(curl -sf "$BASE_URL/tenants/$ID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')
echo "tenant status after replay/stale publish: $FINAL (expect: active, unchanged)"
