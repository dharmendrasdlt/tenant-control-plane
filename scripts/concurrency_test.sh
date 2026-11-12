#!/usr/bin/env bash
# Fires two concurrent PATCHes carrying the same version at a running
# control plane and shows that exactly one wins. Requires `make up` first.
set -euo pipefail

BASE_URL="${CP_BASE_URL:-http://localhost:8080}"
SLUG="race-$(date +%s)-$RANDOM"

echo "creating tenant $SLUG ..."
TENANT=$(curl -sf -X POST "$BASE_URL/tenants" \
  -H 'Content-Type: application/json' \
  -d "{\"slug\":\"$SLUG\",\"name\":\"Race Test\"}")
ID=$(echo "$TENANT" | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')

echo "waiting for tenant to become active ..."
for i in $(seq 1 75); do
  STATUS=$(curl -sf "$BASE_URL/tenants/$ID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')
  [ "$STATUS" = "active" ] && break
  sleep 0.2
done
if [ "$STATUS" != "active" ]; then
  echo "tenant never became active (status=$STATUS) — is the worker running?" >&2
  exit 1
fi

VERSION=$(curl -sf "$BASE_URL/tenants/$ID" | python3 -c 'import json,sys;print(json.load(sys.stdin)["version"])')
echo "racing two PATCHes at version $VERSION ..."

curl -s -o /tmp/patch_a.json -w '%{http_code}' -X PATCH "$BASE_URL/tenants/$ID" \
  -H 'Content-Type: application/json' -d "{\"name\":\"A\",\"version\":$VERSION}" > /tmp/patch_a.code &
PID_A=$!
curl -s -o /tmp/patch_b.json -w '%{http_code}' -X PATCH "$BASE_URL/tenants/$ID" \
  -H 'Content-Type: application/json' -d "{\"name\":\"B\",\"version\":$VERSION}" > /tmp/patch_b.code &
PID_B=$!
wait $PID_A $PID_B

echo "PATCH A -> $(cat /tmp/patch_a.code) $(cat /tmp/patch_a.json)"
echo "PATCH B -> $(cat /tmp/patch_b.code) $(cat /tmp/patch_b.json)"
echo "expect exactly one 200 and one 409 (tenant_version_conflict) above."
