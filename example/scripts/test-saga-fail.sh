#!/bin/bash
# test-saga-fail.sh - drive the two failure scenarios exposed by saga-fail-demo
# and pretty-print the LastError fields so it's obvious how the saga framework
# surfaces sub-transaction business errors (HTTP 200 + code != "0").
set -e

BASE_URL="${BASE_URL:-http://localhost:8082}"

echo "=== Scenario A: single-step saga, sub-transaction returns code=\"VALIDATION_ERROR\" ==="
echo "POST $BASE_URL/api/fail-first"
curl -s -X POST "$BASE_URL/api/fail-first" | jq .
echo

echo "=== Scenario B: two-step saga, step 1 OK, step 2 returns code=\"BUSINESS_FAIL\" ==="
echo "POST $BASE_URL/api/fail-middle"
curl -s -X POST "$BASE_URL/api/fail-middle" | jq .
echo

echo "=== Tests Complete ==="
echo "Inspect the saga-fail-demo container logs to see [Query] lines printing"
echo "each step's LastError — that's the canonical way to read sub-transaction"
echo "errors out of the saga engine."
