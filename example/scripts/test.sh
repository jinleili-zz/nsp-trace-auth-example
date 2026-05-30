#!/bin/bash
set -e

BASE_URL="${BASE_URL:-http://localhost:8080}"

echo "=== Test 1: Health Check ==="
curl -s "$BASE_URL/health" | jq .

echo ""
echo "=== Test 2: Simple Query ==="
curl -s "$BASE_URL/api/query?name=alice" | jq .

echo ""
echo "=== Test 4: Order Creation (Saga) ==="
curl -s -X POST "$BASE_URL/api/order" \
  -H "Content-Type: application/json" \
  -d '{"sku":"SKU-001","count":2}' | jq .

echo ""
echo "=== Tests Complete ==="
