# saga-fail-demo

Demonstrates how the NSP saga engine surfaces **sub-transaction business
errors** — the case where a downstream service returns HTTP 200 but the
response body carries `code != "0"` and a human-readable message — back to
the saga creator.

The existing `top` example covers the happy path: every sub-transaction
returns `{"code":"0", ...}` and the saga terminates successfully. This demo
is the unhappy counterpart.

## What it shows

1. `parseHTTPResponseEnvelope` rejects HTTP 200 responses whose body has
   `code != "0"`. The rejection error is the literal
   `HTTP 200: response code="<code>": <raw body>` string.
2. That error string is persisted as the failing step's `LastError` field.
3. The coordinator then flips the transaction to `compensating` (reason:
   `"step failed: <name>"`) and runs any compensatable prior steps in
   reverse order.
4. On terminal failure, `Engine.SubmitAndWait` returns an error wrapping
   `saga.ErrTransactionFailed` plus the transaction's `LastError`.
5. `Engine.Query(txID)` returns the same state asynchronously — useful for
   operators or for callers that didn't block on the saga.

## Two scenarios

| Endpoint | Scenario | What happens |
|---|---|---|
| `POST /api/fail-first` | A: single-step, fail at action | 1-step saga whose action returns `{code:"VALIDATION_ERROR", message:"sku is invalid: ..."}`. Step fails immediately; no prior step to compensate. |
| `POST /api/fail-middle` | B: two-step, fail at second action | Step 1 (prepare) returns `{code:"0", reservation_id:"..."}`. Step 2 (commit) returns `{code:"BUSINESS_FAIL", message:"quota exceeded"}`. Step 1 is compensated via `/demo/b/prepare-cancel`. |

Both endpoints respond with a JSON object that already contains:

- `saga_status` — terminal transaction status (`failed`)
- `saga_last_error` — transaction-level `LastError`
- `saga_returned` — the error string returned by `SubmitAndWait`
- `error_is_failed` — `errors.Is(err, saga.ErrTransactionFailed)` result
- `steps[]` — per-step `index / name / status / last_error / poll_count`
  (mirrored from the in-memory return value of `SubmitAndWait`)
- `query_after_term` — the same data fetched via `Engine.Query(txID)`
  after the saga reached a terminal state

So you can read the full error chain from a single HTTP response without
touching the database.

## How to run

The demo needs the saga schema initialized once. The repo's existing
`docker-compose.yml` does that automatically via the `saga-init` one-shot.

### With docker-compose

From `example/`:

```bash
# Build the binary first — the Dockerfile copies it in.
go build -o saga-fail-demo ./cmd/saga-fail-demo

# Bring up the full stack (postgres + redis + az + top + saga-fail-demo).
docker-compose up -d

# Drive both scenarios.
./scripts/test-saga-fail.sh
```

The compose file maps port `18082 -> 8082`, so from the host:

```bash
BASE_URL=http://localhost:18082 ./scripts/test-saga-fail.sh
```

### Standalone (local postgres already running)

```bash
POSTGRES_DSN=postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable \
  go run ./cmd/saga-fail-demo
```

In another shell:

```bash
curl -s -X POST http://localhost:8082/api/fail-first  | jq .
curl -s -X POST http://localhost:8082/api/fail-middle | jq .
```

## Reading the LastError from logs

Inside `runScenario`, after `SubmitAndWait` returns, the demo calls
`logSagaQueryResult` which logs every step's `LastError` at INFO level:

```
[Query] saga transaction status  tx_id=... status=failed last_error="step failed: validate-input" step_count=1
[Query] saga step  step_index=0 step_name=validate-input step_status=failed step_last_error="HTTP 200: response code=\"VALIDATION_ERROR\": {\"code\":\"VALIDATION_ERROR\",\"hint\":\"...\",\"message\":\"sku is invalid: must not be empty\"}"
```

These `[Query]` lines are the canonical pattern for printing sub-transaction
errors out of a saga. The same data is also visible in the JSON response.

## How errors flow end-to-end

```
sub-transaction HTTP response
   │  body: {"code":"VALIDATION_ERROR", "message":"sku is invalid"}
   ▼
saga.parseHTTPResponseEnvelope        (nsp-platform/saga/executor.go:477)
   │  returns error:  HTTP 200: response code="VALIDATION_ERROR": {...}
   ▼
saga.Executor.handleHTTPError         (nsp-platform/saga/executor.go:393)
   │  MaxRetry exhausted → marks step failed, persists err.Error() as LastError
   ▼
saga.Coordinator.executeNextStep      (nsp-platform/saga/coordinator.go:380)
   │  observes failed step → triggerCompensation("step failed: <name>")
   ▼
saga.Coordinator.executeCompensation  (nsp-platform/saga/coordinator.go:681)
   │  compensates prior succeeded steps in reverse order;
   │  then UpdateTransactionStatus(TxStatusFailed, "compensation completed")
   ▼
Engine.SubmitAndWait returns          (nsp-platform/saga/engine.go:400)
   │  err = ErrTransactionFailed wrapped with transaction LastError
   │  status.LastError, status.Steps[].LastError populated
```

Every layer preserves the original envelope body verbatim — nothing is
truncated or rewritten — so the operator can always see exactly what the
downstream service returned.
