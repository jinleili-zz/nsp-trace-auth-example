# NSP Platform Example Application Design

**Date**: 2026-05-06  
**Topic**: nsp-platform-example  
**Status**: Approved

---

## 1. Overview

This document describes the design of an example application built on top of `nsp-platform`. The application consists of three Go services that demonstrate the usage of `auth`, `trace`, `saga`, and `taskqueue` modules.

### 1.1 Business Scenarios

1. **Simple HTTP Query**: User calls `GET /api/query?name=xxx` on `top`, which validates parameters and forwards the request to `az` via authenticated HTTP.
2. **Saga Order Creation**: User calls `POST /api/order` on `top`, which submits a distributed saga transaction to `az` via `saga.Engine`.
3. **Async Worker Calculation**: During saga execution, `az` sends a calculation task to `worker` via `taskqueue`, and `worker` replies with the result.

### 1.2 Services

| Service | Type | Port | Description |
|---------|------|------|-------------|
| `top` | Gin HTTP Server | `:8080` | Entry point, handles user requests, forwards to `az` |
| `az` | Gin HTTP Server | `:8081` | Business logic, saga HTTP endpoints, taskqueue producer/consumer |
| `worker` | TaskQueue Consumer | N/A | Processes calculation tasks from `az`, replies via reply queue |

### 1.3 Infrastructure

| Component | Purpose | Image |
|-----------|---------|-------|
| PostgreSQL | Saga persistence | `postgres:15-alpine` |
| Redis | TaskQueue backend | `redis:7-alpine` |

---

## 2. Architecture

### 2.1 Directory Structure

```
example/
├── cmd/
│   ├── top/
│   │   └── main.go          # top service entry point
│   ├── az/
│   │   └── main.go          # az service entry point
│   └── worker/
│       └── main.go          # worker service entry point
├── internal/
│   ├── config/
│   │   └── config.go        # Shared configuration (PostgreSQL, Redis, service addresses)
│   └── types/
│       └── types.go         # Shared types (CalcTask, CalcResult, OrderRequest)
├── docker-compose.yml       # One-click startup for all services + PostgreSQL + Redis
├── Dockerfile.top
├── Dockerfile.az
├── Dockerfile.worker
└── go.mod
```

### 2.2 Go Module

All three services live in a single Go module:

```go
module github.com/jinleili-zz/nsp-platform/example

go 1.22

require github.com/jinleili-zz/nsp-platform v0.0.0
```

Replace directive points to the local `nsp-platform` directory.

---

## 3. Service Design

### 3.1 top Service

**Middleware Stack** (follows `nsp-platform` convention):

1. `middleware.GinRecovery()` - Panic recovery
2. `trace.TraceMiddleware(instanceId)` - Distributed tracing (extract/generate trace-id)
3. `middleware.GinLogger()` - Request logging
4. `auth.AKSKAuthMiddleware(verifier, opt)` - Optional AK/SK auth for user requests

**Routes**:

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | Skip | Health check |
| GET | `/api/query` | Required | Forward query to `az` via authenticated HTTP |
| POST | `/api/order` | Required | Submit saga order creation to `az` |

**Handler: `/api/query`**

1. Validate `name` parameter (non-empty).
2. Create HTTP request to `az` (`GET /query?name=xxx`).
3. Use `trace.TracedClient` to send request (auto-injects `X-B3-TraceId`, `X-B3-SpanId`).
4. Use `auth.NewSigner` to sign the request with shared AK/SK.
5. Forward `az` response to user.

**Handler: `/api/order`**

1. Parse `OrderRequest` from body.
2. Initialize `saga.Engine` with PostgreSQL DSN and shared `CredentialStore`.
3. Build `SagaDefinition`:
   - Step 1: `POST /inventory/reserve` (sync) with compensation `POST /inventory/release`
   - Step 2: `POST /orders/create` (sync) with compensation `POST /orders/cancel`
   - Step 2 sends a taskqueue message to `worker` and waits for reply
4. Call `engine.SubmitAndWait(ctx, def)`.
5. Return transaction result to user.

### 3.2 az Service

**Middleware Stack**:

1. `middleware.GinRecovery()`
2. `trace.TraceMiddleware(instanceId)`
3. `middleware.GinLogger()`
4. `auth.AKSKAuthMiddleware(verifier, opt)` - Authenticate requests from `top`

**Routes**:

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/health` | Skip | Health check |
| GET | `/query` | Required | Simple query response |
| POST | `/inventory/reserve` | Required | Saga step 1: Reserve inventory |
| POST | `/inventory/release` | Required | Saga compensation 1: Release inventory |
| POST | `/orders/create` | Required | Saga step 2: Create order (sends task to worker) |
| POST | `/orders/cancel` | Required | Saga compensation 2: Cancel order |

**Handler: `/query`**

1. Extract trace-id from context (`trace.TraceFromGin` or `logger.TraceIDFromContext`).
2. Log request with trace context.
3. Return mock data: `{"status": "ok", "name": "xxx", "timestamp": "..."}`.

**Handler: `/inventory/reserve`**

1. Log with trace-id.
2. Return `{"reservation_id": "RSV-<uuid>", "status": "reserved"}`.

**Handler: `/inventory/release`**

1. Log with trace-id.
2. Return 200 OK.

**Handler: `/orders/create`**

1. Extract trace-id from context.
2. Build `CalcTask` (operation: `multiply`, operands: `[price, quantity]`).
3. Initialize `asynqbroker.Broker` and `asynqbroker.Consumer` for reply.
4. Publish task to `example:calc:incoming` with `ReplySpec{Queue: "example:calc:outgoing"}`.
5. Wait for reply on `example:calc:outgoing` (with timeout).
6. Return order result including worker calculation: `{"order_id": "ORD-<uuid>", "total": <result>}`.

**Handler: `/orders/cancel`**

1. Log with trace-id.
2. Return 200 OK.

### 3.3 worker Service

**Initialization**:

1. Connect to Redis.
2. Create `asynqbroker.Consumer` with:
   - Queue: `example:calc:incoming`
   - Concurrency: 2

**Task Handler (`example:calc:incoming`)**:

1. Unmarshal `CalcTask` from `task.Payload`.
2. Log processing with trace-id from context (auto-injected by asynqbroker).
3. Execute calculation based on `Operation` (`add`, `subtract`, `multiply`, `divide`).
4. If `task.Reply != nil`:
   - Marshal `CalcResult`.
   - Create `asynqbroker.Broker`.
   - Publish reply to `task.Reply.Queue` (`example:calc:outgoing`).
5. Return nil on success.

---

## 4. Data Flow

### 4.1 Simple HTTP Query Flow

```
User
  │ GET /api/query?name=alice
  ▼
top (:8080)
  │ 1. TraceMiddleware: generates trace-id T1, span-id S1
  │ 2. GinLogger: logs request with trace_id=T1
  │ 3. AKSKAuthMiddleware: verifies user auth (optional)
  │ 4. Handler validates name
  │ 5. TracedClient.Get(ctx, "http://az:8081/query?name=alice")
  │    - Inject: X-B3-TraceId=T1, X-B3-SpanId=S1
  │    - Signer.Sign: adds Authorization header
  ▼
az (:8081)
  │ 1. TraceMiddleware: extracts T1, generates span-id S2, parent=S1
  │ 2. AKSKAuthMiddleware: verifies signature from top
  │ 3. Handler logs with trace_id=T1
  │ 4. Returns {"status":"ok","name":"alice"}
  ▼
top
  │ 5. Forwards response to user
  ▼
User
```

### 4.2 Saga Order Creation Flow

```
User
  │ POST /api/order {"sku":"SKU-001","count":2}
  ▼
top (:8080)
  │ 1. TraceMiddleware: generates trace-id T2, span-id S3
  │ 2. Handler builds SagaDefinition
  │ 3. engine.SubmitAndWait(ctx, def)
  │    - Writes _trace_id=T2, _span_id=S3 to transaction payload
  │    - Persists to PostgreSQL
  │    - Coordinator picks up transaction
  ▼
Saga Coordinator (in top process)
  │ Step 1: POST http://az:8081/inventory/reserve
  │         - Uses TracedClient: injects T2 + new span-id
  │         - Uses AuthAK for signature
  ▼
az (:8081)
  │ 1. TraceMiddleware: extracts T2, generates new span-id
  │ 2. Handler logs with trace_id=T2
  │ 3. Returns {"reservation_id":"RSV-001","status":"reserved"}
  ▼
Saga Coordinator
  │ Step 2: POST http://az:8081/orders/create
  │         - Uses TracedClient: injects T2 + new span-id
  ▼
az (:8081)
  │ 1. TraceMiddleware: extracts T2
  │ 2. Handler sends taskqueue message to worker
  │    - MetadataFromContext: trace_id=T2
  │    - Broker.Publish: wraps trace into envelope
  ▼
worker
  │ 1. Consumer unwraps envelope, injects trace context
  │ 2. Logs with trace_id=T2
  │ 3. Calculates result
  │ 4. Publishes reply with trace envelope
  ▼
az (:8081)
  │ 1. Reply consumer unwraps trace
  │ 2. Handler returns order result
  ▼
Saga Coordinator
  │ Transaction succeeds
  ▼
top
  │ Returns {"tx_id":"xxx","status":"succeeded"} to user
  ▼
User
```

### 4.3 TaskQueue Trace Propagation Detail

When `az` publishes a task:

```go
// In az handler
tc, _ := trace.TraceFromGin(c)
ctx := trace.ContextWithTrace(context.Background(), tc)

// MetadataFromContext extracts trace info
metadata := trace.MetadataFromContext(ctx)
// metadata = {"trace_id": "T2", "span_id": "S5", "sampled": "1"}

task := &taskqueue.Task{
    Type:     "calc:task",
    Payload:  payload,
    Queue:    "example:calc:incoming",
    Reply:    &taskqueue.ReplySpec{Queue: "example:calc:outgoing"},
    Metadata: metadata,  // business metadata
}

broker.Publish(ctx, task)
// wrapWithTrace automatically adds trace to envelope
```

When `worker` receives the task:

```go
// In consumer.Handle callback
consumer.Handle("calc:task", func(ctx context.Context, t *taskqueue.Task) error {
    // ctx already contains trace context (injected by injectTraceFromMetadata)
    tc := trace.MustTraceFromContext(ctx)
    logger.InfoContext(ctx, "processing task", "trace_id", tc.TraceID)
    // ...
})
```

---

## 5. Configuration

### 5.1 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_DSN` | `postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable` | PostgreSQL connection string for saga |
| `REDIS_ADDR` | `localhost:6379` | Redis address for taskqueue |
| `REDIS_PASSWORD` | `` | Redis password |
| `TOP_ADDR` | `:8080` | top HTTP listen address |
| `AZ_ADDR` | `:8081` | az HTTP listen address |
| `AZ_URL` | `http://localhost:8081` | az URL for top to call |
| `ACCESS_KEY` | `example-ak` | Shared AK for auth |
| `SECRET_KEY` | `example-sk-1234567890abcdef` | Shared SK for auth |

### 5.2 docker-compose.yml Services

```yaml
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: nsp
      POSTGRES_PASSWORD: nsp123
      POSTGRES_DB: nsp
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  az:
    build:
      context: .
      dockerfile: Dockerfile.az
    environment:
      POSTGRES_DSN: postgres://nsp:nsp123@postgres:5432/nsp?sslmode=disable
      REDIS_ADDR: redis:6379
      AZ_ADDR: :8081
    depends_on:
      - postgres
      - redis

  worker:
    build:
      context: .
      dockerfile: Dockerfile.worker
    environment:
      REDIS_ADDR: redis:6379
    depends_on:
      - redis

  top:
    build:
      context: .
      dockerfile: Dockerfile.top
    environment:
      AZ_URL: http://az:8081
      TOP_ADDR: :8080
      POSTGRES_DSN: postgres://nsp:nsp123@postgres:5432/nsp?sslmode=disable
    depends_on:
      - az
    ports:
      - "8080:8080"
```

---

## 6. Error Handling

### 6.1 HTTP Errors

All services return unified JSON error responses:

```json
{
  "code": 400,
  "message": "name is required",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

### 6.2 Saga Errors

- `ErrTransactionFailed`: Transaction reached failed terminal state (compensation executed)
- `ErrTransactionDisappeared`: Transaction disappeared during `SubmitAndWait`
- Both use `errors.Is()` for checking

### 6.3 TaskQueue Errors

- Handler errors trigger asynq retry mechanism (default max retry: 25)
- Reply timeout in `az`: return error to saga coordinator, triggering compensation

---

## 7. Testing Strategy

### 7.1 Unit Tests

- Test calculation logic in `worker`
- Test configuration parsing
- Test request validation in `top`

### 7.2 Integration Tests

- Start PostgreSQL + Redis via docker-compose
- Run all three services
- Test end-to-end flows:
  1. `curl http://localhost:8080/api/query?name=test`
  2. `curl -X POST http://localhost:8080/api/order -d '{"sku":"SKU-001","count":2}'`
- Verify trace-id propagation across all hops
- Verify saga transaction completion in PostgreSQL

---

## 8. Implementation Plan

After this design is approved, the implementation will follow the plan created by the `writing-plans` skill. The high-level steps are:

1. Initialize Go module and shared packages (`internal/config`, `internal/types`)
2. Implement `top` service (HTTP handlers, auth signing, traced client, saga integration)
3. Implement `az` service (HTTP handlers, auth verification, taskqueue producer, reply consumer)
4. Implement `worker` service (taskqueue consumer, calculation logic, reply publisher)
5. Create Dockerfiles and docker-compose.yml
6. Run integration tests end-to-end
7. Verify trace propagation and saga completion
