# saga-fail-demo Example Design

**Date**: 2026-06-06
**Topic**: saga-fail-demo
**Status**: Approved & Implemented

---

## 1. Overview

新增一个独立示例服务 `saga-fail-demo`，专门演示当 saga 子事务返回 **HTTP 200 + body 内 `code != "0"`** 这种业务级失败时，saga 引擎如何把错误完整地传递给 saga 创建者。

现有的 `top / az / worker` 三件套主要演示 trace + auth + saga 正常路径（所有子事务都返回 `{"code":"0", ...}`），刻意不去碰子事务失败场景。本示例作为它的"不幸路径"对照。

### 1.1 演示的两个场景

| 路由 | 场景 | 期望行为 |
|---|---|---|
| `POST /api/fail-first` | **A**：单步 saga，action 直接返回 `code="VALIDATION_ERROR"` | 单步立刻失败、`step.LastError` 携带完整 envelope、不触发补偿（无可补偿步骤） |
| `POST /api/fail-middle` | **B**：两步 saga，第 1 步成功、第 2 步返回 `code="BUSINESS_FAIL"` | 第 2 步失败、反向补偿第 1 步、`step.LastError` 携带完整 envelope、事务终态 `failed` |

### 1.2 设计目标

1. 让运维和开发者在 5 分钟内复现"子事务 200 + 业务错误"这一最常见却最隐蔽的失败模式
2. 验证 saga 创建者可以通过三种途径拿到子事务错误：
   - `Engine.SubmitAndWait` 返回的 `status.LastError` 与 `status.Steps[].LastError`
   - `Engine.Query(txID)` 异步查询
   - HTTP 响应直接序列化上面两者
3. 验证错误链每一跳都没有丢失信息：从 `parseHTTPResponseEnvelope` 拼出的 `HTTP 200: response code="..."` 字符串，到 `executor.handleHTTPError` 写入 step 行，再到 `coordinator.triggerCompensation` 写入 tx.LastError，最终被 `SubmitAndWait` 包装成 `ErrTransactionFailed`

---

## 2. Architecture

### 2.1 部署形态

复用现有 `example/docker-compose.yml` 的 postgres / redis / saga-init，新增一个 `saga-fail-demo` 服务：

```
┌────────────────────────────────────────────────────┐
│              saga-fail-demo  :8082                 │
│                                                    │
│  ┌──────────────┐         ┌──────────────────┐    │
│  │ saga creator │         │ sub-tx endpoints │    │
│  │  /api/*      │         │   /demo/*        │    │
│  │ (no AK/SK)   │         │ (AK/SK required) │    │
│  └──────┬───────┘         └─────────▲────────┘    │
│         │                            │             │
│         │   saga.Engine.SubmitAndWait│             │
│         │   ─────────────────────────┐             │
│         │                            │ signed      │
│         │                            │ HTTP call   │
│         ▼                            │             │
│  ┌──────────────────────────────────┐│             │
│  │ saga.Engine + Executor + Coord   │├─────────────┘
│  │  - CredentialStore (in-memory)   │              │
│  │  - WorkerCount: 2                │              │
│  └──────────────────────────────────┘              │
└────────────────────────────────────────────────────┘
                  │
                  ▼
            ┌──────────┐
            │ postgres │  (复用现有 saga schema)
            └──────────┘
```

**关键决策**：单进程、单端口 `:8082` 同时承担 saga 创建者 API 和故意失败的子事务端点。Go HTTP server 每请求独立 goroutine，saga executor 回调本机端点不会死锁。

### 2.2 目录结构

```
example/
├── cmd/
│   ├── top/                 # 现有，未修改
│   ├── az/                  # 现有，未修改
│   ├── worker/              # 现有，未修改
│   └── saga-fail-demo/      # 新增
│       ├── main.go          # ~430 行
│       └── README.md        # 端到端错误流分析
├── scripts/
│   └── test-saga-fail.sh    # 新增，curl 两个场景
├── Dockerfile.sagafail      # 新增
├── docker-compose.yml       # 追加 saga-fail-demo 服务
└── internal/                # 完全复用，未修改
```

---

## 3. Component Design

### 3.1 中间件顺序

严格遵循 `nsp-platform/AGENTS.md` 的规范：

```
Recovery → Trace → Logger → (Auth 仅对 /demo/* 启用)
```

- `/health`、`/api/*`：user-facing 入口，无 AK/SK
- `/demo/*`：子事务端点，强制 AK/SK 验签

### 3.2 子事务端点契约

| 路由 | 行为 | 响应 |
|---|---|---|
| `POST /demo/a/check` | 总是返回业务失败 | `200 OK` + `{code:"VALIDATION_ERROR", message:"sku is invalid: must not be empty", hint:"..."}` |
| `POST /demo/b/prepare` | 总是成功 | `200 OK` + `{code:"0", reservation_id:"RSV-DEMO-001", status:"reserved"}` |
| `POST /demo/b/prepare-cancel` | 补偿动作，总是成功 | `200 OK` + `{code:"0", message:"reservation released"}` |
| `POST /demo/b/commit` | 总是返回业务失败 | `200 OK` + `{code:"BUSINESS_FAIL", message:"quota exceeded for tenant", hint:"..."}` |

注意 `parseHTTPResponseEnvelope`（`nsp-platform/saga/executor.go:477`）对非零 code 的拒绝信息格式是：
```
HTTP 200: response code="<code>": <raw body>
```
这条字符串会被原封不动写入 step.LastError。

### 3.3 Saga 定义

#### Scenario A — `scenario-A-fail-first`

```go
saga.NewSaga("scenario-A-fail-first").
    WithPayload({scenario:"A", note:"..."}).
    AddStep({
        name: "validate-input",
        type: StepTypeSync,
        action:   POST selfURL/demo/a/check,
        compensate: POST selfURL/demo/a/check,  // 占位，永不会触发
        authAK: cfg.AccessKey,
        maxRetry: 1,  // 必须显式设 1；==0 会被 builder 默认成 3
    })
```

#### Scenario B — `scenario-B-fail-middle`

```go
saga.NewSaga("scenario-B-fail-middle").
    AddStep({
        name: "prepare-reservation",
        action: POST selfURL/demo/b/prepare,
        compensate: POST selfURL/demo/b/prepare-cancel,
        compensatePayload: {reservation_id:"{step[0].action_response.reservation_id}"},
        authAK: cfg.AccessKey, maxRetry: 1,
    }).
    AddStep({
        name: "commit-order",
        action: POST selfURL/demo/b/commit,
        compensate: POST selfURL/demo/b/commit,  // 占位
        authAK: cfg.AccessKey, maxRetry: 1,
    })
```

> **Gotcha**：`saga.SagaBuilder.Build()` 在 `nsp-platform/saga/definition.go:269-270` 把 `MaxRetry==0` 视为"未设置"并默认成 3。要 fail-fast 必须显式写 `MaxRetry: 1`。这个细节在示例代码注释里特别标出。

### 3.4 错误返回结构

`runScenario` 把以下信息一并塞进 HTTP 响应，调用方一次就能看到全链路：

```json
{
  "scenario": "scenario-A-fail-first",
  "tx_id": "...",
  "trace_id": "...",
  "error_is_failed": true,                       // errors.Is(err, ErrTransactionFailed)
  "saga_status": "failed",
  "saga_last_error": "compensation completed",   // tx.LastError
  "saga_returned": "transaction reached terminal failed state: compensation completed",
  "steps": [                                     // SubmitAndWait 返回的 status.Steps
    { "index":0, "name":"...", "status":"failed", "last_error":"HTTP 200: response code=\"...\":" }
  ],
  "query_after_term": {                          // engine.Query 的返回，与上面字段对照
    "tx_id":"...", "status":"failed", "last_error":"...",
    "steps":[ ... ]
  }
}
```

HTTP 状态码故意仍是 `200 OK` —— 这个端点的职责是"报告 saga 观察到了什么"，而非"信号一次传输级失败"。

### 3.5 服务端日志：如何打印 LastError

`logSagaQueryResult`（`example/cmd/saga-fail-demo/main.go:380`）是规范打印姿势：

```go
qs, _ := engine.Query(ctx, txID)
logger.InfoContext(ctx, "[Query] saga transaction status",
    "tx_id", qs.ID, "status", qs.Status,
    "last_error", qs.LastError, ...)
for _, s := range qs.Steps {
    logger.InfoContext(ctx, "[Query] saga step",
        "step_index", s.Index, "step_name", s.Name,
        "step_status", s.Status,
        "step_last_error", s.LastError, ...)
}
```

---

## 4. Error Flow (End-to-End)

```
子事务 HTTP 响应 body: {"code":"BUSINESS_FAIL", "message":"quota exceeded for tenant"}
   │
   ▼
saga.parseHTTPResponseEnvelope       (nsp-platform/saga/executor.go:477)
   返回 error: HTTP 200: response code="BUSINESS_FAIL": {"code":"BUSINESS_FAIL",...}
   │
   ▼
saga.Executor.handleHTTPError        (nsp-platform/saga/executor.go:393)
   MaxRetry 耗尽 → 标记 step failed
   持久化 err.Error() 到 step.LastError
   │
   ▼
saga.Coordinator.executeNextStep     (nsp-platform/saga/coordinator.go:380)
   观察到 failed step → triggerCompensation("step failed: <name>")
   │
   ▼
saga.Coordinator.executeCompensation (nsp-platform/saga/coordinator.go:681)
   反向补偿所有 status==succeeded|polling|compensating 的步骤
   最后 UpdateTransactionStatus(TxStatusFailed, "compensation completed")
   │
   ▼
Engine.SubmitAndWait 返回             (nsp-platform/saga/engine.go:400)
   err = ErrTransactionFailed wrapped with tx.LastError
   status.LastError / status.Steps[].LastError 全部填充
```

每一跳都没有信息丢失。原始 envelope body（含 message / hint 等业务字段）一字不漏保留到 step.LastError。

---

## 5. Testing

### 5.1 验证已通过的项目

| 项目 | 期望 | 实际 |
|---|---|---|
| 场景 A：单步失败 | step.LastError 含完整 envelope | ✅ `HTTP 200: response code="VALIDATION_ERROR": {"code":"VALIDATION_ERROR","message":"sku is invalid:..."}` |
| 场景 A：重试次数 | maxRetry=1 → 只调用 1 次 | ✅ `/demo/a/check` 日志只出现 1 次 |
| 场景 B：补偿触发 | 第 1 步状态变为 compensated | ✅ step 0 status=compensated, last_error="" |
| 场景 B：第 2 步错误 | step.LastError 含 BUSINESS_FAIL envelope | ✅ |
| `errors.Is(err, ErrTransactionFailed)` | true | ✅ |
| `Engine.Query` 异步可查 | 返回相同 last_error | ✅ `query_after_term` 字段对照 |
| AK/SK 验签 | 未签名请求被拒 | ✅ `curl -X POST /demo/a/check` 返回 400 |
| 数据库持久化 | `saga_steps.last_error` 列存原文 | ✅ `psql` 直查确认 |
| 中间件顺序 | Recovery→Trace→Logger→Auth | ✅ 与 AGENTS.md 一致 |

### 5.2 复现命令

```bash
cd example
go build -o saga-fail-demo ./cmd/saga-fail-demo

POSTGRES_DSN=postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable \
ACCESS_KEY=example-ak SECRET_KEY=example-sk-1234567890abcdef \
LOG_MODE=dev ./saga-fail-demo

# 另一个 shell
./scripts/test-saga-fail.sh
```

或 docker-compose：

```bash
docker-compose up -d saga-fail-demo
BASE_URL=http://localhost:18082 ./scripts/test-saga-fail.sh
```

---

## 6. YAGNI — 显式不做的事

- **异步轮询失败（async step + poll 返回 code!="0"）**：当前实现 `parseHTTPResponseEnvelope` 在 poll-status 匹配前就拒绝非零 code（见 `executor_http_client_test.go:353-383`），错误传播路径与同步步骤完全一致。增加此场景需引入 worker 配合，复杂度不值得。
- **`GET /api/saga/:txID` 异步查询端点**：`Engine.Query` 已经在响应体的 `query_after_term` 字段里完整展示，单独加端点只是 API 风格差异，无新信息。
- **sagactl CLI 集成**：`nsp-platform/cmd/sagactl` 已能查 saga 状态，演示侧无需重复。
- **MaxRetry 多次重试场景**：核心错误传播机制与重试次数无关，多等几秒看 3 次重试日志并不能展示新的传递路径。

---

## 7. Future Work

若后续需要扩展：

1. 加 `GET /api/saga/:txID` 真正的异步查询端点（用户提交后离开，事后凭 txID 查）
2. 加一个 "scenario C：async step poll 返回 code!=0"
3. 接入 `sagactl watch`，演示运维侧实时观测

---

## 8. Implementation Outcome

实施完成，所有验证项通过。详细测试输出与日志见 PR 描述。
