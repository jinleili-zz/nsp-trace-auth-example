# NSP Platform 集成指南

本文档基于 `nsp-platform` 与 `example` 模块的当前代码实现，说明如何在基于 Gin 的 HTTP 应用中集成 NSP Platform 提供的 logger、trace、auth、taskqueue、saga 等基础能力。

## 目录

1. [Gin HTTP 应用集成 logger / trace / auth 中间件](#1-gin-http-应用集成-logger--trace--auth-中间件)
2. [HTTP Server 提取 traceid 并通过 HTTP Client 透传（top → az）](#2-http-server-提取-traceid-并通过-http-client-透传top--az)
3. [HTTP Server 提取 traceid 并通过 Saga 透传（top → az）](#3-http-server-提取-traceid-并通过-saga-透传top--az)
4. [HTTP Server 提取 traceid 并通过 TaskQueue 透传（az → worker）](#4-http-server-提取-traceid-并通过-taskqueue-透传az--worker)
5. [使用 AK/SK 对 API 请求签名（基于 HTTP Client，以 top 为例）](#5-使用-aksk-对-api-请求签名基于-http-client以-top-为例)
6. [使用 AK/SK 对 API 请求签名（基于 Saga，以 top 为例）](#6-使用-aksk-对-api-请求签名基于-saga以-top-为例)
7. [使用 Auth 对请求进行校验（以 az 代码为例）](#7-使用-auth-对请求进行校验以-az-代码为例)

---

## 1. Gin HTTP 应用集成 logger / trace / auth 中间件

### 1.1 初始化 Logger

在应用启动时初始化 `logger` 模块，所有后续日志输出都会使用该实例。

**文件**: `example/cmd/az/main.go:38`
```go
if err := logger.Init(logger.DevelopmentConfig("az")); err != nil {
    panic(err)
}
defer logger.Sync()
```

**文件**: `example/cmd/top/main.go:33`
```go
if err := logger.Init(logger.DevelopmentConfig("top")); err != nil {
    panic(err)
}
defer logger.Sync()
```

### 1.2 中间件注册顺序

当前示例推荐的中间件顺序为：

1. `middleware.GinRecovery()` — panic 恢复
2. `trace.TraceMiddleware(instanceId)` — trace 提取与注入
3. `middleware.GinLogger()` — 请求日志
4. `auth.AKSKAuthMiddleware(verifier, opt)` — AK/SK 鉴权（按需）

**文件**: `example/cmd/az/main.go:43-65`
```go
gin.SetMode(gin.ReleaseMode)
r := gin.New()

instanceId := trace.GetInstanceId()

// Auth setup: verify requests from top
credStore := auth.NewMemoryStore([]*auth.Credential{
    {
        AccessKey: cfg.AccessKey,
        SecretKey: cfg.SecretKey,
        Label:     "top-client",
        Enabled:   true,
    },
})
nonceStore := auth.NewMemoryNonceStore()
verifier := auth.NewVerifier(credStore, nonceStore, nil)

r.Use(middleware.GinRecovery())
r.Use(trace.TraceMiddleware(instanceId))
r.Use(middleware.GinLogger())
r.Use(auth.AKSKAuthMiddleware(verifier, &auth.MiddlewareOption{
    Skipper: auth.NewSkipperByPath("/health"),
}))
```

### 1.3 TraceMiddleware 实现细节

**文件**: `nsp-platform/trace/middleware.go:23-46`
```go
func TraceMiddleware(instanceId string) gin.HandlerFunc {
    return func(c *gin.Context) {
        // 1. 从请求中提取或生成 TraceContext
        tc := Extract(c.Request, instanceId)

        // 2. 将 TraceContext 注入标准 context，并更新 c.Request
        ctx := ContextWithTrace(c.Request.Context(), tc)

        // 3. 同时写入 logger 模块的 context keys，保证日志自动关联 trace_id 和 span_id
        ctx = logger.ContextWithTraceID(ctx, tc.TraceID)
        ctx = logger.ContextWithSpanID(ctx, tc.SpanId)

        c.Request = c.Request.WithContext(ctx)

        // 4. 同时写入 gin.Context（供不使用标准 context 的 Handler 直接访问）
        c.Set(ginTraceKey, tc)

        // 5. 向响应头写入追踪信息
        InjectResponse(c.Writer, tc)

        // 6. 继续处理请求
        c.Next()
    }
}
```

### 1.4 GinLogger 实现细节

**文件**: `example/internal/middleware/middleware.go:33-68`
```go
func GinLogger() gin.HandlerFunc {
    return func(c *gin.Context) {
        start := time.Now()
        path := c.Request.URL.Path
        query := c.Request.URL.RawQuery

        logger.InfoContext(c.Request.Context(), "request started",
            logger.FieldMethod, c.Request.Method,
            logger.FieldPath, path,
            logger.FieldPeerAddr, c.ClientIP(),
        )

        c.Next()

        latency := time.Since(start)
        fields := []interface{}{
            logger.FieldMethod, c.Request.Method,
            logger.FieldPath, path,
            logger.FieldCode, c.Writer.Status(),
            logger.FieldLatencyMS, latency.Milliseconds(),
            "response_size", c.Writer.Size(),
        }

        if query != "" {
            fields = append(fields, "query", query)
        }

        status := c.Writer.Status()
        if status >= 500 {
            logger.ErrorContext(c.Request.Context(), "request completed", fields...)
        } else if status >= 400 {
            logger.WarnContext(c.Request.Context(), "request completed", fields...)
        } else {
            logger.InfoContext(c.Request.Context(), "request completed", fields...)
        }
    }
}
```

---

## 2. HTTP Server 提取 traceid 并通过 HTTP Client 透传（top → az）

### 2.1 服务端提取 TraceContext

当外部请求到达 top 服务时，`TraceMiddleware` 会自动从 HTTP 请求头中提取 trace 信息。

**文件**: `nsp-platform/trace/propagator.go:48-88`
```go
func Extract(r *http.Request, instanceId string) *TraceContext {
    tc := &TraceContext{
        InstanceId: instanceId,
        Sampled:    true,
    }

    // 1. 提取 TraceID（优先级：X-B3-TraceId > X-Request-Id > 新生成）
    traceID := r.Header.Get(HeaderTraceID)
    if traceID != "" && isValidHexString(traceID, 32) {
        tc.TraceID = traceID
    } else {
        requestID := r.Header.Get(HeaderRequestID)
        if requestID != "" && isValidHexString(requestID, 32) {
            tc.TraceID = requestID
        } else {
            tc.TraceID = NewTraceID()
        }
    }

    // 2. 提取 ParentSpanId（来自请求头中的 X-B3-SpanId）
    parentSpanId := r.Header.Get(HeaderSpanId)
    if parentSpanId != "" && isValidHexString(parentSpanId, 16) {
        tc.ParentSpanId = parentSpanId
    }

    // 3. SpanId 始终新生成
    tc.SpanId = NewSpanId()

    // 4. 提取 Sampled 标志
    sampled := r.Header.Get(HeaderSampled)
    if sampled == "0" {
        tc.Sampled = false
    }

    return tc
}
```

### 2.2 使用 TracedClient 透传 Trace

**文件**: `nsp-platform/trace/client.go:12-42`
```go
type TracedClient struct {
    inner *http.Client
}

func NewTracedClient(inner *http.Client) *TracedClient {
    if inner == nil {
        inner = &http.Client{
            Timeout: 30 * time.Second,
        }
    }
    return &TracedClient{inner: inner}
}

func (c *TracedClient) Do(req *http.Request) (*http.Response, error) {
    tc, ok := TraceFromContext(req.Context())
    if ok && tc != nil {
        Inject(req, tc)
    }
    return c.inner.Do(req)
}
```

### 2.3 Top 服务完整调用示例

**文件**: `example/cmd/top/main.go:77-127`
```go
func makeQueryHandler(cfg *config.Config) gin.HandlerFunc {
    client := trace.NewTracedClient(nil)
    signer := auth.NewSigner(cfg.AccessKey, cfg.SecretKey)

    return func(c *gin.Context) {
        ctx := c.Request.Context()
        name := c.Query("name")
        if name == "" {
            c.JSON(http.StatusBadRequest, types.APIResponse{
                Code:    400,
                Message: "name is required",
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        url := fmt.Sprintf("%s/query?name=%s", cfg.AzURL, name)
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
        if err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: err.Error(),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        if err := signer.Sign(req); err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: err.Error(),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        resp, err := client.Do(req)
        if err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: err.Error(),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }
        defer resp.Body.Close()

        body, _ := io.ReadAll(resp.Body)
        c.Data(resp.StatusCode, "application/json", body)
    }
}
```

关键要点：
- `c.Request.Context()` 中已包含 `TraceContext`（由 `TraceMiddleware` 注入）
- `trace.NewTracedClient(nil)` 创建带追踪能力的 HTTP 客户端
- `client.Do(req)` 会自动从 `req.Context()` 提取 trace 信息并注入到出站请求头中
- 出站请求头包含 `X-B3-TraceId`、`X-B3-SpanId`、`X-B3-Sampled`

---

## 3. HTTP Server 提取 traceid 并通过 Saga 透传（top → az）

### 3.1 Saga 引擎初始化与 Trace 注入

当 top 服务调用 `engine.SubmitAndWait` 时，`Engine.Submit` 会将调用方 context 中的 trace 信息写入事务 payload：

**文件**: `nsp-platform/saga/executor.go:140-149`
```go
// Inject trace context for distributed tracing
// First try from context, then fall back to transaction payload
tc, ok := trace.TraceFromContext(ctx)
if !ok || tc == nil {
    // Try to extract trace from transaction payload
    tc = extractTraceFromPayload(tx.Payload)
}
if tc != nil {
    trace.Inject(req, tc)
}
```

### 3.2 从 Payload 恢复 Trace（后台补偿/轮询场景）

**文件**: `nsp-platform/saga/executor.go:518-540`
```go
func extractTraceFromPayload(payload map[string]any) *trace.TraceContext {
    if payload == nil {
        return nil
    }

    traceID, ok := payload["_trace_id"].(string)
    if !ok || traceID == "" {
        return nil
    }

    spanID, _ := payload["_span_id"].(string)

    tc := &trace.TraceContext{
        TraceID:      traceID,
        SpanId:       trace.NewSpanId(), // Generate new span for this operation
        ParentSpanId: spanID,            // Parent is the original request span
        Sampled:      true,
    }

    return tc
}
```

### 3.3 Top 服务 Saga 调用完整示例

**文件**: `example/cmd/top/main.go:129-257`
```go
func makeOrderHandler(cfg *config.Config) gin.HandlerFunc {
    return func(c *gin.Context) {
        ctx := c.Request.Context()

        var req types.OrderRequest
        if err := c.ShouldBindJSON(&req); err != nil {
            c.JSON(http.StatusBadRequest, types.APIResponse{
                Code:    400,
                Message: err.Error(),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        credStore := auth.NewMemoryStore([]*auth.Credential{
            {
                AccessKey: cfg.AccessKey,
                SecretKey: cfg.SecretKey,
                Label:     "top-client",
                Enabled:   true,
            },
        })

        engine, err := saga.NewEngine(
            &saga.Config{
                DSN:               cfg.PostgresDSN,
                WorkerCount:       2,
                PollBatchSize:     10,
                PollScanInterval:  200 * time.Millisecond,
                CoordScanInterval: 300 * time.Millisecond,
                HTTPTimeout:       5 * time.Second,
                InstanceID:        trace.GetInstanceId(),
                CredentialStore:   credStore,
            })
        if err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: fmt.Sprintf("create saga engine: %v", err),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }
        defer engine.Stop()

        if err := engine.Start(ctx); err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: fmt.Sprintf("start saga engine: %v", err),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        def, err := saga.NewSaga("example-order").
            WithPayload(map[string]any{
                "sku":   req.SKU,
                "count": req.Count,
            }).
            AddStep(saga.Step{
                Name:         "reserve-inventory",
                Type:         saga.StepTypeSync,
                ActionMethod: "POST",
                ActionURL:    cfg.AzURL + "/inventory/reserve",
                ActionPayload: map[string]any{
                    "sku":   "{transaction.payload.sku}",
                    "count": "{transaction.payload.count}",
                },
                CompensateMethod: "POST",
                CompensateURL:    cfg.AzURL + "/inventory/release",
                CompensatePayload: map[string]any{
                    "sku": "{transaction.payload.sku}",
                },
                AuthAK: cfg.AccessKey,
            }).
            AddStep(saga.Step{
                Name:         "create-order",
                Type:         saga.StepTypeSync,
                ActionMethod: "POST",
                ActionURL:    cfg.AzURL + "/orders/create",
                ActionPayload: map[string]any{
                    "sku":   "{transaction.payload.sku}",
                    "count": "{transaction.payload.count}",
                },
                CompensateMethod: "POST",
                CompensateURL:    cfg.AzURL + "/orders/cancel",
                CompensatePayload: map[string]any{
                    "sku": "{transaction.payload.sku}",
                },
                AuthAK: cfg.AccessKey,
            }).
            Build()
        if err != nil {
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: fmt.Sprintf("build saga: %v", err),
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
        defer cancel()

        txID, status, err := engine.SubmitAndWait(waitCtx, def)
        if err != nil {
            msg := fmt.Sprintf("saga failed: %v", err)
            if status != nil {
                msg = fmt.Sprintf("saga %s failed: %v", status.Status, err)
            }
            c.JSON(http.StatusInternalServerError, types.APIResponse{
                Code:    500,
                Message: msg,
                TraceID: logger.TraceIDFromContext(ctx),
            })
            return
        }

        c.JSON(http.StatusOK, types.APIResponse{
            Code:    0,
            Message: "success",
            Data: map[string]any{
                "tx_id":  txID,
                "status": status.Status,
            },
            TraceID: logger.TraceIDFromContext(ctx),
        })
    }
}
```

关键要点：
- `Engine.Submit` 会将调用方 context 中的 trace 信息写入事务 payload 的 `_trace_id`、`_span_id` 字段
- Saga 执行器在执行步骤时，优先从 context 提取 trace，若 context 不可用则从 transaction payload 恢复
- 每个步骤的出站 HTTP 请求都会自动注入 `X-B3-TraceId`、`X-B3-SpanId`、`X-B3-Sampled` 头
- 补偿步骤同样支持 trace 透传

---

## 4. HTTP Server 提取 traceid 并通过 TaskQueue 透传（az → worker）

### 4.1 Broker.Publish 自动透传 Trace

`Broker.Publish(ctx, task)` 内部通过 `wrapWithTrace(ctx, ...)` **自动**从 `ctx` 提取 `TraceContext`，并将 `TraceID`、`SpanID`、`Sampled` 写入消息 envelope 的专属字段（`_tid`、`_sid`、`_smpl`）。调用方**无需**手动将 trace 信息放入 `task.Metadata`。

`task.Metadata` 的设计目的是携带**业务级别**的 KV 元数据（如 tenant_id、request_source 等），不应用于 trace 传播。

### 4.2 Az 服务发布任务（正确用法）

**文件**: `example/cmd/az/main.go:177-183`
```go
task := &taskqueue.Task{
    Type:    taskTypeCalc,
    Payload: payload,
    Queue:   queueCalcIncoming,
    Reply:   &taskqueue.ReplySpec{Queue: queueCalcOutgoing},
}

// ctx 已包含 TraceContext（由 trace.TraceMiddleware 注入）
// Publish 会自动从 ctx 提取 trace 写入 envelope，无需手动处理
broker.Publish(ctx, task)
```

### 4.3 Broker 自动封装 Trace Envelope

**文件**: `nsp-platform/taskqueue/asynqbroker/trace_envelope.go:14-60`
```go
type taskEnvelope struct {
    Version int               `json:"_v"`
    TraceID string            `json:"_tid,omitempty"`
    SpanID  string            `json:"_sid,omitempty"`
    Sampled bool              `json:"_smpl"`
    ReplyTo *json.RawMessage  `json:"_rto,omitempty"`
    Meta    map[string]string `json:"_meta,omitempty"`
    Payload []byte            `json:"payload"`
}

func wrapWithTrace(ctx context.Context, payload []byte, reply *taskqueue.ReplySpec, metadata map[string]string) []byte {
    tc, ok := trace.TraceFromContext(ctx)
    if (!ok || tc == nil) && reply == nil && len(metadata) == 0 {
        return payload
    }

    env := taskEnvelope{
        Version: 1,
        Payload: payload,
    }
    if ok && tc != nil {
        env.TraceID = tc.TraceID
        env.SpanID = tc.SpanId
        env.Sampled = tc.Sampled
    }
    if reply != nil {
        replyJSON, err := json.Marshal(reply)
        if err != nil {
            return payload
        }
        raw := json.RawMessage(replyJSON)
        env.ReplyTo = &raw
    }
    if len(metadata) > 0 {
        env.Meta = cloneMetadata(metadata)
    }

    data, err := json.Marshal(env)
    if err != nil {
        return payload
    }
    return data
}
```

### 4.4 Worker 消费时恢复 Trace Context

**文件**: `nsp-platform/taskqueue/asynqbroker/consumer.go:96-122`
```go
func (c *Consumer) Handle(taskType string, handler taskqueue.HandlerFunc) {
    c.mux.HandleFunc(taskType, func(ctx context.Context, t *asynq.Task) error {
        payload, traceMeta, reply, metadata := unwrapEnvelope(t.Payload())
        ctx = injectTraceFromMetadata(ctx, traceMeta)

        queueName, _ := asynq.GetQueueName(ctx)
        task := &taskqueue.Task{
            Type:     t.Type(),
            Payload:  payload,
            Queue:    queueName,
            Reply:    reply,
            Metadata: metadata,
        }

        if err := handler(ctx, task); err != nil {
            // ... error handling
            return err
        }
        return nil
    })
}
```

**文件**: `nsp-platform/taskqueue/asynqbroker/trace_envelope.go:107-118`
```go
func injectTraceFromMetadata(ctx context.Context, metadata map[string]string) context.Context {
    tc := trace.TraceFromMetadata(metadata, trace.GetInstanceId())
    if tc == nil {
        return ctx
    }

    ctx = trace.ContextWithTrace(ctx, tc)
    ctx = logger.ContextWithTraceID(ctx, tc.TraceID)
    ctx = logger.ContextWithSpanID(ctx, tc.SpanId)
    return ctx
}
```

### 4.5 Worker 中使用 Trace 的示例

**文件**: `example/cmd/worker/main.go:63-76`
```go
consumer.Handle(taskTypeCalc, func(ctx context.Context, t *taskqueue.Task) error {
    var calcTask types.CalcTask
    if err := json.Unmarshal(t.Payload, &calcTask); err != nil {
        logger.ErrorContext(ctx, "failed to parse task payload", logger.FieldError, err)
        return err
    }

    tc := trace.MustTraceFromContext(ctx)
    logger.InfoContext(ctx, "processing calculation task",
        logger.FieldTaskID, calcTask.TaskID,
        "operation", calcTask.Operation,
        "operands", calcTask.Operands,
        "trace_id", tc.TraceID,
    )
    // ...
})
```

关键要点：
- `Broker.Publish(ctx, task)` 自动从 `ctx` 提取 trace 并封装到 envelope，调用方无需手动处理
- `task.Metadata` 仅用于业务级别的 KV 元数据，不应用于 trace 传播
- `Consumer` 通过 `unwrapEnvelope` 和 `injectTraceFromMetadata` 恢复 trace context
- Worker 中的 `logger.InfoContext(ctx, ...)` 会自动包含 `trace_id` 和 `span_id`

---

## 5. 使用 AK/SK 对 API 请求签名（基于 HTTP Client，以 top 为例）

### 5.1 Signer 实现

**文件**: `nsp-platform/auth/aksk.go:87-142`
```go
type Signer struct {
    accessKey string
    secretKey string
}

func NewSigner(ak, sk string) *Signer {
    return &Signer{
        accessKey: ak,
        secretKey: sk,
    }
}

func (s *Signer) Sign(req *http.Request) error {
    // Step 1: Set timestamp
    timestamp := time.Now().Unix()
    req.Header.Set(HeaderTimestamp, strconv.FormatInt(timestamp, 10))

    // Step 2: Generate and set nonce (16 bytes = 32 hex chars)
    nonce, err := generateNonce(16)
    if err != nil {
        return fmt.Errorf("failed to generate nonce: %w", err)
    }
    req.Header.Set(HeaderNonce, nonce)

    // Step 3: Read and hash body
    bodyHash, err := hashRequestBody(req)
    if err != nil {
        return fmt.Errorf("failed to hash request body: %w", err)
    }

    // Step 4: Determine and set signed headers
    signedHeaders := DefaultSignedHeaders
    req.Header.Set(HeaderSignedHeaders, signedHeaders)

    // Step 5: Build StringToSign
    stringToSign := buildStringToSign(req, signedHeaders, bodyHash)

    // Step 6: Compute signature
    signature := computeHMACSHA256(s.secretKey, stringToSign)

    // Step 7: Set Authorization header
    authHeader := fmt.Sprintf("%s AK=%s, Signature=%s", AuthScheme, s.accessKey, signature)
    req.Header.Set(HeaderAuthorization, authHeader)

    return nil
}
```

### 5.2 Top 服务签名调用示例

**文件**: `example/cmd/top/main.go:78-113`
```go
func makeQueryHandler(cfg *config.Config) gin.HandlerFunc {
    client := trace.NewTracedClient(nil)
    signer := auth.NewSigner(cfg.AccessKey, cfg.SecretKey)

    return func(c *gin.Context) {
        ctx := c.Request.Context()
        name := c.Query("name")

        url := fmt.Sprintf("%s/query?name=%s", cfg.AzURL, name)
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
        if err != nil {
            // ... error handling
            return
        }

        if err := signer.Sign(req); err != nil {
            // ... error handling
            return
        }

        resp, err := client.Do(req)
        if err != nil {
            // ... error handling
            return
        }
        defer resp.Body.Close()
        // ...
    }
}
```

签名后的请求头示例：
- `Authorization: NSP-HMAC-SHA256 AK=<access_key>, Signature=<hmac_sha256_signature>`
- `X-NSP-Timestamp: 1715000000`
- `X-NSP-Nonce: <32位hex随机字符串>`
- `X-NSP-SignedHeaders: content-type;x-nsp-nonce;x-nsp-timestamp`

---

## 6. 使用 AK/SK 对 API 请求签名（基于 Saga，以 top 为例）

### 6.1 Saga Executor 中的签名逻辑

**文件**: `nsp-platform/saga/executor.go:542-560`
```go
func (e *Executor) signRequestIfNeeded(ctx context.Context, step *Step, req *http.Request) error {
    if step == nil || step.AuthAK == "" || e.credStore == nil {
        return nil
    }

    cred, err := e.credStore.GetByAK(ctx, step.AuthAK)
    if err != nil {
        return fmt.Errorf("%w: credential lookup failed for AK %q: %v", ErrSigningFailed, step.AuthAK, err)
    }
    if cred == nil || !cred.Enabled {
        return fmt.Errorf("%w: credential not found or disabled for AK %q", ErrSigningFailed, step.AuthAK)
    }

    if err := auth.NewSigner(cred.AccessKey, cred.SecretKey).Sign(req); err != nil {
        return fmt.Errorf("%w: %v", ErrSigningFailed, err)
    }

    return nil
}
```

### 6.2 Top 服务 Saga 配置示例

**文件**: `example/cmd/top/main.go:143-220`
```go
credStore := auth.NewMemoryStore([]*auth.Credential{
    {
        AccessKey: cfg.AccessKey,
        SecretKey: cfg.SecretKey,
        Label:     "top-client",
        Enabled:   true,
    },
})

engine, err := saga.NewEngine(
    &saga.Config{
        DSN:               cfg.PostgresDSN,
        WorkerCount:       2,
        PollBatchSize:     10,
        PollScanInterval:  200 * time.Millisecond,
        CoordScanInterval: 300 * time.Millisecond,
        HTTPTimeout:       5 * time.Second,
        InstanceID:        trace.GetInstanceId(),
        CredentialStore:   credStore,  // 注入凭证存储
    })

def, err := saga.NewSaga("example-order").
    WithPayload(map[string]any{
        "sku":   req.SKU,
        "count": req.Count,
    }).
    AddStep(saga.Step{
        Name:         "reserve-inventory",
        Type:         saga.StepTypeSync,
        ActionMethod: "POST",
        ActionURL:    cfg.AzURL + "/inventory/reserve",
        ActionPayload: map[string]any{
            "sku":   "{transaction.payload.sku}",
            "count": "{transaction.payload.count}",
        },
        CompensateMethod: "POST",
        CompensateURL:    cfg.AzURL + "/inventory/release",
        CompensatePayload: map[string]any{
            "sku": "{transaction.payload.sku}",
        },
        AuthAK: cfg.AccessKey,  // 指定该步骤使用此 AK 签名
    }).
    AddStep(saga.Step{
        Name:         "create-order",
        Type:         saga.StepTypeSync,
        ActionMethod: "POST",
        ActionURL:    cfg.AzURL + "/orders/create",
        ActionPayload: map[string]any{
            "sku":   "{transaction.payload.sku}",
            "count": "{transaction.payload.count}",
        },
        CompensateMethod: "POST",
        CompensateURL:    cfg.AzURL + "/orders/cancel",
        CompensatePayload: map[string]any{
            "sku": "{transaction.payload.sku}",
        },
        AuthAK: cfg.AccessKey,  // 指定该步骤使用此 AK 签名
    }).
    Build()
```

关键要点：
- `Config.CredentialStore` 必须注入，用于查询 AK/SK
- `Step.AuthAK` 指定该步骤使用哪个 AK 进行签名
- `signRequestIfNeeded` 在执行步骤前自动完成签名
- 补偿步骤同样支持自动签名

---

## 7. 使用 Auth 对请求进行校验（以 az 代码为例）

### 7.1 Verifier 实现

**文件**: `nsp-platform/auth/aksk.go:156-255`
```go
type Verifier struct {
    store              CredentialStore
    nonces             NonceStore
    timestampTolerance time.Duration
    nonceTTL           time.Duration
}

func NewVerifier(store CredentialStore, nonces NonceStore, cfg *VerifierConfig) *Verifier {
    v := &Verifier{
        store:              store,
        nonces:             nonces,
        timestampTolerance: DefaultTimestampTolerance,
        nonceTTL:           DefaultNonceTTL,
    }
    if cfg != nil {
        if cfg.TimestampTolerance > 0 {
            v.timestampTolerance = cfg.TimestampTolerance
        }
        if cfg.NonceTTL > 0 {
            v.nonceTTL = cfg.NonceTTL
        }
    }
    return v
}

func (v *Verifier) Verify(req *http.Request) (*Credential, error) {
    ctx := req.Context()

    // Step 1: Parse Authorization header
    ak, clientSignature, err := parseAuthHeader(req.Header.Get(HeaderAuthorization))
    if err != nil {
        return nil, err
    }

    // Step 2: Verify timestamp
    timestamp, err := parseAndValidateTimestamp(req.Header.Get(HeaderTimestamp), v.timestampTolerance)
    if err != nil {
        return nil, err
    }

    // Step 3: Verify nonce
    nonce := req.Header.Get(HeaderNonce)
    if nonce == "" {
        return nil, ErrMissingNonce
    }
    used, err := v.nonces.CheckAndStore(ctx, nonce, v.nonceTTL)
    if err != nil {
        return nil, fmt.Errorf("nonce check failed: %w", err)
    }
    if used {
        return nil, ErrNonceReused
    }

    // Step 4: Look up credential
    cred, err := v.store.GetByAK(ctx, ak)
    if err != nil {
        return nil, fmt.Errorf("credential lookup failed: %w", err)
    }
    if cred == nil || !cred.Enabled {
        return nil, ErrAKNotFound
    }

    // Step 5: Read and hash body
    bodyHash, err := hashRequestBody(req)
    if err != nil {
        return nil, fmt.Errorf("failed to hash request body: %w", err)
    }

    // Step 6: Build StringToSign and compute expected signature
    signedHeaders := req.Header.Get(HeaderSignedHeaders)
    if signedHeaders == "" {
        signedHeaders = DefaultSignedHeaders
    }
    stringToSign := buildStringToSign(req, signedHeaders, bodyHash)
    expectedSignature := computeHMACSHA256(cred.SecretKey, stringToSign)

    // Step 7: Compare signatures (constant-time to prevent timing attacks)
    if !hmac.Equal([]byte(clientSignature), []byte(expectedSignature)) {
        return nil, ErrSignatureMismatch
    }

    return cred, nil
}
```

### 7.2 Gin 中间件实现

**文件**: `nsp-platform/auth/middleware.go:28-62`
```go
func AKSKAuthMiddleware(verifier *Verifier, opt *MiddlewareOption) gin.HandlerFunc {
    return func(c *gin.Context) {
        // Check if authentication should be skipped
        if opt != nil && opt.Skipper != nil && opt.Skipper(c) {
            c.Next()
            return
        }

        // Verify the request
        cred, err := verifier.Verify(c.Request)
        if err != nil {
            // Authentication failed
            if opt != nil && opt.OnAuthFailed != nil {
                opt.OnAuthFailed(c, err)
            } else {
                defaultAuthFailedHandler(c, err)
            }
            c.Abort()
            return
        }

        // Authentication succeeded
        // Store credential in gin.Context
        c.Set(ginContextKey, cred)

        // Store credential in standard context and update request
        ctx := ContextWithCredential(c.Request.Context(), cred)
        c.Request = c.Request.WithContext(ctx)

        c.Next()
    }
}
```

### 7.3 Az 服务鉴权配置示例

**文件**: `example/cmd/az/main.go:48-65`
```go
// Auth setup: verify requests from top
credStore := auth.NewMemoryStore([]*auth.Credential{
    {
        AccessKey: cfg.AccessKey,
        SecretKey: cfg.SecretKey,
        Label:     "top-client",
        Enabled:   true,
    },
})
nonceStore := auth.NewMemoryNonceStore()
verifier := auth.NewVerifier(credStore, nonceStore, nil)

r.Use(middleware.GinRecovery())
r.Use(trace.TraceMiddleware(instanceId))
r.Use(middleware.GinLogger())
r.Use(auth.AKSKAuthMiddleware(verifier, &auth.MiddlewareOption{
    Skipper: auth.NewSkipperByPath("/health"),
}))
```

### 7.4 跳过特定路径的鉴权

**文件**: `nsp-platform/auth/middleware.go:119-131`
```go
func NewSkipperByPath(paths ...string) func(c *gin.Context) bool {
    pathSet := make(map[string]struct{}, len(paths))
    for _, p := range paths {
        pathSet[p] = struct{}{}
    }

    return func(c *gin.Context) bool {
        _, skip := pathSet[c.Request.URL.Path]
        return skip
    }
}
```

### 7.5 错误码映射

**文件**: `nsp-platform/auth/aksk.go:464-481`
```go
func ErrorToHTTPStatus(err error) int {
    switch {
    case errors.Is(err, ErrMissingAuthHeader),
        errors.Is(err, ErrInvalidAuthFormat),
        errors.Is(err, ErrMissingTimestamp),
        errors.Is(err, ErrMissingNonce):
        return http.StatusBadRequest

    case errors.Is(err, ErrTimestampExpired),
        errors.Is(err, ErrNonceReused),
        errors.Is(err, ErrAKNotFound),
        errors.Is(err, ErrSignatureMismatch):
        return http.StatusUnauthorized

    default:
        return http.StatusInternalServerError
    }
}
```

关键要点：
- `MemoryStore` 是内存凭证存储，生产环境应替换为持久化存储
- `MemoryNonceStore` 会启动后台清理 goroutine，提供 `Stop()` 方法
- 默认时间戳容忍窗口为 `5 * time.Minute`
- 默认 Nonce TTL 为 `15 * time.Minute`
- 请求体签名最大读取限制为 `10MB`
- 签名算法为 `HMAC-SHA256`
- 签名校验使用 `hmac.Equal` 进行常量时间比较，防止时序攻击

---

## 附录：请求头规范

### Trace 请求头

| Header | 说明 |
|--------|------|
| `X-B3-TraceId` | 全链路唯一标识，32位hex字符串 |
| `X-B3-SpanId` | 当前服务Span标识，16位hex字符串 |
| `X-B3-Sampled` | 采样标志，`1` 或 `0` |
| `X-Request-Id` | 兼容网关的请求ID，与 `X-B3-TraceId` 值相同 |

### Auth 请求头

| Header | 说明 |
|--------|------|
| `Authorization` | `NSP-HMAC-SHA256 AK=<ak>, Signature=<signature>` |
| `X-NSP-Timestamp` | Unix时间戳（秒） |
| `X-NSP-Nonce` | 16字节随机hex字符串（32位） |
| `X-NSP-SignedHeaders` | 参与签名的头列表，分号分隔 |

### Saga 请求头

| Header | 说明 |
|--------|------|
| `X-Saga-Transaction-Id` | Saga事务ID |
| `X-Idempotency-Key` | 幂等键（步骤ID） |
