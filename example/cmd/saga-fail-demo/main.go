// saga-fail-demo demonstrates how the saga engine surfaces sub-transaction
// business errors (HTTP 200 with code != "0") back to the saga creator.
//
// Two scenarios are exposed:
//
//   POST /api/fail-first  - single-step saga whose action always returns
//                           {code:"VALIDATION_ERROR", message:"..."}.
//                           Verifies that the failing step's LastError carries
//                           the full envelope and that the transaction is
//                           marked failed without invoking compensation.
//
//   POST /api/fail-middle - two-step saga: step 1 succeeds, step 2 returns
//                           {code:"BUSINESS_FAIL", message:"..."}.
//                           Verifies that step 1 is compensated (its cancel
//                           endpoint is invoked), step 2's LastError carries
//                           the business error envelope, and the transaction
//                           terminates as failed.
//
// The same binary also hosts the sub-transaction HTTP endpoints under /demo/*.
// Those endpoints are protected by AK/SK auth (NSP-HMAC-SHA256) — the saga
// executor signs outbound requests via Step.AuthAK and the corresponding
// verifier rejects unsigned calls.
//
// Run locally:
//
//	POSTGRES_DSN=postgres://nsp:nsp123@localhost:5432/nsp?sslmode=disable \
//	  go run ./cmd/saga-fail-demo
//	# then in another shell:
//	curl -X POST http://localhost:8082/api/fail-first  | jq .
//	curl -X POST http://localhost:8082/api/fail-middle | jq .
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinleili-zz/nsp-platform/auth"
	"github.com/jinleili-zz/nsp-platform/example/internal/config"
	"github.com/jinleili-zz/nsp-platform/example/internal/handler"
	"github.com/jinleili-zz/nsp-platform/example/internal/middleware"
	"github.com/jinleili-zz/nsp-platform/logger"
	"github.com/jinleili-zz/nsp-platform/saga"
	"github.com/jinleili-zz/nsp-platform/trace"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(logger.ResolveConfig("saga-fail-demo", cfg.LogMode)); err != nil {
		panic(err)
	}
	defer logger.Sync()

	addr := getEnv("SAGA_FAIL_DEMO_ADDR", ":8082")
	selfURL := getEnv("SAGA_FAIL_DEMO_URL", "http://localhost:8082")

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	instanceID := trace.GetInstanceId()

	// Verifier for inbound sub-transaction calls. The saga executor will sign
	// requests with the same AK/SK pair (resolved via the in-memory credential
	// store passed to the engine below).
	credStore := auth.NewMemoryStore([]*auth.Credential{
		{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			Label:     "saga-fail-demo",
			Enabled:   true,
		},
	})
	nonceStore := auth.NewMemoryNonceStore()
	defer nonceStore.Stop()
	verifier := auth.NewVerifier(credStore, nonceStore, nil)

	// Middleware order MUST match the canonical pattern documented in
	// nsp-platform/AGENTS.md: Recovery -> Trace -> Logger -> (Auth).
	r.Use(middleware.GinRecovery())
	r.Use(trace.TraceMiddleware(instanceID))
	r.Use(middleware.GinLogger())

	r.GET("/health", handler.Health)

	// User-facing saga-creator endpoints. Intentionally NOT behind AK/SK auth —
	// these are the entry points for the demo.
	r.POST("/api/fail-first", makeFailFirstHandler(cfg, selfURL))
	r.POST("/api/fail-middle", makeFailMiddleHandler(cfg, selfURL))

	// Sub-transaction endpoints. The saga executor signs every outbound call,
	// so these MUST go through AKSKAuthMiddleware. Unsigned requests are
	// rejected before reaching the handler.
	demo := r.Group("/", auth.AKSKAuthMiddleware(verifier, nil))
	{
		demo.POST("/demo/a/check", failFirstCheckHandler)

		demo.POST("/demo/b/prepare", prepareSuccessHandler)
		demo.POST("/demo/b/prepare-cancel", prepareCancelHandler)
		demo.POST("/demo/b/commit", commitFailHandler)
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		logger.Info("saga-fail-demo service starting",
			"addr", addr, "self_url", selfURL, "ak", cfg.AccessKey)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("saga-fail-demo service failed", logger.FieldError, err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("saga-fail-demo service shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("saga-fail-demo service stopped")
}

// -----------------------------------------------------------------------------
// Sub-transaction handlers (HTTP 200 + business-failure envelopes)
// -----------------------------------------------------------------------------

// failFirstCheckHandler returns HTTP 200 with a non-zero code.
// saga.parseHTTPResponseEnvelope will reject this and persist the full
// envelope string into the step's LastError.
func failFirstCheckHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "[demo/a/check] returning business-failure envelope")
	c.JSON(http.StatusOK, gin.H{
		"code":    "VALIDATION_ERROR",
		"message": "sku is invalid: must not be empty",
		"hint":    "scenario A: sub-transaction rejected the request",
	})
}

func prepareSuccessHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "[demo/b/prepare] returning success envelope")
	c.JSON(http.StatusOK, gin.H{
		"code":           "0",
		"reservation_id": "RSV-DEMO-001",
		"status":         "reserved",
	})
}

func prepareCancelHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "[demo/b/prepare-cancel] compensation executed")
	c.JSON(http.StatusOK, gin.H{
		"code":    "0",
		"message": "reservation released",
	})
}

// commitFailHandler returns HTTP 200 with code="BUSINESS_FAIL". This is the
// classic "transport succeeded but business layer rejected" failure that
// saga must surface end-to-end.
func commitFailHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "[demo/b/commit] returning business-failure envelope")
	c.JSON(http.StatusOK, gin.H{
		"code":    "BUSINESS_FAIL",
		"message": "quota exceeded for tenant",
		"hint":    "scenario B: sub-transaction rejected at commit step",
	})
}

// -----------------------------------------------------------------------------
// Saga creator handlers
// -----------------------------------------------------------------------------

func makeFailFirstHandler(cfg *config.Config, selfURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		def := buildFailFirstDef(cfg, selfURL)
		runScenario(c, cfg, "scenario-A-fail-first", def)
	}
}

func makeFailMiddleHandler(cfg *config.Config, selfURL string) gin.HandlerFunc {
	return func(c *gin.Context) {
		def := buildFailMiddleDef(cfg, selfURL)
		runScenario(c, cfg, "scenario-B-fail-middle", def)
	}
}

func buildFailFirstDef(cfg *config.Config, selfURL string) *saga.SagaDefinition {
	def, err := saga.NewSaga("scenario-A-fail-first").
		WithPayload(map[string]any{
			"scenario": "A",
			"note":     "single step that fails at business layer (HTTP 200, code!=0)",
		}).
		AddStep(saga.Step{
			Name:         "validate-input",
			Type:         saga.StepTypeSync,
			ActionMethod: "POST",
			ActionURL:    selfURL + "/demo/a/check",
			ActionPayload: map[string]any{
				"scenario": "{transaction.payload.scenario}",
			},
			// CompensateMethod/URL are required by the saga builder even when
			// the step can never succeed — point them at a no-op path.
			CompensateMethod: "POST",
			CompensateURL:    selfURL + "/demo/a/check",
			AuthAK:           cfg.AccessKey,
			// NOTE: saga.SagaBuilder treats MaxRetry==0 as "not set" and defaults
			// to 3 (see nsp-platform/saga/definition.go:269). To fail-fast on the
			// first business error, set MaxRetry=1 explicitly — that means
			// "execute at most once, no retries".
			MaxRetry: 1,
		}).
		Build()
	if err != nil {
		panic(fmt.Sprintf("build saga A: %v", err))
	}
	return def
}

func buildFailMiddleDef(cfg *config.Config, selfURL string) *saga.SagaDefinition {
	def, err := saga.NewSaga("scenario-B-fail-middle").
		WithPayload(map[string]any{
			"scenario": "B",
			"note":     "step1 succeeds, step2 fails (HTTP 200, code!=0), step1 must be compensated",
		}).
		AddStep(saga.Step{
			Name:         "prepare-reservation",
			Type:         saga.StepTypeSync,
			ActionMethod: "POST",
			ActionURL:    selfURL + "/demo/b/prepare",
			ActionPayload: map[string]any{
				"scenario": "{transaction.payload.scenario}",
			},
			CompensateMethod: "POST",
			CompensateURL:    selfURL + "/demo/b/prepare-cancel",
			CompensatePayload: map[string]any{
				"reservation_id": "{step[0].action_response.reservation_id}",
			},
			AuthAK:   cfg.AccessKey,
			MaxRetry: 1, // see note in buildFailFirstDef
		}).
		AddStep(saga.Step{
			Name:         "commit-order",
			Type:         saga.StepTypeSync,
			ActionMethod: "POST",
			ActionURL:    selfURL + "/demo/b/commit",
			ActionPayload: map[string]any{
				"scenario": "{transaction.payload.scenario}",
			},
			CompensateMethod: "POST",
			CompensateURL:    selfURL + "/demo/b/commit",
			AuthAK:           cfg.AccessKey,
			MaxRetry:         1, // see note in buildFailFirstDef
		}).
		Build()
	if err != nil {
		panic(fmt.Sprintf("build saga B: %v", err))
	}
	return def
}

// runScenario submits the saga, waits for terminal status, then issues an
// explicit Engine.Query to demonstrate how saga creators read back per-step
// errors. The HTTP response includes:
//   - The terminal status from SubmitAndWait
//   - The fully-resolved LastError at both transaction and step level
//   - The same data returned by Engine.Query (proving async queries see the
//     same error after termination)
func runScenario(c *gin.Context, cfg *config.Config, scenarioName string, def *saga.SagaDefinition) {
	ctx := c.Request.Context()

	// Fresh in-memory credential store for the engine. Same AK/SK as the
	// verifier registered above; this is what makes signed sub-transaction
	// calls succeed.
	engineCreds := auth.NewMemoryStore([]*auth.Credential{
		{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
			Label:     "saga-fail-demo-engine",
			Enabled:   true,
		},
	})

	engine, err := saga.NewEngine(&saga.Config{
		DSN:               cfg.PostgresDSN,
		WorkerCount:       2,
		PollBatchSize:     10,
		PollScanInterval:  200 * time.Millisecond,
		CoordScanInterval: 300 * time.Millisecond,
		HTTPTimeout:       5 * time.Second,
		InstanceID:        trace.GetInstanceId(),
		CredentialStore:   engineCreds,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":     500,
			"message":  fmt.Sprintf("create saga engine: %v", err),
			"trace_id": logger.TraceIDFromContext(ctx),
		})
		return
	}
	defer engine.Stop()

	if err := engine.Start(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":     500,
			"message":  fmt.Sprintf("start saga engine: %v", err),
			"trace_id": logger.TraceIDFromContext(ctx),
		})
		return
	}

	logger.InfoContext(ctx, "submitting saga", "scenario", scenarioName)

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	txID, status, submitErr := engine.SubmitAndWait(waitCtx, def)

	// After termination, query the engine to demonstrate the Query API.
	// logSagaQueryResult also logs every step's LastError to stderr so the
	// operator can see exactly which sub-transaction error was persisted.
	queryResult := logSagaQueryResult(ctx, engine, txID)

	resp := gin.H{
		"scenario":         scenarioName,
		"tx_id":            txID,
		"trace_id":         logger.TraceIDFromContext(ctx),
		"error_is_failed":  errors.Is(submitErr, saga.ErrTransactionFailed),
		"saga_status":      nil,
		"saga_last_error":  nil,
		"saga_returned":    "",
		"steps":            []gin.H{},
		"query_after_term": queryResult,
	}
	if submitErr != nil {
		resp["saga_returned"] = submitErr.Error()
	}
	if status != nil {
		resp["saga_status"] = status.Status
		resp["saga_last_error"] = status.LastError
		for _, s := range status.Steps {
			resp["steps"] = append(resp["steps"].([]gin.H), gin.H{
				"index":      s.Index,
				"name":       s.Name,
				"status":     s.Status,
				"last_error": s.LastError,
				"poll_count": s.PollCount,
			})
		}
	}

	// We return HTTP 200 on purpose — the demo's job is to report what the
	// saga framework observed, not to signal a transport-level failure.
	c.JSON(http.StatusOK, resp)
}

// logSagaQueryResult mirrors the same-named function in example/cmd/top/main.go
// but additionally returns the queried state so the HTTP response can include
// it. Logging step.LastError here is the canonical way to print the exact
// error string the saga framework persists from the sub-transaction envelope.
func logSagaQueryResult(ctx context.Context, engine *saga.Engine, txID string) gin.H {
	if txID == "" {
		return gin.H{"error": "no tx_id (submit failed before persisted)"}
	}
	queryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	qs, qerr := engine.Query(queryCtx, txID)
	if qerr != nil {
		logger.ErrorContext(ctx, "[Query] failed to query saga transaction",
			"tx_id", txID, logger.FieldError, qerr)
		return gin.H{"error": qerr.Error()}
	}

	logger.InfoContext(ctx, "[Query] saga transaction status",
		"tx_id", qs.ID,
		"status", qs.Status,
		"current_step", qs.CurrentStep,
		"last_error", qs.LastError,
		"step_count", len(qs.Steps),
	)

	steps := []gin.H{}
	for _, s := range qs.Steps {
		logger.InfoContext(ctx, "[Query] saga step",
			"step_index", s.Index,
			"step_name", s.Name,
			"step_status", s.Status,
			"poll_count", s.PollCount,
			"step_last_error", s.LastError,
		)
		steps = append(steps, gin.H{
			"index":      s.Index,
			"name":       s.Name,
			"status":     s.Status,
			"last_error": s.LastError,
			"poll_count": s.PollCount,
		})
	}

	return gin.H{
		"tx_id":      qs.ID,
		"status":     qs.Status,
		"last_error": qs.LastError,
		"steps":      steps,
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
