package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinleili-zz/nsp-platform/auth"
	"github.com/jinleili-zz/nsp-platform/logger"
	"github.com/jinleili-zz/nsp-platform/saga"
	"github.com/jinleili-zz/nsp-platform/trace"

	"github.com/jinleili-zz/nsp-platform/example/internal/config"
	"github.com/jinleili-zz/nsp-platform/example/internal/handler"
	"github.com/jinleili-zz/nsp-platform/example/internal/middleware"
	"github.com/jinleili-zz/nsp-platform/example/internal/types"
)

const (
	queueCalcIncoming = "example:calc:incoming"
	queueCalcOutgoing = "example:calc:outgoing"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(logger.ResolveConfig("top", cfg.LogMode)); err != nil {
		panic(err)
	}
	defer logger.Sync()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	instanceId := trace.GetInstanceId()

	// 注册中间件（必须在路由注册之前执行，中间件按注册顺序依次生效）
	r.Use(middleware.GinRecovery())
	r.Use(trace.TraceMiddleware(instanceId))
	r.Use(middleware.GinLogger())

	// 注册路由（中间件在此之前已注册完成，所有路由都会经过上述中间件处理）
	r.GET("/health", handler.Health)
	r.GET("/api/query", makeQueryHandler(cfg))
	r.POST("/api/order", makeOrderHandler(cfg))

	srv := &http.Server{
		Addr:         cfg.TopAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.Info("top service starting", "addr", cfg.TopAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("top service failed", logger.FieldError, err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("top service shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("top service stopped")
}

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

		logger.InfoContext(ctx, "creating saga engine", "credential_store_set", credStore != nil)

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

		tc, ok := trace.TraceFromContext(waitCtx)
		logger.InfoContext(waitCtx, "submitting saga",
			"trace_context_found", ok,
			"trace_id", tc.TraceID,
			"span_id", tc.SpanId,
			"parent_span_id", tc.ParentSpanId,
			"instance_id", tc.InstanceId,
		)

		txID, status, err := engine.SubmitAndWait(waitCtx, def)
		logSagaQueryResult(ctx, engine, txID)
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

// logSagaQueryResult calls Engine.Query and prints the full status.
// Used to demonstrate how to read back saga execution results.
func logSagaQueryResult(ctx context.Context, engine *saga.Engine, txID string) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	queryStatus, queryErr := engine.Query(queryCtx, txID)
	if queryErr != nil {
		logger.ErrorContext(ctx, "[Query] failed to query saga transaction",
			"tx_id", txID,
			logger.FieldError, queryErr,
		)
		return
	}

	logger.InfoContext(ctx, "[Query] saga transaction status",
		"tx_id", queryStatus.ID,
		"status", queryStatus.Status,
		"current_step", queryStatus.CurrentStep,
		"last_error", queryStatus.LastError,
		"created_at", queryStatus.CreatedAt.Format(time.RFC3339),
		"finished_at", formatOptionalTime(queryStatus.FinishedAt),
		"step_count", len(queryStatus.Steps),
	)

	for _, step := range queryStatus.Steps {
		logger.InfoContext(ctx, "[Query] saga step",
			"step_index", step.Index,
			"step_name", step.Name,
			"step_status", step.Status,
			"poll_count", step.PollCount,
			"step_last_error", step.LastError,
		)
	}
}

func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}
