package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jinleili-zz/nsp-platform/auth"
	"github.com/jinleili-zz/nsp-platform/logger"
	"github.com/jinleili-zz/nsp-platform/taskqueue"
	"github.com/jinleili-zz/nsp-platform/taskqueue/asynqbroker"
	"github.com/jinleili-zz/nsp-platform/trace"

	"github.com/jinleili-zz/nsp-platform/example/internal/config"
	"github.com/jinleili-zz/nsp-platform/example/internal/handler"
	"github.com/jinleili-zz/nsp-platform/example/internal/middleware"
	"github.com/jinleili-zz/nsp-platform/example/internal/types"
)

const (
	queueCalcIncoming = "example:calc:incoming"
	queueCalcOutgoing = "example:calc:outgoing"
	taskTypeCalc      = "example:calc"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(logger.DevelopmentConfig("az")); err != nil {
		panic(err)
	}
	defer logger.Sync()

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

	// Routes
	r.GET("/health", handler.Health)
	r.GET("/query", queryHandler)
	r.POST("/inventory/reserve", reserveInventoryHandler)
	r.POST("/inventory/release", releaseInventoryHandler)
	r.POST("/orders/create", makeCreateOrderHandler(cfg))
	r.POST("/orders/cancel", cancelOrderHandler)

	srv := &http.Server{
		Addr:         cfg.AzAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.Info("az service starting", "addr", cfg.AzAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("az service failed", logger.FieldError, err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("az service shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("az service stopped")
}

func queryHandler(c *gin.Context) {
	ctx := c.Request.Context()
	name := c.Query("name")

	logger.InfoContext(ctx, "handling query request", "name", name)

	c.JSON(http.StatusOK, types.QueryResponse{
		Status:    "ok",
		Name:      name,
		Timestamp: time.Now().Format(time.RFC3339),
	})
}

func reserveInventoryHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "reserving inventory")

	c.JSON(http.StatusOK, gin.H{
		"code":           "0",
		"reservation_id": fmt.Sprintf("RSV-%s", uuid.New().String()[:8]),
		"status":         "reserved",
	})
}

func releaseInventoryHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "releasing inventory")
	c.JSON(http.StatusOK, gin.H{"code": "0"})
}

func cancelOrderHandler(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "cancelling order")
	c.JSON(http.StatusOK, gin.H{"code": "0"})
}

func makeCreateOrderHandler(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		logger.InfoContext(ctx, "orders/create received request")

		var req struct {
			SKU   string      `json:"sku"`
			Count json.Number `json:"count"`
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, types.APIResponse{
				Code:    400,
				Message: err.Error(),
				TraceID: logger.TraceIDFromContext(ctx),
			})
			return
		}

		count, _ := req.Count.Int64()

		// Send calculation task to worker via taskqueue
		calcTask := &types.CalcTask{
			TaskID:    uuid.New().String(),
			Operation: "multiply",
			Operands:  []float64{float64(count), 99.99}, // mock price
		}

		payload, err := json.Marshal(calcTask)
		if err != nil {
			c.JSON(http.StatusInternalServerError, types.APIResponse{
				Code:    500,
				Message: err.Error(),
				TraceID: logger.TraceIDFromContext(ctx),
			})
			return
		}

		opt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPass}
		broker := asynqbroker.NewBrokerWithConfig(opt, asynqbroker.BrokerConfig{})
		defer broker.Close()

		task := &taskqueue.Task{
			Type:    taskTypeCalc,
			Payload: payload,
			Queue:   queueCalcIncoming,
			Reply:   &taskqueue.ReplySpec{Queue: queueCalcOutgoing},
		}

		// Set up reply consumer before publishing
		replyConsumer := asynqbroker.NewConsumer(opt, asynqbroker.ConsumerConfig{
			Concurrency: 1,
			Queues: map[string]int{
				queueCalcOutgoing: 1,
			},
		})

		var result *types.CalcResult
		var resultMu sync.Mutex
		done := make(chan struct{})

		replyConsumer.Handle(taskTypeCalc, func(ctx context.Context, t *taskqueue.Task) error {
			var r types.CalcResult
			if err := json.Unmarshal(t.Payload, &r); err != nil {
				return err
			}
			resultMu.Lock()
			result = &r
			resultMu.Unlock()
			close(done)
			return nil
		})

		replyCtx, replyCancel := context.WithCancel(context.Background())
		defer replyCancel()

		go func() {
			if err := replyConsumer.Start(replyCtx); err != nil {
				logger.Error("reply consumer stopped", logger.FieldError, err)
			}
		}()

		// Give consumer time to start
		time.Sleep(200 * time.Millisecond)

		if _, err := broker.Publish(ctx, task); err != nil {
			replyCancel()
			c.JSON(http.StatusInternalServerError, types.APIResponse{
				Code:    500,
				Message: err.Error(),
				TraceID: logger.TraceIDFromContext(ctx),
			})
			return
		}

		// Wait for reply with timeout
		select {
		case <-done:
			replyCancel()
		case <-time.After(10 * time.Second):
			replyCancel()
			c.JSON(http.StatusInternalServerError, types.APIResponse{
				Code:    500,
				Message: "timeout waiting for worker reply",
				TraceID: logger.TraceIDFromContext(ctx),
			})
			return
		}

		resultMu.Lock()
		calcResult := result
		resultMu.Unlock()

		if calcResult != nil && calcResult.Error != "" {
			c.JSON(http.StatusInternalServerError, types.APIResponse{
				Code:    500,
				Message: fmt.Sprintf("calculation failed: %s", calcResult.Error),
				TraceID: logger.TraceIDFromContext(ctx),
			})
			return
		}

		total := 0.0
		if calcResult != nil {
			total = calcResult.Result
		}

		c.JSON(http.StatusOK, gin.H{
			"code":     "0",
			"order_id": fmt.Sprintf("ORD-%s", uuid.New().String()[:8]),
			"sku":      req.SKU,
			"count":    req.Count,
			"total":    total,
			"status":   "created",
		})
	}
}
