package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/hibiken/asynq"
	"github.com/jinleili-zz/nsp-platform/logger"
	"github.com/jinleili-zz/nsp-platform/taskqueue"
	"github.com/jinleili-zz/nsp-platform/taskqueue/asynqbroker"
	"github.com/jinleili-zz/nsp-platform/trace"

	"github.com/jinleili-zz/nsp-platform/example/internal/config"
	"github.com/jinleili-zz/nsp-platform/example/internal/types"
)

const (
	queueCalcIncoming = "example:calc:incoming"
	queueCalcOutgoing = "example:calc:outgoing"
	taskTypeCalc      = "example:calc"
)

func main() {
	cfg := config.Load()

	if err := logger.Init(logger.DevelopmentConfig("worker")); err != nil {
		panic(err)
	}
	defer logger.Sync()

	runtimeLog := logger.Platform().With("service", "worker")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		runtimeLog.Info("received shutdown signal")
		cancel()
	}()

	opt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPass}

	// Broker for sending replies
	broker := asynqbroker.NewBrokerWithConfig(opt, asynqbroker.BrokerConfig{Logger: runtimeLog})
	defer broker.Close()

	// Consumer for processing tasks
	consumer := asynqbroker.NewConsumer(opt, asynqbroker.ConsumerConfig{
		Concurrency: 2,
		Queues: map[string]int{
			queueCalcIncoming: 1,
		},
		RuntimeLogger: runtimeLog,
	})

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

		result := calculate(&calcTask)

		// Send reply if ReplySpec is present
		if t.Reply != nil && t.Reply.Queue != "" {
			replyPayload, err := json.Marshal(result)
			if err != nil {
				logger.ErrorContext(ctx, "failed to marshal reply payload", logger.FieldError, err)
				return err
			}

			replyTask := &taskqueue.Task{
				Type:    taskTypeCalc,
				Payload: replyPayload,
				Queue:   t.Reply.Queue,
			}

			if _, err := broker.Publish(ctx, replyTask); err != nil {
				logger.ErrorContext(ctx, "failed to send reply task", logger.FieldError, err)
				return err
			}

			logger.InfoContext(ctx, "sent reply task",
				logger.FieldTaskID, result.TaskID,
				"result", result.Result,
				logger.FieldQueue, t.Reply.Queue,
			)
		}

		return nil
	})

	runtimeLog.Info("worker started", logger.FieldQueue, queueCalcIncoming)
	if err := consumer.Start(ctx); err != nil {
		runtimeLog.Fatal("worker stopped with error", logger.FieldError, err)
	}
}

func calculate(t *types.CalcTask) *types.CalcResult {
	res := &types.CalcResult{TaskID: t.TaskID}

	if len(t.Operands) < 2 {
		res.Error = "need at least 2 operands"
		return res
	}

	a, b := t.Operands[0], t.Operands[1]
	switch t.Operation {
	case "add":
		res.Result = a + b
	case "subtract":
		res.Result = a - b
	case "multiply":
		res.Result = a * b
	case "divide":
		if b == 0 {
			res.Error = "division by zero"
		} else {
			res.Result = a / b
		}
	default:
		res.Error = fmt.Sprintf("unknown operation: %s", t.Operation)
	}

	return res
}
