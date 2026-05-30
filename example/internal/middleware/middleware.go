package middleware

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jinleili-zz/nsp-platform/logger"
)

// GinRecovery recovers from panics and logs the error.
func GinRecovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.ErrorContext(c.Request.Context(), "panic recovered",
					logger.FieldError, err,
					"stacktrace", string(debug.Stack()),
				)
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"code":     500,
					"message":  "Internal Server Error",
					"trace_id": logger.TraceIDFromContext(c.Request.Context()),
				})
			}
		}()
		c.Next()
	}
}

// GinLogger logs HTTP requests with trace context.
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
