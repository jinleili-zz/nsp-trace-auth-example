package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jinleili-zz/nsp-platform/logger"
)

// Health handles health check requests.
func Health(c *gin.Context) {
	ctx := c.Request.Context()
	logger.InfoContext(ctx, "health check")

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "ok",
		"trace_id": logger.TraceIDFromContext(ctx),
	})
}
