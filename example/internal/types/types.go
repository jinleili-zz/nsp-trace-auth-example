package types

// CalcTask represents a calculation request sent from az to worker.
type CalcTask struct {
	TaskID    string    `json:"task_id"`
	Operation string    `json:"operation"`
	Operands  []float64 `json:"operands"`
}

// CalcResult represents a calculation reply sent from worker back to az.
type CalcResult struct {
	TaskID string  `json:"task_id"`
	Result float64 `json:"result"`
	Error  string  `json:"error,omitempty"`
}

// OrderRequest represents a user order creation request.
type OrderRequest struct {
	SKU   string `json:"sku"`
	Count int    `json:"count"`
}

// OrderResponse represents the result of an order creation.
type OrderResponse struct {
	OrderID string  `json:"order_id"`
	SKU     string  `json:"sku"`
	Count   int     `json:"count"`
	Total   float64 `json:"total"`
	Status  string  `json:"status"`
}

// QueryResponse represents the response for a simple query.
type QueryResponse struct {
	Status    string `json:"status"`
	Name      string `json:"name"`
	Timestamp string `json:"timestamp"`
}

// APIResponse is a generic wrapper for HTTP responses.
type APIResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	TraceID string      `json:"trace_id,omitempty"`
}
