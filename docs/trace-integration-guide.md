# 链路追踪接入指南

## 概述

本服务通过 HTTP 请求头传递链路追踪 ID，用于串联上下游调用链路，支持日志检索和问题排查。

## 请求头

| 请求头 | 用途 | 必传 |
|--------|------|------|
| `X-Request-Id` | 链路追踪 ID | 推荐 |

## 格式要求

- 长度：固定 **32 个字符**
- 字符集：仅允许十六进制字符（`0-9`、`a-f`、`A-F`）
- 不包含横线、空格或其他特殊字符

> 这与 B3、W3C Trace Context 等主流分布式追踪协议的 TraceID 格式一致。

## 生成方式

如果使用 UUID 生成器，去掉横线即可：

```
UUID:       550e8400-e29b-41d4-a716-446655440000  (36字符，不可直接使用)
TraceID:    550e8400e29b41d4a716446655440000      (32字符，正确格式)
```

各语言示例：

```go
// Go
import "github.com/google/uuid"

traceID := strings.ReplaceAll(uuid.New().String(), "-", "")
```

```java
// Java
String traceID = UUID.randomUUID().toString().replace("-", "");
```

```python
# Python
import uuid

trace_id = uuid.uuid4().hex
```

```javascript
// Node.js
const crypto = require('crypto');

const traceID = crypto.randomBytes(16).toString('hex');
```

## 调用示例

```http
GET /api/orders/12345 HTTP/1.1
Host: order-service.internal
X-Request-Id: 550e8400e29b41d4a716446655440000
```

```bash
# cURL
curl -H "X-Request-Id: 550e8400e29b41d4a716446655440000" \
  https://order-service.internal/api/orders/12345
```

## 注意事项

1. **格式不合法时链路会断开** — 如果传入的值不符合 32 位 hex 要求，系统会自动生成新的 TraceID，导致上下游无法通过同一个 ID 串联日志
2. **同一请求链路保持一致** — 上游生成 TraceID 后，后续所有内部调用应透传相同的值
3. **未传时自动生成** — 如果请求中未携带该头，系统作为入口节点自动生成新 TraceID
