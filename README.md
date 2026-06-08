# pymonitor-sdk

Lightweight monitoring SDK for FastAPI applications. Captures per-request CPU, memory, duration, and unhandled exceptions, then ships them over a persistent TCP connection to a [pymonitor-collector](https://github.com/yourorg/pymonitor-collector) instance.

## Installation

```bash
pip install pymonitor-sdk
```

## Quick Start

```python
from fastapi import FastAPI
from contextlib import asynccontextmanager

from pymonitor_sdk import py_minitor_configure, py_monitor_start, py_monitor_shutdown
from pymonitor_sdk.fastapi import PyMonitorMiddleware

# 1. Configure the transport (call once at module level)
py_minitor_configure(
    host="collector.example.com",
    port=9000,
    service="my-api",
)

# 2. Start/stop the background writer with a lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    await py_monitor_start()
    yield
    await py_monitor_shutdown()

app = FastAPI(lifespan=lifespan)

# 3. Add the middleware
app.add_middleware(PyMonitorMiddleware, service="my-api")
```

That's it. Every request will now emit a metric event (CPU %, memory MB, duration ms) or an exception event if an unhandled error occurs.

## Configuration

`py_minitor_configure()` accepts the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | `str` | `"localhost"` | Collector server hostname or IP |
| `port` | `int` | `9000` | Collector TCP port |
| `service` | `str` | `"app"` | Service name included in every event (keep unique per service) |
| `batch_size` | `int` | `100` | Max events per TCP frame |
| `flush_interval` | `float` | `1.0` | Max seconds to hold a partial batch before flushing |
| `queue_maxsize` | `int` | `10_000` | In-process event buffer cap (events beyond this are dropped) |
| `heartbeat_interval` | `float` | `30.0` | Seconds between TCP keepalive frames |
| `poll_interval` | `float` | `5.0` | Background CPU/memory sampling interval in seconds (0 = disabled) |

## Middleware

`PyMonitorMiddleware` is a Starlette `BaseHTTPMiddleware` that captures:

- **Request duration** (ms)
- **Process CPU usage** (%)
- **Process memory** (RSS in MB)
- **HTTP method and path**

On unhandled exceptions, it emits an exception event with the traceback and request context, then re-raises the exception so normal error handling still applies.

```python
app.add_middleware(PyMonitorMiddleware, service="my-api")
```

The `service` parameter on the middleware controls the service name attached to request-level events.

## Event Types

The SDK defines four event types:

| Type | Factory Function | Description |
|------|-----------------|-------------|
| `metric` | `metric_event(service, cpu_percent, mem_mb, context)` | CPU and memory metrics |
| `exception` | `exception_event(service, exc, context)` | Captured exceptions with traceback |
| `log` | `log_event(service, level, message, context)` | Structured log entries |
| `job` | `job_event(service, job_name, status, duration_ms, ...)` | Background job tracking |

## Sending Custom Events

You can enqueue events manually anywhere in your application:

```python
from pymonitor_sdk import enqueue
from pymonitor_sdk.models import log_event, job_event

# Log event
await enqueue(log_event("my-api", "warning", "Cache miss rate above threshold"))

# Job event
await enqueue(job_event(
    service="my-api",
    job_name="sync_users",
    status="success",
    duration_ms=1520.3,
    cpu_percent=12.5,
    mem_mb=256.0,
))
```

## Background Polling

When `poll_interval > 0` (default: 5s), the SDK periodically samples process CPU and memory usage independently of incoming requests. This is useful for tracking resource consumption during background tasks or idle periods.

## Transport Details

- Single persistent TCP connection with automatic reconnect and exponential backoff
- Wire format: `[4-byte big-endian length][msgpack payload]`
- Zero-length frames serve as heartbeats to detect dead connections
- Events are batched for efficiency (configurable via `batch_size` and `flush_interval`)

## Requirements

- Python >= 3.11
- FastAPI >= 0.110
- msgpack >= 1.0
- psutil >= 5.9
