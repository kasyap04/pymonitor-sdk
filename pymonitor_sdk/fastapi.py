from __future__ import annotations
import time
import asyncio
import psutil
import os

from fastapi import FastAPI, Request
from starlette.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware

import pymonitor_sdk
from pymonitor_sdk.models import exception_event, metric_event


_proc = psutil.Process(os.getpid())
print(f"{_proc   = }")


class PyMonitorMiddleware(BaseHTTPMiddleware):
    """
    Per-request CPU, memory, and duration capture.
    Unhandled exceptions are captured before propagating.

    Add via install() or manually:
        app.add_middleware(PyMonitorMiddleware, service="my-api")
    """

    def __init__(self, app, service: str = "fastapi"):
        super().__init__(app)
        self.service = service

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.perf_counter()
        # _proc.cpu_percent(interval=None)  # prime the measurement


        exc_to_report: BaseException | None = None
        try:
            response = await call_next(request)
        except Exception as exc:
            exc_to_report = exc
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            cpu = _proc.cpu_percent(interval=None)

            mem_mb = _proc.memory_info().rss / 1024 / 1024
            ctx = {
                "method": request.method,
                "path": request.url.path,
                "duration_ms": round(duration_ms, 2),
            }
            if exc_to_report is not None:
                asyncio.ensure_future(
                    pymonitor_sdk.enqueue(exception_event(self.service, exc_to_report, ctx))
                )
            else:
                asyncio.ensure_future(
                    pymonitor_sdk.enqueue(metric_event(self.service, cpu, mem_mb, ctx))
                )


        return response


def install(app: FastAPI, service: str = "fastapi") -> None:
    """
    Wire up monitoring on a FastAPI app with one call.

    Adds:
    - PyMonitorMiddleware  — per-request CPU/mem/duration + exception capture
    - shutdown handler   — flushes remaining events before process exits

    Usage:
        import pymonitor
        from pymonitor.integrations.fastapi import install

        pymonitor.configure("collector-host", port=9000)

        app = FastAPI()
        install(app, service="my-api")
    """
    app.add_middleware(PyMonitorMiddleware, service=service)

    @app.on_event("shutdown")
    async def _flush_on_shutdown() -> None:
        await pymonitor_sdk.shutdown()
