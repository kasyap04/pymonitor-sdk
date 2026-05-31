from __future__ import annotations
import time
import traceback
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any


class EventType(str, Enum):
    EXCEPTION = "exception"
    LOG = "log"
    METRIC = "metric"
    JOB = "job"


@dataclass
class Event:
    event_type: EventType
    service: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        return d


def exception_event(service: str, exc: BaseException, context: dict | None = None) -> Event:
    return Event(
        event_type=EventType.EXCEPTION,
        service=service,
        payload={
            "exc_type": type(exc).__name__,
            "exc_message": str(exc),
            "traceback": traceback.format_exc(),
            **(context or {}),
        },
    )


def metric_event(service: str, cpu_percent: float, mem_mb: float, context: dict | None = None) -> Event:
    return Event(
        event_type=EventType.METRIC,
        service=service,
        payload={
            "cpu_percent": cpu_percent,
            "mem_mb": mem_mb,
            **(context or {}),
        },
    )


def log_event(service: str, level: str, message: str, context: dict | None = None) -> Event:
    return Event(
        event_type=EventType.LOG,
        service=service,
        payload={
            "level": level,
            "message": message,
            **(context or {}),
        },
    )


def job_event(service: str, job_name: str, status: str, duration_ms: float,
              cpu_percent: float = 0.0, mem_mb: float = 0.0,
              error: str | None = None) -> Event:
    return Event(
        event_type=EventType.JOB,
        service=service,
        payload={
            "job_name": job_name,
            "status": status,           # "started" | "success" | "failed" | "aborted"
            "duration_ms": duration_ms,
            "cpu_percent": cpu_percent,
            "mem_mb": mem_mb,
            "error": error,
        },
    )
