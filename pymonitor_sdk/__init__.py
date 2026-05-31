"""
pymonitor — lightweight monitoring SDK for FastAPI + ARQ

Quick start:
    import pymonitor
    pymonitor.configure("my-collector-host", port=9000)
"""

from pymonitor_sdk.transport import configure, enqueue, shutdown, start
from pymonitor_sdk.models import EventType, Event, metric_event

__all__ = ["configure", "enqueue", "shutdown", "start", "EventType", "Event", "metric_event"]
