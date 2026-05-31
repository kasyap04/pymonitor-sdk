"""
pymonitor — lightweight monitoring SDK for FastAPI + ARQ

Quick start:
    import pymonitor
    pymonitor.configure("my-collector-host", port=9000)
"""
# from pym.transport import configure, enqueue, shutdown
from pymonitor_sdk.transport import configure, enqueue, shutdown
from pymonitor_sdk.models import EventType, Event

__all__ = ["configure", "enqueue", "shutdown", "EventType", "Event"]
