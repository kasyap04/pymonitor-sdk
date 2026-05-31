import time
from pymonitor_sdk.models import (
    EventType,
    Event,
    exception_event,
    metric_event,
    log_event,
    job_event,
)


class TestEventType:
    def test_enum_values(self):
        assert EventType.EXCEPTION == "exception"
        assert EventType.LOG == "log"
        assert EventType.METRIC == "metric"
        assert EventType.JOB == "job"

    def test_enum_is_str(self):
        assert isinstance(EventType.EXCEPTION, str)


class TestEvent:
    def test_basic_creation(self):
        evt = Event(
            event_type=EventType.LOG,
            service="test-svc",
            payload={"msg": "hello"},
        )
        assert evt.event_type == EventType.LOG
        assert evt.service == "test-svc"
        assert evt.payload == {"msg": "hello"}
        assert isinstance(evt.timestamp, float)

    def test_timestamp_default(self):
        before = time.time()
        evt = Event(event_type=EventType.LOG, service="s", payload={})
        after = time.time()
        assert before <= evt.timestamp <= after

    def test_to_dict(self):
        evt = Event(
            event_type=EventType.METRIC,
            service="api",
            payload={"cpu": 50.0},
            timestamp=1000.0,
        )
        d = evt.to_dict()
        assert d["event_type"] == "metric"
        assert d["service"] == "api"
        assert d["payload"] == {"cpu": 50.0}
        assert d["timestamp"] == 1000.0


class TestExceptionEvent:
    def test_basic(self):
        try:
            raise ValueError("boom")
        except ValueError as exc:
            evt = exception_event("my-svc", exc)

        assert evt.event_type == EventType.EXCEPTION
        assert evt.service == "my-svc"
        assert evt.payload["exc_type"] == "ValueError"
        assert evt.payload["exc_message"] == "boom"
        assert "traceback" in evt.payload

    def test_with_context(self):
        try:
            raise RuntimeError("fail")
        except RuntimeError as exc:
            evt = exception_event("svc", exc, context={"path": "/foo"})

        assert evt.payload["path"] == "/foo"

    def test_without_context(self):
        try:
            raise TypeError("oops")
        except TypeError as exc:
            evt = exception_event("svc", exc, context=None)

        assert "path" not in evt.payload


class TestMetricEvent:
    def test_basic(self):
        evt = metric_event("api", cpu_percent=42.5, mem_mb=128.0)
        assert evt.event_type == EventType.METRIC
        assert evt.service == "api"
        assert evt.payload["cpu_percent"] == 42.5
        assert evt.payload["mem_mb"] == 128.0

    def test_with_context(self):
        evt = metric_event("api", 10.0, 64.0, context={"path": "/health"})
        assert evt.payload["path"] == "/health"

    def test_without_context(self):
        evt = metric_event("api", 10.0, 64.0, context=None)
        assert "path" not in evt.payload


class TestLogEvent:
    def test_basic(self):
        evt = log_event("svc", level="INFO", message="started")
        assert evt.event_type == EventType.LOG
        assert evt.payload["level"] == "INFO"
        assert evt.payload["message"] == "started"

    def test_with_context(self):
        evt = log_event("svc", "ERROR", "bad", context={"request_id": "abc"})
        assert evt.payload["request_id"] == "abc"

    def test_without_context(self):
        evt = log_event("svc", "DEBUG", "trace", context=None)
        assert "request_id" not in evt.payload


class TestJobEvent:
    def test_basic(self):
        evt = job_event("worker", "send_email", "success", 120.5)
        assert evt.event_type == EventType.JOB
        assert evt.service == "worker"
        assert evt.payload["job_name"] == "send_email"
        assert evt.payload["status"] == "success"
        assert evt.payload["duration_ms"] == 120.5
        assert evt.payload["cpu_percent"] == 0.0
        assert evt.payload["mem_mb"] == 0.0
        assert evt.payload["error"] is None

    def test_with_error(self):
        evt = job_event("worker", "task", "failed", 50.0, error="timeout")
        assert evt.payload["error"] == "timeout"

    def test_with_resources(self):
        evt = job_event("worker", "task", "success", 200.0, cpu_percent=80.0, mem_mb=512.0)
        assert evt.payload["cpu_percent"] == 80.0
        assert evt.payload["mem_mb"] == 512.0
