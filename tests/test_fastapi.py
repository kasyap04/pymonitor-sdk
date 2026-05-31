import asyncio
from unittest.mock import patch, AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pymonitor_sdk.fastapi import PyMonitorMiddleware, install


@pytest.fixture
def app():
    app = FastAPI()

    @app.get("/ok")
    async def ok():
        return {"status": "ok"}

    @app.get("/error")
    async def error():
        raise ValueError("test error")

    return app


class TestPyMonitorMiddleware:
    def test_successful_request_enqueues_metric(self, app):
        app.add_middleware(PyMonitorMiddleware, service="test-api")

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/ok")

        assert resp.status_code == 200
        assert mock_enqueue.called
        evt = mock_enqueue.call_args[0][0]
        assert evt.event_type.value == "metric"
        assert evt.service == "test-api"
        assert "cpu_percent" in evt.payload
        assert "mem_mb" in evt.payload

    def test_failed_request_enqueues_exception(self, app):
        app.add_middleware(PyMonitorMiddleware, service="test-api")

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.get("/error")

        assert resp.status_code == 500
        assert mock_enqueue.called
        evt = mock_enqueue.call_args[0][0]
        assert evt.event_type.value == "exception"
        assert evt.payload["exc_type"] == "ValueError"
        assert evt.payload["exc_message"] == "test error"

    def test_context_includes_method_and_path(self, app):
        app.add_middleware(PyMonitorMiddleware, service="svc")

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            client = TestClient(app, raise_server_exceptions=False)
            client.get("/ok")

        evt = mock_enqueue.call_args[0][0]
        assert evt.payload["method"] == "GET"
        assert evt.payload["path"] == "/ok"
        assert "duration_ms" in evt.payload

    def test_default_service_name(self, app):
        app.add_middleware(PyMonitorMiddleware)

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            client = TestClient(app, raise_server_exceptions=False)
            client.get("/ok")

        evt = mock_enqueue.call_args[0][0]
        assert evt.service == "fastapi"


class TestInstall:
    def test_install_adds_middleware_and_shutdown(self):
        app = FastAPI()

        @app.get("/ping")
        async def ping():
            return "pong"

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            install(app, service="installed-svc")
            client = TestClient(app, raise_server_exceptions=False)
            client.get("/ping")

        assert mock_enqueue.called
        evt = mock_enqueue.call_args[0][0]
        assert evt.service == "installed-svc"

    def test_install_default_service(self):
        app = FastAPI()

        @app.get("/x")
        async def x():
            return "x"

        with patch("pymonitor_sdk.enqueue", new_callable=lambda: AsyncMock) as mock_enqueue:
            install(app)
            client = TestClient(app, raise_server_exceptions=False)
            client.get("/x")

        evt = mock_enqueue.call_args[0][0]
        assert evt.service == "fastapi"
