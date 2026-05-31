import asyncio
import struct
from unittest.mock import patch, AsyncMock, MagicMock

import msgpack
import pytest

import pymonitor_sdk.transport as transport
from pymonitor_sdk.models import Event, EventType


@pytest.fixture(autouse=True)
def reset_transport_state():
    """Reset module-level state between tests."""
    transport._queue = None
    transport._writer_task = None
    transport._host = "localhost"
    transport._port = 9000
    transport._batch_size = 100
    transport._flush_interval = 1.0
    transport._queue_maxsize = 10_000
    transport._heartbeat_interval = 30.0
    yield
    transport._queue = None
    transport._writer_task = None


class TestConfigure:
    def test_defaults(self):
        transport.py_minitor_configure("collector.example.com")
        assert transport._host == "collector.example.com"
        assert transport._port == 9000
        assert transport._batch_size == 100
        assert transport._flush_interval == 1.0
        assert transport._queue_maxsize == 10_000
        assert transport._heartbeat_interval == 30.0

    def test_custom_values(self):
        transport.py_minitor_configure(
            "10.0.0.1",
            port=8080,
            batch_size=50,
            flush_interval=0.5,
            queue_maxsize=5000,
            heartbeat_interval=15.0,
        )
        assert transport._host == "10.0.0.1"
        assert transport._port == 8080
        assert transport._batch_size == 50
        assert transport._flush_interval == 0.5
        assert transport._queue_maxsize == 5000
        assert transport._heartbeat_interval == 15.0


class TestGetQueue:
    def test_creates_queue(self):
        q = transport._get_queue()
        assert isinstance(q, asyncio.Queue)
        assert q.maxsize == 10_000

    def test_reuses_queue(self):
        q1 = transport._get_queue()
        q2 = transport._get_queue()
        assert q1 is q2

    def test_respects_maxsize(self):
        transport._queue_maxsize = 5
        q = transport._get_queue()
        assert q.maxsize == 5


class TestPackFrame:
    def test_basic(self):
        batch = [{"event_type": "log", "service": "svc"}]
        frame = transport._pack_frame(batch)
        body = msgpack.packb(batch, use_bin_type=True)
        expected = struct.pack("!I", len(body)) + body
        assert frame == expected

    def test_header_length(self):
        batch = [{"a": 1}, {"b": 2}]
        frame = transport._pack_frame(batch)
        length = struct.unpack("!I", frame[:4])[0]
        assert length == len(frame) - 4

    def test_empty_batch(self):
        frame = transport._pack_frame([])
        body = msgpack.packb([], use_bin_type=True)
        length = struct.unpack("!I", frame[:4])[0]
        assert length == len(body)


class TestEnqueue:
    @pytest.mark.asyncio
    async def test_enqueues_event(self):
        transport.py_minitor_configure("localhost")
        evt = Event(event_type=EventType.LOG, service="svc", payload={"msg": "hi"})

        with patch.object(transport, "_ensure_writer"):
            await transport.enqueue(evt)

        q = transport._get_queue()
        assert not q.empty()
        item = q.get_nowait()
        assert item["event_type"] == "log"
        assert item["service"] == "svc"

    @pytest.mark.asyncio
    async def test_drops_when_queue_full(self):
        transport._queue_maxsize = 1
        transport._queue = None  # force recreation
        q = transport._get_queue()
        # Fill the queue
        q.put_nowait({"dummy": True})

        evt = Event(event_type=EventType.LOG, service="svc", payload={})
        with patch.object(transport, "_ensure_writer"):
            await transport.enqueue(evt)  # should not raise

        assert q.qsize() == 1  # still only the original item


class TestEnsureWriter:
    @pytest.mark.asyncio
    async def test_creates_writer_task(self):
        transport._writer_task = None
        with patch.object(transport, "test_loop", new_callable=lambda: AsyncMock):
            transport._ensure_writer()
            assert transport._writer_task is not None

    @pytest.mark.asyncio
    async def test_does_not_recreate_running_task(self):
        transport._writer_task = None
        with patch.object(transport, "test_loop", new_callable=lambda: AsyncMock):
            transport._ensure_writer()
            task1 = transport._writer_task
            transport._ensure_writer()
            task2 = transport._writer_task
            assert task1 is task2

    def test_no_loop_no_crash(self):
        """If no event loop is running, _ensure_writer should not raise."""
        # This test runs without an async context
        transport._ensure_writer()
        assert transport._writer_task is None


class TestShutdown:
    @pytest.mark.asyncio
    async def test_shutdown_no_task(self):
        transport._writer_task = None
        await transport.py_monitor_shutdown()  # should not raise

    @pytest.mark.asyncio
    async def test_shutdown_cancels_task(self):
        async def fake_loop():
            await asyncio.sleep(100)

        transport._writer_task = asyncio.get_running_loop().create_task(fake_loop())
        await transport.py_monitor_shutdown()
        assert transport._writer_task is None


class TestHeartbeatFrame:
    def test_heartbeat_frame_is_zero_length(self):
        length = struct.unpack("!I", transport._HEARTBEAT_FRAME)[0]
        assert length == 0


class TestSendLoop:
    @pytest.mark.asyncio
    async def test_sends_batch(self):
        transport._flush_interval = 0.05
        transport._batch_size = 2

        q = transport._get_queue()
        q.put_nowait({"event_type": "log", "a": 1})
        q.put_nowait({"event_type": "log", "b": 2})

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        task = asyncio.get_running_loop().create_task(transport._send_loop(writer))
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert writer.write.called
        assert writer.drain.called


class TestHeartbeatLoop:
    @pytest.mark.asyncio
    async def test_sends_heartbeat(self):
        transport._heartbeat_interval = 0.05

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        task = asyncio.get_running_loop().create_task(transport._heartbeat_loop(writer))
        await asyncio.sleep(0.12)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert writer.write.called
        writer.write.assert_called_with(transport._HEARTBEAT_FRAME)


class TestFlushRemaining:
    @pytest.mark.asyncio
    async def test_flushes_queue(self):
        q = transport._get_queue()
        q.put_nowait({"event_type": "metric", "x": 1})
        q.put_nowait({"event_type": "metric", "x": 2})

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        await transport._flush_remaining(writer)

        assert writer.write.called
        assert q.empty()

    @pytest.mark.asyncio
    async def test_empty_queue_no_write(self):
        transport._get_queue()  # ensure queue exists, empty

        writer = MagicMock()
        writer.write = MagicMock()
        writer.drain = AsyncMock()

        await transport._flush_remaining(writer)

        writer.write.assert_not_called()


class TestWriterLoop:
    @pytest.mark.asyncio
    async def test_reconnect_on_connection_refused(self):
        """Writer loop retries on ConnectionRefusedError."""
        transport._host = "localhost"
        transport._port = 19999  # unlikely to be open
        transport._BACKOFF_MIN = 0.01

        task = asyncio.get_running_loop().create_task(transport._writer_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
