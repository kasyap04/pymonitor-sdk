"""
Persistent TCP transport for pymonitor.

Wire format (per frame):
    [ 4 bytes big-endian uint32: payload length ][ N bytes: msgpack list of event dicts ]

A payload length of 0 is a heartbeat frame — the server ignores it, but it
keeps the OS from silently killing an idle connection and lets both sides detect
a dead peer quickly without waiting for a send to fail.

One long-lived TCP connection is opened when the first event is enqueued, then
reused for every subsequent send. On any failure (broken pipe, timeout, refused)
the writer waits with exponential backoff then reconnects — events accumulate in
the in-process queue during that window and are flushed once the connection
comes back up.
"""

from __future__ import annotations

import asyncio
import logging
import struct
import psutil
import os
from typing import TYPE_CHECKING

import msgpack

if TYPE_CHECKING:
    from pymonitor_sdk.models import Event

logger = logging.getLogger("pymonitor.transport")

# ── Config (set via configure()) ───────────────────────────────────────────
_host: str = "localhost"
_port: int = 9000
_batch_size: int = 100
_flush_interval: float = 1.0  # max seconds to hold a partial batch
_queue_maxsize: int = 10_000
_heartbeat_interval: float = 30.0  # seconds between keepalive frames
_poll_interval: float = 5.0  # 0 = disabled
_service: str = "app"  # included in every event

# ── Internal state ─────────────────────────────────────────────────────────
_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None
_poller_task: asyncio.Task | None = None


_BACKOFF_MIN: float = 1.0
_BACKOFF_MAX: float = 60.0
_HEADER = struct.Struct("!I")  # 4-byte big-endian unsigned int
_HEARTBEAT_FRAME = _HEADER.pack(0)  # zero-length frame = keepalive


_proc = psutil.Process(os.getpid())


def py_minitor_configure(
    host: str = "localhost",
    port: int = 9000,
    service: str = "app",
    batch_size: int = 100,
    flush_interval: float = 1.0,
    queue_maxsize: int = 10_000,
    heartbeat_interval: float = 30.0,
    poll_interval: float = 5.0,
) -> None:
    """
    Call once at application startup.

    Args:
        host:               Collector server hostname or IP.
        port:               Collector TCP port (default 9000).
        service:            Service name to include in every event, try to `keep unique service` name for each service (default "app").
        batch_size:         Max events per TCP frame. Larger = fewer syscalls.
        flush_interval:     Max seconds to hold a partial batch before flushing.
        queue_maxsize:      In-process event buffer cap. Events beyond this are
                            silently dropped — raise this if your collector goes
                            down for extended periods.
        poll_interval:      If > 0, sample CPU% and memory usage every N seconds and 
                            send as a metric_event with context.source="poller". 
                            This is in addition to the per-request metrics captured 
                            by the FastAPI middleware, and can be used to track background 
                            tasks or overall resource usage between requests.
        heartbeat_interval: Seconds between keepalive frames. Keepalives let
                            both sides detect a dead connection without waiting
                            for the next real send to fail.
    """
    global _host, _port, _service, _batch_size, _flush_interval, _queue_maxsize, _poll_interval
    global _heartbeat_interval
    _host = host
    _port = port
    _service = service
    _batch_size = batch_size
    _flush_interval = flush_interval
    _queue_maxsize = queue_maxsize
    _heartbeat_interval = heartbeat_interval
    _poll_interval = poll_interval


async def py_monitor_start() -> None:
    """
    Start the TCP writer and background CPU/memory poller.

    Call from your app's startup hook. install() and patch_worker_settings()
    do this automatically — only call manually if wiring things up yourself.

        FastAPI lifespan / on_event("startup"):
            await pymonitor.py_monitor_start()

        ARQ WorkerSettings.on_startup:
            async def on_startup(ctx):
                await pymonitor.py_monitor_start()
    """
    _ensure_writer()
    _ensure_poller()


async def py_monitor_shutdown() -> None:
    """
    Flush remaining queued events and close the TCP connection cleanly.
    Call this from your app's shutdown hook so in-flight events aren't lost.

    FastAPI example:
        @app.on_event("shutdown")
        async def on_shutdown():
            await pymonitor_sdk.shutdown()

    ARQ example (in WorkerSettings):
        async def on_shutdown(ctx):
            await pymonitor_sdk.shutdown()
    """
    global _writer_task
    if _writer_task and not _writer_task.done():
        _writer_task.cancel()
        try:
            await _writer_task
        except asyncio.CancelledError:
            pass
    _writer_task = None


def _mem_usage_mb() -> float:
    """Current process memory usage in MB."""
    return _proc.memory_info().rss / 1024 / 1024


def _cpu_usage_percent() -> float:
    """Current process CPU usage as a percentage."""
    return _proc.cpu_percent(interval=None)


def mem_cpu() -> dict[str, float]:
    """Convenience for capturing both memory and CPU usage in one call."""
    return {
        "mem_mb": _mem_usage_mb(),
        "cpu_percent": _cpu_usage_percent(),
    }


def _get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue(maxsize=_queue_maxsize)
    return _queue


async def enqueue(event: "Event") -> None:
    """
    Non-blocking enqueue. Never raises. Silently drops if the queue is full.
    Spawns the writer task on the first call inside a running event loop.
    """
    q = _get_queue()
    try:
        q.put_nowait(event.to_dict())
    except asyncio.QueueFull:
        logger.debug("pymonitor: queue full — dropping 1 event")
        return
    _ensure_writer()


def _ensure_writer() -> None:
    global _writer_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # called outside a running loop — task will spawn on next enqueue
    if _writer_task is None or _writer_task.done():
        _writer_task = loop.create_task(_writer_loop(), name="pymonitor-tcp-writer")


def _ensure_poller() -> None:
    global _poller_task
    if _poll_interval <= 0:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if _poller_task is None or _poller_task.done():
        _poller_task = loop.create_task(_poller_loop(), name="pymonitor-poller")


async def _poller_loop() -> None:
    """
    Sample process CPU% and RSS memory every _poll_interval seconds.

    psutil.cpu_percent(interval=None) measures CPU time elapsed since the
    previous call. The first call always returns 0.0 (nothing to compare
    against), so we prime it once at startup and discard that result.
    From then on, each call after a sleep(poll_interval) returns the average
    CPU% over exactly that interval — which is exactly what we want.
    """

    from pymonitor_sdk import metric_event

    # Prime the CPU counter — first reading is always 0.0, discard it
    _proc.cpu_percent(interval=None)

    logger.debug(
        "pymonitor: poller started (interval=%.0fs, service=%s)",
        _poll_interval,
        _service,
    )

    while True:
        try:
            await asyncio.sleep(_poll_interval)

            cpu_percent = _proc.cpu_percent(interval=None)
            mem_info = _proc.memory_info()
            mem_mb = mem_info.rss / 1024 / 1024

            await enqueue(
                metric_event(
                    service=_service,
                    cpu_percent=round(cpu_percent, 2),
                    mem_mb=round(mem_mb, 2),
                    context={"source": "poller"},
                )
            )
            logger.debug(
                "pymonitor: polled — cpu=%.1f%% mem=%.1fMB", cpu_percent, mem_mb
            )

        except asyncio.CancelledError:
            logger.debug("pymonitor: poller stopped")
            raise


def _pack_frame(batch: list[dict]) -> bytes:
    """4-byte length header + msgpack body."""
    body = msgpack.packb(batch, use_bin_type=True)
    return _HEADER.pack(len(body)) + body


async def _writer_loop() -> None:
    """
    Owns the TCP connection for the lifetime of the process.
    Reconnects with exponential backoff after any failure.
    """
    backoff = _BACKOFF_MIN

    while True:
        writer: asyncio.StreamWriter | None = None
        try:
            _, writer = await asyncio.open_connection(_host, _port)

            # TCP_NODELAY — don't buffer small frames; we do our own batching
            sock = writer.get_extra_info("socket")
            if sock is not None:
                import socket as _socket

                sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)

            logger.info("pymonitor: connected to collector %s:%s", _host, _port)
            backoff = _BACKOFF_MIN  # reset on successful connect

            await _connected(writer)

        except asyncio.CancelledError:
            # Graceful shutdown — best-effort flush already done in _connected
            raise

        except (
            ConnectionRefusedError,
            ConnectionResetError,
            BrokenPipeError,
            TimeoutError,
            OSError,
            EOFError,
        ) as exc:
            logger.warning(
                "pymonitor: connection lost (%s: %s) — retry in %.0fs",
                type(exc).__name__,
                exc,
                backoff,
            )
        except Exception as exc:
            logger.warning(
                "pymonitor: unexpected transport error (%s) — retry in %.0fs",
                exc,
                backoff,
            )
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await asyncio.wait_for(writer.wait_closed(), timeout=2.0)
                except Exception:
                    pass

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)


async def _connected(writer: asyncio.StreamWriter) -> None:
    """
    Run the send loop and heartbeat concurrently on one connection.
    Either task raises → both are cancelled → _writer_loop reconnects.
    """
    loop = asyncio.get_running_loop()

    send_task = loop.create_task(_send_loop(writer), name="pymonitor-send")
    hb_task = loop.create_task(_heartbeat_loop(writer), name="pymonitor-heartbeat")

    try:
        # Wait for whichever task finishes first (usually due to an error)
        done, pending = await asyncio.wait(
            {send_task, hb_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel the sibling task
        for t in pending:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

        # Re-raise the exception from the task that died (if any)
        for t in done:
            if not t.cancelled():
                exc = t.exception()
                if exc is not None:
                    raise exc

    except asyncio.CancelledError:
        # Shutdown path — flush whatever remains in the batch
        send_task.cancel()
        hb_task.cancel()
        for t in (send_task, hb_task):
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        await _flush_remaining(writer)
        raise


async def _send_loop(writer: asyncio.StreamWriter) -> None:
    """
    Drain the queue and write frames to the connection.
    Raises on any write failure so _connected can trigger a reconnect.
    """
    q = _get_queue()
    batch: list[dict] = []
    loop = asyncio.get_running_loop()

    while True:
        deadline = loop.time() + _flush_interval

        while len(batch) < _batch_size:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                event = await asyncio.wait_for(q.get(), timeout=remaining)
                batch.append(event)
            except asyncio.TimeoutError:
                break

        if batch:
            writer.write(_pack_frame(batch))
            await writer.drain()  # raises immediately if connection is broken
            logger.debug("pymonitor: sent %d events", len(batch))
            batch.clear()


async def _heartbeat_loop(writer: asyncio.StreamWriter) -> None:
    """
    Send a zero-length frame every heartbeat_interval seconds.
    Causes a BrokenPipeError quickly if the peer has gone away, which
    triggers reconnection faster than waiting for a real send to fail.
    """

    while True:
        await asyncio.sleep(_heartbeat_interval)
        writer.write(_HEARTBEAT_FRAME)
        await writer.drain()
        logger.debug("pymonitor: heartbeat sent")


async def _flush_remaining(writer: asyncio.StreamWriter) -> None:
    """Best-effort drain of the queue on shutdown."""
    q = _get_queue()
    batch: list[dict] = []
    while not q.empty():
        try:
            batch.append(q.get_nowait())
        except asyncio.QueueEmpty:
            break
    if batch:
        try:
            writer.write(_pack_frame(batch))
            await asyncio.wait_for(writer.drain(), timeout=3.0)
            logger.info("pymonitor: flushed %d events on shutdown", len(batch))
        except Exception as exc:
            logger.warning("pymonitor: shutdown flush failed: %s", exc)
