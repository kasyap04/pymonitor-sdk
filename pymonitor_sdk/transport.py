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
from typing import TYPE_CHECKING

import msgpack

if TYPE_CHECKING:
    from pymonitor_sdk.models import Event

logger = logging.getLogger("pymonitor.transport")

# ── Config (set via configure()) ───────────────────────────────────────────
_host: str = "localhost"
_port: int = 9000
_batch_size: int = 100
_flush_interval: float = 0.0  # max seconds to hold a partial batch
_queue_maxsize: int = 10_000
_heartbeat_interval: float = 30.0  # seconds between keepalive frames

# ── Internal state ─────────────────────────────────────────────────────────
_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None

_BACKOFF_MIN: float = 1.0
_BACKOFF_MAX: float = 60.0
_HEADER = struct.Struct("!I")  # 4-byte big-endian unsigned int
_HEARTBEAT_FRAME = _HEADER.pack(0)  # zero-length frame = keepalive


def configure(
    host: str = "localhost",
    port: int = 9000,
    batch_size: int = 100,
    flush_interval: float = 1.0,
    queue_maxsize: int = 10_000,
    heartbeat_interval: float = 30.0,
) -> None:
    """
    Call once at application startup.

    Args:
        host:               Collector server hostname or IP.
        port:               Collector TCP port (default 9000).
        batch_size:         Max events per TCP frame. Larger = fewer syscalls.
        flush_interval:     Max seconds to hold a partial batch before flushing.
        queue_maxsize:      In-process event buffer cap. Events beyond this are
                            silently dropped — raise this if your collector goes
                            down for extended periods.
        heartbeat_interval: Seconds between keepalive frames. Keepalives let
                            both sides detect a dead connection without waiting
                            for the next real send to fail.
    """
    global _host, _port, _batch_size, _flush_interval, _queue_maxsize
    global _heartbeat_interval
    _host = host
    _port = port
    _batch_size = batch_size
    _flush_interval = flush_interval
    _queue_maxsize = queue_maxsize
    _heartbeat_interval = heartbeat_interval


async def shutdown() -> None:
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


# ── Internal helpers ───────────────────────────────────────────────────────


def _get_queue() -> asyncio.Queue:
    global _queue
    if _queue is None:
        _queue = asyncio.Queue(maxsize=_queue_maxsize)
    return _queue


async def test_loop():
    q = _get_queue()
    while not q.empty():
        event = await q.get()
        print(f"\n\nGot event: {event}\n\n")

        q.task_done()


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
        # _writer_task = loop.create_task(test_loop(), name="pymonitor-tcp-writer")
        _writer_task = loop.create_task(_writer_loop(), name="pymonitor-tcp-writer")


# ── Wire encoding ──────────────────────────────────────────────────────────


def _pack_frame(batch: list[dict]) -> bytes:
    """4-byte length header + msgpack body."""
    body = msgpack.packb(batch, use_bin_type=True)
    return _HEADER.pack(len(body)) + body


# ── Writer loop ────────────────────────────────────────────────────────────


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

    # _batch_size = 1

    while True:
        deadline = loop.time() + _flush_interval

        while len(batch) < _batch_size:
            remaining = deadline - loop.time()
            print(f"{remaining = }")
            if remaining <= 0:
                break
            try:
                event = await asyncio.wait_for(q.get(), timeout=remaining)
                batch.append(event)
            except asyncio.TimeoutError:
                break
        
        print(f"\n\nBatch size: {batch} === {_batch_size}  {loop.time()}\n\n")
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
