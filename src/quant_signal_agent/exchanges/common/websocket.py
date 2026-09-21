"""Resilient public WebSocket JSON stream."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

import aiohttp

from quant_signal_agent.exchanges.common.errors import ExchangeConnectionError
from quant_signal_agent.exchanges.common.http import _failure_stage, _TimeoutResolver


async def stream_json_messages(
    url: str,
    subscribe_messages: Sequence[dict[str, Any]],
    *,
    heartbeat_seconds: float = 20.0,
    application_ping: dict[str, Any] | str | None = None,
    dns_timeout_seconds: float = 5.0,
    connect_timeout_seconds: float = 5.0,
    tls_timeout_seconds: float = 10.0,
    read_timeout_seconds: float = 60.0,
    dns_cache_seconds: int = 60,
    subscription_updates: asyncio.Queue[dict[str, Any]] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield one connection generation and surface disconnects to the supervisor."""

    if not url.startswith("wss://"):
        raise ValueError("Public WebSocket transport requires WSS")
    if min(
        dns_timeout_seconds,
        connect_timeout_seconds,
        tls_timeout_seconds,
        read_timeout_seconds,
        dns_cache_seconds,
    ) <= 0:
        raise ValueError("WebSocket timeouts and DNS cache duration must be positive")
    try:
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=(
                dns_timeout_seconds + connect_timeout_seconds + tls_timeout_seconds
            ),
            sock_connect=connect_timeout_seconds,
            sock_read=read_timeout_seconds,
        )
        connector = aiohttp.TCPConnector(
            resolver=_TimeoutResolver(dns_timeout_seconds),
            use_dns_cache=True,
            ttl_dns_cache=dns_cache_seconds,
        )
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.ws_connect(
                url,
                heartbeat=heartbeat_seconds,
                autoping=True,
                timeout=aiohttp.ClientWSTimeout(
                    ws_receive=read_timeout_seconds,
                    ws_close=min(read_timeout_seconds, 10.0),
                ),
            ) as ws:
                for message in subscribe_messages:
                    await ws.send_json(message)
                ping_task = (
                    asyncio.create_task(
                        _send_application_heartbeats(ws, application_ping, heartbeat_seconds)
                    )
                    if application_ping is not None
                    else None
                )
                try:
                    receive_task: asyncio.Task[aiohttp.WSMessage] | None = None
                    update_task: asyncio.Task[dict[str, Any]] | None = None
                    while True:
                        if receive_task is None:
                            receive_task = asyncio.create_task(ws.receive())
                        waiters: set[asyncio.Task[Any]] = {receive_task}
                        if subscription_updates is not None:
                            if update_task is None:
                                update_task = asyncio.create_task(
                                    subscription_updates.get()
                                )
                            waiters.add(update_task)
                        done, _ = await asyncio.wait(
                            waiters, return_when=asyncio.FIRST_COMPLETED
                        )
                        if update_task is not None and update_task in done:
                            update = update_task.result()
                            update_task = None
                            assert subscription_updates is not None
                            try:
                                await ws.send_json(update)
                            finally:
                                subscription_updates.task_done()
                        if receive_task not in done:
                            continue
                        incoming = receive_task.result()
                        receive_task = None
                        if incoming.type is aiohttp.WSMsgType.TEXT:
                            if incoming.data == "pong":
                                continue
                            payload = incoming.json()
                            if isinstance(payload, dict):
                                yield payload
                        elif incoming.type in {
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        }:
                            break
                finally:
                    for task in (receive_task, update_task):
                        if task is not None:
                            task.cancel()
                    await asyncio.gather(
                        *(task for task in (receive_task, update_task) if task is not None),
                        return_exceptions=True,
                    )
                    if ping_task is not None:
                        ping_task.cancel()
                        await asyncio.gather(ping_task, return_exceptions=True)
    except asyncio.CancelledError:
        raise
    except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
        raise ExchangeConnectionError(
            "Public WebSocket connection failed",
            failure_stage=_failure_stage(exc),
            root_error_type=type(exc).__name__,
        ) from exc
    raise ExchangeConnectionError("Public WebSocket connection closed")


async def _send_application_heartbeats(
    ws: aiohttp.ClientWebSocketResponse,
    payload: dict[str, Any] | str,
    interval_seconds: float,
) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        if isinstance(payload, str):
            await ws.send_str(payload)
        else:
            await ws.send_json(payload)
