"""Helpers for merging independent async public-data streams."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast


async def merge_streams[T](*streams: AsyncIterator[T]) -> AsyncIterator[T]:
    queue: asyncio.Queue[T | BaseException | object] = asyncio.Queue(maxsize=1_000)
    sentinel = object()

    async def consume(stream: AsyncIterator[T]) -> None:
        try:
            async for item in stream:
                await queue.put(item)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await queue.put(exc)
        finally:
            close = getattr(stream, "aclose", None)
            if close is not None:
                await close()
        # A cancelled producer must not publish a completion marker. In
        # particular, putting the marker from ``finally`` can deadlock when the
        # bounded queue is full and the merge consumer has already stopped.
        await queue.put(sentinel)

    tasks = [asyncio.create_task(consume(stream)) for stream in streams]
    completed = 0
    try:
        while completed < len(tasks):
            item = await queue.get()
            if item is sentinel:
                completed += 1
            elif isinstance(item, BaseException):
                raise item
            else:
                yield cast(T, item)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
