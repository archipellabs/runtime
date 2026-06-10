"""Context — what handlers and producers receive: resource access + emit.

`Context` is the type contract (a Protocol). `Handler` is the consumer type (an
async `(ctx, event)`); `ProducerFn` is the producer type (an async `(ctx)`).
`RuntimeContext` is the concrete implementation the runtime hands to both: it
carries the container's shared resources + the injected config, and routes
`emit` to the right stream. A consumer slot and a producer each get one with
their container's lifespan resources (or an empty mapping if no lifespan).
"""

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from runtime.broker import Broker, stream_name
from runtime.types import Config, Event, Resources


class Context(Protocol):
    """Passed to every handler and producer. Gives access to shared resources
    and to emit."""

    resources: Resources
    """Resources exposed by the container's lifespan (POOL scope).
    E.g. ``ctx.resources["browser"]``. Shared by everything in the container."""

    config: Config
    """Config injected at mount time via ``App.include(config=...)``."""

    async def emit(self, event_type: str, /, **payload: Any) -> None:
        """Push an event into the queue. Mostly producer-side, but a handler may
        re-emit (chaining). (Deduplication via a deterministic job-id per bucket,
        for idempotent producers, is planned.)"""
        ...


Handler = Callable[[Context, Event], Awaitable[None]]
"""A flow: an async ``(ctx, event)`` handler for ONE event type. One worker
(slot) calls it sequentially per event; the pool semaphore bounds how many run
at once. Must be idempotent (AT-LEAST-ONCE → replay possible at reclaim). Two
scopes only: POOL (shared resources via the lifespan, reached as ctx.resources)
and HANDLE (per-event setup/teardown in the body, a plain try/finally)."""

ProducerFn = Callable[[Context], Awaitable[None]]
"""A producer: an async ``(ctx)`` body that emits events. A scheduler calls it on
a fixed interval; shared resources (an API/DB client) come from the scheduler's
lifespan, reached as ctx.resources."""


class RuntimeContext:
    """Concrete Context. ``emit`` resolves event type → stream and appends via the
    broker — no dedup yet. Emitting an event type that has no consumer in this
    process is fine and expected (a producer-only process emits to streams its
    own process never reads)."""

    def __init__(
        self,
        broker: Broker,
        *,
        resources: Resources | None = None,
        config: Config | None = None,
    ) -> None:
        self.resources: Resources = resources if resources is not None else {}
        self.config: Config = config if config is not None else {}
        self._broker: Broker = broker

    async def emit(self, event_type: str, /, **payload: Any) -> None:
        await self._broker.append(stream_name(event_type), payload)
