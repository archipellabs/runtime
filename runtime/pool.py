"""Pool — the consumer subsystem: the declarative container AND its worker.

A `Pool` is a unit of consumption AND of isolation. Split into distinct pools by
COST PROFILE (Playwright ≠ SSE ≠ API) or to isolate blast radius. Everything runs
on ONE asyncio event loop. The imperative `register` is the ground truth; the
`flow` decorator is a shell that delegates to it (single code path).

`slot_worker` (bottom of the file) is the runner App._serve spawns per slot —
kept here, next to `Pool`/`FlowRegistration` it consumes, mirroring how
`scheduler.py` holds both `Scheduler` and `run_producer`.
"""

import asyncio
import logging
import warnings
from dataclasses import dataclass
from typing import Callable, TypeVar

from runtime.broker import Broker, stream_name
from runtime.context import Handler, RuntimeContext
from runtime.types import Config, Event, Lifespan, Resources

log = logging.getLogger("runtime")

_HandlerFn = TypeVar("_HandlerFn", bound=Handler)


@dataclass(frozen=True)
class FlowRegistration:
    """Inspectable state of a registered flow (testable without the runtime)."""

    handler: Handler
    consumes: str                  # the event type (unique within the pool)
    max_slots: int | None = None   # optional cap; None = can take the whole budget


class Pool:
    def __init__(
        self,
        name: str,
        *,
        max_slots: int,                     # shared BUDGET of the pool (memory bound)
        lifespan: Lifespan | None = None,   # shared resource (POOL scope)
    ) -> None:
        if max_slots < 1:
            raise ValueError(f"{name}: max_slots must be >= 1, got {max_slots}")
        self.name: str = name
        self.max_slots: int = max_slots
        self._lifespan: Lifespan | None = lifespan
        self._flows: list[FlowRegistration] = []

    # ── IMPERATIVE API: the runtime's ground truth. Everything goes through here.
    def register(
        self, handler: Handler, *, consumes: str, max_slots: int | None = None
    ) -> FlowRegistration:
        """First-class entry point. Usable dynamically (conditional registration
        by flag, in a loop, etc.). ``handler`` is an async ``(ctx, event)`` fn."""
        reg = FlowRegistration(handler, consumes=consumes, max_slots=max_slots)
        self._validate(reg)
        self._flows.append(reg)
        return reg

    # ── SUGAR: the decorator is just a shell that DELEGATES to register.
    #    It does NOTHING the method doesn't do — a single code path.
    def flow(
        self, *, consumes: str, max_slots: int | None = None
    ) -> Callable[[_HandlerFn], _HandlerFn]:
        def deco(fn: _HandlerFn) -> _HandlerFn:
            self.register(fn, consumes=consumes, max_slots=max_slots)
            return fn                       # function returned unchanged → testable bare
        return deco

    def _validate(self, reg: FlowRegistration) -> None:
        # - consumes unique WITHIN this pool (one event type → one logical flow)
        if any(f.consumes == reg.consumes for f in self._flows):
            raise ValueError(f"{self.name}: event type already consumed: {reg.consumes!r}")
        # - per-flow cap > pool budget = useless (warning, not error)
        if reg.max_slots is not None and reg.max_slots > self.max_slots:
            warnings.warn(f"{self.name}/{reg.consumes}: max_slots>{self.max_slots} has no effect")


# ─────────────────────────────────────────────────────────────────────────────
# Slot worker — one concurrency slot running a flow handler.
#
# Per (pool, flow) the runtime spawns N slot workers, each a distinct broker
# consumer of the flow's stream (competing consumers). A worker runs a sequential
# claim → handle → ack loop; the pool semaphore wraps the handler call so total
# in-flight across the pool never exceeds the budget. No per-slot state object:
# shared resources come from the pool lifespan (ctx.resources); per-event
# setup/teardown lives in the body.
#
# Deliberately naive (no reclaim, no dead-letter, no heartbeat yet): a handler
# that raises is logged and left UN-ACKED, so the message stays pending for a
# future reclaim loop to pick up.
# ─────────────────────────────────────────────────────────────────────────────

_BLOCK_MS = 5000  # how long claim() parks when idle; cancellation is immediate


async def slot_worker(
    broker: Broker,
    handler: Handler,
    *,
    consumes: str,
    consumer: str,
    resources: Resources,
    config: Config,
    pool_sem: asyncio.Semaphore,
) -> None:
    ctx = RuntimeContext(broker, resources=resources, config=config)
    stream = stream_name(consumes)
    log.debug("slot up [%s] consuming %r", consumer, consumes)
    try:
        while True:
            msgs = await broker.claim(stream, consumer=consumer, count=1, block_ms=_BLOCK_MS)
            if not msgs:
                # A real broker parks on claim (socket I/O = a real suspension
                # point). Some clients (e.g. fakeredis) return instantly, so yield
                # here explicitly to avoid pinning the event loop in a tight loop.
                await asyncio.sleep(0)
                continue
            for msg_id, event in msgs:
                log.debug("handle %r [%s] id=%s", consumes, consumer, msg_id)
                if await _process(handler, ctx, event, pool_sem, consumer, msg_id):
                    await broker.ack(stream, msg_id)
    finally:
        log.debug("slot down [%s]", consumer)


async def _process(
    handler: Handler,
    ctx: RuntimeContext,
    event: Event,
    pool_sem: asyncio.Semaphore,
    consumer: str,
    msg_id: str,
) -> bool:
    """Run the handler for one event under the pool budget. Returns True if it
    succeeded (→ ack), False if it raised (→ leave it pending for redelivery)."""
    async with pool_sem:
        try:
            await handler(ctx, event)
            return True
        except Exception:
            log.exception("handler failed; leaving unacked [%s %s]", consumer, msg_id)
            return False
