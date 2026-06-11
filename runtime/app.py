"""App — declarative root. include() wires Pools (consumers) AND Schedulers
(producers); start() is the explicit entry point (no CLI).

start() logs the topology, then runs: it enters each container's lifespan (POOL
scope), creates the consumer group per flow, spawns the slot workers under a
shared pool semaphore, and starts each producer's emit loop — blocking until
interrupted. Graceful drain on SIGTERM is not implemented yet.
"""

import asyncio
import logging
import os
import socket
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass

from runtime.broker import Broker, stream_name
from runtime.pool import Pool, slot_worker
from runtime.redis_io import connect
from runtime.scheduler import Scheduler, run_every, run_once
from runtime.types import Config, Lifespan, Resources

log = logging.getLogger("runtime")


@dataclass
class _PoolInclusion:
    pool: Pool
    max_slots: int | None
    config: Config | None


@dataclass
class _SchedulerInclusion:
    scheduler: Scheduler
    config: Config | None


def _ensure_logging() -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


async def _enter_lifespan(
    stack: AsyncExitStack, name: str, lifespan: Lifespan, config: Config
) -> Resources:
    """Call the lifespan and enter the async context manager it returns. Guards a
    common mistake — forgetting `@asynccontextmanager` — with a clear, named error
    at boot instead of a generic protocol failure deep in the stack."""
    cm = lifespan(config)
    if not isinstance(cm, AbstractAsyncContextManager):
        raise TypeError(
            f"{name}: lifespan must return an async context manager "
            f"(decorate it with @asynccontextmanager); got {cm!r}"
        )
    return await stack.enter_async_context(cm)


def _check_unique_consumes(pools: list[Pool]) -> None:
    """One event type → one logical flow, across ALL pools (no logical fan-out).
    Pool._validate guards within a pool; this guards across them."""
    owners: dict[str, str] = {}
    for pool in pools:
        for reg in pool._flows:
            owner = f"{pool.name}/{reg.handler.__name__}"
            if reg.consumes in owners:
                raise ValueError(
                    f"event type {reg.consumes!r} consumed by both "
                    f"{owners[reg.consumes]} and {owner} — no logical fan-out"
                )
            owners[reg.consumes] = owner


class App:
    def __init__(self, *, redis: str, namespace: str = "") -> None:
        self.redis: str = redis
        self.namespace: str = (
            namespace  # prefixes every stream key — isolate dev/staging on one Redis
        )
        self._pools: list[_PoolInclusion] = []
        self._schedulers: list[_SchedulerInclusion] = []

    def include(
        self,
        component: Pool | Scheduler,
        *,
        enabled: bool = True,  # kill-switch (static bool for now)
        max_slots: int | None = None,  # override the pool budget
        config: Config | None = None,  # injected into the lifespan
    ) -> None:
        """EXPLICIT wiring, in main. An `enabled=False` component is NEITHER
        started NOR armed."""
        if not enabled:
            return
        if isinstance(component, Pool):
            self._pools.append(_PoolInclusion(component, max_slots, config))
        elif isinstance(component, Scheduler):
            self._schedulers.append(_SchedulerInclusion(component, config))
        else:
            raise TypeError(
                f"include() expects a Pool or a Scheduler, got {component!r}"
            )

    # ── boot ──────────────────────────────────────────────────────────────────
    def start(self) -> None:
        """Blocking. Log the topology, then run pools' consumers and schedulers'
        producers until interrupted (Ctrl-C)."""
        self._log_topology()
        try:
            asyncio.run(self._serve())
        except KeyboardInterrupt:
            log.info("interrupted — shutting down")

    def _log_topology(self) -> None:
        # cross-pool uniqueness is validated by _serve (the run path); this is a
        # pure logger.
        _ensure_logging()
        log.info("App(redis=%s, namespace=%r) — topology:", self.redis, self.namespace)
        for pool_inc in self._pools:
            pool = pool_inc.pool
            budget = (
                pool_inc.max_slots if pool_inc.max_slots is not None else pool.max_slots
            )
            override = (
                f" (overridden from {pool.max_slots})"
                if pool_inc.max_slots is not None
                and pool_inc.max_slots != pool.max_slots
                else ""
            )
            lifespan = getattr(pool._lifespan, "__name__", None)
            log.info(
                "  Pool %r max_slots=%d%s lifespan=%s",
                pool.name,
                budget,
                override,
                lifespan,
            )
            for reg in pool._flows:
                log.info(
                    "    flow %s consumes %r → stream %r slots=%d",
                    reg.handler.__name__,
                    reg.consumes,
                    stream_name(reg.consumes),
                    reg.max_slots if reg.max_slots is not None else budget,
                )

        for sched_inc in self._schedulers:
            sched = sched_inc.scheduler
            lifespan = getattr(sched._lifespan, "__name__", None)
            log.info("  Scheduler %r lifespan=%s", sched.name, lifespan)
            for ereg in sched._every:
                log.info(
                    "    every %gs id=%r (%s)",
                    ereg.interval,
                    ereg.id,
                    ereg.handler.__name__,
                )
            for oreg in sched._once:
                when = f" after {oreg.delay:g}s" if oreg.delay else ""
                log.info(
                    "    once%s id=%r (%s)",
                    when,
                    oreg.id,
                    oreg.handler.__name__,
                )

    async def _serve(self, broker: Broker | None = None) -> None:
        """Enter container lifespans, spawn slot workers + producer loops, run
        until cancelled. Accepts an injected broker for testing; else opens one
        from self.redis."""
        _check_unique_consumes([inc.pool for inc in self._pools])
        own_broker = broker is None
        bk = (
            broker
            if broker is not None
            else connect(self.redis, namespace=self.namespace)
        )
        pod = f"{socket.gethostname()}-{os.getpid()}"
        try:
            async with AsyncExitStack() as stack:
                tasks: list[asyncio.Task[None]] = []

                for inc in self._pools:
                    pool = inc.pool
                    budget = (
                        inc.max_slots if inc.max_slots is not None else pool.max_slots
                    )
                    config: Config = inc.config if inc.config is not None else {}
                    resources: Resources = {}
                    if pool._lifespan is not None:
                        resources = await _enter_lifespan(
                            stack, f"pool {pool.name!r}", pool._lifespan, config
                        )
                    pool_sem = asyncio.Semaphore(budget)
                    for reg in pool._flows:
                        await bk.ensure_stream(stream_name(reg.consumes))
                        n = reg.max_slots if reg.max_slots is not None else budget
                        for i in range(n):
                            consumer = f"{pod}:{reg.consumes}:{i}"
                            tasks.append(
                                asyncio.create_task(
                                    slot_worker(
                                        bk,
                                        reg.handler,
                                        consumes=reg.consumes,
                                        consumer=consumer,
                                        resources=resources,
                                        config=config,
                                        pool_sem=pool_sem,
                                    ),
                                    name=consumer,
                                )
                            )

                n_slots = len(tasks)
                for inc_s in self._schedulers:
                    sched = inc_s.scheduler
                    sconfig: Config = inc_s.config if inc_s.config is not None else {}
                    sresources: Resources = {}
                    if sched._lifespan is not None:
                        sresources = await _enter_lifespan(
                            stack, f"scheduler {sched.name!r}", sched._lifespan, sconfig
                        )
                    for ereg in sched._every:
                        tasks.append(
                            asyncio.create_task(
                                run_every(
                                    bk, ereg, resources=sresources, config=sconfig
                                ),
                                name=f"every:{sched.name}:{ereg.id}",
                            )
                        )
                    for oreg in sched._once:
                        tasks.append(
                            asyncio.create_task(
                                run_once(
                                    bk, oreg, resources=sresources, config=sconfig
                                ),
                                name=f"once:{sched.name}:{oreg.id}",
                            )
                        )
                n_producers = len(tasks) - n_slots

                if not tasks:
                    log.info("nothing to run")
                    return
                log.info(
                    "serving %d task(s): %d consumer slot(s) + %d producer(s)",
                    len(tasks),
                    n_slots,
                    n_producers,
                )
                try:
                    await asyncio.gather(*tasks)
                except asyncio.CancelledError:
                    # Ctrl-C cancels us; cancel the workers and wait for them to
                    # unwind (return_exceptions so a worker's teardown error can't
                    # mask the shutdown) before the lifespans and broker close.
                    log.info("draining %d task(s)", len(tasks))
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise
        finally:
            if own_broker:
                await bk.aclose()
