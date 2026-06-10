"""Scheduler — the producer-side container, mirror of Pool.

A Scheduler groups producers that share an (optional) lifespan resource and is
included into the App like a Pool. Each producer is an async ``(ctx)`` body
registered with ``@scheduler.every(interval)``; the runtime calls it on that
fixed interval, passing a Context whose ``resources`` come from the scheduler's
lifespan (this is how a cron-style producer gets its API/DB client).

  Scheduler ↔ Pool         @scheduler.every ↔ @pool.flow         producer ↔ handler

Deliberately ONE trigger (`every`) for now: a periodic producer and a load
producer currently do the same thing (emit on a fixed interval). Specialized
triggers — real calendar `cron` (5-field, level-triggered) and stochastic load
(Poisson, ramps) — are planned for when they actually diverge.
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final, TypeVar

from runtime.broker import Broker
from runtime.context import ProducerFn, RuntimeContext
from runtime.types import Config, Lifespan, Resources

log = logging.getLogger("runtime")

# Duration suffixes → seconds. Longer suffixes first so "ms"/"min" win over "s"/"m".
_UNITS: Final[list[tuple[str, float]]] = [
    ("ms", 0.001),
    ("min", 60.0),
    ("h", 3600.0),
    ("s", 1.0),
    ("m", 60.0),
]


def parse_duration(value: str | float) -> float:
    """Interval → seconds. Accepts a number (seconds) or a string with a unit
    suffix: ``"500ms"``, ``"1.5s"``, ``"10min"``, ``"1h"`` (bare number = seconds)."""
    if isinstance(value, (int, float)):
        seconds = float(value)
    else:
        s = value.strip()
        for suffix, mult in _UNITS:
            if s.endswith(suffix):
                seconds = float(s[: -len(suffix)]) * mult
                break
        else:
            seconds = float(s)  # bare number string → seconds (raises on garbage)
    if seconds <= 0:
        raise ValueError(f"interval must be > 0: {value!r}")
    return seconds


@dataclass(frozen=True)
class ProducerRegistration:
    """Inspectable state of a registered producer (testable without the runtime)."""

    handler: ProducerFn
    interval: float  # seconds between ticks
    id: str  # identity / base of the deterministic dedup key (planned)


_ProducerFnT = TypeVar("_ProducerFnT", bound=ProducerFn)


class Scheduler:
    def __init__(self, name: str, *, lifespan: Lifespan | None = None) -> None:
        self.name: str = name
        self._lifespan: Lifespan | None = lifespan
        self._producers: list[ProducerRegistration] = []

    # ── IMPERATIVE API: the runtime's ground truth. Everything goes through here.
    def register(
        self, fn: ProducerFn, *, interval: str | float, id: str | None = None
    ) -> ProducerRegistration:
        reg = ProducerRegistration(fn, parse_duration(interval), id or fn.__name__)
        self._producers.append(reg)
        return reg

    # ── SUGAR: the decorator is a shell that DELEGATES to register (single path).
    def every(
        self, interval: str | float, *, id: str | None = None
    ) -> Callable[[_ProducerFnT], _ProducerFnT]:
        def deco(fn: _ProducerFnT) -> _ProducerFnT:
            self.register(fn, interval=interval, id=id)
            return fn  # function returned unchanged → testable bare

        return deco


async def run_producer(
    broker: Broker,
    reg: ProducerRegistration,
    *,
    resources: Resources,
    config: Config,
) -> None:
    """Drive one producer: call its body on a fixed interval (emit-then-sleep, so
    it fires once at boot). A body that raises is logged and the loop continues —
    one bad tick must not kill the schedule. Cancellation propagates out cleanly."""
    ctx = RuntimeContext(broker, resources=resources, config=config)
    while True:
        try:
            await reg.handler(ctx)
        except Exception:
            log.exception("producer %r tick failed", reg.id)
        await asyncio.sleep(reg.interval)
