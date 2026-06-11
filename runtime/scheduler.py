"""Scheduler — the producer-side container, mirror of Pool.

A Scheduler groups producers that share an (optional) lifespan resource and is
included into the App like a Pool. Each producer is an async ``(ctx)`` body
registered with ``@scheduler.every(interval)``; the runtime calls it on that
fixed interval, passing a Context whose ``resources`` come from the scheduler's
lifespan (this is how a cron-style producer gets its API/DB client).

  Scheduler ↔ Pool         @scheduler.every ↔ @pool.flow         producer ↔ handler

Two triggers today: `every` (periodic — loops on a fixed interval) and `once` (a
one-shot producer that fires a single time at boot, after an optional `delay`).
Further specialized triggers — real calendar `cron` (5-field, level-triggered)
and stochastic load (Poisson, ramps) — are planned for when they actually diverge.
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


def parse_duration(value: str | float, *, allow_zero: bool = False) -> float:
    """Duration → seconds. Accepts a number (seconds) or a string with a unit
    suffix: ``"500ms"``, ``"1.5s"``, ``"10min"``, ``"1h"`` (bare number = seconds).
    Intervals must be > 0; a `once` delay may be 0 (fire at boot) — pass
    ``allow_zero=True`` for that."""
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
    if seconds < 0 or (seconds == 0 and not allow_zero):
        raise ValueError(
            f"duration must be {'>= 0' if allow_zero else '> 0'}: {value!r}"
        )
    return seconds


@dataclass(frozen=True)
class EveryRegistration:
    """Inspectable state of a periodic (`every`) producer (testable without the runtime)."""

    handler: ProducerFn
    interval: float  # seconds between ticks
    id: str  # identity / base of the deterministic dedup key (planned)


@dataclass(frozen=True)
class OnceRegistration:
    """Inspectable state of a one-shot producer (testable without the runtime)."""

    handler: ProducerFn
    delay: float  # seconds to wait before the single fire (0 = at boot)
    id: str


_ProducerFnT = TypeVar("_ProducerFnT", bound=ProducerFn)


class Scheduler:
    def __init__(self, name: str, *, lifespan: Lifespan | None = None) -> None:
        self.name: str = name
        self._lifespan: Lifespan | None = lifespan
        self._every: list[EveryRegistration] = []
        self._once: list[OnceRegistration] = []

    # ── IMPERATIVE API: the runtime's ground truth. Everything goes through here.
    def register_every(
        self, fn: ProducerFn, *, interval: str | float, id: str | None = None
    ) -> EveryRegistration:
        reg = EveryRegistration(fn, parse_duration(interval), id or fn.__name__)
        self._every.append(reg)
        return reg

    def register_once(
        self, fn: ProducerFn, *, delay: str | float = 0, id: str | None = None
    ) -> OnceRegistration:
        reg = OnceRegistration(
            fn, parse_duration(delay, allow_zero=True), id or fn.__name__
        )
        self._once.append(reg)
        return reg

    # ── SUGAR: the decorators are shells that DELEGATE to register (single path).
    def every(
        self, interval: str | float, *, id: str | None = None
    ) -> Callable[[_ProducerFnT], _ProducerFnT]:
        def deco(fn: _ProducerFnT) -> _ProducerFnT:
            self.register_every(fn, interval=interval, id=id)
            return fn  # function returned unchanged → testable bare

        return deco

    def once(
        self, *, delay: str | float = 0, id: str | None = None
    ) -> Callable[[_ProducerFnT], _ProducerFnT]:
        def deco(fn: _ProducerFnT) -> _ProducerFnT:
            self.register_once(fn, delay=delay, id=id)
            return fn  # function returned unchanged → testable bare

        return deco


async def run_every(
    broker: Broker,
    reg: EveryRegistration,
    *,
    resources: Resources,
    config: Config,
) -> None:
    """Drive one periodic (`every`) producer: call its body on a fixed interval
    (emit-then-sleep, so it fires once at boot). A body that raises is logged and
    the loop continues — one bad tick must not kill the schedule. Cancellation
    propagates out cleanly."""
    ctx = RuntimeContext(broker, resources=resources, config=config)
    while True:
        try:
            await reg.handler(ctx)
        except Exception:
            log.exception("every %r tick failed", reg.id)
        await asyncio.sleep(reg.interval)


async def run_once(
    broker: Broker,
    reg: OnceRegistration,
    *,
    resources: Resources,
    config: Config,
) -> None:
    """Drive a one-shot producer: wait its (optional) delay, then call the body
    exactly once and return. A body that raises is logged, not re-raised — one bad
    one-shot must not crash the app. Cancellation propagates out cleanly."""
    ctx = RuntimeContext(broker, resources=resources, config=config)
    if reg.delay:
        await asyncio.sleep(reg.delay)
    try:
        await reg.handler(ctx)
    except Exception:
        log.exception("once %r failed", reg.id)
