"""Poisson example — a pool that is BOTH a consumer and a producer.

The point: a flow handler can itself ``emit``. There's nothing special about
producers — emitting is just a method on the context every handler already has.

A scheduler opens a window every 10s. The ``generator`` pool *consumes* that
window event and, per window, *emits* a Poisson-distributed number of ``request``
events; the ``workers`` pool consumes those. So ``generator`` sits in the middle —
consumer of ``window.open``, producer of ``request`` — wiring two pools together
through nothing but event names.

    python -m examples.poisson.main

Needs a Redis on :6379. Single process here; the same App could be split across
deployments (run the scheduler once, scale ``workers`` to N) with no code change.
"""

import math
import os
import random

from runtime import App, Pool, Scheduler


def _poisson(lam: float) -> int:
    """Sample a Poisson(lam) count with Knuth's algorithm — stdlib only, no numpy.
    This is the number of arrivals in one window (a Poisson process over a fixed
    interval); spreading them in real time would use ``random.expovariate``."""
    target = math.exp(-lam)
    k, p = 0, 1.0
    while p > target:
        k += 1
        p *= random.random()
    return k - 1


# ── scheduler: open a window on a fixed interval ────────────────────────────
clock = Scheduler("clock")


@clock.every("10s")
async def open_window(ctx):
    await ctx.emit("window.open")


# ── pool #1: CONSUMER of windows, PRODUCER of requests ──────────────────────
generator = Pool("generator", max_slots=1)


@generator.flow(consumes="window.open")
async def fan_out(ctx, event):
    lam = ctx.config.get("lam", 8.0)  # mean arrivals per window
    n = _poisson(lam)
    print(f"[generator] window → {n} arrival(s)")
    for seq in range(n):
        await ctx.emit("request", seq=seq)  # ← the pool acting as a producer


# ── pool #2: CONSUMER of the generated requests ─────────────────────────────
workers = Pool("workers", max_slots=10)


@workers.flow(consumes="request")
async def handle(ctx, event):
    print(f"[workers] handling request {event['seq']}")


def build_app() -> App:
    app = App(redis=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    app.include(clock)
    app.include(generator, config={"lam": 8.0})
    app.include(workers)
    return app


if __name__ == "__main__":
    build_app().start()
