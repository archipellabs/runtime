# archipellabs-runtime

An async runtime for running lots of tasks at once — Playwright sessions, API
calls, SSE streams, timed waits — without losing control of how many run together.

These tasks spend most of their time *waiting* (on a network call, a browser, a
timer), so you want many of them going at once. Two easy approaches both break:

- **A new task for every event:** a sudden spike starts thousands at once and falls
  over — too many browsers, too many open connections.
- **One event at a time:** safe, but a single slow call (say a 2-second request)
  holds up everything behind it. All that waiting happens one after another instead
  of together.

A **pool** is the middle ground: a fixed number of workers (`max_slots`, say 20)
sharing the work. Up to 20 events run together — enough to overlap the waiting — and
never more, so nothing gets overwhelmed. Each pool has its own size, so a slow batch
of browser sessions can't hog the workers your API calls need.

You pick each pool's size from whatever its work is bound by — a RAM budget for
Playwright browsers, a rate limit for an HTTP API, a connection cap for a database.

Work is driven by **events** over a Redis stream: **producers** emit events on a
schedule, **consumers** handle them concurrently. The two sides are decoupled —
they share only an event name, never a reference — so you can run them together in
one process or scale them apart.

> Distribution name `archipellabs-runtime`; it imports as `runtime`.

## Install

```sh
pip install archipellabs-runtime  # requires Python 3.12+ and a Redis server
```

```python
from runtime import App, Pool, Scheduler
```

## Quickstart

A consumer (`Pool` + a flow) and a producer (`Scheduler` + `@every`), wired into
one `App`:

```python
import os
from runtime import App, Pool, Scheduler

# ── consumer: a Pool of flows ──────────────────────────────────────────────
orders = Pool("orders", max_slots=20)

@orders.flow(consumes="order.placed")
async def fulfill(ctx, event):
    print(f"fulfilling order {event['id']}")
    # ... do the work; called once per event, should be idempotent ...

# ── producer: a Scheduler that emits on an interval ────────────────────────
load = Scheduler("load")

@load.every("500ms")                       # emit twice a second
async def place_orders(ctx):
    await ctx.emit("order.placed", id=os.urandom(4).hex())

# ── wire it up and run ─────────────────────────────────────────────────────
app = App(redis="redis://localhost:6379/0")
app.include(orders)
app.include(load)
app.start()                                # blocking; Ctrl-C to stop
```

`app.start()` connects to Redis, spawns the consumer workers and the producer
loop, and runs until interrupted. You'll need a Redis on `:6379` — for local dev,
the quickest is:

```sh
docker run --rm -p 6379:6379 redis
```

Runnable examples live in [`examples/`](examples):

- **`minimal/`** — the smallest app (one pool, one flow, one producer; no deps).
- **`orders/`** — a `lifespan` resource on *both* sides (a store on the consumer,
  a catalog client on the producer). `main.py` spawns the `producer` and
  `consumer` as two processes (the split deployment); no external Python deps.
- **`playwright/`** — a browser-driven load sim (needs the `playwright` extra).

## Shared resources: the lifespan

A `Pool` (or `Scheduler`) can hold a resource opened once at boot and shared by
everything in it — a browser process, an HTTP client, a database pool. Provide a
`lifespan`: an async context manager `(config) -> resources`. Whatever it yields
is reachable as `ctx.resources`.

```python
from contextlib import asynccontextmanager
from runtime import Pool

@asynccontextmanager
async def browser(config):
    pw = await launch_browser(headless=config["headless"])
    try:
        yield {"browser": pw}              # → ctx.resources["browser"]
    finally:
        await pw.close()                   # torn down on shutdown

shop = Pool("shop", max_slots=12, lifespan=browser)

@shop.flow(consumes="shopping.session")
async def shop_session(ctx, event):
    page = await ctx.resources["browser"].new_context()
    try:
        ...
    finally:
        await page.close()                 # per-event teardown lives in the body

# config is injected at wiring time:
app.include(shop, config={"headless": True})
```

## Core concepts

- **Pool → flows (consumers).** A `Pool` groups *flows* — plain async
  `(ctx, event)` handlers, each bound to one event type with
  `@pool.flow(consumes=...)` — that share a lifespan and a concurrency budget
  (`max_slots`). Workers compete for messages; the pool's semaphore caps how many
  run at once.
- **Scheduler → producers.** The mirror of a `Pool`: *producers* are async `(ctx)`
  bodies bound with `@scheduler.every(interval)` and called on that interval.
  Intervals are durations — `"10min"`, `"1.5s"`, `"500ms"`, `"1h"`, or a number of
  seconds.
- **Context.** Every handler and producer gets a `ctx`: `ctx.resources` (the
  lifespan's shared resources), `ctx.config` (injected via
  `App.include(config=...)`), and `await ctx.emit(event_type, **payload)` to push
  an event.
- **Delivery.** Events flow through a Redis stream behind a small `Broker` protocol,
  at-least-once — a crash can redeliver a message — so handlers should be
  idempotent.

Both decorators have an imperative twin for dynamic wiring:
`pool.register(handler, consumes=...)` and `scheduler.register(producer, interval=...)`.

## Deployment

Because the two sides talk only through Redis, you can run everything in **one
process** (as in the quickstart) or **split** it across deployments — the same
`App`, with `include()` deciding what each entrypoint runs.

At scale, the simplest pattern is **one pool (or scheduler) per app**, gated by
`enabled`, and let your infra (Kubernetes, ECS, …) run and scale each independently.
Every entrypoint builds the same `App`; an env var picks its role:

```python
role = os.environ["ROLE"]
app.include(load,     enabled=role == "producer")   # schedulers — 1 replica
app.include(browsers, enabled=role == "browsers")   # heavy pool — N replicas
app.include(apis,     enabled=role == "apis")        # light pool — M replicas
app.start()
```

A disabled component is neither started nor armed, so one image + one env var per
deployment is all it takes — no per-role code, and each pool scales on its own.

To run several environments against **one** Redis (e.g. dev and staging on the
same box), give each its own `namespace` — it prefixes every stream key so their
streams and consumer groups can't mix:

```python
app = App(redis="redis://localhost:6379/0", namespace=os.environ["ENV"])  # "dev", "staging", …
```

## Development

```sh
uv sync --extra dev
uv run python -m pytest        # falls back to fakeredis if no Redis is running
uv run mypy
```

## License

MIT — see [LICENSE](LICENSE).
