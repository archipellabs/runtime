"""Minimal example — the smallest possible app.

One pool (no shared resource) with one flow, and one scheduler with one producer,
all in a single process. No external dependencies. Run:

    python -m examples.minimal.main
"""

import os

from runtime import App, Pool, Scheduler

pool = Pool("hello", max_slots=5)  # lifespan=None: no shared resource


@pool.flow(consumes="greeting")
async def greet(ctx, event):
    print(f"[minimal] hello {event['name']}")


greeter = Scheduler("greeter")  # lifespan=None: producer needs no resource


@greeter.every("1s")  # id defaults to the function name
async def emit_greeting(ctx):
    await ctx.emit("greeting", name="world")


def build_app() -> App:
    app = App(redis=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    app.include(pool)
    app.include(greeter)
    return app


if __name__ == "__main__":
    build_app().start()
