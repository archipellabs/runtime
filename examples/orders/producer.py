"""Producer process — runs the producer side (the load Scheduler). One instance.

    python -m examples.orders.producer

Point it at the same Redis as the consumer: the `order.placed` events it emits are
fulfilled over there.
"""

import os

from runtime import App

from examples.orders.pipeline import load


def build_app() -> App:
    app = App(redis=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    app.include(load)
    return app


if __name__ == "__main__":
    build_app().start()
