"""Consumer process — runs the consumer side (the warehouse Pool). Scale to N.

    python -m examples.orders.consumer

Point it at the same Redis as the producer.
"""

import os

from examples.orders.pipeline import warehouse
from runtime import App


def build_app() -> App:
    app = App(redis=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    app.include(warehouse, config={"dsn": os.environ.get("DB_DSN", "memory://")})
    return app


if __name__ == "__main__":
    build_app().start()
