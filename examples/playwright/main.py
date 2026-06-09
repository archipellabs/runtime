"""Single-process entrypoint — producer AND consumer in one process.

Producers and consumers are connected only through Redis, so they can run as two
separate deployments (the production scaling pattern) OR together in a single
process, as here. Same App, you just include() both. Handy for local dev and
self-contained demos.

Run: `python -m examples.playwright.main`
"""

import os

from runtime import App

from examples.playwright.pipeline import browser_pool, load


def build_app() -> App:
    app = App(redis=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    app.include(
        browser_pool,                          # consumer (the "how")
        enabled=True,
        config={
            "headless": True,
            "base_url": os.environ.get("TARGET_SITE", "http://localhost:8000"),
        },
    )
    app.include(load, enabled=True)            # scheduler (the "when") — same process
    return app


if __name__ == "__main__":
    build_app().start()
