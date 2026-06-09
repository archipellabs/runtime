import asyncio

from fakeredis.aioredis import FakeRedis

from runtime import App
from runtime.redis_io import RedisBroker

from examples.orders.pipeline import load, warehouse
from tests.helpers import wait_for_output


async def test_orders_pipeline_end_to_end(capsys):
    """The orders pipeline, both sides co-located in one process on fakeredis
    (main.py runs them as two processes against a real Redis). The load Scheduler
    (catalog lifespan) emits order.placed → the warehouse Pool (store lifespan)
    fulfills — exercising producer- AND consumer-side lifespan resources."""
    app = App(redis="unused://")
    app.include(warehouse, config={"dsn": "memory://"})
    app.include(load)

    broker = RedisBroker(FakeRedis(decode_responses=True))
    serve = asyncio.create_task(app._serve(broker))
    try:
        out = await wait_for_output(
            capsys,
            "[producer] placed order",
            "[consumer] fulfilled order",
        )
    finally:
        serve.cancel()
        await asyncio.gather(serve, return_exceptions=True)
        await broker.aclose()

    assert "[producer] placed order" in out
    assert "[consumer] fulfilled order" in out
