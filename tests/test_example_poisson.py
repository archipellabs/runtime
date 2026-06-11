import asyncio
import random

from fakeredis.aioredis import FakeRedis

from examples.poisson.main import build_app
from runtime.redis_io import RedisBroker
from tests.helpers import wait_for_output


async def test_poisson_example_end_to_end(capsys):
    """The poisson app on fakeredis: a scheduler opens a window, the `generator`
    pool consumes it and emits a Poisson burst of `request` events, the `workers`
    pool consumes those — exercising a pool that is consumer AND producer. The
    seed fixes the draw so at least one arrival is emitted."""
    random.seed(0)
    app = build_app()
    broker = RedisBroker(FakeRedis(decode_responses=True))
    serve = asyncio.create_task(app._serve(broker))
    try:
        out = await wait_for_output(
            capsys,
            "[generator] window",
            "[workers] handling request",
        )
    finally:
        serve.cancel()
        await asyncio.gather(serve, return_exceptions=True)
        await broker.aclose()

    assert "[generator] window" in out
    assert "[workers] handling request" in out
