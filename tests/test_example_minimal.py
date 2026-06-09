import asyncio

from fakeredis.aioredis import FakeRedis

from examples.minimal.main import build_app
from runtime.redis_io import RedisBroker
from tests.helpers import wait_for_output


async def test_minimal_example_end_to_end(capsys):
    """Smoke: the real examples.minimal app, end-to-end on a fakeredis-backed
    broker. The scheduler's producer emits → the greet handler prints. Asserts
    the full producer → broker → handler path through App._serve."""
    app = build_app()
    broker = RedisBroker(FakeRedis(decode_responses=True))
    serve = asyncio.create_task(app._serve(broker))
    try:
        out = await wait_for_output(capsys, "[minimal] hello world")
    finally:
        serve.cancel()
        await asyncio.gather(serve, return_exceptions=True)
        await broker.aclose()

    assert "[minimal] hello world" in out
