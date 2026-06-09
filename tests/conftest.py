import os
import socket

import pytest_asyncio

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _real_redis_reachable(host: str = "localhost", port: int = 6379) -> bool:
    try:
        socket.create_connection((host, port), timeout=0.5).close()
        return True
    except OSError:
        return False


@pytest_asyncio.fixture
async def redis():
    """Raw client (for direct assertions): real Redis when reachable, else
    fakeredis so the suite runs without Docker."""
    if _real_redis_reachable():
        from redis.asyncio import Redis

        r = Redis.from_url(REDIS_URL, decode_responses=True)
    else:
        from fakeredis.aioredis import FakeRedis

        r = FakeRedis(decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


@pytest_asyncio.fixture
async def broker(redis):
    """A RedisBroker over the SAME client as the `redis` fixture, so a test can
    emit through the broker and assert on the raw client (shared data)."""
    from runtime.redis_io import RedisBroker

    return RedisBroker(redis)
