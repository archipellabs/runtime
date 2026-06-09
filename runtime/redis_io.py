"""RedisBroker — the Redis Streams implementation of the Broker seam.

Deliberately minimal: no dedup, no reclaim, no retry yet (planned). The event
payload is json-encoded into a single stream field, so producers and consumers
never argue about field layout. The consumer-group name is an internal Redis-ism
(a SQL backend has no equivalent), so it lives here, not in the Broker contract.
"""

import asyncio
import json
from typing import Any, Final, cast

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from runtime.broker import Broker
from runtime.types import Event, Payload

PAYLOAD_FIELD: Final = "data"
GROUP: Final = "workers"

# We open every client with decode_responses=True, so every reply is `str`. The
# redis-py stubs are response-type-agnostic (they return bytes|str|int|… unions
# and an opaque nested shape for XREADGROUP), so we cast at this single library
# boundary instead of scattering type:ignores through the runtime.
_RawRead = list[tuple[str, list[tuple[str, dict[str, str]]]]]


def to_fields(payload: Payload) -> dict[str, str]:
    """Encode an event payload into stream fields (one json field)."""
    return {PAYLOAD_FIELD: json.dumps(payload, separators=(",", ":"))}


def from_fields(fields: dict[str, str]) -> Event:
    """Decode stream fields back into the event payload."""
    raw = fields.get(PAYLOAD_FIELD)
    return json.loads(raw) if raw is not None else {}


class RedisBroker:
    """Broker backed by Redis Streams + a consumer group. Implements the Broker
    Protocol structurally (no inheritance needed)."""

    def __init__(self, redis: Redis, *, namespace: str = "") -> None:
        self._redis = redis
        self._namespace = namespace

    def _key(self, stream: str) -> str:
        """Physical Redis key for a logical stream, scoped by the namespace so
        several environments (e.g. dev + staging) can share one Redis without
        their streams and consumer groups mixing. Empty namespace = no prefix."""
        return f"{self._namespace}:{stream}" if self._namespace else stream

    async def append(self, stream: str, payload: Payload) -> str:
        return cast(str, await self._redis.xadd(self._key(stream), cast(Any, to_fields(payload))))

    async def ensure_stream(self, stream: str) -> None:
        # Create the consumer group + stream (MKSTREAM). Idempotent: an existing
        # group (BUSYGROUP) is a no-op, so it's safe on every boot / every replica.
        try:
            await self._redis.xgroup_create(
                name=self._key(stream), groupname=GROUP, id="0", mkstream=True
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def claim(
        self, stream: str, *, consumer: str, count: int, block_ms: int
    ) -> list[tuple[str, Event]]:
        try:
            resp = cast(
                "_RawRead | None",
                await self._redis.xreadgroup(
                    groupname=GROUP,
                    consumername=consumer,
                    streams={self._key(stream): ">"},
                    count=count,
                    block=block_ms,
                ),
            )
        except RedisTimeoutError:
            # The BLOCK window elapsed with no new messages: redis-py surfaces the
            # client read-timeout as TimeoutError. Treat it as an empty poll. A
            # cancelled read (shutdown) reaches here the same way, so honor a
            # pending cancellation instead of looping.
            task = asyncio.current_task()
            if task is not None and task.cancelling():
                raise asyncio.CancelledError
            return []
        if not resp:
            return []
        out: list[tuple[str, Event]] = []
        for _stream, messages in resp:
            for msg_id, fields in messages:
                out.append((msg_id, from_fields(fields)))
        return out

    async def ack(self, stream: str, message_id: str) -> None:
        await self._redis.xack(self._key(stream), GROUP, message_id)

    async def aclose(self) -> None:
        await self._redis.aclose()


def connect(url: str, *, namespace: str = "") -> Broker:
    """Open a Redis-backed broker (``decode_responses=True`` → str in/out).
    ``namespace`` prefixes every stream key to isolate environments sharing one
    Redis. When a second backend lands, this becomes a scheme-dispatching factory."""
    return RedisBroker(Redis.from_url(url, decode_responses=True), namespace=namespace)
