"""Broker — the message-transport seam.

The runtime depends only on this Protocol; the concrete backend (Redis Streams
today, in ``redis_io.py``) is chosen at the App wiring root. The queue is the
only seam between producers and consumers, made literal here: ``append``
produces, ``claim``/``ack`` consume with at-least-once delivery.

``stream_name`` lives here because it is backend-agnostic routing — which logical
stream an event type maps to (1:1 with a flow). It is deterministic, so a
producer in one process and a consumer in another agree on the stream by sharing
only the event-type string.
"""

from typing import Final, Protocol

from runtime.types import Event, Payload

STREAM_PREFIX: Final = "flow"


def stream_name(event_type: str) -> str:
    """The logical stream that carries a given event type (1:1 with a flow)."""
    return f"{STREAM_PREFIX}:{event_type}"


class Broker(Protocol):
    """What the runtime needs from a queue backend — a consumer-group-style
    at-least-once contract. A Redis-Streams impl lives in ``redis_io.py``; a SQL
    impl (e.g. Postgres ``SELECT … FOR UPDATE SKIP LOCKED``) would implement the
    same five methods. Reclaim of stale-unacked messages is not implemented yet."""

    async def append(self, stream: str, payload: Payload) -> str:
        """Produce: add an event to a stream. Returns the message id."""
        ...

    async def ensure_stream(self, stream: str) -> None:
        """Idempotently set up a stream for consumption (Redis: create the
        consumer group with MKSTREAM; SQL: ensure the table/partition)."""
        ...

    async def claim(
        self, stream: str, *, consumer: str, count: int, block_ms: int
    ) -> list[tuple[str, Event]]:
        """Hand up to ``count`` never-yet-delivered messages to ``consumer``,
        parking up to ``block_ms`` if the stream is empty. Returns
        ``[(message_id, event), ...]``; empty on timeout. An un-acked message
        stays pending for redelivery (reclaim is not implemented yet)."""
        ...

    async def ack(self, stream: str, message_id: str) -> None:
        """Mark a message done (Redis: XACK — drops it from the pending set)."""
        ...

    async def aclose(self) -> None:
        """Close the underlying connection."""
        ...
