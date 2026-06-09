import json
import uuid

from runtime.broker import stream_name
from runtime.context import RuntimeContext


async def test_emit_lands_on_stream(broker, redis):
    """ctx.emit(type, **payload) appends the json payload to the routed stream —
    even with no consumer set up (producer-only-process case)."""
    event_type = f"restock-{uuid.uuid4().hex}"
    ctx = RuntimeContext(broker)
    assert ctx.resources == {} and ctx.config == {}  # producer-style defaults

    await ctx.emit(event_type, sku="ABC", qty=3)

    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) == 1
    _msg_id, fields = entries[0]
    assert json.loads(fields["data"]) == {"sku": "ABC", "qty": 3}

    await redis.delete(stream_name(event_type))


async def test_emit_then_consume_roundtrip(broker, redis):
    event_type = f"shopping-{uuid.uuid4().hex}"
    stream = stream_name(event_type)
    await broker.ensure_stream(stream)

    ctx = RuntimeContext(broker, config={"base_url": "http://x"})
    assert ctx.config == {"base_url": "http://x"}
    await ctx.emit(event_type, name="world")

    msgs = await broker.claim(stream, consumer="c1", count=1, block_ms=500)
    assert len(msgs) == 1
    _id, payload = msgs[0]
    assert payload == {"name": "world"}

    await redis.delete(stream)
