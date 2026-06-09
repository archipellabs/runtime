import uuid

from runtime.broker import stream_name
from runtime.redis_io import PAYLOAD_FIELD, RedisBroker, from_fields, to_fields


def test_field_codec_roundtrips_nested_payload():
    payload = {
        "sku": "ABC",
        "qty": 3,
        "tags": ["sale", "priority"],
        "meta": {"fragile": True, "note": None},
    }

    fields = to_fields(payload)

    assert set(fields) == {PAYLOAD_FIELD}
    assert from_fields(fields) == payload


def test_from_fields_missing_payload_returns_empty_event():
    assert from_fields({"other": "{}"}) == {}


async def test_redisbroker_roundtrip(broker, redis):
    stream = stream_name(f"test-{uuid.uuid4().hex}")

    await broker.ensure_stream(stream)
    await broker.ensure_stream(stream)  # idempotent: BUSYGROUP is a no-op

    msg_id = await broker.append(stream, {"sku": "ABC", "qty": 3})
    assert msg_id

    msgs = await broker.claim(stream, consumer="c1", count=1, block_ms=1000)
    assert len(msgs) == 1
    read_id, payload = msgs[0]
    assert read_id == msg_id
    assert payload == {"sku": "ABC", "qty": 3}

    await broker.ack(stream, read_id)

    # nothing left undelivered
    assert await broker.claim(stream, consumer="c1", count=1, block_ms=100) == []

    await redis.delete(stream)


async def test_namespace_isolates_streams(redis):
    """Two brokers with different namespaces over one Redis don't see each other's
    messages — the case for running dev + staging against a single Redis."""
    event = stream_name(f"test-{uuid.uuid4().hex}")
    dev = RedisBroker(redis, namespace="dev")
    stg = RedisBroker(redis, namespace="staging")

    for b in (dev, stg):
        await b.ensure_stream(event)
    await dev.append(event, {"env": "dev"})
    await stg.append(event, {"env": "staging"})

    dev_msgs = await dev.claim(event, consumer="c", count=10, block_ms=100)
    stg_msgs = await stg.claim(event, consumer="c", count=10, block_ms=100)

    assert [p["env"] for _, p in dev_msgs] == ["dev"]
    assert [p["env"] for _, p in stg_msgs] == ["staging"]
    assert await redis.exists(f"dev:{event}", f"staging:{event}") == 2  # keys are prefixed

    await redis.delete(f"dev:{event}", f"staging:{event}")
