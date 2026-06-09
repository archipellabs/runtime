import asyncio
import uuid

from runtime.broker import stream_name
from runtime.pool import slot_worker
from runtime.redis_io import GROUP, from_fields  # raw PEL probe helpers


async def _drain(*tasks):
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def _pending_messages(redis, stream: str, consumer: str):
    pending = await redis.xreadgroup(
        groupname=GROUP,
        consumername=consumer,
        streams={stream: "0"},
    )
    if not pending:
        return []
    return pending[0][1]


async def test_handler_runs_with_resources(broker, redis):
    consumes = f"ev-{uuid.uuid4().hex}"
    stream = stream_name(consumes)
    await broker.ensure_stream(stream)

    seen: list[dict] = []
    done = asyncio.Event()

    async def handler(ctx, event):
        assert ctx.resources["db"] == "DB"     # lifespan resources reach the handler
        seen.append(event)
        done.set()

    await broker.append(stream, {"sku": "X"})
    task = asyncio.create_task(
        slot_worker(
            broker, handler, consumes=consumes, consumer="c0",
            resources={"db": "DB"}, config={}, pool_sem=asyncio.Semaphore(4),
        )
    )
    await asyncio.wait_for(done.wait(), 2.0)
    await _drain(task)

    assert seen == [{"sku": "X"}]
    assert await _pending_messages(redis, stream, "c0") == []
    await redis.delete(stream)


async def test_handler_exception_leaves_unacked(broker, redis):
    consumes = f"ev-{uuid.uuid4().hex}"
    stream = stream_name(consumes)
    await broker.ensure_stream(stream)

    attempted = asyncio.Event()

    async def handler(ctx, event):
        attempted.set()
        raise RuntimeError("boom")

    await broker.append(stream, {"x": 1})
    task = asyncio.create_task(
        slot_worker(
            broker, handler, consumes=consumes, consumer="c0",
            resources={}, config={}, pool_sem=asyncio.Semaphore(1),
        )
    )
    await asyncio.wait_for(attempted.wait(), 2.0)
    await asyncio.sleep(0.05)   # give it time to (not) ack and loop
    await _drain(task)

    # message is still pending for c0 (unacked → stays in the PEL); raw probe
    pending = await _pending_messages(redis, stream, "c0")
    assert len(pending) == 1
    _msg_id, fields = pending[0]
    assert from_fields(fields) == {"x": 1}
    await redis.delete(stream)


async def test_pool_semaphore_serializes(broker, redis):
    consumes = f"ev-{uuid.uuid4().hex}"
    stream = stream_name(consumes)
    await broker.ensure_stream(stream)

    n = 3
    state = {"cur": 0, "max": 0, "done": 0}
    done = asyncio.Event()

    async def handler(ctx, event):
        state["cur"] += 1
        state["max"] = max(state["max"], state["cur"])
        await asyncio.sleep(0.02)
        state["cur"] -= 1
        state["done"] += 1
        if state["done"] == n:
            done.set()

    for i in range(n):
        await broker.append(stream, {"i": i})

    sem = asyncio.Semaphore(1)   # budget of 1 → strictly serial across all workers
    tasks = [
        asyncio.create_task(
            slot_worker(
                broker, handler, consumes=consumes, consumer=f"c{i}",
                resources={}, config={}, pool_sem=sem,
            )
        )
        for i in range(n)
    ]
    await asyncio.wait_for(done.wait(), 3.0)
    await _drain(*tasks)

    assert state["max"] == 1     # never two handlers in flight under a budget of 1
    await redis.delete(stream)
