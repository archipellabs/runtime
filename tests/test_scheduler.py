import asyncio
import uuid

import pytest

from runtime import Scheduler
from runtime.broker import stream_name
from runtime.scheduler import parse_duration, run_producer


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1s", 1.0),
        ("1.5s", 1.5),
        ("10min", 600.0),
        ("1h", 3600.0),
        ("500ms", 0.5),
        ("2m", 120.0),
        ("10", 10.0),
        (3, 3.0),
    ],
)
def test_parse_duration(value, expected):
    assert parse_duration(value) == expected


@pytest.mark.parametrize("bad", ["0s", "-1s", "abc", "10/min"])
def test_parse_duration_rejects_bad(bad):
    with pytest.raises(ValueError):
        parse_duration(bad)


def test_every_registers():
    sched = Scheduler("s")

    @sched.every("10min", id="restock")
    async def restock(ctx): ...

    reg = sched._producers[0]
    assert reg.handler is restock
    assert reg.interval == 600.0
    assert reg.id == "restock"


def test_every_id_defaults_to_fn_name():
    sched = Scheduler("s")

    @sched.every("1s")
    async def tick(ctx): ...

    assert sched._producers[0].id == "tick"


async def test_run_producer_emits_with_resources(broker, redis):
    """A producer reads its scheduler's lifespan resources via ctx.resources and
    emits — producer-side shared resources come from the scheduler's lifespan."""
    event_type = f"load-{uuid.uuid4().hex}"
    state = {"n": 0}
    done = asyncio.Event()

    sched = Scheduler("s")

    @sched.every("0.01s", id="t")
    async def prod(ctx):
        assert ctx.resources["api"] == "API"  # from the scheduler lifespan
        state["n"] += 1
        await ctx.emit(event_type, i=state["n"])
        if state["n"] >= 3:
            done.set()

    reg = sched._producers[0]
    task = asyncio.create_task(
        run_producer(broker, reg, resources={"api": "API"}, config={})
    )
    await asyncio.wait_for(done.wait(), 2.0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) >= 3
    await redis.delete(stream_name(event_type))


async def test_run_producer_logs_and_continues_after_tick_failure(
    caplog, broker, redis
):
    event_type = f"load-{uuid.uuid4().hex}"
    state = {"n": 0}
    done = asyncio.Event()
    sched = Scheduler("s")

    @sched.every("0.01s", id="flaky")
    async def prod(ctx):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("first tick failed")
        await ctx.emit(event_type, recovered=True)
        done.set()

    with caplog.at_level("ERROR", logger="runtime"):
        task = asyncio.create_task(
            run_producer(broker, sched._producers[0], resources={}, config={})
        )
        await asyncio.wait_for(done.wait(), 2.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert "producer 'flaky' tick failed" in caplog.text
    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) == 1
    await redis.delete(stream_name(event_type))
