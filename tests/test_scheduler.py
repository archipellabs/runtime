import asyncio
import uuid

import pytest

from runtime import Scheduler
from runtime.broker import stream_name
from runtime.scheduler import parse_duration, run_every, run_once


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


def test_parse_duration_delay_allows_zero():
    assert parse_duration("0s", allow_zero=True) == 0.0
    assert parse_duration(0, allow_zero=True) == 0.0
    with pytest.raises(ValueError):
        parse_duration("-1s", allow_zero=True)


def test_every_registers():
    sched = Scheduler("s")

    @sched.every("10min", id="restock")
    async def restock(ctx): ...

    reg = sched._every[0]
    assert reg.handler is restock
    assert reg.interval == 600.0
    assert reg.id == "restock"


def test_every_id_defaults_to_fn_name():
    sched = Scheduler("s")

    @sched.every("1s")
    async def tick(ctx): ...

    assert sched._every[0].id == "tick"


def test_once_registers():
    sched = Scheduler("s")

    @sched.once(id="warmup")
    async def warmup(ctx): ...

    reg = sched._once[0]
    assert reg.handler is warmup
    assert reg.delay == 0.0
    assert reg.id == "warmup"


def test_once_with_delay():
    sched = Scheduler("s")

    @sched.once(delay="10min")
    async def warmup(ctx): ...

    assert sched._once[0].delay == 600.0


def test_once_id_defaults_to_fn_name():
    sched = Scheduler("s")

    @sched.once()
    async def warmup(ctx): ...

    assert sched._once[0].id == "warmup"


async def test_run_every_emits_with_resources(broker, redis):
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

    reg = sched._every[0]
    task = asyncio.create_task(
        run_every(broker, reg, resources={"api": "API"}, config={})
    )
    await asyncio.wait_for(done.wait(), 2.0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) >= 3
    await redis.delete(stream_name(event_type))


async def test_run_every_logs_and_continues_after_tick_failure(
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
            run_every(broker, sched._every[0], resources={}, config={})
        )
        await asyncio.wait_for(done.wait(), 2.0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert "every 'flaky' tick failed" in caplog.text
    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) == 1
    await redis.delete(stream_name(event_type))


async def test_run_once_emits_once_with_resources(broker, redis):
    """A one-shot producer reads its scheduler's lifespan resources and emits
    exactly once; the runner returns on its own (no cancel needed)."""
    event_type = f"load-{uuid.uuid4().hex}"
    state = {"n": 0}

    sched = Scheduler("s")

    @sched.once(id="t")
    async def prod(ctx):
        assert ctx.resources["api"] == "API"  # from the scheduler lifespan
        state["n"] += 1
        await ctx.emit(event_type, i=state["n"])

    await asyncio.wait_for(
        run_once(broker, sched._once[0], resources={"api": "API"}, config={}), 2.0
    )

    assert state["n"] == 1
    entries = await redis.xrange(stream_name(event_type))
    assert len(entries) == 1
    await redis.delete(stream_name(event_type))


async def test_run_once_waits_for_delay(broker, redis):
    """The delay defers the single fire — nothing is emitted before it elapses."""
    event_type = f"load-{uuid.uuid4().hex}"
    sched = Scheduler("s")

    @sched.once(delay="0.2s", id="t")
    async def prod(ctx):
        await ctx.emit(event_type, ok=True)

    task = asyncio.create_task(run_once(broker, sched._once[0], resources={}, config={}))
    await asyncio.sleep(0.02)  # well before the delay elapses
    assert len(await redis.xrange(stream_name(event_type))) == 0
    await asyncio.wait_for(task, 2.0)
    assert len(await redis.xrange(stream_name(event_type))) == 1
    await redis.delete(stream_name(event_type))


async def test_run_once_logs_and_returns_after_failure(caplog, broker, redis):
    """A one-shot that raises is logged and the runner still returns (no loop)."""
    sched = Scheduler("s")

    @sched.once(id="boom")
    async def prod(ctx):
        raise RuntimeError("nope")

    with caplog.at_level("ERROR", logger="runtime"):
        await asyncio.wait_for(
            run_once(broker, sched._once[0], resources={}, config={}), 2.0
        )

    assert "once 'boom' failed" in caplog.text
