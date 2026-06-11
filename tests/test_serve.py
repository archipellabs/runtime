import asyncio
import uuid
from contextlib import asynccontextmanager

import pytest

import runtime.app as app_module
from runtime import App, Pool, Scheduler
from runtime.broker import stream_name


class FakeBroker:
    def __init__(self) -> None:
        self.ensured: list[str] = []
        self.closed = False

    async def append(self, stream, payload):
        return "1-0"

    async def ensure_stream(self, stream):
        self.ensured.append(stream)

    async def claim(self, stream, *, consumer, count, block_ms):
        return []

    async def ack(self, stream, message_id):
        pass

    async def aclose(self):
        self.closed = True


async def test_serve_enters_lifespan_runs_handle_and_closes(broker, redis):
    consumes = f"ev-{uuid.uuid4().hex}"
    stream = stream_name(consumes)

    life: list[str] = []
    handled = asyncio.Event()

    @asynccontextmanager
    async def resource(config):
        life.append("open")
        try:
            yield {"tag": config["tag"]}  # POOL-scope resource from config
        finally:
            life.append("close")

    pool = Pool("p", max_slots=1, lifespan=resource)

    @pool.flow(consumes=consumes)
    async def handler(ctx, event):
        assert ctx.resources["tag"] == "T"  # lifespan resource reached the handler
        assert event == {"hello": 1}
        handled.set()

    app = App(redis="unused://")
    app.include(pool, config={"tag": "T"})

    # pre-seed one event, then run the consumer side with the injected broker
    await broker.ensure_stream(stream)
    await broker.append(stream, {"hello": 1})

    serve = asyncio.create_task(app._serve(broker))
    await asyncio.wait_for(handled.wait(), 3.0)
    serve.cancel()
    await asyncio.gather(serve, return_exceptions=True)

    assert life == ["open", "close"]  # lifespan finally ran on shutdown
    await redis.delete(stream)


async def test_serve_producer_drives_consumer(broker, redis):
    """End-to-end: a producer in the same App emits events that the pool's flow
    consumes — producer → broker → handler, all under one _serve."""
    event_type = f"sess-{uuid.uuid4().hex}"
    handled = asyncio.Event()
    seen: list[dict] = []

    pool = Pool("p", max_slots=2)

    @pool.flow(consumes=event_type)
    async def handler(ctx, event):
        seen.append(event)
        handled.set()

    sched = Scheduler("load")

    @sched.every("0.01s", id="load")
    async def emit(ctx):
        await ctx.emit(event_type, hit=1)

    app = App(redis="unused://")
    app.include(pool)
    app.include(sched)

    serve = asyncio.create_task(app._serve(broker))
    await asyncio.wait_for(handled.wait(), 3.0)
    serve.cancel()
    await asyncio.gather(serve, return_exceptions=True)

    assert seen and seen[0] == {"hit": 1}
    await redis.delete(stream_name(event_type))


async def test_serve_rejects_non_acm_lifespan(broker):
    """A lifespan that doesn't return an async context manager (the classic
    forgot-@asynccontextmanager mistake) fails fast at boot with a clear error."""

    def not_a_cm(config):  # missing @asynccontextmanager
        return {"oops": True}

    pool = Pool("p", max_slots=1, lifespan=not_a_cm)  # type: ignore[arg-type]

    @pool.flow(consumes="x")
    async def handler(ctx, event): ...

    app = App(redis="unused://")
    app.include(pool, config={})

    with pytest.raises(TypeError, match="async context manager"):
        await app._serve(broker)


async def test_serve_spawns_workers_from_overrides_and_flow_caps(monkeypatch):
    started = asyncio.Event()
    captured: list[dict] = []

    async def fake_slot_worker(
        broker, handler, *, consumes, consumer, resources, config, pool_sem
    ):
        captured.append(
            {
                "consumes": consumes,
                "consumer": consumer,
                "resources": resources,
                "config": config,
                "pool_sem": pool_sem,
            }
        )
        if len(captured) == 5:
            started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "slot_worker", fake_slot_worker)

    @asynccontextmanager
    async def resource(config):
        yield {"tag": config["tag"]}

    pool = Pool("p", max_slots=10, lifespan=resource)

    @pool.flow(consumes="default")
    async def default_handler(ctx, event): ...

    @pool.flow(consumes="limited", max_slots=2)
    async def limited_handler(ctx, event): ...

    app = App(redis="unused://")
    app.include(pool, max_slots=3, config={"tag": "T"})
    broker = FakeBroker()

    serve = asyncio.create_task(app._serve(broker))
    await asyncio.wait_for(started.wait(), 1.0)
    serve.cancel()
    await asyncio.gather(serve, return_exceptions=True)

    assert broker.ensured == [stream_name("default"), stream_name("limited")]
    assert [c["consumes"] for c in captured].count("default") == 3
    assert [c["consumes"] for c in captured].count("limited") == 2
    assert {id(c["pool_sem"]) for c in captured} == {id(captured[0]["pool_sem"])}
    assert all(c["resources"] == {"tag": "T"} for c in captured)
    assert all(c["config"] == {"tag": "T"} for c in captured)
    assert not broker.closed


async def test_serve_enters_scheduler_lifespan_and_passes_config(monkeypatch):
    started = asyncio.Event()
    life: list[str] = []
    seen: dict = {}

    async def fake_run_every(broker, reg, *, resources, config):
        seen["id"] = reg.id
        seen["resources"] = resources
        seen["config"] = config
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "run_every", fake_run_every)

    @asynccontextmanager
    async def resource(config):
        life.append(f"open:{config['api_url']}")
        try:
            yield {"api": config["api_url"]}
        finally:
            life.append("close")

    sched = Scheduler("load", lifespan=resource)

    @sched.every("1s", id="tick")
    async def tick(ctx): ...

    app = App(redis="unused://")
    app.include(sched, config={"api_url": "https://api.example"})
    broker = FakeBroker()

    serve = asyncio.create_task(app._serve(broker))
    await asyncio.wait_for(started.wait(), 1.0)
    serve.cancel()
    await asyncio.gather(serve, return_exceptions=True)

    assert life == ["open:https://api.example", "close"]
    assert seen == {
        "id": "tick",
        "resources": {"api": "https://api.example"},
        "config": {"api_url": "https://api.example"},
    }
    assert not broker.closed


async def test_serve_closes_entered_lifespans_when_startup_fails():
    life: list[str] = []

    @asynccontextmanager
    async def pool_resource(config):
        life.append("pool-open")
        try:
            yield {}
        finally:
            life.append("pool-close")

    @asynccontextmanager
    async def broken_scheduler_resource(config):
        raise RuntimeError("scheduler startup failed")
        yield {}

    pool = Pool("p", max_slots=1, lifespan=pool_resource)
    sched = Scheduler("s", lifespan=broken_scheduler_resource)

    @sched.every("1s")
    async def tick(ctx): ...

    app = App(redis="unused://")
    app.include(pool)
    app.include(sched)

    with pytest.raises(RuntimeError, match="scheduler startup failed"):
        await app._serve(FakeBroker())

    assert life == ["pool-open", "pool-close"]


async def test_serve_closes_owned_broker_but_not_injected(monkeypatch):
    owned = FakeBroker()
    monkeypatch.setattr(app_module, "connect", lambda url, *, namespace="": owned)

    await App(redis="redis://owned")._serve()
    assert owned.closed

    injected = FakeBroker()
    await App(redis="unused://")._serve(injected)
    assert not injected.closed
