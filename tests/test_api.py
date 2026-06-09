import logging

import pytest

from runtime import App, Pool, Scheduler
from runtime.app import _check_unique_consumes
from runtime.broker import stream_name


def make_pool(name="p", budget=10):
    return Pool(name, max_slots=budget)


async def _noop(ctx, event): ...


def test_validate_duplicate_consumes_within_pool():
    pool = make_pool()
    pool.register(_noop, consumes="x")
    with pytest.raises(ValueError):
        pool.register(_noop, consumes="x")


def test_decorator_returns_func_unchanged():
    pool = make_pool()

    @pool.flow(consumes="y")
    async def handler(ctx, event): ...

    assert pool._flows[0].handler is handler   # decorator returns the fn unchanged
    assert len(pool._flows) == 1


def test_cap_over_budget_warns_not_raises():
    pool = make_pool(budget=5)
    with pytest.warns(UserWarning):
        pool.register(_noop, consumes="z", max_slots=10)


def test_pool_rejects_nonpositive_max_slots():
    for bad in (0, -1):
        with pytest.raises(ValueError, match="max_slots"):
            Pool("p", max_slots=bad)


def test_stream_name_mapping():
    assert stream_name("restock") == "flow:restock"


def test_check_unique_consumes_ok():
    p1, p2 = make_pool("p1"), make_pool("p2")
    p1.register(_noop, consumes="restock")
    p2.register(_noop, consumes="audit")
    _check_unique_consumes([p1, p2])  # distinct → no raise


def test_check_unique_consumes_across_pools():
    p1, p2 = make_pool("p1"), make_pool("p2")
    p1.register(_noop, consumes="dup")
    p2.register(_noop, consumes="dup")
    with pytest.raises(ValueError, match="fan-out"):
        _check_unique_consumes([p1, p2])


def test_include_skips_disabled_and_collects_pool():
    app = App(redis="redis://x")
    pool = make_pool()
    pool.register(_noop, consumes="restock")
    app.include(pool, enabled=False, max_slots=3, config={"x": 1})
    assert app._pools == []
    app.include(pool, enabled=True, max_slots=4, config={"x": 2})
    assert len(app._pools) == 1
    inc = app._pools[0]
    assert inc.pool is pool
    assert inc.max_slots == 4
    assert inc.config == {"x": 2}


_sched = Scheduler("s")


@_sched.every("5min", id="c1")
async def _producer(ctx): ...


def test_include_scheduler_and_rejects_garbage():
    app = App(redis="redis://x")
    app.include(_sched, enabled=True, config={"rate": "fast"})
    assert len(app._schedulers) == 1
    inc = app._schedulers[0]
    assert inc.scheduler is _sched
    assert inc.config == {"rate": "fast"}
    with pytest.raises(TypeError):
        app.include(object())


def test_log_topology(caplog):
    # start() blocks on the event loop, so assert on the topology log helper.
    app = App(redis="redis://x")
    pool = make_pool()
    pool.register(_noop, consumes="restock")
    app.include(pool)
    app.include(_sched)
    with caplog.at_level(logging.INFO, logger="runtime"):
        app._log_topology()
    assert "topology" in caplog.text.lower()
    assert "restock" in caplog.text
    assert "Scheduler" in caplog.text
    assert "c1" in caplog.text


def test_start_logs_topology_then_runs_serve(monkeypatch):
    app = App(redis="redis://x")
    calls: list[str] = []

    def fake_log_topology(self):
        calls.append("log")

    async def fake_serve(self):
        calls.append("serve")

    monkeypatch.setattr(App, "_log_topology", fake_log_topology)
    monkeypatch.setattr(App, "_serve", fake_serve)

    app.start()

    assert calls == ["log", "serve"]


def test_start_handles_keyboard_interrupt(monkeypatch, caplog):
    app = App(redis="redis://x")
    calls: list[str] = []

    def fake_log_topology(self):
        calls.append("log")

    async def fake_serve(self):
        calls.append("serve")
        raise KeyboardInterrupt

    monkeypatch.setattr(App, "_log_topology", fake_log_topology)
    monkeypatch.setattr(App, "_serve", fake_serve)

    with caplog.at_level(logging.INFO, logger="runtime"):
        app.start()

    assert calls == ["log", "serve"]
    assert "interrupted" in caplog.text
