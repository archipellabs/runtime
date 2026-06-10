"""The orders pipeline — a warehouse Pool (consumers) and a load Scheduler
(producers), each with its own lifespan resource.

The two sides share only the ``order.placed`` event type — never a reference.
main.py spawns them as two processes — producer.py (producers) and consumer.py
(consumers) — which in production would be separate deployments.
"""

import uuid
from contextlib import asynccontextmanager

from examples.orders.resources import connect_catalog, connect_store
from runtime import Pool, Scheduler


# ── consumers: a Pool whose lifespan opens a shared store, used by the flow ──
@asynccontextmanager
async def store_lifespan(config):
    store = await connect_store(config["dsn"])  # opened once, shared by all slots
    try:
        yield {"store": store}  # → ctx.resources["store"]
    finally:
        await store.close()


warehouse = Pool("warehouse", max_slots=8, lifespan=store_lifespan)


@warehouse.flow(consumes="order.placed")
async def fulfill(ctx, event):
    await ctx.resources["store"].fulfill(event["id"], event["sku"])
    print(f"[consumer] fulfilled order {event['id']} ({event['sku']})")


# ── producers: a Scheduler whose lifespan opens a shared catalog client ──────
@asynccontextmanager
async def catalog_lifespan(config):
    catalog = await connect_catalog()  # opened once, shared by all producers
    try:
        yield {"catalog": catalog}  # → ctx.resources["catalog"]
    finally:
        await catalog.close()


load = Scheduler("load", lifespan=catalog_lifespan)


@load.every("500ms", id="place-orders")
async def place_orders(ctx):
    sku = await ctx.resources["catalog"].low_stock_sku()  # producer reads its resource
    order_id = uuid.uuid4().hex[:8]
    await ctx.emit("order.placed", id=order_id, sku=sku)
    print(f"[producer] placed order {order_id} ({sku})")
