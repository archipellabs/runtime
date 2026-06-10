"""In-memory fakes for the orders example — no external dependencies, so the
example runs against fakeredis out of the box. In a real app these would be a
real database driver and an HTTP API client.
"""

import asyncio
import random

_SKUS = ["SKU-shoes", "SKU-shirt", "SKU-hat", "SKU-socks"]


class Catalog:
    """Fake catalog/inventory API client — what the Scheduler's lifespan provides."""

    async def low_stock_sku(self) -> str:
        await asyncio.sleep(0)  # stand-in for a network call
        return random.choice(_SKUS)

    async def close(self) -> None: ...


class Store:
    """Fake order store / DB — what the Pool's lifespan provides."""

    def __init__(self) -> None:
        self.fulfilled: list[str] = []

    async def fulfill(self, order_id: str, sku: str) -> None:
        await asyncio.sleep(0)  # stand-in for a DB write
        self.fulfilled.append(order_id)

    async def close(self) -> None: ...


async def connect_catalog() -> Catalog:
    return Catalog()


async def connect_store(dsn: str) -> Store:
    return Store()
