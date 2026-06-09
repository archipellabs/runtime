"""Playwright load sim — simulated users on an e-commerce site.

A single Pool (`browser`, whose lifespan owns a shared browser process) and a
Scheduler (`shopping-load`) that drives sessions at a fixed rate. Run together via
main.py. Playwright is a heavy cost profile, so in production it gets its own
isolated deployment.

Playwright is imported LAZILY inside the lifespan, so this module imports and
prints topology without it installed. To actually run the flow:

    pip install -e '.[playwright]'
    playwright install chromium
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from runtime import Pool, Scheduler


# ── consumer: a Pool whose lifespan owns a shared browser process ────────────
@asynccontextmanager
async def browser_resource(config: dict) -> AsyncIterator[dict]:
    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=config["headless"])  # shared process
    try:
        yield {"browser": browser, "base_url": config["base_url"]}
    finally:
        await browser.close()
        await pw.stop()


browser_pool = Pool("browser", max_slots=12, lifespan=browser_resource)


@browser_pool.flow(consumes="shopping.session")
async def shopping(ctx, event):                 # HANDLE scope = one isolated user
    context = await ctx.resources["browser"].new_context()   # fresh cookies/cart
    try:
        page = await context.new_page()
        await page.goto(ctx.resources["base_url"] + "/category/shoes")
        # think-time, browse, add-to-cart, checkout would follow here
    finally:
        await context.close()


# ── producer: a Scheduler driving sessions (no lifespan needed) ──────────────
load = Scheduler("shopping-load")


@load.every("1.5s", id="shopping-load")         # ≈ 40/min
async def emit_session(ctx):
    await ctx.emit("shopping.session")
