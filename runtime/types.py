"""Shared type vocabulary for the runtime's public surface.

The dict aliases are all ``dict[str, Any]`` underneath, but the distinct names
document intent at every call site (a payload is not a config is not a set of
resources).
"""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, TypeAlias

Payload: TypeAlias = dict[str, Any]
"""The kwargs handed to ``ctx.emit(type, **payload)`` — the body of an event."""

Event: TypeAlias = dict[str, Any]
"""A delivered event: the decoded payload a flow handler receives."""

Resources: TypeAlias = dict[str, Any]
"""What a ``lifespan`` yields; exposed to handlers/producers as ``ctx.resources``."""

Config: TypeAlias = dict[str, Any]
"""Deployment config injected via ``App.include(config=...)``; reaches the
lifespan as its second argument and handlers/producers as ``ctx.config``."""

Lifespan: TypeAlias = Callable[[Config], AbstractAsyncContextManager[Resources]]
"""A container's shared-resource factory (POOL scope). Called once with the
deployment ``config`` and returns an async context manager whose yielded mapping
becomes ``ctx.resources`` and whose teardown runs on shutdown. Typically an
``@asynccontextmanager`` async generator."""
