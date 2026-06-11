"""archipellabs-runtime — an async Redis runtime for load simulation and
orchestration. Public API.

Two families of objects, connected only by an event name (through a Redis
stream): producers (a `Scheduler`'s `@every`/`@once` bodies) emit; consumers (a
`Pool`'s `@flow` handlers) handle. App.include() wires them; App.start() runs.
"""

from runtime._version import __version__
from runtime.app import App
from runtime.broker import Broker
from runtime.context import Context, Handler, ProducerFn
from runtime.pool import FlowRegistration, Pool
from runtime.scheduler import EveryRegistration, OnceRegistration, Scheduler
from runtime.types import Config, Event, Lifespan, Payload, Resources

__all__ = [
    "__version__",
    # core
    "App",
    "Pool",
    "Scheduler",
    "Context",
    "Handler",
    "ProducerFn",
    "FlowRegistration",
    "EveryRegistration",
    "OnceRegistration",
    # backend seam
    "Broker",
    # type vocabulary
    "Lifespan",
    "Payload",
    "Event",
    "Resources",
    "Config",
]
