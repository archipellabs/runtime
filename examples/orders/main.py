"""Run the whole orders pipeline locally — spawns the consumer and producer as two
separate processes, the way they'd be deployed in production, but with one
command. Needs a Redis on :6379 (the two processes talk through it).

    python -m examples.orders.main

Ctrl-C stops both. To run a side on its own:
    python -m examples.orders.consumer      # consumers (scale to N)
    python -m examples.orders.producer      # producers (one instance)
"""

import subprocess
import sys

_MODULES = ["examples.orders.consumer", "examples.orders.producer"]


def main() -> None:
    # -u: unbuffered child stdout so the demo prints show up live (and aren't
    # lost in the buffer when the children are terminated on Ctrl-C).
    procs = [subprocess.Popen([sys.executable, "-u", "-m", m]) for m in _MODULES]
    try:
        for p in procs:
            p.wait()
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()


if __name__ == "__main__":
    main()
