import asyncio
from typing import Any


async def wait_for_output(capsys: Any, *needles: str, timeout: float = 2.0) -> str:
    out = ""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        out += capsys.readouterr().out
        if all(needle in out for needle in needles):
            return out
        await asyncio.sleep(0.01)

    out += capsys.readouterr().out
    missing = [needle for needle in needles if needle not in out]
    raise AssertionError(f"missing output {missing!r}; got {out!r}")
