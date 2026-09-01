"""Shared test helpers."""

import asyncio


class Gate:
    """Async step callable that parks forever on chosen calls.

    Lets a test freeze a workflow at a known point, simulate a crash
    (``Runtime.shutdown()`` is crash-equivalent), and resume with a fresh
    Runtime over the same store.
    """

    def __init__(self, hang_on=frozenset({1})):
        self.hang_on = set(hang_on)
        self.calls = 0
        self.reached = asyncio.Event()

    async def __call__(self):
        self.calls += 1
        if self.calls in self.hang_on:
            self.reached.set()
            await asyncio.Event().wait()  # park forever
        return f"gate-{self.calls}"

    async def wait_reached(self):
        await asyncio.wait_for(self.reached.wait(), timeout=5)


def counting_step(counters, key, result=None):
    """Async step that counts its executions."""

    async def step():
        counters[key] = counters.get(key, 0) + 1
        return result if result is not None else key

    step.__name__ = key
    return step


async def wait_for_events(store, run_id, kind, n, timeout=5.0):
    """Poll the store until the run's journal holds >= n events of ``kind``."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        events = [e for e in await store.load_events(run_id) if e.kind == kind]
        if len(events) >= n:
            return events
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {n} '{kind}' events (have {len(events)})")
        await asyncio.sleep(0.01)
