"""Store push notifications: cross-worker signal & cancel without poll waits.

The PushStore tests exercise the Runtime's subscription plumbing on every
platform; the pg tests prove the real LISTEN/NOTIFY path end to end (they run
only under the postgres store param). In both, poll intervals and heartbeats
are set far beyond the assertion timeouts, so only push can deliver in time.
"""

import asyncio

import pytest
from helpers import wait_for_events

from sicim import InMemoryStore, Kind, Runtime, RunStatus, Store, WorkflowCancelled, workflow


class PushStore(InMemoryStore):
    """InMemoryStore plus a subscribe() push channel, mimicking LISTEN/NOTIFY."""

    def __init__(self):
        super().__init__()
        self.callbacks = []

    async def subscribe(self, on_notify):
        self.callbacks.append(on_notify)

        async def unsubscribe():
            self.callbacks.remove(on_notify)

        return unsubscribe

    def _notify(self, kind, run_id):
        for callback in list(self.callbacks):
            callback(kind, run_id)

    async def append_signal(self, run_id, name, payload):
        seq = await super().append_signal(run_id, name, payload)
        self._notify("signal", run_id)
        return seq

    async def update_run(self, run_id, **kwargs):
        await super().update_run(run_id, **kwargs)
        if kwargs.get("cancel_requested"):
            self._notify("cancel", run_id)


async def test_push_signal_wakes_waiting_run_across_workers():
    store = PushStore()

    @workflow(name="wf_push_signal")
    async def wf(ctx):
        return await ctx.wait_event("go")

    # Poll far beyond the timeout: only the push channel can deliver in time.
    rt1 = Runtime(store, worker_id="w1", signal_poll_interval=60.0)
    handle = await rt1.start(wf, run_id="N1")
    await wait_for_events(store, "N1", Kind.WAIT_CREATED, 1)
    assert store.callbacks  # the driving worker subscribed

    rt2 = Runtime(store, worker_id="w2")
    await rt2.signal("N1", "go", {"n": 7})
    assert await asyncio.wait_for(handle.result(), timeout=3) == {"n": 7}

    await rt1.shutdown()
    await rt2.shutdown()
    assert not store.callbacks  # shutdown unsubscribed


async def test_push_cancel_reaches_driving_worker_before_heartbeat():
    store = PushStore()
    order = []

    async def ok():
        return "ok"

    async def comp():
        order.append("c1")

    @workflow(name="wf_push_cancel")
    async def wf(ctx):
        await ctx.step(ok, name="s1", compensate=comp)
        await ctx.wait_event("never")

    # Heartbeat (ttl/3 = 20s) and poll (60s) are useless within the timeout.
    rt1 = Runtime(store, worker_id="w1", lease_ttl=60.0, signal_poll_interval=60.0)
    handle = await rt1.start(wf, run_id="N2")
    await wait_for_events(store, "N2", Kind.WAIT_CREATED, 1)

    rt2 = Runtime(store, worker_id="w2")
    await rt2.cancel("N2")
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=3)
    assert order == ["c1"]
    assert (await rt2.status("N2")).status is RunStatus.CANCELLED

    await rt1.shutdown()
    await rt2.shutdown()


def _needs_push(store):
    if type(store).subscribe is Store.subscribe:
        pytest.skip("backend has no push channel (memory/sqlite)")


async def test_pg_notify_delivers_signal(store):
    _needs_push(store)

    @workflow(name="wf_pg_push_signal")
    async def wf(ctx):
        return await ctx.wait_event("go")

    rt1 = Runtime(store, worker_id="w1", signal_poll_interval=60.0)
    handle = await rt1.start(wf, run_id="PGN1")
    await wait_for_events(store, "PGN1", Kind.WAIT_CREATED, 1)

    rt2 = Runtime(store, worker_id="w2")
    await rt2.signal("PGN1", "go", {"via": "notify"})
    assert await asyncio.wait_for(handle.result(), timeout=5) == {"via": "notify"}

    await rt1.shutdown()
    await rt2.shutdown()


async def test_pg_notify_delivers_cancel(store):
    _needs_push(store)

    @workflow(name="wf_pg_push_cancel")
    async def wf(ctx):
        await ctx.wait_event("never")

    rt1 = Runtime(store, worker_id="w1", lease_ttl=60.0, signal_poll_interval=60.0)
    handle = await rt1.start(wf, run_id="PGN2")
    await wait_for_events(store, "PGN2", Kind.WAIT_CREATED, 1)

    rt2 = Runtime(store, worker_id="w2")
    await rt2.cancel("PGN2")
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=5)
    assert (await rt2.status("PGN2")).status is RunStatus.CANCELLED

    await rt1.shutdown()
    await rt2.shutdown()
