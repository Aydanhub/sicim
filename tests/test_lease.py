"""Worker leases: exclusive driving, TTL takeover, cross-worker signal/cancel."""

import asyncio

import pytest
from helpers import Gate, counting_step, wait_for_events

from sicim import Kind, LeaseUnavailable, Runtime, RunStatus, WorkflowCancelled, workflow


async def test_leased_run_cannot_be_driven_by_second_worker(store):
    gate = Gate()

    @workflow(name="wf_lease_basic")
    async def wf(ctx):
        g = await ctx.step(gate, name="gate")
        return g

    rt1 = Runtime(store, worker_id="w1")
    await rt1.start(wf, run_id="L1")
    await gate.wait_reached()

    rt2 = Runtime(store, worker_id="w2")
    with pytest.raises(LeaseUnavailable):
        await rt2.resume("L1")
    assert await rt2.recover() == []  # leased runs are skipped, not stolen

    await rt1.shutdown()  # graceful stop releases the lease immediately
    [handle] = await rt2.recover()
    assert await handle.result() == "gate-2"
    await rt2.shutdown()


async def test_expired_lease_is_claimable(store):
    gate = Gate()

    @workflow(name="wf_lease_expiry")
    async def wf(ctx):
        return await ctx.step(gate, name="gate")

    rt1 = Runtime(store, worker_id="w1")
    await rt1.start(wf, run_id="L2")
    await gate.wait_reached()
    await rt1.shutdown()

    # A worker that died without releasing (hard crash) leaves a stale lease.
    assert await store.try_acquire_lease("L2", "dead-worker", 0.05)
    rt2 = Runtime(store, worker_id="w2")
    with pytest.raises(LeaseUnavailable) as excinfo:
        await rt2.resume("L2")
    assert excinfo.value.owner == "dead-worker"

    await asyncio.sleep(0.1)  # TTL expires
    handle = await rt2.resume("L2")
    assert await handle.result() == "gate-2"
    await rt2.shutdown()


async def test_heartbeat_keeps_long_run_leased(store):
    counters = {}

    async def slow_work():
        counters["work"] = counters.get("work", 0) + 1
        await asyncio.sleep(0.4)
        return "done"

    @workflow(name="wf_lease_heartbeat")
    async def wf(ctx):
        return await ctx.step(slow_work)

    # ttl (0.15) is far shorter than the run (0.4); heartbeats must keep it.
    rt1 = Runtime(store, worker_id="w1", lease_ttl=0.15)
    handle = await rt1.start(wf, run_id="L3")
    rt2 = Runtime(store, worker_id="w2")
    for _ in range(3):
        await asyncio.sleep(0.1)
        assert await rt2.recover() == []

    assert await handle.result() == "done"
    assert counters["work"] == 1  # never stolen, never double-driven
    await rt1.shutdown()


async def test_signal_from_another_worker_via_polling(store):
    @workflow(name="wf_lease_xsignal")
    async def wf(ctx):
        return await ctx.wait_event("go")

    rt1 = Runtime(store, worker_id="w1", signal_poll_interval=0.05)
    handle = await rt1.start(wf, run_id="L4")
    await wait_for_events(store, "L4", Kind.WAIT_CREATED, 1)

    # w2 cannot wake w1's waiter in-process; the store inbox + poll carries it.
    rt2 = Runtime(store, worker_id="w2")
    await rt2.signal("L4", "go", {"n": 7})
    assert await asyncio.wait_for(handle.result(), timeout=3) == {"n": 7}
    await rt1.shutdown()


async def test_cancel_from_another_worker_via_heartbeat(store):
    order = []

    async def step_ok():
        return "ok"

    async def comp():
        order.append("c1")

    @workflow(name="wf_lease_xcancel")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=comp)
        await ctx.wait_event("never")

    rt1 = Runtime(store, worker_id="w1", lease_ttl=0.15, signal_poll_interval=0.05)
    handle = await rt1.start(wf, run_id="L5")
    await wait_for_events(store, "L5", Kind.WAIT_CREATED, 1)

    rt2 = Runtime(store, worker_id="w2")
    await rt2.cancel("L5")  # persists the flag; w1's heartbeat picks it up
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=3)
    assert order == ["c1"]
    assert (await rt2.status("L5")).status is RunStatus.CANCELLED
    await rt1.shutdown()
