"""Cooperative cancellation with saga compensation."""

import asyncio

import pytest
from helpers import Gate, wait_for_events

from sicim import Kind, Runtime, RunStatus, WorkflowCancelled, workflow


def make_comp(order, name):
    async def comp():
        order.append(name)

    comp.__name__ = name
    return comp


async def step_ok():
    return "ok"


async def test_cancel_waiting_run_compensates(rt):
    order = []

    @workflow(name="wf_cancel_wait")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=make_comp(order, "c1"))
        await ctx.wait_event("never")

    handle = await rt.start(wf, run_id="c1")
    await wait_for_events(rt.store, "c1", Kind.WAIT_CREATED, 1)
    await rt.cancel("c1")
    with pytest.raises(WorkflowCancelled):
        await handle.result()

    assert order == ["c1"]
    record = await rt.status("c1")
    assert record.status is RunStatus.CANCELLED
    assert record.cancel_requested is True


async def test_cancel_interrupts_long_sleep_promptly(rt):
    @workflow(name="wf_cancel_sleep")
    async def wf(ctx):
        await ctx.sleep(300)
        return "never"

    handle = await rt.start(wf, run_id="c2")
    await wait_for_events(rt.store, "c2", Kind.TIMER_CREATED, 1)
    await rt.cancel("c2")
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=3)
    assert (await rt.status("c2")).status is RunStatus.CANCELLED


async def test_cancel_persisted_run_replays_then_compensates(store):
    order = []
    gate = Gate()

    @workflow(name="wf_cancel_down")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=make_comp(order, "c1"))
        await ctx.step(gate, name="gate")

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="c3")
    await gate.wait_reached()
    await rt1.shutdown()

    rt2 = Runtime(store)
    await rt2.cancel("c3")  # cancels a run that is not in memory
    handle = await rt2.resume("c3")
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=3)

    assert order == ["c1"]  # compensation from the replayed stack
    assert gate.calls == 1  # the pending step was NOT re-executed after cancel
    assert (await rt2.status("c3")).status is RunStatus.CANCELLED
    await rt2.shutdown()


async def test_cancel_terminal_run_is_noop(rt):
    @workflow(name="wf_cancel_done")
    async def wf(ctx):
        return 1

    handle = await rt.start(wf, run_id="c4")
    await handle.result()
    await rt.cancel("c4")  # must not raise or change anything
    assert (await rt.status("c4")).status is RunStatus.COMPLETED
