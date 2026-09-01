"""Durable timers and external events (signals)."""

import asyncio
import time

import pytest
from helpers import Gate, wait_for_events

from sicim import Kind, Runtime, SicimError, WaitTimeout, workflow


async def test_sleep_resumes_with_remaining_time(store):
    @workflow(name="wf_timer_resume")
    async def wf(ctx):
        await ctx.sleep(1.0)
        return "woke"

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="t1")
    [created] = await wait_for_events(store, "t1", Kind.TIMER_CREATED, 1)
    fire_at = created.payload["fire_at"]
    await asyncio.sleep(0.5)
    await rt1.shutdown()  # crash halfway through the sleep

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    assert await handle.result() == "woke"
    finished = time.time()

    # The timer fired at (about) its original deadline: the resume waited only
    # the remaining ~0.5s instead of restarting the full 1.0s sleep.
    assert finished >= fire_at
    assert finished <= fire_at + 0.4
    assert len(await wait_for_events(store, "t1", Kind.TIMER_CREATED, 1)) == 1
    await rt2.shutdown()


async def test_elapsed_timer_fires_immediately_on_resume(store):
    @workflow(name="wf_timer_elapsed")
    async def wf(ctx):
        await ctx.sleep(0.2)
        return "late"

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="t2")
    await wait_for_events(store, "t2", Kind.TIMER_CREATED, 1)
    await rt1.shutdown()
    await asyncio.sleep(0.3)  # deadline passes while "down"

    rt2 = Runtime(store)
    started = time.time()
    [handle] = await rt2.recover()
    assert await handle.result() == "late"
    assert time.time() - started < 0.2  # no re-wait
    await rt2.shutdown()


async def test_wait_event_delivers_payload(rt):
    @workflow(name="wf_event_basic")
    async def wf(ctx):
        approval = await ctx.wait_event("approval")
        return {"approved_by": approval["by"]}

    await rt.start(wf, run_id="e1")
    await wait_for_events(rt.store, "e1", Kind.WAIT_CREATED, 1)
    await rt.signal("e1", "approval", {"by": "ayse"})
    handle = await rt.resume("e1")
    assert await handle.result() == {"approved_by": "ayse"}


async def test_signal_buffered_while_run_is_down(store):
    gate = Gate()

    @workflow(name="wf_event_buffered")
    async def wf(ctx):
        await ctx.step(gate, name="gate")
        payload = await ctx.wait_event("go")
        return payload

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="e2")
    await gate.wait_reached()
    await rt1.shutdown()

    # Signalling a persisted-but-inactive run wakes it automatically.
    rt2 = Runtime(store)
    await rt2.signal("e2", "go", {"n": 42})
    handle = await rt2.resume("e2")
    assert await handle.result() == {"n": 42}
    await rt2.shutdown()


async def test_wait_event_timeout(rt):
    @workflow(name="wf_event_timeout")
    async def wf(ctx):
        try:
            await ctx.wait_event("never", timeout=0.2)
        except WaitTimeout:
            return "timed-out"

    handle = await rt.start(wf, run_id="e3")
    assert await handle.result() == "timed-out"
    events = await rt.events("e3")
    assert any(e.kind == Kind.WAIT_TIMED_OUT for e in events)


async def test_sequential_waits_consume_signals_in_order(rt):
    @workflow(name="wf_event_order")
    async def wf(ctx):
        first = await ctx.wait_event("msg")
        second = await ctx.wait_event("msg")
        return [first, second]

    await rt.start(wf, run_id="e4")
    await rt.signal("e4", "msg", "one")
    await rt.signal("e4", "msg", "two")
    handle = await rt.resume("e4")
    assert await handle.result() == ["one", "two"]


async def test_signal_to_terminal_run_is_rejected(rt):
    @workflow(name="wf_event_done")
    async def wf(ctx):
        return "done"

    handle = await rt.start(wf, run_id="e5")
    await handle.result()
    with pytest.raises(SicimError):
        await rt.signal("e5", "late", None)
