"""Child workflows: independent journals, crash resume, failure and cancel flow."""

import asyncio

import pytest
from helpers import Gate, counting_step, wait_for_events

from sicim import ChildFailed, Kind, NonRetryable, Runtime, RunStatus, WorkflowCancelled, WorkflowFailed, workflow


async def test_child_result_is_replayed_after_parent_crash(store):
    counters = {}
    gate = Gate()

    @workflow(name="child_square")
    async def child_square(ctx, x):
        return await ctx.step(counting_step(counters, "sq", result=x * x), name="sq")

    @workflow(name="parent_square")
    async def parent(ctx, x):
        squared = await ctx.child(child_square, x)
        g = await ctx.step(gate, name="gate")
        return [squared, g]

    rt1 = Runtime(store)
    await rt1.start(parent, 6, run_id="P1")
    await gate.wait_reached()  # child already completed
    await rt1.shutdown()

    rt2 = Runtime(store)
    handles = await rt2.recover()
    result = await [h for h in handles if h.run_id == "P1"][0].result()
    assert result == [36, "gate-2"]
    assert counters["sq"] == 1  # the child was not re-executed

    child_record = await rt2.status("P1.c0")  # deterministic child run id
    assert child_record.status is RunStatus.COMPLETED
    assert child_record.result == 36
    assert child_record.parent_run_id == "P1"  # persisted parent linkage
    await rt2.shutdown()


async def test_crash_mid_child_resumes_both_runs(store):
    counters = {}
    gate = Gate()

    @workflow(name="child_with_gate")
    async def child(ctx):
        a = await ctx.step(counting_step(counters, "pre"), name="pre")
        g = await ctx.step(gate, name="gate")
        return [a, g]

    @workflow(name="parent_awaits_child")
    async def parent(ctx):
        inner = await ctx.child(child)
        return {"child": inner}

    rt1 = Runtime(store)
    await rt1.start(parent, run_id="P2")
    await gate.wait_reached()  # parked inside the child
    await rt1.shutdown()

    rt2 = Runtime(store)
    handles = await rt2.recover()
    result = await [h for h in handles if h.run_id == "P2"][0].result()
    assert result == {"child": ["pre", "gate-2"]}
    assert counters["pre"] == 1
    assert gate.calls == 2
    await rt2.shutdown()


async def test_child_failure_raises_childfailed_and_compensates_parent(rt):
    order = []

    async def step_ok():
        return "ok"

    async def comp():
        order.append("c1")

    async def boom():
        raise NonRetryable("child exploded")

    @workflow(name="child_boom")
    async def child(ctx):
        await ctx.step(boom)

    @workflow(name="parent_of_boom")
    async def parent(ctx):
        await ctx.step(step_ok, name="s1", compensate=comp)
        await ctx.child(child)

    handle = await rt.start(parent, run_id="P3")
    with pytest.raises(WorkflowFailed) as excinfo:
        await handle.result()

    assert excinfo.value.error_type == "ChildFailed"
    assert order == ["c1"]
    assert (await rt.status("P3")).status is RunStatus.FAILED
    assert (await rt.status("P3.c1")).status is RunStatus.FAILED


async def test_parent_can_catch_childfailed(rt):
    async def boom():
        raise NonRetryable("nope")

    @workflow(name="child_boom_caught")
    async def child(ctx):
        await ctx.step(boom)

    @workflow(name="parent_catches")
    async def parent(ctx):
        try:
            await ctx.child(child)
        except ChildFailed as exc:
            return {"caught": exc.error_type, "child_run": exc.run_id}

    handle = await rt.start(parent, run_id="P4")
    assert await handle.result() == {"caught": "WorkflowFailed", "child_run": "P4.c0"}


async def test_cancelling_parent_cancels_awaited_child(store):
    order = []

    async def step_ok():
        return "ok"

    def make_comp(name):
        async def comp():
            order.append(name)

        comp.__name__ = name
        return comp

    @workflow(name="child_waits")
    async def child(ctx):
        await ctx.step(step_ok, name="cs1", compensate=make_comp("child_comp"))
        await ctx.wait_event("never")

    @workflow(name="parent_cancelled")
    async def parent(ctx):
        await ctx.step(step_ok, name="ps1", compensate=make_comp("parent_comp"))
        await ctx.child(child)

    rt = Runtime(store, signal_poll_interval=0.05)
    handle = await rt.start(parent, run_id="P5")
    await wait_for_events(store, "P5.c1", Kind.WAIT_CREATED, 1)

    await rt.cancel("P5")
    with pytest.raises(WorkflowCancelled):
        await asyncio.wait_for(handle.result(), timeout=3)

    # The child receives cooperative cancellation and compensates itself.
    for _ in range(100):
        if (await rt.status("P5.c1")).status is RunStatus.CANCELLED:
            break
        await asyncio.sleep(0.03)
    assert (await rt.status("P5.c1")).status is RunStatus.CANCELLED
    assert (await rt.status("P5")).status is RunStatus.CANCELLED
    assert sorted(order) == ["child_comp", "parent_comp"]
    await rt.shutdown()


async def test_child_driven_by_another_worker_is_awaited_not_failed(store):
    @workflow(name="child_remote")
    async def child(ctx):
        return await ctx.wait_event("go")

    @workflow(name="parent_remote")
    async def parent(ctx):
        return await ctx.child(child)

    # Worker A starts the tree and crashes while the child waits for a signal.
    rt_a = Runtime(store, worker_id="A")
    await rt_a.start(parent, run_id="PR")
    await wait_for_events(store, "PR.c0", Kind.WAIT_CREATED, 1)
    await rt_a.shutdown()

    # Worker C grabs the child first; worker B recovers only the parent.
    rt_c = Runtime(store, worker_id="C", signal_poll_interval=0.05)
    child_handle = await rt_c.resume("PR.c0")
    rt_b = Runtime(store, worker_id="B", signal_poll_interval=0.05)
    [parent_handle] = await rt_b.recover()
    assert parent_handle.run_id == "PR"

    await asyncio.sleep(0.15)
    assert (await rt_b.status("PR")).status is RunStatus.RUNNING  # waiting on C's run, not failed

    await rt_c.signal("PR.c0", "go", {"ok": 1})
    assert await child_handle.result() == {"ok": 1}
    assert await asyncio.wait_for(parent_handle.result(), timeout=3) == {"ok": 1}
    assert (await rt_b.status("PR")).status is RunStatus.COMPLETED
    await rt_b.shutdown()
    await rt_c.shutdown()
