"""Runtime.reset: rewinding a run's journal to an operation and replaying it."""

import asyncio

import pytest
from helpers import Gate, wait_for_events

from sicim import (
    Kind,
    LeaseUnavailable,
    NonRetryable,
    Runtime,
    RunStatus,
    SicimError,
    WorkflowFailed,
    workflow,
)


def make_step(calls, key, result=None):
    async def step():
        calls[key] = calls.get(key, 0) + 1
        return result if result is not None else key

    step.__name__ = key
    return step


async def test_reset_defaults_to_the_failed_op(rt):
    calls, state = {}, {"fail": True}

    async def flaky():
        calls["flaky"] = calls.get("flaky", 0) + 1
        if state["fail"]:
            raise NonRetryable("bad answer")
        return "fixed"

    @workflow(name="wf_reset_failed")
    async def wf(ctx):
        a = await ctx.step(make_step(calls, "a"))
        b = await ctx.step(flaky)
        c = await ctx.step(make_step(calls, "c"))
        return [a, b, c]

    handle = await rt.start(wf, run_id="r1")
    with pytest.raises(WorkflowFailed):
        await handle.result()

    state["fail"] = False
    handle = await rt.reset("r1")  # to_op=None -> the failing op
    assert await handle.result() == ["a", "fixed", "c"]

    assert calls["a"] == 1  # replayed from the journal, not re-executed
    assert calls["flaky"] == 2  # rewound and run again
    assert calls["c"] == 1
    record = await rt.status("r1")
    assert record.status is RunStatus.COMPLETED
    assert record.error is None


async def test_reset_to_zero_reruns_everything(rt):
    calls = {}

    @workflow(name="wf_reset_zero")
    async def wf(ctx):
        a = await ctx.step(make_step(calls, "a"))
        b = await ctx.step(make_step(calls, "b"))
        return [a, b]

    assert await (await rt.start(wf, run_id="r2")).result() == ["a", "b"]

    handle = await rt.reset("r2", to_op=0)
    assert await handle.result() == ["a", "b"]
    assert calls == {"a": 2, "b": 2}


async def test_reset_rewinds_deterministic_values(rt):
    @workflow(name="wf_reset_values")
    async def wf(ctx):
        first = await ctx.uuid4()
        second = await ctx.uuid4()
        return [first, second]

    before = await (await rt.start(wf, run_id="r3")).result()
    after = await (await rt.reset("r3", to_op=1)).result()

    assert after[0] == before[0]  # kept: op 0 is still journaled
    assert after[1] != before[1]  # rewound: drawn again


async def test_reset_leaves_a_clean_contiguous_journal(rt):
    @workflow(name="wf_reset_journal")
    async def wf(ctx):
        await ctx.step(make_step({}, "s1"))
        await ctx.step(make_step({}, "s2"))
        return "done"

    await (await rt.start(wf, run_id="r4")).result()
    await rt.reset("r4", to_op=1, resume=False)

    events = await rt.events("r4")
    assert [e.seq for e in events] == list(range(len(events)))
    assert [e.kind for e in events].count(Kind.RUN_COMPLETED) == 0
    assert max(e.op_id for e in events) == 0  # op 1 and later are gone
    marker = next(e for e in events if e.kind == Kind.RUN_RESET)
    assert marker.payload["to_op"] == 1
    assert marker.payload["from_status"] == RunStatus.COMPLETED.value
    assert (await rt.status("r4")).status is RunStatus.RUNNING


async def test_reset_returns_consumed_signals_to_the_inbox(rt):
    calls = {}

    @workflow(name="wf_reset_signal")
    async def wf(ctx):
        payload = await ctx.wait_event("approval")
        await ctx.step(make_step(calls, "after"))
        return payload

    handle = await rt.start(wf, run_id="r5")
    await rt.signal("r5", "approval", {"ok": True})
    assert await handle.result() == {"ok": True}

    # Rewound past the wait: the signal is available again, so the replayed
    # wait_event consumes it without anyone sending a new one.
    handle = await rt.reset("r5", to_op=0)
    assert await asyncio.wait_for(handle.result(), timeout=5) == {"ok": True}
    assert calls["after"] == 2
    assert [s.consumed for s in await rt.store.load_signals("r5")] == [True]


async def test_reset_reruns_dropped_child_runs(rt):
    calls = {}

    @workflow(name="wf_reset_child")
    async def child(ctx, label):
        return await ctx.step(make_step(calls, "child", result=f"child-{label}"))

    @workflow(name="wf_reset_parent")
    async def parent(ctx):
        a = await ctx.step(make_step(calls, "a"))
        b = await ctx.child(child, "x")
        return [a, b]

    assert await (await rt.start(parent, run_id="p1")).result() == ["a", "child-x"]
    child_id = "p1.c1"
    first_child = await rt.status(child_id)

    handle = await rt.reset("p1", to_op=1)
    assert await handle.result() == ["a", "child-x"]

    assert calls["a"] == 1  # the parent's kept op was replayed
    assert calls["child"] == 2  # the child run was deleted and started fresh
    fresh_child = await rt.status(child_id)
    assert fresh_child.created_at > first_child.created_at
    assert fresh_child.status is RunStatus.COMPLETED


async def test_reset_refuses_after_compensations_unless_forced(rt):
    order, state = [], {"fail": True}

    async def undo():
        order.append("undo")

    async def ship():
        if state["fail"]:
            raise NonRetryable("shipment failed")
        return "shipped"

    @workflow(name="wf_reset_saga")
    async def wf(ctx):
        await ctx.step(make_step({}, "reserve"), compensate=undo)
        return await ctx.step(ship)

    with pytest.raises(WorkflowFailed):
        await (await rt.start(wf, run_id="r6")).result()
    assert order == ["undo"]

    with pytest.raises(SicimError, match="compensations"):
        await rt.reset("r6")
    assert (await rt.status("r6")).status is RunStatus.FAILED  # untouched

    state["fail"] = False
    assert await (await rt.reset("r6", force=True)).result() == "shipped"
    events = await rt.events("r6")
    assert [e.kind for e in events].count(Kind.COMP_COMPLETED) == 0


async def test_reset_refuses_a_continued_link(rt):
    @workflow(name="wf_reset_continued")
    async def wf(ctx, n):
        if n >= 2:
            return n
        await ctx.continue_as_new(n + 1)

    assert await (await rt.start(wf, 1, run_id="c1")).result() == 2
    with pytest.raises(SicimError, match="continuing as"):
        await rt.reset("c1")
    assert (await rt.status("c1")).status is RunStatus.CONTINUED


async def test_reset_refuses_a_run_driven_by_another_worker(store):
    gate = Gate()

    @workflow(name="wf_reset_leased")
    async def wf(ctx):
        return await ctx.step(gate, name="gate")

    rt1 = Runtime(store, worker_id="w1")
    rt2 = Runtime(store, worker_id="w2")
    await rt1.start(wf, run_id="r7")
    await gate.wait_reached()
    try:
        with pytest.raises(LeaseUnavailable):
            await rt2.reset("r7")
        assert len(await store.load_events("r7")) > 0
    finally:
        await rt1.shutdown()
        await rt2.shutdown()


async def test_reset_stops_a_driver_this_worker_owns(rt):
    gate = Gate(hang_on={1})

    @workflow(name="wf_reset_live")
    async def wf(ctx):
        return await ctx.step(gate, name="gate")

    await rt.start(wf, run_id="r8")
    await gate.wait_reached()

    handle = await rt.reset("r8", to_op=0)
    assert await asyncio.wait_for(handle.result(), timeout=5) == "gate-2"


async def test_reset_without_resume_leaves_the_run_for_recovery(store):
    calls = {}

    @workflow(name="wf_reset_norusme")
    async def wf(ctx):
        return await ctx.step(make_step(calls, "s"))

    rt1 = Runtime(store, worker_id="w1")
    await (await rt1.start(wf, run_id="r9")).result()
    handle = await rt1.reset("r9", to_op=0, resume=False)
    assert handle.done() is False
    assert (await rt1.status("r9")).status is RunStatus.RUNNING
    assert await store.load_lease("r9") is None  # claimable elsewhere
    await rt1.shutdown()

    rt2 = Runtime(store, worker_id="w2")
    handles = await rt2.recover()
    assert [h.run_id for h in handles] == ["r9"]
    assert await handles[0].result() == "s"
    assert calls["s"] == 2
    await rt2.shutdown()


async def test_refused_reset_puts_the_run_back_to_work(store):
    """A reset that cannot claim a dropped child's lease must leave the run
    exactly as it found it — including a driver this worker owns."""
    calls = {}

    @workflow(name="wf_reset_restore_child")
    async def child(ctx):
        return await ctx.step(make_step(calls, "child"))

    @workflow(name="wf_reset_restore")
    async def parent(ctx):
        a = await ctx.step(make_step(calls, "a"))
        c = await ctx.child(child)
        return [a, c, await ctx.wait_event("go")]

    rt = Runtime(store, worker_id="w1", signal_poll_interval=0.05)
    await rt.start(parent, run_id="pr")
    await wait_for_events(store, "pr", Kind.WAIT_CREATED, 1)
    before = await store.load_events("pr")
    assert await store.try_acquire_lease("pr.c1", "other-worker", 60.0)

    try:
        with pytest.raises(LeaseUnavailable):
            await rt.reset("pr", to_op=1)

        assert [(e.seq, e.kind) for e in await store.load_events("pr")] == [
            (e.seq, e.kind) for e in before
        ]
        assert (await rt.status("pr")).status is RunStatus.RUNNING
        # The driver is a new task (the old handle stopped with the old one),
        # so the run is picked up again through the runtime.
        handle = await rt.resume("pr")
        await rt.signal("pr", "go", "ok")
        assert await asyncio.wait_for(handle.result(), timeout=5) == ["a", "child", "ok"]
        assert calls == {"a": 1, "child": 1}
    finally:
        await rt.shutdown()


async def test_reset_rejects_a_negative_op(rt):
    @workflow(name="wf_reset_negative")
    async def wf(ctx):
        return 1

    await (await rt.start(wf, run_id="r10")).result()
    with pytest.raises(ValueError):
        await rt.reset("r10", to_op=-1)
