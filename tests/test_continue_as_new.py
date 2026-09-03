"""Continue-as-new: bounded journals for infinitely-looping workflows."""

import asyncio

import pytest
from helpers import Gate, counting_step, wait_for_events

from sicim import Kind, Runtime, RunStatus, workflow


async def test_chain_runs_and_follow_result(rt):
    counters = {}

    @workflow(name="wf_can_basic")
    async def wf(ctx, n, acc):
        item = await ctx.step(counting_step(counters, "work", result=f"item-{n}"), name="work")
        acc = acc + [item]
        if n > 1:
            await ctx.continue_as_new(n - 1, acc)
        return {"acc": acc}

    handle = await rt.start(wf, 3, [], run_id="chain")
    result = await handle.result()  # transparently follows chain -> chain#2 -> chain#3

    assert result == {"acc": ["item-3", "item-2", "item-1"]}
    assert counters["work"] == 3  # one execution per run in the chain

    first = await rt.status("chain")
    assert first.status is RunStatus.CONTINUED
    assert first.continued_to == "chain#2"
    assert (await rt.status("chain#2")).continued_to == "chain#3"
    assert (await rt.status("chain#3")).status is RunStatus.COMPLETED

    # Each run's journal stays small — that is the point.
    for run_id in ("chain", "chain#2"):
        kinds = [e.kind for e in await rt.events(run_id)]
        assert kinds[-1] == Kind.RUN_CONTINUED
        assert len(kinds) <= 5


async def test_crash_mid_chain_resumes_from_the_live_run(store):
    counters = {}
    gate = Gate()

    @workflow(name="wf_can_crash")
    async def wf(ctx, n):
        await ctx.step(counting_step(counters, "work"), name="work")
        if n > 1:
            await ctx.continue_as_new(n - 1)
        g = await ctx.step(gate, name="gate")
        return {"finished_at": n, "gate": g}

    rt1 = Runtime(store)
    await rt1.start(wf, 3, run_id="cchain")
    await gate.wait_reached()  # we are in the third run of the chain
    await rt1.shutdown()
    assert counters["work"] == 3

    rt2 = Runtime(store)
    recovered = await rt2.recover()
    # Only the live tail is RUNNING; continued predecessors are terminal.
    assert [h.run_id for h in recovered] == ["cchain#3"]

    handle = await rt2.resume("cchain")  # old id still resolves to the final result
    assert await handle.result() == {"finished_at": 1, "gate": "gate-2"}
    assert counters["work"] == 3  # completed runs were not re-executed
    await rt2.shutdown()


async def test_signal_and_cancel_follow_the_chain(rt):
    @workflow(name="wf_can_signal")
    async def wf(ctx, hops):
        if hops > 0:
            await ctx.continue_as_new(hops - 1)
        payload = await ctx.wait_event("go")
        return payload

    handle = await rt.start(wf, 2, run_id="schain")
    await wait_for_events(rt.store, "schain#3", Kind.WAIT_CREATED, 1)

    # Signalling the ORIGINAL id reaches the live run at the end of the chain.
    await rt.signal("schain", "go", {"ok": True})
    assert await handle.result() == {"ok": True}


async def test_chain_keep_prunes_old_link_histories(store):
    @workflow(name="wf_can_autoprune")
    async def wf(ctx, n):
        if n > 0:
            await ctx.continue_as_new(n - 1)
        return await ctx.wait_event("finish")

    rt = Runtime(store, chain_keep=1)
    handle = await rt.start(wf, 3, run_id="ap")  # ap -> ap#2 -> ap#3 -> ap#4 (live)
    await wait_for_events(store, "ap#4", Kind.WAIT_CREATED, 1)

    # Run records stay behind for routing, but only the newest finished link
    # keeps its journal — that is what bounds storage for infinite agents.
    assert await store.load_events("ap") == []
    assert await store.load_events("ap#2") == []
    assert len(await store.load_events("ap#3")) > 0
    for run_id in ("ap", "ap#2", "ap#3"):
        assert (await rt.status(run_id)).status is RunStatus.CONTINUED

    # Old ids still route to the live run, and the original handle still follows.
    await rt.signal("ap", "finish", {"ok": True})
    assert await handle.result() == {"ok": True}
    await rt.shutdown()


async def test_chain_keep_zero_keeps_only_the_live_journal(store):
    @workflow(name="wf_can_autoprune0")
    async def wf(ctx, n):
        if n > 0:
            await ctx.continue_as_new(n - 1)
        return "end"

    rt = Runtime(store, chain_keep=0)
    handle = await rt.start(wf, 2, run_id="zp")  # zp -> zp#2 -> zp#3
    assert await handle.result() == "end"

    assert await store.load_events("zp") == []
    assert await store.load_events("zp#2") == []
    assert (await rt.status("zp")).status is RunStatus.CONTINUED
    assert (await rt.status("zp#3")).result == "end"

    # A late result() from the original id still walks the pruned chain.
    late = await rt.resume("zp")
    assert await late.result() == "end"
    await rt.shutdown()


async def test_continued_runs_are_prunable_history(rt):
    @workflow(name="wf_can_prune")
    async def wf(ctx, n):
        if n > 0:
            await ctx.continue_as_new(n - 1)
        return "end"

    handle = await rt.start(wf, 2, run_id="pchain")
    assert await handle.result() == "end"

    # Old chain links can be deleted without touching the live tail's result.
    await rt.store.delete_run("pchain")
    await rt.store.delete_run("pchain#2")
    assert (await rt.status("pchain#3")).result == "end"
    assert await rt.store.load_run("pchain") is None
    assert await rt.store.load_events("pchain") == []


async def test_result_follows_a_chain_driven_by_another_worker(store):
    @workflow(name="wf_can_remote")
    async def wf(ctx, hop):
        if hop:
            await ctx.continue_as_new(False)
        return await ctx.wait_event("go")

    rt_a = Runtime(store, worker_id="A")
    await rt_a.start(wf, True, run_id="CR")
    await wait_for_events(store, "CR#2", Kind.WAIT_CREATED, 1)
    await rt_a.shutdown()

    rt_c = Runtime(store, worker_id="C", signal_poll_interval=0.05)
    [live] = await rt_c.recover()
    assert live.run_id == "CR#2"
    rt_b = Runtime(store, worker_id="B", signal_poll_interval=0.05)
    handle = await rt_b.resume("CR")  # a CONTINUED record: no lease needed for the handle itself
    waiter = asyncio.ensure_future(handle.result())
    await asyncio.sleep(0.15)
    assert not waiter.done()  # following C's run through the store, not failing on its lease

    await rt_c.signal("CR#2", "go", "done")
    assert await asyncio.wait_for(waiter, timeout=3) == "done"
    await rt_b.shutdown()
    await rt_c.shutdown()
