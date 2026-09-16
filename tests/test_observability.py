"""The journal observers: the on_event hook and add_observer."""

from helpers import wait_for_events

from sicim import Kind, Runtime, workflow


async def ok_step():
    return "ok"


async def test_on_event_sees_every_journal_append(store):
    seen = []
    rt = Runtime(store, on_event=lambda run_id, event: seen.append((run_id, event.kind)))

    @workflow(name="wf_observed")
    async def wf(ctx):
        return await ctx.step(ok_step)

    handle = await rt.start(wf, run_id="obs1")
    assert await handle.result() == "ok"

    kinds = [kind for run_id, kind in seen if run_id == "obs1"]
    assert kinds[0] == Kind.RUN_STARTED
    assert Kind.STEP_COMPLETED in kinds
    assert kinds[-1] == Kind.RUN_COMPLETED
    await rt.shutdown()


async def test_broken_observer_does_not_break_the_run(store):
    def bad_observer(run_id, event):
        raise RuntimeError("observer bug")

    rt = Runtime(store, on_event=bad_observer)

    @workflow(name="wf_observed_broken")
    async def wf(ctx):
        return await ctx.step(ok_step)

    handle = await rt.start(wf, run_id="obs2")
    assert await handle.result() == "ok"
    await rt.shutdown()


async def test_add_observer_runs_alongside_on_event_and_unsubscribes(store):
    hook, extra = [], []
    rt = Runtime(store, on_event=lambda run_id, event: hook.append(event.kind))
    remove = rt.add_observer(lambda run_id, event: extra.append((run_id, event.kind)))

    @workflow(name="wf_observed_extra")
    async def wf(ctx):
        return await ctx.step(ok_step)

    assert await (await rt.start(wf, run_id="obs3")).result() == "ok"
    assert [kind for _, kind in extra] == hook  # both saw the same appends
    assert Kind.STEP_COMPLETED in hook

    remove()
    remove()  # detaching twice is not an error
    before = len(extra)
    assert await (await rt.start(wf, run_id="obs4")).result() == "ok"
    assert len(extra) == before  # detached
    assert hook[-1] == Kind.RUN_COMPLETED  # the hook itself stayed attached
    await rt.shutdown()


async def test_an_observer_added_mid_flight_sees_later_appends(store):
    """Observers attach to runs already in flight (the UI opens streams late)."""
    seen = []

    @workflow(name="wf_observed_late")
    async def wf(ctx):
        payload = await ctx.wait_event("go")
        return await ctx.step(ok_step) + payload

    rt = Runtime(store, signal_poll_interval=0.05)
    handle = await rt.start(wf, run_id="obs5")
    await wait_for_events(store, "obs5", Kind.WAIT_CREATED, 1)

    rt.add_observer(lambda run_id, event: seen.append(event.kind))
    await rt.signal("obs5", "go", "!")
    assert await handle.result() == "ok!"
    assert Kind.EVENT_CONSUMED in seen and Kind.RUN_COMPLETED in seen
    assert Kind.RUN_STARTED not in seen  # it was appended before we attached
    await rt.shutdown()


async def test_a_broken_added_observer_does_not_break_the_run(store):
    def bad(run_id, event):
        raise RuntimeError("observer bug")

    rt = Runtime(store)
    rt.add_observer(bad)

    @workflow(name="wf_observed_extra_broken")
    async def wf(ctx):
        return await ctx.step(ok_step)

    assert await (await rt.start(wf, run_id="obs6")).result() == "ok"
    await rt.shutdown()
