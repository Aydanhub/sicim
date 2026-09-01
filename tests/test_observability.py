"""The on_event journal hook."""

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
