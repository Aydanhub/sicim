"""Non-determinism detection: changed code must be caught, not corrupt state."""

import pytest
from helpers import Gate, counting_step

from sicim import NonDeterminismError, Runtime, RunStatus, workflow


async def test_changed_step_name_is_detected(store):
    counters = {}
    gate = Gate()

    @workflow(name="wf_nd")
    async def v1(ctx):
        await ctx.step(counting_step(counters, "alpha"), name="alpha")
        await ctx.step(gate, name="gate")
        return "v1"

    rt1 = Runtime(store)
    await rt1.start(v1, run_id="nd1")
    await gate.wait_reached()
    await rt1.shutdown()

    # "Deploy" different code under the same workflow name.
    @workflow(name="wf_nd")
    async def v2(ctx):
        await ctx.step(counting_step(counters, "beta"), name="beta")
        await ctx.step(gate, name="gate")
        return "v2"

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    with pytest.raises(NonDeterminismError):
        await handle.result()

    # The run is left untouched (still RUNNING): fix the code and resume again.
    assert (await rt2.status("nd1")).status is RunStatus.RUNNING
    assert counters.get("beta", 0) == 0  # divergent code never executed live
    await rt2.shutdown()


async def test_changed_operation_kind_is_detected(store):
    gate = Gate()

    @workflow(name="wf_nd_kind")
    async def v1(ctx):
        await ctx.step(counting_step({}, "a"), name="a")
        await ctx.step(gate, name="gate")

    rt1 = Runtime(store)
    await rt1.start(v1, run_id="nd2")
    await gate.wait_reached()
    await rt1.shutdown()

    @workflow(name="wf_nd_kind")
    async def v2(ctx):
        await ctx.sleep(10)  # a timer where a step was recorded
        await ctx.step(gate, name="gate")

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    with pytest.raises(NonDeterminismError):
        await handle.result()
    await rt2.shutdown()


async def test_fixed_code_can_resume_after_detection(store):
    """The recovery path: revert the code, resume the same run, it completes."""
    counters = {}
    gate = Gate()

    def define_good():
        @workflow(name="wf_nd_fix")
        async def good(ctx):
            a = await ctx.step(counting_step(counters, "alpha"), name="alpha")
            g = await ctx.step(gate, name="gate")
            return [a, g]

        return good

    define_good()
    rt1 = Runtime(store)
    await rt1.start(define_good(), run_id="nd3")
    await gate.wait_reached()
    await rt1.shutdown()

    @workflow(name="wf_nd_fix")  # bad deploy
    async def bad(ctx):
        await ctx.step(counting_step(counters, "other"), name="other")

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    with pytest.raises(NonDeterminismError):
        await handle.result()
    await rt2.shutdown()

    define_good()  # revert
    rt3 = Runtime(store)
    [handle] = await rt3.recover()
    assert await handle.result() == ["alpha", "gate-2"]
    assert counters["alpha"] == 1
    await rt3.shutdown()
