"""Pinned workflow versions: old runs finish old logic, new runs take new logic."""

from helpers import Gate, counting_step

from sicim import Runtime, workflow


async def test_version_pinning_lets_old_runs_finish_old_logic(store):
    counters = {}
    gate = Gate()

    @workflow(name="wf_ver_pin", version=1)
    async def v1(ctx):
        base = await ctx.step(counting_step(counters, "base"), name="base")
        g = await ctx.step(gate, name="gate")
        return f"v1:{base}:{g}"

    rt1 = Runtime(store)
    await rt1.start(v1, run_id="old")
    await gate.wait_reached()
    await rt1.shutdown()

    # "Deploy" v2: new logic behind a version branch, old path preserved.
    @workflow(name="wf_ver_pin", version=2)
    async def v2(ctx):
        if ctx.version >= 2:
            extra = await ctx.step(counting_step(counters, "extra"), name="extra")
            return f"v2:{extra}"
        base = await ctx.step(counting_step(counters, "base"), name="base")
        g = await ctx.step(gate, name="gate")
        return f"v1:{base}:{g}"

    rt2 = Runtime(store)
    [old] = await rt2.recover()
    # The in-flight run resumes on the v1 branch — no NonDeterminismError.
    assert await old.result() == "v1:base:gate-2"
    assert (await rt2.status("old")).version == 1
    assert counters["base"] == 1

    new = await rt2.start(v2, run_id="new")
    assert await new.result() == "v2:extra"
    assert (await rt2.status("new")).version == 2
    assert counters["extra"] == 1
    await rt2.shutdown()
