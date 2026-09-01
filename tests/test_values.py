"""Deterministic values: ctx.now / ctx.random / ctx.uuid4 replay identically."""

from helpers import Gate

from sicim import RetryPolicy, Runtime, workflow


async def test_values_are_stable_across_replay(store):
    observed = []
    gate = Gate()

    @workflow(name="wf_values")
    async def wf(ctx):
        values = {
            "now": await ctx.now(),
            "random": await ctx.random(),
            "uuid": await ctx.uuid4(),
        }
        observed.append(values)
        await ctx.step(gate, name="gate")
        return values

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="v1")
    await gate.wait_reached()
    await rt1.shutdown()

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    result = await handle.result()

    assert len(observed) == 2  # body executed twice (original + replay)...
    assert observed[0] == observed[1] == result  # ...but saw identical values
    assert isinstance(result["uuid"], str) and len(result["uuid"]) == 36
    await rt2.shutdown()


async def test_uuid_as_idempotency_key(rt):
    """The journaled uuid pattern: same key on every retry/replay of a step."""
    seen_keys = []

    async def charge(key):
        seen_keys.append(key)
        if len(seen_keys) < 2:
            raise ValueError("transient")
        return "charged"

    @workflow(name="wf_idem_key")
    async def wf(ctx):
        key = await ctx.uuid4()
        return await ctx.step(charge, key, retry=RetryPolicy(max_attempts=3, initial_interval=0.01))

    handle = await rt.start(wf, run_id="v2")
    assert await handle.result() == "charged"
    assert len(seen_keys) == 2
    assert seen_keys[0] == seen_keys[1]  # retries reuse the same idempotency key
