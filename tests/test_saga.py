"""Saga-style compensation: LIFO order, durability, failure surfacing."""

import pytest
from helpers import Gate

from sicim import (
    CompensationFailed,
    Kind,
    NO_RETRY,
    NonRetryable,
    Runtime,
    RunStatus,
    WorkflowFailed,
    workflow,
)


def make_comp(order, name):
    async def comp():
        order.append(name)

    comp.__name__ = name
    return comp


async def step_ok():
    return "ok"


async def boom():
    raise NonRetryable("shipment failed")


async def test_compensations_run_in_reverse_order(rt):
    order = []

    @workflow(name="wf_saga_lifo")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=make_comp(order, "c1"))
        await ctx.step(step_ok, name="s2", compensate=make_comp(order, "c2"))
        await ctx.step(boom, name="s3")

    handle = await rt.start(wf, run_id="saga1")
    with pytest.raises(WorkflowFailed) as excinfo:
        await handle.result()

    assert excinfo.value.compensated is True
    assert order == ["c2", "c1"]  # LIFO
    record = await rt.status("saga1")
    assert record.status is RunStatus.FAILED
    assert record.error["compensated"] is True
    events = await rt.events("saga1")
    assert sum(e.kind == Kind.COMP_COMPLETED for e in events) == 2


async def test_no_compensation_on_success(rt):
    order = []

    @workflow(name="wf_saga_success")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=make_comp(order, "c1"))
        return "done"

    handle = await rt.start(wf, run_id="saga-ok")
    assert await handle.result() == "done"
    assert order == []


async def test_compensation_survives_crash(store):
    """A crash mid-compensation resumes and finishes the remaining compensations."""
    order = []
    comp_gate = Gate()  # c2: hangs on its first execution

    @workflow(name="wf_saga_crash")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=make_comp(order, "c1"))
        await ctx.step(step_ok, name="s2", compensate=comp_gate)
        await ctx.step(boom, name="s3")

    rt1 = Runtime(store)
    await rt1.start(wf, run_id="saga2")
    await comp_gate.wait_reached()  # failure happened, c2 is mid-flight
    await rt1.shutdown()
    assert order == []  # c1 was never reached before the crash

    rt2 = Runtime(store)
    [handle] = await rt2.recover()
    with pytest.raises(WorkflowFailed):
        await handle.result()

    assert comp_gate.calls == 2  # c2 re-ran once after the crash
    assert order == ["c1"]  # c1 ran exactly once
    assert (await rt2.status("saga2")).status is RunStatus.FAILED
    await rt2.shutdown()


async def test_failed_compensation_marks_run_for_intervention(rt):
    async def broken_comp():
        raise RuntimeError("refund API down")

    @workflow(name="wf_saga_comp_fail")
    async def wf(ctx):
        await ctx.step(step_ok, name="s1", compensate=broken_comp, compensate_retry=NO_RETRY)
        await ctx.step(boom, name="s2")

    handle = await rt.start(wf, run_id="saga3")
    with pytest.raises(CompensationFailed) as excinfo:
        await handle.result()

    assert [f["name"] for f in excinfo.value.failures] == ["broken_comp"]
    record = await rt.status("saga3")
    assert record.status is RunStatus.COMPENSATION_FAILED
    assert record.error["compensation_failures"][0]["error"]["type"] == "RuntimeError"


async def test_manual_add_compensation(rt):
    order = []

    @workflow(name="wf_saga_manual")
    async def wf(ctx):
        await ctx.add_compensation(make_comp(order, "manual"))
        await ctx.step(boom, name="s1")

    handle = await rt.start(wf, run_id="saga4")
    with pytest.raises(WorkflowFailed):
        await handle.result()
    assert order == ["manual"]
