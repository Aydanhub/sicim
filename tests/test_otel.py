"""OpenTelemetry observer: spans from journal events."""

import pytest

otel_sdk = pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from sicim import NonRetryable, Runtime, WorkflowFailed, workflow
from sicim.otel import otel_observer


def make_tracer():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("sicim-test"), exporter


async def ok_step():
    return "ok"


async def test_run_and_step_spans_are_linked(store):
    tracer, exporter = make_tracer()
    rt = Runtime(store, on_event=otel_observer(tracer))

    @workflow(name="wf_otel_ok")
    async def wf(ctx):
        await ctx.step(ok_step, name="işlem")
        return "done"

    handle = await rt.start(wf, run_id="ot1")
    assert await handle.result() == "done"
    await rt.shutdown()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    run_span = spans["sicim.run wf_otel_ok"]
    step_span = spans["sicim.step işlem"]
    assert step_span.parent is not None
    assert step_span.parent.span_id == run_span.context.span_id
    assert step_span.context.trace_id == run_span.context.trace_id
    assert step_span.attributes["sicim.attempts"] == 1
    assert run_span.attributes["sicim.outcome"] == "run_completed"
    assert step_span.end_time >= step_span.start_time


async def test_child_run_span_joins_the_parents_trace(store):
    tracer, exporter = make_tracer()
    rt = Runtime(store, on_event=otel_observer(tracer))

    @workflow(name="wf_otel_child")
    async def child(ctx):
        await ctx.step(ok_step, name="inner")
        return "c"

    @workflow(name="wf_otel_parent")
    async def parent(ctx):
        return await ctx.child(child)

    handle = await rt.start(parent, run_id="otp")
    assert await handle.result() == "c"
    await rt.shutdown()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    parent_run = spans["sicim.run wf_otel_parent"]
    child_run = spans["sicim.run wf_otel_child"]
    assert child_run.parent is not None
    assert child_run.parent.span_id == parent_run.context.span_id
    assert child_run.context.trace_id == parent_run.context.trace_id
    assert child_run.attributes["sicim.parent_run_id"] == "otp"
    # The whole agent tree lands in one trace, inner step included.
    assert spans["sicim.step inner"].context.trace_id == parent_run.context.trace_id
    assert spans["sicim.child wf_otel_child"].context.trace_id == parent_run.context.trace_id


async def test_failed_run_span_has_error_status(store):
    tracer, exporter = make_tracer()
    rt = Runtime(store, on_event=otel_observer(tracer))

    async def boom():
        raise NonRetryable("patladı")

    @workflow(name="wf_otel_fail")
    async def wf(ctx):
        await ctx.step(boom)

    handle = await rt.start(wf, run_id="ot2")
    with pytest.raises(WorkflowFailed):
        await handle.result()
    await rt.shutdown()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert spans["sicim.step boom"].status.status_code is StatusCode.ERROR
    assert spans["sicim.run wf_otel_fail"].status.status_code is StatusCode.ERROR


async def test_run_span_carries_tags(store):
    tracer, exporter = make_tracer()
    rt = Runtime(store, on_event=otel_observer(tracer))

    @workflow(name="wf_otel_tags")
    async def wf(ctx):
        return "t"

    handle = await rt.start(wf, run_id="ott", tags={"customer": "42"})
    assert await handle.result() == "t"
    await rt.shutdown()

    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert spans["sicim.run wf_otel_tags"].attributes["sicim.tag.customer"] == "42"


async def test_ctx_tag_updates_become_run_span_attributes(store):
    tracer, exporter = make_tracer()
    rt = Runtime(store, on_event=otel_observer(tracer))

    @workflow(name="wf_otel_tags")
    async def wf(ctx):
        await ctx.tag({"stage": "review"})
        return "ok"

    await (await rt.start(wf, run_id="ot-tags", tags={"customer": "42"})).result()
    await rt.shutdown()
    [run_span] = [s for s in exporter.get_finished_spans() if s.name == "sicim.run wf_otel_tags"]
    assert run_span.attributes["sicim.tag.customer"] == "42"
    assert run_span.attributes["sicim.tag.stage"] == "review"
