"""OpenTelemetry integration (optional).

Requires the ``otel`` extra:  ``pip install 'sicim[otel]'``

    from sicim.otel import otel_observer
    rt = sicim.Runtime(store, on_event=otel_observer())

Spans are derived from journal appends, so timings are the journal's own:

* one span per run (``sicim.run <workflow>``), open from RUN_STARTED until the
  terminal event, ERROR status on failure;
* one child span per finished operation — step, timer, wait, child workflow,
  compensation — from its defining event to its completion, with attempt
  counts and error details as attributes.

Replayed operations never re-emit spans (replay does not append events). If a
run crosses a process crash, operations whose defining event happened in the
previous process get zero-length spans in the new one; the journal still holds
the true timing. Spans for runs that are mid-flight when the process dies are
lost — the journal, not the trace, is the source of truth.
"""

from __future__ import annotations

from typing import Any, Callable

from .journal import Event, Kind

_STARTING_KINDS = frozenset(
    {Kind.STEP_SCHEDULED, Kind.TIMER_CREATED, Kind.WAIT_CREATED, Kind.CHILD_SCHEDULED}
)


def otel_observer(tracer: Any = None) -> Callable[[str, Event], None]:
    """Build an ``on_event`` hook that emits OpenTelemetry spans.

    ``tracer`` defaults to ``opentelemetry.trace.get_tracer("sicim")``; pass
    your own for testing or custom providers.
    """
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode

    tracer = tracer or trace.get_tracer("sicim")
    run_spans: dict[str, Any] = {}
    op_starts: dict[tuple[str, int], Event] = {}

    def ns(ts: float) -> int:
        return int(ts * 1_000_000_000)

    def emit_op(
        run_id: str,
        event: Event,
        name: str,
        *,
        error: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> None:
        started = op_starts.pop((run_id, event.op_id), None)
        parent_context = None
        run_span = run_spans.get(run_id)
        if run_span is not None:
            parent_context = trace.set_span_in_context(run_span)
        span = tracer.start_span(
            name, context=parent_context, start_time=ns(started.ts if started else event.ts)
        )
        span.set_attribute("sicim.run_id", run_id)
        span.set_attribute("sicim.op_id", event.op_id)
        for key, value in (attributes or {}).items():
            span.set_attribute(key, value)
        if error is not None:
            span.set_status(Status(StatusCode.ERROR, f"{error.get('type')}: {error.get('message')}"))
        span.end(end_time=ns(event.ts))

    def observer(run_id: str, event: Event) -> None:
        kind, payload = event.kind, event.payload
        if kind == Kind.RUN_STARTED:
            span = tracer.start_span(
                f"sicim.run {payload.get('workflow', '')}", start_time=ns(event.ts)
            )
            span.set_attribute("sicim.run_id", run_id)
            span.set_attribute("sicim.workflow", payload.get("workflow", ""))
            run_spans[run_id] = span
        elif kind in _STARTING_KINDS:
            op_starts[(run_id, event.op_id)] = event
        elif kind == Kind.STEP_COMPLETED:
            emit_op(
                run_id, event, f"sicim.step {payload.get('name', '')}",
                attributes={"sicim.attempts": payload.get("attempts", 1)},
            )
        elif kind == Kind.STEP_FAILED:
            emit_op(
                run_id, event, f"sicim.step {payload.get('name', '')}",
                error=payload.get("error", {}),
                attributes={"sicim.attempts": payload.get("attempts", 1)},
            )
        elif kind == Kind.TIMER_FIRED:
            emit_op(run_id, event, "sicim.timer")
        elif kind == Kind.EVENT_CONSUMED:
            emit_op(run_id, event, f"sicim.wait {payload.get('name', '')}")
        elif kind == Kind.WAIT_TIMED_OUT:
            emit_op(
                run_id, event, f"sicim.wait {payload.get('name', '')}",
                error={"type": "WaitTimeout", "message": "no signal before deadline"},
            )
        elif kind == Kind.CHILD_COMPLETED:
            emit_op(
                run_id, event, f"sicim.child {payload.get('name', '')}",
                attributes={"sicim.child_run_id": payload.get("child_run_id", "")},
            )
        elif kind == Kind.CHILD_FAILED:
            emit_op(
                run_id, event, f"sicim.child {payload.get('name', '')}",
                error=payload.get("error", {}),
                attributes={"sicim.child_run_id": payload.get("child_run_id", "")},
            )
        elif kind == Kind.COMP_COMPLETED:
            emit_op(run_id, event, f"sicim.compensate {payload.get('name', '')}")
        elif kind == Kind.COMP_FAILED:
            emit_op(
                run_id, event, f"sicim.compensate {payload.get('name', '')}",
                error=payload.get("error", {}),
            )
        elif kind in (Kind.RUN_COMPLETED, Kind.RUN_FAILED, Kind.RUN_CANCELLED, Kind.RUN_CONTINUED):
            span = run_spans.pop(run_id, None)
            if span is not None:
                span.set_attribute("sicim.outcome", kind)
                if kind == Kind.RUN_FAILED:
                    error = payload.get("error", {})
                    span.set_status(
                        Status(StatusCode.ERROR, f"{error.get('type')}: {error.get('message')}")
                    )
                span.end(end_time=ns(event.ts))
        # VALUE_RECORDED, attempt-level and registration events: no spans (noise).

    return observer
