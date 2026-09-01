"""Exception hierarchy for sicim."""

from __future__ import annotations

from typing import Any


class SicimError(Exception):
    """Base class for all sicim errors."""


class SerializationError(SicimError):
    """A value crossing a durability boundary is not JSON-serializable.

    Workflow inputs, step results and signal payloads are persisted to the
    journal, so they must round-trip through JSON. Step *arguments* are exempt:
    they are re-supplied by code on replay and may be live objects (clients,
    connections, ...).
    """


class WorkflowNotFound(SicimError):
    """No workflow registered under the requested name."""


class RunNotFound(SicimError):
    """No run exists with the requested run_id."""


class NonDeterminismError(SicimError):
    """Replay diverged from the recorded journal.

    The workflow code requested a different operation than the one recorded at
    the same position. This usually means the workflow code changed between the
    original execution and the resume. The run is left untouched (still
    RUNNING) so it can be resumed again once the code is fixed.
    """


class NonRetryable(SicimError):
    """Raise (or wrap) inside a step to fail immediately, skipping retries."""


class StepFailed(SicimError):
    """A step exhausted its retry policy (or failed with a non-retryable error)."""

    def __init__(self, name: str, op_id: int, attempts: int, error_type: str, error_message: str):
        self.name = name
        self.op_id = op_id
        self.attempts = attempts
        self.error_type = error_type
        self.error_message = error_message
        super().__init__(
            f"step '{name}' (op {op_id}) failed after {attempts} attempt(s): "
            f"{error_type}: {error_message}"
        )


class WaitTimeout(SicimError):
    """A ctx.wait_event() call reached its deadline before a signal arrived."""

    def __init__(self, name: str, op_id: int):
        self.name = name
        self.op_id = op_id
        super().__init__(f"wait_event('{name}') (op {op_id}) timed out")


class WorkflowFailed(SicimError):
    """The workflow raised an unhandled exception; compensations (if any) succeeded."""

    def __init__(self, run_id: str, error_type: str, error_message: str, *, compensated: bool = False):
        self.run_id = run_id
        self.error_type = error_type
        self.error_message = error_message
        self.compensated = compensated
        suffix = " (compensations applied)" if compensated else ""
        super().__init__(f"run '{run_id}' failed: {error_type}: {error_message}{suffix}")


class WorkflowCancelled(SicimError):
    """The run was cancelled; compensations (if any) succeeded."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        super().__init__(f"run '{run_id}' was cancelled (compensations applied)")


class CompensationFailed(SicimError):
    """One or more compensations failed permanently; manual intervention needed.

    ``failures`` holds one dict per failed compensation:
    ``{"name": ..., "error": {"type": ..., "message": ...}, "attempts": ...}``.
    """

    def __init__(self, run_id: str, failures: list[dict[str, Any]], original_type: str, original_message: str):
        self.run_id = run_id
        self.failures = failures
        self.original_type = original_type
        self.original_message = original_message
        names = ", ".join(str(f.get("name")) for f in failures)
        super().__init__(
            f"run '{run_id}' failed ({original_type}: {original_message}) and "
            f"{len(failures)} compensation(s) also failed: [{names}]"
        )
