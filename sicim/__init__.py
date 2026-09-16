"""sicim — durable agent runtime.

Deterministic replay, checkpoint resume and saga-style compensation for
long-running (LLM agent) workflows. Zero runtime dependencies.
"""

from .context import WorkflowContext
from .errors import (
    ChildFailed,
    CompensationFailed,
    LeaseUnavailable,
    NonDeterminismError,
    NonRetryable,
    RunNotFound,
    ScheduleNotFound,
    SerializationError,
    SicimError,
    StepFailed,
    WaitTimeout,
    WorkflowCancelled,
    WorkflowFailed,
    WorkflowNotFound,
)
from .journal import Event, Kind
from .retry import NO_RETRY, RetryPolicy
from .runtime import RunHandle, Runtime
from .store import (
    InMemoryStore,
    RunRecord,
    RunStatus,
    ScheduleRecord,
    SignalRecord,
    SQLiteStore,
    Store,
)
from .workflow import get_workflow, workflow

__version__ = "0.8.0"

__all__ = [
    "ChildFailed",
    "CompensationFailed",
    "Event",
    "LeaseUnavailable",
    "InMemoryStore",
    "Kind",
    "NO_RETRY",
    "NonDeterminismError",
    "NonRetryable",
    "RetryPolicy",
    "RunHandle",
    "RunNotFound",
    "RunRecord",
    "RunStatus",
    "Runtime",
    "SQLiteStore",
    "ScheduleNotFound",
    "ScheduleRecord",
    "SerializationError",
    "SicimError",
    "SignalRecord",
    "StepFailed",
    "Store",
    "WaitTimeout",
    "WorkflowCancelled",
    "WorkflowContext",
    "WorkflowFailed",
    "WorkflowNotFound",
    "get_workflow",
    "workflow",
    "__version__",
]
