"""Workflow registration.

Workflows are plain ``async def`` functions taking a :class:`WorkflowContext`
as their first argument. The ``@workflow`` decorator registers them by name so
that ``Runtime.recover()`` can find the code for persisted runs after a
restart.
"""

from __future__ import annotations

import inspect
import logging
from typing import Any, Callable, Coroutine

from .errors import WorkflowNotFound

logger = logging.getLogger("sicim")

WorkflowFn = Callable[..., Coroutine[Any, Any, Any]]

_REGISTRY: dict[str, WorkflowFn] = {}


def workflow(fn: WorkflowFn | None = None, *, name: str | None = None, version: int = 1):
    """Register an ``async def`` function as a workflow.

    Usable bare (``@workflow``) or with options (``@workflow(name="x", version=2)``).
    Give long-lived workflows an explicit name so renaming the function does
    not orphan persisted runs.

    ``version`` is pinned into each run at start: resumed runs keep the version
    they started with, exposed as ``ctx.version``, so new code can branch
    (``if ctx.version >= 2: ...``) without breaking in-flight runs.
    """

    def decorate(func: WorkflowFn) -> WorkflowFn:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"workflow {func!r} must be an 'async def' function")
        if version < 1:
            raise ValueError("workflow version must be >= 1")
        wf_name = name or func.__name__
        if wf_name in _REGISTRY and _REGISTRY[wf_name] is not func:
            logger.debug("workflow %r re-registered (previous definition replaced)", wf_name)
        _REGISTRY[wf_name] = func
        func.__sicim_workflow__ = wf_name  # type: ignore[attr-defined]
        func.__sicim_version__ = version  # type: ignore[attr-defined]
        return func

    return decorate(fn) if fn is not None else decorate


def workflow_version(fn: WorkflowFn) -> int:
    return getattr(fn, "__sicim_version__", 1)


def workflow_name(fn: WorkflowFn) -> str:
    name = getattr(fn, "__sicim_workflow__", None)
    if name is None:
        raise TypeError(
            f"{fn!r} is not a registered workflow; decorate it with @sicim.workflow"
        )
    return name


def get_workflow(name: str) -> WorkflowFn:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise WorkflowNotFound(
            f"no workflow registered under {name!r}; make sure the module defining it "
            "is imported before Runtime.recover()/resume()"
        ) from None
