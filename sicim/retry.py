"""Retry policies for steps and compensations."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from .errors import NonRetryable, SerializationError


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with jitter.

    Attempt counts survive crashes: failed attempts are journaled, so a step
    that already burned 2 of its 5 attempts before a crash resumes at attempt 3.
    Backoff waits survive too: each failed attempt journals the time of the
    next try (jitter included), so a crash mid-backoff resumes with only the
    remaining wait — and none at all if that time has already passed.
    """

    max_attempts: int = 3
    initial_interval: float = 0.5
    backoff: float = 2.0
    max_interval: float = 30.0
    jitter: float = 0.1
    non_retryable: tuple[type[BaseException], ...] = field(default=())

    def delay(self, attempt: int) -> float:
        """Delay before the attempt *after* ``attempt`` (1-based)."""
        base = min(self.initial_interval * (self.backoff ** max(attempt - 1, 0)), self.max_interval)
        if self.jitter:
            base *= 1.0 + random.uniform(-self.jitter, self.jitter)
        return max(base, 0.0)

    def should_retry(self, exc: BaseException, attempt: int) -> bool:
        if isinstance(exc, (NonRetryable, SerializationError)):
            return False
        if self.non_retryable and isinstance(exc, self.non_retryable):
            return False
        return attempt < self.max_attempts


NO_RETRY = RetryPolicy(max_attempts=1)
