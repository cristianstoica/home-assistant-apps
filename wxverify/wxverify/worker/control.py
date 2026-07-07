"""Typed worker-control exceptions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobContinuation:
    job_type: str
    site_id: int | None
    job_key: str
    payload: dict[str, object]


class JobControl(Exception):
    """Base class for expected worker control flow."""


class JobDeferred(JobControl):
    def __init__(self, next_attempt_at: str) -> None:
        super().__init__(next_attempt_at)
        self.next_attempt_at = next_attempt_at


class JobCancelled(JobControl):
    """Raised when site-scoped work has been deleted or disabled."""
