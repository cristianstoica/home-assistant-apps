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
    """Raised when the job's work cannot or need not be done.

    Its site-scoped target has been deleted or disabled, or (fetch_feed)
    the feed's adapter cannot be built at all. The worker completes the row
    WITHOUT the ``result='ok'`` success marker -- see
    ``_complete_and_continue`` in ``wxverify.worker.processor``.
    """
