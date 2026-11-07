"""Typed worker-control exceptions."""

from __future__ import annotations


class JobControl(Exception):
    """Base class for expected worker control flow."""


class JobDeferred(JobControl):
    def __init__(self, next_attempt_at: str) -> None:
        super().__init__(next_attempt_at)
        self.next_attempt_at = next_attempt_at


class JobCancelled(JobControl):
    """Raised when the job's work cannot or need not be done.

    Reasons include a target that has been deleted or disabled, a feed
    adapter that cannot be built, a malformed payload, a write window that
    has closed, or a chain that a later state has superseded -- the list is
    open. The worker completes the row WITHOUT the ``result='ok'`` success
    marker -- see ``run_claimed_job`` in ``wxverify.worker.processor``.
    """
