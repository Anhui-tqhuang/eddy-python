"""Exceptions that steer how eddy treats a reconcile."""

from __future__ import annotations

from datetime import timedelta


class NotFound(LookupError):
    """The row a Table call names does not exist."""


class MigrationsMissing(RuntimeError):
    """River's tables are missing or too old in the connected schema."""


class Retry(Exception):
    """Raise from a reconciler to be called again after ``after`` without it
    counting as a failure: use it to poll something that is still converging.

        if not ready:
            raise eddy_pg.Retry(15)
    """

    def __init__(self, after: float | timedelta) -> None:
        seconds = after.total_seconds() if isinstance(after, timedelta) else float(after)
        self.after: float = max(seconds, 0.0)
        super().__init__(f"retry after {self.after:g}s")


class Permanent(Exception):
    """Raise from a reconciler when retrying cannot help: the job is cancelled
    instead of retried. Use it when the row itself is invalid and only a new
    write can fix it.
    """


OUTCOME_OK = "ok"  # reconciled
OUTCOME_RETRY = "retry"  # asked to be checked again later (Retry)
OUTCOME_ERROR = "error"  # failed; will be retried
OUTCOME_PERMANENT = "permanent"  # failed for good (Permanent)


def outcome(error: BaseException | None) -> str:
    """Classify how a reconcile ended, for metrics and tracing: "ok", "retry",
    "error" or "permanent". The Go library's instrumentation uses the same
    values."""
    if error is None:
        return OUTCOME_OK
    if isinstance(error, Permanent):
        return OUTCOME_PERMANENT
    if isinstance(error, Retry):
        return OUTCOME_RETRY
    return OUTCOME_ERROR
