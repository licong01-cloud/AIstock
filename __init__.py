"""
tdx_core package initialization.

Provides:
- Scheduler: validates tasks against bounded repair dates and required identity.
- QuoteAdapter: safely returns the latest close price, avoiding stale data.
"""

from __future__ import annotations
from datetime import datetime, date
from typing import Any, Dict, Optional, Tuple


class SchedulerError(Exception):
    """Base exception for scheduler related validation errors."""
    pass


class MissingTaskIdentityError(SchedulerError):
    """Raised when a task does not contain a required identifier."""
    pass


class RepairDateOutOfBoundsError(SchedulerError):
    """Raised when a task's repair date falls outside the allowed window."""
    pass


class Scheduler:
    """
    Simple scheduler that enforces:
    * Every task must have a unique identifier (`id` key).
    * If a bounded repair window is supplied, the task's `repair_date`
      must lie within that interval (inclusive).
    """

    def __init__(self, repair_window: Optional[Tuple[date, date]] = None):
        """
        Initialise the scheduler.

        Parameters
        ----------
        repair_window : tuple(date, date) | None
            A (start_date, end_date) tuple defining the permissible repair period.
            If ``None`` no date bounds are enforced.
        """
        self.repair_window = repair_window

    @staticmethod
    def _extract_task_id(task: Dict[str, Any]) -> Any:
        """Extract the identifier from a task, raising if missing."""
        if "id" not in task:
            raise MissingTaskIdentityError("Task is missing required 'id' field.")
        return task["id"]

    def _validate_repair_date(self, task: Dict[str, Any]) -> None:
        """Validate the task's repair_date against the configured window."""
        if not self.repair_window:
            return  # No bounds to enforce

        start, end = self.repair_window
        raw_date = task.get("repair_date")
        if raw_date is None:
            # No date to validate – treat as acceptable
            return

        # Accept datetime, date or ISO‑8601 string
        if isinstance(raw_date, (datetime, date)):
            repair_date = raw_date.date() if isinstance(raw_date, datetime) else raw_date
        elif isinstance(raw_date, str):
            try:
                repair_date = datetime.fromisoformat(raw_date).date()
            except ValueError as exc:
                raise RepairDateOutOfBoundsError(
                    f"Unable to parse repair_date '{raw_date}': {exc}"
                ) from exc
        else:
            raise RepairDateOutOfBoundsError(
                f"Unsupported type for repair_date: {type(raw_date)}"
            )

        if not (start <= repair_date <= end):
            raise RepairDateOutOfBoundsError(
                f"Repair date {repair_date} is outside the allowed window "
                f"({start} .. {end})."
            )

    def schedule_task(self, task: Dict[str, Any]) -> str:
        """
        Validate and accept a task for scheduling.

        Parameters
        ----------
        task : dict
            Must contain at least an ``id`` key. May contain ``repair_date``.

        Returns
        -------
        str
            Confirmation message containing the task identifier.
        """
        task_id = self._extract_task_id(task)
        self._validate_repair_date(task)
        # In a real system, scheduling logic would be placed here.
        return f"Task {task_id} scheduled successfully."


class QuoteAdapterError(Exception):
    """Base exception for quote‑adapter related problems."""
    pass


class StaleQuoteError(QuoteAdapterError):
    """Raised when the adapter would return a prior/expired close price."""
    pass


class QuoteAdapter:
    """
    Adapter that extracts the most recent close price from a quote payload,
    guarding against returning stale information.
    """

    def __init__(self, allow_stale: bool = False):
        """
        Initialise the adapter.

        Parameters
        ----------
        allow_stale : bool
            If ``False`` (default) the adapter raises ``StaleQuoteError`` when the
            supplied close price is older than the quoted ``timestamp``.
        """
        self.allow_stale = allow_stale

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        """Convert various timestamp representations to a ``datetime``."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            # Assume Unix epoch seconds
            return datetime.fromtimestamp(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as exc:
                raise QuoteAdapterError(
                    f"Unable to parse timestamp string '{value}'."
                ) from exc
        raise QuoteAdapterError(f"Unsupported timestamp type: {type(value)}")

    def get_close(self, quote: Dict[str, Any]) -> float:
        """
        Return the most recent close price from ``quote``.

        The ``quote`` dictionary is expected to contain:
        * ``close`` – numeric price.
        * ``timestamp`` – point in time the price was observed.

        If ``allow_stale`` is ``False`` and the ``timestamp`` indicates the price
        is older than the current system time, a ``StaleQuoteError`` is raised.

        Parameters
        ----------
        quote : dict
            Quote payload.

        Returns
        -------
        float
            The close price.
        """
        if "close" not in quote:
            raise QuoteAdapterError("Quote payload missing required 'close' field.")
        if "timestamp" not in quote:
            raise QuoteAdapterError("Quote payload missing required 'timestamp' field.")

        close_price = float(quote["close"])
        quote_ts = self._parse_timestamp(quote["timestamp"])

        if not self.allow_stale:
            now = datetime.utcnow()
            if quote_ts < now:
                raise StaleQuoteError(
                    f"Quote timestamp {quote_ts.isoformat()} is older than current UTC time {now.isoformat()}."
                )

        return close_price


__all__ = [
    "Scheduler",
    "SchedulerError",
    "MissingTaskIdentityError",
    "RepairDateOutOfBoundsError",
    "QuoteAdapter",
    "QuoteAdapterError",
    "StaleQuoteError",
]