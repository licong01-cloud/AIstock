"""Bounded offline computation helpers; no trading rules or account writes."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from copy import copy
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import multiprocessing
from time import perf_counter

from backend.services.position_timing.intraday.contracts import digest
from backend.services.trading_core.models import PositionLot


@dataclass(frozen=True)
class ResearchPerformanceOptions:
    """Operational choices only; all modes must produce identical facts."""

    batch_days: int = 1
    account_workers: int = 1
    optimized: bool = False

    def __post_init__(self):
        if (type(self.batch_days) is not int or not 1 <= self.batch_days <= 20
                or type(self.account_workers) is not int or not 1 <= self.account_workers <= 4
                or type(self.optimized) is not bool):
            raise ValueError("LOCALSIM_RESEARCH_PERFORMANCE_OPTIONS_INVALID")


class ResearchMarketWorkspace:
    """Thin owner-API consumer; never decodes or repairs a dataset itself."""

    def __init__(self, reader, batch_days: int):
        self.reader = reader
        if batch_days > 1 and not callable(getattr(reader, "prepare_window", None)):
            raise ValueError("LOCALSIM_SHARED_BATCH_PREPARATION_UNAVAILABLE")

    def __enter__(self):
        return self.reader

    def __exit__(self, *_):
        close = getattr(self.reader, "close", None)
        if callable(close):
            close()


class VisibleAccountIndex:
    """Index append-only native account observations without seeing future fills.

    Observations come from _account(), so each view owns its immutable JSON
    snapshot. Duplicate acknowledgement clocks retain the first observation,
    exactly like max(..., key=clock) in the original portfolio adapter.
    """

    def __init__(self, views: list, run_id: str = ""):
        self.views = views
        self.clocks: list[datetime] = []
        self.run_id = run_id
        self._view_index = None
        self._positions = {}
        self._initial_ids = {}
        self._merged_ids = {}

    def visible(self, clock: datetime):
        if len(self.views) < len(self.clocks):
            raise ValueError("LOCALSIM_RESEARCH_ACCOUNT_OBSERVATIONS_TRUNCATED")
        for stamp, _ in self.views[len(self.clocks):]:
            if self.clocks and stamp < self.clocks[-1]:
                raise ValueError("LOCALSIM_RESEARCH_ACCOUNT_OBSERVATIONS_REVERSED")
            self.clocks.append(stamp)
        index = bisect_right(self.clocks, clock) - 1
        if index < 0:
            raise ValueError("LOCALSIM_RESEARCH_ACCOUNT_NOT_YET_VISIBLE")
        index = bisect_left(self.clocks, self.clocks[index])
        if index != self._view_index:
            self._view_index = index
            self._positions = {s: PositionLot.model_validate(p) for s, p in self.views[index][1]["positions"].items()}
            self._initial_ids.clear()
            self._merged_ids.clear()
        return self.views[index]

    def initial_feedback_id(self, revision: int) -> str:
        if revision not in self._initial_ids:
            self._initial_ids[revision] = digest([self.run_id, self.views[self._view_index][1], revision])
        return self._initial_ids[revision]

    def merged_feedback_id(self, feedback_id: str) -> str:
        if feedback_id not in self._merged_ids:
            self._merged_ids[feedback_id] = digest([feedback_id, self.views[self._view_index][1]])
        return self._merged_ids[feedback_id]

    def nav(self, ledger, *, marks: dict, clock: datetime):
        _, account = self.visible(clock)
        # account_snapshot is the existing read-only ledger authority. It reads
        # only cash/positions/prices: transaction history need not be deep-copied.
        valuation = copy(ledger)
        valuation.cash = Decimal(account["cash_cny"])
        valuation.positions = self._positions
        return valuation.account_snapshot(prices={s: float(p) for s, p in marks.items()}, snapshot_time=clock).nav


class AccountWorkerError(ValueError):
    def __init__(self, message: str, native_exception_type: str):
        super().__init__(message)
        self.native_exception_type = native_exception_type


def _isolated_account_day(job: dict) -> dict:
    """Owned compute child: no journal, DB, network, or service operations."""
    from .dual_mode_research import _compute_account_day
    started = perf_counter()
    try:
        result = _compute_account_day(job)
        return {"result": result, "compute_seconds": perf_counter() - started}
    except Exception as exc:
        # Native exceptions with custom constructors are not necessarily
        # pickleable. Preserve their type/reason without faking a success.
        return {"failure": {"exception_type": type(exc).__name__, "message": str(exc)}}


class AccountComputations:
    """At most N account jobs outstanding; only the caller commits in order."""

    def __init__(self, pool, jobs: list[dict | None]):
        self.pool = pool
        self.jobs = jobs
        self.cursor = 0
        self.futures = {}

    def _fill(self):
        while len(self.futures) < self.pool.workers and self.cursor < len(self.jobs):
            index = self.cursor
            self.cursor += 1
            if self.jobs[index] is not None:
                self.futures[index] = self.pool.executor.submit(_isolated_account_day, self.jobs[index])

    def result(self, index: int):
        job = self.jobs[index]
        if job is None:
            raise ValueError("LOCALSIM_RESEARCH_COMMITTED_DAY_RECOMPUTE_REQUESTED")
        if self.pool.workers == 1:
            from .dual_mode_research import _compute_account_day
            started = perf_counter()
            result = _compute_account_day(job)
            self.pool.compute_seconds += perf_counter() - started
            return result
        self.pool.start()
        self._fill()
        future = self.futures.pop(index)
        outcome = future.result()
        if "failure" in outcome:
            failure = outcome["failure"]
            raise AccountWorkerError(failure["message"], failure["exception_type"])
        self.pool.compute_seconds += outcome["compute_seconds"]
        self._fill()
        return outcome["result"]


class ResearchAccountPool:
    """One bounded, lazy pool per offline study, never a background scheduler."""

    def __init__(self, workers: int):
        self.workers = ResearchPerformanceOptions(account_workers=workers).account_workers
        self.executor = None
        self.compute_seconds = 0.0

    def start(self):
        if self.executor is None:
            self.executor = ProcessPoolExecutor(max_workers=self.workers, mp_context=multiprocessing.get_context("spawn"))

    def prepare(self, jobs: list[dict | None]) -> AccountComputations:
        return AccountComputations(self, jobs)

    def close(self):
        if self.executor is not None:
            self.executor.shutdown(wait=True, cancel_futures=True)
            self.executor = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
